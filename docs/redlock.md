# Redlock Implementation in Kore

Kore now supports the **Redlock algorithm** for distributed locking across multiple instances, providing stronger guarantees for mutual exclusion in distributed systems.

## What is Redlock?

Redlock is a distributed lock algorithm designed by Redis creator Antirez. It provides fault-tolerant distributed locks by requiring a quorum of independent instances to agree on lock acquisition.

### Key Features

- **Quorum-based**: Requires majority (N/2 + 1) of instances to acquire a lock
- **Fault-tolerant**: Continues working even if some instances fail
- **Clock drift handling**: Accounts for time differences between servers
- **Automatic retry**: Built-in retry logic with randomized backoff
- **Auto-release**: Locks automatically released when dropped

## How It Works

1. **Lock Acquisition**:
   - Generate a unique lock identifier (e.g., UUID)
   - Try to acquire the lock on all N instances in parallel
   - If acquired on >= N/2 + 1 instances within TTL, lock is successful
   - Calculate validity time accounting for clock drift

2. **Lock Release**:
   - Release lock on all instances
   - Verify ownership before releasing (prevents accidental release)

3. **Lock Extension**:
   - Extend TTL on all instances
   - Requires quorum to succeed

## Usage Examples

### Basic Usage

```rust
use kore::{Cache, Redlock};
use bytes::Bytes;
use std::sync::Arc;

// Create 3 cache instances
let cache1 = Arc::new(Cache::new(CacheConfig::default()));
let cache2 = Arc::new(Cache::new(CacheConfig::default()));
let cache3 = Arc::new(Cache::new(CacheConfig::default()));

// Create Redlock (quorum = 2)
let redlock = Redlock::new(vec![cache1, cache2, cache3])?;

// Acquire a lock
let lock = redlock.lock(
    "my-resource",           // Resource name
    Bytes::from("client-1"), // Unique identifier
    10000                    // TTL in milliseconds
)?;

// Perform critical section
println!("Lock acquired for: {}", lock.resource());

// Lock is automatically released when dropped
```

### With Manual Release

```rust
let lock = redlock.lock("resource", Bytes::from("id"), 5000)?;

// Do work...

// Manually release
redlock.unlock(&lock)?;
```

### Extending Lock TTL

```rust
let lock = redlock.lock("resource", Bytes::from("id"), 5000)?;

// Need more time...
lock.extend(5000)?; // Add 5 more seconds

// Continue work...
```

### Custom Configuration

```rust
let redlock = Redlock::with_config(
    vec![cache1, cache2, cache3],
    3,      // retry_count
    200,    // retry_delay_ms
    0.01    // clock_drift_factor (1%)
)?;
```

### Concurrent Access Example

```rust
use std::thread;

let redlock = Arc::new(Redlock::new(vec![cache1, cache2, cache3])?);

// Spawn multiple threads competing for the same lock
for i in 0..10 {
    let redlock_clone = Arc::clone(&redlock);
    thread::spawn(move || {
        let client_id = Bytes::from(format!("client-{}", i));
        
        match redlock_clone.lock("shared-resource", client_id, 1000) {
            Ok(_lock) => {
                println!("Thread {} acquired lock", i);
                // Only one thread at a time can reach here
            }
            Err(e) => {
                println!("Thread {} failed: {}", i, e);
            }
        }
    });
}
```

## Configuration

### Command Line

```bash
# Enable Redlock (requires ≥3 instance addresses)
kore --enable-redlock \
     --redlock-instances "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003" \
     --redlock-retry-count 3 \
     --redlock-retry-delay-ms 200
```

Flags are validated at startup and wired into the running `Server` via
`Redlock::from_config` → `Server::with_redlock`. When enabled, `INFO` reports:

```
redlock_enabled:1
redlock_instances:3
redlock_retry_count:3
redlock_retry_delay_ms:200
```

### Options

- `--enable-redlock`: Enable Redlock distributed locking
- `--redlock-instances`: Comma-separated instance addresses (must parse as `host:port`; ≥3 when enabled)
- `--redlock-retry-count`: Number of retry attempts (default: 3)
- `--redlock-retry-delay-ms`: Delay between retries (default: 200ms)

### Backend wiring

With `--enable-redlock`, Kore opens **remote RESP backends** to each address in
`--redlock-instances` (Kore or Redis). Lock ops use:

- acquire: `SET lock:<resource> <token> NX PX <ttl>`
- release: `GET` + `DEL` if token matches
- extend: `GET` + `PEXPIRE` if token matches

Unreachable instances fail soft (count as not-acquired). Fair queue and deadlock
detection remain **in-process** on the coordinating Redlock.

Programmatic / test usage can inject **local** `Cache` backends:

```rust
use kore::{Config, Redlock, Cache};
use std::sync::Arc;

let mut config = Config::default();
config.enable_redlock = true;
config.redlock_instances = "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003".into();
config.redlock_retry_count = 5;

// Inject local caches for unit tests (skip network):
let backends = vec![
    Cache::new_with_sweep(16, 16 << 20, 1 << 20, false),
    Cache::new_with_sweep(16, 16 << 20, 1 << 20, false),
    Cache::new_with_sweep(16, 16 << 20, 1 << 20, false),
];
let redlock = Redlock::from_config(&config, Some(backends))?.unwrap();
// Or None → remote RESP backends for the listed addresses
```

## Architecture

```
┌─────────────┐
│   Client    │
└──────┬──────┘
       │
       │ lock("resource", "uuid", 10000)
       │
       v
┌──────────────────┐
│     Redlock      │
│  (Quorum: 2/3)   │
└────┬─────┬─────┬─┘
     │     │     │
     v     v     v
  ┌───┐ ┌───┐ ┌───┐
  │ C1│ │ C2│ │ C3│  Cache Instances
  └───┘ └───┘ └───┘
```

## Algorithm Details

### Quorum Calculation

- **N instances**: Quorum = N/2 + 1
- **3 instances**: Quorum = 2
- **5 instances**: Quorum = 3
- **1 instance**: Quorum = 1 (degrades to simple lock)

### Validity Time

```
validity_time = TTL - elapsed_time - drift
drift = (TTL * clock_drift_factor) + 2ms
```

### Retry Logic

1. Attempt to acquire lock
2. If failed and retries remaining:
   - Wait `retry_delay_ms + random_jitter`
   - Try again
3. Return error after max retries

## Safety Properties

1. **Mutual Exclusion**: At most one client holds a lock at any time
2. **Deadlock Free**: Locks automatically expire via TTL
3. **Fault Tolerance**: Works with minority of instances failed
4. **Liveness**: Lock acquisition eventually succeeds if instances are available

## Performance

### Time Complexity

- **Lock**: O(N) where N = number of instances
- **Unlock**: O(N)
- **Extend**: O(N)

### Recommendations

- **3-5 instances**: Optimal for most use cases
- **Odd number**: Easier quorum calculation
- **Fast network**: Minimize lock acquisition time
- **TTL >= 3 seconds**: Account for network delays

## Testing

Run the Redlock tests:

```bash
cargo test --test redlock_test
cargo test --test redlock_wiring_test
```

Available tests:
- `test_redlock_basic_lock`: Basic lock acquisition
- `test_redlock_mutual_exclusion`: Ensures only one client holds lock
- `test_redlock_auto_release`: Verifies automatic lock release
- `test_redlock_extend`: Tests lock TTL extension
- `test_redlock_concurrent_access`: Concurrent access patterns
- `test_redlock_quorum_requirement`: Verifies quorum logic
- `test_redlock_ttl_expiration`: Tests TTL expiration
- Wiring (`redlock_wiring_test`): `from_config` disabled/retry params, Server exposure, INFO fields

## Comparison: Basic vs Redlock

| Feature | Basic Mode | Redlock Mode |
|---------|-----------|--------------|
| Instances | Single | Multiple (3+) |
| Quorum | N/A | N/2 + 1 |
| Fault Tolerance | None | High |
| Clock Drift | Not handled | Handled |
| Retry Logic | Manual | Automatic |
| Use Case | Simple apps | Distributed systems |
| Complexity | O(1) | O(N) |

## Best Practices

1. **Use UUID for lock values**: Ensures uniqueness
2. **Set appropriate TTL**: Long enough for operation, short enough for recovery
3. **Handle failures**: Always check lock acquisition result
4. **Use try-finally**: Or rely on Drop for cleanup
5. **Monitor lock metrics**: Track acquisition failures
6. **Odd instance count**: Simplifies quorum
7. **Network reliability**: Ensure stable connections between instances

## Limitations

1. **Not for long-running tasks**: TTL must account for operation time
2. **Network dependent**: Requires reliable network
3. **Clock synchronization**: Assumes reasonable clock sync (NTP)
4. **No lock queuing**: First-come-first-served with retries


## Fair lock queueing

When multiple clients contend for the same resource, fair queueing ensures
FIFO (with optional priority) ordering so waiters are not starved.

### Enable via CLI

```bash
kore --enable-redlock \
     --redlock-instances 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 \
     --enable-fair-queue \
     --fair-queue-max-size 1024 \
     --fair-queue-cleanup-ms 500
```

### Programmatic API

```rust
let redlock = Redlock::new(vec![c1, c2, c3])?
    .with_fair_queueing(1024);
// or with background cleanup of expired waiters:
let redlock = Redlock::new(vec![c1, c2, c3])?
    .with_fair_queueing_cleanup(1024, 500);
```

### Semantics

1. On `lock`, the client is **enqueued** for the resource (priority, then FIFO).
2. Only the **front** waiter may attempt quorum acquisition (`try_acquire` under a write lock).
3. On success, the waiter is removed with `dequeue_client` (front-safe: never pops a different client).
4. On timeout / final failure, the waiter is **removed** so the next client can proceed.
5. Queue entries expire after their request TTL; a background cleanup thread (optional) sweeps them.

### INFO metrics

`INFO` includes a `# FairQueue` section when Redlock is wired into the server:

| Field | Meaning |
|-------|---------|
| `fair_queue_enabled` | 1 if fair queueing is on |
| `fair_queue_total_queued` | Current waiters across resources |
| `fair_queue_active_queues` | Resources with non-empty queues |
| `fair_queue_total_enqueued` | Lifetime enqueues |
| `fair_queue_total_dequeued` | Lifetime successful dequeue |
| `fair_queue_total_expired` | Lifetime expired removals |
| `fair_queue_total_claim_denied` | Non-front clients denied a turn |
| `fair_queue_total_removed` | Explicit removes (timeout/fail) |
| `fair_queue_max_wait_time_ms` | Max observed wait |
| `fair_queue_avg_wait_time_ms` | Rolling average wait |

### Production notes

- Prefer `with_fair_queueing_cleanup` (or CLI `--enable-fair-queue`) so abandoned waiters do not pin memory.
- Fair queueing raises the effective retry budget to the lock TTL so waiters are not cut off by a small `retry_count`.
- Remote multi-process Redlock backends remain deferred; fair queue state is **in-process** with the local Redlock instance.
