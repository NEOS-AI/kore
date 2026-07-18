# Deadlock Detection in Kore

Kore provides **automatic deadlock detection** for distributed locks using a wait-for graph algorithm. This feature helps identify and optionally resolve deadlock situations in multi-client scenarios.

## What is Deadlock?

A deadlock occurs when two or more clients are waiting for resources held by each other, creating a circular dependency that prevents any of them from proceeding.

### Classic Deadlock Example

```
Client 1: Holds Lock A, Waits for Lock B
Client 2: Holds Lock B, Waits for Lock A
→ Deadlock! Neither can proceed.
```

## Features

- **Automatic Detection**: Uses wait-for graph and cycle detection
- **Lock Tracking**: Monitors which clients hold which locks
- **Wait Graph**: Tracks dependencies between waiting clients
- **Victim Selection**: Chooses which lock to release (auto-resolve mode)
- **Statistics**: Provides metrics on locks and waits
- **Configurable**: Adjustable timeouts and auto-resolution
- **Async API**: Tokio-friendly wrappers + optional background monitor

## How It Works

### Wait-For Graph

The deadlock detector maintains a directed graph where:
- **Nodes**: Represent clients
- **Edges**: Represent "client A waits for resource held by client B"
- **Cycle**: Indicates a deadlock

### Detection Algorithm

1. **Track Lock Acquisitions**: Record when clients acquire locks
2. **Track Waits**: Record when clients wait for locks
3. **Build Graph**: Create edges from waiters to holders
4. **Detect Cycles**: Use Depth-First Search (DFS) to find cycles
5. **Report/Resolve**: Return deadlock information or auto-resolve

## Usage

### Basic Usage with Redlock

```rust
use kore::{Cache, Redlock};
use bytes::Bytes;
use std::sync::Arc;

// Create cache instances
let cache1 = Cache::new(256, 100 * 1024 * 1024);
let cache2 = Cache::new(256, 100 * 1024 * 1024);
let cache3 = Cache::new(256, 100 * 1024 * 1024);

// Create Redlock with deadlock detection
let redlock = Redlock::new(vec![cache1, cache2, cache3])?
    .with_deadlock_detection(
        30000,  // max_wait_time_ms: 30 seconds
        false   // auto_resolve: manual handling
    );

// Try to acquire a lock
let client_id = Bytes::from("client-1");
match redlock.lock("my-resource", client_id, 10000) {
    Ok(lock) => {
        println!("Lock acquired");
        // ... do work ...
    }
    Err(kore::Error::DeadlockDetected(msg)) => {
        println!("Deadlock detected: {}", msg);
        // Handle deadlock (retry, abort, etc.)
    }
    Err(e) => {
        println!("Other error: {}", e);
    }
}
```

### Standalone Deadlock Detector

```rust
use kore::DeadlockDetector;
use bytes::Bytes;

// Create detector
let detector = DeadlockDetector::new(
    30000,  // max wait time in ms
    false   // auto-resolve disabled
);

// Record lock events
let client1 = Bytes::from("client-1");
let client2 = Bytes::from("client-2");

// Client 1 holds resource A
detector.record_lock_acquired("resource-a".to_string(), client1.clone(), 10000);

// Client 2 holds resource B
detector.record_lock_acquired("resource-b".to_string(), client2.clone(), 10000);

// Client 1 waits for resource B
detector.record_lock_wait("resource-b".to_string(), client1.clone(), 10000);

// Client 2 waits for resource A (creates deadlock)
detector.record_lock_wait("resource-a".to_string(), client2.clone(), 10000);

// Check for deadlock
use kore::DeadlockStatus;

match detector.detect_deadlock() {
    DeadlockStatus::Deadlock { cycle, resources } => {
        println!("Deadlock detected!");
        println!("  Clients involved: {:?}", cycle);
        println!("  Resources involved: {:?}", resources);
    }
    DeadlockStatus::NoDeadlock => {
        println!("No deadlock");
    }
}
```

### Auto-Resolution Mode

```rust
// Enable automatic deadlock resolution
let redlock = Redlock::new(vec![cache1, cache2, cache3])?
    .with_deadlock_detection(30000, true); // auto_resolve = true

// Deadlocks will be automatically resolved by selecting a victim
```

### Checking Statistics

```rust
if let Some(stats) = redlock.get_deadlock_stats() {
    println!("Held locks: {}", stats.held_locks_count);
    println!("Waiting clients: {}", stats.waiting_clients_count);
    println!("Wait graph edges: {}", stats.wait_graph_edges);
}
```

### Manual Deadlock Check

```rust
use kore::DeadlockStatus;

if let Some(status) = redlock.check_deadlock() {
    match status {
        DeadlockStatus::Deadlock { cycle, resources } => {
            println!("Found deadlock:");
            println!("  Cycle length: {}", cycle.len());
            println!("  Resources: {:?}", resources);
            
            // Handle manually (e.g., release a lock, notify admin)
        }
        DeadlockStatus::NoDeadlock => {
            println!("System is healthy");
        }
    }
}
```

## Deadlock Scenarios

### 1. Simple Two-Client Deadlock

```rust
// Client 1
let lock_a = redlock.lock("resource-a", client1_id, 10000)?;
// ... try to get resource-b ...

// Client 2
let lock_b = redlock.lock("resource-b", client2_id, 10000)?;
// ... try to get resource-a ...  ← DEADLOCK
```

### 2. Three-Way Circular Deadlock

```rust
// Client 1: A → waits for B
// Client 2: B → waits for C
// Client 3: C → waits for A  ← DEADLOCK CYCLE
```

### 3. Complex Multi-Resource Deadlock

```rust
// Client 1: holds A, B → waits for C
// Client 2: holds C, D → waits for E
// Client 3: holds E → waits for A  ← DEADLOCK
```

## Prevention Strategies

While Kore can *detect* deadlocks, prevention is better:

### 1. Lock Ordering

```rust
// Always acquire locks in the same order
let resources = vec!["resource-a", "resource-b", "resource-c"];
resources.sort(); // Consistent ordering

for resource in resources {
    let lock = redlock.lock(resource, client_id.clone(), 10000)?;
    locks.push(lock);
}
```

### 2. Timeout-Based Acquisition

```rust
use std::time::Duration;

// Set reasonable TTLs
let lock = redlock.lock("resource", client_id, 5000)?; // 5 second TTL

// Use bounded retries
let redlock_with_retries = Redlock::with_config(
    instances,
    3,      // Only 3 retries
    200,    // 200ms between retries
    0.01
)?;
```

### 3. Try-Lock Pattern

```rust
// Try to get all locks, release if can't get all
let lock_a = redlock.lock("resource-a", client_id.clone(), 10000)?;

match redlock.lock("resource-b", client_id.clone(), 1000) {
    Ok(lock_b) => {
        // Got both locks, proceed
    }
    Err(_) => {
        // Couldn't get second lock, release first
        drop(lock_a);
        // Retry or abort
    }
}
```

## Configuration

### DeadlockDetector Parameters

```rust
use kore::{DeadlockDetector, VictimSelectionStrategy};

// Default strategy is Youngest (backward compatible)
DeadlockDetector::new(
    max_wait_time_ms: u64,  // Max time to wait before flagging (default: 30000)
    auto_resolve: bool       // Automatically resolve deadlocks (default: false)
);

// Explicit strategy
DeadlockDetector::new_with_strategy(
    30000,
    true,
    VictimSelectionStrategy::Oldest,
);

// Builder-style override
DeadlockDetector::new(30000, true)
    .with_victim_strategy(VictimSelectionStrategy::FewestLocks);
```

- **max_wait_time_ms**: Maximum wait time before considering it a potential deadlock
  - Too low: False positives under load
  - Too high: Delayed detection
  - Recommended: 10-30 seconds

- **auto_resolve**: Automatic victim selection and lock release
  - `false`: Detection only, manual handling required
  - `true`: Automatic resolution using the configured strategy

### Victim Selection Strategies

When auto-resolve is enabled, the detector selects a victim from the deadlock cycle
using [`VictimSelectionStrategy`]:

| Strategy | Behavior |
|----------|----------|
| **`Youngest`** (default) | Client whose most recent held-lock acquisition is newest. Minimizes wasted work by aborting newer operations. |
| **`Oldest`** | Client whose earliest held-lock acquisition is oldest. Prefers aborting long-running holders. |
| **`FewestLocks`** | Client holding the fewest locks in the cycle. Ties break toward **Youngest** (most recent max timestamp). |

Via Redlock:

```rust
use kore::{Redlock, VictimSelectionStrategy};

// Default Youngest
let redlock = Redlock::new(instances)?
    .with_deadlock_detection(30000, true);

// Explicit strategy
let redlock = Redlock::new(instances)?
    .with_deadlock_detection_strategy(
        30000,
        true,
        VictimSelectionStrategy::FewestLocks,
    );
```

## Async API (Tokio)

`DeadlockDetector` remains synchronous under the hood (`parking_lot::RwLock`
with short critical sections). Async wrappers call the sync methods directly —
they are safe to `.await` on a Tokio worker without `spawn_blocking`.

### Async detect / resolve / stats

```rust
use kore::{DeadlockDetector, DeadlockStatus};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

async fn check(detector: &DeadlockDetector) {
    match detector.detect_deadlock_async().await {
        DeadlockStatus::Deadlock { cycle, resources } => {
            println!("cycle={:?} resources={:?}", cycle, resources);
            // Optional: pick a victim (requires auto_resolve = true)
            if let Some(victim) = detector.resolve_deadlock_async(&cycle).await {
                detector.release_client_locks(&victim);
                detector.remove_from_waiting(&victim);
            }
        }
        DeadlockStatus::NoDeadlock => {}
    }

    let stats = detector.get_stats_async().await;
    let _long = detector.check_long_waits_async().await;
    let _ = stats;
}
```

| Sync | Async |
|------|-------|
| `detect_deadlock` | `detect_deadlock_async` |
| `resolve_deadlock` | `resolve_deadlock_async` |
| `get_stats` | `get_stats_async` |
| `check_long_waits` | `check_long_waits_async` |

### Background monitor

```rust
use kore::DeadlockDetector;
use std::sync::Arc;
use std::time::Duration;

let detector = Arc::new(DeadlockDetector::new(30_000, true)); // auto_resolve
let handle = DeadlockDetector::spawn_monitor(
    Arc::clone(&detector),
    Duration::from_secs(1),
);

// ... application work ...

// Stop the monitor (dropping the handle alone does not abort the task)
handle.abort();
```

On each tick the monitor:
1. Runs cycle detection
2. If a deadlock is found **and** `auto_resolve` is enabled, selects a victim
   (via the configured [`VictimSelectionStrategy`]), releases that client's
   tracked locks (`release_client_locks`), and removes them from the wait graph
3. Logs with `tracing` (`warn` on detect, `info` on victim release)

When `auto_resolve` is `false`, the monitor only logs detections and leaves
the wait-for graph unchanged.

### Redlock async helpers

```rust
// Returns None when deadlock detection is not enabled
if let Some(status) = redlock.check_deadlock_async().await {
    // handle status
}
let stats = redlock.get_deadlock_stats_async().await;
```

Lock acquisition itself remains synchronous (`std::thread::sleep` retries).
Use the async check/stats helpers from async tasks for monitoring; do not hold
a Tokio worker across a long `Redlock::lock` call — run that on a blocking
pool if needed.

## Performance Considerations

### Time Complexity

- **Lock tracking**: O(1) per operation
- **Deadlock detection**: O(V + E) where V = clients, E = wait edges
- **Cycle detection**: O(V) per DFS

### Memory Usage

- Lock info: ~100 bytes per held lock
- Wait edge: ~150 bytes per waiting client
- Overall: Minimal for typical workloads (< 100 clients)

### Detection Frequency

```rust
// Deadlock check happens:
// 1. Before each lock attempt (if enabled)
// 2. On-demand via check_deadlock()
// 3. During cleanup (automatic)
```

## Best Practices

### 1. Enable for Critical Systems

```rust
// For high-value transactions or critical sections
let redlock = Redlock::new(instances)?
    .with_deadlock_detection(15000, true);
```

### 2. Monitor Statistics

```rust
// Periodic monitoring
loop {
    if let Some(stats) = redlock.get_deadlock_stats() {
        if stats.waiting_clients_count > 10 {
            println!("Warning: {} clients waiting!", stats.waiting_clients_count);
        }
    }
    thread::sleep(Duration::from_secs(60));
}
```

### 3. Handle Deadlock Errors Gracefully

```rust
match redlock.lock("resource", client_id, 10000) {
    Err(Error::DeadlockDetected(msg)) => {
        // Log for analysis
        log::warn!("Deadlock: {}", msg);
        
        // Exponential backoff retry
        thread::sleep(backoff_duration);
        backoff_duration *= 2;
        
        // Retry with different strategy
    }
    Ok(lock) => { /* ... */ }
    Err(e) => { /* ... */ }
}
```

### 4. Use Appropriate Timeouts

```rust
// Long-running operations
let redlock = Redlock::new(instances)?
    .with_deadlock_detection(
        60000,  // 1 minute max wait
        false   // Manual handling
    );

// Short operations
let redlock = Redlock::new(instances)?
    .with_deadlock_detection(
        5000,   // 5 seconds max wait
        true    // Auto-resolve
    );
```

## Limitations

1. **Single-Instance Scope**: Currently detects deadlocks within a single Redlock instance
2. **No Cross-Process Detection**: Doesn't detect deadlocks across different processes (yet)
3. **Redlock acquire is still sync**: `Redlock::lock` uses blocking retries; prefer async
   detect/stats/monitor APIs for Tokio integration rather than awaiting `lock` on a worker
4. **Detection Overhead**: Small performance cost on each lock operation

## Example: Complete Deadlock-Safe Application

```rust
use kore::{Cache, Redlock, Error};
use bytes::Bytes;
use std::sync::Arc;

fn transfer_between_accounts(
    redlock: &Redlock,
    from: &str,
    to: &str,
    amount: u64,
    client_id: Bytes,
) -> Result<(), Error> {
    // Sort account names to ensure consistent lock ordering
    let mut accounts = vec![from, to];
    accounts.sort();
    
    // Acquire locks in order
    let lock1 = redlock.lock(accounts[0], client_id.clone(), 10000)?;
    let lock2 = redlock.lock(accounts[1], client_id.clone(), 10000)?;
    
    // Perform transfer
    println!("Transferring {} from {} to {}", amount, from, to);
    
    // Locks automatically released when dropped
    Ok(())
}

fn main() {
    let cache1 = Cache::new(256, 100 * 1024 * 1024);
    let cache2 = Cache::new(256, 100 * 1024 * 1024);
    let cache3 = Cache::new(256, 100 * 1024 * 1024);
    
    let redlock = Redlock::new(vec![cache1, cache2, cache3])
        .unwrap()
        .with_deadlock_detection(30000, true);
    
    // Safe transfers - no deadlock possible due to lock ordering
    let client1 = Bytes::from("client-1");
    let client2 = Bytes::from("client-2");
    
    match transfer_between_accounts(&redlock, "account-a", "account-b", 100, client1) {
        Ok(_) => println!("Transfer 1 successful"),
        Err(Error::DeadlockDetected(msg)) => println!("Deadlock: {}", msg),
        Err(e) => println!("Error: {}", e),
    }
    
    match transfer_between_accounts(&redlock, "account-b", "account-a", 50, client2) {
        Ok(_) => println!("Transfer 2 successful"),
        Err(Error::DeadlockDetected(msg)) => println!("Deadlock: {}", msg),
        Err(e) => println!("Error: {}", e),
    }
}
```

## Testing Deadlock Detection

Run the deadlock tests:

```bash
cargo test --test deadlock_test
```

Available tests:
- `test_deadlock_detection_simple`: Basic 2-client deadlock
- `test_three_way_deadlock`: 3-client circular dependency
- `test_deadlock_release_breaks_cycle`: Verifying cycle breaking
- `test_victim_selection`: Auto-resolve victim selection
- `test_deadlock_statistics`: Statistics tracking
- `test_lock_expiration_cleanup`: TTL-based cleanup
- `test_async_detect_planted_cycle`: Async detect + Redlock `check_deadlock_async`
- `test_background_monitor_auto_resolves`: `spawn_monitor` clears cycle with auto_resolve
