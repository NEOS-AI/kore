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
- **Cross-process snapshots**: Export/merge wait-for graphs across processes (no built-in transport)
- **Web UI monitoring**: Optional localhost HTML dashboard + JSON API for live wait-for graph

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

### Auto-Resolution Mode (Redlock + backends)

When `auto_resolve` is **true**, a deadlock found during `lock` /
`lock_with_priority`:

1. Selects a victim via the configured [`VictimSelectionStrategy`]
2. **Unlocks the victim's held resources on all Redlock backends**
   (token = client id / `val` — Redlock uses the same value for both)
3. Atomically cleans the wait-for graph (`release_client_locks`: held locks,
   wait edges where the victim is waiter **or** holder, and waiting entries
   for the victim and for released resources)
4. Continues the acquisition loop so a non-victim waiter can proceed

The victim may still own a live `Lock` RAII handle. On Drop, `unlock` uses
`record_lock_released(resource, client_id)` which **only** clears the graph
entry when that client is still the tracked holder. Held removal and wait-graph
prune run in **one** critical section and prune is **holder-scoped**
(`edge.holder == client_id` for that resource), so a concurrent re-acquire +
wait cannot lose edges to a delayed all-resource wipe. If a waiter re-acquired
after force-unlock, the new holder's graph entry is preserved (backends were
already safe via `release_if_equal`).

When `auto_resolve` is **false**, Redlock **fail-fasts** with
`Error::DeadlockDetected` (no backend unlock, no graph resolve). Callers
must release locks or retry manually.

```rust
// Enable automatic deadlock resolution (backend unlock + graph cleanup)
let redlock = Redlock::new(vec![cache1, cache2, cache3])?
    .with_deadlock_detection(30000, true); // auto_resolve = true

// Or with an explicit strategy:
use kore::VictimSelectionStrategy;
let redlock = Redlock::new(vec![cache1, cache2, cache3])?
    .with_deadlock_detection_strategy(
        30000,
        true,
        VictimSelectionStrategy::Youngest, // or Oldest / FewestLocks
    );

// Deadlocks on the lock path are resolved: victim backend keys released,
// wait-for graph cleaned; non-victim side can acquire.
```

**Graph-only vs backend unlock**

| Path | Backend unlock | Graph cleanup |
|------|----------------|---------------|
| `Redlock::lock` with `auto_resolve=true` | Yes (`release_if_equal` on all instances) | Yes (`release_client_locks`) |
| `Redlock::lock` with `auto_resolve=false` | No — returns `DeadlockDetected` | Waiter removed only for the failing client |
| `DeadlockDetector::spawn_monitor` | No (detector has no backends) | Yes when `auto_resolve` |
| `Redlock::spawn_deadlock_monitor` | Yes | Yes when `auto_resolve` |

Standalone detector APIs only mutate the in-process wait-for graph. Use
Redlock's lock path or `spawn_deadlock_monitor` when backend keys must be
released too.

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
            // Optional: pick a victim (requires auto_resolve = true).
            // release_client_locks also clears victim waiting_for.
            if let Some(victim) = detector.resolve_deadlock_async(&cycle).await {
                detector.release_client_locks(&victim);
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

On each tick the **standalone** monitor:
1. Runs cycle detection
2. If a deadlock is found **and** `auto_resolve` is enabled, selects a victim
   (via the configured [`VictimSelectionStrategy`]) and runs atomic
   `release_client_locks` (held locks + wait edges + waiting_for for the
   victim and for released resources)
3. Logs with `tracing` (`warn` on detect, `info` on victim release; `warn`
   if auto_resolve but no victim)

When `auto_resolve` is `false`, the monitor only logs detections and leaves
the wait-for graph unchanged.

**Abort handle is still required** — dropping a `JoinHandle` does not stop the
task. Always `handle.abort()` (and optionally `await` it) on shutdown.

### Redlock monitor + accessor

```rust
// Shared Arc for custom monitoring / tests
if let Some(detector) = redlock.deadlock_detector() {
    let _ = detector.detect_deadlock();
}

// Monitor that unlocks backends + cleans the graph
if let Some(handle) = redlock.spawn_deadlock_monitor(Duration::from_secs(1)) {
    // ... later ...
    handle.abort();
}
```

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

## Cross-process (snapshot merge)

By default each process has its own in-memory wait-for graph. When **multiple
processes** share the same Redlock backends, a cycle can span processes: each
side only sees a half-cycle until graphs are exchanged.

Kore provides a **snapshot export/merge MVP** — no built-in network protocol
or cluster gossip. You transport snapshots over whatever bus you already have
(HTTP, message queue, shared file, admin RPC, …).

### Types

| Type | Role |
|------|------|
| `HeldLockSnapshot` | resource, client_id (UTF-8 lossy), `ttl_ms`, `held_for_ms` |
| `WaitEdgeSnapshot` | waiter, holder, resource, `wait_elapsed_ms` |
| `OrphanWaitSnapshot` | waiter, resource, `wait_elapsed_ms` (no known holder yet) |
| `DeadlockGraphSnapshot` | `held`, `waits`, `orphan_waits`, optional `source_id` |

`Instant` is not serializable; relative durations are stored on export.
On import, held locks use **remaining TTL** =
`ttl_ms.saturating_sub(held_for_ms)` with `timestamp = Instant::now()` so
transit lag does not extend remote holds. Types implement `serde::Serialize` /
`Deserialize` for JSON or any other serde format. Older snapshots without
`orphan_waits` deserialize with an empty list (`#[serde(default)]`).

### API

```rust
use kore::{DeadlockDetector, DeadlockGraphSnapshot, DeadlockStatus};
use bytes::Bytes;

// Realistic half-cycles: only local hold + local wait (no pre-planted peer holds).
// --- Process 1 (holds A, waits for B) ---
let det1 = DeadlockDetector::new(30_000, false);
let c1 = Bytes::from("client-1");
let c2 = Bytes::from("client-2");
det1.record_lock_acquired("resource-a".into(), c1.clone(), 10_000);
det1.record_lock_wait("resource-b".into(), c1.clone(), 10_000); // orphan wait

// --- Process 2 (holds B, waits for A) ---
let det2 = DeadlockDetector::new(30_000, false);
det2.record_lock_acquired("resource-b".into(), c2.clone(), 10_000);
det2.record_lock_wait("resource-a".into(), c2.clone(), 10_000); // orphan wait

// Neither process alone has edges or a cycle yet.
assert!(matches!(det1.detect_deadlock(), DeadlockStatus::NoDeadlock));
assert!(matches!(det2.detect_deadlock(), DeadlockStatus::NoDeadlock));

// Exchange snapshots over your own transport, then merge:
let mut snap1 = det1.export_snapshot();
snap1.source_id = Some("proc-1".into());
let mut snap2 = det2.export_snapshot();
snap2.source_id = Some("proc-2".into());
// snap*.orphan_waits carry the holder-less waits; snap*.held carry peer holds.

// ... send snap1 to process 2, snap2 to process 1 ...

det1.merge_snapshot(&snap2);
det2.merge_snapshot(&snap1);

// Multi-process cycle is now visible (re-link + orphan import):
match det1.detect_deadlock() {
    DeadlockStatus::Deadlock { cycle, resources } => {
        println!("cross-process deadlock: {:?} on {:?}", cycle, resources);
    }
    DeadlockStatus::NoDeadlock => {}
}
```

You may still pre-plant peer holds (e.g. from GET) before `record_lock_wait`
so a full edge is exported instead of an orphan; both paths work after merge.

### Merge rules

1. **Held locks — local wins**: if both claim the same resource, the local
   hold is kept; the remote claim is ignored. Imported holds use **remaining**
   TTL (`ttl_ms - held_for_ms`); zero remaining skips import.
2. **Edge holder reconcile**: after holds merge, any local edge whose
   `holder` ≠ `held[resource].client_id` is rewritten to the current holder.
   If rewrite would make `holder == waiter`, the edge is **dropped** (no
   self-wait / single-node false cycle). Graph is then deduped by
   `(waiter, resource, holder)`.
3. **Wait edges — union + dedupe**: edges are keyed by
   `(waiter, resource, holder)`. Merging the same snapshot twice does not
   duplicate edges. Remote edge holders are also aligned to post-merge held
   (self-waits skipped).
4. **Local wait re-link**: local `waiting_for` entries whose resource is now
   in `held` get a wait-graph edge if missing (skip self).
5. **Orphan waits**: remote `orphan_waits` become edges when the resource is
   held (local or just imported); otherwise they stay as `waiting_for` only.
6. After merge, `detect_deadlock()` / auto-resolve / monitors use the combined
   graph like any other local state.

**Local acquire path** (`record_lock_acquired`) mirrors steps 2+5 without a
merge: it rewrites edge holders for the resource to the acquirer (drops
self-waits), and re-links any `waiting_for` entries for that resource.

### How to use from two processes

1. Each process records local acquires and waits as usual (Redlock path or
   standalone `record_*` APIs). Peer holds need not be pre-planted.
2. Periodically (or on long wait) call `export_snapshot()`.
3. Publish the snapshot (JSON via serde, or any format) to peers that share
   the same lock namespace.
4. On receive, `merge_snapshot(&peer)` then `detect_deadlock()`.
5. If auto-resolve is enabled, resolution still only unlocks **local** graph
   state (and Redlock backends when using Redlock paths) — remote processes
   must also observe the resolution via their own merge/detect loop or lock
   TTL expiry.

### Honest limitations

| Limitation | Detail |
|------------|--------|
| **No transport** | Kore does not ship gossip, HTTP, or Redis pub/sub for snapshots. |
| **Not consensus** | Last-writer is not used for holds; local always wins. Stale remote holds can linger until TTL cleanup. |
| **Remaining TTL** | Import uses remaining life at export time; clock skew between processes is not corrected. |
| **Partial views** | Orphan waits export holder-less waits; one mutual merge re-links when peers export their holds. Without any exchange, cycles stay invisible. |
| **Client id encoding** | Export uses UTF-8 lossy strings; binary tokens with invalid UTF-8 are mangled on the wire. |

## CLI configuration (Redlock + detection)

Detection is configured independently of the Web UI. Params apply whenever a
detector is attached via `Redlock::from_config`.

| Flag | Default | Meaning |
|------|---------|---------|
| `--enable-deadlock-detection` | `false` | Attach a wait-for-graph detector (requires `--enable-redlock`). |
| `--deadlock-max-wait-ms` | `30000` | Max wait age for edge cleanup / long-wait checks. |
| `--deadlock-auto-resolve` | `false` | Release a victim's locks when a cycle is found on the lock path / monitor. |
| `--deadlock-victim-strategy` | `youngest` | `youngest` \| `oldest` \| `fewest-locks`. |
| `--deadlock-ui-port` | `0` (off) | Bind `127.0.0.1:<port>` for the HTML/JSON UI **only**. |

A detector is attached when **either** `--enable-deadlock-detection` **or**
`--deadlock-ui-port` is non-zero (UI port still auto-attaches for back-compat so
a live graph is available). Detection without a UI:

```bash
kore --enable-redlock --redlock-instances host1:6379,host2:6379,host3:6379 \
     --enable-deadlock-detection \
     --deadlock-max-wait-ms 15000 \
     --deadlock-auto-resolve \
     --deadlock-victim-strategy fewest-locks
```

## Web UI monitoring

Kore ships an optional **localhost-only** deadlock dashboard (hand-rolled HTTP,
same style as `--metrics-port` — no extra HTTP crates).

### Enable

```bash
# UI + live detector (UI port auto-attaches detector; params from flags)
kore --enable-redlock --redlock-instances host1:6379,host2:6379,host3:6379 \
     --deadlock-ui-port 9101 \
     --deadlock-max-wait-ms 30000

# Explicit enable + UI (same result; clearer when scripting)
kore --enable-redlock --redlock-instances host1:6379,host2:6379,host3:6379 \
     --enable-deadlock-detection --deadlock-ui-port 9101

# Open in a browser (loopback only)
open http://127.0.0.1:9101/
```

If Redlock is disabled, the UI still starts but reports `status: "disabled"`.
`--enable-deadlock-detection` without Redlock fails config validation.

### Snapshot semantics (atomic collect)

Each HTML/JSON poll builds a [`DeadlockUiSnapshot`] via
`DeadlockDetector::collect_consistent_view(cleanup = true)`:

1. **Single critical section** (lock order: `held_locks` → `waiting_for` →
   `wait_graph`) so cycle, stats, held rows, wait edges, and orphan waits cannot
   diverge under concurrent acquire/release.
2. **Cleanup-on-poll (default)**: expired holds and max-wait edges are purged
   first — the same side effect as `detect_deadlock()`. UI polls therefore
   **mutate** the in-process graph (they are not pure reads).
3. **Pure-read path**: `DeadlockUiSnapshot::from_detector_with_cleanup(det, false)`
   / `collect_consistent_view(false)` skips cleanup for read-only inspection
   (may include expired edges until a later cleanup detect).

### Endpoints

| Method / path | Response |
|---------------|----------|
| `GET /` or `GET /deadlock` | Self-contained HTML dashboard (meta-refresh 5s + light JS poll) |
| `GET /api/deadlock` or `GET /deadlock.json` | JSON snapshot of held locks, wait edges, orphan waits, cycle, stats, config |

Example JSON shape:

```json
{
  "enabled": true,
  "status": "ok",
  "cycle": [],
  "resources": [],
  "stats": {
    "held_locks_count": 0,
    "waiting_clients_count": 0,
    "wait_graph_edges": 0
  },
  "config": {
    "max_wait_time_ms": 30000,
    "auto_resolve": false,
    "victim_strategy": "youngest",
    "cleanup_on_collect": true
  },
  "held": [],
  "waits": [],
  "orphan_waits": []
}
```

`status` is one of `ok`, `deadlock`, or `disabled`. On deadlock, `cycle` lists
client ids and `resources` the involved lock names. `config` surfaces the
attached detector's parameters (empty/zero when disabled).

### Security

- **Bind address is always `127.0.0.1`** — not configurable in MVP.
- **No authentication** — treat as a local admin tool; do not port-forward or
  reverse-proxy to untrusted networks without adding auth yourself.
- The HTTP API does not call resolve/auto-resolve; it only collects a snapshot.
  Default collect **does** run expired-lock cleanup (see above).

### Programmatic use

```rust
use kore::{run_deadlock_ui_server, DeadlockDetector, DeadlockUiSnapshot};
use std::sync::Arc;
use tokio::sync::watch;

let detector = Arc::new(DeadlockDetector::new(30_000, false));
// Atomic UI snapshot (cleanup + detect + export under one lock hold):
let _snap = DeadlockUiSnapshot::from_detector(&detector);
// Pure read (no cleanup):
let _pure = DeadlockUiSnapshot::from_detector_with_cleanup(&detector, false);

let (tx, rx) = watch::channel(false);
let d = Arc::clone(&detector);
tokio::spawn(async move {
    run_deadlock_ui_server(9101, Some(d), rx).await.unwrap();
});
// later: let _ = tx.send(true);
```

## Limitations

1. **Per-process graph by default**: In-process detection only, unless you
   exchange and merge snapshots (see [Cross-process](#cross-process-snapshot-merge)).
2. **No automatic cross-process transport**: Snapshot API only; no cluster gossip.
3. **Redlock acquire is still sync**: `Redlock::lock` uses blocking retries; prefer async
   detect/stats/monitor APIs for Tokio integration rather than awaiting `lock` on a worker
4. **Detection Overhead**: Small performance cost on each lock operation
5. **Token model**: Auto-resolve backend unlock assumes Redlock `val` is both client id
   and lock token (the normal Redlock usage). Custom schemes that use a different token
   than the detector client id are not supported for backend release.
6. **Standalone monitor is graph-only**: `DeadlockDetector::spawn_monitor` does not
   touch Redlock backends — use `Redlock::spawn_deadlock_monitor` for backend unlock.
7. **Web UI is localhost-only, no auth**: Suitable for local ops; not a multi-tenant
   admin console.

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
- `test_deadlock_detection_simple`: Basic 2-client deadlock (fail-fast)
- `test_three_way_deadlock`: 3-client circular dependency
- `test_deadlock_release_breaks_cycle`: Verifying cycle breaking
- `test_victim_selection`: Auto-resolve victim selection
- `test_deadlock_statistics`: Statistics tracking
- `test_lock_expiration_cleanup`: TTL-based cleanup
- `test_async_detect_planted_cycle`: Async detect + Redlock `check_deadlock_async`
- `test_background_monitor_auto_resolves`: `spawn_monitor` clears cycle with auto_resolve
- `test_redlock_auto_resolve_youngest_releases_backend`: two-client cycle; Youngest victim backend key gone
- `test_victim_lock_drop_preserves_new_holder_graph`: auto-resolve → re-acquire → drop victim `Lock` keeps new holder in graph
- `test_redlock_auto_resolve_false_fail_fast`: `DeadlockDetected`, backends unchanged
- `test_redlock_spawn_monitor_unlocks_backends`: `spawn_deadlock_monitor` backend + graph
- Unit tests in `src/deadlock.rs`: cross-process merge (pre-planted + realistic re-link),
  remaining-TTL import, stale edge holder rewrite, conditional `record_lock_released`,
  holder-scoped release prune, merge self-wait drop, local acquire re-link/rewrite
- Unit/integration tests in `src/deadlock_ui.rs`:
  - `json_disabled_state` / `json_and_html_show_planted_cycle` / `json_escape_quotes`
  - `json_surfaces_detector_config` / `pure_read_collect_skips_cleanup_flag`
  - `http_ui_and_json_endpoints` — HTTP 200 on `/`, `/deadlock`, `/api/deadlock`,
    `/deadlock.json`; planted cycle visible; disabled detector honest
- Unit tests in `src/deadlock.rs`: `test_collect_consistent_view_*`,
  `test_victim_strategy_from_str`
- Wiring tests in `tests/redlock_wiring_test.rs`:
  - `test_from_config_deadlock_off_by_default`
  - `test_from_config_enable_deadlock_detection_flag`
  - `test_from_config_ui_port_auto_attaches_detector`
  - `test_from_config_ui_port_zero_with_detection_off_no_detector`
