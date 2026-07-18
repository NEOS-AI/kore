use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Information about a lock held or waited for
#[derive(Debug, Clone)]
pub struct LockInfo {
    /// Resource being locked
    pub resource: String,
    /// Client/thread identifier holding or waiting for the lock
    pub client_id: Bytes,
    /// When this lock was acquired or wait started
    pub timestamp: Instant,
    /// TTL in milliseconds (if applicable)
    pub ttl_ms: u64,
}

// ── Cross-process snapshot types ───────────────────────────────────────────
//
// `Instant` is not serializable. Snapshots store relative durations
// (`held_for_ms` / `wait_elapsed_ms`) so peers can reconstruct approximate
// local Instants on import. Client ids are UTF-8 lossy strings (fine for
// typical Redlock tokens; non-UTF-8 bytes are replaced on export).

/// Serializable snapshot of a held lock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeldLockSnapshot {
    /// Resource being locked
    pub resource: String,
    /// Client holding the lock (UTF-8 lossy encoding of raw bytes)
    pub client_id: String,
    /// Original TTL in milliseconds at acquisition
    pub ttl_ms: u64,
    /// Milliseconds elapsed since the lock was acquired (export-time relative)
    pub held_for_ms: u64,
}

/// Serializable snapshot of a wait-for graph edge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaitEdgeSnapshot {
    /// Client waiting for the resource
    pub waiter: String,
    /// Client currently holding the resource
    pub holder: String,
    /// Resource being waited for
    pub resource: String,
    /// Milliseconds elapsed since the wait started (export-time relative)
    pub wait_elapsed_ms: u64,
}

/// Portable wait-for graph snapshot for cross-process deadlock detection.
///
/// Exchange these between processes that share Redlock resources (over any
/// bus you choose), then [`DeadlockDetector::merge_snapshot`] and
/// [`DeadlockDetector::detect_deadlock`]. There is **no** built-in transport.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DeadlockGraphSnapshot {
    /// Locks held in the source process's view
    pub held: Vec<HeldLockSnapshot>,
    /// Wait-for edges known to the source process
    pub waits: Vec<WaitEdgeSnapshot>,
    /// Optional identifier of the exporting process (for logging / debugging)
    pub source_id: Option<String>,
}

fn client_id_to_string(id: &Bytes) -> String {
    String::from_utf8_lossy(id).into_owned()
}

fn client_id_from_string(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

impl LockInfo {
    pub fn new(resource: String, client_id: Bytes, ttl_ms: u64) -> Self {
        Self {
            resource,
            client_id,
            timestamp: Instant::now(),
            ttl_ms,
        }
    }
    
    /// Check if this lock has expired
    pub fn is_expired(&self) -> bool {
        self.timestamp.elapsed() > Duration::from_millis(self.ttl_ms)
    }
    
    /// Get remaining TTL in milliseconds
    pub fn remaining_ttl_ms(&self) -> u64 {
        let elapsed = self.timestamp.elapsed().as_millis() as u64;
        self.ttl_ms.saturating_sub(elapsed)
    }
}

/// Deadlock detection result
#[derive(Debug, Clone)]
pub enum DeadlockStatus {
    /// No deadlock detected
    NoDeadlock,
    /// Deadlock detected with cycle of clients
    Deadlock {
        /// Clients involved in the deadlock cycle
        cycle: Vec<Bytes>,
        /// Resources involved in the cycle
        resources: Vec<String>,
    },
}

/// Strategy for selecting which client in a deadlock cycle is the victim.
///
/// Used when auto-resolve is enabled. Default is [`VictimSelectionStrategy::Youngest`]
/// for backward compatibility (abort the most recent acquirer to minimize wasted work).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VictimSelectionStrategy {
    /// Most recent lock acquirer in the cycle (default).
    #[default]
    Youngest,
    /// Earliest lock acquirer in the cycle.
    Oldest,
    /// Client in the cycle holding the fewest locks.
    /// Ties break toward the youngest acquirer (most recent max timestamp).
    FewestLocks,
}

/// Wait-for graph edge
#[derive(Debug, Clone)]
struct WaitEdge {
    /// Client waiting
    waiter: Bytes,
    /// Client holding the lock
    holder: Bytes,
    /// Resource being waited for
    resource: String,
    /// When the wait started
    timestamp: Instant,
}

/// Deadlock detector using wait-for graph
pub struct DeadlockDetector {
    /// Map of resource -> client holding it
    held_locks: Arc<RwLock<HashMap<String, LockInfo>>>,
    
    /// Map of client -> resources they're waiting for
    waiting_for: Arc<RwLock<HashMap<Bytes, Vec<LockInfo>>>>,
    
    /// Wait-for graph edges (client -> clients it's waiting for)
    wait_graph: Arc<RwLock<Vec<WaitEdge>>>,
    
    /// Maximum wait time before considering it a potential deadlock (ms)
    max_wait_time_ms: u64,
    
    /// Enable automatic deadlock resolution
    auto_resolve: bool,

    /// Victim selection strategy used when auto-resolving
    victim_strategy: VictimSelectionStrategy,
}

impl DeadlockDetector {
    /// Create a new deadlock detector with the default [`VictimSelectionStrategy::Youngest`].
    pub fn new(max_wait_time_ms: u64, auto_resolve: bool) -> Self {
        Self::new_with_strategy(
            max_wait_time_ms,
            auto_resolve,
            VictimSelectionStrategy::Youngest,
        )
    }

    /// Create a new deadlock detector with an explicit victim selection strategy.
    pub fn new_with_strategy(
        max_wait_time_ms: u64,
        auto_resolve: bool,
        victim_strategy: VictimSelectionStrategy,
    ) -> Self {
        Self {
            held_locks: Arc::new(RwLock::new(HashMap::new())),
            waiting_for: Arc::new(RwLock::new(HashMap::new())),
            wait_graph: Arc::new(RwLock::new(Vec::new())),
            max_wait_time_ms,
            auto_resolve,
            victim_strategy,
        }
    }

    /// Builder-style: set the victim selection strategy.
    pub fn with_victim_strategy(mut self, strategy: VictimSelectionStrategy) -> Self {
        self.victim_strategy = strategy;
        self
    }

    /// Current victim selection strategy.
    pub fn victim_strategy(&self) -> VictimSelectionStrategy {
        self.victim_strategy
    }

    /// Whether automatic deadlock resolution is enabled.
    pub fn auto_resolve(&self) -> bool {
        self.auto_resolve
    }
    
    /// Record a lock acquisition
    pub fn record_lock_acquired(&self, resource: String, client_id: Bytes, ttl_ms: u64) {
        let lock_info = LockInfo::new(resource.clone(), client_id.clone(), ttl_ms);
        
        // Add to held locks
        self.held_locks.write().insert(resource.clone(), lock_info);
        
        // Remove from waiting list
        let mut waiting = self.waiting_for.write();
        if let Some(wait_list) = waiting.get_mut(&client_id) {
            wait_list.retain(|info| info.resource != resource);
            if wait_list.is_empty() {
                waiting.remove(&client_id);
            }
        }
        
        // Clean up wait graph
        self.wait_graph.write().retain(|edge| {
            edge.waiter != client_id || edge.resource != resource
        });
    }
    
    /// Record a lock release
    pub fn record_lock_released(&self, resource: &str) {
        self.held_locks.write().remove(resource);
        
        // Clean up wait graph edges for this resource
        self.wait_graph.write().retain(|edge| edge.resource != resource);
    }
    
    /// Record a client waiting for a lock
    pub fn record_lock_wait(&self, resource: String, client_id: Bytes, ttl_ms: u64) {
        let lock_info = LockInfo::new(resource.clone(), client_id.clone(), ttl_ms);
        
        // Add to waiting list
        self.waiting_for
            .write()
            .entry(client_id.clone())
            .or_insert_with(Vec::new)
            .push(lock_info);
        
        // Update wait graph
        if let Some(holder_info) = self.held_locks.read().get(&resource) {
            let edge = WaitEdge {
                waiter: client_id,
                holder: holder_info.client_id.clone(),
                resource,
                timestamp: Instant::now(),
            };
            self.wait_graph.write().push(edge);
        }
    }
    
    /// Remove client from waiting list (e.g., timeout or gave up)
    pub fn remove_from_waiting(&self, client_id: &Bytes) {
        self.waiting_for.write().remove(client_id);
        self.wait_graph.write().retain(|edge| edge.waiter != *client_id);
    }
    
    /// Detect deadlocks using cycle detection in wait-for graph
    pub fn detect_deadlock(&self) -> DeadlockStatus {
        // Clean up expired locks first
        self.cleanup_expired_locks();
        
        // Build adjacency list for wait-for graph
        let graph = self.wait_graph.read();
        let mut adjacency: HashMap<Bytes, Vec<(Bytes, String)>> = HashMap::new();
        
        for edge in graph.iter() {
            adjacency
                .entry(edge.waiter.clone())
                .or_insert_with(Vec::new)
                .push((edge.holder.clone(), edge.resource.clone()));
        }
        
        // Detect cycles using DFS
        let all_clients: HashSet<Bytes> = adjacency.keys().cloned().collect();
        
        for start_client in all_clients.iter() {
            if let Some((cycle, resources)) = self.find_cycle_dfs(start_client, &adjacency) {
                return DeadlockStatus::Deadlock { cycle, resources };
            }
        }
        
        DeadlockStatus::NoDeadlock
    }
    
    /// Find cycle using depth-first search
    fn find_cycle_dfs(
        &self,
        start: &Bytes,
        adjacency: &HashMap<Bytes, Vec<(Bytes, String)>>,
    ) -> Option<(Vec<Bytes>, Vec<String>)> {
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();
        let mut path = Vec::new();
        let mut resource_path = Vec::new();
        
        if self.dfs_visit(
            start,
            adjacency,
            &mut visited,
            &mut rec_stack,
            &mut path,
            &mut resource_path,
        ) {
            // Find the cycle in the path
            if let Some(cycle_start) = path.iter().position(|c| c == start) {
                let cycle = path[cycle_start..].to_vec();
                let resources = resource_path[cycle_start..].to_vec();
                return Some((cycle, resources));
            }
        }
        
        None
    }
    
    /// DFS visit helper
    fn dfs_visit(
        &self,
        node: &Bytes,
        adjacency: &HashMap<Bytes, Vec<(Bytes, String)>>,
        visited: &mut HashSet<Bytes>,
        rec_stack: &mut HashSet<Bytes>,
        path: &mut Vec<Bytes>,
        resource_path: &mut Vec<String>,
    ) -> bool {
        visited.insert(node.clone());
        rec_stack.insert(node.clone());
        path.push(node.clone());
        
        if let Some(neighbors) = adjacency.get(node) {
            for (neighbor, resource) in neighbors {
                if !visited.contains(neighbor) {
                    resource_path.push(resource.clone());
                    if self.dfs_visit(neighbor, adjacency, visited, rec_stack, path, resource_path) {
                        return true;
                    }
                    resource_path.pop();
                } else if rec_stack.contains(neighbor) {
                    // Found a cycle
                    resource_path.push(resource.clone());
                    return true;
                }
            }
        }
        
        rec_stack.remove(node);
        path.pop();
        false
    }
    
    /// Clean up expired locks and long waits.
    ///
    /// Expired held locks also drop matching wait-graph edges and
    /// `waiting_for` entries so detect does not leave unresolvable cycles
    /// (holder gone, edges still present → resolve returns `None`).
    ///
    /// Lock order: `held_locks` → `waiting_for` → `wait_graph`.
    fn cleanup_expired_locks(&self) {
        let mut held = self.held_locks.write();
        let mut waiting = self.waiting_for.write();
        let mut graph = self.wait_graph.write();

        let mut expired_resources: HashSet<String> = HashSet::new();
        held.retain(|resource, info| {
            if info.is_expired() {
                expired_resources.insert(resource.clone());
                false
            } else {
                true
            }
        });

        let max_wait = Duration::from_millis(self.max_wait_time_ms);
        graph.retain(|edge| {
            !expired_resources.contains(&edge.resource)
                && edge.timestamp.elapsed() < max_wait
        });

        waiting.retain(|_, wait_list| {
            wait_list.retain(|info| {
                !info.is_expired() && !expired_resources.contains(&info.resource)
            });
            !wait_list.is_empty()
        });
    }
    
    /// Get all currently held locks
    pub fn get_held_locks(&self) -> Vec<LockInfo> {
        self.held_locks.read().values().cloned().collect()
    }
    
    /// Get all waiting clients
    pub fn get_waiting_clients(&self) -> HashMap<Bytes, Vec<LockInfo>> {
        self.waiting_for.read().clone()
    }
    
    /// Get deadlock statistics
    pub fn get_stats(&self) -> DeadlockStats {
        let held_count = self.held_locks.read().len();
        let waiting_count = self.waiting_for.read().len();
        let wait_edges = self.wait_graph.read().len();
        
        DeadlockStats {
            held_locks_count: held_count,
            waiting_clients_count: waiting_count,
            wait_graph_edges: wait_edges,
        }
    }
    
    /// Resolve deadlock by selecting a victim according to the configured strategy.
    ///
    /// Returns `None` when the cycle is empty, auto-resolve is disabled, or no
    /// cycle member currently holds a tracked lock.
    pub fn resolve_deadlock(&self, cycle: &[Bytes]) -> Option<Bytes> {
        if cycle.is_empty() || !self.auto_resolve {
            return None;
        }

        let held = self.held_locks.read();

        // Per cycle client: (lock_count, earliest_ts, latest_ts)
        let mut candidates: Vec<(Bytes, usize, Instant, Instant)> = Vec::new();
        for client in cycle {
            let mut count = 0usize;
            let mut earliest: Option<Instant> = None;
            let mut latest: Option<Instant> = None;
            for info in held.values() {
                if info.client_id == *client {
                    count += 1;
                    earliest = Some(match earliest {
                        Some(t) if t <= info.timestamp => t,
                        _ => info.timestamp,
                    });
                    latest = Some(match latest {
                        Some(t) if t >= info.timestamp => t,
                        _ => info.timestamp,
                    });
                }
            }
            if count > 0 {
                candidates.push((
                    client.clone(),
                    count,
                    earliest.expect("count > 0 implies earliest"),
                    latest.expect("count > 0 implies latest"),
                ));
            }
        }

        if candidates.is_empty() {
            return None;
        }

        match self.victim_strategy {
            VictimSelectionStrategy::Youngest => candidates
                .into_iter()
                .max_by_key(|(_, _, _, latest)| *latest)
                .map(|(client, _, _, _)| client),
            VictimSelectionStrategy::Oldest => candidates
                .into_iter()
                .min_by_key(|(_, _, earliest, _)| *earliest)
                .map(|(client, _, _, _)| client),
            VictimSelectionStrategy::FewestLocks => {
                // Fewest held locks; ties break toward youngest (max latest timestamp).
                candidates
                    .into_iter()
                    .min_by(|a, b| {
                        a.1.cmp(&b.1)
                            .then_with(|| b.3.cmp(&a.3)) // reverse: larger latest wins
                    })
                    .map(|(client, _, _, _)| client)
            }
        }
    }
    
    /// Check for long-running waits (potential deadlock candidates)
    pub fn check_long_waits(&self) -> Vec<LongWaitInfo> {
        let max_wait = Duration::from_millis(self.max_wait_time_ms);
        let graph = self.wait_graph.read();
        
        graph
            .iter()
            .filter(|edge| edge.timestamp.elapsed() > max_wait / 2) // Warning at 50% of max
            .map(|edge| LongWaitInfo {
                waiter: edge.waiter.clone(),
                holder: edge.holder.clone(),
                resource: edge.resource.clone(),
                wait_duration_ms: edge.timestamp.elapsed().as_millis() as u64,
            })
            .collect()
    }

    /// Release all locks currently tracked as held by `client_id`.
    ///
    /// Single write critical section (lock order: `held_locks` →
    /// `waiting_for` → `wait_graph`):
    /// 1. Retain-by-client on `held_locks` (collect released resource names)
    /// 2. Strip wait-graph edges for those resources **and** edges where the
    ///    victim is the waiter
    /// 3. Prune `waiting_for` for the victim entirely and for any waiter
    ///    whose resource was released
    ///
    /// Callers do **not** need a separate [`Self::remove_from_waiting`] —
    /// victim waits are cleared here. Returns the resource names that were
    /// released from the graph (backends must be unlocked separately when
    /// used from Redlock).
    ///
    /// This is race-safe against another client re-acquiring a resource
    /// mid-cleanup: only entries whose `client_id` matches the victim are
    /// removed under the held-locks write lock.
    pub fn release_client_locks(&self, client_id: &Bytes) -> Vec<String> {
        let mut held = self.held_locks.write();
        let mut waiting = self.waiting_for.write();
        let mut graph = self.wait_graph.write();

        let mut released: Vec<String> = Vec::new();
        held.retain(|resource, info| {
            if info.client_id == *client_id {
                released.push(resource.clone());
                false
            } else {
                true
            }
        });

        let released_set: HashSet<&str> =
            released.iter().map(String::as_str).collect();

        // Drop edges for released resources and any wait edges from the victim.
        graph.retain(|edge| {
            edge.waiter != *client_id && !released_set.contains(edge.resource.as_str())
        });

        // Victim no longer waits; other waiters drop entries for released resources.
        waiting.remove(client_id);
        waiting.retain(|_, wait_list| {
            wait_list.retain(|info| !released_set.contains(info.resource.as_str()));
            !wait_list.is_empty()
        });

        released
    }

    /// Resources currently tracked as held by `client_id` (snapshot under read lock).
    ///
    /// Used by Redlock auto-resolve to unlock backends **before** graph cleanup.
    pub fn held_resources_for_client(&self, client_id: &Bytes) -> Vec<String> {
        self.held_locks
            .read()
            .iter()
            .filter(|(_, info)| info.client_id == *client_id)
            .map(|(resource, _)| resource.clone())
            .collect()
    }

    // ── Cross-process snapshot export / merge ──────────────────────────────
    //
    // MVP: processes export their local wait-for graph, exchange snapshots
    // out-of-band, and merge peer state so cycle detection spans processes
    // that share Redlock resources. No transport or consensus is provided.

    /// Export the current held locks and wait-for edges as a portable snapshot.
    ///
    /// Timestamps are converted to relative durations (`held_for_ms` /
    /// `wait_elapsed_ms`) because [`Instant`] is not serializable.
    /// `source_id` is left as `None`; callers may set it after export.
    pub fn export_snapshot(&self) -> DeadlockGraphSnapshot {
        let held = self.held_locks.read();
        let graph = self.wait_graph.read();

        let held_snaps: Vec<HeldLockSnapshot> = held
            .values()
            .map(|info| HeldLockSnapshot {
                resource: info.resource.clone(),
                client_id: client_id_to_string(&info.client_id),
                ttl_ms: info.ttl_ms,
                held_for_ms: info.timestamp.elapsed().as_millis() as u64,
            })
            .collect();

        let wait_snaps: Vec<WaitEdgeSnapshot> = graph
            .iter()
            .map(|edge| WaitEdgeSnapshot {
                waiter: client_id_to_string(&edge.waiter),
                holder: client_id_to_string(&edge.holder),
                resource: edge.resource.clone(),
                wait_elapsed_ms: edge.timestamp.elapsed().as_millis() as u64,
            })
            .collect();

        DeadlockGraphSnapshot {
            held: held_snaps,
            waits: wait_snaps,
            source_id: None,
        }
    }

    /// Merge a remote process's wait-for graph into this detector.
    ///
    /// Merge rules (MVP):
    /// - **Held locks**: local ownership wins. A remote hold is inserted only
    ///   when the resource is not already held locally.
    /// - **Wait edges**: union by `(waiter, resource, holder)` — duplicates
    ///   (including a second merge of the same snapshot) are ignored.
    /// - Remote `waiting_for` entries are synthesised from new wait edges so
    ///   stats stay consistent.
    ///
    /// After a mutual export/merge, [`Self::detect_deadlock`] can find cycles
    /// that span multiple processes (e.g. P1 holds A waits B; P2 holds B
    /// waits A).
    ///
    /// # Limitations
    /// - No automatic transport — callers exchange snapshots themselves.
    /// - Not consensus: stale remote holds may linger until TTL cleanup.
    /// - Relative timestamps are approximate (`Instant` reconstructed from
    ///   elapsed ms at export time).
    pub fn merge_snapshot(&self, remote: &DeadlockGraphSnapshot) {
        let mut held = self.held_locks.write();
        let mut waiting = self.waiting_for.write();
        let mut graph = self.wait_graph.write();

        // 1. Merge held locks — local wins on conflict.
        for remote_hold in &remote.held {
            if held.contains_key(&remote_hold.resource) {
                continue;
            }
            let client_id = client_id_from_string(&remote_hold.client_id);
            let held_for = Duration::from_millis(remote_hold.held_for_ms);
            // Reconstruct an Instant approximately held_for ago.
            let timestamp = Instant::now()
                .checked_sub(held_for)
                .unwrap_or_else(Instant::now);
            held.insert(
                remote_hold.resource.clone(),
                LockInfo {
                    resource: remote_hold.resource.clone(),
                    client_id,
                    timestamp,
                    ttl_ms: remote_hold.ttl_ms,
                },
            );
        }

        // 2. Union wait edges (dedupe waiter + resource + holder).
        for remote_edge in &remote.waits {
            let waiter = client_id_from_string(&remote_edge.waiter);
            let holder = client_id_from_string(&remote_edge.holder);
            let is_dup = graph.iter().any(|e| {
                e.waiter == waiter
                    && e.holder == holder
                    && e.resource == remote_edge.resource
            });
            if is_dup {
                continue;
            }

            let wait_elapsed = Duration::from_millis(remote_edge.wait_elapsed_ms);
            let timestamp = Instant::now()
                .checked_sub(wait_elapsed)
                .unwrap_or_else(Instant::now);

            // Keep waiting_for in sync for stats / long-wait reporting.
            let already_waiting = waiting
                .get(&waiter)
                .map(|list| list.iter().any(|i| i.resource == remote_edge.resource))
                .unwrap_or(false);
            if !already_waiting {
                waiting
                    .entry(waiter.clone())
                    .or_insert_with(Vec::new)
                    .push(LockInfo {
                        resource: remote_edge.resource.clone(),
                        client_id: waiter.clone(),
                        timestamp,
                        // Use remaining max-wait budget as a soft TTL for the wait entry.
                        ttl_ms: self.max_wait_time_ms,
                    });
            }

            graph.push(WaitEdge {
                waiter,
                holder,
                resource: remote_edge.resource.clone(),
                timestamp,
            });
        }
    }

    // ── Async API ──────────────────────────────────────────────────────────
    //
    // Critical sections use short `parking_lot::RwLock` holds, so these async
    // wrappers call the sync implementations directly. They are safe to await
    // on a Tokio worker: they do not perform I/O or long blocking work.
    // Prefer these from async contexts for a clear async surface; call the
    // sync methods when already on a blocking path.

    /// Async wrapper around [`Self::detect_deadlock`].
    ///
    /// Critical sections are short; this does not spawn a blocking task.
    pub async fn detect_deadlock_async(&self) -> DeadlockStatus {
        self.detect_deadlock()
    }

    /// Async wrapper around [`Self::resolve_deadlock`].
    pub async fn resolve_deadlock_async(&self, cycle: &[Bytes]) -> Option<Bytes> {
        self.resolve_deadlock(cycle)
    }

    /// Async wrapper around [`Self::get_stats`].
    pub async fn get_stats_async(&self) -> DeadlockStats {
        self.get_stats()
    }

    /// Async wrapper around [`Self::check_long_waits`].
    pub async fn check_long_waits_async(&self) -> Vec<LongWaitInfo> {
        self.check_long_waits()
    }

    /// Spawn a background Tokio task that periodically detects deadlocks.
    ///
    /// On each tick:
    /// 1. Run [`Self::detect_deadlock`].
    /// 2. If a cycle is found and `auto_resolve` is enabled, select a victim
    ///    via [`Self::resolve_deadlock`], release their tracked locks, and
    ///    remove them from the wait graph.
    ///
    /// The task runs until the returned [`tokio::task::JoinHandle`] is aborted
    /// or the runtime shuts down. Dropping the handle alone does **not** stop
    /// the task — call [`tokio::task::JoinHandle::abort`].
    ///
    /// # Example
    /// ```ignore
    /// let detector = Arc::new(DeadlockDetector::new(30_000, true));
    /// let handle = DeadlockDetector::spawn_monitor(detector, Duration::from_secs(1));
    /// // ... later ...
    /// handle.abort();
    /// ```
    pub fn spawn_monitor(self: Arc<Self>, interval: Duration) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // First tick completes immediately; skip so we wait a full interval first.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                match self.detect_deadlock() {
                    DeadlockStatus::Deadlock { cycle, resources } => {
                        tracing::warn!(
                            cycle_len = cycle.len(),
                            resources = ?resources,
                            "deadlock detected by background monitor"
                        );
                        if let Some(victim) = self.resolve_deadlock(&cycle) {
                            // release_client_locks also clears victim waiting_for
                            let released = self.release_client_locks(&victim);
                            tracing::info!(
                                victim = %String::from_utf8_lossy(&victim),
                                released = ?released,
                                "deadlock victim released by background monitor"
                            );
                        } else if self.auto_resolve {
                            tracing::warn!(
                                cycle_len = cycle.len(),
                                "deadlock detected but no victim selected (auto_resolve)"
                            );
                        }
                    }
                    DeadlockStatus::NoDeadlock => {}
                }
            }
        })
    }
}

impl Default for DeadlockDetector {
    fn default() -> Self {
        Self::new(30000, false) // 30 seconds max wait, no auto-resolve
    }
}

/// Deadlock statistics
#[derive(Debug, Clone)]
pub struct DeadlockStats {
    pub held_locks_count: usize,
    pub waiting_clients_count: usize,
    pub wait_graph_edges: usize,
}

/// Information about a long-running wait
#[derive(Debug, Clone)]
pub struct LongWaitInfo {
    pub waiter: Bytes,
    pub holder: Bytes,
    pub resource: String,
    pub wait_duration_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_deadlock_detector_creation() {
        let detector = DeadlockDetector::new(5000, false);
        let stats = detector.get_stats();
        assert_eq!(stats.held_locks_count, 0);
        assert_eq!(stats.waiting_clients_count, 0);
    }
    
    #[test]
    fn test_lock_tracking() {
        let detector = DeadlockDetector::new(5000, false);
        
        let client1 = Bytes::from("client-1");
        let resource1 = "resource-1".to_string();
        
        detector.record_lock_acquired(resource1.clone(), client1.clone(), 10000);
        
        let held = detector.get_held_locks();
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].resource, resource1);
        assert_eq!(held[0].client_id, client1);
    }
    
    #[test]
    fn test_simple_deadlock_detection() {
        let detector = DeadlockDetector::new(5000, false);
        
        let client1 = Bytes::from("client-1");
        let client2 = Bytes::from("client-2");
        let resource1 = "resource-1".to_string();
        let resource2 = "resource-2".to_string();
        
        // Client 1 holds resource 1
        detector.record_lock_acquired(resource1.clone(), client1.clone(), 10000);
        
        // Client 2 holds resource 2
        detector.record_lock_acquired(resource2.clone(), client2.clone(), 10000);
        
        // Client 1 waits for resource 2 (held by client 2)
        detector.record_lock_wait(resource2.clone(), client1.clone(), 10000);
        
        // Client 2 waits for resource 1 (held by client 1) -> DEADLOCK!
        detector.record_lock_wait(resource1.clone(), client2.clone(), 10000);
        
        match detector.detect_deadlock() {
            DeadlockStatus::Deadlock { cycle, resources } => {
                assert!(cycle.len() >= 2);
                assert!(resources.len() >= 2);
                println!("Deadlock detected! Cycle: {:?}, Resources: {:?}", cycle, resources);
            }
            DeadlockStatus::NoDeadlock => {
                panic!("Expected deadlock to be detected");
            }
        }
    }
    
    #[test]
    fn test_no_deadlock() {
        let detector = DeadlockDetector::new(5000, false);
        
        let client1 = Bytes::from("client-1");
        let resource1 = "resource-1".to_string();
        
        // Client 1 holds resource 1
        detector.record_lock_acquired(resource1.clone(), client1.clone(), 10000);
        
        // No waiting clients
        match detector.detect_deadlock() {
            DeadlockStatus::NoDeadlock => {
                // Expected
            }
            DeadlockStatus::Deadlock { .. } => {
                panic!("No deadlock should be detected");
            }
        }
    }
    
    #[test]
    fn test_lock_release() {
        let detector = DeadlockDetector::new(5000, false);
        
        let client1 = Bytes::from("client-1");
        let resource1 = "resource-1".to_string();
        
        detector.record_lock_acquired(resource1.clone(), client1.clone(), 10000);
        assert_eq!(detector.get_held_locks().len(), 1);
        
        detector.record_lock_released(&resource1);
        assert_eq!(detector.get_held_locks().len(), 0);
    }

    /// Two-client cycle: client1 acquires first (older), client2 second (younger).
    fn setup_two_client_cycle(detector: &DeadlockDetector) -> (Bytes, Bytes) {
        let client1 = Bytes::from("client-1");
        let client2 = Bytes::from("client-2");
        let resource1 = "resource-1".to_string();
        let resource2 = "resource-2".to_string();

        detector.record_lock_acquired(resource1.clone(), client1.clone(), 10000);
        std::thread::sleep(Duration::from_millis(5));
        detector.record_lock_acquired(resource2.clone(), client2.clone(), 10000);

        // Create wait-for cycle (not required for resolve_deadlock, but realistic)
        detector.record_lock_wait(resource2.clone(), client1.clone(), 10000);
        detector.record_lock_wait(resource1.clone(), client2.clone(), 10000);

        (client1, client2)
    }

    #[test]
    fn test_victim_youngest_picks_most_recent_acquirer() {
        let detector = DeadlockDetector::new_with_strategy(
            5000,
            true,
            VictimSelectionStrategy::Youngest,
        );
        let (client1, client2) = setup_two_client_cycle(&detector);
        let cycle = vec![client1.clone(), client2.clone()];

        let victim = detector
            .resolve_deadlock(&cycle)
            .expect("auto_resolve should pick a victim");
        assert_eq!(
            victim, client2,
            "Youngest should pick the more recent acquirer (client2)"
        );
    }

    #[test]
    fn test_victim_oldest_picks_earliest_acquirer() {
        let detector = DeadlockDetector::new_with_strategy(
            5000,
            true,
            VictimSelectionStrategy::Oldest,
        );
        let (client1, client2) = setup_two_client_cycle(&detector);
        let cycle = vec![client1.clone(), client2.clone()];

        let victim = detector
            .resolve_deadlock(&cycle)
            .expect("auto_resolve should pick a victim");
        assert_eq!(
            victim, client1,
            "Oldest should pick the earliest acquirer (client1)"
        );
    }

    #[test]
    fn test_victim_fewest_locks_picks_client_with_fewer_held() {
        // client1 holds 2 locks; client2 holds 1 → FewestLocks picks client2
        let detector = DeadlockDetector::new_with_strategy(
            5000,
            true,
            VictimSelectionStrategy::FewestLocks,
        );
        let client1 = Bytes::from("client-1");
        let client2 = Bytes::from("client-2");

        detector.record_lock_acquired("resource-1".to_string(), client1.clone(), 10000);
        detector.record_lock_acquired("resource-extra".to_string(), client1.clone(), 10000);
        detector.record_lock_acquired("resource-2".to_string(), client2.clone(), 10000);

        let cycle = vec![client1.clone(), client2.clone()];
        let victim = detector
            .resolve_deadlock(&cycle)
            .expect("auto_resolve should pick a victim");
        assert_eq!(
            victim, client2,
            "FewestLocks should pick client2 (1 lock vs 2)"
        );
    }

    #[test]
    fn test_victim_auto_resolve_false_returns_none() {
        let detector = DeadlockDetector::new_with_strategy(
            5000,
            false,
            VictimSelectionStrategy::Youngest,
        );
        let (client1, client2) = setup_two_client_cycle(&detector);
        let cycle = vec![client1, client2];

        assert!(
            detector.resolve_deadlock(&cycle).is_none(),
            "auto_resolve=false must not select a victim"
        );
    }

    #[test]
    fn test_default_strategy_is_youngest() {
        let detector = DeadlockDetector::new(5000, true);
        assert_eq!(
            detector.victim_strategy(),
            VictimSelectionStrategy::Youngest
        );

        let detector = DeadlockDetector::new(5000, true)
            .with_victim_strategy(VictimSelectionStrategy::Oldest);
        assert_eq!(detector.victim_strategy(), VictimSelectionStrategy::Oldest);
    }

    #[test]
    fn test_release_client_locks_breaks_cycle() {
        let detector = DeadlockDetector::new(5000, true);
        let (client1, client2) = setup_two_client_cycle(&detector);

        assert!(matches!(
            detector.detect_deadlock(),
            DeadlockStatus::Deadlock { .. }
        ));

        // release_client_locks also clears victim waiting_for — no second call
        let released = detector.release_client_locks(&client2);
        assert!(
            released.contains(&"resource-2".to_string()),
            "should release client2's held resource"
        );
        assert!(matches!(
            detector.detect_deadlock(),
            DeadlockStatus::NoDeadlock
        ));
        // client1's lock remains
        assert!(detector
            .get_held_locks()
            .iter()
            .any(|l| l.client_id == client1));
        // victim waiting_for pruned
        assert!(!detector.get_waiting_clients().contains_key(&client2));
    }

    /// Atomic retain-by-client: after release, a re-acquire by another client
    /// must still be tracked (TOCTOU-safe — we never delete by resource name
    /// collected under a prior read lock).
    #[test]
    fn test_release_client_locks_toctou_safe_reacquire() {
        let detector = DeadlockDetector::new(5000, true);
        let client1 = Bytes::from("toctou-c1");
        let client2 = Bytes::from("toctou-c2");

        detector.record_lock_acquired("shared".to_string(), client1.clone(), 10000);
        let released = detector.release_client_locks(&client1);
        assert_eq!(released, vec!["shared".to_string()]);

        // Another client re-acquires the same resource name
        detector.record_lock_acquired("shared".to_string(), client2.clone(), 10000);

        let held = detector.get_held_locks();
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].client_id, client2);
        assert_eq!(held[0].resource, "shared");
    }

    /// Waiters on released resources are pruned; victim waits cleared.
    #[test]
    fn test_release_client_locks_prunes_waiting_for() {
        let detector = DeadlockDetector::new(5000, true);
        let client1 = Bytes::from("hold-c1");
        let client2 = Bytes::from("hold-c2");
        let waiter = Bytes::from("waiter");

        detector.record_lock_acquired("r1".to_string(), client1.clone(), 10000);
        detector.record_lock_acquired("r2".to_string(), client2.clone(), 10000);
        // waiter and client2 both wait for r1 (held by client1)
        detector.record_lock_wait("r1".to_string(), waiter.clone(), 10000);
        detector.record_lock_wait("r1".to_string(), client2.clone(), 10000);
        // client1 also waits for r2 (victim will lose this wait entry too)
        detector.record_lock_wait("r2".to_string(), client1.clone(), 10000);

        let released = detector.release_client_locks(&client1);
        assert!(released.contains(&"r1".to_string()));

        let waiting = detector.get_waiting_clients();
        // victim fully removed from waiting_for
        assert!(!waiting.contains_key(&client1));
        // other waiters no longer wait for released r1
        assert!(
            !waiting.contains_key(&waiter),
            "waiter only waited for r1 — entry should be gone"
        );
        if let Some(list) = waiting.get(&client2) {
            assert!(
                !list.iter().any(|i| i.resource == "r1"),
                "client2 must not still wait for released r1"
            );
        }
        // wait-graph edges for r1 gone
        assert_eq!(detector.get_stats().wait_graph_edges, 0);
        // client2's hold remains
        assert!(detector
            .get_held_locks()
            .iter()
            .any(|l| l.client_id == client2 && l.resource == "r2"));
    }

    /// Expired held locks drop matching wait-graph edges and waiting_for entries.
    #[test]
    fn test_cleanup_expired_drops_edges_and_waiting() {
        let detector = DeadlockDetector::new(5000, false);
        let client1 = Bytes::from("exp-c1");
        let client2 = Bytes::from("exp-c2");

        // Short TTL on the held resource that client2 waits for
        detector.record_lock_acquired("expiring".to_string(), client1.clone(), 40);
        detector.record_lock_acquired("stable".to_string(), client2.clone(), 10000);
        detector.record_lock_wait("expiring".to_string(), client2.clone(), 10000);

        assert_eq!(detector.get_stats().wait_graph_edges, 1);

        std::thread::sleep(Duration::from_millis(60));
        // detect_deadlock runs cleanup_expired_locks first
        let status = detector.detect_deadlock();
        assert!(
            matches!(status, DeadlockStatus::NoDeadlock),
            "expired hold should not leave a resolvable/unresolvable cycle"
        );

        assert!(
            !detector
                .get_held_locks()
                .iter()
                .any(|l| l.resource == "expiring"),
            "expired held lock removed"
        );
        assert_eq!(
            detector.get_stats().wait_graph_edges,
            0,
            "edges for expired resource must be dropped"
        );
        let waiting = detector.get_waiting_clients();
        assert!(
            !waiting.contains_key(&client2)
                || !waiting[&client2]
                    .iter()
                    .any(|i| i.resource == "expiring"),
            "waiting_for entry for expired resource must be pruned"
        );
        // stable hold remains
        assert!(detector
            .get_held_locks()
            .iter()
            .any(|l| l.resource == "stable"));
    }

    #[tokio::test]
    async fn test_detect_deadlock_async_finds_cycle() {
        let detector = DeadlockDetector::new(5000, false);
        let client1 = Bytes::from("async-c1");
        let client2 = Bytes::from("async-c2");

        detector.record_lock_acquired("a".to_string(), client1.clone(), 10000);
        detector.record_lock_acquired("b".to_string(), client2.clone(), 10000);
        detector.record_lock_wait("b".to_string(), client1.clone(), 10000);
        detector.record_lock_wait("a".to_string(), client2.clone(), 10000);

        match detector.detect_deadlock_async().await {
            DeadlockStatus::Deadlock { cycle, resources } => {
                assert!(cycle.len() >= 2);
                assert!(resources.len() >= 2);
            }
            DeadlockStatus::NoDeadlock => panic!("expected deadlock via async detect"),
        }

        let stats = detector.get_stats_async().await;
        assert_eq!(stats.held_locks_count, 2);
        assert_eq!(stats.waiting_clients_count, 2);
    }

    #[tokio::test]
    async fn test_resolve_deadlock_async_youngest() {
        let detector = DeadlockDetector::new_with_strategy(
            5000,
            true,
            VictimSelectionStrategy::Youngest,
        );
        let (client1, client2) = setup_two_client_cycle(&detector);
        let cycle = vec![client1, client2.clone()];

        let victim = detector
            .resolve_deadlock_async(&cycle)
            .await
            .expect("auto_resolve should pick a victim");
        assert_eq!(victim, client2);
    }

    #[tokio::test]
    async fn test_spawn_monitor_auto_resolves_deadlock() {
        let detector = Arc::new(DeadlockDetector::new_with_strategy(
            5000,
            true,
            VictimSelectionStrategy::Youngest,
        ));
        let (_client1, client2) = setup_two_client_cycle(&detector);

        assert!(matches!(
            detector.detect_deadlock(),
            DeadlockStatus::Deadlock { .. }
        ));

        let handle =
            DeadlockDetector::spawn_monitor(Arc::clone(&detector), Duration::from_millis(20));

        // Wait for at least one monitor tick + resolution
        tokio::time::sleep(Duration::from_millis(80)).await;

        assert!(
            matches!(detector.detect_deadlock(), DeadlockStatus::NoDeadlock),
            "background monitor with auto_resolve should break the cycle"
        );
        // Youngest victim is client2 — their held lock should be gone
        assert!(
            !detector
                .get_held_locks()
                .iter()
                .any(|l| l.client_id == client2),
            "youngest victim (client2) locks should be released"
        );

        handle.abort();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn test_spawn_monitor_no_auto_resolve_leaves_cycle() {
        let detector = Arc::new(DeadlockDetector::new(5000, false));
        let _ = setup_two_client_cycle(&detector);

        let handle =
            DeadlockDetector::spawn_monitor(Arc::clone(&detector), Duration::from_millis(20));
        tokio::time::sleep(Duration::from_millis(80)).await;

        assert!(
            matches!(detector.detect_deadlock(), DeadlockStatus::Deadlock { .. }),
            "without auto_resolve the monitor must not break the cycle"
        );

        handle.abort();
        let _ = handle.await;
    }

    // ── Cross-process snapshot merge (Batch DC) ────────────────────────────

    /// Plant a half-cycle on each detector:
    /// - det1: c1 holds A, c2 holds B (peer knowledge), c1 waits for B
    /// - det2: c2 holds B, c1 holds A (peer knowledge), c2 waits for A
    /// Neither has a full cycle alone; mutual merge reveals the deadlock.
    fn plant_cross_process_half_cycles(
        det1: &DeadlockDetector,
        det2: &DeadlockDetector,
    ) -> (Bytes, Bytes) {
        let c1 = Bytes::from("xp-c1");
        let c2 = Bytes::from("xp-c2");
        let a = "xp-resource-a".to_string();
        let b = "xp-resource-b".to_string();

        // Process 1 partial view: holds A, knows c2 holds B, waits for B
        det1.record_lock_acquired(a.clone(), c1.clone(), 10_000);
        det1.record_lock_acquired(b.clone(), c2.clone(), 10_000);
        det1.record_lock_wait(b.clone(), c1.clone(), 10_000);

        // Process 2 partial view: holds B, knows c1 holds A, waits for A
        det2.record_lock_acquired(b, c2.clone(), 10_000);
        det2.record_lock_acquired(a, c1.clone(), 10_000);
        det2.record_lock_wait("xp-resource-a".to_string(), c2.clone(), 10_000);

        (c1, c2)
    }

    #[test]
    fn test_cross_process_cycle_detected_after_snapshot_merge() {
        let det1 = DeadlockDetector::new(5000, false);
        let det2 = DeadlockDetector::new(5000, false);
        let (c1, c2) = plant_cross_process_half_cycles(&det1, &det2);

        // Neither process alone sees a cycle
        assert!(
            matches!(det1.detect_deadlock(), DeadlockStatus::NoDeadlock),
            "process 1 half-cycle alone must not detect deadlock"
        );
        assert!(
            matches!(det2.detect_deadlock(), DeadlockStatus::NoDeadlock),
            "process 2 half-cycle alone must not detect deadlock"
        );

        // Mutual export / merge
        let snap1 = det1.export_snapshot();
        let snap2 = det2.export_snapshot();
        assert_eq!(snap1.waits.len(), 1, "each half-cycle exports one wait edge");
        assert_eq!(snap2.waits.len(), 1);

        det1.merge_snapshot(&snap2);
        det2.merge_snapshot(&snap1);

        // Both should now detect a multi-client cycle
        match det1.detect_deadlock() {
            DeadlockStatus::Deadlock { cycle, resources } => {
                assert!(cycle.len() >= 2);
                assert!(
                    cycle.contains(&c1) && cycle.contains(&c2),
                    "cycle should involve both clients: {:?}",
                    cycle
                );
                assert!(
                    resources.iter().any(|r| r.contains("xp-resource")),
                    "resources: {:?}",
                    resources
                );
            }
            DeadlockStatus::NoDeadlock => panic!("det1 should detect cycle after merge"),
        }
        match det2.detect_deadlock() {
            DeadlockStatus::Deadlock { cycle, .. } => {
                assert!(cycle.contains(&c1) && cycle.contains(&c2));
            }
            DeadlockStatus::NoDeadlock => panic!("det2 should detect cycle after merge"),
        }
    }

    #[test]
    fn test_merge_local_held_not_overwritten_by_remote() {
        let local = DeadlockDetector::new(5000, false);
        let remote = DeadlockDetector::new(5000, false);

        let local_client = Bytes::from("local-owner");
        let remote_client = Bytes::from("remote-owner");
        let resource = "contested".to_string();

        local.record_lock_acquired(resource.clone(), local_client.clone(), 10_000);
        remote.record_lock_acquired(resource.clone(), remote_client.clone(), 10_000);

        let snap = remote.export_snapshot();
        assert_eq!(snap.held.len(), 1);
        assert_eq!(snap.held[0].client_id, "remote-owner");

        local.merge_snapshot(&snap);

        let held = local.get_held_locks();
        assert_eq!(held.len(), 1);
        assert_eq!(
            held[0].client_id, local_client,
            "local ownership must win over remote claim for the same resource"
        );
        assert_eq!(held[0].resource, resource);
    }

    #[test]
    fn test_merge_wait_edges_deduped_on_double_merge() {
        let local = DeadlockDetector::new(5000, false);
        let remote = DeadlockDetector::new(5000, false);

        let c1 = Bytes::from("dedupe-c1");
        let c2 = Bytes::from("dedupe-c2");

        remote.record_lock_acquired("r-a".to_string(), c1.clone(), 10_000);
        remote.record_lock_acquired("r-b".to_string(), c2.clone(), 10_000);
        remote.record_lock_wait("r-b".to_string(), c1.clone(), 10_000);

        let snap = remote.export_snapshot();
        assert_eq!(snap.waits.len(), 1);

        local.merge_snapshot(&snap);
        let edges_after_first = local.get_stats().wait_graph_edges;
        assert_eq!(edges_after_first, 1);

        // Second merge of the same snapshot must not duplicate edges
        local.merge_snapshot(&snap);
        assert_eq!(
            local.get_stats().wait_graph_edges,
            edges_after_first,
            "double merge must dedupe wait edges"
        );

        // Held from remote should still be present once
        assert_eq!(local.get_held_locks().len(), 2);
        local.merge_snapshot(&snap);
        assert_eq!(
            local.get_held_locks().len(),
            2,
            "double merge must not duplicate held locks"
        );
    }

    #[test]
    fn test_export_snapshot_roundtrip_fields() {
        let det = DeadlockDetector::new(5000, false);
        let c1 = Bytes::from("rt-c1");
        let c2 = Bytes::from("rt-c2");
        det.record_lock_acquired("rt-a".to_string(), c1.clone(), 8_000);
        det.record_lock_acquired("rt-b".to_string(), c2.clone(), 8_000);
        det.record_lock_wait("rt-b".to_string(), c1, 8_000);

        let mut snap = det.export_snapshot();
        snap.source_id = Some("proc-test".into());

        assert_eq!(snap.held.len(), 2);
        assert_eq!(snap.waits.len(), 1);
        assert_eq!(snap.waits[0].waiter, "rt-c1");
        assert_eq!(snap.waits[0].holder, "rt-c2");
        assert_eq!(snap.waits[0].resource, "rt-b");
        assert_eq!(snap.source_id.as_deref(), Some("proc-test"));

        // All held resources present
        let resources: HashSet<&str> = snap.held.iter().map(|h| h.resource.as_str()).collect();
        assert!(resources.contains("rt-a") && resources.contains("rt-b"));
    }
}
