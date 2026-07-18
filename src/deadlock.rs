use bytes::Bytes;
use parking_lot::RwLock;
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
    
    /// Clean up expired locks and long waits
    fn cleanup_expired_locks(&self) {
        // Remove expired held locks
        self.held_locks.write().retain(|_, info| !info.is_expired());
        
        // Remove long waits that exceed max wait time
        let max_wait = Duration::from_millis(self.max_wait_time_ms);
        self.wait_graph.write().retain(|edge| {
            edge.timestamp.elapsed() < max_wait
        });
        
        // Clean up waiting list
        let mut waiting = self.waiting_for.write();
        waiting.retain(|_, wait_list| {
            wait_list.retain(|info| !info.is_expired());
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
}
