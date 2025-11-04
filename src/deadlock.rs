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
}

impl DeadlockDetector {
    /// Create a new deadlock detector
    pub fn new(max_wait_time_ms: u64, auto_resolve: bool) -> Self {
        Self {
            held_locks: Arc::new(RwLock::new(HashMap::new())),
            waiting_for: Arc::new(RwLock::new(HashMap::new())),
            wait_graph: Arc::new(RwLock::new(Vec::new())),
            max_wait_time_ms,
            auto_resolve,
        }
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
    
    /// Resolve deadlock by selecting a victim
    pub fn resolve_deadlock(&self, cycle: &[Bytes]) -> Option<Bytes> {
        if cycle.is_empty() || !self.auto_resolve {
            return None;
        }
        
        // Select victim: youngest lock holder (most recent acquirer)
        // This is a simple heuristic; more sophisticated strategies can be implemented
        let held = self.held_locks.read();
        
        let victim = cycle
            .iter()
            .filter_map(|client| {
                // Find newest lock held by this client
                held.values()
                    .find(|info| info.client_id == *client)
                    .map(|info| (client.clone(), info.timestamp))
            })
            .max_by_key(|(_, timestamp)| *timestamp)
            .map(|(client, _)| client);
        
        victim
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
}
