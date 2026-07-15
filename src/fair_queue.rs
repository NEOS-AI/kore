use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::thread;

/// Represents a client waiting in the queue for a lock
#[derive(Debug, Clone)]
pub struct QueuedClient {
    /// Unique identifier for the client
    pub client_id: Bytes,

    /// Resource name the client is waiting for
    pub resource: String,

    /// Timestamp when the client joined the queue (milliseconds)
    pub queued_at: u64,

    /// TTL for the lock request (milliseconds)
    pub ttl: u64,

    /// Priority (lower is higher priority, 0 = highest)
    pub priority: u32,
}

impl QueuedClient {
    /// Create a new queued client
    pub fn new(client_id: Bytes, resource: String, ttl: u64, priority: u32) -> Self {
        let queued_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Self {
            client_id,
            resource,
            queued_at,
            ttl,
            priority,
        }
    }

    /// Check if this queue entry has expired
    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        now > self.queued_at + self.ttl
    }

    /// Get waiting time in milliseconds
    pub fn wait_time(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        now.saturating_sub(self.queued_at)
    }
}

/// Fair lock queue implementation using FIFO ordering
///
/// This ensures that clients acquire locks in the order they requested them,
/// preventing starvation and ensuring fairness.
pub struct FairQueue {
    /// Queue for each resource (FIFO order)
    queues: Arc<RwLock<HashMap<String, VecDeque<QueuedClient>>>>,

    /// Maximum queue size per resource (to prevent memory exhaustion)
    max_queue_size: usize,

    /// Statistics
    stats: Arc<RwLock<FairQueueStats>>,

    /// Background cleanup enabled
    cleanup_enabled: Arc<AtomicBool>,
}

/// Statistics for fair queueing
#[derive(Debug, Clone, Default)]
pub struct FairQueueStats {
    /// Total number of queued clients across all resources
    pub total_queued: usize,

    /// Number of resources with active queues
    pub active_queues: usize,

    /// Total number of clients that have been enqueued
    pub total_enqueued: u64,

    /// Total number of clients that have been dequeued (successful claim complete)
    pub total_dequeued: u64,

    /// Total number of expired queue entries removed
    pub total_expired: u64,

    /// Times a non-front client was denied the turn
    pub total_claim_denied: u64,

    /// Times a client was removed due to acquisition timeout/failure
    pub total_removed: u64,

    /// Maximum wait time observed (milliseconds)
    pub max_wait_time: u64,

    /// Average wait time (milliseconds)
    pub avg_wait_time: u64,
}

impl FairQueue {
    /// Create a new fair queue
    pub fn new(max_queue_size: usize) -> Self {
        Self {
            queues: Arc::new(RwLock::new(HashMap::new())),
            max_queue_size,
            stats: Arc::new(RwLock::new(FairQueueStats::default())),
            cleanup_enabled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Create a fair queue with background cleanup
    pub fn new_with_cleanup(max_queue_size: usize, cleanup_interval_ms: u64) -> Self {
        let queue = Self {
            queues: Arc::new(RwLock::new(HashMap::new())),
            max_queue_size,
            stats: Arc::new(RwLock::new(FairQueueStats::default())),
            cleanup_enabled: Arc::new(AtomicBool::new(true)),
        };

        let queues_clone = Arc::clone(&queue.queues);
        let stats_clone = Arc::clone(&queue.stats);
        let enabled_clone = Arc::clone(&queue.cleanup_enabled);

        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_millis(cleanup_interval_ms.max(10)));

                if !enabled_clone.load(Ordering::Relaxed) {
                    break;
                }

                let resources: Vec<String> = {
                    let queues = queues_clone.read();
                    queues.keys().cloned().collect()
                };

                let mut total_removed = 0usize;
                for resource in resources {
                    let mut queues = queues_clone.write();

                    if let Some(q) = queues.get_mut(&resource) {
                        let before = q.len();
                        q.retain(|c| !c.is_expired());
                        let removed = before - q.len();
                        total_removed += removed;

                        if q.is_empty() {
                            queues.remove(&resource);
                        }
                    }
                }

                if total_removed > 0 {
                    let queues = queues_clone.read();
                    let mut stats = stats_clone.write();
                    stats.total_expired += total_removed as u64;
                    stats.total_queued = queues.values().map(|q| q.len()).sum();
                    stats.active_queues = queues.len();
                }
            }
        });

        queue
    }

    /// Stop background cleanup (called on drop)
    pub fn stop_cleanup(&self) {
        self.cleanup_enabled.store(false, Ordering::Relaxed);
    }

    /// Enqueue a client for a resource
    ///
    /// Returns true if successfully enqueued, false if queue is full
    pub fn enqueue(&self, client: QueuedClient) -> bool {
        let mut queues = self.queues.write();
        let queue = queues
            .entry(client.resource.clone())
            .or_insert_with(VecDeque::new);

        if queue.len() >= self.max_queue_size {
            return false;
        }

        // Insert in priority order (then FIFO within same priority)
        let insert_pos = queue
            .iter()
            .position(|c| c.priority > client.priority)
            .unwrap_or(queue.len());

        queue.insert(insert_pos, client);

        let mut stats = self.stats.write();
        stats.total_queued = queues.values().map(|q| q.len()).sum();
        stats.active_queues = queues.len();
        stats.total_enqueued += 1;

        true
    }

    /// Check if a client is at the front of the queue for a resource
    pub fn is_next(&self, resource: &str, client_id: &Bytes) -> bool {
        self.cleanup_expired(resource);

        let queues = self.queues.read();

        if let Some(queue) = queues.get(resource) {
            if let Some(front) = queue.front() {
                return &front.client_id == client_id;
            }
        }

        // If no queue exists or queue is empty, allow the lock
        true
    }

    /// Atomically check whether `client_id` is next and may attempt acquisition.
    ///
    /// Holds the write lock across cleanup + front inspection so two waiters
    /// cannot both observe themselves as front under concurrent cleanup.
    pub fn try_acquire(&self, resource: &str, client_id: &Bytes) -> bool {
        let mut queues = self.queues.write();

        // Inline expired cleanup under the same write lock
        let mut expired = 0usize;
        if let Some(queue) = queues.get_mut(resource) {
            let before = queue.len();
            queue.retain(|c| !c.is_expired());
            expired = before - queue.len();
            if queue.is_empty() {
                queues.remove(resource);
            }
        }

        if expired > 0 {
            let mut stats = self.stats.write();
            stats.total_expired += expired as u64;
            stats.total_queued = queues.values().map(|q| q.len()).sum();
            stats.active_queues = queues.len();
        }

        if let Some(queue) = queues.get(resource) {
            if let Some(front) = queue.front() {
                if &front.client_id == client_id {
                    return true;
                }
                let mut stats = self.stats.write();
                stats.total_claim_denied += 1;
                return false;
            }
        }

        // Empty / missing queue: allow (caller may be sole contender)
        true
    }

    /// Dequeue the front client only if it matches `client_id`.
    ///
    /// Prevents a successful acquirer from popping a different waiter if the
    /// queue front changed (expiry / remove) between claim and complete.
    pub fn dequeue_client(&self, resource: &str, client_id: &Bytes) -> Option<QueuedClient> {
        let mut queues = self.queues.write();

        let client = if let Some(queue) = queues.get_mut(resource) {
            let matches_front = queue
                .front()
                .map(|c| &c.client_id == client_id)
                .unwrap_or(false);
            let client = if matches_front {
                queue.pop_front()
            } else {
                // Front is someone else — remove this client wherever they sit
                if let Some(pos) = queue.iter().position(|c| &c.client_id == client_id) {
                    queue.remove(pos)
                } else {
                    None
                }
            };

            if queue.is_empty() {
                queues.remove(resource);
            }
            client
        } else {
            None
        };

        if let Some(ref c) = client {
            let mut stats = self.stats.write();
            stats.total_queued = queues.values().map(|q| q.len()).sum();
            stats.active_queues = queues.len();
            stats.total_dequeued += 1;

            let wait_time = c.wait_time();
            if wait_time > stats.max_wait_time {
                stats.max_wait_time = wait_time;
            }

            let total_dequeued = stats.total_dequeued;
            stats.avg_wait_time =
                (stats.avg_wait_time * (total_dequeued - 1) + wait_time) / total_dequeued;
        }

        client
    }

    /// Dequeue the next client for a resource (front only).
    pub fn dequeue(&self, resource: &str) -> Option<QueuedClient> {
        let mut queues = self.queues.write();

        let client = if let Some(queue) = queues.get_mut(resource) {
            let client = queue.pop_front();
            if queue.is_empty() {
                queues.remove(resource);
            }
            client
        } else {
            None
        };

        if let Some(ref c) = client {
            let mut stats = self.stats.write();
            stats.total_queued = queues.values().map(|q| q.len()).sum();
            stats.active_queues = queues.len();
            stats.total_dequeued += 1;

            let wait_time = c.wait_time();
            if wait_time > stats.max_wait_time {
                stats.max_wait_time = wait_time;
            }

            let total_dequeued = stats.total_dequeued;
            stats.avg_wait_time =
                (stats.avg_wait_time * (total_dequeued - 1) + wait_time) / total_dequeued;
        }

        client
    }

    /// Remove a specific client from the queue
    pub fn remove(&self, resource: &str, client_id: &Bytes) -> bool {
        let mut queues = self.queues.write();

        if let Some(queue) = queues.get_mut(resource) {
            if let Some(pos) = queue.iter().position(|c| &c.client_id == client_id) {
                queue.remove(pos);

                if queue.is_empty() {
                    queues.remove(resource);
                }

                let mut stats = self.stats.write();
                stats.total_queued = queues.values().map(|q| q.len()).sum();
                stats.active_queues = queues.len();
                stats.total_removed += 1;

                return true;
            }
        }

        false
    }

    /// Get the queue length for a resource
    pub fn queue_length(&self, resource: &str) -> usize {
        let queues = self.queues.read();
        queues.get(resource).map(|q| q.len()).unwrap_or(0)
    }

    /// Get the position of a client in the queue
    pub fn position(&self, resource: &str, client_id: &Bytes) -> Option<usize> {
        let queues = self.queues.read();

        if let Some(queue) = queues.get(resource) {
            queue.iter().position(|c| &c.client_id == client_id)
        } else {
            None
        }
    }

    /// Clean up expired entries from a specific queue
    pub fn cleanup_expired(&self, resource: &str) -> usize {
        let mut queues = self.queues.write();
        let mut removed_count = 0;

        if let Some(queue) = queues.get_mut(resource) {
            queue.retain(|c| {
                if c.is_expired() {
                    removed_count += 1;
                    false
                } else {
                    true
                }
            });

            if queue.is_empty() {
                queues.remove(resource);
            }
        }

        if removed_count > 0 {
            let mut stats = self.stats.write();
            stats.total_expired += removed_count as u64;
            stats.total_queued = queues.values().map(|q| q.len()).sum();
            stats.active_queues = queues.len();
        }

        removed_count
    }

    /// Clean up all expired entries across all queues
    pub fn cleanup_all_expired(&self) -> usize {
        let resources: Vec<String> = {
            let queues = self.queues.read();
            queues.keys().cloned().collect()
        };

        resources.iter().map(|r| self.cleanup_expired(r)).sum()
    }

    /// Get statistics
    pub fn get_stats(&self) -> FairQueueStats {
        self.stats.read().clone()
    }

    /// INFO-style key:value body for fair queue metrics.
    pub fn to_info_lines(&self) -> String {
        let s = self.get_stats();
        format!(
            "fair_queue_enabled:1\r\n\
             fair_queue_total_queued:{}\r\n\
             fair_queue_active_queues:{}\r\n\
             fair_queue_total_enqueued:{}\r\n\
             fair_queue_total_dequeued:{}\r\n\
             fair_queue_total_expired:{}\r\n\
             fair_queue_total_claim_denied:{}\r\n\
             fair_queue_total_removed:{}\r\n\
             fair_queue_max_wait_time_ms:{}\r\n\
             fair_queue_avg_wait_time_ms:{}\r\n",
            s.total_queued,
            s.active_queues,
            s.total_enqueued,
            s.total_dequeued,
            s.total_expired,
            s.total_claim_denied,
            s.total_removed,
            s.max_wait_time,
            s.avg_wait_time,
        )
    }

    /// Clear all queues
    pub fn clear(&self) {
        let mut queues = self.queues.write();
        queues.clear();

        let mut stats = self.stats.write();
        stats.total_queued = 0;
        stats.active_queues = 0;
    }

    /// Get all queued clients for a resource (for debugging/monitoring)
    pub fn get_queue(&self, resource: &str) -> Vec<QueuedClient> {
        let queues = self.queues.read();
        queues
            .get(resource)
            .map(|q| q.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Maximum queue size configured for this instance.
    pub fn max_queue_size(&self) -> usize {
        self.max_queue_size
    }
}

impl Drop for FairQueue {
    fn drop(&mut self) {
        self.stop_cleanup();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_fair_queue_basic() {
        let queue = FairQueue::new(100);

        let client1 = QueuedClient::new(Bytes::from("client-1"), "resource-a".to_string(), 10000, 0);
        let client2 = QueuedClient::new(Bytes::from("client-2"), "resource-a".to_string(), 10000, 0);

        assert!(queue.enqueue(client1.clone()));
        assert!(queue.enqueue(client2.clone()));

        assert_eq!(queue.queue_length("resource-a"), 2);
        assert!(queue.is_next("resource-a", &client1.client_id));
        assert!(!queue.is_next("resource-a", &client2.client_id));
    }

    #[test]
    fn test_fair_queue_dequeue() {
        let queue = FairQueue::new(100);

        let client1 = QueuedClient::new(Bytes::from("client-1"), "resource-a".to_string(), 10000, 0);
        let client2 = QueuedClient::new(Bytes::from("client-2"), "resource-a".to_string(), 10000, 0);

        queue.enqueue(client1.clone());
        queue.enqueue(client2.clone());

        let dequeued = queue.dequeue("resource-a").unwrap();
        assert_eq!(dequeued.client_id, client1.client_id);

        assert_eq!(queue.queue_length("resource-a"), 1);
        assert!(queue.is_next("resource-a", &client2.client_id));
    }

    #[test]
    fn test_fair_queue_dequeue_client_matches_front() {
        let queue = FairQueue::new(100);
        let c1 = QueuedClient::new(Bytes::from("c1"), "r".to_string(), 10000, 0);
        let c2 = QueuedClient::new(Bytes::from("c2"), "r".to_string(), 10000, 0);
        queue.enqueue(c1.clone());
        queue.enqueue(c2.clone());

        // c2 must not pop c1
        assert!(queue.dequeue_client("r", &c2.client_id).is_some());
        assert_eq!(queue.queue_length("r"), 1);
        assert!(queue.is_next("r", &c1.client_id));
    }

    #[test]
    fn test_try_acquire_atomic_front() {
        let queue = FairQueue::new(100);
        let c1 = QueuedClient::new(Bytes::from("c1"), "r".to_string(), 10000, 0);
        let c2 = QueuedClient::new(Bytes::from("c2"), "r".to_string(), 10000, 0);
        queue.enqueue(c1.clone());
        queue.enqueue(c2.clone());

        assert!(queue.try_acquire("r", &c1.client_id));
        assert!(!queue.try_acquire("r", &c2.client_id));
        let stats = queue.get_stats();
        assert!(stats.total_claim_denied >= 1);
    }

    #[test]
    fn test_fair_queue_priority() {
        let queue = FairQueue::new(100);

        let client1 = QueuedClient::new(Bytes::from("client-1"), "resource-a".to_string(), 10000, 10);
        let client2 = QueuedClient::new(Bytes::from("client-2"), "resource-a".to_string(), 10000, 0);
        let client3 = QueuedClient::new(Bytes::from("client-3"), "resource-a".to_string(), 10000, 5);

        queue.enqueue(client1.clone());
        queue.enqueue(client2.clone());
        queue.enqueue(client3.clone());

        assert!(queue.is_next("resource-a", &client2.client_id));

        queue.dequeue("resource-a");
        assert!(queue.is_next("resource-a", &client3.client_id));

        queue.dequeue("resource-a");
        assert!(queue.is_next("resource-a", &client1.client_id));
    }

    #[test]
    fn test_fair_queue_remove() {
        let queue = FairQueue::new(100);

        let client1 = QueuedClient::new(Bytes::from("client-1"), "resource-a".to_string(), 10000, 0);
        let client2 = QueuedClient::new(Bytes::from("client-2"), "resource-a".to_string(), 10000, 0);

        queue.enqueue(client1.clone());
        queue.enqueue(client2.clone());

        assert!(queue.remove("resource-a", &client1.client_id));
        assert_eq!(queue.queue_length("resource-a"), 1);
        assert!(queue.is_next("resource-a", &client2.client_id));
    }

    #[test]
    fn test_fair_queue_expiration() {
        let queue = FairQueue::new(100);

        let client1 = QueuedClient::new(Bytes::from("client-1"), "resource-a".to_string(), 50, 0);
        let client2 = QueuedClient::new(Bytes::from("client-2"), "resource-a".to_string(), 10000, 0);

        queue.enqueue(client1);
        queue.enqueue(client2.clone());

        thread::sleep(Duration::from_millis(60));

        let removed = queue.cleanup_expired("resource-a");
        assert_eq!(removed, 1);
        assert_eq!(queue.queue_length("resource-a"), 1);
        assert!(queue.is_next("resource-a", &client2.client_id));
    }

    #[test]
    fn test_fair_queue_max_size() {
        let queue = FairQueue::new(2);

        let client1 = QueuedClient::new(Bytes::from("client-1"), "resource-a".to_string(), 10000, 0);
        let client2 = QueuedClient::new(Bytes::from("client-2"), "resource-a".to_string(), 10000, 0);
        let client3 = QueuedClient::new(Bytes::from("client-3"), "resource-a".to_string(), 10000, 0);

        assert!(queue.enqueue(client1));
        assert!(queue.enqueue(client2));
        assert!(!queue.enqueue(client3));
    }

    #[test]
    fn test_fair_queue_statistics() {
        let queue = FairQueue::new(100);

        let client1 = QueuedClient::new(Bytes::from("client-1"), "resource-a".to_string(), 10000, 0);
        let client2 = QueuedClient::new(Bytes::from("client-2"), "resource-a".to_string(), 10000, 0);

        queue.enqueue(client1);
        queue.enqueue(client2);

        let stats = queue.get_stats();
        assert_eq!(stats.total_queued, 2);
        assert_eq!(stats.active_queues, 1);
        assert_eq!(stats.total_enqueued, 2);

        thread::sleep(Duration::from_millis(10));
        queue.dequeue("resource-a");

        let stats = queue.get_stats();
        assert_eq!(stats.total_queued, 1);
        assert_eq!(stats.total_dequeued, 1);
        assert!(stats.avg_wait_time >= 10);
    }

    #[test]
    fn test_cleanup_thread_stops_on_drop() {
        let q = FairQueue::new_with_cleanup(10, 20);
        drop(q);
        // If Drop did not stop the thread, this would not hang; just ensure no panic.
        thread::sleep(Duration::from_millis(50));
    }
}
