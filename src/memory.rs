use std::sync::atomic::{AtomicUsize, Ordering};

/// Memory tracker for different cache components
pub struct MemoryTracker {
    /// Memory used by key-value cache
    cache_memory: AtomicUsize,
    /// Memory used by Pub/Sub (messages, subscriptions)
    pubsub_memory: AtomicUsize,
    /// Memory used by sorted sets
    sorted_sets_memory: AtomicUsize,
    /// Memory used by geospatial sets
    geo_sets_memory: AtomicUsize,
    /// Memory used by Redis hashes
    hashes_memory: AtomicUsize,
    /// Memory used by Redis lists
    lists_memory: AtomicUsize,
    /// Memory used by Redis sets
    sets_memory: AtomicUsize,
    /// Memory used by Redis streams
    streams_memory: AtomicUsize,
    /// Memory used by search indexes (document + inverted index approx)
    search_memory: AtomicUsize,
    /// Total memory limit (live-updatable via CONFIG SET maxmemory)
    max_memory: AtomicUsize,
    /// Maximum message size for Pub/Sub
    max_message_size: usize,
}

impl MemoryTracker {
    pub fn new(max_memory: usize, max_message_size: usize) -> Self {
        Self {
            cache_memory: AtomicUsize::new(0),
            pubsub_memory: AtomicUsize::new(0),
            sorted_sets_memory: AtomicUsize::new(0),
            geo_sets_memory: AtomicUsize::new(0),
            hashes_memory: AtomicUsize::new(0),
            lists_memory: AtomicUsize::new(0),
            sets_memory: AtomicUsize::new(0),
            streams_memory: AtomicUsize::new(0),
            search_memory: AtomicUsize::new(0),
            max_memory: AtomicUsize::new(max_memory),
            max_message_size,
        }
    }

    fn category_atomic(&self, category: MemoryCategory) -> &AtomicUsize {
        match category {
            MemoryCategory::Cache => &self.cache_memory,
            MemoryCategory::PubSub => &self.pubsub_memory,
            MemoryCategory::SortedSets => &self.sorted_sets_memory,
            MemoryCategory::GeoSets => &self.geo_sets_memory,
            MemoryCategory::Hashes => &self.hashes_memory,
            MemoryCategory::Lists => &self.lists_memory,
            MemoryCategory::Sets => &self.sets_memory,
            MemoryCategory::Streams => &self.streams_memory,
            MemoryCategory::Search => &self.search_memory,
        }
    }

    /// Update the total memory limit at runtime.
    pub fn set_max_memory(&self, size: usize) {
        self.max_memory.store(size, Ordering::Relaxed);
    }

    /// Current total memory limit.
    pub fn max_memory(&self) -> usize {
        self.max_memory.load(Ordering::Relaxed)
    }

    /// Check if we can allocate memory for a specific category
    pub fn can_allocate(&self, size: usize, category: MemoryCategory) -> bool {
        // Check category-specific limits
        match category {
            MemoryCategory::PubSub => {
                if size > self.max_message_size {
                    return false;
                }
            }
            _ => {}
        }

        // Check total memory limit (0 = unlimited)
        let limit = self.max_memory.load(Ordering::Relaxed);
        if limit == 0 {
            return true;
        }
        let total = self.total_memory();
        total + size <= limit
    }

    /// Unconditionally record allocated memory (no capacity check).
    /// Use after a successful insert when accounting must not fail.
    pub fn account(&self, size: usize, category: MemoryCategory) {
        self.category_atomic(category)
            .fetch_add(size, Ordering::Relaxed);
    }

    /// Allocate memory for a category
    pub fn allocate(&self, size: usize, category: MemoryCategory) -> bool {
        if !self.can_allocate(size, category) {
            return false;
        }

        self.account(size, category);
        true
    }

    /// Deallocate memory for a category (saturates at zero — never underflows)
    pub fn deallocate(&self, size: usize, category: MemoryCategory) {
        let atomic = self.category_atomic(category);
        let _ = atomic.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_sub(size))
        });
    }

    /// Reset all category counters to zero
    pub fn reset(&self) {
        self.cache_memory.store(0, Ordering::Relaxed);
        self.pubsub_memory.store(0, Ordering::Relaxed);
        self.sorted_sets_memory.store(0, Ordering::Relaxed);
        self.geo_sets_memory.store(0, Ordering::Relaxed);
        self.hashes_memory.store(0, Ordering::Relaxed);
        self.lists_memory.store(0, Ordering::Relaxed);
        self.sets_memory.store(0, Ordering::Relaxed);
        self.streams_memory.store(0, Ordering::Relaxed);
        self.search_memory.store(0, Ordering::Relaxed);
    }

    /// Reset a single category counter to zero
    pub fn reset_category(&self, category: MemoryCategory) {
        self.category_atomic(category).store(0, Ordering::Relaxed);
    }

    /// Get total memory usage
    pub fn total_memory(&self) -> usize {
        self.cache_memory.load(Ordering::Relaxed)
            + self.pubsub_memory.load(Ordering::Relaxed)
            + self.sorted_sets_memory.load(Ordering::Relaxed)
            + self.geo_sets_memory.load(Ordering::Relaxed)
            + self.hashes_memory.load(Ordering::Relaxed)
            + self.lists_memory.load(Ordering::Relaxed)
            + self.sets_memory.load(Ordering::Relaxed)
            + self.streams_memory.load(Ordering::Relaxed)
            + self.search_memory.load(Ordering::Relaxed)
    }

    /// Get memory usage for a specific category
    pub fn category_memory(&self, category: MemoryCategory) -> usize {
        self.category_atomic(category).load(Ordering::Relaxed)
    }

    /// Get memory utilization percentage
    pub fn utilization(&self) -> f64 {
        let total = self.total_memory();
        let limit = self.max_memory.load(Ordering::Relaxed);
        if limit == 0 {
            0.0
        } else {
            (total as f64 / limit as f64) * 100.0
        }
    }

    /// Get max message size for Pub/Sub
    pub fn max_message_size(&self) -> usize {
        self.max_message_size
    }

    /// Set max message size
    pub fn set_max_message_size(&mut self, size: usize) {
        self.max_message_size = size;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryCategory {
    Cache,
    PubSub,
    SortedSets,
    GeoSets,
    Hashes,
    Lists,
    Sets,
    Streams,
    /// Full-text / vector search index documents
    Search,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_tracker() {
        let tracker = MemoryTracker::new(1024 * 1024, 1024); // 1MB total, 1KB max message

        // Allocate cache memory
        assert!(tracker.allocate(512 * 1024, MemoryCategory::Cache));
        assert_eq!(tracker.category_memory(MemoryCategory::Cache), 512 * 1024);

        // Allocate Pub/Sub memory (within message size limit)
        assert!(tracker.allocate(1024, MemoryCategory::PubSub));
        assert_eq!(tracker.category_memory(MemoryCategory::PubSub), 1024);

        // Try to allocate too large message (exceeds max_message_size)
        assert!(!tracker.can_allocate(2048, MemoryCategory::PubSub));

        // Try to exceed total memory (512KB cache + 1KB pubsub + 512KB = exceeds 1MB)
        assert!(!tracker.allocate(512 * 1024, MemoryCategory::SortedSets));

        // Deallocate PubSub memory
        tracker.deallocate(1024, MemoryCategory::PubSub);
        assert_eq!(tracker.category_memory(MemoryCategory::PubSub), 0);

        // Now should be able to allocate (512KB cache + 256KB sorted sets = 768KB < 1MB)
        assert!(tracker.allocate(256 * 1024, MemoryCategory::SortedSets));
        assert_eq!(tracker.total_memory(), 512 * 1024 + 256 * 1024);
    }

    #[test]
    fn test_memory_utilization() {
        let tracker = MemoryTracker::new(1024 * 1024, 512 * 1024); // Increase max message size

        assert_eq!(tracker.utilization(), 0.0);

        assert!(tracker.allocate(512 * 1024, MemoryCategory::Cache));
        assert!((tracker.utilization() - 50.0).abs() < 0.1);

        assert!(tracker.allocate(256 * 1024, MemoryCategory::SortedSets));
        assert!((tracker.utilization() - 75.0).abs() < 0.1);

        tracker.deallocate(256 * 1024, MemoryCategory::SortedSets);
        assert!((tracker.utilization() - 50.0).abs() < 0.1);
    }

    #[test]
    fn test_deallocate_saturates() {
        let tracker = MemoryTracker::new(1024, 1024);
        tracker.account(10, MemoryCategory::Cache);
        tracker.deallocate(100, MemoryCategory::Cache);
        assert_eq!(tracker.category_memory(MemoryCategory::Cache), 0);
    }

    #[test]
    fn test_reset() {
        let tracker = MemoryTracker::new(1024 * 1024, 1024);
        assert!(tracker.allocate(100, MemoryCategory::Cache));
        assert!(tracker.allocate(50, MemoryCategory::PubSub));
        tracker.reset_category(MemoryCategory::Cache);
        assert_eq!(tracker.category_memory(MemoryCategory::Cache), 0);
        assert_eq!(tracker.category_memory(MemoryCategory::PubSub), 50);
        tracker.reset();
        assert_eq!(tracker.total_memory(), 0);
    }
}
