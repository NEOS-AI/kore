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
    /// Total memory limit
    max_memory: usize,
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
            max_memory,
            max_message_size,
        }
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

        // Check total memory limit
        let total = self.total_memory();
        total + size <= self.max_memory
    }

    /// Allocate memory for a category
    pub fn allocate(&self, size: usize, category: MemoryCategory) -> bool {
        if !self.can_allocate(size, category) {
            return false;
        }

        match category {
            MemoryCategory::Cache => {
                self.cache_memory.fetch_add(size, Ordering::Relaxed);
            }
            MemoryCategory::PubSub => {
                self.pubsub_memory.fetch_add(size, Ordering::Relaxed);
            }
            MemoryCategory::SortedSets => {
                self.sorted_sets_memory.fetch_add(size, Ordering::Relaxed);
            }
            MemoryCategory::GeoSets => {
                self.geo_sets_memory.fetch_add(size, Ordering::Relaxed);
            }
        }

        true
    }

    /// Deallocate memory for a category
    pub fn deallocate(&self, size: usize, category: MemoryCategory) {
        match category {
            MemoryCategory::Cache => {
                self.cache_memory.fetch_sub(size, Ordering::Relaxed);
            }
            MemoryCategory::PubSub => {
                self.pubsub_memory.fetch_sub(size, Ordering::Relaxed);
            }
            MemoryCategory::SortedSets => {
                self.sorted_sets_memory.fetch_sub(size, Ordering::Relaxed);
            }
            MemoryCategory::GeoSets => {
                self.geo_sets_memory.fetch_sub(size, Ordering::Relaxed);
            }
        }
    }

    /// Get total memory usage
    pub fn total_memory(&self) -> usize {
        self.cache_memory.load(Ordering::Relaxed)
            + self.pubsub_memory.load(Ordering::Relaxed)
            + self.sorted_sets_memory.load(Ordering::Relaxed)
            + self.geo_sets_memory.load(Ordering::Relaxed)
    }

    /// Get memory usage for a specific category
    pub fn category_memory(&self, category: MemoryCategory) -> usize {
        match category {
            MemoryCategory::Cache => self.cache_memory.load(Ordering::Relaxed),
            MemoryCategory::PubSub => self.pubsub_memory.load(Ordering::Relaxed),
            MemoryCategory::SortedSets => self.sorted_sets_memory.load(Ordering::Relaxed),
            MemoryCategory::GeoSets => self.geo_sets_memory.load(Ordering::Relaxed),
        }
    }

    /// Get memory utilization percentage
    pub fn utilization(&self) -> f64 {
        let total = self.total_memory();
        if self.max_memory == 0 {
            0.0
        } else {
            (total as f64 / self.max_memory as f64) * 100.0
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
}
