//! Memory tracking and size estimation.
//!
//! Kore does not use jemalloc's `zmalloc`-style RSS accounting. Instead we
//! estimate heap cost from structural sizes plus:
//! - per-`Bytes` / string heap headers
//! - hash-map / dict entry overhead
//! - a fixed allocator waste factor (~12.5%, 8-byte aligned)
//!
//! Call sites should prefer the `estimate_*` helpers so capacity checks and
//! `MemoryTracker` counters stay consistent.

use std::sync::atomic::{AtomicUsize, Ordering};

// ── Size estimation (Batch AA) ──────────────────────────────────────────────

/// Approximate heap header / Arc-like overhead for a `Bytes` (or similar) blob.
pub const BYTES_OVERHEAD: usize = 24;

/// Approximate cost of one hash-map / dict entry (pointer, hash, metadata).
pub const DICT_ENTRY_OVERHEAD: usize = 32;

/// Allocator waste: charge ~12.5% extra (×9/8), then align up to 8 bytes.
/// Models jemalloc size-class rounding without depending on the system allocator.
pub fn with_alloc_overhead(raw: usize) -> usize {
    if raw == 0 {
        return 0;
    }
    // raw * 9/8, rounded up, then 8-byte align
    let taxed = raw.saturating_add((raw + 7) / 8);
    align_up_8(taxed)
}

/// Round up to a multiple of 8.
#[inline]
pub fn align_up_8(n: usize) -> usize {
    (n.saturating_add(7)) & !7
}

/// Accounted size of a string KV entry (key + value + `Entry` struct + map slot).
///
/// `entry_struct_size` is `size_of::<Entry>()` passed from the caller to avoid
/// a circular dependency between `entry` and `memory`.
pub fn estimate_string_entry(
    key_len: usize,
    value_len: usize,
    entry_struct_size: usize,
) -> usize {
    let raw = key_len
        + value_len
        + entry_struct_size
        + BYTES_OVERHEAD * 2
        + DICT_ENTRY_OVERHEAD;
    with_alloc_overhead(raw)
}

/// Logical (pre-overhead) string entry size used for `maxentrysize` checks.
pub fn logical_string_entry(
    key_len: usize,
    value_len: usize,
    entry_struct_size: usize,
) -> usize {
    key_len + value_len + entry_struct_size
}

/// Cost of storing `key` as a map key plus an empty typed value container.
pub fn estimate_typed_key_base(key_len: usize, empty_struct_size: usize) -> usize {
    let raw = key_len + BYTES_OVERHEAD + DICT_ENTRY_OVERHEAD + empty_struct_size;
    with_alloc_overhead(raw)
}

/// One hash field → value pair (two blobs + dict entry).
pub fn estimate_hash_field(field_len: usize, value_len: usize) -> usize {
    let raw = field_len + value_len + BYTES_OVERHEAD * 2 + DICT_ENTRY_OVERHEAD;
    with_alloc_overhead(raw)
}

/// One list element.
pub fn estimate_list_element(elem_len: usize) -> usize {
    let raw = elem_len + BYTES_OVERHEAD + 16; // Vec element pointer-ish
    with_alloc_overhead(raw)
}

/// One set member.
pub fn estimate_set_member(member_len: usize) -> usize {
    let raw = member_len + BYTES_OVERHEAD + DICT_ENTRY_OVERHEAD;
    with_alloc_overhead(raw)
}

/// One sorted-set member (member blob + score + skiplist node approx).
pub fn estimate_zset_member(member_len: usize, skip_node_size: usize) -> usize {
    let raw = member_len
        + BYTES_OVERHEAD
        + DICT_ENTRY_OVERHEAD
        + skip_node_size
        + std::mem::size_of::<f64>();
    with_alloc_overhead(raw)
}

/// Keyed object total: map slot for `key` + content estimate (already taxed).
pub fn estimate_keyed_object(key_len: usize, content_size: usize) -> usize {
    let key_part = with_alloc_overhead(key_len + BYTES_OVERHEAD + DICT_ENTRY_OVERHEAD);
    key_part.saturating_add(content_size)
}

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

    #[test]
    fn with_alloc_overhead_grows_and_aligns() {
        assert_eq!(with_alloc_overhead(0), 0);
        let n = with_alloc_overhead(100);
        assert!(n > 100, "overhead must increase size");
        assert_eq!(n % 8, 0, "must be 8-byte aligned");
        // ~12.5%: 100 + 13 = 113 → align 120
        assert_eq!(n, 120);
    }

    #[test]
    fn string_entry_exceeds_payload() {
        let entry_sz = 128; // stand-in for size_of::<Entry>()
        let key = 4usize;
        let val = 10usize;
        let logical = logical_string_entry(key, val, entry_sz);
        let accounted = estimate_string_entry(key, val, entry_sz);
        assert_eq!(logical, key + val + entry_sz);
        assert!(accounted > logical);
        assert!(accounted > key + val);
    }

    #[test]
    fn keyed_object_includes_key_slot() {
        let content = 64;
        let a = estimate_keyed_object(3, content);
        let b = estimate_keyed_object(30, content);
        assert!(b > a);
        assert!(a > content);
    }
}
