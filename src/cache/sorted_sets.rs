use crate::error::{Error, Result};
use crate::memory::MemoryCategory;
use crate::sorted_set::{SharedSortedSet, SortedSet};
use bytes::Bytes;
use parking_lot::RwLock;
use std::sync::Arc;

use super::storage::KeyType;
use super::Cache;

impl Cache {
    /// Account a net memory change for sorted sets (updates MemoryTracker only).
    pub(crate) fn account_sorted_set_delta(&self, old_size: usize, new_size: usize) {
        if old_size > 0 {
            self.memory_tracker
                .deallocate(old_size, MemoryCategory::SortedSets);
        }
        if new_size > 0 {
            self.memory_tracker
                .account(new_size, MemoryCategory::SortedSets);
        }
    }

    /// Ensure capacity for growing a sorted set / geo structure by `needed` bytes.
    pub(crate) fn ensure_non_string_capacity(&self, needed: usize) -> Result<()> {
        if needed == 0 {
            return Ok(());
        }
        if self
            .memory_tracker
            .can_allocate(needed, MemoryCategory::SortedSets)
        {
            return Ok(());
        }
        if self.eviction_allowed() {
            // Evict keys (any type) to free room in the shared max_memory budget.
            if self.evict_memory(needed).is_ok()
                && self
                    .memory_tracker
                    .can_allocate(needed, MemoryCategory::SortedSets)
            {
                return Ok(());
            }
        }
        self.stats.incr(&self.stats.store_no_memory);
        Err(Error::OutOfMemory)
    }

    /// Get or create a sorted set.
    /// Returns WrongType if the key already holds a different type.
    pub fn get_or_create_sorted_set(&self, key: &Bytes) -> Result<SharedSortedSet> {
        self.ensure_type(key, KeyType::ZSet)?;
        if let Some(existing) = self.sorted_sets.get(key) {
            return Ok(existing);
        }
        // Base overhead: key + empty SortedSet (allocator-aware)
        let base = crate::memory::estimate_keyed_object(
            key.len(),
            SortedSet::new().memory_size(),
        );
        self.ensure_non_string_capacity(base)?;
        Ok(self.sorted_sets.get_or_insert_with(key.clone(), || {
            self.memory_tracker
                .account(base, MemoryCategory::SortedSets);
            Arc::new(RwLock::new(SortedSet::new()))
        }))
    }

    /// Get a sorted set if it exists
    pub fn get_sorted_set(&self, key: &Bytes) -> Option<SharedSortedSet> {
        self.sorted_sets.get(key)
    }

    /// Remove a sorted set and free its tracked memory
    pub fn remove_sorted_set(&self, key: &Bytes) -> bool {
        if let Some(set) = self.sorted_sets.remove(key) {
            let size =
                crate::memory::estimate_keyed_object(key.len(), set.read().memory_size());
            self.memory_tracker
                .deallocate(size, MemoryCategory::SortedSets);
            true
        } else {
            false
        }
    }

    /// Check if a sorted set exists
    pub fn sorted_set_exists(&self, key: &Bytes) -> bool {
        self.sorted_sets.contains_key(key)
    }

    /// Export all sorted sets for persistence: (key, [(member, score), ...]).
    /// Skips keys whose typed TTL has already elapsed (no revive without TTL).
    pub fn export_zsets(&self) -> Vec<(Bytes, Vec<(Bytes, f64)>)> {
        let mut out = Vec::new();
        self.sorted_sets.for_each(|key, zset| {
            if !self.typed_key_exportable(key) {
                return;
            }
            let set = zset.read();
            out.push((key.clone(), set.iter_members().collect()));
        });
        out
    }
}
