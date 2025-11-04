use crate::sorted_set::SharedSortedSet;
use bytes::Bytes;
use std::sync::{Arc, RwLock};

use super::Cache;
use crate::sorted_set::SortedSet;

impl Cache {
    /// Get or create a sorted set
    pub fn get_or_create_sorted_set(&self, key: &Bytes) -> SharedSortedSet {
        let mut sets = self.sorted_sets.write().unwrap();
        sets.entry(key.clone())
            .or_insert_with(|| Arc::new(RwLock::new(SortedSet::new())))
            .clone()
    }

    /// Get a sorted set if it exists
    pub fn get_sorted_set(&self, key: &Bytes) -> Option<SharedSortedSet> {
        let sets = self.sorted_sets.read().unwrap();
        sets.get(key).cloned()
    }

    /// Remove a sorted set
    pub fn remove_sorted_set(&self, key: &Bytes) -> bool {
        let mut sets = self.sorted_sets.write().unwrap();
        sets.remove(key).is_some()
    }

    /// Check if a sorted set exists
    pub fn sorted_set_exists(&self, key: &Bytes) -> bool {
        let sets = self.sorted_sets.read().unwrap();
        sets.contains_key(key)
    }
}
