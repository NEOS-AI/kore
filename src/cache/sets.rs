use crate::error::Result;
use crate::set_type::{RedisSet, SharedSet};
use crate::memory::MemoryCategory;
use bytes::Bytes;
use std::sync::{Arc, RwLock};

use super::storage::KeyType;
use super::Cache;

impl Cache {
    pub(crate) fn account_set_delta(&self, old_size: usize, new_size: usize) {
        if old_size > 0 {
            self.memory_tracker
                .deallocate(old_size, MemoryCategory::Sets);
        }
        if new_size > 0 {
            self.memory_tracker
                .account(new_size, MemoryCategory::Sets);
        }
    }

    /// Get or create a set. WrongType if key holds a different type.
    pub fn get_or_create_set(&self, key: &Bytes) -> Result<SharedSet> {
        self.ensure_type(key, KeyType::Set)?;
        let sets = self.sets.write().unwrap();
        if let Some(existing) = sets.get(key) {
            return Ok(existing.clone());
        }
        let base = key.len() + std::mem::size_of::<RedisSet>();
        drop(sets);
        self.ensure_non_string_capacity(base)?;
        let mut sets = self.sets.write().unwrap();
        Ok(sets
            .entry(key.clone())
            .or_insert_with(|| {
                self.memory_tracker
                    .account(base, MemoryCategory::Sets);
                Arc::new(RwLock::new(RedisSet::new()))
            })
            .clone())
    }

    pub fn get_set(&self, key: &Bytes) -> Option<SharedSet> {
        let sets = self.sets.read().unwrap();
        sets.get(key).cloned()
    }

    pub fn remove_set(&self, key: &Bytes) -> bool {
        let mut sets = self.sets.write().unwrap();
        if let Some(s) = sets.remove(key) {
            let size = key.len()
                + s.read()
                    .map(|set| set.memory_size())
                    .unwrap_or(std::mem::size_of::<RedisSet>());
            self.memory_tracker
                .deallocate(size, MemoryCategory::Sets);
            true
        } else {
            false
        }
    }

    pub fn set_exists(&self, key: &Bytes) -> bool {
        self.sets
            .read()
            .map(|s| s.contains_key(key))
            .unwrap_or(false)
    }

    pub fn remove_set_if_empty(&self, key: &Bytes) {
        let should_remove = self
            .get_set(key)
            .and_then(|s| s.read().ok().map(|g| g.is_empty()))
            .unwrap_or(false);
        if should_remove {
            self.remove_set(key);
        }
    }

    /// Export all sets: (key, [members]).
    pub fn export_sets(&self) -> Vec<(Bytes, Vec<Bytes>)> {
        let sets = match self.sets.read() {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let mut out = Vec::with_capacity(sets.len());
        for (key, s) in sets.iter() {
            if let Ok(set) = s.read() {
                out.push((key.clone(), set.iter_members().collect()));
            }
        }
        out
    }
}
