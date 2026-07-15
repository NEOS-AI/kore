use crate::error::Result;
use crate::hash_type::{RedisHash, SharedHash};
use crate::memory::MemoryCategory;
use bytes::Bytes;
use parking_lot::RwLock;
use std::sync::Arc;

use super::storage::KeyType;
use super::Cache;

impl Cache {
    pub(crate) fn account_hash_delta(&self, old_size: usize, new_size: usize) {
        if old_size > 0 {
            self.memory_tracker
                .deallocate(old_size, MemoryCategory::Hashes);
        }
        if new_size > 0 {
            self.memory_tracker
                .account(new_size, MemoryCategory::Hashes);
        }
    }

    /// Get or create a hash. WrongType if key holds a different type.
    pub fn get_or_create_hash(&self, key: &Bytes) -> Result<SharedHash> {
        self.ensure_type(key, KeyType::Hash)?;
        let hashes = self.hashes.write();
        if let Some(existing) = hashes.get(key) {
            return Ok(existing.clone());
        }
        let base = key.len() + std::mem::size_of::<RedisHash>();
        drop(hashes);
        self.ensure_non_string_capacity(base)?;
        let mut hashes = self.hashes.write();
        Ok(hashes
            .entry(key.clone())
            .or_insert_with(|| {
                self.memory_tracker
                    .account(base, MemoryCategory::Hashes);
                Arc::new(RwLock::new(RedisHash::new()))
            })
            .clone())
    }

    pub fn get_hash(&self, key: &Bytes) -> Option<SharedHash> {
        let hashes = self.hashes.read();
        hashes.get(key).cloned()
    }

    pub fn remove_hash(&self, key: &Bytes) -> bool {
        let mut hashes = self.hashes.write();
        if let Some(h) = hashes.remove(key) {
            let size = key.len() + h.read().memory_size();
            self.memory_tracker
                .deallocate(size, MemoryCategory::Hashes);
            true
        } else {
            false
        }
    }

    pub fn hash_exists(&self, key: &Bytes) -> bool {
        self.hashes.read().contains_key(key)
    }

    /// Remove empty hash key after mutations that may empty it.
    pub fn remove_hash_if_empty(&self, key: &Bytes) {
        let should_remove = self
            .get_hash(key)
            .map(|h| h.read().is_empty())
            .unwrap_or(false);
        if should_remove {
            self.remove_hash(key);
        }
    }

    /// Export all hashes: (key, [(field, value), ...]).
    pub fn export_hashes(&self) -> Vec<(Bytes, Vec<(Bytes, Bytes)>)> {
        let hashes = self.hashes.read();
        let mut out = Vec::with_capacity(hashes.len());
        for (key, h) in hashes.iter() {
            let hash = h.read();
            out.push((key.clone(), hash.iter_fields().collect()));
        }
        out
    }
}
