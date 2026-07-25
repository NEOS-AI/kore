//! Redis Hash storage (Batch FG-2/FG-3: physical home is [`Cache::key_values`]).
//!
//! Hashes are stored as [`KeyValue::Hash`] in the unified sharded map alongside
//! list/set/zset/geo/stream. Command APIs remain type-specific for H* handlers;
//! cross-type TYPE/DEL/EXISTS use the keyspace facade.

use crate::error::Result;
use crate::hash_type::{RedisHash, SharedHash};
use crate::memory::MemoryCategory;
use bytes::Bytes;
use parking_lot::RwLock;
use std::sync::Arc;

use super::keyspace::KeyValue;
use super::storage::KeyType;
use super::Cache;

impl Cache {
    /// Apply a hash size change to the tracker.
    ///
    /// When `new_size > old_size`, capacity is checked first (eviction may run).
    /// On `OutOfMemory` nothing is accounted — the caller must roll back the
    /// in-memory hash mutation so tracked totals never exceed maxmemory.
    pub(crate) fn account_hash_delta(&self, old_size: usize, new_size: usize) -> Result<()> {
        if new_size > old_size {
            self.ensure_non_string_capacity(new_size - old_size)?;
        }
        if old_size > 0 {
            self.memory_tracker
                .deallocate(old_size, MemoryCategory::Hashes);
        }
        if new_size > 0 {
            self.memory_tracker
                .account(new_size, MemoryCategory::Hashes);
        }
        Ok(())
    }

    /// Get or create a hash. WrongType if key holds a different type.
    ///
    /// Physical insert goes into [`Self::key_values`] as [`KeyValue::Hash`].
    pub fn get_or_create_hash(&self, key: &Bytes) -> Result<SharedHash> {
        self.ensure_type(key, KeyType::Hash)?;
        if let Some(h) = self.hash_from_key_values(key) {
            return Ok(h);
        }
        let base = crate::memory::estimate_keyed_object(
            key.len(),
            RedisHash::new().memory_size(),
        );
        self.ensure_non_string_capacity(base)?;
        let kv = self.key_values.get_or_insert_with(key.clone(), || {
            self.memory_tracker
                .account(base, MemoryCategory::Hashes);
            KeyValue::Hash(Arc::new(RwLock::new(RedisHash::new())))
        });
        match kv {
            KeyValue::Hash(h) => Ok(h),
            _ => Err(crate::error::Error::WrongType),
        }
    }

    /// Resolve a hash from the unified map (no create).
    #[inline]
    fn hash_from_key_values(&self, key: &Bytes) -> Option<SharedHash> {
        match self.key_values.get(key) {
            Some(KeyValue::Hash(h)) => Some(h),
            _ => None,
        }
    }

    pub fn get_hash(&self, key: &Bytes) -> Option<SharedHash> {
        self.hash_from_key_values(key)
    }

    pub fn remove_hash(&self, key: &Bytes) -> bool {
        match self.key_values.remove(key) {
            Some(KeyValue::Hash(h)) => {
                let size =
                    crate::memory::estimate_keyed_object(key.len(), h.read().memory_size());
                self.memory_tracker
                    .deallocate(size, MemoryCategory::Hashes);
                true
            }
            Some(other) => {
                self.key_values.insert(key.clone(), other);
                false
            }
            None => false,
        }
    }

    pub fn hash_exists(&self, key: &Bytes) -> bool {
        matches!(self.key_values.get(key), Some(KeyValue::Hash(_)))
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
    /// Skips keys whose typed TTL has already elapsed (no revive without TTL).
    pub fn export_hashes(&self) -> Vec<(Bytes, Vec<(Bytes, Bytes)>)> {
        let mut out = Vec::new();
        self.key_values.for_each(|key, kv| {
            let KeyValue::Hash(h) = kv else {
                return;
            };
            if !self.typed_key_exportable(key) {
                return;
            }
            let hash = h.read();
            out.push((key.clone(), hash.iter_fields().collect()));
        });
        out
    }
}
