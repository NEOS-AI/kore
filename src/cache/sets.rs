//! Redis Set storage (Batch FG-3: physical home is [`Cache::key_values`]).

use crate::error::Result;
use crate::set_type::{RedisSet, SharedSet};
use crate::memory::MemoryCategory;
use bytes::Bytes;
use parking_lot::RwLock;
use std::sync::Arc;

use super::keyspace::KeyValue;
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
    ///
    /// Physical insert goes into [`Self::key_values`] as [`KeyValue::Set`].
    pub fn get_or_create_set(&self, key: &Bytes) -> Result<SharedSet> {
        self.ensure_type(key, KeyType::Set)?;
        if let Some(s) = self.set_from_key_values(key) {
            return Ok(s);
        }
        let base = crate::memory::estimate_keyed_object(
            key.len(),
            RedisSet::new().memory_size(),
        );
        self.ensure_non_string_capacity(base)?;
        let kv = self.key_values.get_or_insert_with(key.clone(), || {
            self.memory_tracker
                .account(base, MemoryCategory::Sets);
            KeyValue::Set(Arc::new(RwLock::new(RedisSet::new())))
        });
        match kv {
            KeyValue::Set(s) => Ok(s),
            _ => Err(crate::error::Error::WrongType),
        }
    }

    #[inline]
    fn set_from_key_values(&self, key: &Bytes) -> Option<SharedSet> {
        match self.key_values.get(key) {
            Some(KeyValue::Set(s)) => Some(s),
            _ => None,
        }
    }

    pub fn get_set(&self, key: &Bytes) -> Option<SharedSet> {
        self.set_from_key_values(key)
    }

    pub fn remove_set(&self, key: &Bytes) -> bool {
        match self.key_values.remove(key) {
            Some(KeyValue::Set(s)) => {
                let size =
                    crate::memory::estimate_keyed_object(key.len(), s.read().memory_size());
                self.memory_tracker
                    .deallocate(size, MemoryCategory::Sets);
                true
            }
            Some(other) => {
                self.key_values.insert(key.clone(), other);
                false
            }
            None => false,
        }
    }

    pub fn set_exists(&self, key: &Bytes) -> bool {
        matches!(self.key_values.get(key), Some(KeyValue::Set(_)))
    }

    pub fn remove_set_if_empty(&self, key: &Bytes) {
        let should_remove = self
            .get_set(key)
            .map(|s| s.read().is_empty())
            .unwrap_or(false);
        if should_remove {
            self.remove_set(key);
        }
    }

    /// Export all sets: (key, [members]).
    /// Skips keys whose typed TTL has already elapsed (no revive without TTL).
    pub fn export_sets(&self) -> Vec<(Bytes, Vec<Bytes>)> {
        let mut out = Vec::new();
        self.key_values.for_each(|key, kv| {
            let KeyValue::Set(s) = kv else {
                return;
            };
            if !self.typed_key_exportable(key) {
                return;
            }
            let set = s.read();
            out.push((key.clone(), set.iter_members().collect()));
        });
        out
    }
}
