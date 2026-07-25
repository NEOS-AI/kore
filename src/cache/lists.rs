//! Redis List storage (Batch FG-3: physical home is [`Cache::key_values`]).

use crate::error::Result;
use crate::list_type::{RedisList, SharedList};
use crate::memory::MemoryCategory;
use bytes::Bytes;
use parking_lot::RwLock;
use std::sync::Arc;

use super::keyspace::KeyValue;
use super::storage::KeyType;
use super::Cache;

impl Cache {
    pub(crate) fn account_list_delta(&self, old_size: usize, new_size: usize) {
        if old_size > 0 {
            self.memory_tracker
                .deallocate(old_size, MemoryCategory::Lists);
        }
        if new_size > 0 {
            self.memory_tracker
                .account(new_size, MemoryCategory::Lists);
        }
    }

    /// Get or create a list. WrongType if key holds a different type.
    ///
    /// Physical insert goes into [`Self::key_values`] as [`KeyValue::List`].
    pub fn get_or_create_list(&self, key: &Bytes) -> Result<SharedList> {
        self.ensure_type(key, KeyType::List)?;
        if let Some(l) = self.list_from_key_values(key) {
            return Ok(l);
        }
        let base = crate::memory::estimate_keyed_object(
            key.len(),
            RedisList::new().memory_size(),
        );
        self.ensure_non_string_capacity(base)?;
        let kv = self.key_values.get_or_insert_with(key.clone(), || {
            self.memory_tracker
                .account(base, MemoryCategory::Lists);
            KeyValue::List(Arc::new(RwLock::new(RedisList::new())))
        });
        match kv {
            KeyValue::List(l) => Ok(l),
            _ => Err(crate::error::Error::WrongType),
        }
    }

    #[inline]
    fn list_from_key_values(&self, key: &Bytes) -> Option<SharedList> {
        match self.key_values.get(key) {
            Some(KeyValue::List(l)) => Some(l),
            _ => None,
        }
    }

    pub fn get_list(&self, key: &Bytes) -> Option<SharedList> {
        self.list_from_key_values(key)
    }

    pub fn remove_list(&self, key: &Bytes) -> bool {
        match self.key_values.remove(key) {
            Some(KeyValue::List(l)) => {
                let size =
                    crate::memory::estimate_keyed_object(key.len(), l.read().memory_size());
                self.memory_tracker
                    .deallocate(size, MemoryCategory::Lists);
                true
            }
            Some(other) => {
                self.key_values.insert(key.clone(), other);
                false
            }
            None => false,
        }
    }

    pub fn list_exists(&self, key: &Bytes) -> bool {
        matches!(self.key_values.get(key), Some(KeyValue::List(_)))
    }

    pub fn remove_list_if_empty(&self, key: &Bytes) {
        let should_remove = self
            .get_list(key)
            .map(|l| l.read().is_empty())
            .unwrap_or(false);
        if should_remove {
            self.remove_list(key);
        }
    }

    /// Export all lists: (key, [elements left-to-right]).
    /// Skips keys whose typed TTL has already elapsed (no revive without TTL).
    pub fn export_lists(&self) -> Vec<(Bytes, Vec<Bytes>)> {
        let mut out = Vec::new();
        self.key_values.for_each(|key, kv| {
            let KeyValue::List(l) = kv else {
                return;
            };
            if !self.typed_key_exportable(key) {
                return;
            }
            let list = l.read();
            out.push((key.clone(), list.iter_items().collect()));
        });
        out
    }
}
