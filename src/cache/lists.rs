use crate::error::Result;
use crate::list_type::{RedisList, SharedList};
use crate::memory::MemoryCategory;
use bytes::Bytes;
use parking_lot::RwLock;
use std::sync::Arc;

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
    pub fn get_or_create_list(&self, key: &Bytes) -> Result<SharedList> {
        self.ensure_type(key, KeyType::List)?;
        let lists = self.lists.write();
        if let Some(existing) = lists.get(key) {
            return Ok(existing.clone());
        }
        let base = crate::memory::estimate_keyed_object(
            key.len(),
            RedisList::new().memory_size(),
        );
        drop(lists);
        self.ensure_non_string_capacity(base)?;
        let mut lists = self.lists.write();
        Ok(lists
            .entry(key.clone())
            .or_insert_with(|| {
                self.memory_tracker
                    .account(base, MemoryCategory::Lists);
                Arc::new(RwLock::new(RedisList::new()))
            })
            .clone())
    }

    pub fn get_list(&self, key: &Bytes) -> Option<SharedList> {
        let lists = self.lists.read();
        lists.get(key).cloned()
    }

    pub fn remove_list(&self, key: &Bytes) -> bool {
        let mut lists = self.lists.write();
        if let Some(l) = lists.remove(key) {
            let size = crate::memory::estimate_keyed_object(key.len(), l.read().memory_size());
            self.memory_tracker
                .deallocate(size, MemoryCategory::Lists);
            true
        } else {
            false
        }
    }

    pub fn list_exists(&self, key: &Bytes) -> bool {
        self.lists.read().contains_key(key)
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
        let lists = self.lists.read();
        let mut out = Vec::with_capacity(lists.len());
        for (key, l) in lists.iter() {
            if !self.typed_key_exportable(key) {
                continue;
            }
            let list = l.read();
            out.push((key.clone(), list.iter_items().collect()));
        }
        out
    }
}
