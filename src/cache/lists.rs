use crate::error::Result;
use crate::list_type::{RedisList, SharedList};
use crate::memory::MemoryCategory;
use bytes::Bytes;
use std::sync::{Arc, RwLock};

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
        let lists = self.lists.write().unwrap();
        if let Some(existing) = lists.get(key) {
            return Ok(existing.clone());
        }
        let base = key.len() + std::mem::size_of::<RedisList>();
        drop(lists);
        self.ensure_non_string_capacity(base)?;
        let mut lists = self.lists.write().unwrap();
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
        let lists = self.lists.read().unwrap();
        lists.get(key).cloned()
    }

    pub fn remove_list(&self, key: &Bytes) -> bool {
        let mut lists = self.lists.write().unwrap();
        if let Some(l) = lists.remove(key) {
            let size = key.len()
                + l.read()
                    .map(|s| s.memory_size())
                    .unwrap_or(std::mem::size_of::<RedisList>());
            self.memory_tracker
                .deallocate(size, MemoryCategory::Lists);
            true
        } else {
            false
        }
    }

    pub fn list_exists(&self, key: &Bytes) -> bool {
        self.lists
            .read()
            .map(|l| l.contains_key(key))
            .unwrap_or(false)
    }

    pub fn remove_list_if_empty(&self, key: &Bytes) {
        let should_remove = self
            .get_list(key)
            .and_then(|l| l.read().ok().map(|g| g.is_empty()))
            .unwrap_or(false);
        if should_remove {
            self.remove_list(key);
        }
    }

    /// Export all lists: (key, [elements left-to-right]).
    pub fn export_lists(&self) -> Vec<(Bytes, Vec<Bytes>)> {
        let lists = match self.lists.read() {
            Ok(l) => l,
            Err(_) => return Vec::new(),
        };
        let mut out = Vec::with_capacity(lists.len());
        for (key, l) in lists.iter() {
            if let Ok(list) = l.read() {
                out.push((key.clone(), list.iter_items().collect()));
            }
        }
        out
    }
}
