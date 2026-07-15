//! Stream keyspace helpers on Cache.

use crate::error::Result;
use crate::memory::MemoryCategory;
use crate::stream_type::{RedisStream, SharedStream, StreamStateSnapshot};
use bytes::Bytes;
use parking_lot::RwLock;
use std::sync::Arc;

use super::storage::KeyType;
use super::Cache;

impl Cache {
    pub(crate) fn account_stream_delta(&self, old_size: usize, new_size: usize) {
        if old_size > 0 {
            self.memory_tracker
                .deallocate(old_size, MemoryCategory::Streams);
        }
        if new_size > 0 {
            self.memory_tracker
                .account(new_size, MemoryCategory::Streams);
        }
    }

    /// Get or create a stream. WrongType if key holds a different type.
    pub fn get_or_create_stream(&self, key: &Bytes) -> Result<SharedStream> {
        self.ensure_type(key, KeyType::Stream)?;
        {
            let streams = self.streams.read();
            if let Some(existing) = streams.get(key) {
                return Ok(existing.clone());
            }
        }
        let base = crate::memory::estimate_keyed_object(
            key.len(),
            RedisStream::new().memory_size(),
        );
        self.ensure_non_string_capacity(base)?;
        let mut streams = self.streams.write();
        Ok(streams
            .entry(key.clone())
            .or_insert_with(|| {
                self.memory_tracker
                    .account(base, MemoryCategory::Streams);
                Arc::new(RwLock::new(RedisStream::new()))
            })
            .clone())
    }

    pub fn get_stream(&self, key: &Bytes) -> Option<SharedStream> {
        let streams = self.streams.read();
        streams.get(key).cloned()
    }

    pub fn remove_stream(&self, key: &Bytes) -> bool {
        let mut streams = self.streams.write();
        if let Some(s) = streams.remove(key) {
            let size =
                crate::memory::estimate_keyed_object(key.len(), s.read().memory_size());
            self.memory_tracker
                .deallocate(size, MemoryCategory::Streams);
            true
        } else {
            false
        }
    }

    pub fn stream_exists(&self, key: &Bytes) -> bool {
        self.streams.read().contains_key(key)
    }

    pub fn remove_stream_if_empty(&self, key: &Bytes) {
        // Streams are not auto-deleted when empty in Redis (unlike lists after LPOP).
        // Keep key if groups exist even when empty.
        let _should_remove = self
            .get_stream(key)
            .map(|s| {
                let g = s.read();
                g.is_empty() && g.group_names().is_empty()
            })
            .unwrap_or(false);
        // Redis keeps empty streams; do not auto-remove.
        let _ = _should_remove;
    }

    /// Export all streams with full state (entries, last_generated_id, groups, PEL).
    pub fn export_streams(&self) -> Vec<(Bytes, StreamStateSnapshot)> {
        let streams = self.streams.read();
        let mut out = Vec::with_capacity(streams.len());
        for (key, s) in streams.iter() {
            let stream = s.read();
            out.push((key.clone(), stream.export_state()));
        }
        out
    }

    /// Import a full stream snapshot, replacing any existing stream at `key`.
    pub fn import_stream(&self, key: Bytes, state: StreamStateSnapshot) -> Result<()> {
        // Remove existing stream so memory accounting stays correct.
        let _ = self.remove_stream(&key);
        let shared = self.get_or_create_stream(&key)?;
        let old_size = shared.read().memory_size();
        {
            let mut stream = shared.write();
            stream.import_state(state);
            let new_size = stream.memory_size();
            self.account_stream_delta(old_size, new_size);
        }
        Ok(())
    }
}
