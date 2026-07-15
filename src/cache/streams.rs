//! Stream keyspace helpers on Cache.

use crate::error::Result;
use crate::memory::MemoryCategory;
use crate::stream_type::{RedisStream, SharedStream, StreamStateSnapshot};
use bytes::Bytes;
use std::sync::{Arc, RwLock};

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
            let streams = self.streams.read().unwrap();
            if let Some(existing) = streams.get(key) {
                return Ok(existing.clone());
            }
        }
        let base = key.len() + std::mem::size_of::<RedisStream>();
        self.ensure_non_string_capacity(base)?;
        let mut streams = self.streams.write().unwrap();
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
        let streams = self.streams.read().unwrap();
        streams.get(key).cloned()
    }

    pub fn remove_stream(&self, key: &Bytes) -> bool {
        let mut streams = self.streams.write().unwrap();
        if let Some(s) = streams.remove(key) {
            let size = key.len()
                + s.read()
                    .map(|st| st.memory_size())
                    .unwrap_or(std::mem::size_of::<RedisStream>());
            self.memory_tracker
                .deallocate(size, MemoryCategory::Streams);
            true
        } else {
            false
        }
    }

    pub fn stream_exists(&self, key: &Bytes) -> bool {
        self.streams
            .read()
            .map(|s| s.contains_key(key))
            .unwrap_or(false)
    }

    pub fn remove_stream_if_empty(&self, key: &Bytes) {
        // Streams are not auto-deleted when empty in Redis (unlike lists after LPOP).
        // Keep key if groups exist even when empty.
        let should_remove = self
            .get_stream(key)
            .and_then(|s| {
                s.read().ok().map(|g| g.is_empty() && g.group_names().is_empty())
            })
            .unwrap_or(false);
        if should_remove {
            // Redis keeps empty streams; do not auto-remove.
            let _ = should_remove;
        }
    }

    /// Export all streams with full state (entries, last_generated_id, groups, PEL).
    pub fn export_streams(&self) -> Vec<(Bytes, StreamStateSnapshot)> {
        let streams = match self.streams.read() {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let mut out = Vec::with_capacity(streams.len());
        for (key, s) in streams.iter() {
            if let Ok(stream) = s.read() {
                out.push((key.clone(), stream.export_state()));
            }
        }
        out
    }

    /// Import a full stream snapshot, replacing any existing stream at `key`.
    pub fn import_stream(&self, key: Bytes, state: StreamStateSnapshot) -> Result<()> {
        // Remove existing stream so memory accounting stays correct.
        let _ = self.remove_stream(&key);
        let shared = self.get_or_create_stream(&key)?;
        let old_size = shared
            .read()
            .map(|s| s.memory_size())
            .unwrap_or(0);
        {
            let mut stream = shared
                .write()
                .map_err(|_| crate::error::Error::NetworkError("stream lock poisoned".into()))?;
            stream.import_state(state);
            let new_size = stream.memory_size();
            self.account_stream_delta(old_size, new_size);
        }
        Ok(())
    }
}
