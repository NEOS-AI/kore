use crate::entry::Entry;
use crate::error::{Error, Result};
use crate::hashmap::EntryAction;
use crate::memory::MemoryCategory;
use bytes::Bytes;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::Cache;

impl Cache {
    /// Atomic increment (single-lock get-parse-add-store). Preserves TTL/flags.
    pub fn incr(&self, key: &Bytes, delta: i64) -> Result<i64> {
        self.stats.incr(&self.stats.cmd_incr);
        self.incr_decr(key, delta)
    }

    /// Atomic decrement
    pub fn decr(&self, key: &Bytes, delta: i64) -> Result<i64> {
        self.stats.incr(&self.stats.cmd_decr);
        // Match prior behavior: also counts as an incr command path via shared logic
        // without double-bumping cmd_incr.
        self.incr_decr(key, -delta)
    }

    /// Shared single-lock get-parse-add-store for INCR/DECR.
    fn incr_decr(&self, key: &Bytes, delta: i64) -> Result<i64> {
        enum IncrOutcome {
            Ok {
                value: i64,
                old_size: usize,
                new_size: usize,
            },
            NotInteger,
        }

        let outcome = self.map.mutate(key, |current, next_cas| {
            let (current_val, expires_at, flags) = match current {
                Some(entry) if !entry.is_expired() => {
                    let parsed = std::str::from_utf8(&entry.value)
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok());
                    match parsed {
                        Some(v) => (v, entry.expires_at, entry.flags),
                        None => return (EntryAction::Keep, IncrOutcome::NotInteger),
                    }
                }
                // Missing or expired: start from 0 (Redis semantics)
                _ => (0i64, None, 0u32),
            };

            let new_value = current_val + delta;
            let value_bytes = Bytes::from(new_value.to_string());

            let mut entry = Entry::new(key.clone(), value_bytes);
            // Preserve TTL and flags from the existing entry (Redis keeps TTL on INCR)
            entry.expires_at = expires_at;
            entry = entry.with_flags(flags).with_cas(next_cas);
            let entry = Arc::new(entry);

            let new_size = entry.size();
            // If replacing an expired entry still present in the map, free its size
            let old_size = current.map(|e| e.size()).unwrap_or(0);

            (
                EntryAction::Set(entry),
                IncrOutcome::Ok {
                    value: new_value,
                    old_size,
                    new_size,
                },
            )
        });

        match outcome {
            IncrOutcome::NotInteger => Err(Error::InvalidArgument(
                "value is not a valid integer".into(),
            )),
            IncrOutcome::Ok {
                value,
                old_size,
                new_size,
            } => {
                // Account size change on both counters
                if old_size > 0 {
                    self.memory_usage
                        .fetch_sub(old_size, Ordering::Relaxed);
                    self.memory_tracker
                        .deallocate(old_size, MemoryCategory::Cache);
                }
                if new_size > 0 {
                    self.memory_usage
                        .fetch_add(new_size, Ordering::Relaxed);
                    self.memory_tracker
                        .account(new_size, MemoryCategory::Cache);
                }
                Ok(value)
            }
        }
    }

    /// APPEND value to a string key (create if missing). Returns new string length.
    /// Atomic under the shard lock; preserves TTL/flags.
    pub fn append(&self, key: &Bytes, suffix: &Bytes) -> Result<usize> {
        self.ensure_string_or_absent(key)?;

        enum AppendOutcome {
            Ok {
                len: usize,
                old_size: usize,
                new_size: usize,
            },
            TooLarge,
        }

        // Pre-check capacity using worst-case growth (suffix only if key missing)
        let existing = self.map.get(key);
        let existing_size = existing
            .as_ref()
            .filter(|e| !e.is_expired())
            .map(|e| e.size())
            .unwrap_or(0);
        let existing_val_len = existing
            .as_ref()
            .filter(|e| !e.is_expired())
            .map(|e| e.value.len())
            .unwrap_or(0);
        let projected =
            key.len() + existing_val_len + suffix.len() + std::mem::size_of::<Entry>();
        let max_entry_size = self.max_entry_size.load(Ordering::Relaxed);
        if projected > max_entry_size {
            return Err(Error::EntryTooLarge);
        }
        let net = projected.saturating_sub(existing_size);
        self.ensure_capacity(net)?;

        let outcome = self.map.mutate(key, |current, next_cas| {
            let (old_val, expires_at, flags, old_size) = match current {
                Some(entry) if !entry.is_expired() => (
                    entry.value.clone(),
                    entry.expires_at,
                    entry.flags,
                    entry.size(),
                ),
                Some(entry) => {
                    // Expired: treat as empty string but free old size
                    (Bytes::new(), None, 0u32, entry.size())
                }
                None => (Bytes::new(), None, 0u32, 0usize),
            };

            let mut buf = Vec::with_capacity(old_val.len() + suffix.len());
            buf.extend_from_slice(&old_val);
            buf.extend_from_slice(suffix);
            let new_val = Bytes::from(buf);
            let len = new_val.len();

            let entry_size = key.len() + len + std::mem::size_of::<Entry>();
            if entry_size > max_entry_size {
                return (EntryAction::Keep, AppendOutcome::TooLarge);
            }

            let mut entry = Entry::new(key.clone(), new_val);
            entry.expires_at = expires_at;
            entry = entry.with_flags(flags).with_cas(next_cas);
            let new_size = entry.size();

            (
                EntryAction::Set(Arc::new(entry)),
                AppendOutcome::Ok {
                    len,
                    old_size,
                    new_size,
                },
            )
        });

        match outcome {
            AppendOutcome::TooLarge => Err(Error::EntryTooLarge),
            AppendOutcome::Ok {
                len,
                old_size,
                new_size,
            } => {
                if old_size > 0 {
                    self.memory_usage
                        .fetch_sub(old_size, Ordering::Relaxed);
                    self.memory_tracker
                        .deallocate(old_size, MemoryCategory::Cache);
                }
                if new_size > 0 {
                    self.memory_usage
                        .fetch_add(new_size, Ordering::Relaxed);
                    self.memory_tracker
                        .account(new_size, MemoryCategory::Cache);
                }
                Ok(len)
            }
        }
    }

    /// Rename `src` → `dst`. If `nx`, fail (return false) when `dst` exists.
    /// Works for all key types in the multi-map keyspace.
    pub fn rename(&self, src: &Bytes, dst: &Bytes, nx: bool) -> Result<bool> {
        if src == dst {
            // Redis: RENAME same key is OK if key exists; error if missing
            if self.key_type(src) == super::KeyType::None {
                return Err(Error::InvalidArgument("no such key".into()));
            }
            return Ok(true);
        }

        let src_type = self.key_type(src);
        if src_type == super::KeyType::None {
            return Err(Error::InvalidArgument("no such key".into()));
        }

        if nx && self.exists(dst) {
            return Ok(false);
        }

        // Overwrite destination
        if self.exists(dst) {
            self.delete(dst)?;
        }

        match src_type {
            super::KeyType::String => {
                let entry = self
                    .map
                    .remove(src)
                    .ok_or_else(|| Error::InvalidArgument("no such key".into()))?;
                let old_size = entry.size();
                let mut new_entry = (*entry).clone();
                new_entry.key = dst.clone();
                let new_size = new_entry.size();
                self.map.insert(dst.clone(), Arc::new(new_entry));
                // Adjust memory for key-length delta
                if new_size > old_size {
                    let delta = new_size - old_size;
                    self.memory_usage.fetch_add(delta, Ordering::Relaxed);
                    self.memory_tracker
                        .account(delta, MemoryCategory::Cache);
                } else if old_size > new_size {
                    let delta = old_size - new_size;
                    self.memory_usage.fetch_sub(delta, Ordering::Relaxed);
                    self.memory_tracker
                        .deallocate(delta, MemoryCategory::Cache);
                }
            }
            super::KeyType::Hash => {
                let mut hashes = self.hashes.write();
                let h = hashes
                    .remove(src)
                    .ok_or_else(|| Error::InvalidArgument("no such key".into()))?;
                let content = h.read().memory_size();
                // Re-account key length portion
                if src.len() != dst.len() {
                    self.memory_tracker
                        .deallocate(src.len() + content, MemoryCategory::Hashes);
                    self.memory_tracker
                        .account(dst.len() + content, MemoryCategory::Hashes);
                }
                hashes.insert(dst.clone(), h);
            }
            super::KeyType::List => {
                let mut lists = self.lists.write();
                let l = lists
                    .remove(src)
                    .ok_or_else(|| Error::InvalidArgument("no such key".into()))?;
                let content = l.read().memory_size();
                if src.len() != dst.len() {
                    self.memory_tracker
                        .deallocate(src.len() + content, MemoryCategory::Lists);
                    self.memory_tracker
                        .account(dst.len() + content, MemoryCategory::Lists);
                }
                lists.insert(dst.clone(), l);
            }
            super::KeyType::Set => {
                let mut sets = self.sets.write();
                let s = sets
                    .remove(src)
                    .ok_or_else(|| Error::InvalidArgument("no such key".into()))?;
                let content = s.read().memory_size();
                if src.len() != dst.len() {
                    self.memory_tracker
                        .deallocate(src.len() + content, MemoryCategory::Sets);
                    self.memory_tracker
                        .account(dst.len() + content, MemoryCategory::Sets);
                }
                sets.insert(dst.clone(), s);
            }
            super::KeyType::ZSet => {
                let z = self
                    .sorted_sets
                    .remove(src)
                    .ok_or_else(|| Error::InvalidArgument("no such key".into()))?;
                // Zset memory accounting is approximate; key-length delta only
                if src.len() != dst.len() {
                    if dst.len() > src.len() {
                        self.memory_tracker.account(
                            dst.len() - src.len(),
                            MemoryCategory::SortedSets,
                        );
                    } else {
                        self.memory_tracker.deallocate(
                            src.len() - dst.len(),
                            MemoryCategory::SortedSets,
                        );
                    }
                }
                self.sorted_sets.insert(dst.clone(), z);
            }
            super::KeyType::Geo => {
                let g = self
                    .geo_sets
                    .remove(src)
                    .ok_or_else(|| Error::InvalidArgument("no such key".into()))?;
                if src.len() != dst.len() {
                    if dst.len() > src.len() {
                        self.memory_tracker
                            .account(dst.len() - src.len(), MemoryCategory::GeoSets);
                    } else {
                        self.memory_tracker
                            .deallocate(src.len() - dst.len(), MemoryCategory::GeoSets);
                    }
                }
                self.geo_sets.insert(dst.clone(), g);
            }
            super::KeyType::Stream => {
                let mut streams = self.streams.write();
                let s = streams
                    .remove(src)
                    .ok_or_else(|| Error::InvalidArgument("no such key".into()))?;
                let content = s.read().memory_size();
                if src.len() != dst.len() {
                    self.memory_tracker
                        .deallocate(src.len() + content, MemoryCategory::Streams);
                    self.memory_tracker
                        .account(dst.len() + content, MemoryCategory::Streams);
                }
                streams.insert(dst.clone(), s);
            }
            super::KeyType::None => {
                return Err(Error::InvalidArgument("no such key".into()));
            }
        }

        Ok(true)
    }
}
