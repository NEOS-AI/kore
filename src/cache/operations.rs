use crate::entry::Entry;
use crate::error::{Error, Result};
use crate::hashmap::EntryAction;
use crate::memory::MemoryCategory;
use bytes::Bytes;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::keyspace::{KeySlot, KeyValue};
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

    /// Atomic float increment (INCRBYFLOAT). Preserves TTL/flags. Missing key → 0.0.
    pub fn incr_by_float(&self, key: &Bytes, delta: f64) -> Result<f64> {
        enum Outcome {
            Ok {
                value: f64,
                old_size: usize,
                new_size: usize,
            },
            NotFloat,
            Nan,
        }

        let outcome = match self.mutate_string(key, |current, next_cas| {
            let (current_val, expires_at, flags) = match current {
                Some(entry) if !entry.is_expired() => {
                    let parsed = std::str::from_utf8(&entry.value)
                        .ok()
                        .and_then(|s| s.trim().parse::<f64>().ok());
                    match parsed {
                        Some(v) if !v.is_nan() => (v, entry.expires_at, entry.flags),
                        _ => return (EntryAction::Keep, Outcome::NotFloat),
                    }
                }
                _ => (0.0f64, None, 0u32),
            };

            let new_value = current_val + delta;
            if new_value.is_nan() {
                return (EntryAction::Keep, Outcome::Nan);
            }

            // Redis-ish rendering: drop trailing .0 for exact integers.
            let rendered = if new_value.fract() == 0.0
                && new_value.is_finite()
                && new_value.abs() < 1e15
            {
                format!("{}", new_value as i64)
            } else {
                format!("{}", new_value)
            };
            let value_bytes = Bytes::from(rendered);

            let mut entry = Entry::new(key.clone(), value_bytes);
            entry.expires_at = expires_at;
            entry = entry.with_flags(flags).with_cas(next_cas);
            let entry = Arc::new(entry);

            let new_size = entry.size();
            let old_size = current.map(|e| e.size()).unwrap_or(0);

            (
                EntryAction::Set(entry),
                Outcome::Ok {
                    value: new_value,
                    old_size,
                    new_size,
                },
            )
        }) {
            Ok(o) => o,
            Err(e) => return Err(e),
        };

        match outcome {
            Outcome::NotFloat => Err(Error::InvalidArgument(
                "value is not a valid float".into(),
            )),
            Outcome::Nan => Err(Error::InvalidArgument(
                "increment would produce NaN".into(),
            )),
            Outcome::Ok {
                value,
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
                Ok(value)
            }
        }
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

        let outcome = match self.mutate_string(key, |current, next_cas| {
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
        }) {
            Ok(o) => o,
            Err(e) => return Err(e),
        };

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
        let existing = self.get_string_entry(key);
        let existing_size = existing.as_ref().map(|e| e.size()).unwrap_or(0);
        let existing_val_len = existing.as_ref().map(|e| e.value.len()).unwrap_or(0);
        let entry_sz = std::mem::size_of::<Entry>();
        let logical = crate::memory::logical_string_entry(
            key.len(),
            existing_val_len + suffix.len(),
            entry_sz,
        );
        let max_entry_size = self.max_entry_size.load(Ordering::Relaxed);
        if logical > max_entry_size {
            return Err(Error::EntryTooLarge);
        }
        let projected = crate::memory::estimate_string_entry(
            key.len(),
            existing_val_len + suffix.len(),
            entry_sz,
        );
        let net = projected.saturating_sub(existing_size);
        self.ensure_capacity(net)?;

        let outcome = match self.mutate_string(key, |current, next_cas| {
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

            let logical_sz =
                crate::memory::logical_string_entry(key.len(), len, entry_sz);
            if logical_sz > max_entry_size {
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
        }) {
            Ok(o) => o,
            Err(e) => return Err(e),
        };

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

    /// SETRANGE: overwrite bytes starting at `offset` (zero-pad if needed).
    /// Creates the key if missing. Returns the new string length. Preserves TTL/flags.
    pub fn setrange(&self, key: &Bytes, offset: usize, value: &Bytes) -> Result<usize> {
        self.ensure_string_or_absent(key)?;

        enum SetRangeOutcome {
            Ok {
                len: usize,
                old_size: usize,
                new_size: usize,
            },
            TooLarge,
        }

        let existing = self.get_string_entry(key);
        let existing_size = existing.as_ref().map(|e| e.size()).unwrap_or(0);
        let existing_val_len = existing.as_ref().map(|e| e.value.len()).unwrap_or(0);
        let new_len = (offset + value.len()).max(existing_val_len);
        let entry_sz = std::mem::size_of::<Entry>();
        let logical = crate::memory::logical_string_entry(key.len(), new_len, entry_sz);
        let max_entry_size = self.max_entry_size.load(Ordering::Relaxed);
        if logical > max_entry_size {
            return Err(Error::EntryTooLarge);
        }
        let projected =
            crate::memory::estimate_string_entry(key.len(), new_len, entry_sz);
        let net = projected.saturating_sub(existing_size);
        self.ensure_capacity(net)?;

        let outcome = match self.mutate_string(key, |current, next_cas| {
            let (old_val, expires_at, flags, old_size) = match current {
                Some(entry) if !entry.is_expired() => (
                    entry.value.clone(),
                    entry.expires_at,
                    entry.flags,
                    entry.size(),
                ),
                Some(entry) => (Bytes::new(), None, 0u32, entry.size()),
                None => (Bytes::new(), None, 0u32, 0usize),
            };

            let end = offset + value.len();
            let mut buf = vec![0u8; old_val.len().max(end)];
            buf[..old_val.len()].copy_from_slice(&old_val);
            if !value.is_empty() {
                buf[offset..end].copy_from_slice(value);
            }
            let new_val = Bytes::from(buf);
            let len = new_val.len();

            let logical_sz =
                crate::memory::logical_string_entry(key.len(), len, entry_sz);
            if logical_sz > max_entry_size {
                return (EntryAction::Keep, SetRangeOutcome::TooLarge);
            }

            let mut entry = Entry::new(key.clone(), new_val);
            entry.expires_at = expires_at;
            entry = entry.with_flags(flags).with_cas(next_cas);
            let new_size = entry.size();

            (
                EntryAction::Set(Arc::new(entry)),
                SetRangeOutcome::Ok {
                    len,
                    old_size,
                    new_size,
                },
            )
        }) {
            Ok(o) => o,
            Err(e) => return Err(e),
        };

        match outcome {
            SetRangeOutcome::TooLarge => Err(Error::EntryTooLarge),
            SetRangeOutcome::Ok {
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

        // FG-4: all types live in key_values.
        match src_type {
            super::KeyType::None => {
                return Err(Error::InvalidArgument("no such key".into()));
            }
            super::KeyType::String => {
                let slot = self
                    .key_values
                    .remove(src)
                    .ok_or_else(|| Error::InvalidArgument("no such key".into()))?;
                let KeyValue::String(entry) = slot.value.clone() else {
                    self.key_values.insert(src.clone(), slot);
                    return Err(Error::InvalidArgument("no such key".into()));
                };
                let old_size = entry.size();
                let mut new_entry = (*entry).clone();
                new_entry.key = dst.clone();
                let new_size = new_entry.size();
                self.key_values
                    .insert(dst.clone(), KeySlot::string(Arc::new(new_entry)));
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
            _ => {
                // Batch FP: whole KeySlot moves (value + typed expire).
                let slot = self
                    .key_values
                    .remove(src)
                    .ok_or_else(|| Error::InvalidArgument("no such key".into()))?;
                if slot.value.key_type() != src_type {
                    // Defensive: put back if type mismatch (should not happen).
                    self.key_values.insert(src.clone(), slot);
                    return Err(Error::InvalidArgument("no such key".into()));
                }
                let content = slot.value.content_memory_size();
                let cat = slot.value.memory_category();
                self.account_typed_key_rename(src, dst, content, cat);
                self.key_values.insert(dst.clone(), slot);
            }
        }

        Ok(true)
    }
}
