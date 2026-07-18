use crate::error::Result;
use crate::memory::MemoryCategory;
use bytes::Bytes;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::storage::KeyType;
use super::Cache;

impl Cache {
    /// Set expiration on a key (milliseconds from now). Works for all key types.
    /// Returns true if the timeout was set (or key deleted for `ttl_ms == 0`).
    ///
    /// Redis: `EXPIRE key 0` / zero ms deletes the key immediately.
    pub fn expire(&self, key: &Bytes, ttl_ms: u64) -> Result<bool> {
        // Lazy-delete if already past expiry so we don't revive a ghost key.
        let kt = self.key_type(key);
        if kt == KeyType::None {
            return Ok(false);
        }
        if ttl_ms == 0 {
            let _ = self.delete(key);
            return Ok(true);
        }
        match kt {
            KeyType::None => Ok(false),
            KeyType::String => match self.map.get(key) {
                Some(entry) if !entry.is_expired() => {
                    let mut new_entry = (*entry).clone();
                    new_entry.expires_at = Some(Instant::now() + Duration::from_millis(ttl_ms));
                    self.map.insert(key.clone(), Arc::new(new_entry));
                    Ok(true)
                }
                _ => Ok(false),
            },
            _ => {
                self.typed_expires
                    .write()
                    .insert(key.clone(), Instant::now() + Duration::from_millis(ttl_ms));
                Ok(true)
            }
        }
    }

    /// Set absolute Unix-epoch-millisecond expiration (all key types).
    /// Past or equal timestamps delete the key (Redis EXPIREAT/PEXPIREAT).
    pub fn expire_at_unix_ms(&self, key: &Bytes, expire_unix_ms: i64) -> Result<bool> {
        let kt = self.key_type(key);
        if kt == KeyType::None {
            return Ok(false);
        }
        let now = now_unix_ms();
        if expire_unix_ms <= now {
            let _ = self.delete(key);
            return Ok(true);
        }
        let remaining = (expire_unix_ms - now) as u64;
        self.expire(key, remaining)
    }

    /// Absolute expire time as Unix epoch milliseconds.
    /// `-2` missing, `-1` no expire, else absolute ms.
    pub fn expire_time_unix_ms(&self, key: &Bytes) -> i64 {
        let ttl = self.ttl(key);
        if ttl < 0 {
            return ttl; // -1 or -2
        }
        now_unix_ms() + ttl
    }

    /// Remove any expiration from a key. Returns true if a timeout was removed.
    pub fn persist(&self, key: &Bytes) -> bool {
        match self.key_type(key) {
            KeyType::None => false,
            KeyType::String => match self.map.get(key) {
                Some(entry) if !entry.is_expired() && entry.expires_at.is_some() => {
                    let mut new_entry = (*entry).clone();
                    new_entry.expires_at = None;
                    self.map.insert(key.clone(), Arc::new(new_entry));
                    true
                }
                _ => false,
            },
            _ => self.typed_expires.write().remove(key).is_some(),
        }
    }

    /// Get TTL in milliseconds (-1 = no expiration, -2 = expired/not found).
    pub fn ttl(&self, key: &Bytes) -> i64 {
        // String path first (includes lazy string expire via is_expired).
        if let Some(entry) = self.map.get(key) {
            if !entry.is_expired() {
                return entry.ttl_millis().unwrap_or(-1);
            }
        }

        // Typed: purge if past due, then report remaining or -1.
        if self.purge_typed_if_expired(key) {
            return -2;
        }

        if self.typed_key_present_raw(key) {
            return match self.typed_expires.read().get(key) {
                Some(exp) => {
                    let now = Instant::now();
                    if *exp <= now {
                        // Race: treat as gone; next access purges.
                        0
                    } else {
                        exp.duration_since(now).as_millis() as i64
                    }
                }
                None => -1,
            };
        }

        -2
    }

    /// Absolute Instant expiry for a typed key, if any.
    pub(super) fn typed_expires_at(&self, key: &Bytes) -> Option<Instant> {
        self.typed_expires.read().get(key).copied()
    }

    /// Clear typed expire metadata (call when deleting/overwriting a typed key).
    pub(super) fn clear_typed_expire(&self, key: &Bytes) {
        self.typed_expires.write().remove(key);
    }

    /// Move typed expire from `src` to `dst` (RENAME).
    pub(super) fn move_typed_expire(&self, src: &Bytes, dst: &Bytes) {
        let mut g = self.typed_expires.write();
        if let Some(exp) = g.remove(src) {
            g.insert(dst.clone(), exp);
        } else {
            // Destination may have had its own expire; src had none → clear dst.
            g.remove(dst);
        }
    }

    /// Whether a typed key should be included in persistence export.
    ///
    /// Returns false when the key has a past `typed_expires` entry (would be
    /// revived without TTL if exported). Keys with no expire record are live.
    pub(super) fn typed_key_exportable(&self, key: &Bytes) -> bool {
        match self.typed_expires.read().get(key) {
            Some(exp) if *exp <= Instant::now() => false,
            _ => true,
        }
    }

    /// Export typed expires as absolute Unix ms for persistence.
    pub fn export_typed_expires_unix_ms(&self) -> Vec<(Bytes, i64)> {
        let now_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let now = Instant::now();
        let g = self.typed_expires.read();
        g.iter()
            .filter_map(|(k, exp)| {
                if *exp <= now {
                    return None;
                }
                let remaining_ms = exp.duration_since(now).as_millis() as i64;
                Some((k.clone(), now_unix + remaining_ms))
            })
            .collect()
    }

    /// Restore a typed-key expire from absolute Unix epoch ms (load path).
    pub fn set_typed_expire_unix_ms(&self, key: &Bytes, expire_unix_ms: i64) {
        if expire_unix_ms < 0 {
            return;
        }
        let now_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        if expire_unix_ms <= now_unix {
            // Already expired — delete key if present.
            let _ = self.delete(key);
            return;
        }
        let remaining = (expire_unix_ms - now_unix) as u64;
        // Only set if typed key still exists (skip strings).
        match self.key_type(key) {
            KeyType::None | KeyType::String => {}
            _ => {
                self.typed_expires
                    .write()
                    .insert(key.clone(), Instant::now() + Duration::from_millis(remaining));
            }
        }
    }

    /// If `key` is a typed key past its expire, delete it and return true.
    pub(super) fn purge_typed_if_expired(&self, key: &Bytes) -> bool {
        let exp = match self.typed_expires.read().get(key).copied() {
            Some(e) => e,
            None => return false,
        };
        if Instant::now() < exp {
            return false;
        }
        // Past due: remove expire first to avoid re-entry, then delete value.
        self.typed_expires.write().remove(key);
        let _ = self.delete_without_clearing_expire(key);
        true
    }

    /// Whether a typed (non-string) key is present, without purging.
    fn typed_key_present_raw(&self, key: &Bytes) -> bool {
        self.sorted_sets.contains_key(key)
            || self.geo_sets.contains_key(key)
            || self.hashes.read().contains_key(key)
            || self.lists.read().contains_key(key)
            || self.sets.read().contains_key(key)
            || self.streams.read().contains_key(key)
    }

    /// Delete any key type without touching typed_expires (caller manages expire).
    fn delete_without_clearing_expire(&self, key: &Bytes) -> Result<bool> {
        let deleted = if let Some(entry) = self.map.remove(key) {
            let size = entry.size();
            self.memory_usage.fetch_sub(size, Ordering::Relaxed);
            self.memory_tracker.deallocate(size, MemoryCategory::Cache);
            true
        } else if self.remove_sorted_set(key) {
            true
        } else if self.remove_geo_set(key) {
            true
        } else if self.remove_hash(key) {
            true
        } else if self.remove_list(key) {
            true
        } else if self.remove_set(key) {
            true
        } else if self.remove_stream(key) {
            true
        } else {
            false
        };
        if deleted {
            self.auto_remove_from_indices(key);
            self.touch_watch_key(key);
        }
        Ok(deleted)
    }

    /// Sample expired typed keys and delete them (active expire companion).
    /// Returns (keys_deleted, approx_bytes — always 0; typed accounting is in remove_*).
    pub(super) fn active_expire_typed(&self, samples: usize) -> usize {
        use rand::Rng;
        if samples == 0 {
            return 0;
        }
        let now = Instant::now();
        // Snapshot candidate keys under read lock.
        let candidates: Vec<Bytes> = {
            let g = self.typed_expires.read();
            if g.is_empty() {
                return 0;
            }
            let mut rng = rand::thread_rng();
            let len = g.len();
            let mut out = Vec::with_capacity(samples.min(len));
            let attempts = samples.saturating_mul(3).max(samples);
            for _ in 0..attempts {
                if out.len() >= samples {
                    break;
                }
                let idx = rng.gen_range(0..len);
                if let Some((k, exp)) = g.iter().nth(idx) {
                    if *exp <= now {
                        out.push(k.clone());
                    }
                }
            }
            out
        };

        let mut count = 0usize;
        for key in candidates {
            // Re-check under write path.
            let still_expired = self
                .typed_expires
                .read()
                .get(&key)
                .map(|e| *e <= Instant::now())
                .unwrap_or(false);
            if still_expired {
                self.typed_expires.write().remove(&key);
                if self.delete_without_clearing_expire(&key).unwrap_or(false) {
                    count += 1;
                    self.stats
                        .evicted_expired
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        count
    }
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
