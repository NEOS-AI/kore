use crate::error::Result;
use crate::hashmap::MapAction;
use bytes::Bytes;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::keyspace::KeySlot;
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
            KeyType::String => match self.get_string_entry(key) {
                Some(entry) => {
                    let mut new_entry = (*entry).clone();
                    new_entry.expires_at = Some(Instant::now() + Duration::from_millis(ttl_ms));
                    self.key_values
                        .insert(key.clone(), KeySlot::string(Arc::new(new_entry)));
                    Ok(true)
                }
                None => Ok(false),
            },
            _ => {
                // Batch FP: typed expire lives on KeySlot.
                let set = self.key_values.mutate(key, |cur, _| {
                    match cur {
                        Some(slot) if slot.value.is_typed_container() => {
                            let mut s = slot.clone();
                            s.expires_at =
                                Some(Instant::now() + Duration::from_millis(ttl_ms));
                            (MapAction::Set(s), true)
                        }
                        _ => (MapAction::Keep, false),
                    }
                });
                Ok(set)
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
            KeyType::String => match self.get_string_entry(key) {
                Some(entry) if entry.expires_at.is_some() => {
                    let mut new_entry = (*entry).clone();
                    new_entry.expires_at = None;
                    self.key_values
                        .insert(key.clone(), KeySlot::string(Arc::new(new_entry)));
                    true
                }
                _ => false,
            },
            _ => self.key_values.mutate(key, |cur, _| {
                match cur {
                    Some(slot) if slot.expires_at.is_some() && slot.value.is_typed_container() => {
                        let mut s = slot.clone();
                        s.expires_at = None;
                        (MapAction::Set(s), true)
                    }
                    _ => (MapAction::Keep, false),
                }
            }),
        }
    }

    /// Get TTL in milliseconds (-1 = no expiration, -2 = expired/not found).
    pub fn ttl(&self, key: &Bytes) -> i64 {
        // String path first (includes lazy string expire via is_expired).
        if let Some(entry) = self.get_string_entry(key) {
            return entry.ttl_millis().unwrap_or(-1);
        }

        // Typed: purge if past due, then report remaining or -1.
        if self.purge_typed_if_expired(key) {
            return -2;
        }

        match self.key_values.get(key) {
            Some(slot) if slot.value.is_typed_container() => match slot.expires_at {
                Some(exp) => {
                    let now = Instant::now();
                    if exp <= now {
                        // Race: treat as gone; next access purges.
                        0
                    } else {
                        exp.duration_since(now).as_millis() as i64
                    }
                }
                None => -1,
            },
            _ => -2,
        }
    }

    /// Whether a typed key should be included in persistence export.
    ///
    /// Returns false when the key has a past expire on its slot (would be
    /// revived without TTL if exported). Keys with no expire record are live.
    pub(super) fn typed_key_exportable(&self, key: &Bytes) -> bool {
        match self.key_values.get(key) {
            Some(slot) if slot.value.is_typed_container() => match slot.expires_at {
                Some(exp) if exp <= Instant::now() => false,
                _ => true,
            },
            _ => true,
        }
    }

    /// Export typed expires as absolute Unix ms for persistence (RDB section).
    pub fn export_typed_expires_unix_ms(&self) -> Vec<(Bytes, i64)> {
        let now_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let now = Instant::now();
        let mut out = Vec::new();
        self.key_values.for_each(|k, slot| {
            if !slot.value.is_typed_container() {
                return;
            }
            let Some(exp) = slot.expires_at else {
                return;
            };
            if exp <= now {
                return;
            }
            let remaining_ms = exp.duration_since(now).as_millis() as i64;
            out.push((k.clone(), now_unix + remaining_ms));
        });
        out
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
        let _ = self.key_values.mutate(key, |cur, _| {
            match cur {
                Some(slot) if slot.value.is_typed_container() => {
                    let mut s = slot.clone();
                    s.expires_at = Some(Instant::now() + Duration::from_millis(remaining));
                    (MapAction::Set(s), ())
                }
                _ => (MapAction::Keep, ()),
            }
        });
    }

    /// If `key` is a typed key past its expire, delete it and return true.
    pub(super) fn purge_typed_if_expired(&self, key: &Bytes) -> bool {
        let exp = match self.key_values.get(key) {
            Some(slot) if slot.value.is_typed_container() => match slot.expires_at {
                Some(e) => e,
                None => return false,
            },
            _ => return false,
        };
        if Instant::now() < exp {
            return false;
        }
        // Past due: remove the whole slot (value + expire).
        let _ = self.delete_without_clearing_expire(key);
        true
    }

    /// Delete any key type; expire is on the slot and drops with remove.
    fn delete_without_clearing_expire(&self, key: &Bytes) -> Result<bool> {
        let deleted = self.remove_key_value_raw(key);
        if deleted {
            self.auto_remove_from_indices(key);
            self.touch_watch_key(key);
        }
        Ok(deleted)
    }

    /// Sample expired typed keys and delete them (active expire companion).
    /// Returns keys deleted count (typed accounting is in remove_*).
    pub(super) fn active_expire_typed(&self, samples: usize) -> usize {
        if samples == 0 {
            return 0;
        }
        let now = Instant::now();
        // Sample random slots; collect keys whose typed expire is past due.
        let mut candidates = Vec::with_capacity(samples);
        let attempts = samples.saturating_mul(5).max(samples);
        for _ in 0..attempts {
            if candidates.len() >= samples {
                break;
            }
            let Some((k, slot)) = self.key_values.get_random() else {
                break;
            };
            if !slot.value.is_typed_container() {
                continue;
            }
            if let Some(exp) = slot.expires_at {
                if exp <= now {
                    candidates.push(k);
                }
            }
        }

        let mut count = 0usize;
        for key in candidates {
            // Re-check under write path.
            let still_expired = match self.key_values.get(&key) {
                Some(slot) if slot.value.is_typed_container() => {
                    slot.expires_at.map(|e| e <= Instant::now()).unwrap_or(false)
                }
                _ => false,
            };
            if still_expired {
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
