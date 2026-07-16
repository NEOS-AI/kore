//! Key dump / restore for MOVE, COPY, and similar transfers.

use crate::entry::{LoadOptions, StoreOptions};
use crate::error::{Error, Result};
use crate::stream_type::StreamStateSnapshot;
use bytes::Bytes;
use std::sync::atomic::Ordering;

use super::storage::KeyType;
use super::Cache;

/// Serializable snapshot of one key (value + remaining TTL).
#[derive(Clone)]
pub enum KeyPayload {
    String {
        value: Bytes,
        flags: u32,
        /// Remaining TTL ms; -1 = none.
        pttl: i64,
    },
    Hash {
        fields: Vec<(Bytes, Bytes)>,
        pttl: i64,
    },
    List {
        elements: Vec<Bytes>,
        pttl: i64,
    },
    Set {
        members: Vec<Bytes>,
        pttl: i64,
    },
    ZSet {
        members: Vec<(Bytes, f64)>,
        pttl: i64,
    },
    Geo {
        members: Vec<(Bytes, f64, f64)>,
        pttl: i64,
    },
    Stream {
        state: StreamStateSnapshot,
        pttl: i64,
    },
}

impl KeyPayload {
    fn pttl(&self) -> i64 {
        match self {
            KeyPayload::String { pttl, .. }
            | KeyPayload::Hash { pttl, .. }
            | KeyPayload::List { pttl, .. }
            | KeyPayload::Set { pttl, .. }
            | KeyPayload::ZSet { pttl, .. }
            | KeyPayload::Geo { pttl, .. }
            | KeyPayload::Stream { pttl, .. } => *pttl,
        }
    }
}

impl Cache {
    /// Snapshot a key for COPY/MOVE. Returns `None` if missing.
    pub fn dump_key(&self, key: &Bytes) -> Option<KeyPayload> {
        let pttl = self.ttl(key);
        // ttl returns -2 if missing
        if pttl == -2 {
            return None;
        }
        match self.key_type(key) {
            KeyType::None => None,
            KeyType::String => {
                let entry = self
                    .load(
                        key,
                        LoadOptions {
                            touch: false,
                            with_cas: false,
                        },
                    )
                    .ok()
                    .flatten()?;
                Some(KeyPayload::String {
                    value: entry.value.clone(),
                    flags: entry.flags,
                    pttl,
                })
            }
            KeyType::Hash => {
                let h = self.get_hash(key)?;
                let fields: Vec<_> = h.read().iter_fields().collect();
                Some(KeyPayload::Hash { fields, pttl })
            }
            KeyType::List => {
                let l = self.get_list(key)?;
                let elements: Vec<_> = l.read().iter_items().collect();
                Some(KeyPayload::List { elements, pttl })
            }
            KeyType::Set => {
                let s = self.get_set(key)?;
                let members: Vec<_> = s.read().iter_members().collect();
                Some(KeyPayload::Set { members, pttl })
            }
            KeyType::ZSet => {
                let z = self.get_sorted_set(key)?;
                let members: Vec<_> = z.read().iter_members().collect();
                Some(KeyPayload::ZSet { members, pttl })
            }
            KeyType::Geo => {
                let g = self.get_geo_set(key)?;
                let members: Vec<_> = g.read().iter_members().collect();
                Some(KeyPayload::Geo { members, pttl })
            }
            KeyType::Stream => {
                let s = self.get_stream(key)?;
                let state = s.read().export_state();
                Some(KeyPayload::Stream { state, pttl })
            }
        }
    }

    /// Materialize a dumped key. If `replace` is false and dest exists, returns `Ok(false)`.
    /// On success returns `Ok(true)`.
    pub fn restore_key(&self, key: &Bytes, payload: &KeyPayload, replace: bool) -> Result<bool> {
        if self.exists(key) {
            if !replace {
                return Ok(false);
            }
            let _ = self.delete(key);
        }

        match payload {
            KeyPayload::String { value, flags, pttl } => {
                let mut opts = StoreOptions::default();
                opts.flags = *flags;
                if *pttl > 0 {
                    opts.ttl_ms = Some(*pttl as u64);
                }
                self.store(key.clone(), value.clone(), opts)?;
            }
            KeyPayload::Hash { fields, pttl } => {
                let h = self.get_or_create_hash(key)?;
                {
                    let mut guard = h.write();
                    let old = crate::memory::estimate_keyed_object(key.len(), guard.memory_size());
                    for (f, v) in fields {
                        guard.hset(f.clone(), v.clone());
                    }
                    let new = crate::memory::estimate_keyed_object(key.len(), guard.memory_size());
                    drop(guard);
                    self.account_hash_delta(old, new)?;
                }
                if *pttl > 0 {
                    let _ = self.expire(key, *pttl as u64);
                }
            }
            KeyPayload::List { elements, pttl } => {
                let l = self.get_or_create_list(key)?;
                {
                    let mut guard = l.write();
                    let old = crate::memory::estimate_keyed_object(key.len(), guard.memory_size());
                    guard.rpush(elements.iter().cloned());
                    let new = crate::memory::estimate_keyed_object(key.len(), guard.memory_size());
                    drop(guard);
                    self.account_list_delta(old, new);
                }
                if *pttl > 0 {
                    let _ = self.expire(key, *pttl as u64);
                }
            }
            KeyPayload::Set { members, pttl } => {
                let s = self.get_or_create_set(key)?;
                {
                    let mut guard = s.write();
                    let old = crate::memory::estimate_keyed_object(key.len(), guard.memory_size());
                    guard.sadd(members.iter().cloned());
                    let new = crate::memory::estimate_keyed_object(key.len(), guard.memory_size());
                    drop(guard);
                    self.account_set_delta(old, new);
                }
                if *pttl > 0 {
                    let _ = self.expire(key, *pttl as u64);
                }
            }
            KeyPayload::ZSet { members, pttl } => {
                let z = self.get_or_create_sorted_set(key)?;
                {
                    let mut guard = z.write();
                    let old = crate::memory::estimate_keyed_object(key.len(), guard.memory_size());
                    for (m, score) in members {
                        guard.add(m.clone(), *score);
                    }
                    let new = crate::memory::estimate_keyed_object(key.len(), guard.memory_size());
                    drop(guard);
                    self.account_sorted_set_delta(old, new);
                }
                if *pttl > 0 {
                    let _ = self.expire(key, *pttl as u64);
                }
            }
            KeyPayload::Geo { members, pttl } => {
                let g = self.get_or_create_geo_set(key)?;
                {
                    let mut guard = g.write();
                    let old = crate::memory::estimate_keyed_object(key.len(), guard.memory_usage());
                    for (m, lon, lat) in members {
                        let _ = guard.add(m.clone(), *lon, *lat);
                    }
                    let new = crate::memory::estimate_keyed_object(key.len(), guard.memory_usage());
                    drop(guard);
                    self.account_geo_set_delta(old, new);
                }
                if *pttl > 0 {
                    let _ = self.expire(key, *pttl as u64);
                }
            }
            KeyPayload::Stream { state, pttl } => {
                self.import_stream(key.clone(), state.clone())?;
                if *pttl > 0 {
                    let _ = self.expire(key, *pttl as u64);
                }
            }
        }
        Ok(true)
    }

    /// COPY `src` → `dst` within this cache (or to `dst_cache` if provided).
    /// Returns true if a new key was created.
    pub fn copy_key(
        &self,
        src: &Bytes,
        dst: &Bytes,
        dst_cache: Option<&Cache>,
        replace: bool,
    ) -> Result<bool> {
        let Some(payload) = self.dump_key(src) else {
            return Ok(false);
        };
        let target = dst_cache.unwrap_or(self);
        // Same key in same cache: Redis returns error for COPY same key... actually
        // COPY a a returns 0 if no REPLACE? Redis: "ERR source and destination objects are the same"
        if std::ptr::eq(self as *const Cache, target as *const Cache) && src == dst {
            return Err(Error::InvalidArgument(
                "source and destination objects are the same".into(),
            ));
        }
        target.restore_key(dst, &payload, replace)
    }

    /// MOVE key to another database cache. Returns true if moved.
    pub fn move_key_to(&self, key: &Bytes, dst: &Cache) -> Result<bool> {
        if std::ptr::eq(self as *const Cache, dst as *const Cache) {
            return Err(Error::InvalidArgument(
                "source and destination objects are the same".into(),
            ));
        }
        let Some(payload) = self.dump_key(key) else {
            return Ok(false);
        };
        // Destination already has key → fail, leave source intact.
        if dst.exists(key) {
            return Ok(false);
        }
        dst.restore_key(key, &payload, false)?;
        // Remove source (including expire / search index).
        let _ = self.delete(key);
        // silence unused warning on pttl helper
        let _ = payload.pttl();
        Ok(true)
    }

    /// Return a random existing key, or None if the DB is empty.
    pub fn random_key(&self) -> Option<Bytes> {
        // Prefer string map random (O(1)-ish); fall back to keys() sample.
        if let Some((k, e)) = self.map.get_random() {
            if !e.is_expired() {
                return Some(k);
            }
        }
        let all = self.keys(None);
        if all.is_empty() {
            return None;
        }
        use rand::Rng;
        let idx = rand::thread_rng().gen_range(0..all.len());
        Some(all[idx].clone())
    }

    /// TOUCH: update last-access for existing keys; returns how many existed.
    pub fn touch_keys(&self, keys: &[Bytes]) -> usize {
        let log_factor = self.lfu_log_factor.load(Ordering::Relaxed);
        let decay = self.lfu_decay_time.load(Ordering::Relaxed);
        let mut n = 0usize;
        for key in keys {
            match self.key_type(key) {
                KeyType::None => {}
                KeyType::String => {
                    if let Some(entry) = self.map.get(key) {
                        if !entry.is_expired() {
                            entry.touch(log_factor, decay);
                            n += 1;
                        }
                    }
                }
                // Typed keys: existence counts (no LRU metadata yet).
                _ => {
                    n += 1;
                }
            }
        }
        n
    }
}
