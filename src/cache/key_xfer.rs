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
        // Prefer O(1)-ish sample from the unified map; fall back to keys() sample.
        if let Some((k, slot)) = self.key_values.get_random() {
            // Batch FQ: slot expire is SoT for all types.
            if !slot.is_expired() {
                return Some(k);
            }
            // expired — fall through to keys() sample
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
                    if let Some(entry) = self.get_string_entry(key) {
                        entry.touch(log_factor, decay);
                        n += 1;
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

// ─── DUMP/RESTORE wire formats (Batch FY + GH) ──────────────────────────────
//
// **DUMP format choice:** Redis-compatible RDB object wire for:
// - string / list / set / hash / zset (FY)
// - **geo** as ZSET_2 with geohash scores (GH; Redis GEO is a zset)
// - **stream** as type 15 + Kore `KST1` entry body (GH; Redis listpack residual)
//
// **RESTORE dual-detect:** magic `KDF1` → Kore path; else Redis RDB object
// (type + encoding + rdb_version u16 LE + crc64). See `crate::rdb_object`.
//
// KDF1 layout (still accepted on RESTORE for all types including geo/stream):
//   magic "KDF1" | type u8 | body…  (no embedded TTL; RESTORE supplies it)

const KDF_MAGIC: &[u8; 4] = b"KDF1";
const KDF_STRING: u8 = 1;
const KDF_HASH: u8 = 2;
const KDF_LIST: u8 = 3;
const KDF_SET: u8 = 4;
const KDF_ZSET: u8 = 5;
const KDF_GEO: u8 = 6;
const KDF_STREAM: u8 = 7;

impl KeyPayload {
    /// Encode for DUMP (TTL is not embedded — RESTORE applies expiry).
    ///
    /// All types emit Redis-framed wire (Batch FY + GH). Legacy KDF1 remains
    /// accepted on RESTORE via [`Self::encode_kdf1`] / dual-detect.
    pub fn encode_dump(&self) -> Vec<u8> {
        match self {
            KeyPayload::String { value, .. } => crate::rdb_object::encode_string_dump(value),
            KeyPayload::List { elements, .. } => crate::rdb_object::encode_list_dump(elements),
            KeyPayload::Set { members, .. } => crate::rdb_object::encode_set_dump(members),
            KeyPayload::Hash { fields, .. } => crate::rdb_object::encode_hash_dump(fields),
            KeyPayload::ZSet { members, .. } => crate::rdb_object::encode_zset_dump(members),
            KeyPayload::Geo { members, .. } => crate::rdb_object::encode_geo_dump(members),
            KeyPayload::Stream { state, .. } => crate::rdb_object::encode_stream_dump(state),
        }
    }

    /// Kore-native KDF1 encoding (still accepted by RESTORE for dual-detect).
    pub fn encode_kdf1(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        out.extend_from_slice(KDF_MAGIC);
        match self {
            KeyPayload::String { value, flags, .. } => {
                out.push(KDF_STRING);
                out.extend_from_slice(&flags.to_le_bytes());
                write_bytes(&mut out, value);
            }
            KeyPayload::Hash { fields, .. } => {
                out.push(KDF_HASH);
                write_u32(&mut out, fields.len() as u32);
                for (k, v) in fields {
                    write_bytes(&mut out, k);
                    write_bytes(&mut out, v);
                }
            }
            KeyPayload::List { elements, .. } => {
                out.push(KDF_LIST);
                write_u32(&mut out, elements.len() as u32);
                for e in elements {
                    write_bytes(&mut out, e);
                }
            }
            KeyPayload::Set { members, .. } => {
                out.push(KDF_SET);
                write_u32(&mut out, members.len() as u32);
                for m in members {
                    write_bytes(&mut out, m);
                }
            }
            KeyPayload::ZSet { members, .. } => {
                out.push(KDF_ZSET);
                write_u32(&mut out, members.len() as u32);
                for (m, score) in members {
                    write_bytes(&mut out, m);
                    out.extend_from_slice(&score.to_le_bytes());
                }
            }
            KeyPayload::Geo { members, .. } => {
                out.push(KDF_GEO);
                write_u32(&mut out, members.len() as u32);
                for (m, lon, lat) in members {
                    write_bytes(&mut out, m);
                    out.extend_from_slice(&lon.to_le_bytes());
                    out.extend_from_slice(&lat.to_le_bytes());
                }
            }
            KeyPayload::Stream { state, .. } => {
                out.push(KDF_STREAM);
                encode_stream(&mut out, state);
            }
        }
        out
    }

    /// Decode DUMP payload into a KeyPayload with `pttl = -1` (caller sets TTL).
    /// Accepts KDF1 or Redis RDB object wire.
    pub fn decode_dump(data: &[u8]) -> std::result::Result<KeyPayload, String> {
        if data.len() >= 4 && &data[0..4] == KDF_MAGIC {
            return Self::decode_kdf1(data);
        }
        Self::decode_redis_wire(data)
    }

    fn decode_redis_wire(data: &[u8]) -> std::result::Result<KeyPayload, String> {
        use crate::rdb_object::{decode_redis_dump, RdbObject};
        let obj = decode_redis_dump(data)?;
        Ok(match obj {
            RdbObject::String(value) => KeyPayload::String {
                value,
                flags: 0,
                pttl: -1,
            },
            RdbObject::List(elements) => KeyPayload::List {
                elements,
                pttl: -1,
            },
            RdbObject::Set(members) => KeyPayload::Set {
                members,
                pttl: -1,
            },
            RdbObject::Hash(fields) => KeyPayload::Hash {
                fields,
                pttl: -1,
            },
            RdbObject::ZSet(members) => KeyPayload::ZSet {
                members,
                pttl: -1,
            },
            RdbObject::Stream(state) => KeyPayload::Stream {
                state,
                pttl: -1,
            },
        })
    }

    fn decode_kdf1(data: &[u8]) -> std::result::Result<KeyPayload, String> {
        if data.len() < 5 || &data[0..4] != KDF_MAGIC {
            return Err("DUMP payload version or checksum are wrong".into());
        }
        let mut r = Reader { data, pos: 4 };
        let ty = r.u8()?;
        let payload = match ty {
            KDF_STRING => {
                let flags = r.u32()?;
                let value = r.bytes()?;
                KeyPayload::String {
                    value,
                    flags,
                    pttl: -1,
                }
            }
            KDF_HASH => {
                let n = r.u32()? as usize;
                let mut fields = Vec::with_capacity(n);
                for _ in 0..n {
                    let k = r.bytes()?;
                    let v = r.bytes()?;
                    fields.push((k, v));
                }
                KeyPayload::Hash { fields, pttl: -1 }
            }
            KDF_LIST => {
                let n = r.u32()? as usize;
                let mut elements = Vec::with_capacity(n);
                for _ in 0..n {
                    elements.push(r.bytes()?);
                }
                KeyPayload::List {
                    elements,
                    pttl: -1,
                }
            }
            KDF_SET => {
                let n = r.u32()? as usize;
                let mut members = Vec::with_capacity(n);
                for _ in 0..n {
                    members.push(r.bytes()?);
                }
                KeyPayload::Set {
                    members,
                    pttl: -1,
                }
            }
            KDF_ZSET => {
                let n = r.u32()? as usize;
                let mut members = Vec::with_capacity(n);
                for _ in 0..n {
                    let m = r.bytes()?;
                    let score = r.f64()?;
                    members.push((m, score));
                }
                KeyPayload::ZSet {
                    members,
                    pttl: -1,
                }
            }
            KDF_GEO => {
                let n = r.u32()? as usize;
                let mut members = Vec::with_capacity(n);
                for _ in 0..n {
                    let m = r.bytes()?;
                    let lon = r.f64()?;
                    let lat = r.f64()?;
                    members.push((m, lon, lat));
                }
                KeyPayload::Geo {
                    members,
                    pttl: -1,
                }
            }
            KDF_STREAM => {
                let state = decode_stream(&mut r)?;
                KeyPayload::Stream { state, pttl: -1 }
            }
            _ => return Err("DUMP payload version or checksum are wrong".into()),
        };
        if r.pos != r.data.len() {
            return Err("DUMP payload version or checksum are wrong".into());
        }
        Ok(payload)
    }

    /// Apply external TTL for RESTORE: `pttl_ms` remaining (or absolute when `absttl`).
    /// `0` means no expiration. Negative remaining is treated as immediate delete by caller.
    pub fn with_restore_ttl(mut self, ttl_arg: i64, absttl: bool) -> KeyPayload {
        let pttl = if ttl_arg <= 0 {
            -1
        } else if absttl {
            // Convert absolute unix ms → remaining; past times → 0 (caller deletes).
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            if ttl_arg <= now {
                0
            } else {
                ttl_arg - now
            }
        } else {
            ttl_arg
        };
        match &mut self {
            KeyPayload::String { pttl: p, .. }
            | KeyPayload::Hash { pttl: p, .. }
            | KeyPayload::List { pttl: p, .. }
            | KeyPayload::Set { pttl: p, .. }
            | KeyPayload::ZSet { pttl: p, .. }
            | KeyPayload::Geo { pttl: p, .. }
            | KeyPayload::Stream { pttl: p, .. } => *p = pttl,
        }
        self
    }
}

impl Cache {
    /// DUMP key — Redis-framed wire for all types (Batch FY + GH); None if missing.
    pub fn dump_serialized(&self, key: &Bytes) -> Option<Bytes> {
        let payload = self.dump_key(key)?;
        Some(Bytes::from(payload.encode_dump()))
    }

    /// RESTORE key from DUMP blob (Redis wire or legacy KDF1). Returns Ok(true) on success.
    /// Err string is Redis-style message (BUSYKEY / bad payload).
    pub fn restore_serialized(
        &self,
        key: &Bytes,
        data: &[u8],
        ttl_arg: i64,
        replace: bool,
        absttl: bool,
    ) -> std::result::Result<bool, String> {
        if self.exists(key) && !replace {
            return Err("BUSYKEY Target key name already exists.".into());
        }
        let payload = KeyPayload::decode_dump(data)
            .map_err(|_| "ERR DUMP payload version or checksum are wrong".to_string())?;
        let mut payload = payload.with_restore_ttl(ttl_arg, absttl);
        // pttl==0 after ABSTTL past → restore then immediately delete.
        let expire_now = payload.pttl() == 0;
        if expire_now {
            payload.set_pttl(-1);
        }
        self.restore_key(key, &payload, true)
            .map_err(|e| e.to_resp_string())?;
        if expire_now {
            let _ = self.delete(key);
        }
        Ok(true)
    }
}

impl KeyPayload {
    fn set_pttl(&mut self, pttl: i64) {
        match self {
            KeyPayload::String { pttl: p, .. }
            | KeyPayload::Hash { pttl: p, .. }
            | KeyPayload::List { pttl: p, .. }
            | KeyPayload::Set { pttl: p, .. }
            | KeyPayload::ZSet { pttl: p, .. }
            | KeyPayload::Geo { pttl: p, .. }
            | KeyPayload::Stream { pttl: p, .. } => *p = pttl,
        }
    }
}

fn write_u32(out: &mut Vec<u8>, n: u32) {
    out.extend_from_slice(&n.to_le_bytes());
}

fn write_bytes(out: &mut Vec<u8>, b: &[u8]) {
    write_u32(out, b.len() as u32);
    out.extend_from_slice(b);
}

fn encode_stream(out: &mut Vec<u8>, state: &StreamStateSnapshot) {
    write_u64(out, state.last_generated_id.ms);
    write_u64(out, state.last_generated_id.seq);
    write_u32(out, state.entries.len() as u32);
    for (id, fields) in &state.entries {
        write_u64(out, id.ms);
        write_u64(out, id.seq);
        write_u32(out, fields.len() as u32);
        for (k, v) in fields {
            write_bytes(out, k);
            write_bytes(out, v);
        }
    }
    write_u32(out, state.groups.len() as u32);
    for g in &state.groups {
        write_bytes(out, &g.name);
        write_u64(out, g.last_delivered_id.ms);
        write_u64(out, g.last_delivered_id.seq);
        write_u32(out, g.pending.len() as u32);
        for p in &g.pending {
            write_u64(out, p.id.ms);
            write_u64(out, p.id.seq);
            write_bytes(out, &p.consumer);
            write_u64(out, p.delivery_time_ms);
            write_u64(out, p.delivery_count);
        }
        write_u32(out, g.consumers.len() as u32);
        for c in &g.consumers {
            write_bytes(out, &c.name);
            write_u64(out, c.seen_time_ms);
            write_u32(out, c.pending as u32);
        }
    }
}

fn decode_stream(r: &mut Reader<'_>) -> std::result::Result<StreamStateSnapshot, String> {
    use crate::stream_type::{
        ConsumerSnapshot, GroupSnapshot, PendingEntrySnapshot, StreamId,
    };
    let last_generated_id = StreamId::new(r.u64()?, r.u64()?);
    let n_entries = r.u32()? as usize;
    let mut entries = Vec::with_capacity(n_entries);
    for _ in 0..n_entries {
        let id = StreamId::new(r.u64()?, r.u64()?);
        let nf = r.u32()? as usize;
        let mut fields = Vec::with_capacity(nf);
        for _ in 0..nf {
            fields.push((r.bytes()?, r.bytes()?));
        }
        entries.push((id, fields));
    }
    let n_groups = r.u32()? as usize;
    let mut groups = Vec::with_capacity(n_groups);
    for _ in 0..n_groups {
        let name = r.bytes()?;
        let last_delivered_id = StreamId::new(r.u64()?, r.u64()?);
        let np = r.u32()? as usize;
        let mut pending = Vec::with_capacity(np);
        for _ in 0..np {
            pending.push(PendingEntrySnapshot {
                id: StreamId::new(r.u64()?, r.u64()?),
                consumer: r.bytes()?,
                delivery_time_ms: r.u64()?,
                delivery_count: r.u64()?,
            });
        }
        let nc = r.u32()? as usize;
        let mut consumers = Vec::with_capacity(nc);
        for _ in 0..nc {
            consumers.push(ConsumerSnapshot {
                name: r.bytes()?,
                seen_time_ms: r.u64()?,
                pending: r.u32()? as usize,
            });
        }
        groups.push(GroupSnapshot {
            name,
            last_delivered_id,
            pending,
            consumers,
        });
    }
    Ok(StreamStateSnapshot {
        last_generated_id,
        entries,
        groups,
    })
}

fn write_u64(out: &mut Vec<u8>, n: u64) {
    out.extend_from_slice(&n.to_le_bytes());
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn need(&self, n: usize) -> std::result::Result<(), String> {
        if self.pos + n > self.data.len() {
            Err("DUMP payload version or checksum are wrong".into())
        } else {
            Ok(())
        }
    }
    fn u8(&mut self) -> std::result::Result<u8, String> {
        self.need(1)?;
        let v = self.data[self.pos];
        self.pos += 1;
        Ok(v)
    }
    fn u32(&mut self) -> std::result::Result<u32, String> {
        self.need(4)?;
        let v = u32::from_le_bytes(self.data[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }
    fn u64(&mut self) -> std::result::Result<u64, String> {
        self.need(8)?;
        let v = u64::from_le_bytes(self.data[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }
    fn f64(&mut self) -> std::result::Result<f64, String> {
        self.need(8)?;
        let v = f64::from_le_bytes(self.data[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }
    fn bytes(&mut self) -> std::result::Result<Bytes, String> {
        let n = self.u32()? as usize;
        self.need(n)?;
        let b = Bytes::copy_from_slice(&self.data[self.pos..self.pos + n]);
        self.pos += n;
        Ok(b)
    }
}
