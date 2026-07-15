//! Thin slot migration: scan keys in a hash slot and move them over RESP.
//!
//! Operator flow (Redis-like):
//! 1. dest:  CLUSTER SETSLOT <s> IMPORTING <source-id>
//! 2. source: CLUSTER SETSLOT <s> MIGRATING <dest-id>
//! 3. source: CLUSTER MIGRATEKEYS <s> <dest-ip> <dest-port>
//! 4. both:   CLUSTER SETSLOT <s> NODE <dest-id>
//!
//! Supports string, hash, list, set, zset, geo, and stream keys. Dest writes use
//! ASKING so IMPORTING slots accept the transfer. Complex types are recreated
//! with the same RESP commands as AOF rewrite (no DUMP/RESTORE).

use super::crc16::{key_hash_slot, SLOT_COUNT};
use crate::cache::{Cache, KeyType};
use crate::entry::LoadOptions;
use crate::protocol::{RespParser, RespValue};
use crate::stream_type::{StreamId, StreamStateSnapshot};
use bytes::Bytes;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Default I/O timeout for migrate RESP commands.
const MIGRATE_IO_TIMEOUT: Duration = Duration::from_secs(5);

/// Result of migrating keys for one slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrateSlotResult {
    /// Keys successfully moved (dest recreate + source DEL).
    pub migrated: usize,
    /// Keys skipped (gone mid-flight, empty, or unsupported).
    pub skipped: usize,
}

/// Back-compat alias field name used by older call sites.
impl MigrateSlotResult {
    /// Previously counted non-string keys left behind; now always 0 (all types migrate).
    #[deprecated(note = "use skipped; multi-type migrate no longer leaves non-string keys")]
    pub fn skipped_non_string(&self) -> usize {
        self.skipped
    }
}

/// Snapshot of one key ready for RESP recreate on the destination.
enum KeySnapshot {
    String {
        value: Bytes,
        pttl: i64,
    },
    Hash(Vec<(Bytes, Bytes)>),
    List(Vec<Bytes>),
    Set(Vec<Bytes>),
    ZSet(Vec<(Bytes, f64)>),
    Geo(Vec<(Bytes, f64, f64)>),
    Stream(StreamStateSnapshot),
}

/// Return all non-expired keys currently stored whose hash slot equals `slot`.
///
/// Covers all key types (string, hash, list, …). Order is not guaranteed.
pub fn keys_in_slot(cache: &Cache, slot: u16) -> Vec<Bytes> {
    if slot >= SLOT_COUNT {
        return Vec::new();
    }
    cache
        .keys(None)
        .into_iter()
        .filter(|k| key_hash_slot(k) == slot)
        .collect()
}

/// String keys only in `slot` (non-expired).
pub fn string_keys_in_slot(cache: &Cache, slot: u16) -> Vec<Bytes> {
    if slot >= SLOT_COUNT {
        return Vec::new();
    }
    cache
        .map_keys_all()
        .into_iter()
        .filter(|k| key_hash_slot(k) == slot)
        .collect()
}

/// Snapshot a single key for migration. Returns `None` if the key is gone or empty.
fn snapshot_key(cache: &Cache, key: &Bytes) -> Option<KeySnapshot> {
    match cache.key_type(key) {
        KeyType::None => None,
        KeyType::String => {
            let entry = cache
                .load(
                    key,
                    LoadOptions {
                        touch: false,
                        with_cas: false,
                    },
                )
                .ok()
                .flatten()?;
            Some(KeySnapshot::String {
                value: entry.value.clone(),
                pttl: entry.ttl_millis().unwrap_or(-1),
            })
        }
        KeyType::Hash => {
            let h = cache.get_hash(key)?;
            let fields: Vec<_> = h.read().iter_fields().collect();
            if fields.is_empty() {
                return None;
            }
            Some(KeySnapshot::Hash(fields))
        }
        KeyType::List => {
            let l = cache.get_list(key)?;
            let items: Vec<_> = l.read().iter_items().collect();
            if items.is_empty() {
                return None;
            }
            Some(KeySnapshot::List(items))
        }
        KeyType::Set => {
            let s = cache.get_set(key)?;
            let members: Vec<_> = s.read().iter_members().collect();
            if members.is_empty() {
                return None;
            }
            Some(KeySnapshot::Set(members))
        }
        KeyType::ZSet => {
            let z = cache.get_sorted_set(key)?;
            let members: Vec<_> = z.read().iter_members().collect();
            if members.is_empty() {
                return None;
            }
            Some(KeySnapshot::ZSet(members))
        }
        KeyType::Geo => {
            let g = cache.get_geo_set(key)?;
            let members: Vec<_> = g.read().iter_members().collect();
            if members.is_empty() {
                return None;
            }
            Some(KeySnapshot::Geo(members))
        }
        KeyType::Stream => {
            let s = cache.get_stream(key)?;
            let state = s.read().export_state();
            // Allow empty streams when groups exist (Redis keeps them).
            if state.entries.is_empty() && state.groups.is_empty() {
                return None;
            }
            Some(KeySnapshot::Stream(state))
        }
    }
}

/// Build the sequence of RESP command arrays needed to recreate `snap` at `key`.
fn recreate_commands(key: &Bytes, snap: &KeySnapshot) -> Vec<Vec<RespValue>> {
    match snap {
        KeySnapshot::String { value, pttl } => {
            let mut parts = vec![
                bulk_static(b"SET"),
                RespValue::BulkString(Some(key.clone())),
                RespValue::BulkString(Some(value.clone())),
            ];
            if *pttl > 0 {
                parts.push(bulk_static(b"PX"));
                parts.push(bulk_owned(pttl.to_string()));
            }
            vec![parts]
        }
        KeySnapshot::Hash(fields) => {
            let mut parts = vec![
                bulk_static(b"HSET"),
                RespValue::BulkString(Some(key.clone())),
            ];
            for (f, v) in fields {
                parts.push(RespValue::BulkString(Some(f.clone())));
                parts.push(RespValue::BulkString(Some(v.clone())));
            }
            vec![parts]
        }
        KeySnapshot::List(items) => {
            let mut parts = vec![
                bulk_static(b"RPUSH"),
                RespValue::BulkString(Some(key.clone())),
            ];
            for e in items {
                parts.push(RespValue::BulkString(Some(e.clone())));
            }
            vec![parts]
        }
        KeySnapshot::Set(members) => {
            let mut parts = vec![
                bulk_static(b"SADD"),
                RespValue::BulkString(Some(key.clone())),
            ];
            for m in members {
                parts.push(RespValue::BulkString(Some(m.clone())));
            }
            vec![parts]
        }
        KeySnapshot::ZSet(members) => {
            let mut parts = vec![
                bulk_static(b"ZADD"),
                RespValue::BulkString(Some(key.clone())),
            ];
            for (m, score) in members {
                parts.push(bulk_owned(score_string(*score)));
                parts.push(RespValue::BulkString(Some(m.clone())));
            }
            vec![parts]
        }
        KeySnapshot::Geo(members) => {
            let mut parts = vec![
                bulk_static(b"GEOADD"),
                RespValue::BulkString(Some(key.clone())),
            ];
            for (m, lon, lat) in members {
                parts.push(bulk_owned(score_string(*lon)));
                parts.push(bulk_owned(score_string(*lat)));
                parts.push(RespValue::BulkString(Some(m.clone())));
            }
            vec![parts]
        }
        KeySnapshot::Stream(state) => stream_recreate_commands(key, state),
    }
}

fn stream_recreate_commands(key: &Bytes, state: &StreamStateSnapshot) -> Vec<Vec<RespValue>> {
    let mut cmds = Vec::new();
    for (id, fields) in &state.entries {
        let mut parts = vec![
            bulk_static(b"XADD"),
            RespValue::BulkString(Some(key.clone())),
            bulk_owned(id.to_string_id()),
        ];
        for (f, v) in fields {
            parts.push(RespValue::BulkString(Some(f.clone())));
            parts.push(RespValue::BulkString(Some(v.clone())));
        }
        if parts.len() > 3 {
            cmds.push(parts);
        }
    }
    let max_entry = state
        .entries
        .iter()
        .map(|(id, _)| *id)
        .max()
        .unwrap_or(StreamId::ZERO);
    if state.last_generated_id > max_entry {
        cmds.push(vec![
            bulk_static(b"XSETID"),
            RespValue::BulkString(Some(key.clone())),
            bulk_owned(state.last_generated_id.to_string_id()),
        ]);
    }
    for g in &state.groups {
        cmds.push(vec![
            bulk_static(b"XGROUP"),
            bulk_static(b"CREATE"),
            RespValue::BulkString(Some(key.clone())),
            RespValue::BulkString(Some(g.name.clone())),
            bulk_owned(g.last_delivered_id.to_string_id()),
            bulk_static(b"MKSTREAM"),
        ]);
        cmds.push(vec![
            bulk_static(b"XGROUP"),
            bulk_static(b"SETID"),
            RespValue::BulkString(Some(key.clone())),
            RespValue::BulkString(Some(g.name.clone())),
            bulk_owned(g.last_delivered_id.to_string_id()),
        ]);
        for pe in &g.pending {
            cmds.push(vec![
                bulk_static(b"XCLAIM"),
                RespValue::BulkString(Some(key.clone())),
                RespValue::BulkString(Some(g.name.clone())),
                RespValue::BulkString(Some(pe.consumer.clone())),
                bulk_static(b"0"),
                bulk_owned(pe.id.to_string_id()),
                bulk_static(b"FORCE"),
                bulk_static(b"TIME"),
                bulk_owned(pe.delivery_time_ms.to_string()),
                bulk_static(b"RETRYCOUNT"),
                bulk_owned(pe.delivery_count.max(1).to_string()),
            ]);
        }
    }
    cmds
}

fn score_string(v: f64) -> String {
    // Compact Redis-friendly float formatting
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{}", v)
    }
}

/// Expect a successful write reply (OK / integer ≥ 0 / bulk id / array).
fn accept_write_reply(reply: &RespValue, cmd: &str) -> Result<(), String> {
    match reply {
        RespValue::SimpleString(s) if s.as_ref() == b"OK" => Ok(()),
        RespValue::Integer(n) if *n >= 0 => Ok(()),
        // XADD returns the entry id as a bulk string
        RespValue::BulkString(Some(_)) => Ok(()),
        // XCLAIM returns array of claimed entries
        RespValue::Array(_) => Ok(()),
        RespValue::Error(e) => Err(format!(
            "ERR CLUSTER MIGRATEKEYS {} failed: {}",
            cmd,
            String::from_utf8_lossy(e)
        )),
        other => Err(format!(
            "ERR CLUSTER MIGRATEKEYS unexpected {} reply: {:?}",
            cmd, other
        )),
    }
}

/// Migrate all keys in `slot` from `cache` to `dest_ip:dest_port` via RESP.
///
/// For each key:
/// 1. Snapshot local value (all types)
/// 2. ASKING + recreate command(s) on dest
/// 3. DEL on source after successful recreate
pub async fn migrate_slot_keys(
    cache: &Cache,
    slot: u16,
    dest_ip: &str,
    dest_port: u16,
) -> Result<MigrateSlotResult, String> {
    if slot >= SLOT_COUNT {
        return Err(format!("ERR Invalid or out of range slot: {}", slot));
    }

    let keys = keys_in_slot(cache, slot);
    if keys.is_empty() {
        return Ok(MigrateSlotResult {
            migrated: 0,
            skipped: 0,
        });
    }

    let addr = format!("{}:{}", dest_ip, dest_port);
    let mut stream = match tokio::time::timeout(MIGRATE_IO_TIMEOUT, TcpStream::connect(&addr)).await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            return Err(format!(
                "ERR CLUSTER MIGRATEKEYS unable to connect to {}: {}",
                addr, e
            ))
        }
        Err(_) => {
            return Err(format!(
                "ERR CLUSTER MIGRATEKEYS timed out connecting to {}",
                addr
            ))
        }
    };
    let _ = stream.set_nodelay(true);

    let mut migrated = 0usize;
    let mut skipped = 0usize;

    for key in keys {
        let snap = match snapshot_key(cache, &key) {
            Some(s) => s,
            None => {
                skipped += 1;
                continue;
            }
        };
        let cmds = recreate_commands(&key, &snap);
        if cmds.is_empty() {
            skipped += 1;
            continue;
        }

        // ASKING is one-shot per following command — re-issue before every write.
        for (i, parts) in cmds.iter().enumerate() {
            match resp_command_bytes(&mut stream, &[bulk_static(b"ASKING")], MIGRATE_IO_TIMEOUT)
                .await
            {
                Ok(RespValue::SimpleString(s)) if s.as_ref() == b"OK" => {}
                Ok(RespValue::Error(e)) => {
                    return Err(format!(
                        "ERR CLUSTER MIGRATEKEYS ASKING failed: {}",
                        String::from_utf8_lossy(&e)
                    ));
                }
                Ok(other) => {
                    return Err(format!(
                        "ERR CLUSTER MIGRATEKEYS unexpected ASKING reply: {:?}",
                        other
                    ));
                }
                Err(e) => return Err(format!("ERR CLUSTER MIGRATEKEYS {}", e)),
            }

            let cmd_name = parts
                .first()
                .and_then(|p| p.as_bulk_string())
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .unwrap_or_else(|| "CMD".into());
            match resp_command_bytes(&mut stream, parts, MIGRATE_IO_TIMEOUT).await {
                Ok(reply) => {
                    if let Err(e) = accept_write_reply(&reply, &cmd_name) {
                        return Err(format!(
                            "{} (key={}, step={})",
                            e,
                            String::from_utf8_lossy(&key),
                            i
                        ));
                    }
                }
                Err(e) => {
                    return Err(format!(
                        "ERR CLUSTER MIGRATEKEYS {} I/O for key {}: {}",
                        cmd_name,
                        String::from_utf8_lossy(&key),
                        e
                    ));
                }
            }
        }

        // Only delete after dest accepted the full key recreate sequence.
        if let Err(e) = cache.delete(&key) {
            return Err(format!(
                "ERR CLUSTER MIGRATEKEYS DEL failed after migrate: {}",
                e
            ));
        }
        migrated += 1;
    }

    Ok(MigrateSlotResult { migrated, skipped })
}

/// Back-compat name: now migrates **all** key types (not only strings).
pub async fn migrate_slot_string_keys(
    cache: &Cache,
    slot: u16,
    dest_ip: &str,
    dest_port: u16,
) -> Result<MigrateSlotResult, String> {
    migrate_slot_keys(cache, slot, dest_ip, dest_port).await
}

fn bulk_static(s: &'static [u8]) -> RespValue {
    RespValue::BulkString(Some(Bytes::from_static(s)))
}

fn bulk_owned(s: String) -> RespValue {
    RespValue::BulkString(Some(Bytes::from(s)))
}

async fn resp_command_bytes(
    stream: &mut TcpStream,
    parts: &[RespValue],
    timeout: Duration,
) -> Result<RespValue, String> {
    let payload = RespValue::Array(parts.to_vec()).serialize();

    match tokio::time::timeout(timeout, stream.write_all(&payload)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(format!("write error: {}", e)),
        Err(_) => return Err("write timed out".into()),
    }

    let mut parser = RespParser::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        if let Some(val) = parser
            .parse()
            .map_err(|e| format!("parse error: {}", e))?
        {
            return Ok(val);
        }
        let n = match tokio::time::timeout(timeout, stream.read(&mut buf)).await {
            Ok(Ok(0)) => return Err("connection closed".into()),
            Ok(Ok(n)) => n,
            Ok(Err(e)) => return Err(format!("read error: {}", e)),
            Err(_) => return Err("read timed out".into()),
        };
        parser.feed(&buf[..n]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::StoreOptions;

    #[test]
    fn keys_in_slot_filters_by_crc16() {
        let cache = Cache::new_with_sweep(4, 1024 * 1024, 1024 * 1024, false);
        // "foo" → slot 12182
        let opts = StoreOptions::default();
        cache
            .store(Bytes::from_static(b"foo"), Bytes::from_static(b"1"), opts.clone())
            .unwrap();
        cache
            .store(Bytes::from_static(b"bar"), Bytes::from_static(b"2"), opts)
            .unwrap();

        let slot_foo = key_hash_slot(b"foo");
        let in_foo = keys_in_slot(&cache, slot_foo);
        assert!(in_foo.iter().any(|k| k.as_ref() == b"foo"));
        assert!(!in_foo.iter().any(|k| k.as_ref() == b"bar"));
    }

    #[test]
    fn snapshot_hash_and_list() {
        let cache = Cache::new_with_sweep(4, 1024 * 1024, 1024 * 1024, false);
        let k = Bytes::from_static(b"{t}.h");
        let h = cache.get_or_create_hash(&k).unwrap();
        h.write()
            .hset(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
        match snapshot_key(&cache, &k).unwrap() {
            KeySnapshot::Hash(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].0.as_ref(), b"f");
            }
            _ => panic!("expected hash"),
        }

        let kl = Bytes::from_static(b"{t}.l");
        let l = cache.get_or_create_list(&kl).unwrap();
        l.write().rpush([Bytes::from_static(b"a")]);
        l.write().rpush([Bytes::from_static(b"b")]);
        match snapshot_key(&cache, &kl).unwrap() {
            KeySnapshot::List(items) => assert_eq!(items.len(), 2),
            _ => panic!("expected list"),
        }
    }
}
