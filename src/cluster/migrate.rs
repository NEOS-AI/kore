//! Thin slot migration: scan keys in a hash slot and move string keys over RESP.
//!
//! Operator flow (Redis-like):
//! 1. dest:  CLUSTER SETSLOT <s> IMPORTING <source-id>
//! 2. source: CLUSTER SETSLOT <s> MIGRATING <dest-id>
//! 3. source: CLUSTER MIGRATEKEYS <s> <dest-ip> <dest-port>
//! 4. both:   CLUSTER SETSLOT <s> NODE <dest-id>
//!
//! Non-string keys are skipped (MVP is string-only). Dest writes use ASKING so
//! IMPORTING slots accept the transfer.

use super::crc16::{key_hash_slot, SLOT_COUNT};
use crate::cache::{Cache, KeyType};
use crate::entry::LoadOptions;
use crate::protocol::{RespParser, RespValue};
use bytes::Bytes;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Default I/O timeout for migrate RESP commands.
const MIGRATE_IO_TIMEOUT: Duration = Duration::from_secs(5);

/// Result of migrating string keys for one slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrateSlotResult {
    /// String keys successfully moved (SET on dest + DEL on source).
    pub migrated: usize,
    /// Non-string keys found in the slot and skipped.
    pub skipped_non_string: usize,
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

/// Migrate all string keys in `slot` from `cache` to `dest_ip:dest_port` via RESP.
///
/// For each string key:
/// 1. Load value + PTTL on source
/// 2. ASKING + SET [PX <ms>] on dest
/// 3. DEL on source after successful SET
///
/// Non-string keys are counted in `skipped_non_string` and left in place.
pub async fn migrate_slot_string_keys(
    cache: &Cache,
    slot: u16,
    dest_ip: &str,
    dest_port: u16,
) -> Result<MigrateSlotResult, String> {
    if slot >= SLOT_COUNT {
        return Err(format!("ERR Invalid or out of range slot: {}", slot));
    }

    let all = keys_in_slot(cache, slot);
    let mut skipped_non_string = 0usize;
    let mut string_keys = Vec::new();
    for k in all {
        match cache.key_type(&k) {
            KeyType::String => string_keys.push(k),
            KeyType::None => {}
            _ => skipped_non_string += 1,
        }
    }

    if string_keys.is_empty() {
        return Ok(MigrateSlotResult {
            migrated: 0,
            skipped_non_string,
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
    for key in string_keys {
        // Re-check type/presence in case of concurrent delete.
        if cache.key_type(&key) != KeyType::String {
            continue;
        }
        let entry = match cache.load(
            &key,
            LoadOptions {
                touch: false,
                with_cas: false,
            },
        ) {
            Ok(Some(e)) => e,
            Ok(None) => continue,
            Err(e) => return Err(format!("ERR CLUSTER MIGRATEKEYS load failed: {}", e)),
        };
        let value = entry.value.clone();
        let pttl = entry.ttl_millis().unwrap_or(-1);

        // Dest must be IMPORTING this slot so ASKING allows the write.
        match resp_command_bytes(&mut stream, &[bulk_static(b"ASKING")], MIGRATE_IO_TIMEOUT).await
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

        let set_parts = if pttl > 0 {
            vec![
                bulk_static(b"SET"),
                RespValue::BulkString(Some(key.clone())),
                RespValue::BulkString(Some(value)),
                bulk_static(b"PX"),
                bulk_owned(pttl.to_string()),
            ]
        } else {
            vec![
                bulk_static(b"SET"),
                RespValue::BulkString(Some(key.clone())),
                RespValue::BulkString(Some(value)),
            ]
        };

        match resp_command_bytes(&mut stream, &set_parts, MIGRATE_IO_TIMEOUT).await {
            Ok(RespValue::SimpleString(s)) if s.as_ref() == b"OK" => {}
            Ok(RespValue::Error(e)) => {
                return Err(format!(
                    "ERR CLUSTER MIGRATEKEYS SET failed for key: {}",
                    String::from_utf8_lossy(&e)
                ));
            }
            Ok(other) => {
                return Err(format!(
                    "ERR CLUSTER MIGRATEKEYS unexpected SET reply: {:?}",
                    other
                ));
            }
            Err(e) => return Err(format!("ERR CLUSTER MIGRATEKEYS {}", e)),
        }

        // Only delete after dest accepted the key.
        if let Err(e) = cache.delete(&key) {
            return Err(format!(
                "ERR CLUSTER MIGRATEKEYS DEL failed after SET: {}",
                e
            ));
        }
        migrated += 1;
    }

    Ok(MigrateSlotResult {
        migrated,
        skipped_non_string,
    })
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
}
