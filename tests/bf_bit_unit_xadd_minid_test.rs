//! Batch BF: BITCOUNT/BITPOS BYTE|BIT, XADD NOMKSTREAM/MINID, XTRIM MINID.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::protocol::RespValue;
use kore::Cache;
use std::sync::Arc;

fn make_cache() -> Arc<Cache> {
    Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false)
}

fn make_handler(cache: Arc<Cache>) -> CommandHandler {
    let config = Config {
        host: "127.0.0.1".to_string(),
        port: 6379,
        threads: 1,
        shards: 16,
        maxmemory: 1024 * 1024 * 100,
        evict: true,
        autosweep: false,
        loadfactor: 0.75,
        maxconns: 100,
        auth: String::new(),
        maxentrysize: 500 * 1024 * 1024,
        verbosity: 0,
        enable_redlock: false,
        redlock_instances: String::new(),
        redlock_retry_count: 3,
        redlock_retry_delay_ms: 200,
        enable_fair_queue: false,
        fair_queue_max_size: 1024,
        fair_queue_cleanup_ms: 500,
        dir: "./data".to_string(),
        dbfilename: "dump.rdb".to_string(),
        appendonly: false,
        appendfilename: "appendonly.aof".to_string(),
        replicaof: String::new(),
        save: "900,1 300,10 60,10000".to_string(),
        maxmemory_policy: "allkeys-lru".to_string(),
        databases: 16,
        metrics_port: 0,
        deadlock_ui_port: 0,
        enable_deadlock_detection: false,
        deadlock_max_wait_ms: 30_000,
        deadlock_auto_resolve: false,
        deadlock_victim_strategy: "youngest".to_string(),
        tls: false,
        tls_cert: String::new(),
        tls_key: String::new(),
        aclfile: String::new(),
        cluster_enabled: false,
        unixsocket: String::new(),
            log_format: "text".to_string(),
    };
    CommandHandler::new(cache, Arc::new(config))
}

fn bulk(s: &str) -> RespValue {
    RespValue::BulkString(Some(Bytes::from(s.to_string())))
}

fn cmd(parts: &[&str]) -> RespValue {
    RespValue::Array(parts.iter().map(|p| bulk(p)).collect())
}

fn handle(handler: &mut CommandHandler, value: RespValue) -> RespValue {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async { handler.handle(value).await.unwrap() })
}

fn as_bulk_str(v: &RespValue) -> Option<String> {
    match v {
        RespValue::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    }
}

#[test]
fn bitcount_byte_and_bit_units() {
    let mut h = make_handler(make_cache());
    // Set bits 0 and 10 → byte0 has bit0, byte1 has bit2 (10 = 1*8+2)
    assert_eq!(
        handle(&mut h, cmd(&["SETBIT", "b", "0", "1"])),
        RespValue::Integer(0)
    );
    assert_eq!(
        handle(&mut h, cmd(&["SETBIT", "b", "10", "1"])),
        RespValue::Integer(0)
    );

    // Full count
    assert_eq!(
        handle(&mut h, cmd(&["BITCOUNT", "b"])),
        RespValue::Integer(2)
    );

    // BYTE unit: first byte only has 1 bit set
    assert_eq!(
        handle(&mut h, cmd(&["BITCOUNT", "b", "0", "0", "BYTE"])),
        RespValue::Integer(1)
    );

    // BIT unit: bits 0..=0 → 1; bits 0..=10 → 2; bits 1..=9 → 0
    assert_eq!(
        handle(&mut h, cmd(&["BITCOUNT", "b", "0", "0", "BIT"])),
        RespValue::Integer(1)
    );
    assert_eq!(
        handle(&mut h, cmd(&["BITCOUNT", "b", "0", "10", "BIT"])),
        RespValue::Integer(2)
    );
    assert_eq!(
        handle(&mut h, cmd(&["BITCOUNT", "b", "1", "9", "BIT"])),
        RespValue::Integer(0)
    );
}

#[test]
fn bitpos_bit_unit() {
    let mut h = make_handler(make_cache());
    assert_eq!(
        handle(&mut h, cmd(&["SETBIT", "p", "10", "1"])),
        RespValue::Integer(0)
    );

    // Default / BYTE: first set bit is absolute offset 10
    assert_eq!(
        handle(&mut h, cmd(&["BITPOS", "p", "1"])),
        RespValue::Integer(10)
    );
    assert_eq!(
        handle(&mut h, cmd(&["BITPOS", "p", "1", "0", "1", "BYTE"])),
        RespValue::Integer(10)
    );

    // BIT unit range starting after bit 10 → not found
    assert_eq!(
        handle(&mut h, cmd(&["BITPOS", "p", "1", "11", "15", "BIT"])),
        RespValue::Integer(-1)
    );
    // BIT unit including bit 10
    assert_eq!(
        handle(&mut h, cmd(&["BITPOS", "p", "1", "8", "15", "BIT"])),
        RespValue::Integer(10)
    );
}

#[test]
fn bitcount_bitpos_invalid_unit() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["SETBIT", "u", "0", "1"]));
    match handle(&mut h, cmd(&["BITCOUNT", "u", "0", "0", "WORDS"])) {
        RespValue::Error(e) => {
            let s = String::from_utf8_lossy(&e);
            assert!(s.contains("BYTE") || s.contains("syntax"), "{s}");
        }
        other => panic!("expected error, got {other:?}"),
    }
    match handle(&mut h, cmd(&["BITPOS", "u", "1", "0", "0", "WORDS"])) {
        RespValue::Error(e) => {
            let s = String::from_utf8_lossy(&e);
            assert!(s.contains("BYTE") || s.contains("syntax"), "{s}");
        }
        other => panic!("expected error, got {other:?}"),
    }
}

#[test]
fn xadd_nomkstream() {
    let mut h = make_handler(make_cache());

    // Missing key + NOMKSTREAM → null bulk
    assert_eq!(
        handle(
            &mut h,
            cmd(&["XADD", "s", "NOMKSTREAM", "*", "f", "v"])
        ),
        RespValue::null()
    );
    assert_eq!(
        handle(&mut h, cmd(&["EXISTS", "s"])),
        RespValue::Integer(0)
    );

    // Create stream, then NOMKSTREAM succeeds
    let id = handle(&mut h, cmd(&["XADD", "s", "*", "a", "1"]));
    assert!(as_bulk_str(&id).is_some());
    let id2 = handle(
        &mut h,
        cmd(&["XADD", "s", "NOMKSTREAM", "*", "b", "2"]),
    );
    assert!(as_bulk_str(&id2).is_some());
    assert_eq!(
        handle(&mut h, cmd(&["XLEN", "s"])),
        RespValue::Integer(2)
    );
}

#[test]
fn xadd_minid_trims_old_entries() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["XADD", "m", "1-0", "f", "a"]));
    handle(&mut h, cmd(&["XADD", "m", "2-0", "f", "b"]));
    handle(&mut h, cmd(&["XADD", "m", "3-0", "f", "c"]));
    assert_eq!(
        handle(&mut h, cmd(&["XLEN", "m"])),
        RespValue::Integer(3)
    );

    // MINID 2-0 drops entries with id < 2-0
    let id = handle(
        &mut h,
        cmd(&["XADD", "m", "MINID", "2-0", "4-0", "f", "d"]),
    );
    assert_eq!(as_bulk_str(&id).as_deref(), Some("4-0"));
    assert_eq!(
        handle(&mut h, cmd(&["XLEN", "m"])),
        RespValue::Integer(3)
    ); // 2-0, 3-0, 4-0

    let range = handle(&mut h, cmd(&["XRANGE", "m", "-", "+"]));
    match range {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 3);
            // Each entry is [id, [field, value, ...]]
            match &items[0] {
                RespValue::Array(entry) => {
                    assert_eq!(as_bulk_str(&entry[0]).as_deref(), Some("2-0"));
                }
                other => panic!("expected entry array, got {other:?}"),
            }
        }
        other => panic!("expected array, got {other:?}"),
    }
}

#[test]
fn xtrim_minid() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["XADD", "t", "10-0", "f", "a"]));
    handle(&mut h, cmd(&["XADD", "t", "20-0", "f", "b"]));
    handle(&mut h, cmd(&["XADD", "t", "30-0", "f", "c"]));

    assert_eq!(
        handle(&mut h, cmd(&["XTRIM", "t", "MINID", "20-0"])),
        RespValue::Integer(1)
    );
    assert_eq!(
        handle(&mut h, cmd(&["XLEN", "t"])),
        RespValue::Integer(2)
    );

    // Approximate marker accepted
    assert_eq!(
        handle(&mut h, cmd(&["XTRIM", "t", "MINID", "~", "30-0"])),
        RespValue::Integer(1)
    );
    assert_eq!(
        handle(&mut h, cmd(&["XLEN", "t"])),
        RespValue::Integer(1)
    );
}

#[test]
fn xadd_nomkstream_wrongtype() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["SET", "k", "v"]));
    match handle(
        &mut h,
        cmd(&["XADD", "k", "NOMKSTREAM", "*", "f", "v"]),
    ) {
        RespValue::Error(e) => {
            assert!(
                String::from_utf8_lossy(&e).contains("WRONGTYPE"),
                "{e:?}"
            );
        }
        other => panic!("expected WRONGTYPE, got {other:?}"),
    }
}
