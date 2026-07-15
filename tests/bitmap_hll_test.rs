//! Bitmaps + HyperLogLog command coverage.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::protocol::RespValue;
use kore::Cache;
use std::sync::Arc;

fn make_handler() -> CommandHandler {
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false);
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
        dir: "./data".to_string(),
        dbfilename: "dump.rdb".to_string(),
        appendonly: false,
        appendfilename: "appendonly.aof".to_string(),
        replicaof: String::new(),
        save: String::new(),
        maxmemory_policy: "allkeys-lru".to_string(),
        databases: 16,
        metrics_port: 0,
        tls: false,
        tls_cert: String::new(),
        tls_key: String::new(),
        aclfile: String::new(),
        cluster_enabled: false,
        unixsocket: String::new(),
    };
    CommandHandler::new(cache, Arc::new(config))
}

fn bulk(s: &str) -> RespValue {
    RespValue::BulkString(Some(Bytes::from(s.to_string())))
}

fn cmd(parts: &[&str]) -> RespValue {
    RespValue::Array(parts.iter().map(|p| bulk(p)).collect())
}

fn handle(h: &mut CommandHandler, value: RespValue) -> RespValue {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async { h.handle(value).await.unwrap() })
}

#[test]
fn setbit_getbit_bitcount() {
    let mut h = make_handler();
    assert_eq!(handle(&mut h, cmd(&["SETBIT", "b", "7", "1"])), RespValue::Integer(0));
    assert_eq!(handle(&mut h, cmd(&["GETBIT", "b", "7"])), RespValue::Integer(1));
    assert_eq!(handle(&mut h, cmd(&["GETBIT", "b", "0"])), RespValue::Integer(0));
    assert_eq!(handle(&mut h, cmd(&["BITCOUNT", "b"])), RespValue::Integer(1));
    assert_eq!(handle(&mut h, cmd(&["SETBIT", "b", "7", "1"])), RespValue::Integer(1));
}

#[test]
fn bitop_and_or_not() {
    let mut h = make_handler();
    handle(&mut h, cmd(&["SETBIT", "a", "0", "1"]));
    handle(&mut h, cmd(&["SETBIT", "a", "1", "1"]));
    handle(&mut h, cmd(&["SETBIT", "b", "1", "1"]));
    handle(&mut h, cmd(&["SETBIT", "b", "2", "1"]));

    let n = handle(&mut h, cmd(&["BITOP", "AND", "c", "a", "b"]));
    assert!(matches!(n, RespValue::Integer(x) if x >= 1));
    assert_eq!(handle(&mut h, cmd(&["GETBIT", "c", "1"])), RespValue::Integer(1));
    assert_eq!(handle(&mut h, cmd(&["GETBIT", "c", "0"])), RespValue::Integer(0));

    handle(&mut h, cmd(&["BITOP", "OR", "d", "a", "b"]));
    assert_eq!(handle(&mut h, cmd(&["GETBIT", "d", "0"])), RespValue::Integer(1));
    assert_eq!(handle(&mut h, cmd(&["GETBIT", "d", "2"])), RespValue::Integer(1));

    handle(&mut h, cmd(&["BITOP", "NOT", "e", "a"]));
    assert_eq!(handle(&mut h, cmd(&["GETBIT", "e", "0"])), RespValue::Integer(0));
}

#[test]
fn bitpos_finds_first_set_bit() {
    let mut h = make_handler();
    handle(&mut h, cmd(&["SETBIT", "p", "10", "1"]));
    assert_eq!(handle(&mut h, cmd(&["BITPOS", "p", "1"])), RespValue::Integer(10));
    // Missing key: bit 0 at offset 0
    assert_eq!(handle(&mut h, cmd(&["BITPOS", "missing", "0"])), RespValue::Integer(0));
    assert_eq!(handle(&mut h, cmd(&["BITPOS", "missing", "1"])), RespValue::Integer(-1));
}

#[test]
fn bitfield_get_set_incrby() {
    let mut h = make_handler();
    let r = handle(
        &mut h,
        cmd(&["BITFIELD", "bf", "SET", "u8", "0", "200", "GET", "u8", "0", "INCRBY", "u8", "0", "1"]),
    );
    match r {
        RespValue::Array(a) => {
            assert_eq!(a.len(), 3);
            // SET returns old value 0
            assert_eq!(a[0], RespValue::Integer(0));
            assert_eq!(a[1], RespValue::Integer(200));
            // WRAP: 200+1 = 201
            assert_eq!(a[2], RespValue::Integer(201));
        }
        other => panic!("expected array, got {:?}", other),
    }

    // OVERFLOW FAIL
    let r = handle(
        &mut h,
        cmd(&[
            "BITFIELD", "bf2", "OVERFLOW", "FAIL", "INCRBY", "u4", "0", "15", "INCRBY", "u4", "0",
            "1",
        ]),
    );
    match r {
        RespValue::Array(a) => {
            assert_eq!(a[0], RespValue::Integer(15));
            assert!(matches!(a[1], RespValue::BulkString(None) | RespValue::Null));
        }
        other => panic!("expected array, got {:?}", other),
    }
}

#[test]
fn bitmap_wrongtype() {
    let mut h = make_handler();
    handle(&mut h, cmd(&["HSET", "h", "f", "v"]));
    let r = handle(&mut h, cmd(&["SETBIT", "h", "0", "1"]));
    assert!(matches!(r, RespValue::Error(_)));
}

#[test]
fn pfadd_pfcount_pfmerge() {
    let mut h = make_handler();
    assert_eq!(
        handle(&mut h, cmd(&["PFADD", "hll", "a", "b", "c"])),
        RespValue::Integer(1)
    );
    assert_eq!(
        handle(&mut h, cmd(&["PFADD", "hll", "a"])),
        RespValue::Integer(0)
    );
    let n = handle(&mut h, cmd(&["PFCOUNT", "hll"]));
    match n {
        RespValue::Integer(x) => assert!(x >= 3 && x <= 10, "estimate={x}"),
        other => panic!("{:?}", other),
    }

    handle(&mut h, cmd(&["PFADD", "hll2", "c", "d", "e"]));
    assert_eq!(
        handle(&mut h, cmd(&["PFMERGE", "dest", "hll", "hll2"])),
        RespValue::ok()
    );
    let n = handle(&mut h, cmd(&["PFCOUNT", "dest"]));
    match n {
        RespValue::Integer(x) => assert!(x >= 4 && x <= 12, "union estimate={x}"),
        other => panic!("{:?}", other),
    }

    // Multi-key PFCOUNT
    let n = handle(&mut h, cmd(&["PFCOUNT", "hll", "hll2"]));
    match n {
        RespValue::Integer(x) => assert!(x >= 4, "union={x}"),
        other => panic!("{:?}", other),
    }
}

#[test]
fn hll_wrongtype_on_plain_string() {
    let mut h = make_handler();
    handle(&mut h, cmd(&["SET", "s", "hello"]));
    let r = handle(&mut h, cmd(&["PFADD", "s", "x"]));
    assert!(matches!(r, RespValue::Error(_)));
}
