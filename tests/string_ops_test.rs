//! Phase C P1: APPEND / STRLEN / SETEX / GETSET / UNLINK / RENAME / RENAMENX

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
fn append_and_strlen() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    // APPEND creates key
    assert_eq!(handle(&mut h, cmd(&["APPEND", "msg", "Hello"])), RespValue::Integer(5));
    assert_eq!(handle(&mut h, cmd(&["APPEND", "msg", " World"])), RespValue::Integer(11));
    assert_eq!(as_bulk_str(&handle(&mut h, cmd(&["GET", "msg"]))).as_deref(), Some("Hello World"));
    assert_eq!(handle(&mut h, cmd(&["STRLEN", "msg"])), RespValue::Integer(11));
    assert_eq!(handle(&mut h, cmd(&["STRLEN", "missing"])), RespValue::Integer(0));
}

#[test]
fn append_wrongtype() {
    let cache = make_cache();
    let mut h = make_handler(cache);
    assert_eq!(handle(&mut h, cmd(&["HSET", "h", "f", "v"])), RespValue::Integer(1));
    match handle(&mut h, cmd(&["APPEND", "h", "x"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).starts_with("WRONGTYPE")),
        other => panic!("expected WRONGTYPE, got {:?}", other),
    }
    match handle(&mut h, cmd(&["STRLEN", "h"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).starts_with("WRONGTYPE")),
        other => panic!("expected WRONGTYPE, got {:?}", other),
    }
}

#[test]
fn setex_and_getset() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    assert_eq!(handle(&mut h, cmd(&["SETEX", "ttlkey", "60", "v1"])), RespValue::ok());
    assert_eq!(as_bulk_str(&handle(&mut h, cmd(&["GET", "ttlkey"]))).as_deref(), Some("v1"));
    // TTL should be positive
    match handle(&mut h, cmd(&["TTL", "ttlkey"])) {
        RespValue::Integer(t) => assert!(t > 0 && t <= 60, "ttl={}", t),
        other => panic!("expected integer TTL, got {:?}", other),
    }

    let old = handle(&mut h, cmd(&["GETSET", "ttlkey", "v2"]));
    assert_eq!(as_bulk_str(&old).as_deref(), Some("v1"));
    assert_eq!(as_bulk_str(&handle(&mut h, cmd(&["GET", "ttlkey"]))).as_deref(), Some("v2"));

    // GETSET on missing key
    assert_eq!(handle(&mut h, cmd(&["GETSET", "new", "x"])), RespValue::null());
    assert_eq!(as_bulk_str(&handle(&mut h, cmd(&["GET", "new"]))).as_deref(), Some("x"));
}

#[test]
fn unlink_like_del() {
    let cache = make_cache();
    let mut h = make_handler(cache);
    handle(&mut h, cmd(&["SET", "a", "1"]));
    handle(&mut h, cmd(&["SET", "b", "2"]));
    assert_eq!(handle(&mut h, cmd(&["UNLINK", "a", "b", "c"])), RespValue::Integer(2));
    assert_eq!(handle(&mut h, cmd(&["EXISTS", "a"])), RespValue::Integer(0));
}

#[test]
fn rename_and_renamenx() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    handle(&mut h, cmd(&["SET", "old", "val"]));
    assert_eq!(handle(&mut h, cmd(&["RENAME", "old", "new"])), RespValue::ok());
    assert_eq!(handle(&mut h, cmd(&["EXISTS", "old"])), RespValue::Integer(0));
    assert_eq!(as_bulk_str(&handle(&mut h, cmd(&["GET", "new"]))).as_deref(), Some("val"));

    // RENAME missing
    match handle(&mut h, cmd(&["RENAME", "nope", "x"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("no such key")),
        other => panic!("expected error, got {:?}", other),
    }

    // RENAMENX fails when dest exists
    handle(&mut h, cmd(&["SET", "src", "s"]));
    handle(&mut h, cmd(&["SET", "dst", "d"]));
    assert_eq!(handle(&mut h, cmd(&["RENAMENX", "src", "dst"])), RespValue::Integer(0));
    assert_eq!(as_bulk_str(&handle(&mut h, cmd(&["GET", "src"]))).as_deref(), Some("s"));
    assert_eq!(as_bulk_str(&handle(&mut h, cmd(&["GET", "dst"]))).as_deref(), Some("d"));

    // RENAMENX succeeds
    assert_eq!(handle(&mut h, cmd(&["RENAMENX", "src", "dst2"])), RespValue::Integer(1));
    assert_eq!(as_bulk_str(&handle(&mut h, cmd(&["GET", "dst2"]))).as_deref(), Some("s"));
}

#[test]
fn rename_hash_and_overwrite() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    handle(&mut h, cmd(&["HSET", "h1", "f", "v"]));
    handle(&mut h, cmd(&["SET", "victim", "gone"]));
    assert_eq!(handle(&mut h, cmd(&["RENAME", "h1", "victim"])), RespValue::ok());
    assert_eq!(
        handle(&mut h, cmd(&["TYPE", "victim"])),
        RespValue::SimpleString(Bytes::from_static(b"hash"))
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["HGET", "victim", "f"]))).as_deref(),
        Some("v")
    );
}

#[test]
fn rename_same_key_ok() {
    let cache = make_cache();
    let mut h = make_handler(cache);
    handle(&mut h, cmd(&["SET", "k", "1"]));
    assert_eq!(handle(&mut h, cmd(&["RENAME", "k", "k"])), RespValue::ok());
    assert_eq!(as_bulk_str(&handle(&mut h, cmd(&["GET", "k"]))).as_deref(), Some("1"));
}

// ── Batch AL: GETRANGE / SETRANGE / MSETNX ───────────────────────────────────

#[test]
fn getrange_setrange() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["SET", "s", "Hello World"]));

    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GETRANGE", "s", "0", "4"]))).unwrap(),
        "Hello"
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GETRANGE", "s", "-5", "-1"]))).unwrap(),
        "World"
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GETRANGE", "s", "6", "100"]))).unwrap(),
        "World"
    );
    // Missing key → empty string
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GETRANGE", "missing", "0", "5"]))).unwrap(),
        ""
    );

    // Overwrite middle
    assert_eq!(
        handle(&mut h, cmd(&["SETRANGE", "s", "6", "Redis"])),
        RespValue::Integer(11)
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GET", "s"]))).unwrap(),
        "Hello Redis"
    );

    // Zero-pad create
    assert_eq!(
        handle(&mut h, cmd(&["SETRANGE", "pad", "4", "ab"])),
        RespValue::Integer(6)
    );
    let v = handle(&mut h, cmd(&["GET", "pad"]));
    match v {
        RespValue::BulkString(Some(b)) => {
            assert_eq!(&b[..], b"\0\0\0\0ab");
        }
        other => panic!("expected padded bulk, got {:?}", other),
    }
}

#[test]
fn msetnx_basic() {
    let mut h = make_handler(make_cache());
    assert_eq!(
        handle(&mut h, cmd(&["MSETNX", "a", "1", "b", "2"])),
        RespValue::Integer(1)
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GET", "a"]))).unwrap(),
        "1"
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GET", "b"]))).unwrap(),
        "2"
    );

    // Any existing key → 0, no partial overwrite of new keys
    assert_eq!(
        handle(&mut h, cmd(&["MSETNX", "a", "x", "c", "3"])),
        RespValue::Integer(0)
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GET", "a"]))).unwrap(),
        "1"
    );
    assert_eq!(
        handle(&mut h, cmd(&["EXISTS", "c"])),
        RespValue::Integer(0)
    );

    // Non-string existing key also blocks
    handle(&mut h, cmd(&["LPUSH", "lst", "x"]));
    assert_eq!(
        handle(&mut h, cmd(&["MSETNX", "lst", "v", "d", "4"])),
        RespValue::Integer(0)
    );
    assert_eq!(
        handle(&mut h, cmd(&["EXISTS", "d"])),
        RespValue::Integer(0)
    );
}

#[test]
fn getrange_wrongtype() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["LPUSH", "l", "x"]));
    match handle(&mut h, cmd(&["GETRANGE", "l", "0", "1"])) {
        RespValue::Error(e) => {
            assert!(String::from_utf8_lossy(&e).starts_with("WRONGTYPE"));
        }
        other => panic!("expected WRONGTYPE, got {:?}", other),
    }
    match handle(&mut h, cmd(&["SETRANGE", "l", "0", "y"])) {
        RespValue::Error(e) => {
            assert!(String::from_utf8_lossy(&e).starts_with("WRONGTYPE"));
        }
        other => panic!("expected WRONGTYPE, got {:?}", other),
    }
}
