//! Batch AO: HINCRBYFLOAT / HSTRLEN / HMSET.

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
            cluster_replica_priority: 100,
            cluster_require_full_coverage: true,
            cluster_allow_reads_when_down: false,
            cluster_announce_ip: String::new(),
            cluster_announce_port: 0,
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
fn test_hincrbyfloat() {
    let mut h = make_handler(make_cache());

    let resp = handle(&mut h, cmd(&["HINCRBYFLOAT", "hk", "f", "1.5"]));
    assert_eq!(as_bulk_str(&resp).as_deref(), Some("1.5"));

    let resp = handle(&mut h, cmd(&["HINCRBYFLOAT", "hk", "f", "0.5"]));
    assert_eq!(as_bulk_str(&resp).as_deref(), Some("2"));

    // Stored value readable via HGET
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["HGET", "hk", "f"]))).as_deref(),
        Some("2")
    );

    // Non-float field
    handle(&mut h, cmd(&["HSET", "hk", "s", "hello"]));
    match handle(&mut h, cmd(&["HINCRBYFLOAT", "hk", "s", "1"])) {
        RespValue::Error(e) => {
            assert!(String::from_utf8_lossy(&e).contains("float") || String::from_utf8_lossy(&e).contains("not"));
        }
        other => panic!("{:?}", other),
    }

    // Wrong type key
    handle(&mut h, cmd(&["SET", "str", "x"]));
    match handle(&mut h, cmd(&["HINCRBYFLOAT", "str", "f", "1"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("WRONGTYPE")),
        other => panic!("{:?}", other),
    }
}

#[test]
fn test_hstrlen() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["HSET", "hk", "name", "alice", "empty", ""]));

    assert_eq!(
        handle(&mut h, cmd(&["HSTRLEN", "hk", "name"])),
        RespValue::Integer(5)
    );
    assert_eq!(
        handle(&mut h, cmd(&["HSTRLEN", "hk", "empty"])),
        RespValue::Integer(0)
    );
    assert_eq!(
        handle(&mut h, cmd(&["HSTRLEN", "hk", "missing"])),
        RespValue::Integer(0)
    );
    assert_eq!(
        handle(&mut h, cmd(&["HSTRLEN", "nope", "f"])),
        RespValue::Integer(0)
    );

    handle(&mut h, cmd(&["SET", "s", "x"]));
    match handle(&mut h, cmd(&["HSTRLEN", "s", "f"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("WRONGTYPE")),
        other => panic!("{:?}", other),
    }
}

#[test]
fn test_hmset() {
    let mut h = make_handler(make_cache());

    let resp = handle(&mut h, cmd(&["HMSET", "hk", "a", "1", "b", "2"]));
    assert_eq!(resp, RespValue::ok());
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["HGET", "hk", "a"]))).as_deref(),
        Some("1")
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["HGET", "hk", "b"]))).as_deref(),
        Some("2")
    );

    // Overwrite existing
    let resp = handle(&mut h, cmd(&["HMSET", "hk", "a", "9"]));
    assert_eq!(resp, RespValue::ok());
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["HGET", "hk", "a"]))).as_deref(),
        Some("9")
    );

    // Arity
    match handle(&mut h, cmd(&["HMSET", "hk", "onlyfield"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("wrong number")),
        other => panic!("{:?}", other),
    }

    handle(&mut h, cmd(&["SET", "s", "x"]));
    match handle(&mut h, cmd(&["HMSET", "s", "f", "v"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("WRONGTYPE")),
        other => panic!("{:?}", other),
    }
}
