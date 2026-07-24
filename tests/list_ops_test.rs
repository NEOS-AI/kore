//! Batch AK: LREM / LTRIM / LINSERT.

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

fn array_bulk_strs(v: &RespValue) -> Vec<String> {
    match v {
        RespValue::Array(arr) => arr.iter().filter_map(as_bulk_str).collect(),
        _ => panic!("expected array, got {:?}", v),
    }
}

#[test]
fn test_lrem() {
    let mut h = make_handler(make_cache());
    handle(
        &mut h,
        cmd(&["RPUSH", "l", "a", "b", "a", "c", "a", "a"]),
    );

    assert_eq!(
        handle(&mut h, cmd(&["LREM", "l", "2", "a"])),
        RespValue::Integer(2)
    );
    assert_eq!(
        array_bulk_strs(&handle(&mut h, cmd(&["LRANGE", "l", "0", "-1"]))),
        vec!["b", "c", "a", "a"]
    );

    assert_eq!(
        handle(&mut h, cmd(&["LREM", "l", "-1", "a"])),
        RespValue::Integer(1)
    );
    assert_eq!(
        array_bulk_strs(&handle(&mut h, cmd(&["LRANGE", "l", "0", "-1"]))),
        vec!["b", "c", "a"]
    );

    assert_eq!(
        handle(&mut h, cmd(&["LREM", "l", "0", "a"])),
        RespValue::Integer(1)
    );
    assert_eq!(
        array_bulk_strs(&handle(&mut h, cmd(&["LRANGE", "l", "0", "-1"]))),
        vec!["b", "c"]
    );

    // Missing key
    assert_eq!(
        handle(&mut h, cmd(&["LREM", "missing", "1", "x"])),
        RespValue::Integer(0)
    );
}

#[test]
fn test_ltrim() {
    let cache = make_cache();
    let mut h = make_handler(cache.clone());
    handle(
        &mut h,
        cmd(&["RPUSH", "l", "0", "1", "2", "3", "4", "5"]),
    );
    assert_eq!(
        handle(&mut h, cmd(&["LTRIM", "l", "1", "3"])),
        RespValue::ok()
    );
    assert_eq!(
        array_bulk_strs(&handle(&mut h, cmd(&["LRANGE", "l", "0", "-1"]))),
        vec!["1", "2", "3"]
    );

    // Empty via inverted range deletes key
    assert_eq!(
        handle(&mut h, cmd(&["LTRIM", "l", "5", "6"])),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["EXISTS", "l"])),
        RespValue::Integer(0)
    );

    // Missing key still OK
    assert_eq!(
        handle(&mut h, cmd(&["LTRIM", "nope", "0", "1"])),
        RespValue::ok()
    );
}

#[test]
fn test_linsert() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["RPUSH", "l", "a", "b", "c"]));

    assert_eq!(
        handle(&mut h, cmd(&["LINSERT", "l", "BEFORE", "b", "x"])),
        RespValue::Integer(4)
    );
    assert_eq!(
        array_bulk_strs(&handle(&mut h, cmd(&["LRANGE", "l", "0", "-1"]))),
        vec!["a", "x", "b", "c"]
    );

    assert_eq!(
        handle(&mut h, cmd(&["LINSERT", "l", "AFTER", "c", "y"])),
        RespValue::Integer(5)
    );
    assert_eq!(
        array_bulk_strs(&handle(&mut h, cmd(&["LRANGE", "l", "0", "-1"]))),
        vec!["a", "x", "b", "c", "y"]
    );

    assert_eq!(
        handle(&mut h, cmd(&["LINSERT", "l", "BEFORE", "missing", "z"])),
        RespValue::Integer(-1)
    );
    assert_eq!(
        handle(&mut h, cmd(&["LINSERT", "nope", "AFTER", "a", "z"])),
        RespValue::Integer(0)
    );
}

#[test]
fn test_list_ops_wrongtype() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["SET", "s", "v"]));
    for c in [
        &["LREM", "s", "1", "x"][..],
        &["LTRIM", "s", "0", "1"][..],
        &["LINSERT", "s", "BEFORE", "a", "b"][..],
    ] {
        match handle(&mut h, cmd(c)) {
            RespValue::Error(e) => {
                assert!(String::from_utf8_lossy(&e).starts_with("WRONGTYPE"));
            }
            other => panic!("expected WRONGTYPE for {:?}, got {:?}", c, other),
        }
    }
}
