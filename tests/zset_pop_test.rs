//! Batch AP: ZPOPMIN / ZPOPMAX / BZPOPMIN / BZPOPMAX.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::protocol::RespValue;
use kore::Cache;
use std::sync::Arc;
use std::time::Duration;

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
        tls_port: 0,
        tls_ca: String::new(),
        tls_auth_clients: false,
        tls_replication: false,
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
fn test_zpopmin_zpopmax() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["ZADD", "z", "1", "a", "2", "b", "3", "c"]));

    let resp = handle(&mut h, cmd(&["ZPOPMIN", "z"]));
    assert_eq!(array_bulk_strs(&resp), vec!["a", "1"]);

    let resp = handle(&mut h, cmd(&["ZPOPMAX", "z"]));
    assert_eq!(array_bulk_strs(&resp), vec!["c", "3"]);

    // count
    handle(&mut h, cmd(&["ZADD", "z2", "10", "x", "20", "y", "30", "z"]));
    let resp = handle(&mut h, cmd(&["ZPOPMIN", "z2", "2"]));
    assert_eq!(array_bulk_strs(&resp), vec!["x", "10", "y", "20"]);

    let resp = handle(&mut h, cmd(&["ZPOPMAX", "z2", "5"]));
    assert_eq!(array_bulk_strs(&resp), vec!["z", "30"]);

    // empty / missing
    assert_eq!(
        handle(&mut h, cmd(&["ZPOPMIN", "z2"])),
        RespValue::Array(vec![])
    );
    assert_eq!(
        handle(&mut h, cmd(&["EXISTS", "z2"])),
        RespValue::Integer(0)
    );
    assert_eq!(
        handle(&mut h, cmd(&["ZPOPMAX", "missing"])),
        RespValue::Array(vec![])
    );
}

#[test]
fn test_zpop_wrongtype() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["SET", "s", "x"]));
    match handle(&mut h, cmd(&["ZPOPMIN", "s"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("WRONGTYPE")),
        other => panic!("{:?}", other),
    }
}

#[test]
fn test_bzpopmin_timeout_and_wake() {
    let cache = make_cache();
    let mut h = make_handler(Arc::clone(&cache));

    let resp = handle(&mut h, cmd(&["BZPOPMIN", "empty", "0.2"]));
    assert_eq!(resp, RespValue::null_array());

    let cache2 = Arc::clone(&cache);
    let mut h_blocker = make_handler(Arc::clone(&cache));
    let blocker = std::thread::spawn(move || {
        handle(&mut h_blocker, cmd(&["BZPOPMIN", "wake", "5"]))
    });
    std::thread::sleep(Duration::from_millis(100));
    let mut h_pusher = make_handler(cache2);
    handle(&mut h_pusher, cmd(&["ZADD", "wake", "7", "m"]));
    let resp = blocker.join().unwrap();
    assert_eq!(array_bulk_strs(&resp), vec!["wake", "m", "7"]);

    let mut h = make_handler(cache);
    assert_eq!(
        handle(&mut h, cmd(&["EXISTS", "wake"])),
        RespValue::Integer(0)
    );
}

#[test]
fn test_bzpopmax_multi_key_order() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["ZADD", "b", "1", "bb"]));
    handle(&mut h, cmd(&["ZADD", "a", "9", "aa"]));
    // left-to-right: empty, then b
    let resp = handle(&mut h, cmd(&["BZPOPMAX", "missing", "b", "a", "1"]));
    assert_eq!(array_bulk_strs(&resp), vec!["b", "bb", "1"]);
}

#[test]
fn test_bzpopmax_wake_on_zadd() {
    let cache = make_cache();
    let cache2 = Arc::clone(&cache);
    let mut h_blocker = make_handler(Arc::clone(&cache));
    let blocker = std::thread::spawn(move || {
        handle(&mut h_blocker, cmd(&["BZPOPMAX", "qz", "3"]))
    });
    std::thread::sleep(Duration::from_millis(100));
    let mut h_pusher = make_handler(cache2);
    handle(&mut h_pusher, cmd(&["ZADD", "qz", "1", "lo", "5", "hi"]));
    let resp = blocker.join().unwrap();
    // highest score
    assert_eq!(array_bulk_strs(&resp), vec!["qz", "hi", "5"]);
}
