//! Phase C P0: MULTI / EXEC / DISCARD / WATCH / UNWATCH

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
fn multi_exec_basic() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    assert_eq!(handle(&mut h, cmd(&["MULTI"])), RespValue::ok());
    assert_eq!(
        handle(&mut h, cmd(&["SET", "a", "1"])),
        RespValue::SimpleString(Bytes::from_static(b"QUEUED"))
    );
    assert_eq!(
        handle(&mut h, cmd(&["INCR", "a"])),
        RespValue::SimpleString(Bytes::from_static(b"QUEUED"))
    );
    assert_eq!(
        handle(&mut h, cmd(&["GET", "a"])),
        RespValue::SimpleString(Bytes::from_static(b"QUEUED"))
    );

    let exec = handle(&mut h, cmd(&["EXEC"]));
    match exec {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], RespValue::ok());
            assert_eq!(arr[1], RespValue::Integer(2));
            assert_eq!(as_bulk_str(&arr[2]).as_deref(), Some("2"));
        }
        other => panic!("expected array, got {:?}", other),
    }

    // Outside MULTI, GET works
    let get = handle(&mut h, cmd(&["GET", "a"]));
    assert_eq!(as_bulk_str(&get).as_deref(), Some("2"));
}

#[test]
fn multi_discard() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    assert_eq!(handle(&mut h, cmd(&["MULTI"])), RespValue::ok());
    assert_eq!(
        handle(&mut h, cmd(&["SET", "x", "discarded"])),
        RespValue::SimpleString(Bytes::from_static(b"QUEUED"))
    );
    assert_eq!(handle(&mut h, cmd(&["DISCARD"])), RespValue::ok());

    let get = handle(&mut h, cmd(&["GET", "x"]));
    assert_eq!(get, RespValue::null());

    // DISCARD without MULTI
    match handle(&mut h, cmd(&["DISCARD"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("DISCARD without MULTI")),
        other => panic!("expected error, got {:?}", other),
    }
}

#[test]
fn exec_without_multi() {
    let cache = make_cache();
    let mut h = make_handler(cache);
    match handle(&mut h, cmd(&["EXEC"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("EXEC without MULTI")),
        other => panic!("expected error, got {:?}", other),
    }
}

#[test]
fn nested_multi_rejected() {
    let cache = make_cache();
    let mut h = make_handler(cache);
    assert_eq!(handle(&mut h, cmd(&["MULTI"])), RespValue::ok());
    match handle(&mut h, cmd(&["MULTI"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("nested")),
        other => panic!("expected error, got {:?}", other),
    }
    // Can still discard
    assert_eq!(handle(&mut h, cmd(&["DISCARD"])), RespValue::ok());
}

#[test]
fn empty_multi_exec() {
    let cache = make_cache();
    let mut h = make_handler(cache);
    assert_eq!(handle(&mut h, cmd(&["MULTI"])), RespValue::ok());
    let exec = handle(&mut h, cmd(&["EXEC"]));
    assert_eq!(exec, RespValue::Array(vec![]));
}

#[test]
fn watch_aborts_on_external_write() {
    let cache = make_cache();
    let mut h1 = make_handler(cache.clone());
    let mut h2 = make_handler(cache);

    // Seed key
    assert_eq!(handle(&mut h1, cmd(&["SET", "balance", "100"])), RespValue::ok());

    // Client 1 watches and starts transaction
    assert_eq!(handle(&mut h1, cmd(&["WATCH", "balance"])), RespValue::ok());
    assert_eq!(handle(&mut h1, cmd(&["MULTI"])), RespValue::ok());
    assert_eq!(
        handle(&mut h1, cmd(&["SET", "balance", "50"])),
        RespValue::SimpleString(Bytes::from_static(b"QUEUED"))
    );

    // Client 2 modifies watched key
    assert_eq!(handle(&mut h2, cmd(&["SET", "balance", "200"])), RespValue::ok());

    // Client 1 EXEC returns null (transaction aborted)
    let exec = handle(&mut h1, cmd(&["EXEC"]));
    assert_eq!(exec, RespValue::null());

    // Key remains client 2's value
    let get = handle(&mut h2, cmd(&["GET", "balance"]));
    assert_eq!(as_bulk_str(&get).as_deref(), Some("200"));
}

#[test]
fn watch_succeeds_when_untouched() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    assert_eq!(handle(&mut h, cmd(&["SET", "k", "1"])), RespValue::ok());
    assert_eq!(handle(&mut h, cmd(&["WATCH", "k"])), RespValue::ok());
    assert_eq!(handle(&mut h, cmd(&["MULTI"])), RespValue::ok());
    assert_eq!(
        handle(&mut h, cmd(&["INCR", "k"])),
        RespValue::SimpleString(Bytes::from_static(b"QUEUED"))
    );
    let exec = handle(&mut h, cmd(&["EXEC"]));
    match exec {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], RespValue::Integer(2));
        }
        other => panic!("expected array, got {:?}", other),
    }
}

#[test]
fn unwatch_clears_optimistic_lock() {
    let cache = make_cache();
    let mut h1 = make_handler(cache.clone());
    let mut h2 = make_handler(cache);

    assert_eq!(handle(&mut h1, cmd(&["SET", "k", "1"])), RespValue::ok());
    assert_eq!(handle(&mut h1, cmd(&["WATCH", "k"])), RespValue::ok());
    assert_eq!(handle(&mut h1, cmd(&["UNWATCH"])), RespValue::ok());

    // External write after UNWATCH should not abort
    assert_eq!(handle(&mut h2, cmd(&["SET", "k", "99"])), RespValue::ok());

    assert_eq!(handle(&mut h1, cmd(&["MULTI"])), RespValue::ok());
    assert_eq!(
        handle(&mut h1, cmd(&["SET", "k", "2"])),
        RespValue::SimpleString(Bytes::from_static(b"QUEUED"))
    );
    let exec = handle(&mut h1, cmd(&["EXEC"]));
    match exec {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0], RespValue::ok());
        }
        other => panic!("expected array, got {:?}", other),
    }
    assert_eq!(as_bulk_str(&handle(&mut h1, cmd(&["GET", "k"]))).as_deref(), Some("2"));
}

#[test]
fn watch_inside_multi_rejected() {
    let cache = make_cache();
    let mut h = make_handler(cache);
    assert_eq!(handle(&mut h, cmd(&["MULTI"])), RespValue::ok());
    match handle(&mut h, cmd(&["WATCH", "k"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("WATCH inside MULTI")),
        other => panic!("expected error, got {:?}", other),
    }
    assert_eq!(handle(&mut h, cmd(&["DISCARD"])), RespValue::ok());
}

#[test]
fn multi_with_hash_list_set() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    assert_eq!(handle(&mut h, cmd(&["MULTI"])), RespValue::ok());
    assert_eq!(
        handle(&mut h, cmd(&["HSET", "h", "f", "v"])),
        RespValue::SimpleString(Bytes::from_static(b"QUEUED"))
    );
    assert_eq!(
        handle(&mut h, cmd(&["LPUSH", "l", "a"])),
        RespValue::SimpleString(Bytes::from_static(b"QUEUED"))
    );
    assert_eq!(
        handle(&mut h, cmd(&["SADD", "s", "m"])),
        RespValue::SimpleString(Bytes::from_static(b"QUEUED"))
    );
    let exec = handle(&mut h, cmd(&["EXEC"]));
    match exec {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], RespValue::Integer(1));
            assert_eq!(arr[1], RespValue::Integer(1));
            assert_eq!(arr[2], RespValue::Integer(1));
        }
        other => panic!("expected array, got {:?}", other),
    }
}
