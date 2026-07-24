//! Batch AG: MOVE / COPY / RANDOMKEY / TOUCH

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::databases::Databases;
use kore::protocol::RespValue;
use std::sync::Arc;

fn make_config() -> Arc<Config> {
    Arc::new(Config {
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
    })
}

fn make_handler() -> CommandHandler {
    let databases = Databases::create(16, 16, 1024 * 1024 * 100, 500 * 1024 * 1024, false, 0.75);
    CommandHandler::with_databases(databases, make_config(), None)
}

fn bulk(s: &str) -> RespValue {
    RespValue::BulkString(Some(Bytes::from(s.to_string())))
}

fn cmd(parts: &[&str]) -> RespValue {
    RespValue::Array(parts.iter().map(|p| bulk(p)).collect())
}

fn handle(h: &mut CommandHandler, value: RespValue) -> RespValue {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { h.handle(value).await.unwrap() })
}

fn as_bulk(v: &RespValue) -> Option<String> {
    match v {
        RespValue::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    }
}

#[test]
fn move_string_between_dbs() {
    let mut h = make_handler();
    assert_eq!(handle(&mut h, cmd(&["SET", "k", "v"])), RespValue::ok());
    assert_eq!(
        handle(&mut h, cmd(&["EXPIRE", "k", "60"])),
        RespValue::Integer(1)
    );
    assert_eq!(handle(&mut h, cmd(&["MOVE", "k", "1"])), RespValue::Integer(1));
    assert_eq!(handle(&mut h, cmd(&["GET", "k"])), RespValue::null());
    assert_eq!(handle(&mut h, cmd(&["SELECT", "1"])), RespValue::ok());
    assert_eq!(as_bulk(&handle(&mut h, cmd(&["GET", "k"]))).as_deref(), Some("v"));
    let ttl = match handle(&mut h, cmd(&["TTL", "k"])) {
        RespValue::Integer(n) => n,
        o => panic!("{:?}", o),
    };
    assert!(ttl > 0 && ttl <= 60, "ttl={ttl}");
}

#[test]
fn move_hash_and_conflict() {
    let mut h = make_handler();
    handle(&mut h, cmd(&["HSET", "h", "f", "1"]));
    assert_eq!(handle(&mut h, cmd(&["MOVE", "h", "2"])), RespValue::Integer(1));
    assert_eq!(
        handle(&mut h, cmd(&["TYPE", "h"])),
        RespValue::SimpleString(Bytes::from_static(b"none"))
    );

    handle(&mut h, cmd(&["SELECT", "2"]));
    assert_eq!(
        as_bulk(&handle(&mut h, cmd(&["HGET", "h", "f"]))).as_deref(),
        Some("1")
    );

    // Conflict: create same key on DB 0 and try MOVE again from 2
    handle(&mut h, cmd(&["SELECT", "0"]));
    handle(&mut h, cmd(&["HSET", "h", "f", "other"]));
    handle(&mut h, cmd(&["SELECT", "2"]));
    assert_eq!(handle(&mut h, cmd(&["MOVE", "h", "0"])), RespValue::Integer(0));
    // Source still present
    assert_eq!(
        as_bulk(&handle(&mut h, cmd(&["HGET", "h", "f"]))).as_deref(),
        Some("1")
    );
}

#[test]
fn move_same_db_errors() {
    let mut h = make_handler();
    handle(&mut h, cmd(&["SET", "k", "v"]));
    match handle(&mut h, cmd(&["MOVE", "k", "0"])) {
        RespValue::Error(e) => {
            assert!(String::from_utf8_lossy(&e).contains("same"), "{:?}", e);
        }
        o => panic!("{:?}", o),
    }
}

#[test]
fn copy_same_db_and_replace() {
    let mut h = make_handler();
    handle(&mut h, cmd(&["SET", "a", "1"]));
    assert_eq!(
        handle(&mut h, cmd(&["COPY", "a", "b"])),
        RespValue::Integer(1)
    );
    assert_eq!(as_bulk(&handle(&mut h, cmd(&["GET", "b"]))).as_deref(), Some("1"));
    // Source intact
    assert_eq!(as_bulk(&handle(&mut h, cmd(&["GET", "a"]))).as_deref(), Some("1"));

    handle(&mut h, cmd(&["SET", "b", "old"]));
    assert_eq!(
        handle(&mut h, cmd(&["COPY", "a", "b"])),
        RespValue::Integer(0)
    );
    assert_eq!(as_bulk(&handle(&mut h, cmd(&["GET", "b"]))).as_deref(), Some("old"));
    assert_eq!(
        handle(&mut h, cmd(&["COPY", "a", "b", "REPLACE"])),
        RespValue::Integer(1)
    );
    assert_eq!(as_bulk(&handle(&mut h, cmd(&["GET", "b"]))).as_deref(), Some("1"));
}

#[test]
fn copy_to_other_db() {
    let mut h = make_handler();
    handle(&mut h, cmd(&["SADD", "s", "m1", "m2"]));
    handle(&mut h, cmd(&["EXPIRE", "s", "120"]));
    assert_eq!(
        handle(&mut h, cmd(&["COPY", "s", "s", "DB", "3"])),
        RespValue::Integer(1)
    );
    // Source still on db0
    assert_eq!(handle(&mut h, cmd(&["SCARD", "s"])), RespValue::Integer(2));
    handle(&mut h, cmd(&["SELECT", "3"]));
    assert_eq!(handle(&mut h, cmd(&["SCARD", "s"])), RespValue::Integer(2));
    let ttl = match handle(&mut h, cmd(&["TTL", "s"])) {
        RespValue::Integer(n) => n,
        o => panic!("{:?}", o),
    };
    assert!(ttl > 0, "ttl={ttl}");
}

#[test]
fn randomkey_empty_and_nonempty() {
    let mut h = make_handler();
    assert_eq!(handle(&mut h, cmd(&["RANDOMKEY"])), RespValue::null());
    handle(&mut h, cmd(&["SET", "only", "x"]));
    assert_eq!(
        as_bulk(&handle(&mut h, cmd(&["RANDOMKEY"]))).as_deref(),
        Some("only")
    );
}

#[test]
fn touch_counts_existing() {
    let mut h = make_handler();
    handle(&mut h, cmd(&["SET", "a", "1"]));
    handle(&mut h, cmd(&["HSET", "b", "f", "v"]));
    assert_eq!(
        handle(&mut h, cmd(&["TOUCH", "a", "b", "missing"])),
        RespValue::Integer(2)
    );
    assert_eq!(
        handle(&mut h, cmd(&["TOUCH", "missing"])),
        RespValue::Integer(0)
    );
}

#[test]
fn copy_missing_returns_zero() {
    let mut h = make_handler();
    assert_eq!(
        handle(&mut h, cmd(&["COPY", "nope", "x"])),
        RespValue::Integer(0)
    );
}
