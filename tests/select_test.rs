//! Phase C P1: Multi-DB SELECT / FLUSHDB / FLUSHALL isolation

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
        dir: "./data".to_string(),
        dbfilename: "dump.rdb".to_string(),
        appendonly: false,
        appendfilename: "appendonly.aof".to_string(),
        replicaof: String::new(),
        save: "900,1 300,10 60,10000".to_string(),
        maxmemory_policy: "allkeys-lru".to_string(),
        databases: 16,
        metrics_port: 0,
        tls: false,
        tls_cert: String::new(),
        tls_key: String::new(),
        cluster_enabled: false,
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

fn as_bulk_str(v: &RespValue) -> Option<String> {
    match v {
        RespValue::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    }
}

#[test]
fn select_isolates_keys_between_dbs() {
    let mut h = make_handler();

    assert_eq!(handle(&mut h, cmd(&["SET", "k", "db0"])), RespValue::ok());
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GET", "k"]))).as_deref(),
        Some("db0")
    );

    assert_eq!(handle(&mut h, cmd(&["SELECT", "1"])), RespValue::ok());
    assert_eq!(h.selected_db(), 1);
    // Same key name, empty on DB 1
    assert_eq!(handle(&mut h, cmd(&["GET", "k"])), RespValue::null());
    assert_eq!(handle(&mut h, cmd(&["SET", "k", "db1"])), RespValue::ok());
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GET", "k"]))).as_deref(),
        Some("db1")
    );

    // Back to DB 0 — original value intact
    assert_eq!(handle(&mut h, cmd(&["SELECT", "0"])), RespValue::ok());
    assert_eq!(h.selected_db(), 0);
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GET", "k"]))).as_deref(),
        Some("db0")
    );
}

#[test]
fn select_out_of_range() {
    let mut h = make_handler();
    match handle(&mut h, cmd(&["SELECT", "99"])) {
        RespValue::Error(e) => {
            let s = String::from_utf8_lossy(&e);
            assert!(s.contains("out of range"), "{s}");
        }
        other => panic!("expected error, got {other:?}"),
    }
    match handle(&mut h, cmd(&["SELECT", "-1"])) {
        RespValue::Error(e) => {
            let s = String::from_utf8_lossy(&e);
            assert!(s.contains("out of range"), "{s}");
        }
        other => panic!("expected error, got {other:?}"),
    }
    // Still on DB 0
    assert_eq!(h.selected_db(), 0);
    assert_eq!(handle(&mut h, cmd(&["SET", "x", "1"])), RespValue::ok());
}

#[test]
fn flushdb_only_current_database() {
    let mut h = make_handler();
    handle(&mut h, cmd(&["SET", "a", "0"]));
    handle(&mut h, cmd(&["SELECT", "2"]));
    handle(&mut h, cmd(&["SET", "a", "2"]));

    assert_eq!(handle(&mut h, cmd(&["FLUSHDB"])), RespValue::ok());
    assert_eq!(handle(&mut h, cmd(&["EXISTS", "a"])), RespValue::Integer(0));

    handle(&mut h, cmd(&["SELECT", "0"]));
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GET", "a"]))).as_deref(),
        Some("0")
    );
}

#[test]
fn flushall_clears_every_database() {
    let mut h = make_handler();
    handle(&mut h, cmd(&["SET", "a", "0"]));
    handle(&mut h, cmd(&["SELECT", "3"]));
    handle(&mut h, cmd(&["SET", "a", "3"]));
    handle(&mut h, cmd(&["SELECT", "0"]));

    assert_eq!(handle(&mut h, cmd(&["FLUSHALL"])), RespValue::ok());
    assert_eq!(handle(&mut h, cmd(&["EXISTS", "a"])), RespValue::Integer(0));
    handle(&mut h, cmd(&["SELECT", "3"]));
    assert_eq!(handle(&mut h, cmd(&["EXISTS", "a"])), RespValue::Integer(0));
}

#[test]
fn dbsize_is_per_database() {
    let mut h = make_handler();
    handle(&mut h, cmd(&["SET", "a", "1"]));
    handle(&mut h, cmd(&["SET", "b", "2"]));
    assert_eq!(handle(&mut h, cmd(&["DBSIZE"])), RespValue::Integer(2));

    handle(&mut h, cmd(&["SELECT", "1"]));
    assert_eq!(handle(&mut h, cmd(&["DBSIZE"])), RespValue::Integer(0));
    handle(&mut h, cmd(&["SET", "c", "3"]));
    assert_eq!(handle(&mut h, cmd(&["DBSIZE"])), RespValue::Integer(1));
}

#[test]
fn config_get_databases() {
    let mut h = make_handler();
    let resp = handle(&mut h, cmd(&["CONFIG", "GET", "databases"]));
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(as_bulk_str(&arr[0]).as_deref(), Some("databases"));
            assert_eq!(as_bulk_str(&arr[1]).as_deref(), Some("16"));
        }
        other => panic!("expected array, got {other:?}"),
    }
}

#[test]
fn reset_returns_to_db0() {
    let mut h = make_handler();
    handle(&mut h, cmd(&["SELECT", "5"]));
    assert_eq!(h.selected_db(), 5);
    let resp = handle(&mut h, cmd(&["RESET"]));
    assert_eq!(
        resp,
        RespValue::SimpleString(Bytes::from_static(b"RESET"))
    );
    assert_eq!(h.selected_db(), 0);
}

#[test]
fn select_in_multi_exec() {
    let mut h = make_handler();
    handle(&mut h, cmd(&["SET", "k", "on0"]));
    assert_eq!(handle(&mut h, cmd(&["MULTI"])), RespValue::ok());
    assert_eq!(
        handle(&mut h, cmd(&["SELECT", "1"])),
        RespValue::SimpleString(Bytes::from_static(b"QUEUED"))
    );
    assert_eq!(
        handle(&mut h, cmd(&["SET", "k", "on1"])),
        RespValue::SimpleString(Bytes::from_static(b"QUEUED"))
    );
    let exec = handle(&mut h, cmd(&["EXEC"]));
    match exec {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], RespValue::ok());
            assert_eq!(arr[1], RespValue::ok());
        }
        other => panic!("expected EXEC array, got {other:?}"),
    }
    assert_eq!(h.selected_db(), 1);
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GET", "k"]))).as_deref(),
        Some("on1")
    );
    handle(&mut h, cmd(&["SELECT", "0"]));
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GET", "k"]))).as_deref(),
        Some("on0")
    );
}
