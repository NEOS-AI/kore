//! Batch AU: XINFO STREAM|GROUPS|CONSUMERS, XGROUP CREATECONSUMER/DELCONSUMER.

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

/// Find integer value after a bulk field name in a flat field-value array.
fn field_int(arr: &[RespValue], name: &str) -> Option<i64> {
    let mut i = 0;
    while i + 1 < arr.len() {
        if as_bulk_str(&arr[i]).as_deref() == Some(name) {
            return match &arr[i + 1] {
                RespValue::Integer(n) => Some(*n),
                _ => None,
            };
        }
        i += 2;
    }
    None
}

fn field_bulk(arr: &[RespValue], name: &str) -> Option<String> {
    let mut i = 0;
    while i + 1 < arr.len() {
        if as_bulk_str(&arr[i]).as_deref() == Some(name) {
            return as_bulk_str(&arr[i + 1]);
        }
        i += 2;
    }
    None
}

#[test]
fn test_xgroup_create_del_consumer() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["XADD", "s", "1-0", "a", "1"]));
    handle(&mut h, cmd(&["XGROUP", "CREATE", "s", "g", "0"]));

    assert_eq!(
        handle(&mut h, cmd(&["XGROUP", "CREATECONSUMER", "s", "g", "alice"])),
        RespValue::Integer(1)
    );
    assert_eq!(
        handle(&mut h, cmd(&["XGROUP", "CREATECONSUMER", "s", "g", "alice"])),
        RespValue::Integer(0)
    );

    // Read so alice has pending after claim path via another consumer
    handle(
        &mut h,
        cmd(&["XREADGROUP", "GROUP", "g", "bob", "STREAMS", "s", ">"]),
    );
    // bob has 1 pending; delete bob
    assert_eq!(
        handle(&mut h, cmd(&["XGROUP", "DELCONSUMER", "s", "g", "bob"])),
        RespValue::Integer(1)
    );
    // pending cleared
    match handle(&mut h, cmd(&["XPENDING", "s", "g"])) {
        RespValue::Array(summary) => assert_eq!(summary[0], RespValue::Integer(0)),
        other => panic!("{other:?}"),
    }
    // missing consumer → 0
    assert_eq!(
        handle(&mut h, cmd(&["XGROUP", "DELCONSUMER", "s", "g", "nobody"])),
        RespValue::Integer(0)
    );
}

#[test]
fn test_xinfo_stream_groups_consumers() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["XADD", "jobs", "1-0", "t", "a"]));
    handle(&mut h, cmd(&["XADD", "jobs", "1-1", "t", "b"]));
    handle(&mut h, cmd(&["XGROUP", "CREATE", "jobs", "workers", "0"]));
    handle(
        &mut h,
        cmd(&[
            "XREADGROUP",
            "GROUP",
            "workers",
            "c1",
            "COUNT",
            "1",
            "STREAMS",
            "jobs",
            ">",
        ]),
    );

    // XINFO STREAM
    match handle(&mut h, cmd(&["XINFO", "STREAM", "jobs"])) {
        RespValue::Array(arr) => {
            assert_eq!(field_int(&arr, "length"), Some(2));
            assert_eq!(field_int(&arr, "groups"), Some(1));
            assert_eq!(field_bulk(&arr, "last-generated-id").as_deref(), Some("1-1"));
            // first/last entry present
            assert!(field_bulk(&arr, "length").is_none()); // not bulk
        }
        other => panic!("{other:?}"),
    }

    // XINFO GROUPS
    match handle(&mut h, cmd(&["XINFO", "GROUPS", "jobs"])) {
        RespValue::Array(groups) => {
            assert_eq!(groups.len(), 1);
            match &groups[0] {
                RespValue::Array(g) => {
                    assert_eq!(field_bulk(g, "name").as_deref(), Some("workers"));
                    assert_eq!(field_int(g, "consumers"), Some(1));
                    assert_eq!(field_int(g, "pending"), Some(1));
                    assert_eq!(field_bulk(g, "last-delivered-id").as_deref(), Some("1-0"));
                    // one entry still unread → lag 1
                    assert_eq!(field_int(g, "lag"), Some(1));
                }
                other => panic!("{other:?}"),
            }
        }
        other => panic!("{other:?}"),
    }

    // XINFO CONSUMERS
    match handle(&mut h, cmd(&["XINFO", "CONSUMERS", "jobs", "workers"])) {
        RespValue::Array(cons) => {
            assert_eq!(cons.len(), 1);
            match &cons[0] {
                RespValue::Array(c) => {
                    assert_eq!(field_bulk(c, "name").as_deref(), Some("c1"));
                    assert_eq!(field_int(c, "pending"), Some(1));
                    assert!(field_int(c, "idle").is_some());
                    assert!(field_int(c, "inactive").is_some());
                }
                other => panic!("{other:?}"),
            }
        }
        other => panic!("{other:?}"),
    }

    // explicit createconsumer shows up
    handle(
        &mut h,
        cmd(&["XGROUP", "CREATECONSUMER", "jobs", "workers", "c2"]),
    );
    match handle(&mut h, cmd(&["XINFO", "CONSUMERS", "jobs", "workers"])) {
        RespValue::Array(cons) => assert_eq!(cons.len(), 2),
        other => panic!("{other:?}"),
    }
}

#[test]
fn test_xinfo_missing_and_wrongtype() {
    let mut h = make_handler(make_cache());
    match handle(&mut h, cmd(&["XINFO", "STREAM", "gone"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("no such key")),
        other => panic!("{other:?}"),
    }
    handle(&mut h, cmd(&["SET", "s", "x"]));
    match handle(&mut h, cmd(&["XINFO", "STREAM", "s"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("WRONGTYPE")),
        other => panic!("{other:?}"),
    }
}
