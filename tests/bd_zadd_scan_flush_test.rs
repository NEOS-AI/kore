//! Batch BD: ZADD NX|XX|GT|LT|CH|INCR, SCAN TYPE, FLUSHDB/FLUSHALL ASYNC|SYNC.

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
fn zadd_nx_xx_ch() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    assert_eq!(
        handle(&mut h, cmd(&["ZADD", "z", "1", "a", "2", "b"])),
        RespValue::Integer(2)
    );
    // NX: skip existing a, add c
    assert_eq!(
        handle(&mut h, cmd(&["ZADD", "z", "NX", "5", "a", "3", "c"])),
        RespValue::Integer(1)
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["ZSCORE", "z", "a"]))).as_deref(),
        Some("1")
    );
    // XX: update a only, skip missing d
    assert_eq!(
        handle(&mut h, cmd(&["ZADD", "z", "XX", "9", "a", "1", "d"])),
        RespValue::Integer(0)
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["ZSCORE", "z", "a"]))).as_deref(),
        Some("9")
    );
    // CH counts score updates
    assert_eq!(
        handle(&mut h, cmd(&["ZADD", "z", "CH", "10", "a", "4", "e"])),
        RespValue::Integer(2)
    );
    // XX on missing key → 0, no key created
    assert_eq!(
        handle(&mut h, cmd(&["ZADD", "missing", "XX", "1", "m"])),
        RespValue::Integer(0)
    );
    assert_eq!(
        handle(&mut h, cmd(&["EXISTS", "missing"])),
        RespValue::Integer(0)
    );
}

#[test]
fn zadd_gt_lt_incr() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    handle(&mut h, cmd(&["ZADD", "z", "5", "m"]));
    // GT: only if new > old
    assert_eq!(
        handle(&mut h, cmd(&["ZADD", "z", "GT", "3", "m"])),
        RespValue::Integer(0)
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["ZSCORE", "z", "m"]))).as_deref(),
        Some("5")
    );
    assert_eq!(
        handle(&mut h, cmd(&["ZADD", "z", "GT", "CH", "8", "m"])),
        RespValue::Integer(1)
    );
    // LT: only if new < old
    assert_eq!(
        handle(&mut h, cmd(&["ZADD", "z", "LT", "10", "m"])),
        RespValue::Integer(0)
    );
    assert_eq!(
        handle(&mut h, cmd(&["ZADD", "z", "LT", "2", "m"])),
        RespValue::Integer(0) // without CH: update is not "added"
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["ZSCORE", "z", "m"]))).as_deref(),
        Some("2")
    );

    // INCR
    match handle(&mut h, cmd(&["ZADD", "z", "INCR", "3", "m"])) {
        RespValue::BulkString(Some(b)) => {
            assert_eq!(String::from_utf8_lossy(&b), "5");
        }
        other => panic!("expected bulk score, got {:?}", other),
    }
    // INCR + NX when exists → null
    assert_eq!(
        handle(&mut h, cmd(&["ZADD", "z", "NX", "INCR", "1", "m"])),
        RespValue::null()
    );
    // NX+XX conflict
    match handle(&mut h, cmd(&["ZADD", "z", "NX", "XX", "1", "x"])) {
        RespValue::Error(e) => assert!(
            String::from_utf8_lossy(&e).contains("XX and NX"),
            "{}",
            String::from_utf8_lossy(&e)
        ),
        other => panic!("{:?}", other),
    }
}

#[test]
fn scan_type_filter() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    handle(&mut h, cmd(&["SET", "s1", "v"]));
    handle(&mut h, cmd(&["RPUSH", "l1", "a"]));
    handle(&mut h, cmd(&["SADD", "set1", "m"]));
    handle(&mut h, cmd(&["ZADD", "z1", "1", "m"]));
    handle(&mut h, cmd(&["HSET", "h1", "f", "v"]));

    match handle(&mut h, cmd(&["SCAN", "0", "TYPE", "string", "COUNT", "100"])) {
        RespValue::Array(parts) => {
            assert_eq!(parts.len(), 2);
            match &parts[1] {
                RespValue::Array(keys) => {
                    let names: Vec<_> = keys.iter().filter_map(as_bulk_str).collect();
                    assert!(names.iter().any(|k| k == "s1"), "{:?}", names);
                    assert!(!names.iter().any(|k| k == "l1"), "{:?}", names);
                    assert!(!names.iter().any(|k| k == "z1"), "{:?}", names);
                }
                other => panic!("{:?}", other),
            }
        }
        other => panic!("{:?}", other),
    }

    match handle(&mut h, cmd(&["SCAN", "0", "TYPE", "list", "COUNT", "100"])) {
        RespValue::Array(parts) => match &parts[1] {
            RespValue::Array(keys) => {
                let names: Vec<_> = keys.iter().filter_map(as_bulk_str).collect();
                assert_eq!(names, vec!["l1"]);
            }
            other => panic!("{:?}", other),
        },
        other => panic!("{:?}", other),
    }

    match handle(&mut h, cmd(&["SCAN", "0", "TYPE", "zset", "COUNT", "100"])) {
        RespValue::Array(parts) => match &parts[1] {
            RespValue::Array(keys) => {
                let names: Vec<_> = keys.iter().filter_map(as_bulk_str).collect();
                assert!(names.contains(&"z1".to_string()), "{:?}", names);
            }
            other => panic!("{:?}", other),
        },
        other => panic!("{:?}", other),
    }
}

#[test]
fn flush_async_sync_and_catalog() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    handle(&mut h, cmd(&["SET", "a", "1"]));
    assert_eq!(handle(&mut h, cmd(&["FLUSHDB", "ASYNC"])), RespValue::ok());
    assert_eq!(handle(&mut h, cmd(&["EXISTS", "a"])), RespValue::Integer(0));

    handle(&mut h, cmd(&["SET", "b", "1"]));
    assert_eq!(handle(&mut h, cmd(&["FLUSHALL", "SYNC"])), RespValue::ok());
    assert_eq!(handle(&mut h, cmd(&["EXISTS", "b"])), RespValue::Integer(0));

    match handle(&mut h, cmd(&["FLUSHDB", "NOPE"])) {
        RespValue::Error(e) => assert!(
            String::from_utf8_lossy(&e).contains("syntax")
                || String::from_utf8_lossy(&e).contains("ASYNC"),
            "{}",
            String::from_utf8_lossy(&e)
        ),
        other => panic!("{:?}", other),
    }

    // Shard pub/sub + zadd still in COMMAND catalog
    match handle(
        &mut h,
        cmd(&["COMMAND", "INFO", "spublish", "ssubscribe", "sunsubscribe", "zadd"]),
    ) {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 4);
            for item in &items {
                assert!(
                    !matches!(item, RespValue::BulkString(None)),
                    "null catalog entry"
                );
            }
        }
        other => panic!("{:?}", other),
    }
}
