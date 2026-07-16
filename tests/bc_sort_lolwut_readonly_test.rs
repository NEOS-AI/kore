//! Batch BC: SORT (list/set/zset), LOLWUT, READONLY/READWRITE.

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

fn array_bulk_strs(v: &RespValue) -> Vec<String> {
    match v {
        RespValue::Array(items) => items.iter().filter_map(as_bulk_str).collect(),
        _ => panic!("expected array, got {:?}", v),
    }
}

#[test]
fn sort_list_numeric_and_alpha() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    assert_eq!(
        handle(&mut h, cmd(&["RPUSH", "nums", "3", "1", "2"])),
        RespValue::Integer(3)
    );
    assert_eq!(
        array_bulk_strs(&handle(&mut h, cmd(&["SORT", "nums"]))),
        vec!["1", "2", "3"]
    );
    assert_eq!(
        array_bulk_strs(&handle(&mut h, cmd(&["SORT", "nums", "DESC"]))),
        vec!["3", "2", "1"]
    );

    assert_eq!(
        handle(&mut h, cmd(&["RPUSH", "words", "banana", "apple", "cherry"])),
        RespValue::Integer(3)
    );
    assert_eq!(
        array_bulk_strs(&handle(&mut h, cmd(&["SORT", "words", "ALPHA"]))),
        vec!["apple", "banana", "cherry"]
    );
    assert_eq!(
        array_bulk_strs(&handle(&mut h, cmd(&["SORT", "words", "ALPHA", "DESC"]))),
        vec!["cherry", "banana", "apple"]
    );
}

#[test]
fn sort_limit_and_store() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    handle(&mut h, cmd(&["RPUSH", "n", "10", "20", "30", "40", "50"]));
    assert_eq!(
        array_bulk_strs(&handle(
            &mut h,
            cmd(&["SORT", "n", "LIMIT", "1", "2"])
        )),
        vec!["20", "30"]
    );

    assert_eq!(
        handle(
            &mut h,
            cmd(&["SORT", "n", "DESC", "STORE", "out"])
        ),
        RespValue::Integer(5)
    );
    assert_eq!(
        array_bulk_strs(&handle(&mut h, cmd(&["LRANGE", "out", "0", "-1"]))),
        vec!["50", "40", "30", "20", "10"]
    );
    // STORE overwrites any type
    handle(&mut h, cmd(&["SET", "out", "string"]));
    assert_eq!(
        handle(&mut h, cmd(&["SORT", "n", "STORE", "out"])),
        RespValue::Integer(5)
    );
    assert_eq!(
        handle(&mut h, cmd(&["TYPE", "out"])),
        RespValue::SimpleString(Bytes::from_static(b"list"))
    );
}

#[test]
fn sort_set_zset_and_by_nosort() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    handle(&mut h, cmd(&["SADD", "s", "3", "1", "2"]));
    assert_eq!(
        array_bulk_strs(&handle(&mut h, cmd(&["SORT", "s"]))),
        vec!["1", "2", "3"]
    );

    handle(&mut h, cmd(&["ZADD", "z", "2", "b", "1", "a", "3", "c"]));
    assert_eq!(
        array_bulk_strs(&handle(&mut h, cmd(&["SORT", "z", "ALPHA"]))),
        vec!["a", "b", "c"]
    );

    // BY nosort preserves list order
    handle(&mut h, cmd(&["RPUSH", "ord", "c", "a", "b"]));
    assert_eq!(
        array_bulk_strs(&handle(
            &mut h,
            cmd(&["SORT", "ord", "BY", "nosort"])
        )),
        vec!["c", "a", "b"]
    );
}

#[test]
fn sort_wrong_type_and_non_numeric() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    handle(&mut h, cmd(&["SET", "str", "x"]));
    match handle(&mut h, cmd(&["SORT", "str"])) {
        RespValue::Error(e) => assert!(
            String::from_utf8_lossy(&e).contains("WRONGTYPE"),
            "got {}",
            String::from_utf8_lossy(&e)
        ),
        other => panic!("expected WRONGTYPE, got {:?}", other),
    }

    handle(&mut h, cmd(&["RPUSH", "mix", "1", "notanumber"]));
    match handle(&mut h, cmd(&["SORT", "mix"])) {
        RespValue::Error(e) => assert!(
            String::from_utf8_lossy(&e).contains("scores"),
            "got {}",
            String::from_utf8_lossy(&e)
        ),
        other => panic!("expected score error, got {:?}", other),
    }

    // Missing key → empty
    assert_eq!(
        handle(&mut h, cmd(&["SORT", "missing"])),
        RespValue::Array(vec![])
    );
}

#[test]
fn sort_command_getkeys() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    match handle(
        &mut h,
        cmd(&["COMMAND", "GETKEYS", "SORT", "mylist", "STORE", "dest"]),
    ) {
        RespValue::Array(keys) => {
            let s: Vec<_> = keys.iter().filter_map(as_bulk_str).collect();
            assert_eq!(s, vec!["mylist", "dest"]);
        }
        other => panic!("expected keys array, got {:?}", other),
    }
}

#[test]
fn lolwut_and_readonly_readwrite() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    match handle(&mut h, cmd(&["LOLWUT"])) {
        RespValue::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("Kore"), "art missing Kore: {}", s);
            assert!(s.contains("LOLWUT") || s.contains("version"), "{}", s);
        }
        other => panic!("expected bulk art, got {:?}", other),
    }
    // Optional version arg accepted
    match handle(&mut h, cmd(&["LOLWUT", "6"])) {
        RespValue::BulkString(Some(_)) => {}
        other => panic!("expected bulk, got {:?}", other),
    }

    assert_eq!(handle(&mut h, cmd(&["READONLY"])), RespValue::ok());
    assert_eq!(handle(&mut h, cmd(&["READWRITE"])), RespValue::ok());

    // Catalog lists the new commands
    match handle(&mut h, cmd(&["COMMAND", "INFO", "sort", "lolwut", "readonly"])) {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 3);
            for item in &items {
                assert!(!matches!(item, RespValue::BulkString(None)));
            }
        }
        other => panic!("expected COMMAND INFO array, got {:?}", other),
    }
}
