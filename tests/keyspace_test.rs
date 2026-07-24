//! Phase A P0 unified keyspace tests: type safety + cross-type key ops.

use bytes::Bytes;
use kore::cache::KeyType;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::entry::StoreOptions;
use kore::error::Error;
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

fn is_wrongtype(resp: &RespValue) -> bool {
    match resp {
        RespValue::Error(e) => String::from_utf8_lossy(e).starts_with("WRONGTYPE"),
        _ => false,
    }
}

#[test]
fn test_set_then_zadd_same_key_wrongtype() {
    let cache = make_cache();
    let mut handler = make_handler(cache);

    let resp = handle(&mut handler, cmd(&["SET", "mykey", "hello"]));
    assert!(matches!(resp, RespValue::SimpleString(_)));

    let resp = handle(&mut handler, cmd(&["ZADD", "mykey", "1", "member"]));
    assert!(is_wrongtype(&resp), "expected WRONGTYPE, got {:?}", resp);
}

#[test]
fn test_zadd_then_set_same_key_wrongtype() {
    let cache = make_cache();
    let mut handler = make_handler(cache);

    let resp = handle(&mut handler, cmd(&["ZADD", "mykey", "1", "member"]));
    assert_eq!(resp, RespValue::Integer(1));

    let resp = handle(&mut handler, cmd(&["SET", "mykey", "hello"]));
    assert!(is_wrongtype(&resp), "expected WRONGTYPE, got {:?}", resp);
}

#[test]
fn test_get_on_zset_wrongtype() {
    let cache = make_cache();
    let mut handler = make_handler(cache);

    handle(&mut handler, cmd(&["ZADD", "z", "1.5", "a"]));

    let resp = handle(&mut handler, cmd(&["GET", "z"]));
    assert!(is_wrongtype(&resp), "expected WRONGTYPE, got {:?}", resp);
}

#[test]
fn test_zrange_on_string_wrongtype() {
    let cache = make_cache();
    let mut handler = make_handler(cache);

    handle(&mut handler, cmd(&["SET", "s", "value"]));

    let resp = handle(&mut handler, cmd(&["ZRANGE", "s", "0", "-1"]));
    assert!(is_wrongtype(&resp), "expected WRONGTYPE, got {:?}", resp);
}

#[test]
fn test_zadd_then_del_exists() {
    let cache = make_cache();
    let mut handler = make_handler(cache.clone());

    handle(&mut handler, cmd(&["ZADD", "zk", "10", "m1"]));

    assert!(cache.exists(&Bytes::from("zk")));
    assert_eq!(cache.key_type(&Bytes::from("zk")), KeyType::ZSet);

    let resp = handle(&mut handler, cmd(&["DEL", "zk"]));
    assert_eq!(resp, RespValue::Integer(1));

    assert!(!cache.exists(&Bytes::from("zk")));
    assert_eq!(cache.key_type(&Bytes::from("zk")), KeyType::None);

    let resp = handle(&mut handler, cmd(&["EXISTS", "zk"]));
    assert_eq!(resp, RespValue::Integer(0));
}

#[test]
fn test_keys_includes_zset_and_geo() {
    let cache = make_cache();
    let mut handler = make_handler(cache.clone());

    handle(&mut handler, cmd(&["SET", "strkey", "v"]));
    handle(&mut handler, cmd(&["ZADD", "zkey", "1", "m"]));
    handle(
        &mut handler,
        cmd(&["GEOADD", "gkey", "13.361389", "38.115556", "Palermo"]),
    );

    let keys = cache.keys(Some("*"));
    let key_strs: Vec<String> = keys
        .iter()
        .map(|k| String::from_utf8_lossy(k).into_owned())
        .collect();

    assert!(key_strs.contains(&"strkey".to_string()));
    assert!(key_strs.contains(&"zkey".to_string()));
    assert!(key_strs.contains(&"gkey".to_string()));
    assert_eq!(cache.dbsize(), 3);

    let resp = handle(&mut handler, cmd(&["KEYS", "*"]));
    if let RespValue::Array(arr) = resp {
        assert_eq!(arr.len(), 3);
    } else {
        panic!("expected array from KEYS, got {:?}", resp);
    }
}

#[test]
fn test_flushall_clears_zsets_and_geo() {
    let cache = make_cache();
    let mut handler = make_handler(cache.clone());

    handle(&mut handler, cmd(&["SET", "a", "1"]));
    handle(&mut handler, cmd(&["ZADD", "b", "1", "m"]));
    handle(&mut handler, cmd(&["GEOADD", "c", "0", "0", "origin"]));

    assert_eq!(cache.dbsize(), 3);

    let resp = handle(&mut handler, cmd(&["FLUSHALL"]));
    assert!(matches!(resp, RespValue::SimpleString(_)));

    assert_eq!(cache.dbsize(), 0);
    assert!(!cache.exists(&Bytes::from("a")));
    assert!(!cache.exists(&Bytes::from("b")));
    assert!(!cache.exists(&Bytes::from("c")));
    assert!(cache.keys(Some("*")).is_empty());
}

#[test]
fn test_type_command() {
    let cache = make_cache();
    let mut handler = make_handler(cache);

    let resp = handle(&mut handler, cmd(&["TYPE", "missing"]));
    assert_eq!(
        resp,
        RespValue::SimpleString(Bytes::from_static(b"none"))
    );

    handle(&mut handler, cmd(&["SET", "s", "v"]));
    let resp = handle(&mut handler, cmd(&["TYPE", "s"]));
    assert_eq!(
        resp,
        RespValue::SimpleString(Bytes::from_static(b"string"))
    );

    handle(&mut handler, cmd(&["ZADD", "z", "1", "m"]));
    let resp = handle(&mut handler, cmd(&["TYPE", "z"]));
    assert_eq!(
        resp,
        RespValue::SimpleString(Bytes::from_static(b"zset"))
    );

    // Geo reports as zset (Redis-compatible)
    handle(
        &mut handler,
        cmd(&["GEOADD", "g", "13.361389", "38.115556", "Palermo"]),
    );
    let resp = handle(&mut handler, cmd(&["TYPE", "g"]));
    assert_eq!(
        resp,
        RespValue::SimpleString(Bytes::from_static(b"zset"))
    );
}

#[test]
fn test_cache_api_cross_type_delete_exists() {
    let cache = make_cache();
    let key = Bytes::from("zk");

    cache
        .get_or_create_sorted_set(&key)
        .unwrap()
        .write()
        .add(Bytes::from("m"), 1.0);

    assert!(cache.exists(&key));
    assert_eq!(cache.key_type(&key), KeyType::ZSet);
    assert!(cache.delete(&key).unwrap());
    assert!(!cache.exists(&key));
}

#[test]
fn test_ensure_type_helpers() {
    let cache = make_cache();
    let key = Bytes::from("k");

    assert!(cache.ensure_type(&key, KeyType::String).is_ok());
    assert!(cache.ensure_type(&key, KeyType::ZSet).is_ok());

    cache
        .store(key.clone(), Bytes::from("v"), StoreOptions::default())
        .unwrap();

    assert!(cache.ensure_type(&key, KeyType::String).is_ok());
    assert!(matches!(
        cache.ensure_type(&key, KeyType::ZSet),
        Err(Error::WrongType)
    ));
    assert!(cache.ensure_string_or_absent(&key).is_ok());

    let zkey = Bytes::from("zk");
    cache.get_or_create_sorted_set(&zkey).unwrap();
    assert!(matches!(
        cache.ensure_string_or_absent(&zkey),
        Err(Error::WrongType)
    ));
}

#[test]
fn test_geoadd_on_string_wrongtype() {
    let cache = make_cache();
    let mut handler = make_handler(cache);

    handle(&mut handler, cmd(&["SET", "k", "v"]));
    let resp = handle(
        &mut handler,
        cmd(&["GEOADD", "k", "13.361389", "38.115556", "Palermo"]),
    );
    assert!(is_wrongtype(&resp), "expected WRONGTYPE, got {:?}", resp);
}

#[test]
fn test_wrongtype_error_message_format() {
    assert_eq!(
        Error::WrongType.to_resp_string(),
        "WRONGTYPE Operation against a key holding the wrong kind of value"
    );
    // Must not be prefixed with ERR
    assert!(!Error::WrongType.to_resp_string().starts_with("ERR "));
}
