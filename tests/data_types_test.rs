//! Phase C P0: Hash / List / Set data types.

use bytes::Bytes;
use kore::cache::KeyType;
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

fn is_wrongtype(resp: &RespValue) -> bool {
    match resp {
        RespValue::Error(e) => String::from_utf8_lossy(e).starts_with("WRONGTYPE"),
        _ => false,
    }
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

// ── Hashes ──────────────────────────────────────────────────────────────────

#[test]
fn test_hset_hget_hgetall_hdel() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    let resp = handle(&mut h, cmd(&["HSET", "user:1", "name", "alice", "age", "30"]));
    assert_eq!(resp, RespValue::Integer(2));

    let resp = handle(&mut h, cmd(&["HGET", "user:1", "name"]));
    assert_eq!(as_bulk_str(&resp).as_deref(), Some("alice"));

    let resp = handle(&mut h, cmd(&["HGETALL", "user:1"]));
    let pairs = array_bulk_strs(&resp);
    assert_eq!(pairs.len(), 4);
    // field/value pairs unordered
    let mut map = std::collections::HashMap::new();
    for i in (0..pairs.len()).step_by(2) {
        map.insert(pairs[i].clone(), pairs[i + 1].clone());
    }
    assert_eq!(map.get("name").map(|s| s.as_str()), Some("alice"));
    assert_eq!(map.get("age").map(|s| s.as_str()), Some("30"));

    let resp = handle(&mut h, cmd(&["HDEL", "user:1", "age"]));
    assert_eq!(resp, RespValue::Integer(1));

    let resp = handle(&mut h, cmd(&["HGET", "user:1", "age"]));
    assert!(matches!(resp, RespValue::BulkString(None)));

    let resp = handle(&mut h, cmd(&["HLEN", "user:1"]));
    assert_eq!(resp, RespValue::Integer(1));

    let resp = handle(&mut h, cmd(&["HEXISTS", "user:1", "name"]));
    assert_eq!(resp, RespValue::Integer(1));
}

// ── Lists ───────────────────────────────────────────────────────────────────

#[test]
fn test_lpush_rpush_lrange_lpop() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    let resp = handle(&mut h, cmd(&["LPUSH", "mylist", "a"]));
    assert_eq!(resp, RespValue::Integer(1));
    let resp = handle(&mut h, cmd(&["LPUSH", "mylist", "b"]));
    assert_eq!(resp, RespValue::Integer(2));
    let resp = handle(&mut h, cmd(&["RPUSH", "mylist", "c"]));
    assert_eq!(resp, RespValue::Integer(3));

    // list is [b, a, c]
    let resp = handle(&mut h, cmd(&["LRANGE", "mylist", "0", "-1"]));
    assert_eq!(array_bulk_strs(&resp), vec!["b", "a", "c"]);

    let resp = handle(&mut h, cmd(&["LPOP", "mylist"]));
    assert_eq!(as_bulk_str(&resp).as_deref(), Some("b"));

    let resp = handle(&mut h, cmd(&["LRANGE", "mylist", "0", "-1"]));
    assert_eq!(array_bulk_strs(&resp), vec!["a", "c"]);

    let resp = handle(&mut h, cmd(&["LLEN", "mylist"]));
    assert_eq!(resp, RespValue::Integer(2));
}

// ── Sets ────────────────────────────────────────────────────────────────────

#[test]
fn test_sadd_smembers_sismember_srem() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    let resp = handle(&mut h, cmd(&["SADD", "tags", "red", "blue", "green"]));
    assert_eq!(resp, RespValue::Integer(3));

    // duplicate
    let resp = handle(&mut h, cmd(&["SADD", "tags", "red"]));
    assert_eq!(resp, RespValue::Integer(0));

    let resp = handle(&mut h, cmd(&["SCARD", "tags"]));
    assert_eq!(resp, RespValue::Integer(3));

    let resp = handle(&mut h, cmd(&["SISMEMBER", "tags", "blue"]));
    assert_eq!(resp, RespValue::Integer(1));
    let resp = handle(&mut h, cmd(&["SISMEMBER", "tags", "yellow"]));
    assert_eq!(resp, RespValue::Integer(0));

    let resp = handle(&mut h, cmd(&["SMEMBERS", "tags"]));
    let mut members = array_bulk_strs(&resp);
    members.sort();
    assert_eq!(members, vec!["blue", "green", "red"]);

    let resp = handle(&mut h, cmd(&["SREM", "tags", "blue", "yellow"]));
    assert_eq!(resp, RespValue::Integer(1));

    let resp = handle(&mut h, cmd(&["SCARD", "tags"]));
    assert_eq!(resp, RespValue::Integer(2));
}

// ── Type safety ─────────────────────────────────────────────────────────────

#[test]
fn test_set_then_hset_wrongtype() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    handle(&mut h, cmd(&["SET", "k", "stringval"]));
    let resp = handle(&mut h, cmd(&["HSET", "k", "f", "v"]));
    assert!(is_wrongtype(&resp), "expected WRONGTYPE, got {:?}", resp);

    let resp = handle(&mut h, cmd(&["LPUSH", "k", "x"]));
    assert!(is_wrongtype(&resp));

    let resp = handle(&mut h, cmd(&["SADD", "k", "x"]));
    assert!(is_wrongtype(&resp));
}

#[test]
fn test_hset_then_get_wrongtype() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    handle(&mut h, cmd(&["HSET", "hk", "f", "v"]));
    let resp = handle(&mut h, cmd(&["GET", "hk"]));
    assert!(is_wrongtype(&resp), "expected WRONGTYPE, got {:?}", resp);
}

// ── DEL / TYPE / KEYS ───────────────────────────────────────────────────────

#[test]
fn test_del_removes_hash_list_set() {
    let cache = make_cache();
    let mut h = make_handler(cache.clone());

    handle(&mut h, cmd(&["HSET", "h", "f", "v"]));
    handle(&mut h, cmd(&["LPUSH", "l", "a"]));
    handle(&mut h, cmd(&["SADD", "s", "m"]));

    assert_eq!(cache.key_type(&Bytes::from("h")), KeyType::Hash);
    assert_eq!(cache.key_type(&Bytes::from("l")), KeyType::List);
    assert_eq!(cache.key_type(&Bytes::from("s")), KeyType::Set);

    let resp = handle(&mut h, cmd(&["DEL", "h", "l", "s"]));
    assert_eq!(resp, RespValue::Integer(3));

    assert_eq!(cache.key_type(&Bytes::from("h")), KeyType::None);
    assert_eq!(cache.key_type(&Bytes::from("l")), KeyType::None);
    assert_eq!(cache.key_type(&Bytes::from("s")), KeyType::None);
}

#[test]
fn test_type_returns_hash_list_set() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    handle(&mut h, cmd(&["HSET", "h", "f", "v"]));
    handle(&mut h, cmd(&["LPUSH", "l", "a"]));
    handle(&mut h, cmd(&["SADD", "s", "m"]));

    let resp = handle(&mut h, cmd(&["TYPE", "h"]));
    assert_eq!(resp, RespValue::SimpleString(Bytes::from_static(b"hash")));

    let resp = handle(&mut h, cmd(&["TYPE", "l"]));
    assert_eq!(resp, RespValue::SimpleString(Bytes::from_static(b"list")));

    let resp = handle(&mut h, cmd(&["TYPE", "s"]));
    assert_eq!(resp, RespValue::SimpleString(Bytes::from_static(b"set")));

    let resp = handle(&mut h, cmd(&["TYPE", "missing"]));
    assert_eq!(resp, RespValue::SimpleString(Bytes::from_static(b"none")));
}

#[test]
fn test_keys_includes_hash_list_set() {
    let cache = make_cache();
    let mut h = make_handler(cache.clone());

    handle(&mut h, cmd(&["SET", "str", "v"]));
    handle(&mut h, cmd(&["HSET", "hashk", "f", "v"]));
    handle(&mut h, cmd(&["LPUSH", "listk", "a"]));
    handle(&mut h, cmd(&["SADD", "setk", "m"]));

    let keys = cache.keys(Some("*"));
    let mut key_strs: Vec<String> = keys
        .iter()
        .map(|k| String::from_utf8_lossy(k).into_owned())
        .collect();
    key_strs.sort();
    assert!(key_strs.contains(&"str".to_string()));
    assert!(key_strs.contains(&"hashk".to_string()));
    assert!(key_strs.contains(&"listk".to_string()));
    assert!(key_strs.contains(&"setk".to_string()));
    assert_eq!(cache.dbsize(), 4);
}

#[test]
fn test_sinter_basic() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    handle(&mut h, cmd(&["SADD", "a", "1", "2", "3"]));
    handle(&mut h, cmd(&["SADD", "b", "2", "3", "4"]));

    let resp = handle(&mut h, cmd(&["SINTER", "a", "b"]));
    let mut members = array_bulk_strs(&resp);
    members.sort();
    assert_eq!(members, vec!["2", "3"]);
}

// ── Batch AH: set algebra + random set ops ──────────────────────────────────

#[test]
fn test_sunion_sdiff() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    handle(&mut h, cmd(&["SADD", "a", "1", "2", "3"]));
    handle(&mut h, cmd(&["SADD", "b", "2", "3", "4"]));

    let resp = handle(&mut h, cmd(&["SUNION", "a", "b"]));
    let mut members = array_bulk_strs(&resp);
    members.sort();
    assert_eq!(members, vec!["1", "2", "3", "4"]);

    let resp = handle(&mut h, cmd(&["SDIFF", "a", "b"]));
    let mut members = array_bulk_strs(&resp);
    members.sort();
    assert_eq!(members, vec!["1"]);

    let resp = handle(&mut h, cmd(&["SDIFF", "b", "a"]));
    let mut members = array_bulk_strs(&resp);
    members.sort();
    assert_eq!(members, vec!["4"]);

    // Missing keys act as empty for union/diff.
    let resp = handle(&mut h, cmd(&["SUNION", "a", "missing"]));
    let mut members = array_bulk_strs(&resp);
    members.sort();
    assert_eq!(members, vec!["1", "2", "3"]);

    let resp = handle(&mut h, cmd(&["SDIFF", "missing", "a"]));
    assert_eq!(array_bulk_strs(&resp), Vec::<String>::new());
}

#[test]
fn test_set_store_variants() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    handle(&mut h, cmd(&["SADD", "a", "1", "2", "3"]));
    handle(&mut h, cmd(&["SADD", "b", "2", "3", "4"]));

    let resp = handle(&mut h, cmd(&["SINTERSTORE", "out", "a", "b"]));
    assert_eq!(resp, RespValue::Integer(2));
    let mut members = array_bulk_strs(&handle(&mut h, cmd(&["SMEMBERS", "out"])));
    members.sort();
    assert_eq!(members, vec!["2", "3"]);

    let resp = handle(&mut h, cmd(&["SUNIONSTORE", "out", "a", "b"]));
    assert_eq!(resp, RespValue::Integer(4));
    let mut members = array_bulk_strs(&handle(&mut h, cmd(&["SMEMBERS", "out"])));
    members.sort();
    assert_eq!(members, vec!["1", "2", "3", "4"]);

    let resp = handle(&mut h, cmd(&["SDIFFSTORE", "out", "a", "b"]));
    assert_eq!(resp, RespValue::Integer(1));
    let members = array_bulk_strs(&handle(&mut h, cmd(&["SMEMBERS", "out"])));
    assert_eq!(members, vec!["1"]);

    // Empty result deletes destination.
    handle(&mut h, cmd(&["SADD", "c", "9"]));
    handle(&mut h, cmd(&["SADD", "d", "9"]));
    let resp = handle(&mut h, cmd(&["SDIFFSTORE", "out", "c", "d"]));
    assert_eq!(resp, RespValue::Integer(0));
    let resp = handle(&mut h, cmd(&["EXISTS", "out"]));
    assert_eq!(resp, RespValue::Integer(0));

    // Overwrite non-set destination.
    handle(&mut h, cmd(&["SET", "strdest", "hello"]));
    handle(&mut h, cmd(&["SADD", "only1", "1"]));
    let resp = handle(&mut h, cmd(&["SUNIONSTORE", "strdest", "only1"]));
    assert_eq!(resp, RespValue::Integer(1));
    let resp = handle(&mut h, cmd(&["TYPE", "strdest"]));
    assert_eq!(resp, RespValue::SimpleString(Bytes::from_static(b"set")));
}

#[test]
fn test_smove() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    handle(&mut h, cmd(&["SADD", "src", "x", "y"]));
    handle(&mut h, cmd(&["SADD", "dst", "z"]));

    let resp = handle(&mut h, cmd(&["SMOVE", "src", "dst", "x"]));
    assert_eq!(resp, RespValue::Integer(1));
    assert_eq!(
        handle(&mut h, cmd(&["SISMEMBER", "src", "x"])),
        RespValue::Integer(0)
    );
    assert_eq!(
        handle(&mut h, cmd(&["SISMEMBER", "dst", "x"])),
        RespValue::Integer(1)
    );

    // Missing member.
    let resp = handle(&mut h, cmd(&["SMOVE", "src", "dst", "nope"]));
    assert_eq!(resp, RespValue::Integer(0));

    // Same-key no-op when present.
    let resp = handle(&mut h, cmd(&["SMOVE", "src", "src", "y"]));
    assert_eq!(resp, RespValue::Integer(1));

    // Creates dest if absent.
    let resp = handle(&mut h, cmd(&["SMOVE", "src", "newdst", "y"]));
    assert_eq!(resp, RespValue::Integer(1));
    assert_eq!(
        handle(&mut h, cmd(&["SCARD", "src"])),
        RespValue::Integer(0)
    );
    assert_eq!(
        handle(&mut h, cmd(&["SISMEMBER", "newdst", "y"])),
        RespValue::Integer(1)
    );

    // WRONGTYPE on dest.
    handle(&mut h, cmd(&["SET", "str", "v"]));
    handle(&mut h, cmd(&["SADD", "s2", "m"]));
    let resp = handle(&mut h, cmd(&["SMOVE", "s2", "str", "m"]));
    assert!(is_wrongtype(&resp), "expected WRONGTYPE, got {:?}", resp);
}

#[test]
fn test_spop_srandmember() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    handle(&mut h, cmd(&["SADD", "s", "a", "b", "c", "d"]));

    // SRANDMEMBER single: bulk string, set unchanged.
    let resp = handle(&mut h, cmd(&["SRANDMEMBER", "s"]));
    match resp {
        RespValue::BulkString(Some(m)) => {
            let s = String::from_utf8_lossy(&m).into_owned();
            assert!(["a", "b", "c", "d"].contains(&s.as_str()));
        }
        other => panic!("expected bulk member, got {:?}", other),
    }
    assert_eq!(
        handle(&mut h, cmd(&["SCARD", "s"])),
        RespValue::Integer(4)
    );

    let resp = handle(&mut h, cmd(&["SRANDMEMBER", "s", "2"]));
    assert_eq!(array_bulk_strs(&resp).len(), 2);

    let resp = handle(&mut h, cmd(&["SRANDMEMBER", "s", "-3"]));
    assert_eq!(array_bulk_strs(&resp).len(), 3);

    // SPOP single.
    let resp = handle(&mut h, cmd(&["SPOP", "s"]));
    match resp {
        RespValue::BulkString(Some(_)) => {}
        other => panic!("expected popped member, got {:?}", other),
    }
    assert_eq!(
        handle(&mut h, cmd(&["SCARD", "s"])),
        RespValue::Integer(3)
    );

    // SPOP count.
    let resp = handle(&mut h, cmd(&["SPOP", "s", "2"]));
    assert_eq!(array_bulk_strs(&resp).len(), 2);
    assert_eq!(
        handle(&mut h, cmd(&["SCARD", "s"])),
        RespValue::Integer(1)
    );

    // Empty key.
    let resp = handle(&mut h, cmd(&["SPOP", "missing"]));
    assert_eq!(resp, RespValue::BulkString(None));
    let resp = handle(&mut h, cmd(&["SRANDMEMBER", "missing", "5"]));
    assert_eq!(array_bulk_strs(&resp), Vec::<String>::new());
}

#[test]
fn test_set_algebra_wrongtype() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    handle(&mut h, cmd(&["SET", "str", "v"]));
    handle(&mut h, cmd(&["SADD", "s", "m"]));

    for cmd_name in ["SINTER", "SUNION", "SDIFF"] {
        let resp = handle(&mut h, cmd(&[cmd_name, "s", "str"]));
        assert!(
            is_wrongtype(&resp),
            "{} expected WRONGTYPE, got {:?}",
            cmd_name,
            resp
        );
    }
    let resp = handle(&mut h, cmd(&["SINTERSTORE", "out", "s", "str"]));
    assert!(is_wrongtype(&resp));
}

#[test]
fn test_flush_clears_new_types() {
    let cache = make_cache();
    let mut h = make_handler(cache.clone());

    handle(&mut h, cmd(&["HSET", "h", "f", "v"]));
    handle(&mut h, cmd(&["LPUSH", "l", "a"]));
    handle(&mut h, cmd(&["SADD", "s", "m"]));
    assert_eq!(cache.dbsize(), 3);

    handle(&mut h, cmd(&["FLUSHDB"]));
    assert_eq!(cache.dbsize(), 0);
    assert_eq!(cache.key_type(&Bytes::from("h")), KeyType::None);
}
