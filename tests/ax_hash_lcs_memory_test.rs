//! Batch AX: HGETDEL, ZRANK/ZREVRANK WITHSCORE, LCS, MEMORY USAGE, OBJECT ENCODING.

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
fn hgetdel_returns_values_and_deletes_fields() {
    let mut h = make_handler(make_cache());
    assert!(matches!(
        handle(&mut h, cmd(&["HSET", "h", "a", "1", "b", "2", "c", "3"])),
        RespValue::Integer(3)
    ));

    let resp = handle(&mut h, cmd(&["HGETDEL", "h", "a", "missing", "c"]));
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(as_bulk_str(&items[0]).as_deref(), Some("1"));
            assert!(matches!(items[1], RespValue::BulkString(None) | RespValue::Null));
            assert_eq!(as_bulk_str(&items[2]).as_deref(), Some("3"));
        }
        other => panic!("expected array, got {:?}", other),
    }

    assert!(matches!(
        handle(&mut h, cmd(&["HEXISTS", "h", "a"])),
        RespValue::Integer(0)
    ));
    assert!(matches!(
        handle(&mut h, cmd(&["HEXISTS", "h", "b"])),
        RespValue::Integer(1)
    ));
    assert!(matches!(
        handle(&mut h, cmd(&["HLEN", "h"])),
        RespValue::Integer(1)
    ));
}

#[test]
fn hgetdel_removes_empty_hash_and_wrongtype() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["HSET", "h", "only", "v"]));
    let resp = handle(&mut h, cmd(&["HGETDEL", "h", "only"]));
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 1);
            assert_eq!(as_bulk_str(&items[0]).as_deref(), Some("v"));
        }
        other => panic!("{:?}", other),
    }
    assert!(matches!(
        handle(&mut h, cmd(&["EXISTS", "h"])),
        RespValue::Integer(0)
    ));

    // Missing key → nulls
    let resp = handle(&mut h, cmd(&["HGETDEL", "nope", "f"]));
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 1);
            assert!(matches!(items[0], RespValue::BulkString(None) | RespValue::Null));
        }
        other => panic!("{:?}", other),
    }

    handle(&mut h, cmd(&["SET", "s", "x"]));
    assert!(matches!(
        handle(&mut h, cmd(&["HGETDEL", "s", "f"])),
        RespValue::Error(_)
    ));
}

#[test]
fn zrank_withscore_and_zrevrank_withscore() {
    let mut h = make_handler(make_cache());
    handle(
        &mut h,
        cmd(&["ZADD", "z", "1", "a", "2", "b", "3", "c"]),
    );

    // Ascending: a=0, b=1, c=2
    assert!(matches!(
        handle(&mut h, cmd(&["ZRANK", "z", "b"])),
        RespValue::Integer(1)
    ));
    match handle(&mut h, cmd(&["ZRANK", "z", "b", "WITHSCORE"])) {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 2);
            assert!(matches!(items[0], RespValue::Integer(1)));
            assert_eq!(as_bulk_str(&items[1]).as_deref(), Some("2"));
        }
        other => panic!("{:?}", other),
    }

    // Descending: c=0, b=1, a=2
    match handle(&mut h, cmd(&["ZREVRANK", "z", "a", "WITHSCORE"])) {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 2);
            assert!(matches!(items[0], RespValue::Integer(2)));
            assert_eq!(as_bulk_str(&items[1]).as_deref(), Some("1"));
        }
        other => panic!("{:?}", other),
    }

    assert!(matches!(
        handle(&mut h, cmd(&["ZRANK", "z", "nope", "WITHSCORE"])),
        RespValue::BulkString(None) | RespValue::Null
    ));
    assert!(matches!(
        handle(&mut h, cmd(&["ZRANK", "z", "a", "NOPE"])),
        RespValue::Error(_)
    ));
}

#[test]
fn lcs_basic_and_len() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["SET", "k1", "ohmytext"]));
    handle(&mut h, cmd(&["SET", "k2", "mynewtext"]));

    // LCS of "ohmytext" and "mynewtext" includes "mytext"
    let resp = handle(&mut h, cmd(&["LCS", "k1", "k2"]));
    let s = as_bulk_str(&resp).expect("bulk LCS");
    assert!(s.contains("my") || s.contains("text") || s.len() >= 4, "lcs={s}");
    // Known LCS length for these Redis docs examples is often "mytext" (6)
    // Verify LEN matches bulk length
    let len_resp = handle(&mut h, cmd(&["LCS", "k1", "k2", "LEN"]));
    match len_resp {
        RespValue::Integer(n) => assert_eq!(n as usize, s.len()),
        other => panic!("{:?}", other),
    }

    // Missing keys treated as empty
    assert!(matches!(
        handle(&mut h, cmd(&["LCS", "missing", "k1", "LEN"])),
        RespValue::Integer(0)
    ));

    handle(&mut h, cmd(&["HSET", "hh", "f", "v"]));
    assert!(matches!(
        handle(&mut h, cmd(&["LCS", "hh", "k1"])),
        RespValue::Error(_)
    ));
}

#[test]
fn memory_usage_and_object_encoding() {
    let mut h = make_handler(make_cache());

    assert!(matches!(
        handle(&mut h, cmd(&["MEMORY", "USAGE", "nope"])),
        RespValue::BulkString(None) | RespValue::Null
    ));
    assert!(matches!(
        handle(&mut h, cmd(&["OBJECT", "ENCODING", "nope"])),
        RespValue::BulkString(None) | RespValue::Null
    ));

    handle(&mut h, cmd(&["SET", "s", "hello"]));
    match handle(&mut h, cmd(&["MEMORY", "USAGE", "s"])) {
        RespValue::Integer(n) => assert!(n > 0, "string usage {n}"),
        other => panic!("{:?}", other),
    }
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["OBJECT", "ENCODING", "s"]))).as_deref(),
        Some("raw")
    );

    handle(&mut h, cmd(&["HSET", "h", "f", "v"]));
    match handle(&mut h, cmd(&["MEMORY", "USAGE", "h", "SAMPLES", "5"])) {
        RespValue::Integer(n) => assert!(n > 0),
        other => panic!("{:?}", other),
    }
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["OBJECT", "ENCODING", "h"]))).as_deref(),
        Some("hashtable")
    );

    handle(&mut h, cmd(&["LPUSH", "l", "a"]));
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["OBJECT", "ENCODING", "l"]))).as_deref(),
        Some("quicklist")
    );

    handle(&mut h, cmd(&["SADD", "set", "m"]));
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["OBJECT", "ENCODING", "set"]))).as_deref(),
        Some("hashtable")
    );

    handle(&mut h, cmd(&["ZADD", "z", "1", "m"]));
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["OBJECT", "ENCODING", "z"]))).as_deref(),
        Some("skiplist")
    );

    // COMMAND catalog entries exist
    match handle(&mut h, cmd(&["COMMAND", "INFO", "hgetdel", "lcs", "memory", "object"])) {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 4);
            for it in &items {
                assert!(matches!(it, RespValue::Array(_)), "{:?}", it);
            }
        }
        other => panic!("{:?}", other),
    }
}
