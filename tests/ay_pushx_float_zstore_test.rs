//! Batch AY: LPUSHX/RPUSHX, PSETEX, INCRBYFLOAT, SUBSTR, TIME, ZRANGESTORE.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::protocol::RespValue;
use kore::Cache;
use std::sync::Arc;
use std::thread;
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
fn lpushx_rpushx_only_if_exists() {
    let mut h = make_handler(make_cache());

    // Missing key → 0
    assert!(matches!(
        handle(&mut h, cmd(&["LPUSHX", "lst", "a"])),
        RespValue::Integer(0)
    ));
    assert!(matches!(
        handle(&mut h, cmd(&["RPUSHX", "lst", "a"])),
        RespValue::Integer(0)
    ));
    assert!(matches!(
        handle(&mut h, cmd(&["EXISTS", "lst"])),
        RespValue::Integer(0)
    ));

    handle(&mut h, cmd(&["LPUSH", "lst", "mid"]));
    assert!(matches!(
        handle(&mut h, cmd(&["LPUSHX", "lst", "left"])),
        RespValue::Integer(2)
    ));
    assert!(matches!(
        handle(&mut h, cmd(&["RPUSHX", "lst", "right"])),
        RespValue::Integer(3)
    ));

    match handle(&mut h, cmd(&["LRANGE", "lst", "0", "-1"])) {
        RespValue::Array(items) => {
            let vals: Vec<_> = items.iter().filter_map(as_bulk_str).collect();
            assert_eq!(vals, vec!["left", "mid", "right"]);
        }
        other => panic!("{:?}", other),
    }

    handle(&mut h, cmd(&["SET", "s", "x"]));
    assert!(matches!(
        handle(&mut h, cmd(&["LPUSHX", "s", "y"])),
        RespValue::Error(_)
    ));
}

#[test]
fn psetex_and_substr() {
    let mut h = make_handler(make_cache());
    assert!(matches!(
        handle(&mut h, cmd(&["PSETEX", "k", "200", "hello-world"])),
        RespValue::SimpleString(_)
    ));
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GET", "k"]))).as_deref(),
        Some("hello-world")
    );
    // SUBSTR == GETRANGE
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["SUBSTR", "k", "0", "4"]))).as_deref(),
        Some("hello")
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GETRANGE", "k", "0", "4"]))).as_deref(),
        Some("hello")
    );

    // TTL in ms should be positive and <= 200
    match handle(&mut h, cmd(&["PTTL", "k"])) {
        RespValue::Integer(n) => assert!(n > 0 && n <= 200, "pttl={n}"),
        other => panic!("{:?}", other),
    }

    thread::sleep(Duration::from_millis(250));
    assert!(matches!(
        handle(&mut h, cmd(&["GET", "k"])),
        RespValue::BulkString(None) | RespValue::Null
    ));
}

#[test]
fn incrbyfloat_basic() {
    let mut h = make_handler(make_cache());
    match handle(&mut h, cmd(&["INCRBYFLOAT", "f", "10.5"])) {
        RespValue::BulkString(Some(b)) => {
            assert_eq!(String::from_utf8_lossy(&b), "10.5");
        }
        other => panic!("{:?}", other),
    }
    match handle(&mut h, cmd(&["INCRBYFLOAT", "f", "0.5"])) {
        RespValue::BulkString(Some(b)) => {
            assert_eq!(String::from_utf8_lossy(&b), "11");
        }
        other => panic!("{:?}", other),
    }
    // Non-float stored value
    handle(&mut h, cmd(&["SET", "s", "abc"]));
    assert!(matches!(
        handle(&mut h, cmd(&["INCRBYFLOAT", "s", "1"])),
        RespValue::Error(_)
    ));
    // Wrong type
    handle(&mut h, cmd(&["HSET", "h", "a", "1"]));
    assert!(matches!(
        handle(&mut h, cmd(&["INCRBYFLOAT", "h", "1"])),
        RespValue::Error(_)
    ));
}

#[test]
fn time_returns_unix_pair() {
    let mut h = make_handler(make_cache());
    match handle(&mut h, cmd(&["TIME"])) {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 2);
            let secs: u64 = as_bulk_str(&items[0]).unwrap().parse().unwrap();
            let usecs: u32 = as_bulk_str(&items[1]).unwrap().parse().unwrap();
            assert!(secs > 1_700_000_000, "secs={secs}"); // after 2023
            assert!(usecs < 1_000_000);
        }
        other => panic!("{:?}", other),
    }
}

#[test]
fn zrangestore_rank_byscore_rev() {
    let mut h = make_handler(make_cache());
    handle(
        &mut h,
        cmd(&["ZADD", "src", "1", "a", "2", "b", "3", "c", "4", "d"]),
    );

    // Rank range 1..2 → b,c
    assert!(matches!(
        handle(&mut h, cmd(&["ZRANGESTORE", "dst", "src", "1", "2"])),
        RespValue::Integer(2)
    ));
    match handle(&mut h, cmd(&["ZRANGE", "dst", "0", "-1", "WITHSCORES"])) {
        RespValue::Array(items) => {
            let flat: Vec<_> = items.iter().filter_map(as_bulk_str).collect();
            assert_eq!(flat, vec!["b", "2", "c", "3"]);
        }
        other => panic!("{:?}", other),
    }

    // BYSCORE + LIMIT
    assert!(matches!(
        handle(
            &mut h,
            cmd(&["ZRANGESTORE", "dst2", "src", "2", "4", "BYSCORE", "LIMIT", "0", "2"])
        ),
        RespValue::Integer(2)
    ));
    match handle(&mut h, cmd(&["ZRANGE", "dst2", "0", "-1"])) {
        RespValue::Array(items) => {
            let flat: Vec<_> = items.iter().filter_map(as_bulk_str).collect();
            assert_eq!(flat, vec!["b", "c"]);
        }
        other => panic!("{:?}", other),
    }

    // REV rank: start=0 stop=1 descending → d,c
    assert!(matches!(
        handle(
            &mut h,
            cmd(&["ZRANGESTORE", "dst3", "src", "0", "1", "REV"])
        ),
        RespValue::Integer(2)
    ));
    match handle(&mut h, cmd(&["ZRANGE", "dst3", "0", "-1"])) {
        RespValue::Array(items) => {
            let flat: Vec<_> = items.iter().filter_map(as_bulk_str).collect();
            assert_eq!(flat, vec!["c", "d"]); // stored scores preserve order by score asc in zrange
        }
        other => panic!("{:?}", other),
    }

    // Empty range deletes dest / yields 0
    handle(&mut h, cmd(&["SET", "wipe", "x"]));
    assert!(matches!(
        handle(
            &mut h,
            cmd(&["ZRANGESTORE", "wipe", "src", "100", "200"])
        ),
        RespValue::Integer(0)
    ));
    assert!(matches!(
        handle(&mut h, cmd(&["EXISTS", "wipe"])),
        RespValue::Integer(0)
    ));

    // Wrong type source
    handle(&mut h, cmd(&["SET", "s", "x"]));
    assert!(matches!(
        handle(&mut h, cmd(&["ZRANGESTORE", "d", "s", "0", "-1"])),
        RespValue::Error(_)
    ));
}
