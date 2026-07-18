//! Batch AN: LPOS / LMOVE / BLMOVE.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::protocol::RespValue;
use kore::Cache;
use std::sync::Arc;
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

fn array_ints(v: &RespValue) -> Vec<i64> {
    match v {
        RespValue::Array(arr) => arr
            .iter()
            .map(|x| match x {
                RespValue::Integer(i) => *i,
                other => panic!("expected integer, got {:?}", other),
            })
            .collect(),
        other => panic!("expected array, got {:?}", other),
    }
}

#[test]
fn test_lpos_basic_rank_count_maxlen() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["RPUSH", "l", "a", "b", "a", "c", "a"]));

    assert_eq!(
        handle(&mut h, cmd(&["LPOS", "l", "a"])),
        RespValue::Integer(0)
    );
    assert_eq!(
        handle(&mut h, cmd(&["LPOS", "l", "a", "RANK", "2"])),
        RespValue::Integer(2)
    );
    assert_eq!(
        handle(&mut h, cmd(&["LPOS", "l", "a", "RANK", "-1"])),
        RespValue::Integer(4)
    );
    assert_eq!(
        handle(&mut h, cmd(&["LPOS", "l", "missing"])),
        RespValue::null()
    );

    let resp = handle(&mut h, cmd(&["LPOS", "l", "a", "COUNT", "0"]));
    assert_eq!(array_ints(&resp), vec![0, 2, 4]);

    let resp = handle(&mut h, cmd(&["LPOS", "l", "a", "RANK", "2", "COUNT", "2"]));
    assert_eq!(array_ints(&resp), vec![2, 4]);

    let resp = handle(&mut h, cmd(&["LPOS", "l", "a", "COUNT", "0", "MAXLEN", "3"]));
    assert_eq!(array_ints(&resp), vec![0, 2]);

    // Missing key + COUNT → empty array
    let resp = handle(&mut h, cmd(&["LPOS", "nope", "a", "COUNT", "2"]));
    assert_eq!(array_ints(&resp), Vec::<i64>::new());
}

#[test]
fn test_lpos_wrongtype_and_rank_zero() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["SET", "s", "x"]));
    match handle(&mut h, cmd(&["LPOS", "s", "x"])) {
        RespValue::Error(e) => {
            assert!(String::from_utf8_lossy(&e).contains("WRONGTYPE"));
        }
        other => panic!("{:?}", other),
    }
    handle(&mut h, cmd(&["RPUSH", "l", "a"]));
    match handle(&mut h, cmd(&["LPOS", "l", "a", "RANK", "0"])) {
        RespValue::Error(e) => {
            assert!(String::from_utf8_lossy(&e).contains("RANK"));
        }
        other => panic!("{:?}", other),
    }
}

#[test]
fn test_lmove_sides_and_same_key() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["RPUSH", "src", "a", "b", "c"]));
    handle(&mut h, cmd(&["RPUSH", "dst", "x"]));

    // LEFT→RIGHT: pop a from src, push right on dst
    let resp = handle(&mut h, cmd(&["LMOVE", "src", "dst", "LEFT", "RIGHT"]));
    assert_eq!(as_bulk_str(&resp).as_deref(), Some("a"));
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["LINDEX", "dst", "-1"]))).as_deref(),
        Some("a")
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["LINDEX", "src", "0"]))).as_deref(),
        Some("b")
    );

    // Same-key rotate RIGHT→LEFT: c moves to head (list is b c)
    let resp = handle(&mut h, cmd(&["LMOVE", "src", "src", "RIGHT", "LEFT"]));
    assert_eq!(as_bulk_str(&resp).as_deref(), Some("c"));
    // head is c, then b
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["LINDEX", "src", "0"]))).as_deref(),
        Some("c")
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["LINDEX", "src", "1"]))).as_deref(),
        Some("b")
    );

    // Empty source
    handle(&mut h, cmd(&["LPOP", "src"]));
    handle(&mut h, cmd(&["LPOP", "src"]));
    assert_eq!(
        handle(&mut h, cmd(&["LMOVE", "src", "dst", "LEFT", "LEFT"])),
        RespValue::null()
    );
    assert_eq!(
        handle(&mut h, cmd(&["EXISTS", "src"])),
        RespValue::Integer(0)
    );
}

#[test]
fn test_lmove_wrongtype() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["RPUSH", "src", "a"]));
    handle(&mut h, cmd(&["SET", "str", "nope"]));
    match handle(&mut h, cmd(&["LMOVE", "src", "str", "LEFT", "LEFT"])) {
        RespValue::Error(e) => {
            assert!(String::from_utf8_lossy(&e).contains("WRONGTYPE"));
        }
        other => panic!("{:?}", other),
    }
    match handle(&mut h, cmd(&["LMOVE", "str", "dst", "LEFT", "LEFT"])) {
        RespValue::Error(e) => {
            assert!(String::from_utf8_lossy(&e).contains("WRONGTYPE"));
        }
        other => panic!("{:?}", other),
    }
}

#[test]
fn test_blmove_timeout_and_wake() {
    let cache = make_cache();
    let mut h = make_handler(Arc::clone(&cache));

    // Timeout → null bulk
    let resp = handle(&mut h, cmd(&["BLMOVE", "empty", "dst", "LEFT", "RIGHT", "0.2"]));
    assert_eq!(resp, RespValue::null());

    // Wake on push to source
    let cache2 = Arc::clone(&cache);
    let mut h_blocker = make_handler(Arc::clone(&cache));
    let blocker = std::thread::spawn(move || {
        handle(
            &mut h_blocker,
            cmd(&["BLMOVE", "wake", "sink", "LEFT", "RIGHT", "5"]),
        )
    });
    std::thread::sleep(Duration::from_millis(100));
    let mut h_pusher = make_handler(cache2);
    handle(&mut h_pusher, cmd(&["LPUSH", "wake", "payload"]));
    let resp = blocker.join().unwrap();
    assert_eq!(as_bulk_str(&resp).as_deref(), Some("payload"));

    let mut h = make_handler(cache);
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["LINDEX", "sink", "0"]))).as_deref(),
        Some("payload")
    );
    assert_eq!(
        handle(&mut h, cmd(&["EXISTS", "wake"])),
        RespValue::Integer(0)
    );
}

#[test]
fn test_blmove_arity() {
    let mut h = make_handler(make_cache());
    match handle(&mut h, cmd(&["BLMOVE", "a", "b", "LEFT"])) {
        RespValue::Error(e) => {
            assert!(String::from_utf8_lossy(&e).contains("wrong number"));
        }
        other => panic!("{:?}", other),
    }
    match handle(&mut h, cmd(&["LMOVE", "a", "b", "UP", "LEFT"])) {
        RespValue::Error(e) => {
            assert!(String::from_utf8_lossy(&e).contains("syntax"));
        }
        other => panic!("{:?}", other),
    }
}
