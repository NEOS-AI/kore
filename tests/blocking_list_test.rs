//! BLPOP / BRPOP blocking list operations

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
fn blpop_immediate_when_list_has_data() {
    let cache = make_cache();
    let mut h = make_handler(cache);
    handle(&mut h, cmd(&["LPUSH", "q", "a", "b"]));
    // list is [b, a] after two lpush
    let resp = handle(&mut h, cmd(&["BLPOP", "q", "1"]));
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(as_bulk_str(&arr[0]).as_deref(), Some("q"));
            assert_eq!(as_bulk_str(&arr[1]).as_deref(), Some("b")); // left pop
        }
        other => panic!("expected array, got {:?}", other),
    }
}

#[test]
fn brpop_immediate_right_side() {
    let cache = make_cache();
    let mut h = make_handler(cache);
    handle(&mut h, cmd(&["RPUSH", "q", "x", "y", "z"]));
    let resp = handle(&mut h, cmd(&["BRPOP", "q", "1"]));
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(as_bulk_str(&arr[0]).as_deref(), Some("q"));
            assert_eq!(as_bulk_str(&arr[1]).as_deref(), Some("z"));
        }
        other => panic!("expected array, got {:?}", other),
    }
}

#[test]
fn blpop_timeout_returns_null_array() {
    let cache = make_cache();
    let mut h = make_handler(cache);
    let start = std::time::Instant::now();
    let resp = handle(&mut h, cmd(&["BLPOP", "empty", "1"]));
    assert!(start.elapsed() >= Duration::from_millis(900));
    assert_eq!(resp, RespValue::NullArray);
}

#[test]
fn blpop_wrongtype() {
    let cache = make_cache();
    let mut h = make_handler(cache);
    handle(&mut h, cmd(&["SET", "notlist", "v"]));
    let resp = handle(&mut h, cmd(&["BLPOP", "notlist", "1"]));
    match resp {
        RespValue::Error(e) => {
            assert!(String::from_utf8_lossy(&e).contains("WRONGTYPE"));
        }
        other => panic!("expected error, got {:?}", other),
    }
}

#[test]
fn blpop_multi_key_order() {
    let cache = make_cache();
    let mut h = make_handler(cache);
    handle(&mut h, cmd(&["RPUSH", "b", "from-b"]));
    handle(&mut h, cmd(&["RPUSH", "a", "from-a"]));
    // First non-empty key in argument order wins
    let resp = handle(&mut h, cmd(&["BLPOP", "missing", "b", "a", "1"]));
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(as_bulk_str(&arr[0]).as_deref(), Some("b"));
            assert_eq!(as_bulk_str(&arr[1]).as_deref(), Some("from-b"));
        }
        other => panic!("expected array, got {:?}", other),
    }
}

#[test]
fn blpop_wakes_on_lpush() {
    let cache = make_cache();
    let cache2 = Arc::clone(&cache);
    let mut h_blocker = make_handler(Arc::clone(&cache));

    let blocker = std::thread::spawn(move || {
        // Block up to 5s
        handle(&mut h_blocker, cmd(&["BLPOP", "wake", "5"]))
    });

    // Give the blocker time to register
    std::thread::sleep(Duration::from_millis(100));

    let mut h_pusher = make_handler(cache2);
    handle(&mut h_pusher, cmd(&["LPUSH", "wake", "payload"]));

    let resp = blocker.join().unwrap();
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(as_bulk_str(&arr[0]).as_deref(), Some("wake"));
            assert_eq!(as_bulk_str(&arr[1]).as_deref(), Some("payload"));
        }
        other => panic!("expected array after wake, got {:?}", other),
    }

    // List should be empty / removed
    let mut h = make_handler(cache);
    let llen = handle(&mut h, cmd(&["LLEN", "wake"]));
    assert_eq!(llen, RespValue::Integer(0));
}

#[test]
fn brpop_wakes_on_rpush() {
    let cache = make_cache();
    let cache2 = Arc::clone(&cache);
    let mut h_blocker = make_handler(Arc::clone(&cache));

    let blocker = std::thread::spawn(move || {
        handle(&mut h_blocker, cmd(&["BRPOP", "rwake", "5"]))
    });

    std::thread::sleep(Duration::from_millis(100));
    let mut h_pusher = make_handler(cache2);
    handle(&mut h_pusher, cmd(&["RPUSH", "rwake", "right"]));

    let resp = blocker.join().unwrap();
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(as_bulk_str(&arr[1]).as_deref(), Some("right"));
        }
        other => panic!("expected array, got {:?}", other),
    }
}

#[test]
fn blpop_removes_empty_list_key() {
    let cache = make_cache();
    let mut h = make_handler(cache);
    handle(&mut h, cmd(&["LPUSH", "one", "only"]));
    let resp = handle(&mut h, cmd(&["BLPOP", "one", "1"]));
    assert!(matches!(resp, RespValue::Array(_)));
    let t = handle(&mut h, cmd(&["TYPE", "one"]));
    assert_eq!(t, RespValue::SimpleString(Bytes::from_static(b"none")));
}

#[test]
fn blpop_arity_and_negative_timeout() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    let resp = handle(&mut h, cmd(&["BLPOP"]));
    match resp {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("wrong number")),
        other => panic!("{:?}", other),
    }
    let resp = handle(&mut h, cmd(&["BLPOP", "onlykey"]));
    match resp {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("wrong number")),
        other => panic!("{:?}", other),
    }
    let resp = handle(&mut h, cmd(&["BLPOP", "k", "-1"]));
    match resp {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("timeout")),
        other => panic!("{:?}", other),
    }
    let resp = handle(&mut h, cmd(&["BRPOP", "k", "not-a-number"]));
    match resp {
        RespValue::Error(e) => {
            let m = String::from_utf8_lossy(&e);
            assert!(m.contains("timeout") || m.contains("float") || m.contains("integer"));
        }
        other => panic!("{:?}", other),
    }
}

#[test]
fn blpop_zero_timeout_blocks_until_push() {
    // timeout 0 means block forever — we still wake on push
    let cache = make_cache();
    let cache2 = Arc::clone(&cache);
    let mut h_blocker = make_handler(Arc::clone(&cache));

    let blocker = std::thread::spawn(move || {
        handle(&mut h_blocker, cmd(&["BLPOP", "forever", "0"]))
    });

    std::thread::sleep(Duration::from_millis(150));
    let mut h_pusher = make_handler(cache2);
    handle(&mut h_pusher, cmd(&["RPUSH", "forever", "item"]));

    let resp = blocker.join().unwrap();
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(as_bulk_str(&arr[0]).as_deref(), Some("forever"));
            assert_eq!(as_bulk_str(&arr[1]).as_deref(), Some("item"));
        }
        other => panic!("expected array, got {:?}", other),
    }
}

#[test]
fn blpop_two_waiters_fifo_best_effort() {
    // Two clients block; one push should only satisfy one waiter
    let cache = make_cache();
    let c1 = Arc::clone(&cache);
    let c2 = Arc::clone(&cache);
    let c3 = Arc::clone(&cache);

    let w1 = std::thread::spawn(move || {
        let mut h = make_handler(c1);
        handle(&mut h, cmd(&["BLPOP", "fifo", "3"]))
    });
    std::thread::sleep(Duration::from_millis(50));
    let w2 = std::thread::spawn(move || {
        let mut h = make_handler(c2);
        handle(&mut h, cmd(&["BLPOP", "fifo", "3"]))
    });
    std::thread::sleep(Duration::from_millis(100));

    let mut h = make_handler(c3);
    handle(&mut h, cmd(&["LPUSH", "fifo", "only-one"]));

    // Collect both results — one should get the element, one timeout (NullArray)
    let r1 = w1.join().unwrap();
    let r2 = w2.join().unwrap();
    let got = [&r1, &r2]
        .iter()
        .filter(|r| matches!(r, RespValue::Array(_)))
        .count();
    let timed = [&r1, &r2]
        .iter()
        .filter(|r| matches!(r, RespValue::NullArray))
        .count();
    assert_eq!(got, 1, "exactly one waiter should receive the element");
    assert_eq!(timed, 1, "the other waiter should time out");
}

#[test]
fn brpop_multi_key_leftmost_wins() {
    let cache = make_cache();
    let mut h = make_handler(cache);
    handle(&mut h, cmd(&["RPUSH", "k2", "v2"]));
    handle(&mut h, cmd(&["RPUSH", "k3", "v3"]));
    let resp = handle(&mut h, cmd(&["BRPOP", "k1", "k2", "k3", "1"]));
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(as_bulk_str(&arr[0]).as_deref(), Some("k2"));
            assert_eq!(as_bulk_str(&arr[1]).as_deref(), Some("v2"));
        }
        other => panic!("{:?}", other),
    }
}

#[test]
fn lpush_notifies_blpop_across_handlers_sharing_cache() {
    let cache = make_cache();
    let cache_b = Arc::clone(&cache);
    let mut blocker = make_handler(Arc::clone(&cache));
    let t = std::thread::spawn(move || handle(&mut blocker, cmd(&["BLPOP", "shared", "2"])));
    std::thread::sleep(Duration::from_millis(80));
    let mut pusher = make_handler(cache_b);
    // Multiple values — left push puts "first" at head for BLPOP
    handle(&mut pusher, cmd(&["LPUSH", "shared", "second", "first"]));
    let resp = t.join().unwrap();
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(as_bulk_str(&arr[1]).as_deref(), Some("first"));
        }
        other => panic!("{:?}", other),
    }
}
