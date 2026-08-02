//! Batch AW: HSETNX, RPOPLPUSH/BRPOPLPUSH, LMPOP/BLMPOP.

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

fn flatten_lmpop(v: &RespValue) -> Option<(String, Vec<String>)> {
    match v {
        RespValue::BulkString(None) | RespValue::Null => None,
        RespValue::Array(arr) if arr.len() == 2 => {
            let key = as_bulk_str(&arr[0])?;
            let elems = match &arr[1] {
                RespValue::Array(items) => items.iter().filter_map(as_bulk_str).collect(),
                _ => return None,
            };
            Some((key, elems))
        }
        _ => panic!("unexpected lmpop reply: {:?}", v),
    }
}

#[test]
fn test_hsetnx() {
    let mut h = make_handler(make_cache());
    assert_eq!(
        handle(&mut h, cmd(&["HSETNX", "hk", "f", "1"])),
        RespValue::Integer(1)
    );
    assert_eq!(
        handle(&mut h, cmd(&["HGET", "hk", "f"])),
        RespValue::BulkString(Some(Bytes::from("1")))
    );
    // Existing field: no overwrite.
    assert_eq!(
        handle(&mut h, cmd(&["HSETNX", "hk", "f", "2"])),
        RespValue::Integer(0)
    );
    assert_eq!(
        handle(&mut h, cmd(&["HGET", "hk", "f"])),
        RespValue::BulkString(Some(Bytes::from("1")))
    );
    // New field on existing hash.
    assert_eq!(
        handle(&mut h, cmd(&["HSETNX", "hk", "g", "9"])),
        RespValue::Integer(1)
    );

    let _ = handle(&mut h, cmd(&["SET", "s", "x"]));
    match handle(&mut h, cmd(&["HSETNX", "s", "f", "1"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("WRONGTYPE")),
        other => panic!("expected WRONGTYPE, got {:?}", other),
    }
}

#[test]
fn test_rpoplpush() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["RPUSH", "src", "a", "b", "c"]));
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["RPOPLPUSH", "src", "dst"]))).as_deref(),
        Some("c")
    );
    // src: a b ; dst: c (LPUSH side)
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["LPOP", "dst"]))).as_deref(),
        Some("c")
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["RPOP", "src"]))).as_deref(),
        Some("b")
    );

    // Same-key rotate.
    handle(&mut h, cmd(&["RPUSH", "rot", "1", "2", "3"]));
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["RPOPLPUSH", "rot", "rot"]))).as_deref(),
        Some("3")
    );
    // RIGHT pop + LEFT push: 3 1 2
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["LINDEX", "rot", "0"]))).as_deref(),
        Some("3")
    );

    assert_eq!(
        handle(&mut h, cmd(&["RPOPLPUSH", "missing", "dst2"])),
        RespValue::null()
    );
}

#[test]
fn test_lmpop() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["RPUSH", "l1", "a", "b", "c"]));
    handle(&mut h, cmd(&["RPUSH", "l2", "x", "y"]));

    let (key, elems) = flatten_lmpop(&handle(
        &mut h,
        cmd(&["LMPOP", "2", "l1", "l2", "LEFT", "COUNT", "2"]),
    ))
    .expect("pop");
    assert_eq!(key, "l1");
    assert_eq!(elems, vec!["a".to_string(), "b".to_string()]);
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["LPOP", "l1"]))).as_deref(),
        Some("c")
    );

    // RIGHT pop from first non-empty.
    let (key, elems) = flatten_lmpop(&handle(
        &mut h,
        cmd(&["LMPOP", "2", "empty", "l2", "RIGHT"]),
    ))
    .expect("pop right");
    assert_eq!(key, "l2");
    assert_eq!(elems, vec!["y".to_string()]);

    // Empty → null.
    assert!(flatten_lmpop(&handle(
        &mut h,
        cmd(&["LMPOP", "1", "gone", "LEFT"])
    ))
    .is_none());

    let _ = handle(&mut h, cmd(&["SET", "s", "v"]));
    match handle(&mut h, cmd(&["LMPOP", "1", "s", "LEFT"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("WRONGTYPE")),
        other => panic!("expected WRONGTYPE, got {:?}", other),
    }
}

#[test]
fn test_blmpop_timeout() {
    let mut h = make_handler(make_cache());
    let start = std::time::Instant::now();
    assert!(
        flatten_lmpop(&handle(
            &mut h,
            cmd(&["BLMPOP", "0.1", "1", "empty", "LEFT"])
        ))
        .is_none()
    );
    assert!(start.elapsed() >= Duration::from_millis(80));
}

#[test]
fn test_brpoplpush_timeout() {
    let mut h = make_handler(make_cache());
    let start = std::time::Instant::now();
    assert_eq!(
        handle(&mut h, cmd(&["BRPOPLPUSH", "empty", "dst", "0.1"])),
        RespValue::null()
    );
    assert!(start.elapsed() >= Duration::from_millis(80));
}

#[test]
fn test_blmpop_wakes_on_push() {
    let cache = make_cache();
    let mut waiter = make_handler(cache.clone());
    let mut pusher = make_handler(cache);

    let t = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(50));
        handle(&mut pusher, cmd(&["LPUSH", "q", "wake"]));
    });

    let (key, elems) = flatten_lmpop(&handle(
        &mut waiter,
        cmd(&["BLMPOP", "2", "1", "q", "LEFT"]),
    ))
    .expect("should wake");
    assert_eq!(key, "q");
    assert_eq!(elems, vec!["wake".to_string()]);
    t.join().unwrap();
}
