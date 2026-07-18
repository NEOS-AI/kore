//! Batch BE: modern ZRANGE (BYSCORE/BYLEX/REV/LIMIT), CLIENT NO-EVICT/NO-TOUCH.

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

fn array_bulk_strs(v: &RespValue) -> Vec<String> {
    match v {
        RespValue::Array(items) => items.iter().filter_map(as_bulk_str).collect(),
        _ => panic!("expected array, got {:?}", v),
    }
}

#[test]
fn zrange_legacy_and_byscore_rev_limit() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    handle(
        &mut h,
        cmd(&["ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e"]),
    );

    // Legacy rank form still works
    assert_eq!(
        array_bulk_strs(&handle(&mut h, cmd(&["ZRANGE", "z", "0", "2"]))),
        vec!["a", "b", "c"]
    );
    assert_eq!(
        array_bulk_strs(&handle(
            &mut h,
            cmd(&["ZRANGE", "z", "0", "-1", "WITHSCORES"])
        )),
        vec!["a", "1", "b", "2", "c", "3", "d", "4", "e", "5"]
    );

    // BYSCORE
    assert_eq!(
        array_bulk_strs(&handle(
            &mut h,
            cmd(&["ZRANGE", "z", "2", "4", "BYSCORE"])
        )),
        vec!["b", "c", "d"]
    );

    // REV + BYSCORE (args are max min)
    assert_eq!(
        array_bulk_strs(&handle(
            &mut h,
            cmd(&["ZRANGE", "z", "4", "2", "BYSCORE", "REV"])
        )),
        vec!["d", "c", "b"]
    );

    // LIMIT
    assert_eq!(
        array_bulk_strs(&handle(
            &mut h,
            cmd(&["ZRANGE", "z", "1", "5", "BYSCORE", "LIMIT", "1", "2"])
        )),
        vec!["b", "c"]
    );

    // Rank + REV
    assert_eq!(
        array_bulk_strs(&handle(
            &mut h,
            cmd(&["ZRANGE", "z", "0", "1", "REV"])
        )),
        vec!["e", "d"]
    );
}

#[test]
fn zrange_bylex() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    // Lex ranges require same score
    handle(
        &mut h,
        cmd(&["ZADD", "z", "0", "apple", "0", "banana", "0", "cherry", "0", "date"]),
    );

    assert_eq!(
        array_bulk_strs(&handle(
            &mut h,
            cmd(&["ZRANGE", "z", "[banana", "[date", "BYLEX"])
        )),
        vec!["banana", "cherry", "date"]
    );

    assert_eq!(
        array_bulk_strs(&handle(
            &mut h,
            cmd(&["ZRANGE", "z", "[date", "[banana", "BYLEX", "REV", "LIMIT", "0", "2"])
        )),
        vec!["date", "cherry"]
    );
}

#[test]
fn client_no_evict_no_touch() {
    let cache = make_cache();
    let mut h = make_handler(cache);

    assert_eq!(
        handle(&mut h, cmd(&["CLIENT", "NO-EVICT", "ON"])),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["CLIENT", "NO-TOUCH", "ON"])),
        RespValue::ok()
    );

    match handle(&mut h, cmd(&["CLIENT", "INFO"])) {
        RespValue::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("no-evict=1"), "{}", s);
            assert!(s.contains("no-touch=1"), "{}", s);
        }
        other => panic!("{:?}", other),
    }

    // NO-TOUCH: GET should still return value
    handle(&mut h, cmd(&["SET", "k", "v"]));
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GET", "k"]))).as_deref(),
        Some("v")
    );

    assert_eq!(
        handle(&mut h, cmd(&["CLIENT", "NO-EVICT", "OFF"])),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["CLIENT", "NO-TOUCH", "OFF"])),
        RespValue::ok()
    );

    match handle(&mut h, cmd(&["CLIENT", "INFO"])) {
        RespValue::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("no-evict=0"), "{}", s);
            assert!(s.contains("no-touch=0"), "{}", s);
        }
        other => panic!("{:?}", other),
    }

    // RESET clears flags
    handle(&mut h, cmd(&["CLIENT", "NO-TOUCH", "ON"]));
    assert_eq!(
        handle(&mut h, cmd(&["RESET"])),
        RespValue::SimpleString(Bytes::from_static(b"RESET"))
    );
    match handle(&mut h, cmd(&["CLIENT", "INFO"])) {
        RespValue::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("no-touch=0"), "{}", s);
        }
        other => panic!("{:?}", other),
    }
}
