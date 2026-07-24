//! Batch AJ: HSCAN / SSCAN / ZSCAN.

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

fn as_bulk_str(v: &RespValue) -> Option<String> {
    match v {
        RespValue::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    }
}

fn parse_scan_reply(resp: &RespValue) -> (String, Vec<String>) {
    match resp {
        RespValue::Array(arr) if arr.len() == 2 => {
            let cursor = as_bulk_str(&arr[0]).expect("cursor bulk");
            let elems = match &arr[1] {
                RespValue::Array(inner) => inner.iter().filter_map(as_bulk_str).collect(),
                other => panic!("expected element array, got {:?}", other),
            };
            (cursor, elems)
        }
        other => panic!("expected scan reply, got {:?}", other),
    }
}

fn scan_all(h: &mut CommandHandler, base: &[&str], count: &str) -> Vec<String> {
    let mut cursor = "0".to_string();
    let mut out = Vec::new();
    loop {
        let mut parts: Vec<&str> = base.to_vec();
        parts.push(&cursor);
        parts.push("COUNT");
        parts.push(count);
        let (next, batch) = parse_scan_reply(&handle(h, cmd(&parts)));
        out.extend(batch);
        if next == "0" {
            break;
        }
        cursor = next;
    }
    out
}

#[test]
fn test_hscan_full_and_match() {
    let mut h = make_handler(make_cache());
    handle(
        &mut h,
        cmd(&["HSET", "user", "name", "alice", "age", "30", "email", "a@b.c", "city", "seoul"]),
    );

    let elems = scan_all(&mut h, &["HSCAN", "user"], "2");
    // field/value pairs, sorted by field
    assert_eq!(
        elems,
        vec!["age", "30", "city", "seoul", "email", "a@b.c", "name", "alice"]
    );

    let (cur, batch) = parse_scan_reply(&handle(
        &mut h,
        cmd(&["HSCAN", "user", "0", "MATCH", "e*", "COUNT", "10"]),
    ));
    assert_eq!(cur, "0");
    assert_eq!(batch, vec!["email", "a@b.c"]);
}

#[test]
fn test_sscan_and_zscan() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["SADD", "tags", "red", "blue", "green", "yellow"]));
    let mut members = scan_all(&mut h, &["SSCAN", "tags"], "2");
    members.sort();
    assert_eq!(members, vec!["blue", "green", "red", "yellow"]);

    let (_, batch) = parse_scan_reply(&handle(
        &mut h,
        cmd(&["SSCAN", "tags", "0", "MATCH", "bl*", "COUNT", "10"]),
    ));
    assert_eq!(batch, vec!["blue"]);

    handle(
        &mut h,
        cmd(&["ZADD", "lb", "1", "alice", "2", "bob", "3", "carol", "4", "dave"]),
    );
    let elems = scan_all(&mut h, &["ZSCAN", "lb"], "2");
    assert_eq!(
        elems,
        vec!["alice", "1", "bob", "2", "carol", "3", "dave", "4"]
    );

    let (_, batch) = parse_scan_reply(&handle(
        &mut h,
        cmd(&["ZSCAN", "lb", "0", "MATCH", "c*", "COUNT", "10"]),
    ));
    assert_eq!(batch, vec!["carol", "3"]);
}

#[test]
fn test_type_scan_missing_and_wrongtype() {
    let mut h = make_handler(make_cache());

    let (cur, elems) = parse_scan_reply(&handle(&mut h, cmd(&["HSCAN", "nope", "0"])));
    assert_eq!(cur, "0");
    assert!(elems.is_empty());

    let (cur, elems) = parse_scan_reply(&handle(&mut h, cmd(&["SSCAN", "nope", "0"])));
    assert_eq!(cur, "0");
    assert!(elems.is_empty());

    let (cur, elems) = parse_scan_reply(&handle(&mut h, cmd(&["ZSCAN", "nope", "0"])));
    assert_eq!(cur, "0");
    assert!(elems.is_empty());

    handle(&mut h, cmd(&["SET", "s", "v"]));
    for c in [
        &["HSCAN", "s", "0"][..],
        &["SSCAN", "s", "0"][..],
        &["ZSCAN", "s", "0"][..],
    ] {
        match handle(&mut h, cmd(c)) {
            RespValue::Error(e) => {
                assert!(String::from_utf8_lossy(&e).starts_with("WRONGTYPE"));
            }
            other => panic!("expected WRONGTYPE for {:?}, got {:?}", c, other),
        }
    }
}
