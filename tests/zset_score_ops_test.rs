//! Batch AI: ZINCRBY, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZCOUNT, ZREMRANGEBY*.

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

fn array_bulk_strs(v: &RespValue) -> Vec<String> {
    match v {
        RespValue::Array(arr) => arr.iter().filter_map(as_bulk_str).collect(),
        _ => panic!("expected array, got {:?}", v),
    }
}

fn seed_board(h: &mut CommandHandler) {
    handle(h, cmd(&["ZADD", "lb", "10", "a", "20", "b", "30", "c", "40", "d", "50", "e"]));
}

#[test]
fn test_zincrby() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["ZADD", "z", "1", "m"]));
    let resp = handle(&mut h, cmd(&["ZINCRBY", "z", "2.5", "m"]));
    assert_eq!(as_bulk_str(&resp).unwrap(), "3.5");
    let resp = handle(&mut h, cmd(&["ZINCRBY", "z", "1", "new"]));
    assert_eq!(as_bulk_str(&resp).unwrap(), "1");
    assert_eq!(
        handle(&mut h, cmd(&["ZCARD", "z"])),
        RespValue::Integer(2)
    );
}

#[test]
fn test_zrangebyscore_and_zcount() {
    let mut h = make_handler(make_cache());
    seed_board(&mut h);

    let resp = handle(&mut h, cmd(&["ZRANGEBYSCORE", "lb", "20", "40"]));
    assert_eq!(array_bulk_strs(&resp), vec!["b", "c", "d"]);

    let resp = handle(
        &mut h,
        cmd(&["ZRANGEBYSCORE", "lb", "(20", "(40", "WITHSCORES"]),
    );
    assert_eq!(array_bulk_strs(&resp), vec!["c", "30"]);

    let resp = handle(
        &mut h,
        cmd(&["ZRANGEBYSCORE", "lb", "-inf", "+inf", "LIMIT", "1", "2"]),
    );
    assert_eq!(array_bulk_strs(&resp), vec!["b", "c"]);

    assert_eq!(
        handle(&mut h, cmd(&["ZCOUNT", "lb", "20", "40"])),
        RespValue::Integer(3)
    );
    assert_eq!(
        handle(&mut h, cmd(&["ZCOUNT", "lb", "(20", "40"])),
        RespValue::Integer(2)
    );
}

#[test]
fn test_zrevrangebyscore() {
    let mut h = make_handler(make_cache());
    seed_board(&mut h);

    // Redis: ZREVRANGEBYSCORE key max min
    let resp = handle(&mut h, cmd(&["ZREVRANGEBYSCORE", "lb", "40", "20"]));
    assert_eq!(array_bulk_strs(&resp), vec!["d", "c", "b"]);

    let resp = handle(
        &mut h,
        cmd(&["ZREVRANGEBYSCORE", "lb", "+inf", "-inf", "LIMIT", "0", "2"]),
    );
    assert_eq!(array_bulk_strs(&resp), vec!["e", "d"]);
}

#[test]
fn test_zremrangebyrank_and_score() {
    let cache = make_cache();
    let mut h = make_handler(cache.clone());
    seed_board(&mut h);

    let resp = handle(&mut h, cmd(&["ZREMRANGEBYRANK", "lb", "0", "1"]));
    assert_eq!(resp, RespValue::Integer(2)); // a, b
    assert_eq!(
        array_bulk_strs(&handle(&mut h, cmd(&["ZRANGE", "lb", "0", "-1"]))),
        vec!["c", "d", "e"]
    );

    let resp = handle(&mut h, cmd(&["ZREMRANGEBYSCORE", "lb", "30", "40"]));
    assert_eq!(resp, RespValue::Integer(2)); // c, d
    assert_eq!(
        array_bulk_strs(&handle(&mut h, cmd(&["ZRANGE", "lb", "0", "-1"]))),
        vec!["e"]
    );

    // Empty key after full score remove
    let resp = handle(&mut h, cmd(&["ZREMRANGEBYSCORE", "lb", "-inf", "+inf"]));
    assert_eq!(resp, RespValue::Integer(1));
    assert_eq!(
        handle(&mut h, cmd(&["EXISTS", "lb"])),
        RespValue::Integer(0)
    );
}

#[test]
fn test_zset_score_ops_wrongtype() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["SET", "s", "v"]));
    for c in [
        &["ZINCRBY", "s", "1", "m"][..],
        &["ZRANGEBYSCORE", "s", "0", "1"][..],
        &["ZCOUNT", "s", "0", "1"][..],
        &["ZREMRANGEBYRANK", "s", "0", "1"][..],
        &["ZREMRANGEBYSCORE", "s", "0", "1"][..],
    ] {
        let resp = handle(&mut h, cmd(c));
        match resp {
            RespValue::Error(e) => {
                assert!(
                    String::from_utf8_lossy(&e).starts_with("WRONGTYPE"),
                    "cmd {:?} got {:?}",
                    c,
                    e
                );
            }
            other => panic!("expected WRONGTYPE for {:?}, got {:?}", c, other),
        }
    }
}
