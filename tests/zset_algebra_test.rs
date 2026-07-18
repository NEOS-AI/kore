//! Batch AQ: ZUNION / ZINTER / ZDIFF / ZDIFFSTORE (read + store algebra).

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
        RespValue::Array(arr) => arr.iter().filter_map(as_bulk_str).collect(),
        _ => panic!("expected array, got {:?}", v),
    }
}

fn seed(h: &mut CommandHandler) {
    handle(h, cmd(&["ZADD", "z1", "1", "a", "2", "b", "3", "c"]));
    handle(h, cmd(&["ZADD", "z2", "1", "b", "2", "c", "3", "d"]));
}

#[test]
fn test_zunion_zinter_withscores() {
    let mut h = make_handler(make_cache());
    seed(&mut h);

    let resp = handle(&mut h, cmd(&["ZUNION", "2", "z1", "z2", "WITHSCORES"]));
    // a:1, b:3, d:3, c:5 — score order
    assert_eq!(
        array_bulk_strs(&resp),
        vec!["a", "1", "b", "3", "d", "3", "c", "5"]
    );

    let resp = handle(&mut h, cmd(&["ZINTER", "2", "z1", "z2", "WITHSCORES"]));
    assert_eq!(array_bulk_strs(&resp), vec!["b", "3", "c", "5"]);

    // without scores
    let resp = handle(&mut h, cmd(&["ZINTER", "2", "z1", "z2"]));
    assert_eq!(array_bulk_strs(&resp), vec!["b", "c"]);
}

#[test]
fn test_zdiff_and_zdiffstore() {
    let mut h = make_handler(make_cache());
    seed(&mut h);

    let resp = handle(&mut h, cmd(&["ZDIFF", "2", "z1", "z2", "WITHSCORES"]));
    // a only from z1
    assert_eq!(array_bulk_strs(&resp), vec!["a", "1"]);

    let resp = handle(&mut h, cmd(&["ZDIFF", "2", "z2", "z1"]));
    assert_eq!(array_bulk_strs(&resp), vec!["d"]);

    let resp = handle(&mut h, cmd(&["ZDIFFSTORE", "out", "2", "z1", "z2"]));
    assert_eq!(resp, RespValue::Integer(1));
    assert_eq!(
        array_bulk_strs(&handle(&mut h, cmd(&["ZRANGE", "out", "0", "-1", "WITHSCORES"]))),
        vec!["a", "1"]
    );

    // empty diff deletes dest
    handle(&mut h, cmd(&["ZADD", "same1", "1", "x"]));
    handle(&mut h, cmd(&["ZADD", "same2", "2", "x"]));
    let resp = handle(&mut h, cmd(&["ZDIFFSTORE", "out", "2", "same1", "same2"]));
    assert_eq!(resp, RespValue::Integer(0));
    assert_eq!(
        handle(&mut h, cmd(&["EXISTS", "out"])),
        RespValue::Integer(0)
    );
}

#[test]
fn test_zunion_weights_aggregate() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["ZADD", "z1", "1", "m"]));
    handle(&mut h, cmd(&["ZADD", "z2", "2", "m"]));

    let resp = handle(
        &mut h,
        cmd(&[
            "ZUNION",
            "2",
            "z1",
            "z2",
            "WEIGHTS",
            "2",
            "3",
            "AGGREGATE",
            "MAX",
            "WITHSCORES",
        ]),
    );
    // max(1*2, 2*3) = 6
    assert_eq!(array_bulk_strs(&resp), vec!["m", "6"]);
}

#[test]
fn test_zdiff_no_weights() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["ZADD", "z1", "1", "a"]));
    match handle(&mut h, cmd(&["ZDIFF", "1", "z1", "WEIGHTS", "1"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("syntax")),
        other => panic!("{:?}", other),
    }
}

#[test]
fn test_zset_algebra_wrongtype_and_missing() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["ZADD", "z1", "1", "a"]));
    handle(&mut h, cmd(&["SET", "s", "x"]));

    match handle(&mut h, cmd(&["ZUNION", "2", "z1", "s"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("WRONGTYPE")),
        other => panic!("{:?}", other),
    }

    // missing source for union → just z1
    let resp = handle(&mut h, cmd(&["ZUNION", "2", "z1", "missing"]));
    assert_eq!(array_bulk_strs(&resp), vec!["a"]);

    // missing for inter → empty
    let resp = handle(&mut h, cmd(&["ZINTER", "2", "z1", "missing"]));
    assert_eq!(array_bulk_strs(&resp), Vec::<String>::new());

    // missing first for diff → empty
    let resp = handle(&mut h, cmd(&["ZDIFF", "2", "missing", "z1"]));
    assert_eq!(array_bulk_strs(&resp), Vec::<String>::new());
}

#[test]
fn test_existing_zunionstore_still_works() {
    let mut h = make_handler(make_cache());
    seed(&mut h);
    let resp = handle(&mut h, cmd(&["ZUNIONSTORE", "out", "2", "z1", "z2"]));
    assert_eq!(resp, RespValue::Integer(4));
    let resp = handle(&mut h, cmd(&["ZINTERSTORE", "out", "2", "z1", "z2"]));
    assert_eq!(resp, RespValue::Integer(2));
}
