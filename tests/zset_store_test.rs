//! Batch AM: ZUNIONSTORE / ZINTERSTORE (WEIGHTS, AGGREGATE).

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

fn array_bulk_strs(v: &RespValue) -> Vec<String> {
    match v {
        RespValue::Array(arr) => arr.iter().filter_map(as_bulk_str).collect(),
        _ => panic!("expected array, got {:?}", v),
    }
}

fn seed_two(h: &mut CommandHandler) {
    handle(h, cmd(&["ZADD", "z1", "1", "a", "2", "b", "3", "c"]));
    handle(h, cmd(&["ZADD", "z2", "1", "b", "2", "c", "3", "d"]));
}

#[test]
fn test_zunionstore_basic() {
    let mut h = make_handler(make_cache());
    seed_two(&mut h);

    let resp = handle(&mut h, cmd(&["ZUNIONSTORE", "out", "2", "z1", "z2"]));
    assert_eq!(resp, RespValue::Integer(4));

    let resp = handle(&mut h, cmd(&["ZRANGE", "out", "0", "-1", "WITHSCORES"]));
    let items = array_bulk_strs(&resp);
    // a:1, b:2+1=3, c:3+2=5, d:3  — ordered by score then member
    assert_eq!(items, vec!["a", "1", "b", "3", "d", "3", "c", "5"]);
}

#[test]
fn test_zinterstore_basic() {
    let mut h = make_handler(make_cache());
    seed_two(&mut h);

    let resp = handle(&mut h, cmd(&["ZINTERSTORE", "out", "2", "z1", "z2"]));
    assert_eq!(resp, RespValue::Integer(2));

    let resp = handle(&mut h, cmd(&["ZRANGE", "out", "0", "-1", "WITHSCORES"]));
    let items = array_bulk_strs(&resp);
    // b:2+1=3, c:3+2=5
    assert_eq!(items, vec!["b", "3", "c", "5"]);
}

#[test]
fn test_zstore_weights_and_aggregate() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["ZADD", "z1", "1", "m", "5", "n"]));
    handle(&mut h, cmd(&["ZADD", "z2", "2", "m", "1", "n"]));

    // WEIGHTS 2 3: m = 1*2 + 2*3 = 8, n = 5*2 + 1*3 = 13
    let resp = handle(
        &mut h,
        cmd(&["ZUNIONSTORE", "wout", "2", "z1", "z2", "WEIGHTS", "2", "3"]),
    );
    assert_eq!(resp, RespValue::Integer(2));
    let items = array_bulk_strs(&handle(
        &mut h,
        cmd(&["ZRANGE", "wout", "0", "-1", "WITHSCORES"]),
    ));
    assert_eq!(items, vec!["m", "8", "n", "13"]);

    // AGGREGATE MIN: m = min(1,2)=1, n = min(5,1)=1
    let resp = handle(
        &mut h,
        cmd(&[
            "ZUNIONSTORE",
            "mout",
            "2",
            "z1",
            "z2",
            "AGGREGATE",
            "MIN",
        ]),
    );
    assert_eq!(resp, RespValue::Integer(2));
    let items = array_bulk_strs(&handle(
        &mut h,
        cmd(&["ZRANGE", "mout", "0", "-1", "WITHSCORES"]),
    ));
    assert_eq!(items, vec!["m", "1", "n", "1"]);

    // AGGREGATE MAX with WEIGHTS: m = max(1*2, 2*3)=6, n = max(5*2, 1*3)=10
    let resp = handle(
        &mut h,
        cmd(&[
            "ZINTERSTORE",
            "xout",
            "2",
            "z1",
            "z2",
            "WEIGHTS",
            "2",
            "3",
            "AGGREGATE",
            "MAX",
        ]),
    );
    assert_eq!(resp, RespValue::Integer(2));
    let items = array_bulk_strs(&handle(
        &mut h,
        cmd(&["ZRANGE", "xout", "0", "-1", "WITHSCORES"]),
    ));
    assert_eq!(items, vec!["m", "6", "n", "10"]);
}

#[test]
fn test_zstore_missing_keys_and_empty() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["ZADD", "z1", "1", "a"]));

    // Union with missing key = just z1
    let resp = handle(&mut h, cmd(&["ZUNIONSTORE", "out", "2", "z1", "missing"]));
    assert_eq!(resp, RespValue::Integer(1));
    assert_eq!(
        array_bulk_strs(&handle(&mut h, cmd(&["ZRANGE", "out", "0", "-1"]))),
        vec!["a"]
    );

    // Inter with missing key = empty, dest deleted
    handle(&mut h, cmd(&["ZADD", "out", "9", "keep"]));
    let resp = handle(&mut h, cmd(&["ZINTERSTORE", "out", "2", "z1", "missing"]));
    assert_eq!(resp, RespValue::Integer(0));
    assert_eq!(
        handle(&mut h, cmd(&["EXISTS", "out"])),
        RespValue::Integer(0)
    );
}

#[test]
fn test_zstore_overwrite_wrongtype_dest_and_source() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["ZADD", "z1", "1", "a"]));
    handle(&mut h, cmd(&["SET", "strdest", "hello"]));

    let resp = handle(&mut h, cmd(&["ZUNIONSTORE", "strdest", "1", "z1"]));
    assert_eq!(resp, RespValue::Integer(1));
    assert_eq!(
        handle(&mut h, cmd(&["TYPE", "strdest"])),
        RespValue::SimpleString(Bytes::from_static(b"zset"))
    );

    handle(&mut h, cmd(&["SET", "strsrc", "nope"]));
    let resp = handle(&mut h, cmd(&["ZUNIONSTORE", "out", "2", "z1", "strsrc"]));
    match resp {
        RespValue::Error(e) => {
            let s = String::from_utf8_lossy(&e);
            assert!(s.contains("WRONGTYPE"), "got {}", s);
        }
        other => panic!("expected WRONGTYPE, got {:?}", other),
    }
}

#[test]
fn test_zstore_arity_and_syntax() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["ZADD", "z1", "1", "a"]));

    match handle(&mut h, cmd(&["ZUNIONSTORE", "out"])) {
        RespValue::Error(e) => {
            let s = String::from_utf8_lossy(&e);
            assert!(s.contains("wrong number"), "got {}", s);
        }
        other => panic!("{:?}", other),
    }
    match handle(&mut h, cmd(&["ZUNIONSTORE", "out", "0", "z1"])) {
        RespValue::Error(e) => {
            let s = String::from_utf8_lossy(&e);
            assert!(s.contains("at least 1"), "got {}", s);
        }
        other => panic!("{:?}", other),
    }
    match handle(
        &mut h,
        cmd(&["ZUNIONSTORE", "out", "1", "z1", "AGGREGATE", "AVG"]),
    ) {
        RespValue::Error(e) => {
            let s = String::from_utf8_lossy(&e);
            assert!(s.contains("AGGREGATE"), "got {}", s);
        }
        other => panic!("{:?}", other),
    }
}
