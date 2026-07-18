//! Batch AV: HRANDFIELD, SMISMEMBER, SINTERCARD.

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

fn as_int_array(v: &RespValue) -> Vec<i64> {
    match v {
        RespValue::Array(arr) => arr
            .iter()
            .map(|x| match x {
                RespValue::Integer(n) => *n,
                _ => panic!("expected integer, got {:?}", x),
            })
            .collect(),
        _ => panic!("expected array, got {:?}", v),
    }
}

fn as_bulk_array(v: &RespValue) -> Vec<String> {
    match v {
        RespValue::Array(arr) => arr.iter().filter_map(as_bulk_str).collect(),
        _ => panic!("expected array, got {:?}", v),
    }
}

#[test]
fn test_hrandfield() {
    let mut h = make_handler(make_cache());
    assert_eq!(
        handle(&mut h, cmd(&["HSET", "hk", "a", "1", "b", "2", "c", "3"])),
        RespValue::Integer(3)
    );

    // Single field: bulk string present in hash.
    let resp = handle(&mut h, cmd(&["HRANDFIELD", "hk"]));
    let f = as_bulk_str(&resp).expect("bulk field");
    assert!(["a", "b", "c"].contains(&f.as_str()));
    assert_eq!(handle(&mut h, cmd(&["HLEN", "hk"])), RespValue::Integer(3));

    // Missing key without count → null.
    assert_eq!(
        handle(&mut h, cmd(&["HRANDFIELD", "gone"])),
        RespValue::null()
    );

    // Count: up to N distinct fields.
    let resp = handle(&mut h, cmd(&["HRANDFIELD", "hk", "2"]));
    let fields = as_bulk_array(&resp);
    assert_eq!(fields.len(), 2);
    for f in &fields {
        assert!(["a", "b", "c"].contains(&f.as_str()));
    }

    // Count larger than cardinality → all fields.
    let resp = handle(&mut h, cmd(&["HRANDFIELD", "hk", "10"]));
    assert_eq!(as_bulk_array(&resp).len(), 3);

    // WITHVALUES: flat field/value pairs.
    let resp = handle(&mut h, cmd(&["HRANDFIELD", "hk", "1", "WITHVALUES"]));
    let pairs = as_bulk_array(&resp);
    assert_eq!(pairs.len(), 2);
    let val = handle(&mut h, cmd(&["HGET", "hk", &pairs[0]]));
    assert_eq!(as_bulk_str(&val).as_deref(), Some(pairs[1].as_str()));

    // Negative count: with replacement.
    let resp = handle(&mut h, cmd(&["HRANDFIELD", "hk", "-5"]));
    assert_eq!(as_bulk_array(&resp).len(), 5);

    // Missing key with count → empty array.
    assert_eq!(
        handle(&mut h, cmd(&["HRANDFIELD", "gone", "3"])),
        RespValue::Array(vec![])
    );

    // Wrong type.
    let _ = handle(&mut h, cmd(&["SET", "s", "x"]));
    match handle(&mut h, cmd(&["HRANDFIELD", "s"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("WRONGTYPE")),
        other => panic!("expected WRONGTYPE, got {:?}", other),
    }
}

#[test]
fn test_smismember() {
    let mut h = make_handler(make_cache());
    assert_eq!(
        handle(&mut h, cmd(&["SADD", "s", "a", "b", "c"])),
        RespValue::Integer(3)
    );

    assert_eq!(
        as_int_array(&handle(
            &mut h,
            cmd(&["SMISMEMBER", "s", "a", "x", "b", "y"])
        )),
        vec![1, 0, 1, 0]
    );

    // Missing key → all zeros.
    assert_eq!(
        as_int_array(&handle(&mut h, cmd(&["SMISMEMBER", "missing", "a", "b"]))),
        vec![0, 0]
    );

    // Wrong type.
    let _ = handle(&mut h, cmd(&["SET", "str", "v"]));
    match handle(&mut h, cmd(&["SMISMEMBER", "str", "a"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("WRONGTYPE")),
        other => panic!("expected WRONGTYPE, got {:?}", other),
    }
}

#[test]
fn test_sintercard() {
    let mut h = make_handler(make_cache());
    let _ = handle(&mut h, cmd(&["SADD", "s1", "a", "b", "c", "d"]));
    let _ = handle(&mut h, cmd(&["SADD", "s2", "b", "c", "e"]));
    let _ = handle(&mut h, cmd(&["SADD", "s3", "c", "b", "z"]));

    assert_eq!(
        handle(&mut h, cmd(&["SINTERCARD", "2", "s1", "s2"])),
        RespValue::Integer(2)
    );
    assert_eq!(
        handle(&mut h, cmd(&["SINTERCARD", "3", "s1", "s2", "s3"])),
        RespValue::Integer(2)
    );

    // LIMIT caps the returned count (early stop).
    assert_eq!(
        handle(&mut h, cmd(&["SINTERCARD", "2", "s1", "s2", "LIMIT", "1"])),
        RespValue::Integer(1)
    );
    // LIMIT 0 = unlimited.
    assert_eq!(
        handle(&mut h, cmd(&["SINTERCARD", "2", "s1", "s2", "LIMIT", "0"])),
        RespValue::Integer(2)
    );

    // Missing key → empty intersection.
    assert_eq!(
        handle(&mut h, cmd(&["SINTERCARD", "2", "s1", "missing"])),
        RespValue::Integer(0)
    );

    // Wrong type.
    let _ = handle(&mut h, cmd(&["SET", "str", "v"]));
    match handle(&mut h, cmd(&["SINTERCARD", "2", "s1", "str"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("WRONGTYPE")),
        other => panic!("expected WRONGTYPE, got {:?}", other),
    }
}
