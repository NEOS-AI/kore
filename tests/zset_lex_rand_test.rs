//! Batch AR: ZMSCORE, ZRANDMEMBER, ZRANGEBYLEX, ZREVRANGEBYLEX, ZLEXCOUNT, ZREMRANGEBYLEX.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::protocol::RespValue;
use kore::Cache;
use std::collections::HashSet;
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
        RespValue::BulkString(None) => None,
        _ => None,
    }
}

fn array_items(v: &RespValue) -> Vec<Option<String>> {
    match v {
        RespValue::Array(arr) => arr
            .iter()
            .map(|x| match x {
                RespValue::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
                RespValue::BulkString(None) => None,
                _ => panic!("unexpected element {:?}", x),
            })
            .collect(),
        _ => panic!("expected array, got {:?}", v),
    }
}

fn array_bulk_strs(v: &RespValue) -> Vec<String> {
    array_items(v).into_iter().map(|o| o.expect("null")).collect()
}

fn seed_alpha(h: &mut CommandHandler) {
    // Equal scores so lex order matches skiplist member order.
    handle(
        h,
        cmd(&[
            "ZADD", "alpha", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e", "0", "f",
        ]),
    );
}

#[test]
fn test_zmscore() {
    let mut h = make_handler(make_cache());
    handle(
        &mut h,
        cmd(&["ZADD", "z", "1.5", "a", "2", "b", "3", "c"]),
    );

    let resp = handle(&mut h, cmd(&["ZMSCORE", "z", "a", "missing", "c"]));
    assert_eq!(
        array_items(&resp),
        vec![Some("1.5".into()), None, Some("3".into())]
    );

    // Missing key → all nulls
    let resp = handle(&mut h, cmd(&["ZMSCORE", "gone", "a", "b"]));
    assert_eq!(array_items(&resp), vec![None, None]);

    // Wrong type
    handle(&mut h, cmd(&["SET", "s", "x"]));
    let resp = handle(&mut h, cmd(&["ZMSCORE", "s", "a"]));
    match resp {
        RespValue::Error(e) => {
            assert!(
                String::from_utf8_lossy(&e).contains("WRONGTYPE"),
                "got {:?}",
                e
            );
        }
        other => panic!("expected WRONGTYPE, got {:?}", other),
    }
}

#[test]
fn test_zrandmember() {
    let mut h = make_handler(make_cache());
    handle(
        &mut h,
        cmd(&["ZADD", "z", "1", "a", "2", "b", "3", "c"]),
    );

    // Single member
    let resp = handle(&mut h, cmd(&["ZRANDMEMBER", "z"]));
    let one = as_bulk_str(&resp).expect("bulk member");
    assert!(["a", "b", "c"].contains(&one.as_str()));

    // Missing key without count → null
    assert_eq!(
        handle(&mut h, cmd(&["ZRANDMEMBER", "missing"])),
        RespValue::null()
    );

    // Distinct count
    let resp = handle(&mut h, cmd(&["ZRANDMEMBER", "z", "2"]));
    let members = array_bulk_strs(&resp);
    assert_eq!(members.len(), 2);
    assert_eq!(members.iter().collect::<HashSet<_>>().len(), 2);
    for m in &members {
        assert!(["a", "b", "c"].contains(&m.as_str()));
    }

    // Count larger than size → all members
    let resp = handle(&mut h, cmd(&["ZRANDMEMBER", "z", "10"]));
    let mut members = array_bulk_strs(&resp);
    members.sort();
    assert_eq!(members, vec!["a", "b", "c"]);

    // WITHSCORES
    let resp = handle(&mut h, cmd(&["ZRANDMEMBER", "z", "1", "WITHSCORES"]));
    let items = array_bulk_strs(&resp);
    assert_eq!(items.len(), 2);
    assert!(["a", "b", "c"].contains(&items[0].as_str()));
    assert!(["1", "2", "3"].contains(&items[1].as_str()));

    // Negative count with replacement
    let resp = handle(&mut h, cmd(&["ZRANDMEMBER", "z", "-5"]));
    assert_eq!(array_bulk_strs(&resp).len(), 5);

    // Missing key with count → empty array
    assert_eq!(
        handle(&mut h, cmd(&["ZRANDMEMBER", "gone", "3"])),
        RespValue::Array(vec![])
    );
}

#[test]
fn test_zrangebylex_and_count() {
    let mut h = make_handler(make_cache());
    seed_alpha(&mut h);

    // Inclusive [b, d]
    let resp = handle(&mut h, cmd(&["ZRANGEBYLEX", "alpha", "[b", "[d"]));
    assert_eq!(array_bulk_strs(&resp), vec!["b", "c", "d"]);

    // Exclusive (b, d)
    let resp = handle(&mut h, cmd(&["ZRANGEBYLEX", "alpha", "(b", "(d"]));
    assert_eq!(array_bulk_strs(&resp), vec!["c"]);

    // Open ends - +
    let resp = handle(&mut h, cmd(&["ZRANGEBYLEX", "alpha", "-", "+"]));
    assert_eq!(
        array_bulk_strs(&resp),
        vec!["a", "b", "c", "d", "e", "f"]
    );

    // LIMIT
    let resp = handle(
        &mut h,
        cmd(&["ZRANGEBYLEX", "alpha", "[a", "[f", "LIMIT", "1", "2"]),
    );
    assert_eq!(array_bulk_strs(&resp), vec!["b", "c"]);

    // ZREVRANGEBYLEX max min
    let resp = handle(&mut h, cmd(&["ZREVRANGEBYLEX", "alpha", "[d", "[b"]));
    assert_eq!(array_bulk_strs(&resp), vec!["d", "c", "b"]);

    // ZLEXCOUNT
    assert_eq!(
        handle(&mut h, cmd(&["ZLEXCOUNT", "alpha", "[b", "[d"])),
        RespValue::Integer(3)
    );
    assert_eq!(
        handle(&mut h, cmd(&["ZLEXCOUNT", "alpha", "-", "+"])),
        RespValue::Integer(6)
    );
    assert_eq!(
        handle(&mut h, cmd(&["ZLEXCOUNT", "missing", "-", "+"])),
        RespValue::Integer(0)
    );

    // Invalid bound
    let resp = handle(&mut h, cmd(&["ZRANGEBYLEX", "alpha", "b", "[d"]));
    match resp {
        RespValue::Error(e) => {
            assert!(
                String::from_utf8_lossy(&e).contains("min or max"),
                "got {:?}",
                e
            );
        }
        other => panic!("expected error, got {:?}", other),
    }
}

#[test]
fn test_zremrangebylex() {
    let mut h = make_handler(make_cache());
    seed_alpha(&mut h);

    let resp = handle(&mut h, cmd(&["ZREMRANGEBYLEX", "alpha", "[b", "[d"]));
    assert_eq!(resp, RespValue::Integer(3));

    let remaining = handle(&mut h, cmd(&["ZRANGEBYLEX", "alpha", "-", "+"]));
    assert_eq!(array_bulk_strs(&remaining), vec!["a", "e", "f"]);

    // Remove rest → key deleted
    assert_eq!(
        handle(&mut h, cmd(&["ZREMRANGEBYLEX", "alpha", "-", "+"])),
        RespValue::Integer(3)
    );
    assert_eq!(
        handle(&mut h, cmd(&["EXISTS", "alpha"])),
        RespValue::Integer(0)
    );

    assert_eq!(
        handle(&mut h, cmd(&["ZREMRANGEBYLEX", "gone", "-", "+"])),
        RespValue::Integer(0)
    );
}
