//! Batch AS: ZINTERCARD, ZMPOP, BZMPOP.

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

/// Flatten nested ZMPOP reply to bulk strings: key, m1, s1, m2, s2, ...
fn flatten_zmpop(v: &RespValue) -> Vec<String> {
    match v {
        RespValue::Array(outer) if outer.len() == 2 => {
            let key = as_bulk_str(&outer[0]).expect("key");
            let mut out = vec![key];
            match &outer[1] {
                RespValue::Array(pairs) => {
                    for p in pairs {
                        match p {
                            RespValue::Array(pair) if pair.len() == 2 => {
                                out.push(as_bulk_str(&pair[0]).expect("member"));
                                out.push(as_bulk_str(&pair[1]).expect("score"));
                            }
                            other => panic!("expected [member, score], got {:?}", other),
                        }
                    }
                }
                other => panic!("expected pairs array, got {:?}", other),
            }
            out
        }
        other => panic!("expected zmpop array, got {:?}", other),
    }
}

#[test]
fn test_zintercard() {
    let mut h = make_handler(make_cache());
    handle(
        &mut h,
        cmd(&["ZADD", "z1", "1", "a", "2", "b", "3", "c"]),
    );
    handle(
        &mut h,
        cmd(&["ZADD", "z2", "1", "b", "2", "c", "3", "d"]),
    );
    handle(&mut h, cmd(&["ZADD", "z3", "1", "c", "2", "d", "3", "e"]));

    assert_eq!(
        handle(&mut h, cmd(&["ZINTERCARD", "2", "z1", "z2"])),
        RespValue::Integer(2)
    );
    assert_eq!(
        handle(&mut h, cmd(&["ZINTERCARD", "3", "z1", "z2", "z3"])),
        RespValue::Integer(1)
    );

    // LIMIT caps the count
    assert_eq!(
        handle(&mut h, cmd(&["ZINTERCARD", "2", "z1", "z2", "LIMIT", "1"])),
        RespValue::Integer(1)
    );
    // LIMIT 0 = unlimited
    assert_eq!(
        handle(&mut h, cmd(&["ZINTERCARD", "2", "z1", "z2", "LIMIT", "0"])),
        RespValue::Integer(2)
    );

    // Missing key → empty intersection
    assert_eq!(
        handle(&mut h, cmd(&["ZINTERCARD", "2", "z1", "missing"])),
        RespValue::Integer(0)
    );

    handle(&mut h, cmd(&["SET", "s", "x"]));
    match handle(&mut h, cmd(&["ZINTERCARD", "2", "z1", "s"])) {
        RespValue::Error(e) => {
            assert!(String::from_utf8_lossy(&e).contains("WRONGTYPE"));
        }
        other => panic!("{:?}", other),
    }
}

#[test]
fn test_zmpop() {
    let mut h = make_handler(make_cache());
    handle(
        &mut h,
        cmd(&["ZADD", "a", "1", "x", "2", "y", "3", "z"]),
    );
    handle(&mut h, cmd(&["ZADD", "b", "10", "p"]));

    // MIN from first non-empty (a)
    let resp = handle(&mut h, cmd(&["ZMPOP", "2", "a", "b", "MIN"]));
    assert_eq!(flatten_zmpop(&resp), vec!["a", "x", "1"]);

    // COUNT 2 MAX
    let resp = handle(&mut h, cmd(&["ZMPOP", "1", "a", "MAX", "COUNT", "2"]));
    assert_eq!(flatten_zmpop(&resp), vec!["a", "z", "3", "y", "2"]);

    // a empty → pop from b
    assert_eq!(
        handle(&mut h, cmd(&["EXISTS", "a"])),
        RespValue::Integer(0)
    );
    let resp = handle(&mut h, cmd(&["ZMPOP", "2", "a", "b", "MAX"]));
    assert_eq!(flatten_zmpop(&resp), vec!["b", "p", "10"]);

    // all empty → null
    assert_eq!(
        handle(&mut h, cmd(&["ZMPOP", "2", "a", "b", "MIN"])),
        RespValue::null()
    );

    // multi-key left-to-right skips empty
    handle(&mut h, cmd(&["ZADD", "c", "5", "cc"]));
    let resp = handle(&mut h, cmd(&["ZMPOP", "3", "missing", "empty", "c", "MIN"]));
    assert_eq!(flatten_zmpop(&resp), vec!["c", "cc", "5"]);
}

#[test]
fn test_bzmpop_timeout_and_wake() {
    let cache = make_cache();
    let mut h = make_handler(Arc::clone(&cache));

    let resp = handle(&mut h, cmd(&["BZMPOP", "0.2", "1", "empty", "MIN"]));
    assert_eq!(resp, RespValue::null());

    let cache2 = Arc::clone(&cache);
    let mut h_blocker = make_handler(Arc::clone(&cache));
    let blocker = std::thread::spawn(move || {
        handle(
            &mut h_blocker,
            cmd(&["BZMPOP", "5", "1", "wake", "MAX", "COUNT", "2"]),
        )
    });
    std::thread::sleep(Duration::from_millis(100));
    let mut h_pusher = make_handler(cache2);
    handle(
        &mut h_pusher,
        cmd(&["ZADD", "wake", "1", "lo", "9", "hi", "5", "mid"]),
    );
    let resp = blocker.join().unwrap();
    // MAX COUNT 2 → hi, mid
    assert_eq!(flatten_zmpop(&resp), vec!["wake", "hi", "9", "mid", "5"]);
}

#[test]
fn test_bzmpop_multi_key() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["ZADD", "k2", "1", "bb"]));
    handle(&mut h, cmd(&["ZADD", "k3", "9", "aa"]));
    let resp = handle(
        &mut h,
        cmd(&["BZMPOP", "1", "3", "missing", "k2", "k3", "MIN"]),
    );
    assert_eq!(flatten_zmpop(&resp), vec!["k2", "bb", "1"]);
}
