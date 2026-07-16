//! Batch AZ: OBJECT IDLETIME/REFCOUNT/FREQ, LCS IDX, BITFIELD_RO, GEORADIUS*_RO, SWAPDB.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::databases::Databases;
use kore::protocol::RespValue;
use kore::Cache;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn make_config() -> Arc<Config> {
    Arc::new(Config {
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
    })
}

fn make_cache() -> Arc<Cache> {
    Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false)
}

fn make_handler(cache: Arc<Cache>) -> CommandHandler {
    CommandHandler::new(cache, make_config())
}

fn make_multi_db_handler() -> CommandHandler {
    let databases = Databases::create(16, 16, 1024 * 1024 * 100, 500 * 1024 * 1024, false, 0.75);
    CommandHandler::with_databases(databases, make_config(), None)
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

fn is_null(v: &RespValue) -> bool {
    matches!(v, RespValue::BulkString(None) | RespValue::Null)
}

#[test]
fn object_idletime_refcount_freq() {
    let mut h = make_handler(make_cache());

    assert!(is_null(&handle(
        &mut h,
        cmd(&["OBJECT", "IDLETIME", "missing"])
    )));
    assert!(is_null(&handle(
        &mut h,
        cmd(&["OBJECT", "REFCOUNT", "missing"])
    )));
    assert!(is_null(&handle(
        &mut h,
        cmd(&["OBJECT", "FREQ", "missing"])
    )));

    handle(&mut h, cmd(&["SET", "s", "hello"]));
    assert_eq!(
        handle(&mut h, cmd(&["OBJECT", "REFCOUNT", "s"])),
        RespValue::Integer(1)
    );

    // Fresh string should have non-negative idle and LFU freq.
    match handle(&mut h, cmd(&["OBJECT", "IDLETIME", "s"])) {
        RespValue::Integer(n) => assert!(n >= 0),
        other => panic!("idletime: {:?}", other),
    }
    match handle(&mut h, cmd(&["OBJECT", "FREQ", "s"])) {
        RespValue::Integer(n) => assert!(n >= 0),
        other => panic!("freq: {:?}", other),
    }

    // Idle should not go backwards after a short sleep; touch:false keeps idle growing.
    thread::sleep(Duration::from_millis(1100));
    match handle(&mut h, cmd(&["OBJECT", "IDLETIME", "s"])) {
        RespValue::Integer(n) => assert!(n >= 1, "expected idle >= 1s, got {n}"),
        other => panic!("{:?}", other),
    }

    // Typed keys: idle 0, refcount 1, freq 0.
    handle(&mut h, cmd(&["HSET", "h", "f", "v"]));
    assert_eq!(
        handle(&mut h, cmd(&["OBJECT", "IDLETIME", "h"])),
        RespValue::Integer(0)
    );
    assert_eq!(
        handle(&mut h, cmd(&["OBJECT", "REFCOUNT", "h"])),
        RespValue::Integer(1)
    );
    assert_eq!(
        handle(&mut h, cmd(&["OBJECT", "FREQ", "h"])),
        RespValue::Integer(0)
    );
}

#[test]
fn lcs_idx_minmatchlen_withmatchlen() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["SET", "k1", "ohmytext"]));
    handle(&mut h, cmd(&["SET", "k2", "mynewtext"]));

    let resp = handle(&mut h, cmd(&["LCS", "k1", "k2", "IDX"]));
    match resp {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 4);
            assert_eq!(as_bulk_str(&items[0]).as_deref(), Some("matches"));
            assert!(matches!(items[1], RespValue::Array(_)));
            assert_eq!(as_bulk_str(&items[2]).as_deref(), Some("len"));
            match &items[3] {
                RespValue::Integer(n) => assert!(*n >= 4, "lcs len {n}"),
                other => panic!("len: {:?}", other),
            }
            if let RespValue::Array(matches) = &items[1] {
                assert!(!matches.is_empty(), "expected at least one match range");
                // Each match: [[a0,a1],[b0,b1]]
                if let RespValue::Array(m0) = &matches[0] {
                    assert_eq!(m0.len(), 2);
                    assert!(matches!(m0[0], RespValue::Array(_)));
                    assert!(matches!(m0[1], RespValue::Array(_)));
                }
            }
        }
        other => panic!("IDX reply: {:?}", other),
    }

    // WITHMATCHLEN adds length as third element of each match.
    let resp = handle(
        &mut h,
        cmd(&["LCS", "k1", "k2", "IDX", "WITHMATCHLEN"]),
    );
    match resp {
        RespValue::Array(items) => {
            if let RespValue::Array(matches) = &items[1] {
                assert!(!matches.is_empty());
                if let RespValue::Array(m0) = &matches[0] {
                    assert_eq!(m0.len(), 3, "pair + matchlen");
                    assert!(matches!(m0[2], RespValue::Integer(_)));
                }
            }
        }
        other => panic!("{:?}", other),
    }

    // MINMATCHLEN filters short ranges; huge threshold → empty matches, len still full LCS.
    let resp = handle(
        &mut h,
        cmd(&["LCS", "k1", "k2", "IDX", "MINMATCHLEN", "100"]),
    );
    match resp {
        RespValue::Array(items) => {
            if let RespValue::Array(matches) = &items[1] {
                assert!(matches.is_empty());
            }
            assert!(matches!(items[3], RespValue::Integer(n) if n >= 4));
        }
        other => panic!("{:?}", other),
    }

    // LEN + IDX is a syntax error in Redis.
    assert!(matches!(
        handle(&mut h, cmd(&["LCS", "k1", "k2", "LEN", "IDX"])),
        RespValue::Error(_)
    ));
}

#[test]
fn bitfield_ro_get_only_rejects_writes() {
    let mut h = make_handler(make_cache());
    handle(
        &mut h,
        cmd(&["BITFIELD", "bf", "SET", "u8", "0", "42"]),
    );

    let r = handle(&mut h, cmd(&["BITFIELD_RO", "bf", "GET", "u8", "0"]));
    match r {
        RespValue::Array(a) => {
            assert_eq!(a.len(), 1);
            assert_eq!(a[0], RespValue::Integer(42));
        }
        other => panic!("GET: {:?}", other),
    }

    assert!(matches!(
        handle(&mut h, cmd(&["BITFIELD_RO", "bf", "SET", "u8", "0", "1"])),
        RespValue::Error(_)
    ));
    assert!(matches!(
        handle(
            &mut h,
            cmd(&["BITFIELD_RO", "bf", "INCRBY", "u8", "0", "1"])
        ),
        RespValue::Error(_)
    ));

    // Value unchanged after rejected writes.
    match handle(&mut h, cmd(&["BITFIELD_RO", "bf", "GET", "u8", "0"])) {
        RespValue::Array(a) => assert_eq!(a[0], RespValue::Integer(42)),
        other => panic!("{:?}", other),
    }
}

#[test]
fn georadius_ro_rejects_store_and_reads() {
    let mut h = make_handler(make_cache());
    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "GEOADD",
                "cities",
                "126.9780",
                "37.5665",
                "Seoul",
                "129.0756",
                "35.1796",
                "Busan",
                "126.7052",
                "37.4563",
                "Incheon",
            ])
        ),
        RespValue::Integer(3)
    );

    // Read path works.
    let resp = handle(
        &mut h,
        cmd(&[
            "GEORADIUS_RO",
            "cities",
            "126.9780",
            "37.5665",
            "50",
            "km",
        ]),
    );
    match resp {
        RespValue::Array(items) => assert!(items.len() >= 2),
        other => panic!("georadius_ro: {:?}", other),
    }

    // STORE / STOREDIST rejected.
    assert!(matches!(
        handle(
            &mut h,
            cmd(&[
                "GEORADIUS_RO",
                "cities",
                "126.9780",
                "37.5665",
                "50",
                "km",
                "STORE",
                "near",
            ])
        ),
        RespValue::Error(_)
    ));
    assert!(matches!(
        handle(
            &mut h,
            cmd(&[
                "GEORADIUS_RO",
                "cities",
                "126.9780",
                "37.5665",
                "50",
                "km",
                "STOREDIST",
                "dists",
            ])
        ),
        RespValue::Error(_)
    ));
    assert_eq!(
        handle(&mut h, cmd(&["EXISTS", "near", "dists"])),
        RespValue::Integer(0)
    );

    // GEORADIUSBYMEMBER_RO similarly.
    let resp = handle(
        &mut h,
        cmd(&[
            "GEORADIUSBYMEMBER_RO",
            "cities",
            "Seoul",
            "50",
            "km",
        ]),
    );
    match resp {
        RespValue::Array(items) => assert!(items.len() >= 2),
        other => panic!("{:?}", other),
    }
    assert!(matches!(
        handle(
            &mut h,
            cmd(&[
                "GEORADIUSBYMEMBER_RO",
                "cities",
                "Seoul",
                "50",
                "km",
                "STORE",
                "from_seoul",
            ])
        ),
        RespValue::Error(_)
    ));
}

#[test]
fn swapdb_exchanges_keyspaces() {
    let mut h = make_multi_db_handler();

    // DB 0
    assert_eq!(handle(&mut h, cmd(&["SET", "a", "db0-a"])), RespValue::ok());
    assert_eq!(
        handle(&mut h, cmd(&["LPUSH", "list0", "x"])),
        RespValue::Integer(1)
    );
    handle(&mut h, cmd(&["EXPIRE", "a", "3600"]));

    // DB 1
    assert_eq!(handle(&mut h, cmd(&["SELECT", "1"])), RespValue::ok());
    assert_eq!(handle(&mut h, cmd(&["SET", "b", "db1-b"])), RespValue::ok());
    assert_eq!(
        handle(&mut h, cmd(&["HSET", "h1", "f", "v"])),
        RespValue::Integer(1)
    );

    // Swap 0 <-> 1
    assert_eq!(handle(&mut h, cmd(&["SWAPDB", "0", "1"])), RespValue::ok());

    // Still on DB 1: should now see former DB 0 contents
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GET", "a"]))).as_deref(),
        Some("db0-a")
    );
    assert!(is_null(&handle(&mut h, cmd(&["GET", "b"]))));
    assert_eq!(
        handle(&mut h, cmd(&["LLEN", "list0"])),
        RespValue::Integer(1)
    );
    // TTL preserved
    match handle(&mut h, cmd(&["TTL", "a"])) {
        RespValue::Integer(t) => assert!(t > 0 && t <= 3600, "ttl={t}"),
        other => panic!("{:?}", other),
    }

    // DB 0 has former DB 1
    assert_eq!(handle(&mut h, cmd(&["SELECT", "0"])), RespValue::ok());
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GET", "b"]))).as_deref(),
        Some("db1-b")
    );
    assert!(is_null(&handle(&mut h, cmd(&["GET", "a"]))));
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["HGET", "h1", "f"]))).as_deref(),
        Some("v")
    );

    // Same index is no-op OK
    assert_eq!(handle(&mut h, cmd(&["SWAPDB", "0", "0"])), RespValue::ok());

    // Out of range
    assert!(matches!(
        handle(&mut h, cmd(&["SWAPDB", "0", "99"])),
        RespValue::Error(_)
    ));
}
