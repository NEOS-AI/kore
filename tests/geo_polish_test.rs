//! Batch AV geo polish: WITHHASH, GEORADIUS STORE/STOREDIST, GEOSEARCHSTORE overwrite.

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

fn seed_cities(h: &mut CommandHandler) {
    assert_eq!(
        handle(
            h,
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
}

#[test]
fn test_geosearch_withhash() {
    let mut h = make_handler(make_cache());
    seed_cities(&mut h);

    let resp = handle(
        &mut h,
        cmd(&[
            "GEOSEARCH",
            "cities",
            "FROMLONLAT",
            "126.9780",
            "37.5665",
            "BYRADIUS",
            "50",
            "km",
            "WITHHASH",
        ]),
    );
    match resp {
        RespValue::Array(items) => {
            assert!(items.len() >= 2); // Seoul + Incheon
            // Each item is [member, hash] when WITHHASH alone.
            for item in &items {
                match item {
                    RespValue::Array(sub) => {
                        assert_eq!(sub.len(), 2);
                        assert!(as_bulk_str(&sub[0]).is_some());
                        // Hash is bulk decimal of u64 geohash.
                        let hash_s = as_bulk_str(&sub[1]).expect("hash bulk");
                        assert!(hash_s.parse::<u64>().is_ok(), "hash={}", hash_s);
                    }
                    other => panic!("expected nested array, got {:?}", other),
                }
            }
        }
        other => panic!("expected array, got {:?}", other),
    }
}

#[test]
fn test_georadius_store_and_storedist() {
    let mut h = make_handler(make_cache());
    seed_cities(&mut h);

    // STORE → destination geo set with nearby members.
    let resp = handle(
        &mut h,
        cmd(&[
            "GEORADIUS",
            "cities",
            "126.9780",
            "37.5665",
            "50",
            "km",
            "STORE",
            "near",
        ]),
    );
    assert_eq!(resp, RespValue::Integer(2));
    // Kore reports geo keys as "zset" for Redis client compatibility.
    assert_eq!(
        handle(&mut h, cmd(&["TYPE", "near"])),
        RespValue::SimpleString("zset".into())
    );
    // GEOPOS on stored member should work.
    let pos = handle(&mut h, cmd(&["GEOPOS", "near", "Seoul"]));
    match pos {
        RespValue::Array(a) => assert!(matches!(a[0], RespValue::Array(_))),
        other => panic!("expected geopos array, got {:?}", other),
    }

    // STOREDIST → sorted set with distance scores.
    let resp = handle(
        &mut h,
        cmd(&[
            "GEORADIUS",
            "cities",
            "126.9780",
            "37.5665",
            "50",
            "km",
            "STOREDIST",
            "dists",
        ]),
    );
    assert_eq!(resp, RespValue::Integer(2));
    assert_eq!(
        handle(&mut h, cmd(&["TYPE", "dists"])),
        RespValue::SimpleString("zset".into())
    );
    assert_eq!(
        handle(&mut h, cmd(&["ZCARD", "dists"])),
        RespValue::Integer(2)
    );
}

#[test]
fn test_georadiusbymember_store() {
    let mut h = make_handler(make_cache());
    seed_cities(&mut h);

    let resp = handle(
        &mut h,
        cmd(&[
            "GEORADIUSBYMEMBER",
            "cities",
            "Seoul",
            "50",
            "km",
            "STORE",
            "from_seoul",
        ]),
    );
    assert_eq!(resp, RespValue::Integer(2));
    assert_eq!(
        handle(&mut h, cmd(&["TYPE", "from_seoul"])),
        RespValue::SimpleString("zset".into())
    );
}

#[test]
fn test_geosearchstore_overwrite() {
    let mut h = make_handler(make_cache());
    seed_cities(&mut h);

    // Pre-seed dest with unrelated type/content.
    let _ = handle(&mut h, cmd(&["SET", "dest", "old"]));
    let resp = handle(
        &mut h,
        cmd(&[
            "GEOSEARCHSTORE",
            "dest",
            "cities",
            "FROMLONLAT",
            "126.9780",
            "37.5665",
            "BYRADIUS",
            "50",
            "km",
        ]),
    );
    assert_eq!(resp, RespValue::Integer(2));
    assert_eq!(
        handle(&mut h, cmd(&["TYPE", "dest"])),
        RespValue::SimpleString("zset".into())
    );

    // STOREDIST overwrite of geo dest.
    let resp = handle(
        &mut h,
        cmd(&[
            "GEOSEARCHSTORE",
            "dest",
            "cities",
            "FROMLONLAT",
            "126.9780",
            "37.5665",
            "BYRADIUS",
            "50",
            "km",
            "STOREDIST",
        ]),
    );
    assert_eq!(resp, RespValue::Integer(2));
    assert_eq!(
        handle(&mut h, cmd(&["TYPE", "dest"])),
        RespValue::SimpleString("zset".into())
    );
}

#[test]
fn test_geo_wrongtype() {
    let mut h = make_handler(make_cache());
    let _ = handle(&mut h, cmd(&["SET", "s", "x"]));
    match handle(
        &mut h,
        cmd(&[
            "GEOSEARCH",
            "s",
            "FROMLONLAT",
            "0",
            "0",
            "BYRADIUS",
            "1",
            "km",
        ]),
    ) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("WRONGTYPE")),
        other => panic!("expected WRONGTYPE, got {:?}", other),
    }
}
