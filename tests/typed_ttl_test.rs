//! Batch AE: EXPIRE/TTL/active expire/volatile eviction for typed keys.

use bytes::Bytes;
use kore::cache::KeyType;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::persistence::rdb::{self, DbSnapshot};
use kore::protocol::RespValue;
use kore::Cache;
use kore::EvictionPolicy;
use std::sync::Arc;
use std::time::Duration;

fn base_config() -> Config {
    Config {
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
    }
}

fn make_cache(maxmemory: usize) -> Arc<Cache> {
    Cache::new_with_sweep(16, maxmemory, 500 * 1024 * 1024, false)
}

fn make_handler(cache: Arc<Cache>) -> CommandHandler {
    CommandHandler::new(cache, Arc::new(base_config()))
}

fn cmd(parts: &[&str]) -> RespValue {
    RespValue::Array(
        parts
            .iter()
            .map(|s| RespValue::BulkString(Some(Bytes::from(s.to_string()))))
            .collect(),
    )
}

fn handle(h: &mut CommandHandler, c: RespValue) -> RespValue {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async { h.handle(c).await.unwrap() })
}

#[test]
fn expire_ttl_on_hash_list_set() {
    let cache = make_cache(16 * 1024 * 1024);
    let mut h = make_handler(cache.clone());

    handle(&mut h, cmd(&["HSET", "h", "f", "v"]));
    handle(&mut h, cmd(&["LPUSH", "l", "a"]));
    handle(&mut h, cmd(&["SADD", "s", "m"]));
    handle(&mut h, cmd(&["ZADD", "z", "1", "m"]));

    // No TTL yet
    assert_eq!(handle(&mut h, cmd(&["TTL", "h"])), RespValue::Integer(-1));
    assert_eq!(handle(&mut h, cmd(&["TTL", "l"])), RespValue::Integer(-1));
    assert_eq!(handle(&mut h, cmd(&["TTL", "s"])), RespValue::Integer(-1));
    assert_eq!(handle(&mut h, cmd(&["TTL", "z"])), RespValue::Integer(-1));

    assert_eq!(
        handle(&mut h, cmd(&["EXPIRE", "h", "10"])),
        RespValue::Integer(1)
    );
    assert_eq!(
        handle(&mut h, cmd(&["PEXPIRE", "l", "5000"])),
        RespValue::Integer(1)
    );
    assert_eq!(
        handle(&mut h, cmd(&["EXPIRE", "s", "60"])),
        RespValue::Integer(1)
    );
    assert_eq!(
        handle(&mut h, cmd(&["EXPIRE", "z", "30"])),
        RespValue::Integer(1)
    );

    let th = match handle(&mut h, cmd(&["TTL", "h"])) {
        RespValue::Integer(n) => n,
        other => panic!("unexpected TTL: {:?}", other),
    };
    assert!(th >= 8 && th <= 10, "hash TTL {th}");

    let pl = match handle(&mut h, cmd(&["PTTL", "l"])) {
        RespValue::Integer(n) => n,
        other => panic!("unexpected PTTL: {:?}", other),
    };
    assert!(pl >= 4000 && pl <= 5000, "list PTTL {pl}");

    assert_eq!(cache.key_type(&Bytes::from_static(b"h")), KeyType::Hash);
    assert!(cache.exists(&Bytes::from_static(b"h")));
}

#[test]
fn typed_key_lazy_expires() {
    let cache = make_cache(16 * 1024 * 1024);
    let mut h = make_handler(cache.clone());

    handle(&mut h, cmd(&["HSET", "gone", "f", "v"]));
    assert_eq!(
        handle(&mut h, cmd(&["PEXPIRE", "gone", "40"])),
        RespValue::Integer(1)
    );
    assert!(cache.exists(&Bytes::from_static(b"gone")));

    std::thread::sleep(Duration::from_millis(60));

    // Lazy expire on access
    assert!(!cache.exists(&Bytes::from_static(b"gone")));
    assert_eq!(
        handle(&mut h, cmd(&["TTL", "gone"])),
        RespValue::Integer(-2)
    );
    assert_eq!(
        handle(&mut h, cmd(&["HGET", "gone", "f"])),
        RespValue::null()
    );
}

#[test]
fn typed_active_expire_sweep() {
    let cache = make_cache(16 * 1024 * 1024);
    let mut h = make_handler(cache.clone());

    for i in 0..20 {
        let k = format!("th:{i}");
        handle(&mut h, cmd(&["HSET", &k, "f", "v"]));
        handle(&mut h, cmd(&["PEXPIRE", &k, "30"]));
    }
    // Permanent hash
    handle(&mut h, cmd(&["HSET", "keep", "f", "v"]));

    std::thread::sleep(Duration::from_millis(50));

    let mut removed = 0usize;
    for _ in 0..50 {
        removed += cache.active_expire();
    }
    removed += cache.sweep();

    assert!(
        removed >= 20,
        "expected >=20 typed expires removed, got {removed}"
    );
    assert!(cache.exists(&Bytes::from_static(b"keep")));
    for i in 0..20 {
        let k = Bytes::from(format!("th:{i}"));
        assert!(!cache.exists(&k), "expired hash {i} still present");
    }
}

#[test]
fn volatile_lru_evicts_typed_keys_with_ttl() {
    // Small budget so eviction is forced.
    let maxmemory = 8 * 1024;
    let cache = make_cache(maxmemory);
    cache.set_eviction_policy(EvictionPolicy::VolatileLru);
    cache.set_eviction_sample_size(16).unwrap();
    let mut h = make_handler(cache.clone());

    // Permanent string that should survive volatile policy
    handle(&mut h, cmd(&["SET", "perm", "x"]));

    let big = "Z".repeat(200);
    let mut volatile_ok = 0usize;
    for i in 0..40 {
        let k = format!("vh:{i}");
        let resp = handle(&mut h, cmd(&["HSET", &k, "blob", &big]));
        if matches!(resp, RespValue::Integer(_)) {
            handle(&mut h, cmd(&["EXPIRE", &k, "3600"]));
            volatile_ok += 1;
        }
        assert!(
            cache.tracked_memory() <= maxmemory,
            "tracked {} > max {}",
            cache.tracked_memory(),
            maxmemory
        );
    }
    assert!(volatile_ok >= 3, "indexed volatile hashes: {volatile_ok}");

    // More pressure
    for i in 40..80 {
        let k = format!("vh:{i}");
        let _ = handle(&mut h, cmd(&["HSET", &k, "blob", &big]));
        let _ = handle(&mut h, cmd(&["EXPIRE", &k, "3600"]));
        assert!(cache.tracked_memory() <= maxmemory);
    }

    // Permanent string should still exist (volatile only targets TTL keys)
    assert!(
        cache.exists(&Bytes::from_static(b"perm")),
        "volatile-lru must not evict keys without TTL"
    );
}

#[test]
fn rdb_roundtrip_preserves_typed_ttl() {
    let cache = make_cache(16 * 1024 * 1024);
    let mut h = make_handler(cache.clone());

    handle(&mut h, cmd(&["HSET", "rh", "f", "v"]));
    handle(&mut h, cmd(&["EXPIRE", "rh", "100"]));
    handle(&mut h, cmd(&["SADD", "rs", "m"]));
    // no expire on rs

    let snap = DbSnapshot::from_cache(&cache).unwrap();
    assert!(
        snap.typed_expires.iter().any(|(k, _)| k.as_ref() == b"rh"),
        "typed_expires should include rh: {:?}",
        snap.typed_expires
    );

    let bytes = rdb::save_to_bytes(&cache).unwrap();
    // KORDB v6 (typed-expires v4+; search v5+; HNSW graph v6+)
    let version = u32::from_le_bytes(bytes[6..10].try_into().unwrap());
    assert_eq!(version, 6);

    let cache2 = make_cache(16 * 1024 * 1024);
    rdb::load_bytes(&cache2, &bytes, true).unwrap();

    assert_eq!(cache2.key_type(&Bytes::from_static(b"rh")), KeyType::Hash);
    let ttl = cache2.ttl(&Bytes::from_static(b"rh"));
    assert!(ttl > 0 && ttl <= 100_000, "restored TTL ms = {ttl}");
    assert_eq!(cache2.ttl(&Bytes::from_static(b"rs")), -1);
}

#[test]
fn rename_keeps_typed_ttl() {
    let cache = make_cache(16 * 1024 * 1024);
    let mut h = make_handler(cache.clone());

    handle(&mut h, cmd(&["HSET", "old", "f", "v"]));
    handle(&mut h, cmd(&["EXPIRE", "old", "50"]));
    handle(&mut h, cmd(&["RENAME", "old", "new"]));

    assert!(!cache.exists(&Bytes::from_static(b"old")));
    assert_eq!(cache.key_type(&Bytes::from_static(b"new")), KeyType::Hash);
    let ttl = cache.ttl(&Bytes::from_static(b"new"));
    assert!(ttl > 0, "TTL after rename: {ttl}");
}

#[test]
fn expire_missing_typed_returns_zero() {
    let cache = make_cache(16 * 1024 * 1024);
    let mut h = make_handler(cache.clone());
    assert_eq!(
        handle(&mut h, cmd(&["EXPIRE", "nope", "10"])),
        RespValue::Integer(0)
    );
    assert_eq!(
        handle(&mut h, cmd(&["TTL", "nope"])),
        RespValue::Integer(-2)
    );
}

#[test]
fn persist_removes_ttl_string_and_hash() {
    let cache = make_cache(16 * 1024 * 1024);
    let mut h = make_handler(cache.clone());

    handle(&mut h, cmd(&["SET", "s", "v"]));
    handle(&mut h, cmd(&["EXPIRE", "s", "100"]));
    assert!(matches!(
        handle(&mut h, cmd(&["TTL", "s"])),
        RespValue::Integer(n) if n > 0
    ));
    assert_eq!(
        handle(&mut h, cmd(&["PERSIST", "s"])),
        RespValue::Integer(1)
    );
    assert_eq!(handle(&mut h, cmd(&["TTL", "s"])), RespValue::Integer(-1));
    // Second persist: no timeout
    assert_eq!(
        handle(&mut h, cmd(&["PERSIST", "s"])),
        RespValue::Integer(0)
    );

    handle(&mut h, cmd(&["HSET", "h", "f", "v"]));
    handle(&mut h, cmd(&["EXPIRE", "h", "50"]));
    assert_eq!(
        handle(&mut h, cmd(&["PERSIST", "h"])),
        RespValue::Integer(1)
    );
    assert_eq!(handle(&mut h, cmd(&["TTL", "h"])), RespValue::Integer(-1));
    assert_eq!(
        handle(&mut h, cmd(&["PERSIST", "missing"])),
        RespValue::Integer(0)
    );
}

#[test]
fn expireat_and_pexpireat_absolute() {
    let cache = make_cache(16 * 1024 * 1024);
    let mut h = make_handler(cache.clone());

    handle(&mut h, cmd(&["SET", "a", "1"]));
    handle(&mut h, cmd(&["HSET", "b", "f", "v"]));

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let future_ms = now_ms + 30_000;
    let future_sec = future_ms / 1000;

    assert_eq!(
        handle(
            &mut h,
            cmd(&["PEXPIREAT", "a", &future_ms.to_string()])
        ),
        RespValue::Integer(1)
    );
    assert_eq!(
        handle(
            &mut h,
            cmd(&["EXPIREAT", "b", &future_sec.to_string()])
        ),
        RespValue::Integer(1)
    );

    let pttl = match handle(&mut h, cmd(&["PTTL", "a"])) {
        RespValue::Integer(n) => n,
        o => panic!("{:?}", o),
    };
    assert!(pttl > 20_000 && pttl <= 30_000, "pttl={pttl}");

    let et = match handle(&mut h, cmd(&["EXPIRETIME", "b"])) {
        RespValue::Integer(n) => n,
        o => panic!("{:?}", o),
    };
    assert!(
        (et - future_sec).abs() <= 1,
        "expiretime={et} expected ~{future_sec}"
    );

    let pet = match handle(&mut h, cmd(&["PEXPIRETIME", "a"])) {
        RespValue::Integer(n) => n,
        o => panic!("{:?}", o),
    };
    assert!((pet - future_ms).abs() < 2000, "pexpiretime={pet}");

    // No expire
    handle(&mut h, cmd(&["SET", "c", "1"]));
    assert_eq!(
        handle(&mut h, cmd(&["EXPIRETIME", "c"])),
        RespValue::Integer(-1)
    );
    assert_eq!(
        handle(&mut h, cmd(&["EXPIRETIME", "missing"])),
        RespValue::Integer(-2)
    );
}

#[test]
fn expireat_past_deletes_key() {
    let cache = make_cache(16 * 1024 * 1024);
    let mut h = make_handler(cache.clone());

    handle(&mut h, cmd(&["SET", "gone", "v"]));
    handle(&mut h, cmd(&["HSET", "hgone", "f", "v"]));

    // Past absolute times
    assert_eq!(
        handle(&mut h, cmd(&["EXPIREAT", "gone", "1"])),
        RespValue::Integer(1)
    );
    assert!(!cache.exists(&Bytes::from_static(b"gone")));

    assert_eq!(
        handle(&mut h, cmd(&["PEXPIREAT", "hgone", "1"])),
        RespValue::Integer(1)
    );
    assert!(!cache.exists(&Bytes::from_static(b"hgone")));
}

#[test]
fn expire_zero_deletes_key() {
    let cache = make_cache(16 * 1024 * 1024);
    let mut h = make_handler(cache.clone());

    handle(&mut h, cmd(&["SET", "z", "v"]));
    handle(&mut h, cmd(&["SADD", "sz", "m"]));
    assert_eq!(
        handle(&mut h, cmd(&["EXPIRE", "z", "0"])),
        RespValue::Integer(1)
    );
    assert!(!cache.exists(&Bytes::from_static(b"z")));
    assert_eq!(
        handle(&mut h, cmd(&["PEXPIRE", "sz", "0"])),
        RespValue::Integer(1)
    );
    assert!(!cache.exists(&Bytes::from_static(b"sz")));
}

#[test]
fn expire_negative_is_error() {
    let cache = make_cache(16 * 1024 * 1024);
    let mut h = make_handler(cache.clone());
    handle(&mut h, cmd(&["SET", "k", "v"]));
    let resp = handle(&mut h, cmd(&["EXPIRE", "k", "-5"]));
    match resp {
        RespValue::Error(e) => {
            assert!(
                String::from_utf8_lossy(&e).contains("invalid expire"),
                "{:?}",
                e
            );
        }
        other => panic!("expected error, got {:?}", other),
    }
}
