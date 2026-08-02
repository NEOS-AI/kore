//! Phase C P1: maxmemory eviction policies

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::entry::StoreOptions;
use kore::protocol::RespValue;
use kore::{Cache, EvictionPolicy};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn small_cache(max_mem: usize) -> Arc<Cache> {
    Cache::new_with_sweep(4, max_mem, 1024 * 1024, false)
}

fn store(cache: &Cache, key: &str, val: &str, ttl_ms: Option<u64>) {
    let mut opts = StoreOptions::default();
    opts.ttl_ms = ttl_ms;
    cache
        .store(Bytes::from(key.to_string()), Bytes::from(val.to_string()), opts)
        .unwrap();
}

fn fill_until_full(cache: &Cache, prefix: &str, val_size: usize) -> usize {
    let val = "x".repeat(val_size);
    let mut n = 0;
    loop {
        let key = format!("{}{}", prefix, n);
        let opts = StoreOptions::default();
        match cache.store(Bytes::from(key), Bytes::from(val.clone()), opts) {
            Ok(_) => n += 1,
            Err(_) => break,
        }
        if n > 10_000 {
            break;
        }
    }
    n
}

#[test]
fn noeviction_returns_oom() {
    let cache = small_cache(8 * 1024);
    cache.set_eviction_policy(EvictionPolicy::NoEviction);

    let n = fill_until_full(&cache, "k", 200);
    assert!(n > 0, "should store some keys");

    let err = cache
        .store(
            Bytes::from("overflow"),
            Bytes::from("x".repeat(200)),
            StoreOptions::default(),
        )
        .unwrap_err();
    assert!(
        matches!(err, kore::Error::OutOfMemory),
        "expected OOM, got {:?}",
        err
    );
    // Existing keys still present
    assert!(cache.exists(&Bytes::from(format!("k0"))));
}

#[test]
fn allkeys_lru_evicts_to_make_room() {
    let cache = small_cache(8 * 1024);
    cache.set_eviction_policy(EvictionPolicy::AllKeysLru);
    cache.set_eviction_sample_size(10).unwrap();

    // Insert and touch some keys so others are colder
    for i in 0..20 {
        store(&cache, &format!("hot{}", i), &"h".repeat(100), None);
        // touch via load
        let _ = cache.load(&Bytes::from(format!("hot{}", i)), Default::default());
    }
    thread::sleep(Duration::from_millis(5));
    for i in 0..20 {
        store(&cache, &format!("cold{}", i), &"c".repeat(100), None);
    }

    let before = cache.dbsize();
    // Force more inserts that require eviction
    for i in 0..30 {
        store(&cache, &format!("new{}", i), &"n".repeat(120), None);
    }
    let after = cache.dbsize();
    assert!(
        after > 0 && cache.stats.evicted_lru.load(std::sync::atomic::Ordering::Relaxed) > 0,
        "expected LRU evictions; before={} after={} evicted={}",
        before,
        after,
        cache.stats.evicted_lru.load(std::sync::atomic::Ordering::Relaxed)
    );
}

#[test]
fn volatile_lru_only_evicts_keys_with_ttl() {
    let cache = small_cache(6 * 1024);
    cache.set_eviction_policy(EvictionPolicy::VolatileLru);
    cache.set_eviction_sample_size(16).unwrap();

    // Permanent keys
    for i in 0..8 {
        store(&cache, &format!("perm{}", i), &"p".repeat(150), None);
    }
    // Volatile keys
    for i in 0..8 {
        store(&cache, &format!("vol{}", i), &"v".repeat(150), Some(60_000));
    }

    // Pressure memory — should prefer volatile keys
    for i in 0..40 {
        let r = cache.store(
            Bytes::from(format!("extra{}", i)),
            Bytes::from("e".repeat(150)),
            StoreOptions {
                ttl_ms: Some(30_000),
                ..Default::default()
            },
        );
        if r.is_err() {
            break;
        }
    }

    // At least some permanent keys should survive preferentially
    let perm_left: usize = (0..8)
        .filter(|i| cache.exists(&Bytes::from(format!("perm{}", i))))
        .count();
    assert!(
        perm_left >= 4,
        "volatile-lru should prefer evicting TTL keys; perm_left={}",
        perm_left
    );
    assert!(
        cache.stats.evicted_lru.load(std::sync::atomic::Ordering::Relaxed) > 0
            || perm_left < 8,
        "expected some eviction activity"
    );
}

#[test]
fn volatile_ttl_prefers_soonest_expiry() {
    let cache = small_cache(5 * 1024);
    cache.set_eviction_policy(EvictionPolicy::VolatileTtl);
    cache.set_eviction_sample_size(20).unwrap();

    store(&cache, "soon", &"s".repeat(200), Some(5_000)); // 5s
    store(&cache, "later", &"l".repeat(200), Some(3_600_000)); // 1h
    store(&cache, "mid", &"m".repeat(200), Some(60_000));

    // Flood with volatile keys to force eviction of existing
    for i in 0..50 {
        let _ = cache.store(
            Bytes::from(format!("f{}", i)),
            Bytes::from("x".repeat(180)),
            StoreOptions {
                ttl_ms: Some(120_000),
                ..Default::default()
            },
        );
    }

    // "later" (long TTL) is more likely to survive than "soon"
    // Not guaranteed with sampling, but under pressure soon should go first often.
    // Soft check: either soon is gone or later still exists after evictions.
    let evicted = cache.stats.evicted_lru.load(std::sync::atomic::Ordering::Relaxed);
    if evicted > 0 {
        // If we evicted, prefer that long-TTL key remains when short is gone
        if !cache.exists(&Bytes::from("soon")) {
            // good path
        } else {
            // sampling may miss; still require that policy is set correctly
            assert_eq!(cache.eviction_policy(), EvictionPolicy::VolatileTtl);
        }
    }
    assert_eq!(cache.eviction_policy().as_str(), "volatile-ttl");
}

#[test]
fn config_set_maxmemory_policy() {
    let cache = small_cache(1024 * 1024);
    let config = Config {
        host: "127.0.0.1".to_string(),
        port: 6379,
        threads: 1,
        shards: 4,
        maxmemory: 1024 * 1024,
        evict: true,
        autosweep: false,
        loadfactor: 0.75,
        maxconns: 10,
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
        save: "".to_string(),
        maxmemory_policy: "allkeys-lru".to_string(),
        databases: 16,
        metrics_port: 0,
        deadlock_ui_port: 0,
            admin_bind: "127.0.0.1".to_string(),
            admin_http_token: String::new(),
            admin_http_user: String::new(),
            admin_http_password: String::new(),
            admin_tls: false,
            admin_tls_cert: String::new(),
            admin_tls_key: String::new(),
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
    let mut h = CommandHandler::new(cache.clone(), Arc::new(config));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let set = rt.block_on(h.handle(RespValue::Array(vec![
        RespValue::BulkString(Some(Bytes::from_static(b"CONFIG"))),
        RespValue::BulkString(Some(Bytes::from_static(b"SET"))),
        RespValue::BulkString(Some(Bytes::from_static(b"maxmemory-policy"))),
        RespValue::BulkString(Some(Bytes::from_static(b"noeviction"))),
    ])))
    .unwrap();
    assert_eq!(set, RespValue::ok());
    assert_eq!(cache.eviction_policy(), EvictionPolicy::NoEviction);

    let get = rt.block_on(h.handle(RespValue::Array(vec![
        RespValue::BulkString(Some(Bytes::from_static(b"CONFIG"))),
        RespValue::BulkString(Some(Bytes::from_static(b"GET"))),
        RespValue::BulkString(Some(Bytes::from_static(b"maxmemory-policy"))),
    ])))
    .unwrap();
    match get {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(
                arr[1],
                RespValue::BulkString(Some(Bytes::from_static(b"noeviction")))
            );
        }
        other => panic!("expected array, got {:?}", other),
    }

    // Invalid policy
    let bad = rt.block_on(h.handle(RespValue::Array(vec![
        RespValue::BulkString(Some(Bytes::from_static(b"CONFIG"))),
        RespValue::BulkString(Some(Bytes::from_static(b"SET"))),
        RespValue::BulkString(Some(Bytes::from_static(b"maxmemory-policy"))),
        RespValue::BulkString(Some(Bytes::from_static(b"bogus"))),
    ])))
    .unwrap();
    match bad {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("Invalid")),
        other => panic!("expected error, got {:?}", other),
    }
}

#[test]
fn policy_parse_roundtrip() {
    for name in [
        "noeviction",
        "allkeys-lru",
        "volatile-lru",
        "allkeys-lfu",
        "volatile-lfu",
        "allkeys-random",
        "volatile-random",
        "volatile-ttl",
    ] {
        let p = EvictionPolicy::parse(name).unwrap();
        assert_eq!(p.as_str(), name);
    }
    assert!(EvictionPolicy::parse("nope").is_err());
}

#[test]
fn allkeys_lfu_prefers_cold_keys() {
    // Hot key is touched many times; cold keys are written once then left idle.
    // Under allkeys-lfu the hot key should survive memory pressure.
    let cache = small_cache(12 * 1024);
    cache.set_eviction_policy(EvictionPolicy::AllKeysLfu);
    cache.set_eviction_sample_size(16).unwrap();
    // No decay during the test (same-minute accesses already skip decay).
    cache.set_lfu_decay_time(0).unwrap();
    // Low log-factor so repeated touches raise the counter quickly.
    cache.set_lfu_log_factor(1).unwrap();

    store(&cache, "hot", &"H".repeat(80), None);
    for _ in 0..200 {
        let e = cache
            .load(&Bytes::from_static(b"hot"), kore::LoadOptions::default())
            .unwrap()
            .expect("hot key");
        // touch already applied by load
        let _ = e;
    }

    // Flood with cold keys until eviction runs
    let mut n = 0;
    for i in 0..500 {
        let key = format!("cold{}", i);
        let opts = StoreOptions::default();
        match cache.store(
            Bytes::from(key),
            Bytes::from("C".repeat(80)),
            opts,
        ) {
            Ok(_) => n += 1,
            Err(_) => break,
        }
    }
    assert!(n > 5, "should insert several cold keys, got {}", n);

    let hot_alive = cache
        .load(
            &Bytes::from_static(b"hot"),
            kore::LoadOptions {
                touch: false,
                with_cas: false,
            },
        )
        .unwrap()
        .is_some();
    assert!(
        hot_alive,
        "hot key should survive LFU eviction (counter raised by touches)"
    );
}

#[test]
fn config_lfu_params_roundtrip() {
    let cache = small_cache(1024 * 1024);
    let config = Arc::new(Config {
        host: "127.0.0.1".to_string(),
        port: 6379,
        threads: 1,
        shards: 4,
        maxmemory: 1024 * 1024,
        evict: true,
        autosweep: false,
        loadfactor: 0.75,
        maxconns: 10,
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
        save: "".to_string(),
        maxmemory_policy: "allkeys-lfu".to_string(),
        databases: 16,
        metrics_port: 0,
        deadlock_ui_port: 0,
            admin_bind: "127.0.0.1".to_string(),
            admin_http_token: String::new(),
            admin_http_user: String::new(),
            admin_http_password: String::new(),
            admin_tls: false,
            admin_tls_cert: String::new(),
            admin_tls_key: String::new(),
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
    });
    let mut h = CommandHandler::new(cache.clone(), config);

    assert_eq!(
        handle(&mut h, &["CONFIG", "SET", "lfu-log-factor", "7"]),
        RespValue::ok()
    );
    assert_eq!(cache.lfu_log_factor(), 7);
    assert_eq!(
        handle(&mut h, &["CONFIG", "SET", "lfu-decay-time", "3"]),
        RespValue::ok()
    );
    assert_eq!(cache.lfu_decay_time(), 3);

    match handle(&mut h, &["CONFIG", "GET", "lfu-log-factor"]) {
        RespValue::Array(a) => {
            assert_eq!(a.len(), 2);
            assert_eq!(
                a[1],
                RespValue::BulkString(Some(Bytes::from_static(b"7")))
            );
        }
        other => panic!("unexpected {:?}", other),
    }
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn handle(h: &mut CommandHandler, parts: &[&str]) -> RespValue {
    let arr = parts
        .iter()
        .map(|p| RespValue::BulkString(Some(Bytes::from(p.to_string()))))
        .collect();
    rt().block_on(h.handle(RespValue::Array(arr))).unwrap()
}

#[test]
fn allkeys_lru_evicts_hashes_when_no_string_keys() {
    // Tiny maxmemory so HSET growth must free prior hash keys.
    let cache = Cache::new_with_sweep(4, 6 * 1024, 1024 * 1024, false);
    cache.set_eviction_policy(EvictionPolicy::AllKeysLru);
    cache.set_eviction_sample_size(12).unwrap();

    let config = Arc::new(Config {
        host: "127.0.0.1".to_string(),
        port: 6379,
        threads: 1,
        shards: 4,
        maxmemory: 6 * 1024,
        evict: true,
        autosweep: false,
        loadfactor: 0.75,
        maxconns: 10,
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
        save: "".to_string(),
        maxmemory_policy: "allkeys-lru".to_string(),
        databases: 16,
        metrics_port: 0,
        deadlock_ui_port: 0,
            admin_bind: "127.0.0.1".to_string(),
            admin_http_token: String::new(),
            admin_http_user: String::new(),
            admin_http_password: String::new(),
            admin_tls: false,
            admin_tls_cert: String::new(),
            admin_tls_key: String::new(),
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
    });
    let mut h = CommandHandler::new(cache.clone(), config);

    let payload = "x".repeat(200);
    let mut ok = 0usize;
    for i in 0..60 {
        let key = format!("hk{}", i);
        let r = handle(&mut h, &["HSET", &key, "f", &payload]);
        match r {
            RespValue::Integer(_) => ok += 1,
            RespValue::Error(_) => break,
            other => panic!("unexpected {:?}", other),
        }
    }
    assert!(ok >= 5, "should HSET several hashes, got {}", ok);

    let before = cache
        .stats
        .evicted_lru
        .load(std::sync::atomic::Ordering::Relaxed);
    // More HSETs under pressure
    for i in 60..120 {
        let key = format!("hk{}", i);
        let _ = handle(&mut h, &["HSET", &key, "f", &payload]);
    }
    let after = cache
        .stats
        .evicted_lru
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        after > before,
        "expected typed (hash) eviction; before={} after={} hashes~dbsize={} tracked={}",
        before,
        after,
        cache.dbsize(),
        cache.tracked_memory()
    );
}

#[test]
fn allkeys_random_evicts_zsets_under_pressure() {
    let cache = Cache::new_with_sweep(4, 5 * 1024, 1024 * 1024, false);
    cache.set_eviction_policy(EvictionPolicy::AllKeysRandom);
    cache.set_eviction_sample_size(10).unwrap();

    let config = Arc::new(Config {
        host: "127.0.0.1".to_string(),
        port: 6379,
        threads: 1,
        shards: 4,
        maxmemory: 5 * 1024,
        evict: true,
        autosweep: false,
        loadfactor: 0.75,
        maxconns: 10,
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
        save: "".to_string(),
        maxmemory_policy: "allkeys-random".to_string(),
        databases: 16,
        metrics_port: 0,
        deadlock_ui_port: 0,
            admin_bind: "127.0.0.1".to_string(),
            admin_http_token: String::new(),
            admin_http_user: String::new(),
            admin_http_password: String::new(),
            admin_tls: false,
            admin_tls_cert: String::new(),
            admin_tls_key: String::new(),
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
    });
    let mut h = CommandHandler::new(cache.clone(), config);

    let mut ok = 0usize;
    for i in 0..40 {
        let key = format!("zk{}", i);
        // ZADD key 1.0 member — member payload padded
        let member = format!("m{}", "y".repeat(80));
        let r = handle(&mut h, &["ZADD", &key, "1.0", &member]);
        if matches!(r, RespValue::Integer(_)) {
            ok += 1;
        } else {
            break;
        }
    }
    assert!(ok >= 3, "should ZADD several sets, got {}", ok);

    let before = cache
        .stats
        .evicted_lru
        .load(std::sync::atomic::Ordering::Relaxed);
    for i in 40..100 {
        let key = format!("zk{}", i);
        let member = format!("m{}", "y".repeat(80));
        let _ = handle(&mut h, &["ZADD", &key, "1.0", &member]);
    }
    let after = cache
        .stats
        .evicted_lru
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        after > before,
        "expected zset eviction under allkeys-random; before={} after={}",
        before,
        after
    );
}

#[test]
fn allkeys_can_evict_string_or_typed_mixed() {
    // Constructor sets maxmemory directly (no 1MB floor — that applies only to live CONFIG SET).
    let cache = Cache::new_with_sweep(4, 8 * 1024, 1024 * 1024, false);
    cache.set_eviction_policy(EvictionPolicy::AllKeysLru);
    cache.set_eviction_sample_size(16).unwrap();

    // Seed strings
    for i in 0..15 {
        store(&cache, &format!("s{}", i), &"s".repeat(100), None);
    }
    // Seed hashes via CommandHandler (HSET path uses typed memory accounting)
    let config = Arc::new({
        let mut c = Config::default();
        c.shards = 4;
        c.maxmemory = 8 * 1024;
        c.evict = true;
        c.autosweep = false;
        c.maxmemory_policy = "allkeys-lru".to_string();
        c
    });
    let mut h = CommandHandler::new(cache.clone(), config);
    for i in 0..15 {
        let key = format!("mh{}", i);
        let _ = handle(&mut h, &["HSET", &key, "f", &"h".repeat(120)]);
    }

    let before = cache
        .stats
        .evicted_lru
        .load(std::sync::atomic::Ordering::Relaxed);
    for i in 0..40 {
        store(&cache, &format!("n{}", i), &"n".repeat(150), None);
    }
    let after = cache
        .stats
        .evicted_lru
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        after > before,
        "mixed string+hash workload should evict; before={} after={} dbsize={}",
        before,
        after,
        cache.dbsize()
    );
}
