//! Batch AA: allocator-aware memory sizing + multi-type used_memory.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::entry::StoreOptions;
use kore::memory::{
    estimate_string_entry, with_alloc_overhead, MemoryCategory,
};
use kore::protocol::RespValue;
use kore::Cache;
use std::sync::Arc;

fn make_cache(max_mem: usize) -> Arc<Cache> {
    Cache::new_with_sweep(4, max_mem, 1024 * 1024, false)
}

fn make_handler(cache: Arc<Cache>) -> CommandHandler {
    let config = Config {
        host: "127.0.0.1".to_string(),
        port: 6379,
        threads: 1,
        shards: 4,
        maxmemory: cache.max_memory(),
        evict: true,
        autosweep: false,
        loadfactor: 0.75,
        maxconns: 100,
        auth: String::new(),
        maxentrysize: 1024 * 1024,
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

#[test]
fn string_store_charges_more_than_key_plus_value() {
    let cache = make_cache(10 * 1024 * 1024);
    let key = Bytes::from("k1");
    let val = Bytes::from("hello-world");
    cache
        .store(key.clone(), val.clone(), StoreOptions::default())
        .unwrap();

    let used = cache.memory_usage();
    let raw = key.len() + val.len();
    assert!(used > raw, "accounted {} should exceed payload {}", used, raw);

    // Matches estimator used by Entry::size
    let expected = estimate_string_entry(
        key.len(),
        val.len(),
        std::mem::size_of::<kore::entry::Entry>(),
    );
    assert_eq!(used, expected);
    assert_eq!(cache.tracked_cache_memory(), expected);
    assert_eq!(cache.memory_usage(), cache.tracked_memory());
}

#[test]
fn replace_and_delete_keep_counters_consistent() {
    let cache = make_cache(10 * 1024 * 1024);
    let key = Bytes::from("rk");
    cache
        .store(key.clone(), Bytes::from("a"), StoreOptions::default())
        .unwrap();
    let s1 = cache.memory_usage();
    cache
        .store(key.clone(), Bytes::from("aaaaaaaaaa"), StoreOptions::default())
        .unwrap();
    let s2 = cache.memory_usage();
    assert!(s2 > s1);
    assert_eq!(cache.string_memory_usage(), cache.tracked_cache_memory());
    cache.delete(&key).unwrap();
    assert_eq!(cache.memory_usage(), 0);
    assert_eq!(cache.string_memory_usage(), 0);
}

#[test]
fn hash_growth_increases_used_memory() {
    let cache = make_cache(10 * 1024 * 1024);
    let mut h = make_handler(cache.clone());

    assert_eq!(cache.memory_usage(), 0);
    assert_eq!(
        handle(&mut h, cmd(&["HSET", "user:1", "name", "alice"])),
        RespValue::Integer(1)
    );
    let after_one = cache.memory_usage();
    assert!(after_one > 0);
    assert!(cache.category_memory(MemoryCategory::Hashes) > 0);

    assert_eq!(
        handle(
            &mut h,
            cmd(&["HSET", "user:1", "email", "a@example.com", "city", "seoul"])
        ),
        RespValue::Integer(2)
    );
    let after_more = cache.memory_usage();
    assert!(
        after_more > after_one,
        "more fields must increase memory {} -> {}",
        after_one,
        after_more
    );

    // used_memory is total (not string-only)
    assert_eq!(cache.memory_usage(), cache.tracked_memory());
    assert_eq!(cache.string_memory_usage(), 0);
}

#[test]
fn mixed_types_share_maxmemory_budget() {
    let cache = make_cache(10 * 1024 * 1024);
    let mut h = make_handler(cache.clone());

    handle(&mut h, cmd(&["SET", "s", "v"]));
    handle(&mut h, cmd(&["HSET", "h", "f", "1"]));
    handle(&mut h, cmd(&["LPUSH", "l", "a", "b"]));
    handle(&mut h, cmd(&["SADD", "set", "x"]));
    handle(&mut h, cmd(&["ZADD", "z", "1", "m"]));

    let total = cache.memory_usage();
    let parts = cache.category_memory(MemoryCategory::Cache)
        + cache.category_memory(MemoryCategory::Hashes)
        + cache.category_memory(MemoryCategory::Lists)
        + cache.category_memory(MemoryCategory::Sets)
        + cache.category_memory(MemoryCategory::SortedSets);
    assert_eq!(total, parts);
    assert!(
        total
            > cache.category_memory(MemoryCategory::Cache)
                + cache.category_memory(MemoryCategory::Hashes)
                - 1
    );
}

#[test]
fn alloc_overhead_helper_is_monotonic() {
    assert!(with_alloc_overhead(1) >= 8);
    assert!(with_alloc_overhead(64) > 64);
    // Alignment can equalize adjacent sizes; step far enough to grow.
    assert!(with_alloc_overhead(2000) > with_alloc_overhead(1000));
    assert!(with_alloc_overhead(64) >= with_alloc_overhead(32));
}

#[test]
fn flush_clears_all_category_memory() {
    let cache = make_cache(10 * 1024 * 1024);
    let mut h = make_handler(cache.clone());
    handle(&mut h, cmd(&["SET", "a", "1"]));
    handle(&mut h, cmd(&["HSET", "b", "f", "v"]));
    assert!(cache.memory_usage() > 0);
    handle(&mut h, cmd(&["FLUSHDB"]));
    assert_eq!(cache.memory_usage(), 0);
    assert_eq!(cache.tracked_cache_memory(), 0);
    assert_eq!(cache.category_memory(MemoryCategory::Hashes), 0);
}
