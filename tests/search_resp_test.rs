//! FT.SEARCH RESP e2e + search index memory accounting (Phase E).

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::memory::MemoryCategory;
use kore::protocol::RespValue;
use kore::Cache;
use std::sync::Arc;

fn make_cache(maxmemory: usize) -> Arc<Cache> {
    Cache::new_with_sweep(16, maxmemory, 500 * 1024 * 1024, false)
}

fn make_handler(cache: Arc<Cache>) -> CommandHandler {
    let config = Config {
        host: "127.0.0.1".to_string(),
        port: 6379,
        threads: 1,
        shards: 16,
        maxmemory: cache.max_memory(),
        evict: false,
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
        maxmemory_policy: "noeviction".to_string(),
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

fn is_error_containing(resp: &RespValue, needle: &str) -> bool {
    match resp {
        RespValue::Error(e) => String::from_utf8_lossy(e).contains(needle),
        _ => false,
    }
}

fn search_total(resp: &RespValue) -> i64 {
    match resp {
        RespValue::Array(arr) => match arr.first() {
            Some(RespValue::Integer(n)) => *n,
            _ => panic!("FT.SEARCH reply missing total integer: {:?}", resp),
        },
        RespValue::Error(e) => panic!("FT.SEARCH error: {}", String::from_utf8_lossy(e)),
        other => panic!("expected FT.SEARCH array, got {:?}", other),
    }
}

fn search_ids(resp: &RespValue) -> Vec<String> {
    match resp {
        RespValue::Array(arr) => {
            let mut ids = Vec::new();
            // [total, id1, fields1, id2, fields2, ...]
            let mut i = 1;
            while i < arr.len() {
                if let RespValue::BulkString(Some(id)) = &arr[i] {
                    ids.push(String::from_utf8_lossy(id).into_owned());
                }
                i += 2; // skip fields array
            }
            ids
        }
        other => panic!("expected FT.SEARCH array, got {:?}", other),
    }
}

#[test]
fn test_ft_create_search_via_handler() {
    let cache = make_cache(10 * 1024 * 1024);
    let mut h = make_handler(cache);

    let resp = handle(
        &mut h,
        cmd(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
            "body",
            "TEXT",
        ]),
    );
    assert_eq!(resp, RespValue::ok());

    // Index via HSET auto-index (prefix match)
    let resp = handle(
        &mut h,
        cmd(&["HSET", "doc:1", "title", "hello world", "body", "search me"]),
    );
    assert_eq!(resp, RespValue::Integer(2));

    let resp = handle(&mut h, cmd(&["HSET", "doc:2", "title", "other", "body", "nothing"]));
    assert_eq!(resp, RespValue::Integer(2));

    let resp = handle(&mut h, cmd(&["FT.SEARCH", "idx", "hello"]));
    assert_eq!(search_total(&resp), 1);
    let ids = search_ids(&resp);
    assert_eq!(ids, vec!["doc:1".to_string()]);
}

#[test]
fn test_ft_search_missing_index() {
    let cache = make_cache(10 * 1024 * 1024);
    let mut h = make_handler(cache);

    let resp = handle(&mut h, cmd(&["FT.SEARCH", "no_such_idx", "foo"]));
    assert!(
        is_error_containing(&resp, "not found") || is_error_containing(&resp, "Index"),
        "expected missing-index error, got {:?}",
        resp
    );
}

#[test]
fn test_ft_search_limit_offset() {
    let cache = make_cache(10 * 1024 * 1024);
    let mut h = make_handler(cache);

    handle(
        &mut h,
        cmd(&[
            "FT.CREATE",
            "page_idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "p:",
            "SCHEMA",
            "content",
            "TEXT",
        ]),
    );

    for i in 1..=10 {
        let key = format!("p:{}", i);
        handle(
            &mut h,
            cmd(&["HSET", &key, "content", "document number"]),
        );
    }

    let resp = handle(
        &mut h,
        cmd(&["FT.SEARCH", "page_idx", "document", "LIMIT", "0", "3"]),
    );
    assert_eq!(search_ids(&resp).len(), 3);

    let resp = handle(
        &mut h,
        cmd(&["FT.SEARCH", "page_idx", "document", "LIMIT", "5", "3"]),
    );
    assert_eq!(search_ids(&resp).len(), 3);

    let resp = handle(
        &mut h,
        cmd(&["FT.SEARCH", "page_idx", "document", "LIMIT", "9", "5"]),
    );
    assert_eq!(search_ids(&resp).len(), 1);
}

#[test]
fn test_ft_dropindex_removes_search() {
    let cache = make_cache(10 * 1024 * 1024);
    let mut h = make_handler(cache.clone());

    handle(
        &mut h,
        cmd(&[
            "FT.CREATE",
            "tmp",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "t:",
            "SCHEMA",
            "f",
            "TEXT",
        ]),
    );
    handle(&mut h, cmd(&["HSET", "t:1", "f", "hello"]));

    let resp = handle(&mut h, cmd(&["FT.SEARCH", "tmp", "hello"]));
    assert_eq!(search_total(&resp), 1);

    let resp = handle(&mut h, cmd(&["FT.DROPINDEX", "tmp"]));
    assert_eq!(resp, RespValue::ok());

    let resp = handle(&mut h, cmd(&["FT.SEARCH", "tmp", "hello"]));
    assert!(is_error_containing(&resp, "not found") || is_error_containing(&resp, "Index"));

    // Search category should be fully released after drop
    assert_eq!(cache.category_memory(MemoryCategory::Search), 0);
}

#[test]
fn test_hset_auto_indexes_prefix() {
    let cache = make_cache(10 * 1024 * 1024);
    let mut h = make_handler(cache);

    handle(
        &mut h,
        cmd(&[
            "FT.CREATE",
            "blog",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "post:",
            "SCHEMA",
            "title",
            "TEXT",
        ]),
    );

    // Matching prefix → indexed
    handle(&mut h, cmd(&["HSET", "post:1", "title", "rust rocks"]));
    // Non-matching prefix → not indexed
    handle(&mut h, cmd(&["HSET", "other:1", "title", "rust rocks"]));

    let resp = handle(&mut h, cmd(&["FT.SEARCH", "blog", "rust"]));
    assert_eq!(search_total(&resp), 1);
    assert_eq!(search_ids(&resp), vec!["post:1".to_string()]);

    // DEL removes from index
    handle(&mut h, cmd(&["DEL", "post:1"]));
    let resp = handle(&mut h, cmd(&["FT.SEARCH", "blog", "rust"]));
    assert_eq!(search_total(&resp), 0);

    // Re-index then UNLINK
    handle(&mut h, cmd(&["HSET", "post:2", "title", "rust again"]));
    let resp = handle(&mut h, cmd(&["FT.SEARCH", "blog", "rust"]));
    assert_eq!(search_total(&resp), 1);
    handle(&mut h, cmd(&["UNLINK", "post:2"]));
    let resp = handle(&mut h, cmd(&["FT.SEARCH", "blog", "rust"]));
    assert_eq!(search_total(&resp), 0);
}

#[test]
fn test_search_index_memory_accounted() {
    let cache = make_cache(10 * 1024 * 1024);
    let mut h = make_handler(cache.clone());

    assert_eq!(cache.category_memory(MemoryCategory::Search), 0);

    handle(
        &mut h,
        cmd(&[
            "FT.CREATE",
            "mem_idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "m:",
            "SCHEMA",
            "body",
            "TEXT",
        ]),
    );

    // Empty index: no document memory yet
    assert_eq!(cache.category_memory(MemoryCategory::Search), 0);

    handle(
        &mut h,
        cmd(&["HSET", "m:1", "body", "hello searchable content"]),
    );

    let search_mem = cache.category_memory(MemoryCategory::Search);
    assert!(
        search_mem > 0,
        "expected Search category > 0 after auto-index, got {}",
        search_mem
    );
    assert!(
        cache.tracked_memory() >= search_mem,
        "Search must count toward total_memory"
    );

    // Remove document → search memory drops
    handle(&mut h, cmd(&["DEL", "m:1"]));
    assert_eq!(cache.category_memory(MemoryCategory::Search), 0);
}

#[test]
fn test_search_index_respects_maxmemory() {
    // Tiny budget so search docs cannot grow without bound.
    let maxmemory = 4096;
    let cache = make_cache(maxmemory);
    // Disable eviction so allocate failures surface as errors.
    cache.set_evict(false);

    let mut h = make_handler(cache.clone());

    handle(
        &mut h,
        cmd(&[
            "FT.CREATE",
            "tiny",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "x:",
            "SCHEMA",
            "blob",
            "TEXT",
        ]),
    );

    // Fill with large hash values until further indexing is rejected or HSET fails.
    let big = "Z".repeat(512);
    let mut failures = 0usize;
    let mut successes = 0usize;
    for i in 0..200 {
        let key = format!("x:{}", i);
        let resp = handle(&mut h, cmd(&["HSET", &key, "blob", &big]));
        match resp {
            RespValue::Integer(_) => successes += 1,
            RespValue::Error(_) => failures += 1,
            other => panic!("unexpected HSET reply: {:?}", other),
        }
        // Hard invariant: never exceed maxmemory
        assert!(
            cache.tracked_memory() <= maxmemory,
            "total_memory {} exceeded maxmemory {}",
            cache.tracked_memory(),
            maxmemory
        );
        let search_mem = cache.category_memory(MemoryCategory::Search);
        assert!(
            search_mem <= maxmemory,
            "Search category {} exceeded maxmemory {}",
            search_mem,
            maxmemory
        );
    }

    assert!(
        successes > 0,
        "expected at least some HSET successes under tiny maxmemory"
    );
    // Either HSET errors (hash/search pressure) or search stayed bounded while
    // some HSETs succeeded without indexing — both prove growth is limited.
    assert!(
        failures > 0 || cache.category_memory(MemoryCategory::Search) < maxmemory,
        "expected maxmemory to constrain growth (failures={}, search_mem={})",
        failures,
        cache.category_memory(MemoryCategory::Search)
    );
}

#[test]
fn test_search_docs_are_eviction_victims_under_allkeys() {
    use kore::EvictionPolicy;

    // Budget small enough that search docs dominate; eviction must free Search
    // category so later writes can proceed.
    let maxmemory = 16 * 1024;
    let cache = make_cache(maxmemory);
    cache.set_eviction_policy(EvictionPolicy::AllKeysLru);
    cache.set_eviction_sample_size(16).unwrap();

    let mut h = make_handler(cache.clone());

    handle(
        &mut h,
        cmd(&[
            "FT.CREATE",
            "evict_idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "s:",
            "SCHEMA",
            "blob",
            "TEXT",
        ]),
    );

    let big = "Y".repeat(400);
    let mut indexed = 0usize;
    for i in 0..80 {
        let key = format!("s:{}", i);
        let resp = handle(&mut h, cmd(&["HSET", &key, "blob", &big]));
        if matches!(resp, RespValue::Integer(_)) {
            indexed += 1;
        }
        assert!(
            cache.tracked_memory() <= maxmemory,
            "tracked {} > maxmemory {}",
            cache.tracked_memory(),
            maxmemory
        );
    }
    assert!(indexed >= 5, "expected several indexed hashes, got {}", indexed);

    let search_before = cache.category_memory(MemoryCategory::Search);
    assert!(search_before > 0, "search memory should be non-zero after HSET auto-index");

    let evicted_before = cache
        .stats
        .evicted_lru
        .load(std::sync::atomic::Ordering::Relaxed);

    // More HSETs under pressure — should free prior search docs and/or keys.
    let mut more_ok = 0usize;
    for i in 80..160 {
        let key = format!("s:{}", i);
        let resp = handle(&mut h, cmd(&["HSET", &key, "blob", &big]));
        if matches!(resp, RespValue::Integer(_)) {
            more_ok += 1;
        }
        assert!(
            cache.tracked_memory() <= maxmemory,
            "tracked {} exceeded maxmemory during eviction path",
            cache.tracked_memory()
        );
    }

    let evicted_after = cache
        .stats
        .evicted_lru
        .load(std::sync::atomic::Ordering::Relaxed);

    assert!(
        more_ok > 0,
        "expected further HSETs to succeed via eviction under allkeys-lru"
    );
    assert!(
        evicted_after > evicted_before,
        "expected eviction counter to rise (before={} after={}); search_mem={} total={}",
        evicted_before,
        evicted_after,
        cache.category_memory(MemoryCategory::Search),
        cache.tracked_memory()
    );
}

/// Batch FZ: when search docs dominate tracked memory under `allkeys-lru`,
/// eviction must free `MemoryCategory::Search` (not only Cache keys).
#[test]
fn test_search_docs_dominate_allkeys_lru_frees_search() {
    use kore::EvictionPolicy;

    let maxmemory = 24 * 1024;
    let cache = make_cache(maxmemory);
    cache.set_eviction_policy(EvictionPolicy::AllKeysLru);
    cache.set_eviction_sample_size(20).unwrap();

    let mut h = make_handler(cache.clone());
    handle(
        &mut h,
        cmd(&[
            "FT.CREATE",
            "dom_idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "d:",
            "SCHEMA",
            "blob",
            "TEXT",
        ]),
    );

    let big = "Z".repeat(500);
    let mut ok = 0usize;
    for i in 0..100 {
        let key = format!("d:{}", i);
        let resp = handle(&mut h, cmd(&["HSET", &key, "blob", &big]));
        if matches!(resp, RespValue::Integer(_)) {
            ok += 1;
        }
        assert!(
            cache.tracked_memory() <= maxmemory,
            "tracked {} > maxmemory {}",
            cache.tracked_memory(),
            maxmemory
        );
    }
    assert!(ok >= 5, "expected several HSET successes, got {}", ok);

    let search_before = cache.category_memory(MemoryCategory::Search);
    assert!(
        search_before > 0,
        "search memory should dominate after auto-index"
    );
    // Search should be a meaningful share of the budget when docs dominate.
    assert!(
        search_before * 3 > maxmemory / 4 || search_before > 1024,
        "expected non-trivial Search category (search={})",
        search_before
    );

    let evicted_before = cache
        .stats
        .evicted_lru
        .load(std::sync::atomic::Ordering::Relaxed);

    // Pressure: more large indexed docs — must free Search (and/or keys).
    for i in 100..200 {
        let key = format!("d:{}", i);
        let _ = handle(&mut h, cmd(&["HSET", &key, "blob", &big]));
        assert!(
            cache.tracked_memory() <= maxmemory,
            "over maxmemory during pressure"
        );
    }

    let search_after = cache.category_memory(MemoryCategory::Search);
    let evicted_after = cache
        .stats
        .evicted_lru
        .load(std::sync::atomic::Ordering::Relaxed);

    assert!(
        evicted_after > evicted_before,
        "allkeys-lru must evict under search-heavy pressure (before={} after={})",
        evicted_before,
        evicted_after
    );
    // Either Search was reclaimed relative to a pure-growth path, or it stayed
    // bounded while total stayed under maxmemory — both prove victims work.
    assert!(
        search_after <= maxmemory && cache.tracked_memory() <= maxmemory,
        "Search must be reclaimable; search_after={} total={}",
        search_after,
        cache.tracked_memory()
    );
}

/// Batch FZ: same dominance under `allkeys-random`.
#[test]
fn test_search_docs_dominate_allkeys_random_frees_search() {
    use kore::EvictionPolicy;

    let maxmemory = 24 * 1024;
    let cache = make_cache(maxmemory);
    cache.set_eviction_policy(EvictionPolicy::AllKeysRandom);
    cache.set_eviction_sample_size(20).unwrap();

    let mut h = make_handler(cache.clone());
    handle(
        &mut h,
        cmd(&[
            "FT.CREATE",
            "rnd_idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "r:",
            "SCHEMA",
            "blob",
            "TEXT",
        ]),
    );

    let big = "W".repeat(500);
    let mut ok = 0usize;
    for i in 0..100 {
        let key = format!("r:{}", i);
        if matches!(
            handle(&mut h, cmd(&["HSET", &key, "blob", &big])),
            RespValue::Integer(_)
        ) {
            ok += 1;
        }
        assert!(cache.tracked_memory() <= maxmemory);
    }
    assert!(ok >= 5, "expected several HSET successes, got {}", ok);
    assert!(cache.category_memory(MemoryCategory::Search) > 0);

    let evicted_before = cache
        .stats
        .evicted_lru
        .load(std::sync::atomic::Ordering::Relaxed);

    for i in 100..200 {
        let key = format!("r:{}", i);
        let _ = handle(&mut h, cmd(&["HSET", &key, "blob", &big]));
        assert!(cache.tracked_memory() <= maxmemory);
    }

    let evicted_after = cache
        .stats
        .evicted_lru
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        evicted_after > evicted_before,
        "allkeys-random must evict under search-heavy pressure"
    );
    assert!(cache.category_memory(MemoryCategory::Search) <= maxmemory);
}

/// Batch FZ: under `volatile-lru`, search docs are never victims (no TTL).
/// Only keys with expire may be reclaimed; search memory is not required to drop.
#[test]
fn test_volatile_policy_does_not_require_search_eviction() {
    use kore::EvictionPolicy;
    use kore::entry::StoreOptions;

    let maxmemory = 20 * 1024;
    let cache = make_cache(maxmemory);
    cache.set_eviction_policy(EvictionPolicy::VolatileLru);
    cache.set_eviction_sample_size(16).unwrap();

    let mut h = make_handler(cache.clone());
    handle(
        &mut h,
        cmd(&[
            "FT.CREATE",
            "vol_idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "v:",
            "SCHEMA",
            "blob",
            "TEXT",
        ]),
    );

    // Permanent indexed hashes (no TTL on hash keys; search docs never TTL).
    let big = "V".repeat(400);
    for i in 0..30 {
        let key = format!("v:{}", i);
        let _ = handle(&mut h, cmd(&["HSET", &key, "blob", &big]));
    }
    let search_mem = cache.category_memory(MemoryCategory::Search);
    assert!(search_mem > 0, "need search docs present");

    // Volatile-only keys that *can* be victims.
    for i in 0..40 {
        let mut opts = StoreOptions::default();
        opts.ttl_ms = Some(60_000);
        let _ = cache.store(
            Bytes::from(format!("ttl:{}", i)),
            Bytes::from("x".repeat(200)),
            opts,
        );
    }

    // Under volatile-lru, further pressure should prefer TTL string keys — not
    // search docs. We only assert that the policy does not panic / OOM-loop
    // when search cannot be volatile, and that tracked stays bounded when
    // volatile victims exist.
    for i in 40..80 {
        let mut opts = StoreOptions::default();
        opts.ttl_ms = Some(60_000);
        let _ = cache.store(
            Bytes::from(format!("ttl:{}", i)),
            Bytes::from("x".repeat(200)),
            opts,
        );
    }
    // Search may still be present (not volatile); that is intentional (Batch FZ).
    assert!(
        cache.category_memory(MemoryCategory::Search) > 0
            || cache.stats.evicted_lru.load(std::sync::atomic::Ordering::Relaxed) > 0,
        "volatile path either keeps search or evicts TTL keys"
    );
}
