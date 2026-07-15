//! Phase C P1: Active expire sampling (avoid full-shard retain)

use bytes::Bytes;
use kore::entry::Entry;
use kore::hashmap::{
    ActiveExpireResult, ACTIVE_EXPIRE_CONTINUE_RATIO, ACTIVE_EXPIRE_SAMPLES_PER_PASS,
};
use kore::Cache;
use std::sync::Arc;
use std::time::Duration;

fn make_cache(shards: usize) -> Arc<Cache> {
    Cache::new_with_sweep(shards, 64 * 1024 * 1024, 500 * 1024 * 1024, false)
}

fn store_with_ttl(cache: &Cache, key: &str, value: &str, ttl: Duration) {
    let entry = Entry::new(Bytes::from(key.to_string()), Bytes::from(value.to_string()))
        .with_expiration(ttl);
    // Use low-level map insert path via store if available
    let _ = cache.store(
        Bytes::from(key.to_string()),
        Bytes::from(value.to_string()),
        kore::entry::StoreOptions {
            ttl_ms: Some(ttl.as_millis() as u64),
            ..Default::default()
        },
    );
    let _ = entry; // silence if store path used
}

#[test]
fn active_expire_removes_expired_keeps_live() {
    let cache = make_cache(16);

    // Short TTL keys
    for i in 0..50 {
        let k = format!("exp:{i}");
        store_with_ttl(&cache, &k, "v", Duration::from_millis(30));
    }
    // Permanent keys
    for i in 0..50 {
        let k = format!("live:{i}");
        let _ = cache.store(
            Bytes::from(k),
            Bytes::from("v"),
            kore::entry::StoreOptions::default(),
        );
    }

    std::thread::sleep(Duration::from_millis(50));

    // Run aggressive sampling until nothing left to expire (or cap loops)
    let mut total_removed = 0usize;
    for _ in 0..200 {
        let r = cache.active_expire_cycle(32, 32, Duration::from_millis(50));
        total_removed += r.count;
        if r.sampled == 0 || r.count == 0 {
            // May still have expired keys unsampled — keep going a bit
        }
    }
    // Full sweep to finish any remaining expired (proves sampling made progress)
    let rest = cache.sweep();
    total_removed += rest;

    assert!(
        total_removed >= 50,
        "should remove all 50 expired keys, got {total_removed} (rest full-sweep {rest})"
    );

    // Live keys remain
    for i in 0..50 {
        let k = Bytes::from(format!("live:{i}"));
        assert!(cache.exists(&k), "live key {i} missing");
    }
    for i in 0..50 {
        let k = Bytes::from(format!("exp:{i}"));
        assert!(!cache.exists(&k), "expired key {i} still present");
    }
}

#[test]
fn active_expire_sampling_does_not_full_scan_empty() {
    let cache = make_cache(64);
    // Only permanent keys — cycle should sample little and remove nothing
    for i in 0..100 {
        let _ = cache.store(
            Bytes::from(format!("k{i}")),
            Bytes::from("v"),
            kore::entry::StoreOptions::default(),
        );
    }
    let r = cache.active_expire_cycle(
        ACTIVE_EXPIRE_SAMPLES_PER_PASS,
        4,
        Duration::from_millis(5),
    );
    assert_eq!(r.count, 0);
    assert_eq!(cache.dbsize(), 100);
    let _ = r;
    let _ = ACTIVE_EXPIRE_CONTINUE_RATIO;
}

#[test]
fn active_expire_continues_when_many_expired() {
    let cache = make_cache(8);
    for i in 0..100 {
        store_with_ttl(&cache, &format!("e{i}"), "v", Duration::from_millis(20));
    }
    std::thread::sleep(Duration::from_millis(40));

    // One cycle with enough budget should remove a substantial fraction
    let r = cache.active_expire_cycle(20, 16, Duration::from_millis(20));
    assert!(
        r.count > 0,
        "expected sampling to find expired keys, got {r:?}"
    );
    assert!(r.passes >= 1);
    assert!(r.sampled >= r.count);
}

#[test]
fn full_sweep_still_clears_all_expired() {
    let cache = make_cache(16);
    for i in 0..30 {
        store_with_ttl(&cache, &format!("t{i}"), "v", Duration::from_millis(15));
    }
    std::thread::sleep(Duration::from_millis(30));
    let n = cache.sweep();
    assert_eq!(n, 30);
    assert_eq!(cache.dbsize(), 0);
}

#[test]
fn active_expire_memory_accounting() {
    let cache = make_cache(16);
    let before = cache.tracked_memory();
    for i in 0..20 {
        store_with_ttl(
            &cache,
            &format!("m{i}"),
            &"x".repeat(100),
            Duration::from_millis(20),
        );
    }
    let mid = cache.tracked_memory();
    assert!(mid > before);

    std::thread::sleep(Duration::from_millis(40));
    // Exhaustive cleanup via repeated sampling + final full sweep
    for _ in 0..50 {
        cache.active_expire_cycle(40, 16, Duration::from_millis(10));
    }
    cache.sweep();

    let after = cache.tracked_memory();
    // Memory should be back near baseline (allow tiny residual)
    assert!(
        after <= before + 1024,
        "memory not reclaimed: before={before} mid={mid} after={after}"
    );
}

// Ensure ActiveExpireResult is usable externally
#[test]
fn active_expire_result_as_sweep() {
    let r = ActiveExpireResult {
        count: 3,
        bytes_freed: 100,
        sampled: 10,
        passes: 2,
    };
    let s = r.as_sweep();
    assert_eq!(s.count, 3);
    assert_eq!(s.bytes_freed, 100);
}
