//! Phase A P0 correctness tests: EXAT/PXAT, concurrent RMW, memory accounting.

use bytes::Bytes;
use kore::entry::{LoadOptions, StoreOptions};
use kore::Cache;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn cache() -> Arc<Cache> {
    Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false)
}

#[test]
fn test_exat_absolute_unix_timestamp() {
    let cache = cache();
    let key = Bytes::from("exat_key");
    let value = Bytes::from("v");

    // Expire 2 seconds from now (absolute Unix seconds → EXAT uses seconds; we store ms in opts)
    let exat_ms = now_ms() + 2_000;
    let mut opts = StoreOptions::default();
    opts.exat_ms = Some(exat_ms);

    cache.store(key.clone(), value, opts).unwrap();

    let entry = cache
        .load(&key, LoadOptions::default())
        .unwrap()
        .expect("key should exist");
    let ttl = entry.ttl_millis().expect("should have TTL");
    // Remaining TTL should be roughly 2000ms, not ~exat_ms (which would be decades)
    assert!(
        ttl > 500 && ttl <= 2_500,
        "EXAT should be absolute; expected ~2000ms remaining, got {}",
        ttl
    );
}

#[test]
fn test_pxat_past_timestamp_is_immediately_expired() {
    let cache = cache();
    let key = Bytes::from("pxat_past");
    let mut opts = StoreOptions::default();
    // Far in the past
    opts.exat_ms = Some(1_000);

    cache
        .store(key.clone(), Bytes::from("gone"), opts)
        .unwrap();

    // Immediately expired → load should miss
    let loaded = cache.load(&key, LoadOptions::default()).unwrap();
    assert!(loaded.is_none(), "past EXAT/PXAT key should be expired");
}

#[test]
fn test_exat_not_treated_as_relative_duration() {
    let cache = cache();
    let key = Bytes::from("exat_not_relative");
    // A real-world Unix ms timestamp (year ~2020). If treated as relative Duration
    // from Instant::now(), TTL would be enormous (~50 years in ms).
    let year_2020_ms: u64 = 1_577_836_800_000;
    let mut opts = StoreOptions::default();
    opts.exat_ms = Some(year_2020_ms);

    cache
        .store(key.clone(), Bytes::from("old"), opts)
        .unwrap();

    // 2020 is in the past relative to 2026 → expired
    let loaded = cache.load(&key, LoadOptions::default()).unwrap();
    assert!(
        loaded.is_none(),
        "historical absolute timestamp must expire, not live for decades"
    );
}

#[test]
fn test_concurrent_incr_no_lost_updates() {
    let cache = cache();
    let key = Bytes::from("counter");
    let threads = 8;
    let incs_per_thread = 500;
    let mut handles = Vec::new();

    for _ in 0..threads {
        let c = cache.clone();
        let k = key.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..incs_per_thread {
                c.incr(&k, 1).unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let final_val = cache.incr(&key, 0).unwrap();
    assert_eq!(
        final_val,
        (threads * incs_per_thread) as i64,
        "concurrent INCR must not lose updates"
    );
}

#[test]
fn test_concurrent_set_nx_only_one_wins() {
    let cache = cache();
    let key = Bytes::from("lock");
    let winners = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for i in 0..16 {
        let c = cache.clone();
        let k = key.clone();
        let w = winners.clone();
        handles.push(thread::spawn(move || {
            let mut opts = StoreOptions::default();
            opts.nx = true;
            let result = c
                .store(k, Bytes::from(format!("client-{}", i)), opts)
                .unwrap();
            // Ok(None) when GET not set means success for NX store path;
            // Ok(Some(existing)) means NX failed because key exists.
            // Looking at store: NX fail returns Ok(Some(existing)); success returns Ok(old_for_get) which is None when get=false.
            if result.is_none() {
                w.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(
        winners.load(Ordering::SeqCst),
        1,
        "exactly one concurrent SET NX should succeed"
    );
    assert!(cache.exists(&key));
}

#[test]
fn test_concurrent_cas_serialized() {
    let cache = cache();
    let key = Bytes::from("cas_key");
    cache
        .store(key.clone(), Bytes::from("0"), StoreOptions::default())
        .unwrap();

    let successes = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for _ in 0..8 {
        let c = cache.clone();
        let k = key.clone();
        let s = successes.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..100 {
                // Read current CAS
                let entry = match c.load(&k, LoadOptions::default()).unwrap() {
                    Some(e) => e,
                    None => continue,
                };
                let current: i64 = std::str::from_utf8(&entry.value)
                    .unwrap()
                    .parse()
                    .unwrap_or(0);
                let mut opts = StoreOptions::default();
                opts.cas = Some(entry.cas);
                match c.store(k.clone(), Bytes::from((current + 1).to_string()), opts) {
                    Ok(_) => {
                        s.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(_) => {
                        // CAS mismatch — expected under contention
                    }
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let entry = cache.load(&key, LoadOptions::default()).unwrap().unwrap();
    let final_val: i64 = std::str::from_utf8(&entry.value).unwrap().parse().unwrap();
    let wins = successes.load(Ordering::SeqCst);

    assert_eq!(
        final_val as usize, wins,
        "final value must equal number of successful CAS writes"
    );
    assert!(wins > 0, "at least some CAS writes should succeed");
}

#[test]
fn test_memory_accounting_store_replace_delete() {
    let cache = cache();
    assert_eq!(cache.memory_usage(), 0);
    assert_eq!(cache.tracked_cache_memory(), 0);

    let key = Bytes::from("mem_key");
    cache
        .store(key.clone(), Bytes::from("hello"), StoreOptions::default())
        .unwrap();
    let after_store = cache.memory_usage();
    assert!(after_store > 0);
    assert_eq!(cache.memory_usage(), cache.tracked_cache_memory());

    // Replace with larger value
    cache
        .store(
            key.clone(),
            Bytes::from("hello world, longer value"),
            StoreOptions::default(),
        )
        .unwrap();
    let after_replace = cache.memory_usage();
    assert!(after_replace > after_store);
    assert_eq!(cache.memory_usage(), cache.tracked_cache_memory());

    cache.delete(&key).unwrap();
    assert_eq!(cache.memory_usage(), 0);
    assert_eq!(cache.tracked_cache_memory(), 0);
}

#[test]
fn test_memory_accounting_expire_load_and_sweep() {
    let cache = cache();
    let key = Bytes::from("expire_mem");
    let mut opts = StoreOptions::default();
    opts.ttl_ms = Some(50);

    cache
        .store(key.clone(), Bytes::from("temp"), opts)
        .unwrap();
    assert!(cache.memory_usage() > 0);
    assert_eq!(cache.memory_usage(), cache.tracked_cache_memory());

    thread::sleep(Duration::from_millis(80));

    // Lazy expire on load
    let loaded = cache.load(&key, LoadOptions::default()).unwrap();
    assert!(loaded.is_none());
    assert_eq!(cache.memory_usage(), 0);
    assert_eq!(cache.tracked_cache_memory(), 0);

    // Sweep path: store another short-lived key, sleep, sweep
    let key2 = Bytes::from("expire_sweep");
    let mut opts2 = StoreOptions::default();
    opts2.ttl_ms = Some(50);
    cache
        .store(key2, Bytes::from("sweep_me"), opts2)
        .unwrap();
    assert!(cache.memory_usage() > 0);

    thread::sleep(Duration::from_millis(80));
    let removed = cache.sweep();
    assert!(removed >= 1);
    assert_eq!(cache.memory_usage(), 0);
    assert_eq!(cache.tracked_cache_memory(), 0);
}

#[test]
fn test_memory_accounting_flush_resets_both_counters() {
    let cache = cache();
    for i in 0..10 {
        cache
            .store(
                Bytes::from(format!("k{}", i)),
                Bytes::from("v"),
                StoreOptions::default(),
            )
            .unwrap();
    }
    assert!(cache.memory_usage() > 0);
    assert!(cache.tracked_memory() > 0);

    cache.flush();
    assert_eq!(cache.memory_usage(), 0);
    assert_eq!(cache.tracked_memory(), 0);
    assert_eq!(cache.tracked_cache_memory(), 0);
    assert_eq!(cache.dbsize(), 0);
}
