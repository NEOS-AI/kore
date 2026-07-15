//! Concurrency stress tests for shard RMW paths (INCR/DECR, SET NX, CAS-style ops).
//!
//! These are multi-thread stress tests (not loom model-checking). They aim to
//! surface lost updates and TOCTOU races under contention on the same key and
//! across many keys/shards.

use bytes::Bytes;
use kore::entry::{LoadOptions, StoreOptions};
use kore::Cache;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn cache(shards: usize) -> Arc<Cache> {
    Cache::new_with_sweep(shards, 256 * 1024 * 1024, 16 * 1024 * 1024, false)
}

#[test]
fn stress_concurrent_incr_same_key() {
    let cache = cache(64);
    let key = Bytes::from("counter");
    let threads = 16usize;
    let iters = 500usize;
    let mut handles = Vec::new();

    for _ in 0..threads {
        let c = Arc::clone(&cache);
        let k = key.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..iters {
                c.incr(&k, 1).expect("incr");
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let entry = cache
        .load(&key, LoadOptions::default())
        .unwrap()
        .expect("counter exists");
    let n: i64 = std::str::from_utf8(&entry.value)
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        n,
        (threads * iters) as i64,
        "lost updates under concurrent INCR"
    );
}

#[test]
fn stress_concurrent_incr_decr_net_zero() {
    let cache = cache(32);
    let key = Bytes::from("bal");
    // Seed
    cache
        .store(
            key.clone(),
            Bytes::from("0"),
            StoreOptions::default(),
        )
        .unwrap();

    let threads = 8usize;
    let iters = 400usize;
    let mut handles = Vec::new();
    for t in 0..threads {
        let c = Arc::clone(&cache);
        let k = key.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..iters {
                if t % 2 == 0 {
                    c.incr(&k, 1).unwrap();
                } else {
                    c.incr(&k, -1).unwrap();
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let entry = cache.load(&key, LoadOptions::default()).unwrap().unwrap();
    let n: i64 = std::str::from_utf8(&entry.value).unwrap().parse().unwrap();
    // Equal number of +1 and -1 threads
    assert_eq!(n, 0, "INCR/DECR pair should net to zero, got {n}");
}

#[test]
fn stress_set_nx_single_winner() {
    let cache = cache(16);
    let key = Bytes::from("lock-key");
    let winners = Arc::new(AtomicUsize::new(0));
    let threads = 32usize;
    let mut handles = Vec::new();

    for i in 0..threads {
        let c = Arc::clone(&cache);
        let k = key.clone();
        let w = Arc::clone(&winners);
        handles.push(thread::spawn(move || {
            let mut opts = StoreOptions::default();
            opts.nx = true;
            let val = Bytes::from(format!("t{i}"));
            match c.store(k, val, opts) {
                Ok(None) => {
                    // Newly set
                    w.fetch_add(1, Ordering::SeqCst);
                }
                Ok(Some(_)) => {
                    // Should not happen with NX when we only set once
                }
                Err(_) => {}
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(
        winners.load(Ordering::SeqCst),
        1,
        "exactly one SET NX winner expected"
    );
    assert!(cache.load(&key, LoadOptions::default()).unwrap().is_some());
}

#[test]
fn stress_multi_key_incr_across_shards() {
    let shards = 64usize;
    let cache = cache(shards);
    let keys: Vec<Bytes> = (0..shards)
        .map(|i| Bytes::from(format!("k{i}")))
        .collect();
    let threads = 8usize;
    let iters = 100usize;
    let mut handles = Vec::new();

    for t in 0..threads {
        let c = Arc::clone(&cache);
        let ks = keys.clone();
        handles.push(thread::spawn(move || {
            for i in 0..iters {
                let k = &ks[(t + i) % ks.len()];
                c.incr(k, 1).unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let mut sum = 0i64;
    for k in &keys {
        if let Some(e) = cache.load(k, LoadOptions::default()).unwrap() {
            sum += std::str::from_utf8(&e.value).unwrap().parse::<i64>().unwrap();
        }
    }
    assert_eq!(sum, (threads * iters) as i64);
}

#[test]
fn stress_mixed_rmw_and_reads() {
    let cache = cache(32);
    let key = Bytes::from("mixed");
    cache
        .store(key.clone(), Bytes::from("0"), StoreOptions::default())
        .unwrap();

    let stop = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    // Writers
    for _ in 0..4 {
        let c = Arc::clone(&cache);
        let k = key.clone();
        let s = Arc::clone(&stop);
        handles.push(thread::spawn(move || {
            while s.load(Ordering::Relaxed) == 0 {
                let _ = c.incr(&k, 1);
            }
        }));
    }
    // Readers
    for _ in 0..4 {
        let c = Arc::clone(&cache);
        let k = key.clone();
        let s = Arc::clone(&stop);
        handles.push(thread::spawn(move || {
            while s.load(Ordering::Relaxed) == 0 {
                let _ = c.load(&k, LoadOptions::default());
            }
        }));
    }

    thread::sleep(Duration::from_millis(150));
    stop.store(1, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }

    let entry = cache.load(&key, LoadOptions::default()).unwrap().unwrap();
    let n: i64 = std::str::from_utf8(&entry.value).unwrap().parse().unwrap();
    assert!(n >= 0, "counter should remain a valid non-negative integer");
}

#[test]
fn stress_hash_hincr_concurrent() {
    // Exercise hash field RMW via parking_lot-guarded hash under threads.
    let cache = cache(16);
    let key = Bytes::from("h");
    let field = Bytes::from("f");
    let hash = cache.get_or_create_hash(&key).unwrap();
    {
        let mut h = hash.write();
        h.hset(field.clone(), Bytes::from("0"));
    }

    let threads = 10usize;
    let iters = 200usize;
    let mut handles = Vec::new();
    for _ in 0..threads {
        let hshared = cache.get_hash(&key).unwrap();
        let f = field.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..iters {
                let mut g = hshared.write();
                let cur: i64 = g
                    .hget(&f)
                    .and_then(|b| std::str::from_utf8(&b).ok()?.parse().ok())
                    .unwrap_or(0);
                g.hset(f.clone(), Bytes::from((cur + 1).to_string()));
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let final_v: i64 = hash
        .read()
        .hget(&field)
        .and_then(|b| std::str::from_utf8(&b).ok()?.parse().ok())
        .unwrap();
    assert_eq!(final_v, (threads * iters) as i64);
}
