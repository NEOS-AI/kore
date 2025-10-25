use bytes::Bytes;
use kore::entry::StoreOptions;
use kore::Cache;

#[test]
fn test_basic_set_get() {
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, false); // 100MB, no background sweep

    let key = Bytes::from("test_key");
    let value = Bytes::from("test_value");

    // Store a value
    cache.store(key.clone(), value.clone(), StoreOptions::default()).unwrap();

    // Load the value
    let entry = cache.load(&key, Default::default()).unwrap().unwrap();
    assert_eq!(entry.value, value);
}

#[test]
fn test_delete() {
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, false);

    let key = Bytes::from("test_key");
    let value = Bytes::from("test_value");

    cache.store(key.clone(), value, StoreOptions::default()).unwrap();
    assert!(cache.exists(&key));

    cache.delete(&key).unwrap();
    assert!(!cache.exists(&key));
}

#[test]
fn test_incr_decr() {
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, false);

    let key = Bytes::from("counter");

    // Increment from 0
    let val = cache.incr(&key, 1).unwrap();
    assert_eq!(val, 1);

    // Increment by 10
    let val = cache.incr(&key, 10).unwrap();
    assert_eq!(val, 11);

    // Decrement by 5
    let val = cache.decr(&key, 5).unwrap();
    assert_eq!(val, 6);
}

#[test]
fn test_nx_option() {
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, false);

    let key = Bytes::from("test_key");
    let value1 = Bytes::from("value1");
    let value2 = Bytes::from("value2");

    // Set with NX should succeed
    let opts = StoreOptions {
        nx: true,
        ..Default::default()
    };
    cache.store(key.clone(), value1.clone(), opts.clone()).unwrap();

    let entry = cache.load(&key, Default::default()).unwrap().unwrap();
    assert_eq!(entry.value, value1);

    // Set with NX should fail (key exists)
    let result = cache.store(key.clone(), value2.clone(), opts).unwrap();
    assert!(result.is_none());

    // Value should remain unchanged
    let entry = cache.load(&key, Default::default()).unwrap().unwrap();
    assert_eq!(entry.value, value1);
}

#[test]
fn test_xx_option() {
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, false);

    let key = Bytes::from("test_key");
    let value1 = Bytes::from("value1");
    let value2 = Bytes::from("value2");

    // Set with XX should fail (key doesn't exist)
    let opts = StoreOptions {
        xx: true,
        ..Default::default()
    };
    let result = cache.store(key.clone(), value1.clone(), opts.clone()).unwrap();
    assert!(result.is_none());

    // Set normally
    cache.store(key.clone(), value1.clone(), StoreOptions::default()).unwrap();

    // Set with XX should succeed (key exists)
    cache.store(key.clone(), value2.clone(), opts).unwrap();

    let entry = cache.load(&key, Default::default()).unwrap().unwrap();
    assert_eq!(entry.value, value2);
}

#[test]
fn test_expiration() {
    use std::time::Duration;

    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, false);

    let key = Bytes::from("temp_key");
    let value = Bytes::from("temp_value");

    // Set with 100ms TTL
    let opts = StoreOptions {
        ttl_ms: Some(100),
        ..Default::default()
    };
    cache.store(key.clone(), value, opts).unwrap();

    // Should exist immediately
    assert!(cache.exists(&key));

    // Wait for expiration
    std::thread::sleep(Duration::from_millis(150));

    // Should be expired now
    let result = cache.load(&key, Default::default()).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_flush() {
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, false);

    // Add some entries
    for i in 0..10 {
        let key = Bytes::from(format!("key{}", i));
        let value = Bytes::from(format!("value{}", i));
        cache.store(key, value, StoreOptions::default()).unwrap();
    }

    assert_eq!(cache.dbsize(), 10);

    // Flush all
    cache.flush();

    assert_eq!(cache.dbsize(), 0);
}

#[test]
fn test_keys_pattern() {
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, false);

    // Add entries with different prefixes
    cache.store(Bytes::from("user:1"), Bytes::from("alice"), StoreOptions::default()).unwrap();
    cache.store(Bytes::from("user:2"), Bytes::from("bob"), StoreOptions::default()).unwrap();
    cache.store(Bytes::from("post:1"), Bytes::from("hello"), StoreOptions::default()).unwrap();
    cache.store(Bytes::from("post:2"), Bytes::from("world"), StoreOptions::default()).unwrap();

    // Get all keys
    let all_keys = cache.keys(None);
    assert_eq!(all_keys.len(), 4);

    // Get keys matching pattern
    let user_keys = cache.keys(Some("user:*"));
    assert_eq!(user_keys.len(), 2);

    let post_keys = cache.keys(Some("post:*"));
    assert_eq!(post_keys.len(), 2);
}

#[test]
fn test_stats() {
    use std::sync::atomic::Ordering;

    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, false);

    let key = Bytes::from("test_key");
    let value = Bytes::from("test_value");

    // Store operation
    cache.store(key.clone(), value, StoreOptions::default()).unwrap();
    assert_eq!(cache.stats.cmd_set.load(Ordering::Relaxed), 1);

    // Load operation (hit)
    cache.load(&key, Default::default()).unwrap();
    assert_eq!(cache.stats.cmd_get.load(Ordering::Relaxed), 1);
    assert_eq!(cache.stats.hits.load(Ordering::Relaxed), 1);

    // Load operation (miss)
    let nonexistent = Bytes::from("nonexistent");
    cache.load(&nonexistent, Default::default()).unwrap();
    assert_eq!(cache.stats.cmd_get.load(Ordering::Relaxed), 2);
    assert_eq!(cache.stats.misses.load(Ordering::Relaxed), 1);

    // Delete operation
    cache.delete(&key).unwrap();
    assert_eq!(cache.stats.cmd_del.load(Ordering::Relaxed), 1);
}
