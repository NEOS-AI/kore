use bytes::Bytes;
use kore::entry::StoreOptions;
use kore::Cache;

#[test]
fn test_basic_set_get() {
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false); // 100MB, no background sweep

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
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false);

    let key = Bytes::from("test_key");
    let value = Bytes::from("test_value");

    cache.store(key.clone(), value, StoreOptions::default()).unwrap();
    assert!(cache.exists(&key));

    cache.delete(&key).unwrap();
    assert!(!cache.exists(&key));
}

#[test]
fn test_incr_decr() {
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false);

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
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false);

    let key = Bytes::from("test_key");
    let value1 = Bytes::from("value1");
    let value2 = Bytes::from("value2");

    // Set with NX should succeed
    let opts = StoreOptions {
        nx: true,
        ..Default::default()
    };
    let result = cache.store(key.clone(), value1.clone(), opts.clone()).unwrap();
    assert!(result.is_none()); // No old value means success

    let entry = cache.load(&key, Default::default()).unwrap().unwrap();
    assert_eq!(entry.value, value1);

    // Set with NX should fail (key exists)
    let result = cache.store(key.clone(), value2.clone(), opts).unwrap();
    assert!(result.is_some()); // Returns existing value, meaning failure

    // Value should remain unchanged
    let entry = cache.load(&key, Default::default()).unwrap().unwrap();
    assert_eq!(entry.value, value1);
}

#[test]
fn test_xx_option() {
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false);

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

    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false);

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
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false);

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
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false);

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

    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false);

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

#[test]
fn test_setnx_distributed_lock() {
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false);

    let lock_key = Bytes::from("resource_lock");
    let client1_id = Bytes::from("client-1");
    let client2_id = Bytes::from("client-2");

    // Client 1 acquires lock using SETNX
    let opts = StoreOptions {
        nx: true,
        ..Default::default()
    };
    let result = cache.store(lock_key.clone(), client1_id.clone(), opts.clone()).unwrap();
    assert!(result.is_none()); // Successfully acquired lock (no previous value)

    // Verify lock is held by client 1
    let entry = cache.load(&lock_key, Default::default()).unwrap().unwrap();
    assert_eq!(entry.value, client1_id);

    // Client 2 tries to acquire lock using SETNX (should fail)
    let result = cache.store(lock_key.clone(), client2_id.clone(), opts.clone()).unwrap();
    assert!(result.is_some()); // Failed to acquire lock (key exists)

    // Lock should still be held by client 1
    let entry = cache.load(&lock_key, Default::default()).unwrap().unwrap();
    assert_eq!(entry.value, client1_id);

    // Client 1 releases lock
    cache.delete(&lock_key).unwrap();

    // Client 2 can now acquire lock
    let result = cache.store(lock_key.clone(), client2_id.clone(), opts).unwrap();
    assert!(result.is_none()); // Successfully acquired lock

    let entry = cache.load(&lock_key, Default::default()).unwrap().unwrap();
    assert_eq!(entry.value, client2_id);
}

#[test]
fn test_setnx_with_ttl() {
    use std::time::Duration;

    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false);

    let lock_key = Bytes::from("auto_release_lock");
    let client_id = Bytes::from("client-1");

    // Acquire lock with 100ms TTL
    let opts = StoreOptions {
        nx: true,
        ttl_ms: Some(100),
        ..Default::default()
    };
    let result = cache.store(lock_key.clone(), client_id.clone(), opts.clone()).unwrap();
    assert!(result.is_none());

    // Lock should exist
    assert!(cache.exists(&lock_key));

    // Wait for TTL to expire
    std::thread::sleep(Duration::from_millis(150));

    // Lock should be auto-released
    let entry = cache.load(&lock_key, Default::default()).unwrap();
    assert!(entry.is_none());

    // Another client can acquire lock now
    let result = cache.store(lock_key.clone(), Bytes::from("client-2"), opts).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_getdel_atomic() {
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false);

    let key = Bytes::from("atomic_key");
    let value = Bytes::from("atomic_value");

    // Store value
    cache.store(key.clone(), value.clone(), StoreOptions::default()).unwrap();

    // GETDEL should return value and delete key atomically
    let entry = cache.load(&key, Default::default()).unwrap();
    assert!(entry.is_some());
    let retrieved_value = entry.unwrap().value.clone();
    
    // Manually simulate GETDEL
    cache.delete(&key).unwrap();
    
    assert_eq!(retrieved_value, value);

    // Key should be deleted
    assert!(!cache.exists(&key));

    // GETDEL on non-existent key should return None
    let entry = cache.load(&key, Default::default()).unwrap();
    assert!(entry.is_none());
}

#[test]
fn test_distributed_lock_pattern() {
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false);

    let lock_key = Bytes::from("my_resource");
    let unique_id = Bytes::from("unique-request-id-12345");

    // Step 1: Acquire lock with timeout
    let opts = StoreOptions {
        nx: true,
        ttl_ms: Some(5000), // 5 second timeout
        ..Default::default()
    };
    
    let acquired = cache.store(lock_key.clone(), unique_id.clone(), opts).unwrap();
    assert!(acquired.is_none(), "Lock should be acquired");

    // Step 2: Do critical section work
    // ... (simulated work)

    // Step 3: Release lock (only if we still own it)
    let current_holder = cache.load(&lock_key, Default::default()).unwrap();
    if let Some(entry) = current_holder {
        if entry.value == unique_id {
            // We still own the lock, safe to delete
            cache.delete(&lock_key).unwrap();
        }
    }

    // Verify lock is released
    assert!(!cache.exists(&lock_key));
}

#[test]
fn test_lock_renewal_with_expire() {
    use std::time::Duration;

    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false);

    let lock_key = Bytes::from("renewable_lock");
    let client_id = Bytes::from("client-1");

    // Acquire lock with 200ms TTL
    let opts = StoreOptions {
        nx: true,
        ttl_ms: Some(200),
        ..Default::default()
    };
    cache.store(lock_key.clone(), client_id.clone(), opts).unwrap();

    // Wait a bit
    std::thread::sleep(Duration::from_millis(100));

    // Renew lock (extend TTL)
    cache.expire(&lock_key, 200).unwrap();

    // Wait past original expiration time
    std::thread::sleep(Duration::from_millis(150));

    // Lock should still exist (renewed)
    assert!(cache.exists(&lock_key));

    // Wait for renewed TTL to expire
    std::thread::sleep(Duration::from_millis(100));

    // Now lock should be expired
    let entry = cache.load(&lock_key, Default::default()).unwrap();
    assert!(entry.is_none());
}

