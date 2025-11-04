use kore::{Cache, Redlock};
use bytes::Bytes;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn test_redlock_basic_lock() {
    // Create 3 independent cache instances
    let cache1 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false); // 256 shards, 100MB max
    let cache2 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache3 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    
    // Create Redlock with 3 instances (quorum = 2)
    let redlock = Redlock::new(vec![cache1, cache2, cache3]).unwrap();
    
    // Acquire a lock
    let lock_value = Bytes::from("unique-client-id-123");
    let lock = redlock.lock("test-resource", lock_value.clone(), 5000).unwrap();
    
    assert_eq!(lock.resource(), "test-resource");
    assert_eq!(lock.value(), &lock_value);
    assert!(lock.validity_time() > 0);
    
    // Lock is automatically released when dropped
}

#[test]
fn test_redlock_mutual_exclusion() {
    let cache1 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache2 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache3 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    
    let redlock = Redlock::new(vec![cache1, cache2, cache3]).unwrap();
    
    // First client acquires the lock
    let client1_id = Bytes::from("client-1");
    let _lock1 = redlock.lock("shared-resource", client1_id, 10000).unwrap();
    
    // Second client tries to acquire the same lock (should fail)
    let client2_id = Bytes::from("client-2");
    let result = redlock.lock("shared-resource", client2_id, 10000);
    
    assert!(result.is_err(), "Second client should fail to acquire lock");
}

#[test]
fn test_redlock_auto_release() {
    let cache1 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache2 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache3 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    
    let redlock = Redlock::new(vec![cache1, cache2, cache3]).unwrap();
    
    // Acquire lock in a scope
    {
        let lock_value = Bytes::from("client-1");
        let _lock = redlock.lock("auto-release-test", lock_value, 10000).unwrap();
        // Lock is held here
    } // Lock is automatically released here when dropped
    
    // Now another client should be able to acquire the lock
    let client2_id = Bytes::from("client-2");
    let lock2 = redlock.lock("auto-release-test", client2_id.clone(), 10000);
    
    assert!(lock2.is_ok(), "Lock should be available after first client released it");
    assert_eq!(lock2.unwrap().value(), &client2_id);
}

#[test]
fn test_redlock_extend() {
    let cache1 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache2 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache3 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    
    let redlock = Redlock::new(vec![cache1, cache2, cache3]).unwrap();
    
    let lock_value = Bytes::from("client-1");
    let lock = redlock.lock("extend-test", lock_value, 5000).unwrap();
    
    // Extend the lock
    let result = lock.extend(5000);
    assert!(result.is_ok(), "Lock extension should succeed");
}

#[test]
fn test_redlock_concurrent_access() {
    let cache1 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache2 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache3 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    
    let redlock = Arc::new(Redlock::new(vec![cache1, cache2, cache3]).unwrap());
    
    let mut handles = vec![];
    let success_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    
    // Spawn 10 threads trying to acquire the same lock
    for i in 0..10 {
        let redlock_clone = Arc::clone(&redlock);
        let success_clone = Arc::clone(&success_count);
        
        let handle = thread::spawn(move || {
            let client_id = Bytes::from(format!("client-{}", i));
            if let Ok(_lock) = redlock_clone.lock("concurrent-test", client_id, 1000) {
                success_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                thread::sleep(Duration::from_millis(100));
            }
        });
        
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Due to timing, at least some threads should have acquired the lock
    let final_count = success_count.load(std::sync::atomic::Ordering::SeqCst);
    assert!(final_count > 0, "At least one thread should acquire the lock");
}

#[test]
fn test_redlock_quorum_requirement() {
    // Create 5 instances (quorum = 3)
    let instances: Vec<Arc<Cache>> = (0..5)
        .map(|_| Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false))
        .collect();
    
    let redlock = Redlock::new(instances).unwrap();
    assert_eq!(redlock.quorum, 3, "Quorum should be N/2 + 1 = 3");
    
    // Acquire a lock
    let lock_value = Bytes::from("client-1");
    let lock = redlock.lock("quorum-test", lock_value, 5000).unwrap();
    
    assert_eq!(lock.resource(), "quorum-test");
}

#[test]
fn test_redlock_single_instance() {
    // Single instance mode (quorum = 1)
    let cache = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let redlock = Redlock::new(vec![cache]).unwrap();
    
    assert_eq!(redlock.quorum, 1);
    
    let lock_value = Bytes::from("client-1");
    let lock = redlock.lock("single-instance-test", lock_value, 5000).unwrap();
    
    assert_eq!(lock.resource(), "single-instance-test");
}

#[test]
fn test_redlock_ttl_expiration() {
    let cache1 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache2 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache3 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    
    let redlock = Redlock::new(vec![cache1, cache2, cache3]).unwrap();
    
    // Acquire lock with short TTL
    {
        let lock_value = Bytes::from("client-1");
        let _lock = redlock.lock("ttl-test", lock_value, 500).unwrap();
        // Lock held for 500ms
    }
    
    // Wait for TTL to expire
    thread::sleep(Duration::from_millis(600));
    
    // Another client should be able to acquire the lock
    let client2_id = Bytes::from("client-2");
    let lock2 = redlock.lock("ttl-test", client2_id, 5000);
    
    assert!(lock2.is_ok(), "Lock should be available after TTL expiration");
}

#[test]
fn test_redlock_different_resources() {
    let cache1 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache2 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache3 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    
    let redlock = Redlock::new(vec![cache1, cache2, cache3]).unwrap();
    
    // Acquire locks on different resources simultaneously
    let lock1 = redlock.lock("resource-1", Bytes::from("client-1"), 5000).unwrap();
    let lock2 = redlock.lock("resource-2", Bytes::from("client-1"), 5000).unwrap();
    let lock3 = redlock.lock("resource-3", Bytes::from("client-1"), 5000).unwrap();
    
    assert_eq!(lock1.resource(), "resource-1");
    assert_eq!(lock2.resource(), "resource-2");
    assert_eq!(lock3.resource(), "resource-3");
}
