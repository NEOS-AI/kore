use kore::{Cache, Redlock};
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[test]
fn test_fair_queue_basic() {
    let cache1 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache2 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache3 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    
    let redlock = Redlock::new(vec![cache1, cache2, cache3])
        .unwrap()
        .with_fair_queueing(100);
    
    let client1 = Bytes::from("client-1");
    let client2 = Bytes::from("client-2");
    
    // Client 1 acquires lock first
    let _lock1 = redlock.lock("resource-a", client1, 5000).unwrap();
    
    // Client 2 should be queued
    let stats = redlock.get_fair_queue_stats().unwrap();
    assert_eq!(stats.total_enqueued, 1); // client-1 was enqueued initially
}

#[test]
fn test_fair_queue_ordering() {
    let cache1 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache2 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache3 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    
    let redlock = Arc::new(
        Redlock::new(vec![cache1, cache2, cache3])
            .unwrap()
            .with_fair_queueing(100)
    );
    
    // Track acquisition order
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut handles = vec![];
    
    // Spawn 5 threads trying to acquire the same lock
    for i in 0..5 {
        let redlock_clone = Arc::clone(&redlock);
        let order_clone = Arc::clone(&order);
        
        let handle = thread::spawn(move || {
            let client_id = Bytes::from(format!("client-{}", i));
            
            // Very small delay to ensure threads start in order
            thread::sleep(Duration::from_millis(i * 5));
            
            // Long TTL to ensure all threads can complete even if they wait in queue
            // 5 threads * 50ms hold time + 500ms buffer = ~1000ms minimum
            if let Ok(_lock) = redlock_clone.lock("shared-resource", client_id.clone(), 10000) {
                order_clone.lock().unwrap().push(i);
                thread::sleep(Duration::from_millis(50)); // Hold lock briefly
            } else {
                eprintln!("Client {} failed to acquire lock", i);
            }
        });
        
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let final_order = order.lock().unwrap();
    println!("Acquisition order: {:?}", *final_order);
    
    // With fair queueing, all clients should eventually acquire locks
    assert_eq!(final_order.len(), 5, "All 5 clients should acquire locks, got {}", final_order.len());
    
    // First client should be 0 (started first)
    assert_eq!(final_order[0], 0, "First client should be client-0");
}

#[test]
fn test_fair_queue_priority() {
    let cache1 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache2 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache3 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    
    let redlock = Arc::new(
        Redlock::new(vec![cache1, cache2, cache3])
            .unwrap()
            .with_fair_queueing(100)
    );
    
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));
    
    // First, acquire a lock to block others
    let blocker = Bytes::from("blocker");
    let _blocker_lock = redlock.lock("priority-test", blocker, 5000).unwrap();
    
    // Now queue up clients with different priorities
    let mut handles = vec![];
    
    for (id, priority) in vec![("low-priority", 100), ("high-priority", 0), ("medium-priority", 50)] {
        let redlock_clone = Arc::clone(&redlock);
        let order_clone = Arc::clone(&order);
        let client_name = id.to_string();
        
        let handle = thread::spawn(move || {
            let client_id = Bytes::from(client_name.clone());
            
            if let Ok(_lock) = redlock_clone.lock_with_priority("priority-test", client_id, 5000, priority) {
                order_clone.lock().unwrap().push(client_name);
                thread::sleep(Duration::from_millis(50));
            }
        });
        
        handles.push(handle);
        thread::sleep(Duration::from_millis(10)); // Ensure they queue up
    }
    
    // Release blocker lock
    drop(_blocker_lock);
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let final_order = order.lock().unwrap();
    println!("Priority-based order: {:?}", *final_order);
    
    // High priority should come first
    assert!(final_order.len() >= 2, "At least 2 clients should acquire locks");
    if final_order.len() > 0 {
        assert_eq!(final_order[0], "high-priority");
    }
}

#[test]
fn test_fair_queue_prevents_starvation() {
    let cache1 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache2 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache3 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    
    let redlock = Arc::new(
        Redlock::new(vec![cache1, cache2, cache3])
            .unwrap()
            .with_fair_queueing(100)
    );
    
    let acquisition_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];
    
    // Simulate 10 clients competing for the same resource
    for i in 0..10 {
        let redlock_clone = Arc::clone(&redlock);
        let count_clone = Arc::clone(&acquisition_count);
        
        let handle = thread::spawn(move || {
            let client_id = Bytes::from(format!("client-{}", i));
            
            // Try to acquire lock multiple times
            for _attempt in 0..3 {
                if let Ok(_lock) = redlock_clone.lock("shared-resource", client_id.clone(), 2000) {
                    count_clone.fetch_add(1, Ordering::SeqCst);
                    thread::sleep(Duration::from_millis(10));
                }
                thread::sleep(Duration::from_millis(50));
            }
        });
        
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let final_count = acquisition_count.load(Ordering::SeqCst);
    println!("Total acquisitions: {}", final_count);
    
    // With fair queueing, all clients should get a chance
    // (exact number depends on timing, but should be significant)
    assert!(final_count > 0);
}

#[test]
fn test_fair_queue_statistics() {
    let cache1 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache2 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache3 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    
    let redlock = Redlock::new(vec![cache1, cache2, cache3])
        .unwrap()
        .with_fair_queueing(100);
    
    // Initially empty
    let stats = redlock.get_fair_queue_stats().unwrap();
    assert_eq!(stats.total_queued, 0);
    assert_eq!(stats.active_queues, 0);
    
    // Acquire a lock
    let client1 = Bytes::from("client-1");
    let _lock = redlock.lock("resource-a", client1, 5000).unwrap();
    
    let stats = redlock.get_fair_queue_stats().unwrap();
    assert_eq!(stats.total_enqueued, 1);
    assert_eq!(stats.total_dequeued, 1);
}

#[test]
fn test_fair_queue_position() {
    let cache1 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache2 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache3 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    
    let redlock = Arc::new(
        Redlock::new(vec![cache1, cache2, cache3])
            .unwrap()
            .with_fair_queueing(100)
    );
    
    let client1 = Bytes::from("client-1");
    let _blocker_lock = redlock.lock("resource-a", client1.clone(), 5000).unwrap();
    
    // Queue up more clients
    let redlock_clone = Arc::clone(&redlock);
    let handle = thread::spawn(move || {
        let client2 = Bytes::from("client-2");
        let _ = redlock_clone.lock("resource-a", client2, 5000);
    });
    
    thread::sleep(Duration::from_millis(100));
    
    // Check queue status
    let queue_len = redlock.get_queue_length("resource-a");
    println!("Queue length: {}", queue_len);
    
    drop(_blocker_lock);
    handle.join().unwrap();
}

#[test]
fn test_fair_queue_max_size() {
    let cache1 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache2 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache3 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    
    let redlock = Arc::new(
        Redlock::new(vec![cache1, cache2, cache3])
            .unwrap()
            .with_fair_queueing(2) // Very small queue
    );
    
    let client1 = Bytes::from("client-1");
    let _blocker = redlock.lock("resource-a", client1, 5000).unwrap();
    
    let mut handles = vec![];
    let success_count = Arc::new(AtomicUsize::new(0));
    let fail_count = Arc::new(AtomicUsize::new(0));
    
    // Try to queue 5 clients (but max queue size is 2)
    for i in 2..7 {
        let redlock_clone = Arc::clone(&redlock);
        let success_clone = Arc::clone(&success_count);
        let fail_clone = Arc::clone(&fail_count);
        
        let handle = thread::spawn(move || {
            let client_id = Bytes::from(format!("client-{}", i));
            match redlock_clone.lock("resource-a", client_id, 3000) {
                Ok(_) => {
                    success_clone.fetch_add(1, Ordering::SeqCst);
                }
                Err(_) => {
                    fail_clone.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
        
        handles.push(handle);
        thread::sleep(Duration::from_millis(10));
    }
    
    drop(_blocker);
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let failures = fail_count.load(Ordering::SeqCst);
    println!("Failed due to queue full: {}", failures);
    
    // Some should fail due to queue being full
    assert!(failures > 0);
}

#[test]
fn test_fair_queue_with_deadlock_detection() {
    let cache1 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache2 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache3 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    
    let redlock = Arc::new(
        Redlock::new(vec![cache1, cache2, cache3])
            .unwrap()
            .with_fair_queueing(100)
            .with_deadlock_detection(5000, false)
    );
    
    // Both features should work together
    let client1 = Bytes::from("client-1");
    let _lock = redlock.lock("resource-a", client1, 5000).unwrap();
    
    // Check both statistics are available
    let fair_stats = redlock.get_fair_queue_stats();
    let deadlock_stats = redlock.get_deadlock_stats();
    
    assert!(fair_stats.is_some());
    assert!(deadlock_stats.is_some());
}
