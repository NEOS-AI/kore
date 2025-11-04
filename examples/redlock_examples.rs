// Redlock usage examples
//
// This file demonstrates various ways to use Redlock in Kore

use kore::{Cache, Redlock};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use std::thread;

/// Example 1: Basic lock acquisition and release
fn example_basic_lock() {
    println!("=== Example 1: Basic Lock ===");
    
    let cache1 = Cache::new(256, 100 * 1024 * 1024);
    let cache2 = Cache::new(256, 100 * 1024 * 1024);
    let cache3 = Cache::new(256, 100 * 1024 * 1024);
    
    let redlock = Redlock::new(vec![cache1, cache2, cache3]).unwrap();
    
    // Acquire a lock
    let lock = redlock.lock(
        "user:123:profile",
        Bytes::from("update-request-xyz"),
        5000 // 5 seconds TTL
    ).unwrap();
    
    println!("Lock acquired for resource: {}", lock.resource());
    println!("Lock value: {:?}", lock.value());
    println!("Validity time: {}ms", lock.validity_time());
    
    // Perform critical section work
    println!("Updating user profile...");
    thread::sleep(Duration::from_millis(100));
    
    // Lock is automatically released when it goes out of scope
    println!("Lock will be released automatically\n");
}

/// Example 2: Manual lock release
fn example_manual_release() {
    println!("=== Example 2: Manual Release ===");
    
    let cache1 = Cache::new(256, 100 * 1024 * 1024);
    let cache2 = Cache::new(256, 100 * 1024 * 1024);
    let cache3 = Cache::new(256, 100 * 1024 * 1024);
    
    let redlock = Redlock::new(vec![cache1, cache2, cache3]).unwrap();
    
    let lock = redlock.lock("inventory:item:42", Bytes::from("reserve-xyz"), 10000).unwrap();
    
    println!("Lock acquired, processing order...");
    thread::sleep(Duration::from_millis(200));
    
    // Manually release the lock
    redlock.unlock(&lock).unwrap();
    println!("Lock manually released\n");
}

/// Example 3: Lock extension for long operations
fn example_lock_extension() {
    println!("=== Example 3: Lock Extension ===");
    
    let cache1 = Cache::new(256, 100 * 1024 * 1024);
    let cache2 = Cache::new(256, 100 * 1024 * 1024);
    let cache3 = Cache::new(256, 100 * 1024 * 1024);
    
    let redlock = Redlock::new(vec![cache1, cache2, cache3]).unwrap();
    
    let lock = redlock.lock("batch:processing", Bytes::from("job-123"), 2000).unwrap();
    
    println!("Lock acquired with 2s TTL");
    println!("Starting batch processing...");
    
    thread::sleep(Duration::from_millis(1500));
    
    // Need more time, extend the lock
    lock.extend(3000).unwrap();
    println!("Lock extended by 3 more seconds");
    
    thread::sleep(Duration::from_millis(1000));
    println!("Batch processing complete\n");
}

/// Example 4: Handling lock acquisition failures
fn example_lock_failure_handling() {
    println!("=== Example 4: Lock Failure Handling ===");
    
    let cache1 = Cache::new(256, 100 * 1024 * 1024);
    let cache2 = Cache::new(256, 100 * 1024 * 1024);
    let cache3 = Cache::new(256, 100 * 1024 * 1024);
    
    let redlock = Redlock::new(vec![cache1, cache2, cache3]).unwrap();
    
    // First client acquires lock
    let _lock1 = redlock.lock("shared:counter", Bytes::from("client-1"), 5000).unwrap();
    println!("Client 1 acquired lock");
    
    // Second client tries to acquire the same lock
    match redlock.lock("shared:counter", Bytes::from("client-2"), 5000) {
        Ok(_) => println!("Client 2 acquired lock (unexpected!)"),
        Err(e) => println!("Client 2 failed to acquire lock: {}", e),
    }
    
    println!();
}

/// Example 5: Concurrent access pattern
fn example_concurrent_access() {
    println!("=== Example 5: Concurrent Access ===");
    
    let cache1 = Cache::new(256, 100 * 1024 * 1024);
    let cache2 = Cache::new(256, 100 * 1024 * 1024);
    let cache3 = Cache::new(256, 100 * 1024 * 1024);
    
    let redlock = Arc::new(Redlock::new(vec![cache1, cache2, cache3]).unwrap());
    
    let mut handles = vec![];
    
    // Spawn 5 workers competing for the same resource
    for i in 0..5 {
        let redlock_clone = Arc::clone(&redlock);
        
        let handle = thread::spawn(move || {
            let client_id = Bytes::from(format!("worker-{}", i));
            
            match redlock_clone.lock("database:backup", client_id.clone(), 2000) {
                Ok(lock) => {
                    println!("Worker {} acquired lock", i);
                    thread::sleep(Duration::from_millis(500));
                    println!("Worker {} releasing lock", i);
                    drop(lock);
                }
                Err(e) => {
                    println!("Worker {} failed: {}", i, e);
                }
            }
        });
        
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    println!();
}

/// Example 6: Custom Redlock configuration
fn example_custom_config() {
    println!("=== Example 6: Custom Configuration ===");
    
    let cache1 = Cache::new(256, 100 * 1024 * 1024);
    let cache2 = Cache::new(256, 100 * 1024 * 1024);
    let cache3 = Cache::new(256, 100 * 1024 * 1024);
    
    // Create Redlock with custom retry settings
    let redlock = Redlock::with_config(
        vec![cache1, cache2, cache3],
        5,      // retry_count: Try 5 times
        100,    // retry_delay_ms: Wait 100ms between retries
        0.02    // clock_drift_factor: 2% drift allowance
    ).unwrap();
    
    println!("Created Redlock with custom config:");
    println!("  - Retry count: 5");
    println!("  - Retry delay: 100ms");
    println!("  - Clock drift factor: 2%");
    println!("  - Quorum: {}", redlock.quorum);
    
    let lock = redlock.lock("critical:section", Bytes::from("config-test"), 5000).unwrap();
    println!("Lock acquired successfully\n");
    drop(lock);
}

/// Example 7: Multiple independent locks
fn example_multiple_locks() {
    println!("=== Example 7: Multiple Independent Locks ===");
    
    let cache1 = Cache::new(256, 100 * 1024 * 1024);
    let cache2 = Cache::new(256, 100 * 1024 * 1024);
    let cache3 = Cache::new(256, 100 * 1024 * 1024);
    
    let redlock = Redlock::new(vec![cache1, cache2, cache3]).unwrap();
    
    // Acquire locks on different resources simultaneously
    let lock1 = redlock.lock("user:1:profile", Bytes::from("edit-1"), 5000).unwrap();
    let lock2 = redlock.lock("user:2:profile", Bytes::from("edit-2"), 5000).unwrap();
    let lock3 = redlock.lock("user:3:profile", Bytes::from("edit-3"), 5000).unwrap();
    
    println!("Acquired 3 independent locks:");
    println!("  - {}", lock1.resource());
    println!("  - {}", lock2.resource());
    println!("  - {}", lock3.resource());
    
    println!("All locks can be held simultaneously on different resources\n");
}

/// Example 8: Scalability with 5 instances
fn example_five_instances() {
    println!("=== Example 8: Five Instances (Quorum=3) ===");
    
    let instances: Vec<Arc<Cache>> = (0..5)
        .map(|_| Cache::new(256, 100 * 1024 * 1024))
        .collect();
    
    let redlock = Redlock::new(instances).unwrap();
    
    println!("Created Redlock with 5 instances");
    println!("Quorum requirement: {} out of 5", redlock.quorum);
    
    let lock = redlock.lock("distributed:task", Bytes::from("executor-1"), 5000).unwrap();
    println!("Lock acquired across majority of instances");
    println!("Can tolerate up to 2 instance failures\n");
    drop(lock);
}

fn main() {
    println!("\n🔒 Redlock Examples for Kore\n");
    
    example_basic_lock();
    example_manual_release();
    example_lock_extension();
    example_lock_failure_handling();
    example_concurrent_access();
    example_custom_config();
    example_multiple_locks();
    example_five_instances();
    
    println!("✅ All examples completed!");
}
