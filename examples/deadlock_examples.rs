// Deadlock detection examples
//
// This file demonstrates how to use deadlock detection with Redlock

use kore::{Cache, Redlock, DeadlockStatus, Error};
use bytes::Bytes;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Example 1: Basic deadlock detection
fn example_basic_detection() {
    println!("=== Example 1: Basic Deadlock Detection ===");
    
    let cache1 = Cache::new(256, 100 * 1024 * 1024);
    let cache2 = Cache::new(256, 100 * 1024 * 1024);
    let cache3 = Cache::new(256, 100 * 1024 * 1024);
    
    // Enable deadlock detection with 5 second timeout, no auto-resolve
    let redlock = Redlock::new(vec![cache1, cache2, cache3])
        .unwrap()
        .with_deadlock_detection(5000, false);
    
    println!("Deadlock detection enabled");
    println!("Max wait time: 5 seconds");
    println!("Auto-resolve: disabled\n");
}

/// Example 2: Detecting a simple deadlock
fn example_simple_deadlock() {
    println!("=== Example 2: Simple Deadlock Scenario ===");
    
    let cache1 = Cache::new(256, 100 * 1024 * 1024);
    let cache2 = Cache::new(256, 100 * 1024 * 1024);
    let cache3 = Cache::new(256, 100 * 1024 * 1024);
    
    let redlock = Arc::new(
        Redlock::new(vec![cache1, cache2, cache3])
            .unwrap()
            .with_deadlock_detection(10000, false)
    );
    
    let client1 = Bytes::from("client-1");
    let client2 = Bytes::from("client-2");
    
    // Client 1 acquires lock A
    let _lock_a = redlock.lock("account-a", client1.clone(), 10000).unwrap();
    println!("Client 1: Acquired lock on account-a");
    
    // Client 2 acquires lock B
    let _lock_b = redlock.lock("account-b", client2.clone(), 10000).unwrap();
    println!("Client 2: Acquired lock on account-b");
    
    // Clone for thread
    let redlock_clone = Arc::clone(&redlock);
    let client1_clone = client1.clone();
    
    // Client 1 tries to get lock B (will wait)
    let handle = thread::spawn(move || {
        println!("Client 1: Trying to acquire lock on account-b...");
        redlock_clone.lock("account-b", client1_clone, 10000)
    });
    
    // Give client 1 time to start waiting
    thread::sleep(Duration::from_millis(100));
    
    // Client 2 tries to get lock A (creates deadlock!)
    println!("Client 2: Trying to acquire lock on account-a...");
    match redlock.lock("account-a", client2, 10000) {
        Err(Error::DeadlockDetected(msg)) => {
            println!("❌ DEADLOCK DETECTED: {}", msg);
        }
        Ok(_) => {
            println!("Unexpectedly acquired lock");
        }
        Err(e) => {
            println!("Other error: {}", e);
        }
    }
    
    let _ = handle.join();
    println!();
}

/// Example 3: Auto-resolve mode
fn example_auto_resolve() {
    println!("=== Example 3: Auto-Resolve Mode ===");
    
    let cache1 = Cache::new(256, 100 * 1024 * 1024);
    let cache2 = Cache::new(256, 100 * 1024 * 1024);
    let cache3 = Cache::new(256, 100 * 1024 * 1024);
    
    // Enable auto-resolve
    let redlock = Redlock::new(vec![cache1, cache2, cache3])
        .unwrap()
        .with_deadlock_detection(5000, true); // auto_resolve = true
    
    println!("Auto-resolve enabled");
    println!("Deadlocks will be automatically resolved by selecting a victim\n");
}

/// Example 4: Monitoring statistics
fn example_statistics() {
    println!("=== Example 4: Monitoring Statistics ===");
    
    let cache1 = Cache::new(256, 100 * 1024 * 1024);
    let cache2 = Cache::new(256, 100 * 1024 * 1024);
    let cache3 = Cache::new(256, 100 * 1024 * 1024);
    
    let redlock = Redlock::new(vec![cache1, cache2, cache3])
        .unwrap()
        .with_deadlock_detection(10000, false);
    
    // Initially no locks
    if let Some(stats) = redlock.get_deadlock_stats() {
        println!("Initial state:");
        println!("  Held locks: {}", stats.held_locks_count);
        println!("  Waiting clients: {}", stats.waiting_clients_count);
        println!("  Wait graph edges: {}", stats.wait_graph_edges);
    }
    
    // Acquire some locks
    let client1 = Bytes::from("client-1");
    let client2 = Bytes::from("client-2");
    
    let _lock1 = redlock.lock("resource-1", client1, 10000).unwrap();
    let _lock2 = redlock.lock("resource-2", client2, 10000).unwrap();
    
    if let Some(stats) = redlock.get_deadlock_stats() {
        println!("\nAfter acquiring 2 locks:");
        println!("  Held locks: {}", stats.held_locks_count);
        println!("  Waiting clients: {}", stats.waiting_clients_count);
        println!("  Wait graph edges: {}", stats.wait_graph_edges);
    }
    
    println!();
}

/// Example 5: Manual deadlock check
fn example_manual_check() {
    println!("=== Example 5: Manual Deadlock Check ===");
    
    let cache1 = Cache::new(256, 100 * 1024 * 1024);
    let cache2 = Cache::new(256, 100 * 1024 * 1024);
    let cache3 = Cache::new(256, 100 * 1024 * 1024);
    
    let redlock = Redlock::new(vec![cache1, cache2, cache3])
        .unwrap()
        .with_deadlock_detection(10000, false);
    
    let client1 = Bytes::from("client-1");
    let _lock = redlock.lock("resource-1", client1, 10000).unwrap();
    
    // Manual check
    if let Some(status) = redlock.check_deadlock() {
        match status {
            DeadlockStatus::NoDeadlock => {
                println!("✅ No deadlock detected - system healthy");
            }
            DeadlockStatus::Deadlock { cycle, resources } => {
                println!("❌ Deadlock detected:");
                println!("  Clients in cycle: {:?}", cycle);
                println!("  Resources involved: {:?}", resources);
            }
        }
    }
    
    println!();
}

/// Example 6: Deadlock-safe lock ordering
fn example_lock_ordering() {
    println!("=== Example 6: Deadlock-Safe Lock Ordering ===");
    
    let cache1 = Cache::new(256, 100 * 1024 * 1024);
    let cache2 = Cache::new(256, 100 * 1024 * 1024);
    let cache3 = Cache::new(256, 100 * 1024 * 1024);
    
    let redlock = Redlock::new(vec![cache1, cache2, cache3])
        .unwrap()
        .with_deadlock_detection(10000, false);
    
    // Safe transfer function with lock ordering
    fn transfer(
        redlock: &Redlock,
        from: &str,
        to: &str,
        client_id: Bytes,
    ) -> Result<(), Error> {
        // Always acquire locks in sorted order
        let mut accounts = vec![from, to];
        accounts.sort();
        
        println!("Acquiring locks in order: {} -> {}", accounts[0], accounts[1]);
        
        let _lock1 = redlock.lock(accounts[0], client_id.clone(), 10000)?;
        let _lock2 = redlock.lock(accounts[1], client_id, 10000)?;
        
        println!("✅ Successfully acquired both locks");
        Ok(())
    }
    
    let client1 = Bytes::from("client-1");
    let client2 = Bytes::from("client-2");
    
    // Both transfers will succeed - no deadlock possible
    println!("Transfer 1: account-a -> account-b");
    transfer(&redlock, "account-a", "account-b", client1).unwrap();
    
    println!("\nTransfer 2: account-b -> account-a");
    transfer(&redlock, "account-b", "account-a", client2).unwrap();
    
    println!("\nBoth transfers succeeded without deadlock!\n");
}

/// Example 7: Handling deadlock errors
fn example_error_handling() {
    println!("=== Example 7: Deadlock Error Handling ===");
    
    let cache1 = Cache::new(256, 100 * 1024 * 1024);
    let cache2 = Cache::new(256, 100 * 1024 * 1024);
    let cache3 = Cache::new(256, 100 * 1024 * 1024);
    
    let redlock = Redlock::new(vec![cache1, cache2, cache3])
        .unwrap()
        .with_deadlock_detection(5000, false);
    
    let client_id = Bytes::from("client-1");
    
    match redlock.lock("resource", client_id.clone(), 10000) {
        Ok(lock) => {
            println!("✅ Lock acquired successfully");
            println!("   Resource: {}", lock.resource());
            println!("   Validity: {}ms", lock.validity_time());
        }
        Err(Error::DeadlockDetected(msg)) => {
            println!("❌ Deadlock detected: {}", msg);
            println!("   Action: Retry with backoff or abort");
            
            // Exponential backoff retry strategy
            let mut backoff = Duration::from_millis(100);
            for attempt in 1..=3 {
                println!("   Retry attempt {}/3 after {:?}", attempt, backoff);
                thread::sleep(backoff);
                backoff *= 2;
                
                match redlock.lock("resource", client_id.clone(), 10000) {
                    Ok(_) => {
                        println!("   ✅ Retry successful!");
                        break;
                    }
                    Err(_) => {
                        println!("   ❌ Retry failed");
                    }
                }
            }
        }
        Err(e) => {
            println!("❌ Other error: {}", e);
        }
    }
    
    println!();
}

/// Example 8: Three-way deadlock detection
fn example_three_way_deadlock() {
    println!("=== Example 8: Three-Way Deadlock ===");
    
    let cache1 = Cache::new(256, 100 * 1024 * 1024);
    let cache2 = Cache::new(256, 100 * 1024 * 1024);
    let cache3 = Cache::new(256, 100 * 1024 * 1024);
    
    let redlock = Arc::new(
        Redlock::new(vec![cache1, cache2, cache3])
            .unwrap()
            .with_deadlock_detection(10000, false)
    );
    
    println!("Creating a three-way circular dependency:");
    println!("  Client 1: A -> waits for B");
    println!("  Client 2: B -> waits for C");
    println!("  Client 3: C -> waits for A");
    
    // This would create a deadlock in real scenario
    println!("\nDeadlock detection will catch this circular dependency\n");
}

fn main() {
    println!("\n🔒 Deadlock Detection Examples for Kore\n");
    
    example_basic_detection();
    example_simple_deadlock();
    example_auto_resolve();
    example_statistics();
    example_manual_check();
    example_lock_ordering();
    example_error_handling();
    example_three_way_deadlock();
    
    println!("✅ All examples completed!");
    println!("\n💡 Key Takeaways:");
    println!("   1. Enable deadlock detection for critical systems");
    println!("   2. Use lock ordering to prevent deadlocks");
    println!("   3. Monitor statistics regularly");
    println!("   4. Handle deadlock errors gracefully with retry logic");
    println!("   5. Consider auto-resolve for automatic recovery\n");
}
