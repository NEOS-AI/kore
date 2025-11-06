use kore::{Cache, Redlock};
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

/// Example 1: Basic fair queueing
fn example_basic_fair_queue() {
    println!("\n=== Example 1: Basic Fair Queueing ===");
    
    let cache1 = Cache::new(256, 100 * 1024 * 1024);
    let cache2 = Cache::new(256, 100 * 1024 * 1024);
    let cache3 = Cache::new(256, 100 * 1024 * 1024);
    
    // Enable fair queueing with max queue size of 100
    let redlock = Redlock::new(vec![cache1, cache2, cache3])
        .unwrap()
        .with_fair_queueing(100);
    
    // Clients will be served in FIFO order
    let client_id = Bytes::from("client-1");
    match redlock.lock("shared-resource", client_id, 5000) {
        Ok(lock) => {
            println!("Lock acquired for resource: {}", lock.resource());
            // Do work...
            thread::sleep(Duration::from_millis(100));
            println!("Releasing lock");
        }
        Err(e) => {
            eprintln!("Failed to acquire lock: {}", e);
        }
    }
}

/// Example 2: Multiple clients competing fairly
fn example_fair_competition() {
    println!("\n=== Example 2: Fair Competition ===");
    
    let cache1 = Cache::new(256, 100 * 1024 * 1024);
    let cache2 = Cache::new(256, 100 * 1024 * 1024);
    let cache3 = Cache::new(256, 100 * 1024 * 1024);
    
    let redlock = Arc::new(
        Redlock::new(vec![cache1, cache2, cache3])
            .unwrap()
            .with_fair_queueing(50)
    );
    
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut handles = vec![];
    
    // Spawn 5 clients
    for i in 1..=5 {
        let redlock_clone = Arc::clone(&redlock);
        let order_clone = Arc::clone(&order);
        
        let handle = thread::spawn(move || {
            let client_id = Bytes::from(format!("client-{}", i));
            
            println!("Client {} requesting lock...", i);
            
            match redlock_clone.lock("fair-resource", client_id, 3000) {
                Ok(_lock) => {
                    println!("Client {} acquired lock", i);
                    order_clone.lock().unwrap().push(i);
                    thread::sleep(Duration::from_millis(200));
                    println!("Client {} releasing lock", i);
                }
                Err(e) => {
                    eprintln!("Client {} failed: {}", i, e);
                }
            }
        });
        
        handles.push(handle);
        thread::sleep(Duration::from_millis(50)); // Stagger requests
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let final_order = order.lock().unwrap();
    println!("Acquisition order: {:?}", *final_order);
}

/// Example 3: Priority-based queueing
fn example_priority_queue() {
    println!("\n=== Example 3: Priority-based Queueing ===");
    
    let cache1 = Cache::new(256, 100 * 1024 * 1024);
    let cache2 = Cache::new(256, 100 * 1024 * 1024);
    let cache3 = Cache::new(256, 100 * 1024 * 1024);
    
    let redlock = Arc::new(
        Redlock::new(vec![cache1, cache2, cache3])
            .unwrap()
            .with_fair_queueing(100)
    );
    
    // First acquire a lock to create queue
    let blocker_id = Bytes::from("blocker");
    let blocker_lock = redlock.lock("priority-resource", blocker_id, 5000).unwrap();
    println!("Blocker acquired lock, others will queue up...");
    
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut handles = vec![];
    
    // Queue clients with different priorities
    let clients = vec![
        ("normal-1", 10),      // Normal priority
        ("critical", 0),       // Critical priority (highest)
        ("normal-2", 10),      // Normal priority
        ("high", 5),           // High priority
    ];
    
    for (name, priority) in clients {
        let redlock_clone = Arc::clone(&redlock);
        let order_clone = Arc::clone(&order);
        let client_name = name.to_string();
        
        let handle = thread::spawn(move || {
            let client_id = Bytes::from(client_name.clone());
            
            println!("{} (priority {}) waiting...", client_name, priority);
            
            match redlock_clone.lock_with_priority("priority-resource", client_id, 3000, priority) {
                Ok(_lock) => {
                    println!("{} acquired lock!", client_name);
                    order_clone.lock().unwrap().push(client_name);
                    thread::sleep(Duration::from_millis(100));
                }
                Err(e) => {
                    eprintln!("{} failed: {}", client_name, e);
                }
            }
        });
        
        handles.push(handle);
        thread::sleep(Duration::from_millis(50));
    }
    
    // Release blocker after all clients have queued
    thread::sleep(Duration::from_millis(200));
    println!("Releasing blocker lock...");
    drop(blocker_lock);
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let final_order = order.lock().unwrap();
    println!("\nAcquisition order (by priority): {:?}", *final_order);
    println!("Expected: critical -> high -> normal-1 -> normal-2");
}

/// Example 4: Monitoring queue statistics
fn example_queue_statistics() {
    println!("\n=== Example 4: Queue Statistics ===");
    
    let cache1 = Cache::new(256, 100 * 1024 * 1024);
    let cache2 = Cache::new(256, 100 * 1024 * 1024);
    let cache3 = Cache::new(256, 100 * 1024 * 1024);
    
    let redlock = Arc::new(
        Redlock::new(vec![cache1, cache2, cache3])
            .unwrap()
            .with_fair_queueing(100)
    );
    
    // Monitor thread
    let redlock_monitor = Arc::clone(&redlock);
    let monitor_handle = thread::spawn(move || {
        for _ in 0..20 {
            if let Some(stats) = redlock_monitor.get_fair_queue_stats() {
                println!("\n[STATS] Queued: {}, Enqueued: {}, Dequeued: {}, Avg wait: {}ms",
                    stats.total_queued,
                    stats.total_enqueued,
                    stats.total_dequeued,
                    stats.avg_wait_time
                );
            }
            thread::sleep(Duration::from_millis(200));
        }
    });
    
    // Simulate client activity
    let mut handles = vec![];
    for i in 1..=10 {
        let redlock_clone = Arc::clone(&redlock);
        
        let handle = thread::spawn(move || {
            let client_id = Bytes::from(format!("client-{}", i));
            
            if let Ok(_lock) = redlock_clone.lock("monitored-resource", client_id, 2000) {
                thread::sleep(Duration::from_millis(150));
            }
        });
        
        handles.push(handle);
        thread::sleep(Duration::from_millis(100));
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    monitor_handle.join().unwrap();
    
    // Final statistics
    if let Some(stats) = redlock.get_fair_queue_stats() {
        println!("\n=== Final Statistics ===");
        println!("Total enqueued: {}", stats.total_enqueued);
        println!("Total dequeued: {}", stats.total_dequeued);
        println!("Max wait time: {}ms", stats.max_wait_time);
        println!("Avg wait time: {}ms", stats.avg_wait_time);
    }
}

/// Example 5: Queue position tracking
fn example_queue_position() {
    println!("\n=== Example 5: Queue Position Tracking ===");
    
    let cache1 = Cache::new(256, 100 * 1024 * 1024);
    let cache2 = Cache::new(256, 100 * 1024 * 1024);
    let cache3 = Cache::new(256, 100 * 1024 * 1024);
    
    let redlock = Arc::new(
        Redlock::new(vec![cache1, cache2, cache3])
            .unwrap()
            .with_fair_queueing(50)
    );
    
    // Acquire blocker lock
    let blocker_id = Bytes::from("blocker");
    let blocker_lock = redlock.lock("tracked-resource", blocker_id, 5000).unwrap();
    
    let mut handles = vec![];
    
    // Queue up several clients
    for i in 1..=5 {
        let redlock_clone = Arc::clone(&redlock);
        
        let handle = thread::spawn(move || {
            let client_id = Bytes::from(format!("client-{}", i));
            
            // Try to acquire lock (will be queued)
            let _ = redlock_clone.lock("tracked-resource", client_id.clone(), 3000);
        });
        
        handles.push(handle);
        thread::sleep(Duration::from_millis(100));
    }
    
    // Check queue state
    thread::sleep(Duration::from_millis(200));
    
    println!("Queue length: {}", redlock.get_queue_length("tracked-resource"));
    
    for i in 1..=5 {
        let client_id = Bytes::from(format!("client-{}", i));
        if let Some(pos) = redlock.get_queue_position("tracked-resource", &client_id) {
            println!("Client {} is at position {}", i, pos);
        }
    }
    
    // Release blocker
    println!("\nReleasing blocker...");
    drop(blocker_lock);
    
    for handle in handles {
        handle.join().unwrap();
    }
}

/// Example 6: Handling queue full errors
fn example_queue_full() {
    println!("\n=== Example 6: Handling Queue Full ===");
    
    let cache1 = Cache::new(256, 100 * 1024 * 1024);
    let cache2 = Cache::new(256, 100 * 1024 * 1024);
    let cache3 = Cache::new(256, 100 * 1024 * 1024);
    
    let redlock = Arc::new(
        Redlock::new(vec![cache1, cache2, cache3])
            .unwrap()
            .with_fair_queueing(3)  // Very small queue
    );
    
    // Acquire blocker
    let blocker_id = Bytes::from("blocker");
    let _blocker = redlock.lock("limited-resource", blocker_id, 5000).unwrap();
    
    let success_count = Arc::new(AtomicUsize::new(0));
    let reject_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];
    
    // Try to queue many clients
    for i in 1..=10 {
        let redlock_clone = Arc::clone(&redlock);
        let success = Arc::clone(&success_count);
        let reject = Arc::clone(&reject_count);
        
        let handle = thread::spawn(move || {
            let client_id = Bytes::from(format!("client-{}", i));
            
            match redlock_clone.lock("limited-resource", client_id, 2000) {
                Ok(_lock) => {
                    println!("Client {} acquired lock", i);
                    success.fetch_add(1, Ordering::SeqCst);
                }
                Err(e) => {
                    println!("Client {} rejected: {}", i, e);
                    reject.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
        
        handles.push(handle);
        thread::sleep(Duration::from_millis(50));
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    println!("\nSuccessful: {}", success_count.load(Ordering::SeqCst));
    println!("Rejected (queue full): {}", reject_count.load(Ordering::SeqCst));
}

/// Example 7: Fair queueing with deadlock detection
fn example_with_deadlock_detection() {
    println!("\n=== Example 7: Fair Queue + Deadlock Detection ===");
    
    let cache1 = Cache::new(256, 100 * 1024 * 1024);
    let cache2 = Cache::new(256, 100 * 1024 * 1024);
    let cache3 = Cache::new(256, 100 * 1024 * 1024);
    
    let redlock = Arc::new(
        Redlock::new(vec![cache1, cache2, cache3])
            .unwrap()
            .with_fair_queueing(100)
            .with_deadlock_detection(5000, false)
    );
    
    println!("Redlock configured with both fair queueing and deadlock detection");
    
    // Acquire lock
    let client_id = Bytes::from("safe-client");
    match redlock.lock("protected-resource", client_id, 3000) {
        Ok(_lock) => {
            println!("Lock acquired safely");
            
            // Check both statistics
            if let Some(fair_stats) = redlock.get_fair_queue_stats() {
                println!("Fair queue stats: {} enqueued", fair_stats.total_enqueued);
            }
            
            if let Some(deadlock_stats) = redlock.get_deadlock_stats() {
                println!("Deadlock stats: {} locks held", deadlock_stats.held_locks_count);
            }
        }
        Err(e) => {
            eprintln!("Failed: {}", e);
        }
    }
}

/// Example 8: Real-world request handler
fn example_request_handler() {
    println!("\n=== Example 8: Request Handler with Fair Queue ===");
    
    struct RequestHandler {
        redlock: Arc<Redlock>,
    }
    
    impl RequestHandler {
        fn new() -> Self {
            let cache1 = Cache::new(256, 100 * 1024 * 1024);
            let cache2 = Cache::new(256, 100 * 1024 * 1024);
            let cache3 = Cache::new(256, 100 * 1024 * 1024);
            
            let redlock = Redlock::new(vec![cache1, cache2, cache3])
                .unwrap()
                .with_fair_queueing(200)
                .with_deadlock_detection(10000, false);
            
            Self {
                redlock: Arc::new(redlock),
            }
        }
        
        fn handle_request(&self, user_id: &str, is_premium: bool) -> Result<(), String> {
            // Premium users get higher priority
            let priority = if is_premium { 0 } else { 10 };
            let client_id = Bytes::from(user_id.to_string());
            
            println!("Handling request for {} (premium: {})", user_id, is_premium);
            
            match self.redlock.lock_with_priority("user-data", client_id, 5000, priority) {
                Ok(_lock) => {
                    println!("{} acquired lock, processing...", user_id);
                    thread::sleep(Duration::from_millis(100));
                    Ok(())
                }
                Err(e) => {
                    Err(format!("Failed to acquire lock: {}", e))
                }
            }
        }
        
        fn print_stats(&self) {
            if let Some(stats) = self.redlock.get_fair_queue_stats() {
                println!("\n[Stats] Waiting: {}, Avg wait: {}ms",
                    stats.total_queued,
                    stats.avg_wait_time
                );
            }
        }
    }
    
    let handler = Arc::new(RequestHandler::new());
    let mut handles = vec![];
    
    // Simulate requests
    let users = vec![
        ("user1", false),
        ("user2", true),   // Premium
        ("user3", false),
        ("user4", true),   // Premium
        ("user5", false),
    ];
    
    for (user_id, is_premium) in users {
        let handler_clone = Arc::clone(&handler);
        let user = user_id.to_string();
        
        let handle = thread::spawn(move || {
            let _ = handler_clone.handle_request(&user, is_premium);
        });
        
        handles.push(handle);
        thread::sleep(Duration::from_millis(50));
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    handler.print_stats();
}

fn main() {
    println!("Kore Fair Lock Queueing Examples");
    println!("==================================");
    
    example_basic_fair_queue();
    example_fair_competition();
    example_priority_queue();
    example_queue_statistics();
    example_queue_position();
    example_queue_full();
    example_with_deadlock_detection();
    example_request_handler();
    
    println!("\n=== All examples completed ===");
}
