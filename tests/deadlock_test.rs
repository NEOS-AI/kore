use kore::{Cache, Redlock, DeadlockDetector, DeadlockStatus};
use bytes::Bytes;
use std::thread;
use std::time::Duration;

#[test]
fn test_deadlock_detection_simple() {
    let cache1 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache2 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache3 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    
    let redlock = Redlock::new(vec![cache1, cache2, cache3])
        .unwrap()
        .with_deadlock_detection(5000, false);
    
    let client1 = Bytes::from("client-1");
    let client2 = Bytes::from("client-2");
    
    // Client 1 acquires resource A
    let _lock_a = redlock.lock("resource-a", client1.clone(), 10000).unwrap();
    
    // Client 2 acquires resource B
    let _lock_b = redlock.lock("resource-b", client2.clone(), 10000).unwrap();
    
    // Clone redlock for thread
    let redlock_clone = redlock.clone();
    
    // Client 1 tries to acquire resource B (will wait)
    let handle1 = thread::spawn(move || {
        let result = redlock_clone.lock("resource-b", client1, 10000);
        result
    });
    
    // Give client 1 time to start waiting
    thread::sleep(Duration::from_millis(100));
    
    // Client 2 tries to acquire resource A (creates deadlock)
    let result = redlock.lock("resource-a", client2, 10000);
    
    // Should detect deadlock
    assert!(result.is_err());
    
    // Clean up
    let _ = handle1.join();
}

#[test]
fn test_deadlock_stats() {
    let cache1 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache2 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache3 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    
    let redlock = Redlock::new(vec![cache1, cache2, cache3])
        .unwrap()
        .with_deadlock_detection(5000, false);
    
    // Initially no locks
    let stats = redlock.get_deadlock_stats().unwrap();
    assert_eq!(stats.held_locks_count, 0);
    assert_eq!(stats.waiting_clients_count, 0);
    
    // Acquire a lock
    let client1 = Bytes::from("client-1");
    let _lock = redlock.lock("resource-1", client1, 10000).unwrap();
    
    // Check stats
    let stats = redlock.get_deadlock_stats().unwrap();
    assert_eq!(stats.held_locks_count, 1);
}

#[test]
fn test_no_deadlock_sequential_locks() {
    let cache1 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache2 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache3 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    
    let redlock = Redlock::new(vec![cache1, cache2, cache3])
        .unwrap()
        .with_deadlock_detection(5000, false);
    
    let client1 = Bytes::from("client-1");
    
    // Client acquires multiple locks sequentially (no deadlock)
    let _lock_a = redlock.lock("resource-a", client1.clone(), 10000).unwrap();
    let _lock_b = redlock.lock("resource-b", client1.clone(), 10000).unwrap();
    let _lock_c = redlock.lock("resource-c", client1, 10000).unwrap();
    
    // Check no deadlock
    match redlock.check_deadlock() {
        Some(DeadlockStatus::NoDeadlock) => {
            // Expected
        }
        Some(DeadlockStatus::Deadlock { .. }) => {
            panic!("Should not detect deadlock");
        }
        None => {
            panic!("Deadlock detector should be enabled");
        }
    }
}

#[test]
fn test_deadlock_detector_standalone() {
    let detector = DeadlockDetector::new(5000, false);
    
    let client1 = Bytes::from("client-1");
    let client2 = Bytes::from("client-2");
    
    // Client 1 holds resource A
    detector.record_lock_acquired("resource-a".to_string(), client1.clone(), 10000);
    
    // Client 2 holds resource B
    detector.record_lock_acquired("resource-b".to_string(), client2.clone(), 10000);
    
    // Client 1 waits for resource B
    detector.record_lock_wait("resource-b".to_string(), client1.clone(), 10000);
    
    // Client 2 waits for resource A (creates cycle)
    detector.record_lock_wait("resource-a".to_string(), client2.clone(), 10000);
    
    // Detect deadlock
    match detector.detect_deadlock() {
        DeadlockStatus::Deadlock { cycle, resources } => {
            println!("Detected deadlock:");
            println!("  Cycle: {:?}", cycle);
            println!("  Resources: {:?}", resources);
            assert!(cycle.len() >= 2);
        }
        DeadlockStatus::NoDeadlock => {
            panic!("Expected deadlock to be detected");
        }
    }
}

#[test]
fn test_deadlock_release_breaks_cycle() {
    let detector = DeadlockDetector::new(5000, false);
    
    let client1 = Bytes::from("client-1");
    let client2 = Bytes::from("client-2");
    
    // Create deadlock situation
    detector.record_lock_acquired("resource-a".to_string(), client1.clone(), 10000);
    detector.record_lock_acquired("resource-b".to_string(), client2.clone(), 10000);
    detector.record_lock_wait("resource-b".to_string(), client1, 10000);
    detector.record_lock_wait("resource-a".to_string(), client2.clone(), 10000);
    
    // Verify deadlock exists
    assert!(matches!(detector.detect_deadlock(), DeadlockStatus::Deadlock { .. }));
    
    // Release one lock
    detector.record_lock_released("resource-a");
    
    // Deadlock should be resolved
    assert!(matches!(detector.detect_deadlock(), DeadlockStatus::NoDeadlock));
}

#[test]
fn test_three_way_deadlock() {
    let detector = DeadlockDetector::new(5000, false);
    
    let client1 = Bytes::from("client-1");
    let client2 = Bytes::from("client-2");
    let client3 = Bytes::from("client-3");
    
    // Create 3-way circular dependency
    // Client 1 holds A, waits for B
    detector.record_lock_acquired("resource-a".to_string(), client1.clone(), 10000);
    detector.record_lock_acquired("resource-b".to_string(), client2.clone(), 10000);
    detector.record_lock_acquired("resource-c".to_string(), client3.clone(), 10000);
    
    detector.record_lock_wait("resource-b".to_string(), client1, 10000);
    detector.record_lock_wait("resource-c".to_string(), client2, 10000);
    detector.record_lock_wait("resource-a".to_string(), client3, 10000);
    
    // Should detect the cycle
    match detector.detect_deadlock() {
        DeadlockStatus::Deadlock { cycle, .. } => {
            assert!(cycle.len() >= 3, "Expected 3-way deadlock cycle");
        }
        DeadlockStatus::NoDeadlock => {
            panic!("Expected 3-way deadlock to be detected");
        }
    }
}

#[test]
fn test_lock_expiration_cleanup() {
    let detector = DeadlockDetector::new(100, false); // Very short max wait
    
    let client1 = Bytes::from("client-1");
    
    detector.record_lock_acquired("resource-a".to_string(), client1.clone(), 50);
    
    // Wait for lock to expire
    thread::sleep(Duration::from_millis(60));
    
    // Trigger cleanup by detecting deadlock
    detector.detect_deadlock();
    
    // Lock should be cleaned up
    let held = detector.get_held_locks();
    assert_eq!(held.len(), 0, "Expired locks should be cleaned up");
}

#[test]
fn test_deadlock_statistics() {
    let detector = DeadlockDetector::new(5000, false);
    
    let stats = detector.get_stats();
    assert_eq!(stats.held_locks_count, 0);
    assert_eq!(stats.waiting_clients_count, 0);
    assert_eq!(stats.wait_graph_edges, 0);
    
    // Add some locks
    let client1 = Bytes::from("client-1");
    let client2 = Bytes::from("client-2");
    
    detector.record_lock_acquired("resource-a".to_string(), client1.clone(), 10000);
    detector.record_lock_acquired("resource-b".to_string(), client2.clone(), 10000);
    detector.record_lock_wait("resource-b".to_string(), client1, 10000);
    
    let stats = detector.get_stats();
    assert_eq!(stats.held_locks_count, 2);
    assert_eq!(stats.waiting_clients_count, 1);
    assert_eq!(stats.wait_graph_edges, 1);
}

#[test]
fn test_victim_selection() {
    let detector = DeadlockDetector::new(5000, true); // Enable auto-resolve
    
    let client1 = Bytes::from("client-1");
    let client2 = Bytes::from("client-2");
    
    // Create deadlock
    detector.record_lock_acquired("resource-a".to_string(), client1.clone(), 10000);
    thread::sleep(Duration::from_millis(10)); // Make client1's lock older
    detector.record_lock_acquired("resource-b".to_string(), client2.clone(), 10000);
    
    detector.record_lock_wait("resource-b".to_string(), client1.clone(), 10000);
    detector.record_lock_wait("resource-a".to_string(), client2.clone(), 10000);
    
    // Get the cycle
    if let DeadlockStatus::Deadlock { cycle, .. } = detector.detect_deadlock() {
        // Select victim
        let victim = detector.resolve_deadlock(&cycle);
        assert!(victim.is_some(), "Should select a victim for resolution");
        
        // Victim should be one of the clients in the cycle
        let victim = victim.unwrap();
        assert!(cycle.contains(&victim), "Victim should be in the deadlock cycle");
    } else {
        panic!("Expected deadlock");
    }
}

#[tokio::test]
async fn test_async_detect_planted_cycle() {
    let detector = DeadlockDetector::new(5000, false);
    let client1 = Bytes::from("async-client-1");
    let client2 = Bytes::from("async-client-2");

    detector.record_lock_acquired("resource-a".to_string(), client1.clone(), 10000);
    detector.record_lock_acquired("resource-b".to_string(), client2.clone(), 10000);
    detector.record_lock_wait("resource-b".to_string(), client1.clone(), 10000);
    detector.record_lock_wait("resource-a".to_string(), client2.clone(), 10000);

    match detector.detect_deadlock_async().await {
        DeadlockStatus::Deadlock { cycle, resources } => {
            assert!(cycle.len() >= 2);
            assert!(resources.len() >= 2);
        }
        DeadlockStatus::NoDeadlock => panic!("async detect should find planted cycle"),
    }

    // Redlock async surface
    let cache1 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache2 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let cache3 = Cache::new_with_sweep(256, 100 * 1024 * 1024, 1024 * 1024, false);
    let redlock = Redlock::new(vec![cache1, cache2, cache3])
        .unwrap()
        .with_deadlock_detection(5000, false);
    // No waits recorded via Redlock path — expect NoDeadlock
    assert!(matches!(
        redlock.check_deadlock_async().await,
        Some(DeadlockStatus::NoDeadlock)
    ));
}

#[tokio::test]
async fn test_background_monitor_auto_resolves() {
    use std::sync::Arc;

    let detector = Arc::new(DeadlockDetector::new(5000, true)); // Youngest default
    let client1 = Bytes::from("mon-client-1");
    let client2 = Bytes::from("mon-client-2");

    detector.record_lock_acquired("resource-a".to_string(), client1.clone(), 10000);
    thread::sleep(Duration::from_millis(5));
    detector.record_lock_acquired("resource-b".to_string(), client2.clone(), 10000);
    detector.record_lock_wait("resource-b".to_string(), client1, 10000);
    detector.record_lock_wait("resource-a".to_string(), client2.clone(), 10000);

    assert!(matches!(
        detector.detect_deadlock(),
        DeadlockStatus::Deadlock { .. }
    ));

    let handle =
        DeadlockDetector::spawn_monitor(Arc::clone(&detector), Duration::from_millis(25));
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(
        matches!(detector.detect_deadlock(), DeadlockStatus::NoDeadlock),
        "monitor should auto-resolve planted deadlock"
    );
    // Youngest = client2
    assert!(
        !detector
            .get_held_locks()
            .iter()
            .any(|l| l.client_id == client2),
        "victim locks should be released from the wait-for graph"
    );

    handle.abort();
    let _ = handle.await;
}
