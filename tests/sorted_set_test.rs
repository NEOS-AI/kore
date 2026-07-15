use bytes::Bytes;
use kore::sorted_set::SortedSet;
use kore::Cache;
use std::sync::Arc;
use std::thread;

#[test]
fn test_zadd_and_zcard() {
    let mut zset = SortedSet::new();
    
    // Add members
    assert!(zset.add(Bytes::from("alice"), 100.0));
    assert!(zset.add(Bytes::from("bob"), 200.0));
    assert!(zset.add(Bytes::from("charlie"), 150.0));
    
    // Check count
    assert_eq!(zset.len(), 3);
}

#[test]
fn test_zscore() {
    let mut zset = SortedSet::new();
    
    zset.add(Bytes::from("alice"), 100.0);
    zset.add(Bytes::from("bob"), 200.0);
    
    assert_eq!(zset.score(&Bytes::from("alice")), Some(100.0));
    assert_eq!(zset.score(&Bytes::from("bob")), Some(200.0));
    assert_eq!(zset.score(&Bytes::from("charlie")), None);
}

#[test]
fn test_zrange_ascending() {
    let mut zset = SortedSet::new();
    
    zset.add(Bytes::from("alice"), 100.0);
    zset.add(Bytes::from("bob"), 200.0);
    zset.add(Bytes::from("charlie"), 150.0);
    zset.add(Bytes::from("david"), 175.0);
    
    // Get all members in ascending order
    let members = zset.range(0, -1, false);
    assert_eq!(members.len(), 4);
    assert_eq!(members[0].member, Bytes::from("alice"));
    assert_eq!(members[0].score, 100.0);
    assert_eq!(members[1].member, Bytes::from("charlie"));
    assert_eq!(members[1].score, 150.0);
    assert_eq!(members[2].member, Bytes::from("david"));
    assert_eq!(members[2].score, 175.0);
    assert_eq!(members[3].member, Bytes::from("bob"));
    assert_eq!(members[3].score, 200.0);
    
    // Get partial range
    let members = zset.range(1, 2, false);
    assert_eq!(members.len(), 2);
    assert_eq!(members[0].member, Bytes::from("charlie"));
    assert_eq!(members[1].member, Bytes::from("david"));
}

#[test]
fn test_zrevrange_descending() {
    let mut zset = SortedSet::new();
    
    zset.add(Bytes::from("alice"), 100.0);
    zset.add(Bytes::from("bob"), 200.0);
    zset.add(Bytes::from("charlie"), 150.0);
    zset.add(Bytes::from("david"), 175.0);
    
    // Get all members in descending order
    let members = zset.range(0, -1, true);
    assert_eq!(members.len(), 4);
    assert_eq!(members[0].member, Bytes::from("bob"));
    assert_eq!(members[0].score, 200.0);
    assert_eq!(members[1].member, Bytes::from("david"));
    assert_eq!(members[1].score, 175.0);
    assert_eq!(members[2].member, Bytes::from("charlie"));
    assert_eq!(members[2].score, 150.0);
    assert_eq!(members[3].member, Bytes::from("alice"));
    assert_eq!(members[3].score, 100.0);
}

#[test]
fn test_zrem() {
    let mut zset = SortedSet::new();
    
    zset.add(Bytes::from("alice"), 100.0);
    zset.add(Bytes::from("bob"), 200.0);
    zset.add(Bytes::from("charlie"), 150.0);
    
    // Remove a member
    assert!(zset.remove(&Bytes::from("bob")));
    assert_eq!(zset.len(), 2);
    
    // Try removing non-existent member
    assert!(!zset.remove(&Bytes::from("bob")));
    
    // Check remaining members
    let members = zset.range(0, -1, false);
    assert_eq!(members.len(), 2);
    assert_eq!(members[0].member, Bytes::from("alice"));
    assert_eq!(members[1].member, Bytes::from("charlie"));
}

#[test]
fn test_zrank_and_zrevrank() {
    let mut zset = SortedSet::new();
    
    zset.add(Bytes::from("alice"), 100.0);
    zset.add(Bytes::from("bob"), 200.0);
    zset.add(Bytes::from("charlie"), 150.0);
    
    // Test rank (ascending)
    assert_eq!(zset.rank(&Bytes::from("alice")), Some(0));
    assert_eq!(zset.rank(&Bytes::from("charlie")), Some(1));
    assert_eq!(zset.rank(&Bytes::from("bob")), Some(2));
    
    // Test reverse rank (descending)
    assert_eq!(zset.rev_rank(&Bytes::from("bob")), Some(0));
    assert_eq!(zset.rev_rank(&Bytes::from("charlie")), Some(1));
    assert_eq!(zset.rev_rank(&Bytes::from("alice")), Some(2));
}

#[test]
fn test_zadd_update_score() {
    let mut zset = SortedSet::new();
    
    // Add a member
    assert!(zset.add(Bytes::from("alice"), 100.0));
    
    // Update score (should return false as it's an update)
    assert!(!zset.add(Bytes::from("alice"), 200.0));
    
    // Check the updated score
    assert_eq!(zset.score(&Bytes::from("alice")), Some(200.0));
    assert_eq!(zset.len(), 1);
}

#[test]
fn test_negative_indices() {
    let mut zset = SortedSet::new();
    
    zset.add(Bytes::from("a"), 1.0);
    zset.add(Bytes::from("b"), 2.0);
    zset.add(Bytes::from("c"), 3.0);
    zset.add(Bytes::from("d"), 4.0);
    
    // Last two elements
    let members = zset.range(-2, -1, false);
    assert_eq!(members.len(), 2);
    assert_eq!(members[0].member, Bytes::from("c"));
    assert_eq!(members[1].member, Bytes::from("d"));
    
    // First two elements using negative indices
    let members = zset.range(-4, -3, false);
    assert_eq!(members.len(), 2);
    assert_eq!(members[0].member, Bytes::from("a"));
    assert_eq!(members[1].member, Bytes::from("b"));
}

#[test]
fn test_empty_range() {
    let mut zset = SortedSet::new();
    
    zset.add(Bytes::from("alice"), 100.0);
    
    // Invalid range (start > stop after normalization)
    let members = zset.range(1, 0, false);
    assert_eq!(members.len(), 0);
}

#[test]
fn test_ranking_system_scenario() {
    let mut leaderboard = SortedSet::new();
    
    // Add players with scores
    leaderboard.add(Bytes::from("player1"), 1000.0);
    leaderboard.add(Bytes::from("player2"), 1500.0);
    leaderboard.add(Bytes::from("player3"), 1200.0);
    leaderboard.add(Bytes::from("player4"), 1800.0);
    leaderboard.add(Bytes::from("player5"), 900.0);
    
    // Get top 3 players (highest scores)
    let top3 = leaderboard.range(0, 2, true);
    assert_eq!(top3.len(), 3);
    assert_eq!(top3[0].member, Bytes::from("player4")); // 1800
    assert_eq!(top3[1].member, Bytes::from("player2")); // 1500
    assert_eq!(top3[2].member, Bytes::from("player3")); // 1200
    
    // Update player3's score
    leaderboard.add(Bytes::from("player3"), 2000.0);
    
    // Check new top 3
    let top3 = leaderboard.range(0, 2, true);
    assert_eq!(top3[0].member, Bytes::from("player3")); // 2000 (now highest!)
    assert_eq!(top3[1].member, Bytes::from("player4")); // 1800
    assert_eq!(top3[2].member, Bytes::from("player2")); // 1500
    
    // Get rank of player3
    assert_eq!(leaderboard.rev_rank(&Bytes::from("player3")), Some(0)); // Rank 1 (0-indexed)
    
    // Get bottom 2 players
    let bottom2 = leaderboard.range(0, 1, false);
    assert_eq!(bottom2[0].member, Bytes::from("player5")); // 900
    assert_eq!(bottom2[1].member, Bytes::from("player1")); // 1000
}

#[test]
fn test_zrank_matches_range_position() {
    let mut zset = SortedSet::new();
    for i in 0..100 {
        // Insert out of order
        let score = ((i * 37) % 100) as f64;
        zset.add(Bytes::from(format!("m{i:03}")), score);
    }
    let all = zset.range(0, -1, false);
    for (i, sm) in all.iter().enumerate() {
        assert_eq!(zset.rank(&sm.member), Some(i));
        assert_eq!(zset.rev_rank(&sm.member), Some(all.len() - 1 - i));
    }
}

#[test]
fn test_remove_range_by_rank_empties() {
    let mut zset = SortedSet::new();
    for i in 0..10 {
        zset.add(Bytes::from(format!("m{i}")), i as f64);
    }
    let n = zset.remove_range_by_rank(0, -1);
    assert_eq!(n, 10);
    assert!(zset.is_empty());
    assert_eq!(zset.rank(&Bytes::from("m0")), None);
}

#[test]
fn test_cache_sharded_zsets_concurrent() {
    // Concurrent ZADD/ZREM on different keys across shards
    let cache = Cache::new_with_sweep(32, 1024 * 1024 * 100, 500 * 1024 * 1024, false);
    let mut handles = Vec::new();
    for t in 0..8 {
        let cache = Arc::clone(&cache);
        handles.push(thread::spawn(move || {
            for i in 0..50 {
                let key = Bytes::from(format!("zset-{t}"));
                let zset = cache.get_or_create_sorted_set(&key).unwrap();
                {
                    let mut s = zset.write().unwrap();
                    s.add(Bytes::from(format!("m{i}")), i as f64);
                }
            }
            // Rank check under read lock
            let key = Bytes::from(format!("zset-{t}"));
            let zset = cache.get_sorted_set(&key).unwrap();
            let s = zset.read().unwrap();
            assert_eq!(s.len(), 50);
            assert_eq!(s.rank(&Bytes::from("m0")), Some(0));
            assert_eq!(s.rank(&Bytes::from("m49")), Some(49));
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    // 8 independent keys
    let mut count = 0;
    for t in 0..8 {
        if cache.sorted_set_exists(&Bytes::from(format!("zset-{t}"))) {
            count += 1;
        }
    }
    assert_eq!(count, 8);
}

#[test]
fn test_cache_same_zset_concurrent_updates() {
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 50, 500 * 1024 * 1024, false);
    let key = Bytes::from_static(b"shared-z");
    let _ = cache.get_or_create_sorted_set(&key).unwrap();
    let mut handles = Vec::new();
    for t in 0..4 {
        let cache = Arc::clone(&cache);
        let key = key.clone();
        handles.push(thread::spawn(move || {
            for i in 0..100 {
                let zset = cache.get_or_create_sorted_set(&key).unwrap();
                let mut s = zset.write().unwrap();
                s.add(Bytes::from(format!("t{t}-m{i}")), (t * 1000 + i) as f64);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let zset = cache.get_sorted_set(&key).unwrap();
    let s = zset.read().unwrap();
    assert_eq!(s.len(), 400);
    // Lowest score is t0-m0
    assert_eq!(s.rank(&Bytes::from("t0-m0")), Some(0));
    // Highest is t3-m99
    assert_eq!(s.rank(&Bytes::from("t3-m99")), Some(399));
}
