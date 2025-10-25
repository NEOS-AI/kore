use crate::entry::SharedEntry;
use ahash::RandomState;
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// A single shard containing a hashmap and metadata
pub struct Shard {
    /// The hashmap for this shard
    map: RwLock<HashMap<Bytes, SharedEntry, RandomState>>,
    /// CAS counter for this shard
    cas_counter: AtomicU64,
    /// Total size of entries in this shard
    size: AtomicU64,
}

impl Shard {
    fn new(capacity: usize) -> Self {
        Self {
            map: RwLock::new(HashMap::with_capacity_and_hasher(
                capacity,
                RandomState::new(),
            )),
            cas_counter: AtomicU64::new(1),
            size: AtomicU64::new(0),
        }
    }

    /// Get the next CAS value for this shard
    pub fn next_cas(&self) -> u64 {
        self.cas_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Get the current size of entries in this shard
    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed) as usize
    }

    /// Get an entry from this shard
    pub fn get(&self, key: &Bytes) -> Option<SharedEntry> {
        let map = self.map.read();
        map.get(key).cloned()
    }

    /// Insert an entry into this shard
    pub fn insert(&self, key: Bytes, entry: SharedEntry) -> Option<SharedEntry> {
        let entry_size = entry.size();
        let mut map = self.map.write();
        let old = map.insert(key, entry);

        if let Some(ref old_entry) = old {
            // Subtract old size, add new size
            let old_size = old_entry.size();
            self.size.fetch_sub(old_size as u64, Ordering::Relaxed);
        }
        self.size.fetch_add(entry_size as u64, Ordering::Relaxed);

        old
    }

    /// Remove an entry from this shard
    pub fn remove(&self, key: &Bytes) -> Option<SharedEntry> {
        let mut map = self.map.write();
        let entry = map.remove(key);

        if let Some(ref e) = entry {
            self.size.fetch_sub(e.size() as u64, Ordering::Relaxed);
        }

        entry
    }

    /// Get the number of entries in this shard
    pub fn len(&self) -> usize {
        self.map.read().len()
    }

    /// Check if the shard is empty
    pub fn is_empty(&self) -> bool {
        self.map.read().is_empty()
    }

    /// Clear all entries from this shard
    pub fn clear(&self) {
        let mut map = self.map.write();
        map.clear();
        self.size.store(0, Ordering::Relaxed);
    }

    /// Iterate over entries and remove expired ones
    pub fn sweep_expired(&self) -> usize {
        let mut map = self.map.write();
        let mut removed = 0;
        let mut size_freed = 0;

        map.retain(|_, entry| {
            if entry.is_expired() {
                size_freed += entry.size();
                removed += 1;
                false
            } else {
                true
            }
        });

        if size_freed > 0 {
            self.size.fetch_sub(size_freed as u64, Ordering::Relaxed);
        }

        removed
    }

    /// Get all keys matching a pattern (simple glob-style pattern)
    pub fn keys(&self, pattern: Option<&str>) -> Vec<Bytes> {
        let map = self.map.read();
        if let Some(pat) = pattern {
            map.keys()
                .filter(|k| pattern_match(pat, std::str::from_utf8(k).unwrap_or("")))
                .cloned()
                .collect()
        } else {
            map.keys().cloned().collect()
        }
    }

    /// Get a random entry (for eviction)
    pub fn get_random(&self) -> Option<(Bytes, SharedEntry)> {
        let map = self.map.read();
        map.iter()
            .next()
            .map(|(k, v)| (k.clone(), v.clone()))
    }
}

/// Sharded hashmap for high-concurrency cache operations
pub struct ShardedHashMap {
    shards: Vec<Shard>,
    num_shards: usize,
    hasher: RandomState,
}

impl ShardedHashMap {
    pub fn new(num_shards: usize, capacity_per_shard: usize) -> Self {
        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(Shard::new(capacity_per_shard));
        }

        Self {
            shards,
            num_shards,
            hasher: RandomState::new(),
        }
    }

    /// Get the shard index for a given key
    fn shard_index(&self, key: &Bytes) -> usize {
        let hash = self.hasher.hash_one(key);
        (hash as usize) % self.num_shards
    }

    /// Get a reference to the shard for a given key
    fn get_shard(&self, key: &Bytes) -> &Shard {
        let idx = self.shard_index(key);
        &self.shards[idx]
    }

    /// Get an entry
    pub fn get(&self, key: &Bytes) -> Option<SharedEntry> {
        self.get_shard(key).get(key)
    }

    /// Insert an entry
    pub fn insert(&self, key: Bytes, entry: SharedEntry) -> Option<SharedEntry> {
        let shard = self.get_shard(&key);
        shard.insert(key, entry)
    }

    /// Remove an entry
    pub fn remove(&self, key: &Bytes) -> Option<SharedEntry> {
        self.get_shard(key).remove(key)
    }

    /// Get the total number of entries
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.len()).sum()
    }

    /// Check if the map is empty
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.is_empty())
    }

    /// Get the total size of all entries
    pub fn size(&self) -> usize {
        self.shards.iter().map(|s| s.size()).sum()
    }

    /// Clear all entries
    pub fn clear(&self) {
        for shard in &self.shards {
            shard.clear();
        }
    }

    /// Sweep expired entries from all shards
    pub fn sweep_expired(&self) -> usize {
        self.shards.iter().map(|s| s.sweep_expired()).sum()
    }

    /// Get all keys matching a pattern
    pub fn keys(&self, pattern: Option<&str>) -> Vec<Bytes> {
        self.shards
            .iter()
            .flat_map(|s| s.keys(pattern))
            .collect()
    }

    /// Get next CAS value for a key
    pub fn next_cas(&self, key: &Bytes) -> u64 {
        self.get_shard(key).next_cas()
    }

    /// Get a random entry from a random shard (for eviction)
    pub fn get_random(&self) -> Option<(Bytes, SharedEntry)> {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        // Try up to 10 random shards
        for _ in 0..10 {
            let idx = rng.gen_range(0..self.num_shards);
            if let Some(entry) = self.shards[idx].get_random() {
                return Some(entry);
            }
        }
        None
    }

    /// Get 2 random entries (for 2-random eviction algorithm)
    pub fn get_two_random(&self) -> Vec<(Bytes, SharedEntry)> {
        let mut result = Vec::with_capacity(2);

        if let Some(entry) = self.get_random() {
            result.push(entry);
        }
        if let Some(entry) = self.get_random() {
            result.push(entry);
        }

        result
    }
}

/// Simple glob-style pattern matching (supports * and ?)
fn pattern_match(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    let pattern_chars: Vec<char> = pattern.chars().collect();
    let text_chars: Vec<char> = text.chars().collect();

    let mut p = 0;
    let mut t = 0;
    let mut star_idx = None;
    let mut match_idx = 0;

    while t < text_chars.len() {
        if p < pattern_chars.len() {
            if pattern_chars[p] == '*' {
                star_idx = Some(p);
                match_idx = t;
                p += 1;
                continue;
            }
            if pattern_chars[p] == '?' || pattern_chars[p] == text_chars[t] {
                p += 1;
                t += 1;
                continue;
            }
        }

        if let Some(star) = star_idx {
            p = star + 1;
            match_idx += 1;
            t = match_idx;
        } else {
            return false;
        }
    }

    while p < pattern_chars.len() && pattern_chars[p] == '*' {
        p += 1;
    }

    p == pattern_chars.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_match() {
        assert!(pattern_match("*", "anything"));
        assert!(pattern_match("hello*", "hello world"));
        assert!(pattern_match("*world", "hello world"));
        assert!(pattern_match("h?llo", "hello"));
        assert!(pattern_match("h*o", "hello"));
        assert!(!pattern_match("hello", "world"));
    }
}
