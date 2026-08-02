use crate::entry::SharedEntry;
use ahash::RandomState;
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

/// Action to take on a map entry after a read-modify-write callback.
pub enum EntryAction {
    /// Leave the map unchanged
    Keep,
    /// Insert or replace with this entry
    Set(SharedEntry),
    /// Remove the key
    Remove,
}

/// Generic RMW action for [`ShardedKeyMap`] (Batch FG-4).
pub enum MapAction<V> {
    /// Leave the map unchanged
    Keep,
    /// Insert or replace with this value
    Set(V),
    /// Remove the key
    Remove,
}

/// Result of sweeping expired entries: (count removed, bytes freed).
#[derive(Debug, Clone, Copy, Default)]
pub struct SweepResult {
    pub count: usize,
    pub bytes_freed: usize,
}

/// Extended stats from a sampling-based active-expire cycle.
#[derive(Debug, Clone, Copy, Default)]
pub struct ActiveExpireResult {
    pub count: usize,
    pub bytes_freed: usize,
    /// Keys with a TTL that were examined.
    pub sampled: usize,
    /// Number of sample passes executed.
    pub passes: usize,
}

impl ActiveExpireResult {
    pub fn as_sweep(&self) -> SweepResult {
        SweepResult {
            count: self.count,
            bytes_freed: self.bytes_freed,
        }
    }
}

/// Default samples per pass (Redis `ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP` ≈ 20).
pub const ACTIVE_EXPIRE_SAMPLES_PER_PASS: usize = 20;
/// Continue another pass when expired/sampled exceeds this ratio (Redis 25%).
pub const ACTIVE_EXPIRE_CONTINUE_RATIO: f64 = 0.25;
/// Max passes per cycle to bound work even when many keys are stale.
pub const ACTIVE_EXPIRE_MAX_PASSES: usize = 16;

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

    /// Under a single shard write lock, call `f` with the current entry and next CAS.
    /// Expired entries are still visible — the caller decides how to handle them.
    pub fn mutate<F, R>(&self, key: &Bytes, f: F) -> R
    where
        F: FnOnce(Option<&SharedEntry>, u64) -> (EntryAction, R),
    {
        let mut map = self.map.write();
        let next_cas = self.next_cas();

        let (action, result) = {
            let current = map.get(key);
            f(current, next_cas)
        };

        match action {
            EntryAction::Keep => {}
            EntryAction::Set(entry) => {
                let new_size = entry.size() as u64;
                if let Some(old) = map.insert(key.clone(), entry) {
                    self.size.fetch_sub(old.size() as u64, Ordering::Relaxed);
                }
                self.size.fetch_add(new_size, Ordering::Relaxed);
            }
            EntryAction::Remove => {
                if let Some(old) = map.remove(key) {
                    self.size.fetch_sub(old.size() as u64, Ordering::Relaxed);
                }
            }
        }

        result
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

    /// Drain every entry, leaving the shard empty.
    fn drain_all(&self) -> Vec<(Bytes, SharedEntry)> {
        let mut map = self.map.write();
        let entries: Vec<_> = map.drain().collect();
        self.size.store(0, Ordering::Relaxed);
        entries
    }

    /// Legacy full-scan expire sweep.
    ///
    /// Batch GA: [`crate::entry::Entry`] no longer carries TTL. Live keyspace
    /// expire is [`crate::cache::keyspace::KeySlot::expires_at`] on
    /// `ShardedKeyMap`. This legacy [`ShardedHashMap`] path is a no-op.
    pub fn sweep_expired(&self) -> SweepResult {
        SweepResult::default()
    }

    /// Legacy active-expire sample (no-op after Batch GA; see [`sweep_expired`]).
    pub fn active_expire_sample(&self, _samples: usize) -> ActiveExpireResult {
        ActiveExpireResult::default()
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

    /// Get a true random entry (for eviction sampling)
    pub fn get_random(&self) -> Option<(Bytes, SharedEntry)> {
        use rand::Rng;
        let map = self.map.read();
        let len = map.len();
        if len == 0 {
            return None;
        }
        let idx = rand::thread_rng().gen_range(0..len);
        map.iter()
            .nth(idx)
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

    /// Atomic read-modify-write on the shard that owns `key`.
    pub fn mutate<F, R>(&self, key: &Bytes, f: F) -> R
    where
        F: FnOnce(Option<&SharedEntry>, u64) -> (EntryAction, R),
    {
        self.get_shard(key).mutate(key, f)
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

    /// Number of shards in this map.
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }

    /// Drain every entry across shards (leaves the map empty).
    ///
    /// **Exclusive access**: not atomic under concurrent writers. Used by
    /// scratch-load keyspace swap. Pre-reserves `len()` to reduce mid-drain realloc.
    pub fn drain_all(&self) -> Vec<(Bytes, SharedEntry)> {
        let mut out = Vec::with_capacity(self.len());
        for shard in &self.shards {
            out.extend(shard.drain_all());
        }
        out
    }

    /// Replace contents with `entries` (rehashes into this map's shards).
    ///
    /// **Exclusive access**: not safe under concurrent mutators. Drains prior
    /// entries into a local discard held until the new inserts finish, so a
    /// panic mid-fill still drops the old payload only after unwind (same
    /// process-OOM policy as clear-then-fill, but keeps one consistent drain
    /// path with [`drain_all`]).
    pub fn replace_all(&self, entries: Vec<(Bytes, SharedEntry)>) {
        let _discard = self.drain_all();
        self.fill_all(entries);
    }

    /// Insert `entries` into an **already empty** map (no drain/clear).
    ///
    /// Used after an external [`drain_all`] so install paths do not double-drain.
    /// **Exclusive access** required; caller owns emptiness invariant.
    pub fn fill_all(&self, entries: Vec<(Bytes, SharedEntry)>) {
        debug_assert!(
            self.is_empty(),
            "fill_all requires an empty map (caller must drain first)"
        );
        for (k, v) in entries {
            self.insert(k, v);
        }
    }

    /// Full-scan sweep of expired entries from all shards.
    /// Prefer `active_expire_cycle` for background / production paths.
    /// Returns (count removed, bytes freed).
    pub fn sweep_expired(&self) -> SweepResult {
        let mut total = SweepResult::default();
        for shard in &self.shards {
            let r = shard.sweep_expired();
            total.count += r.count;
            total.bytes_freed += r.bytes_freed;
        }
        total
    }

    /// Redis-style active expire cycle across shards.
    ///
    /// Each pass samples up to `samples_per_pass` random TTL keys (via random
    /// shards). If more than [`ACTIVE_EXPIRE_CONTINUE_RATIO`] of samples were
    /// expired, another pass runs — until `max_passes` or `time_budget` ends.
    pub fn active_expire_cycle(
        &self,
        samples_per_pass: usize,
        max_passes: usize,
        time_budget: std::time::Duration,
    ) -> ActiveExpireResult {
        use rand::Rng;
        use std::time::Instant;

        let start = Instant::now();
        let mut total = ActiveExpireResult::default();
        if self.num_shards == 0 || samples_per_pass == 0 || max_passes == 0 {
            return total;
        }

        let mut rng = rand::thread_rng();

        for pass in 0..max_passes {
            if start.elapsed() >= time_budget {
                break;
            }

            let mut pass_sampled = 0usize;
            let mut pass_expired = 0usize;

            // Draw samples from random shards so we don't pin one hot shard.
            // Each call tries hard to find TTL keys (up to 5× attempts inside).
            let mut left = samples_per_pass;
            while left > 0 {
                if start.elapsed() >= time_budget {
                    break;
                }
                let idx = rng.gen_range(0..self.num_shards);
                // Take a small batch per shard visit (amortize RNG).
                let batch = left.min(4);
                let r = self.shards[idx].active_expire_sample(batch);
                total.count += r.count;
                total.bytes_freed += r.bytes_freed;
                total.sampled += r.sampled;
                pass_sampled += r.sampled;
                pass_expired += r.count;
                // Progress even when shard has no TTL keys so we don't spin.
                left = left.saturating_sub(if r.sampled > 0 { r.sampled } else { batch });
            }

            total.passes = pass + 1;

            // Redis: stop early when the expired fraction looks healthy.
            if pass_sampled == 0 {
                break;
            }
            let ratio = pass_expired as f64 / pass_sampled as f64;
            if ratio <= ACTIVE_EXPIRE_CONTINUE_RATIO {
                break;
            }
        }

        total
    }

    /// Convenience: one active-expire cycle with Redis-like defaults and a short budget.
    pub fn active_expire_default(&self) -> ActiveExpireResult {
        self.active_expire_cycle(
            ACTIVE_EXPIRE_SAMPLES_PER_PASS,
            ACTIVE_EXPIRE_MAX_PASSES,
            std::time::Duration::from_millis(1),
        )
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
        self.get_n_random(2)
    }

    /// Get N unique random entries (for approximated LRU eviction).
    /// Dedupes by key and retries to fill the sample when possible.
    pub fn get_n_random(&self, n: usize) -> Vec<(Bytes, SharedEntry)> {
        let mut result = Vec::with_capacity(n);
        let mut seen: HashSet<Bytes> = HashSet::with_capacity(n);
        // Extra attempts to obtain unique keys when collisions occur
        let max_attempts = n.saturating_mul(5).max(n);

        for _ in 0..max_attempts {
            if result.len() >= n {
                break;
            }
            match self.get_random() {
                Some((key, entry)) => {
                    if seen.insert(key.clone()) {
                        result.push((key, entry));
                    }
                }
                None => break,
            }
        }

        result
    }
}

/// Sharded key → value map for the unified Redis keyspace (`KeyValue`, …).
///
/// Same sharding idea as [`ShardedHashMap`]: keys hash to independent
/// `parking_lot::RwLock` shards so concurrent ops on different keys do not
/// contend on one global lock. Per-shard CAS counters support string RMW
/// (SET NX / INCR / CAS) when values are string slots (`KeySlot` / `KeyValue::String`).
pub struct ShardedKeyMap<V> {
    shards: Vec<RwLock<HashMap<Bytes, V, RandomState>>>,
    /// Per-shard CAS counters for string Entry.cas (Batch FG-4).
    cas_counters: Vec<AtomicU64>,
    num_shards: usize,
    hasher: RandomState,
}

impl<V: Clone> ShardedKeyMap<V> {
    pub fn new(num_shards: usize) -> Self {
        let n = num_shards.max(1);
        let mut shards = Vec::with_capacity(n);
        let mut cas_counters = Vec::with_capacity(n);
        for _ in 0..n {
            shards.push(RwLock::new(HashMap::with_hasher(RandomState::new())));
            cas_counters.push(AtomicU64::new(1));
        }
        Self {
            shards,
            cas_counters,
            num_shards: n,
            hasher: RandomState::new(),
        }
    }

    pub fn num_shards(&self) -> usize {
        self.num_shards
    }

    fn shard_index(&self, key: &Bytes) -> usize {
        (self.hasher.hash_one(key) as usize) % self.num_shards
    }

    fn shard(&self, key: &Bytes) -> &RwLock<HashMap<Bytes, V, RandomState>> {
        &self.shards[self.shard_index(key)]
    }

    /// Next CAS value for the shard that owns `key`.
    pub fn next_cas(&self, key: &Bytes) -> u64 {
        let idx = self.shard_index(key);
        self.cas_counters[idx].fetch_add(1, Ordering::Relaxed)
    }

    pub fn get(&self, key: &Bytes) -> Option<V> {
        self.shard(key).read().get(key).cloned()
    }

    pub fn contains_key(&self, key: &Bytes) -> bool {
        self.shard(key).read().contains_key(key)
    }

    pub fn insert(&self, key: Bytes, value: V) -> Option<V> {
        self.shard(&key).write().insert(key, value)
    }

    pub fn remove(&self, key: &Bytes) -> Option<V> {
        self.shard(key).write().remove(key)
    }

    /// Atomic read-modify-write under the shard write lock.
    ///
    /// Callback receives the current value (if any) and a fresh CAS id.
    pub fn mutate<F, R>(&self, key: &Bytes, f: F) -> R
    where
        F: FnOnce(Option<&V>, u64) -> (MapAction<V>, R),
    {
        let mut map = self.shard(key).write();
        let next_cas = {
            let idx = self.shard_index(key);
            self.cas_counters[idx].fetch_add(1, Ordering::Relaxed)
        };
        let (action, result) = {
            let current = map.get(key);
            f(current, next_cas)
        };
        match action {
            MapAction::Keep => {}
            MapAction::Set(v) => {
                // Batch GC: overwrite in place avoids cloning the map key on
                // the common redis-benchmark / replace path.
                if let Some(slot) = map.get_mut(key) {
                    *slot = v;
                } else {
                    map.insert(key.clone(), v);
                }
            }
            MapAction::Remove => {
                map.remove(key);
            }
        }
        result
    }

    /// Return existing value, or insert via `f` and return the new one.
    ///
    /// Double-checked so `f` runs at most once under the write lock when racing.
    pub fn get_or_insert_with<F>(&self, key: Bytes, f: F) -> V
    where
        F: FnOnce() -> V,
    {
        {
            let map = self.shard(&key).read();
            if let Some(v) = map.get(&key) {
                return v.clone();
            }
        }
        let mut map = self.shard(&key).write();
        map.entry(key).or_insert_with(f).clone()
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.read().is_empty())
    }

    pub fn clear(&self) {
        for s in &self.shards {
            s.write().clear();
        }
    }

    /// Drain every entry across shards (leaves the map empty).
    ///
    /// **Exclusive access**: not atomic under concurrent writers. Pre-reserves
    /// `len()` to reduce mid-drain realloc.
    pub fn drain_all(&self) -> Vec<(Bytes, V)> {
        let mut out = Vec::with_capacity(self.len());
        for s in &self.shards {
            out.extend(s.write().drain());
        }
        out
    }

    /// Replace contents with `entries` (rehashes into this map's shards).
    ///
    /// **Exclusive access**: drain-then-fill (see [`ShardedHashMap::replace_all`]).
    pub fn replace_all(&self, entries: Vec<(Bytes, V)>) {
        let _discard = self.drain_all();
        self.fill_all(entries);
    }

    /// Insert into an already-empty map (no drain). See [`ShardedHashMap::fill_all`].
    pub fn fill_all(&self, entries: Vec<(Bytes, V)>) {
        debug_assert!(
            self.is_empty(),
            "fill_all requires an empty map (caller must drain first)"
        );
        for (k, v) in entries {
            self.insert(k, v);
        }
    }

    /// Collect all keys (optionally filtered by glob pattern).
    pub fn keys(&self, pattern: Option<&str>) -> Vec<Bytes> {
        let mut out = Vec::new();
        for s in &self.shards {
            let map = s.read();
            for key in map.keys() {
                let ok = match pattern {
                    Some(pat) => pattern_match(pat, std::str::from_utf8(key).unwrap_or("")),
                    None => true,
                };
                if ok {
                    out.push(key.clone());
                }
            }
        }
        out
    }

    /// Visit every (key, value) under successive per-shard read locks.
    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(&Bytes, &V),
    {
        for s in &self.shards {
            let map = s.read();
            for (k, v) in map.iter() {
                f(k, v);
            }
        }
    }

    /// Random entry from a random non-empty shard (for eviction sampling).
    pub fn get_random(&self) -> Option<(Bytes, V)> {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        for _ in 0..10 {
            let idx = rng.gen_range(0..self.num_shards);
            let map = self.shards[idx].read();
            let len = map.len();
            if len == 0 {
                continue;
            }
            let i = rng.gen_range(0..len);
            if let Some((k, v)) = map.iter().nth(i) {
                return Some((k.clone(), v.clone()));
            }
        }
        None
    }

    /// Up to `n` unique random entries (approximated sampling for eviction).
    pub fn get_n_random(&self, n: usize) -> Vec<(Bytes, V)> {
        use std::collections::HashSet;
        let mut result = Vec::with_capacity(n);
        let mut seen: HashSet<Bytes> = HashSet::with_capacity(n);
        let max_attempts = n.saturating_mul(5).max(n);
        for _ in 0..max_attempts {
            if result.len() >= n {
                break;
            }
            if let Some((key, val)) = self.get_random() {
                if seen.insert(key.clone()) {
                    result.push((key, val));
                }
            } else {
                break;
            }
        }
        result
    }
}

/// Simple glob-style pattern matching (supports * and ?)
pub(crate) fn pattern_match(pattern: &str, text: &str) -> bool {
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
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_pattern_match() {
        assert!(pattern_match("*", "anything"));
        assert!(pattern_match("hello*", "hello world"));
        assert!(pattern_match("*world", "hello world"));
        assert!(pattern_match("h?llo", "hello"));
        assert!(pattern_match("h*o", "hello"));
        assert!(!pattern_match("hello", "world"));
    }

    #[test]
    fn test_sharded_key_map_basic() {
        let m: ShardedKeyMap<u32> = ShardedKeyMap::new(16);
        assert!(m.is_empty());
        m.insert(Bytes::from("a"), 1);
        m.insert(Bytes::from("b"), 2);
        assert_eq!(m.get(&Bytes::from("a")), Some(1));
        assert_eq!(m.get(&Bytes::from("b")), Some(2));
        assert!(m.contains_key(&Bytes::from("a")));
        assert_eq!(m.len(), 2);
        assert_eq!(m.remove(&Bytes::from("a")), Some(1));
        assert_eq!(m.len(), 1);
        assert!(!m.contains_key(&Bytes::from("a")));
    }

    #[test]
    fn test_sharded_key_map_get_or_insert() {
        let m: ShardedKeyMap<u32> = ShardedKeyMap::new(8);
        let v1 = m.get_or_insert_with(Bytes::from("k"), || 42);
        let v2 = m.get_or_insert_with(Bytes::from("k"), || 99);
        assert_eq!(v1, 42);
        assert_eq!(v2, 42);
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn test_sharded_key_map_keys_and_export() {
        let m: ShardedKeyMap<u32> = ShardedKeyMap::new(4);
        m.insert(Bytes::from("foo"), 1);
        m.insert(Bytes::from("bar"), 2);
        m.insert(Bytes::from("baz"), 3);
        let all = m.keys(None);
        assert_eq!(all.len(), 3);
        let f = m.keys(Some("ba*"));
        assert_eq!(f.len(), 2);
        let mut sum = 0u32;
        m.for_each(|_, v| sum += *v);
        assert_eq!(sum, 6);
        m.clear();
        assert!(m.is_empty());
    }

    #[test]
    fn test_sharded_key_map_concurrent_inserts() {
        let m = Arc::new(ShardedKeyMap::<u64>::new(32));
        let mut handles = Vec::new();
        for t in 0..8 {
            let m = Arc::clone(&m);
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let key = Bytes::from(format!("k-{t}-{i}"));
                    m.insert(key, (t * 1000 + i) as u64);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(m.len(), 800);
    }

    #[test]
    fn test_sharded_key_map_concurrent_mixed_ops() {
        let m = Arc::new(ShardedKeyMap::<u64>::new(16));
        // Seed
        for i in 0..200 {
            m.insert(Bytes::from(format!("seed-{i}")), i);
        }
        let mut handles = Vec::new();
        for t in 0..4 {
            let m = Arc::clone(&m);
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let key = Bytes::from(format!("seed-{}", (t * 50 + i) % 200));
                    let _ = m.get(&key);
                    m.insert(Bytes::from(format!("new-{t}-{i}")), i as u64);
                    if i % 3 == 0 {
                        let _ = m.remove(&Bytes::from(format!("seed-{}", i % 200)));
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // Still consistent: every remaining key is readable
        let keys = m.keys(None);
        for k in &keys {
            assert!(m.get(k).is_some());
        }
        assert!(!keys.is_empty());
    }

    #[test]
    fn test_sharded_key_map_overwrite() {
        let m: ShardedKeyMap<u32> = ShardedKeyMap::new(4);
        m.insert(Bytes::from("k"), 1);
        m.insert(Bytes::from("k"), 2);
        assert_eq!(m.get(&Bytes::from("k")), Some(2));
        assert_eq!(m.len(), 1);
    }
}
