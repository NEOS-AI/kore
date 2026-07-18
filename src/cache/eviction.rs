use crate::error::{Error, Result};
use crate::memory::MemoryCategory;
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::time;

use super::storage::KeyType;
use super::Cache;

/// Redis-compatible maxmemory eviction policies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EvictionPolicy {
    /// Never evict; return OOM on write when over maxmemory.
    NoEviction = 0,
    /// Evict any key using approximated LRU.
    AllKeysLru = 1,
    /// Evict only keys with an expire set, approximated LRU.
    VolatileLru = 2,
    /// Evict any key using approximated LFU (access counter).
    AllKeysLfu = 3,
    /// Evict only keys with expire, approximated LFU.
    VolatileLfu = 4,
    /// Evict any key at random.
    AllKeysRandom = 5,
    /// Evict only keys with expire, at random.
    VolatileRandom = 6,
    /// Evict keys with expire, preferring soonest TTL.
    VolatileTtl = 7,
}

impl EvictionPolicy {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::NoEviction,
            1 => Self::AllKeysLru,
            2 => Self::VolatileLru,
            3 => Self::AllKeysLfu,
            4 => Self::VolatileLfu,
            5 => Self::AllKeysRandom,
            6 => Self::VolatileRandom,
            7 => Self::VolatileTtl,
            _ => Self::AllKeysLru,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoEviction => "noeviction",
            Self::AllKeysLru => "allkeys-lru",
            Self::VolatileLru => "volatile-lru",
            Self::AllKeysLfu => "allkeys-lfu",
            Self::VolatileLfu => "volatile-lfu",
            Self::AllKeysRandom => "allkeys-random",
            Self::VolatileRandom => "volatile-random",
            Self::VolatileTtl => "volatile-ttl",
        }
    }

    pub fn parse(s: &str) -> std::result::Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "noeviction" => Ok(Self::NoEviction),
            "allkeys-lru" => Ok(Self::AllKeysLru),
            "volatile-lru" => Ok(Self::VolatileLru),
            "allkeys-lfu" => Ok(Self::AllKeysLfu),
            "volatile-lfu" => Ok(Self::VolatileLfu),
            "allkeys-random" => Ok(Self::AllKeysRandom),
            "volatile-random" => Ok(Self::VolatileRandom),
            "volatile-ttl" => Ok(Self::VolatileTtl),
            other => Err(format!(
                "Invalid maxmemory policy '{}'. Valid: noeviction, allkeys-lru, \
                 volatile-lru, allkeys-lfu, volatile-lfu, allkeys-random, \
                 volatile-random, volatile-ttl",
                other
            )),
        }
    }

    fn volatile_only(self) -> bool {
        matches!(
            self,
            Self::VolatileLru | Self::VolatileLfu | Self::VolatileRandom | Self::VolatileTtl
        )
    }

    fn allkeys(self) -> bool {
        matches!(
            self,
            Self::AllKeysLru | Self::AllKeysLfu | Self::AllKeysRandom
        )
    }
}

/// Unified eviction candidate across string, typed keyspaces, and search docs.
#[derive(Clone)]
struct EvictCandidate {
    key: Bytes,
    key_type: KeyType,
    /// Approximate bytes freed if this key is removed.
    size: usize,
    last_access: Instant,
    lfu_freq: u64,
    expires_at: Option<Instant>,
    /// When set, `key` is a search document id in this index (not a keyspace key).
    /// Eviction removes the index entry only (frees `MemoryCategory::Search`).
    search_index: Option<String>,
}

impl Cache {
    pub fn eviction_policy(&self) -> EvictionPolicy {
        EvictionPolicy::from_u8(self.eviction_policy.load(Ordering::Relaxed))
    }

    /// Set maxmemory-policy. Also syncs the legacy `evict_enabled` flag.
    pub fn set_eviction_policy(&self, policy: EvictionPolicy) {
        self.eviction_policy
            .store(policy as u8, Ordering::Relaxed);
        self.evict_enabled
            .store(policy != EvictionPolicy::NoEviction, Ordering::Relaxed);
    }

    pub fn set_eviction_policy_str(&self, s: &str) -> Result<()> {
        let policy = EvictionPolicy::parse(s).map_err(Error::InvalidArgument)?;
        self.set_eviction_policy(policy);
        Ok(())
    }

    /// Whether writes may attempt eviction under memory pressure.
    pub fn eviction_allowed(&self) -> bool {
        self.eviction_policy() != EvictionPolicy::NoEviction
            && self.evict_enabled.load(Ordering::Relaxed)
    }

    /// Evict entries according to the configured maxmemory-policy until
    /// approximately `needed` bytes are freed.
    ///
    /// Samples **all key types** (string, hash, list, set, zset, geo, stream)
    /// and **search index documents** under `allkeys-*` policies. Volatile
    /// policies sample string keys with expire and typed keys that have a TTL
    /// (Batch AE). Search docs are not volatile victims.
    pub(super) fn evict_memory(&self, needed: usize) -> Result<()> {
        self.evict_memory_excluding(needed, None)
    }

    /// Like [`evict_memory`], but never selects `exclude_search_doc` as a
    /// search-index victim (avoids double-free while re-indexing that doc).
    pub(super) fn evict_memory_excluding(
        &self,
        needed: usize,
        exclude_search_doc: Option<&Bytes>,
    ) -> Result<()> {
        let policy = self.eviction_policy();
        if policy == EvictionPolicy::NoEviction {
            return Err(Error::OutOfMemory);
        }

        let sample_size = self.eviction_sample_size.load(Ordering::Relaxed).max(1);
        let mut freed = 0;
        let mut empty_rounds = 0;

        while freed < needed {
            let candidates =
                self.sample_eviction_candidates(policy, sample_size, exclude_search_doc);
            if candidates.is_empty() {
                empty_rounds += 1;
                if empty_rounds >= 3 {
                    return Err(Error::OutOfMemory);
                }
                continue;
            }
            empty_rounds = 0;

            let victim = Self::pick_victim(candidates, policy);
            if let Some(c) = victim {
                tracing::debug!(
                    "Evicting key {:?} type={:?} policy={} size={} lfu={} ttl={:?} search={:?}",
                    c.key,
                    c.key_type,
                    policy.as_str(),
                    c.size,
                    c.lfu_freq,
                    c.expires_at,
                    c.search_index
                );

                if let Some(bytes) = self.evict_candidate(&c) {
                    freed += bytes;
                    self.stats.incr(&self.stats.evicted_lru);
                    // Search-only victims are not WATCH keys.
                    if c.search_index.is_none() {
                        self.touch_watch_key(&c.key);
                    }
                }
            } else {
                return Err(Error::OutOfMemory);
            }
        }

        Ok(())
    }

    /// Remove a sampled victim and return bytes freed (0 if race lost).
    fn evict_candidate(&self, c: &EvictCandidate) -> Option<usize> {
        if let Some(ref index_name) = c.search_index {
            return self.evict_search_document(index_name, &c.key);
        }
        match c.key_type {
            KeyType::String => {
                if let Some(entry) = self.map.remove(&c.key) {
                    let size = entry.size();
                    self.memory_usage.fetch_sub(size, Ordering::Relaxed);
                    self.memory_tracker.deallocate(size, MemoryCategory::Cache);
                    self.auto_remove_from_indices(&c.key);
                    Some(size)
                } else {
                    None
                }
            }
            KeyType::ZSet => {
                if self.remove_sorted_set(&c.key) {
                    self.auto_remove_from_indices(&c.key);
                    Some(c.size)
                } else {
                    None
                }
            }
            KeyType::Geo => {
                if self.remove_geo_set(&c.key) {
                    self.auto_remove_from_indices(&c.key);
                    Some(c.size)
                } else {
                    None
                }
            }
            KeyType::Hash => {
                if self.remove_hash(&c.key) {
                    self.auto_remove_from_indices(&c.key);
                    Some(c.size)
                } else {
                    None
                }
            }
            KeyType::List => {
                if self.remove_list(&c.key) {
                    self.auto_remove_from_indices(&c.key);
                    Some(c.size)
                } else {
                    None
                }
            }
            KeyType::Set => {
                if self.remove_set(&c.key) {
                    self.auto_remove_from_indices(&c.key);
                    Some(c.size)
                } else {
                    None
                }
            }
            KeyType::Stream => {
                if self.remove_stream(&c.key) {
                    self.auto_remove_from_indices(&c.key);
                    Some(c.size)
                } else {
                    None
                }
            }
            KeyType::None => None,
        }
    }

    fn sample_eviction_candidates(
        &self,
        policy: EvictionPolicy,
        sample_size: usize,
        exclude_search_doc: Option<&Bytes>,
    ) -> Vec<EvictCandidate> {
        let volatile_only = policy.volatile_only();
        let draw = if volatile_only {
            (sample_size * 4).max(sample_size)
        } else {
            sample_size
        };

        let mut out = Vec::with_capacity(sample_size.saturating_mul(2));

        // --- String keys (LRU/LFU/TTL metadata available) ---
        for _ in 0..4 {
            for (k, e) in self.map.get_n_random(draw) {
                if e.is_expired() {
                    continue;
                }
                if volatile_only && e.expires_at.is_none() {
                    continue;
                }
                let decay = self.lfu_decay_time.load(Ordering::Relaxed);
                out.push(EvictCandidate {
                    key: k,
                    key_type: KeyType::String,
                    size: e.size(),
                    last_access: e.last_access_time(),
                    lfu_freq: e.lfu_freq(decay),
                    expires_at: e.expires_at,
                    search_index: None,
                });
                if out.len() >= sample_size && volatile_only {
                    return out;
                }
            }
            if volatile_only {
                if out.len() >= sample_size {
                    break;
                }
            } else {
                break;
            }
        }

        // --- Typed keys: allkeys always; volatile only when they have a TTL ---
        {
            let per_type = (sample_size / 3).max(1);
            let push_typed = |out: &mut Vec<EvictCandidate>,
                              k: Bytes,
                              kt: KeyType,
                              size: usize,
                              expires_at: Option<Instant>| {
                if volatile_only && expires_at.is_none() {
                    return;
                }
                // Skip already-expired typed keys (lazy purge will clean later).
                if expires_at.map(|e| e <= Instant::now()).unwrap_or(false) {
                    return;
                }
                out.push(typed_candidate(k, kt, size, expires_at));
            };

            // Sharded maps (zset / geo)
            for (k, z) in self.sorted_sets.get_n_random(per_type) {
                let size =
                    crate::memory::estimate_keyed_object(k.len(), z.read().memory_size());
                let exp = self.typed_expires_at(&k);
                push_typed(&mut out, k, KeyType::ZSet, size, exp);
            }
            for (k, g) in self.geo_sets.get_n_random(per_type) {
                let size = crate::memory::estimate_keyed_object(
                    k.len(),
                    g.read().memory_usage(),
                );
                let exp = self.typed_expires_at(&k);
                push_typed(&mut out, k, KeyType::Geo, size, exp);
            }

            // Global maps (hash / list / set / stream)
            sample_map_keys(&self.hashes, per_type, |k, h| {
                let size =
                    crate::memory::estimate_keyed_object(k.len(), h.read().memory_size());
                let exp = self.typed_expires_at(&k);
                push_typed(&mut out, k, KeyType::Hash, size, exp);
            });
            sample_map_keys(&self.lists, per_type, |k, l| {
                let size =
                    crate::memory::estimate_keyed_object(k.len(), l.read().memory_size());
                let exp = self.typed_expires_at(&k);
                push_typed(&mut out, k, KeyType::List, size, exp);
            });
            sample_map_keys(&self.sets, per_type, |k, s| {
                let size =
                    crate::memory::estimate_keyed_object(k.len(), s.read().memory_size());
                let exp = self.typed_expires_at(&k);
                push_typed(&mut out, k, KeyType::Set, size, exp);
            });
            sample_map_keys(&self.streams, per_type, |k, s| {
                let size =
                    crate::memory::estimate_keyed_object(k.len(), s.read().memory_size());
                let exp = self.typed_expires_at(&k);
                push_typed(&mut out, k, KeyType::Stream, size, exp);
            });

            // Search index documents only under allkeys (no TTL).
            if policy.allkeys() {
                let search_n = per_type.max(2);
                for (index_name, doc_id, size) in self
                    .search_index_manager
                    .sample_documents_for_eviction(search_n, exclude_search_doc)
                {
                    out.push(EvictCandidate {
                        key: doc_id,
                        key_type: KeyType::None,
                        size,
                        last_access: cold_instant(),
                        lfu_freq: 0,
                        expires_at: None,
                        search_index: Some(index_name),
                    });
                }
            }
        }

        // Cap sample for pick_victim work
        if out.len() > sample_size.saturating_mul(2) {
            use rand::seq::SliceRandom;
            let mut rng = rand::thread_rng();
            out.shuffle(&mut rng);
            out.truncate(sample_size.saturating_mul(2));
        }

        out
    }

    /// Remove one document from a search index and free its tracked memory.
    fn evict_search_document(&self, index_name: &str, doc_id: &Bytes) -> Option<usize> {
        let index = self.search_index_manager.get_index(index_name)?;
        let mut guard = index.write();
        let fields = guard.get_document_data(doc_id)?.clone();
        let size = crate::search_index::SearchIndex::document_approx_size(doc_id, &fields);
        guard.remove_document(doc_id);
        drop(guard);
        if size > 0 {
            self.memory_tracker
                .deallocate(size, MemoryCategory::Search);
        }
        Some(size)
    }

    fn pick_victim(
        candidates: Vec<EvictCandidate>,
        policy: EvictionPolicy,
    ) -> Option<EvictCandidate> {
        use rand::seq::SliceRandom;
        match policy {
            EvictionPolicy::NoEviction => None,
            EvictionPolicy::AllKeysLru | EvictionPolicy::VolatileLru => {
                // Oldest last_access wins. Typed keys use Instant::EPOCH (idle),
                // so they compete as cold and free non-string memory under pressure.
                candidates
                    .into_iter()
                    .min_by_key(|c| (c.last_access, std::cmp::Reverse(c.size)))
            }
            EvictionPolicy::AllKeysLfu | EvictionPolicy::VolatileLfu => {
                // Lowest frequency first; typed keys score 0 (cold).
                candidates
                    .into_iter()
                    .min_by_key(|c| (c.lfu_freq, std::cmp::Reverse(c.size)))
            }
            EvictionPolicy::AllKeysRandom | EvictionPolicy::VolatileRandom => {
                let mut rng = rand::thread_rng();
                candidates.choose(&mut rng).cloned()
            }
            EvictionPolicy::VolatileTtl => candidates.into_iter().min_by_key(|c| {
                c.expires_at
                    .unwrap_or_else(|| Instant::now() + Duration::from_secs(u64::MAX / 4))
            }),
        }
    }

    /// Background task: Redis-style *active expire sampling* (not full-shard retain).
    pub(super) async fn background_sweep(&self) {
        let mut interval = time::interval(Duration::from_millis(100));

        loop {
            interval.tick().await;

            if !self.autosweep_enabled.load(Ordering::Relaxed) {
                continue;
            }

            // Hold cycle lock for the whole body so `with_autosweep_paused` can
            // wait and exclude concurrent expire during keyspace replace.
            let _cycle = self.autosweep_cycle_lock.lock();
            if !self.autosweep_enabled.load(Ordering::Relaxed) {
                continue;
            }

            let result = self.map.active_expire_cycle(
                crate::hashmap::ACTIVE_EXPIRE_SAMPLES_PER_PASS,
                crate::hashmap::ACTIVE_EXPIRE_MAX_PASSES,
                Duration::from_millis(1),
            );

            if result.count > 0 {
                self.apply_expire_accounting(result.count, result.bytes_freed);
                tracing::debug!(
                    "Active-expire: removed {} keys ({} bytes) sampled={} passes={}",
                    result.count,
                    result.bytes_freed,
                    result.sampled,
                    result.passes
                );
            }
            // Typed keys with TTL (hash/list/set/zset/geo/stream).
            let _ = self.active_expire_typed(crate::hashmap::ACTIVE_EXPIRE_SAMPLES_PER_PASS);
        }
    }

    fn apply_expire_accounting(&self, count: usize, bytes_freed: usize) {
        if bytes_freed > 0 {
            self.memory_usage
                .fetch_sub(bytes_freed, Ordering::Relaxed);
            self.memory_tracker
                .deallocate(bytes_freed, MemoryCategory::Cache);
        }
        self.stats
            .evicted_expired
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    /// Manually trigger a **full** sweep of all shards (admin / SWEEP command).
    pub fn sweep(&self) -> usize {
        let result = self.map.sweep_expired();
        if result.count > 0 {
            self.apply_expire_accounting(result.count, result.bytes_freed);
        }
        // Full typed expire pass: delete every past-due typed key.
        let typed = self.sweep_typed_expired();
        result.count + typed
    }

    /// Run one Redis-style active expire cycle (sampling). Returns keys deleted.
    pub fn active_expire(&self) -> usize {
        let result = self.map.active_expire_default();
        if result.count > 0 {
            self.apply_expire_accounting(result.count, result.bytes_freed);
        }
        result.count + self.active_expire_typed(crate::hashmap::ACTIVE_EXPIRE_SAMPLES_PER_PASS)
    }

    /// Active expire with explicit parameters (for tests / tuning).
    pub fn active_expire_cycle(
        &self,
        samples_per_pass: usize,
        max_passes: usize,
        time_budget: Duration,
    ) -> crate::hashmap::ActiveExpireResult {
        let mut result = self
            .map
            .active_expire_cycle(samples_per_pass, max_passes, time_budget);
        if result.count > 0 {
            self.apply_expire_accounting(result.count, result.bytes_freed);
        }
        let typed = self.active_expire_typed(samples_per_pass);
        result.count += typed;
        result
    }

    /// Delete all typed keys whose expire Instant is in the past.
    fn sweep_typed_expired(&self) -> usize {
        let now = Instant::now();
        let expired: Vec<Bytes> = self
            .typed_expires
            .read()
            .iter()
            .filter(|(_, exp)| **exp <= now)
            .map(|(k, _)| k.clone())
            .collect();
        let mut count = 0usize;
        for key in expired {
            // purge_typed_if_expired re-checks and deletes without cmd_del bump.
            if self.purge_typed_if_expired(&key) {
                count += 1;
                self.stats
                    .evicted_expired
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        count
    }

    /// Set autosweep enabled
    pub fn set_autosweep(&self, enabled: bool) {
        self.autosweep_enabled.store(enabled, Ordering::Relaxed);
    }

    /// Set eviction enabled (legacy). Maps to `allkeys-lru` / `noeviction`.
    pub fn set_evict(&self, enabled: bool) {
        if enabled {
            if self.eviction_policy() == EvictionPolicy::NoEviction {
                self.set_eviction_policy(EvictionPolicy::AllKeysLru);
            } else {
                self.evict_enabled.store(true, Ordering::Relaxed);
            }
        } else {
            self.set_eviction_policy(EvictionPolicy::NoEviction);
        }
    }
}

/// Approximate "never accessed" for typed keys without LRU metadata.
fn cold_instant() -> Instant {
    Instant::now()
        .checked_sub(Duration::from_secs(86400 * 365 * 10))
        .unwrap_or_else(Instant::now)
}

fn typed_candidate(
    key: Bytes,
    key_type: KeyType,
    size: usize,
    expires_at: Option<Instant>,
) -> EvictCandidate {
    // Typed keys lack LRU/LFU access metadata: treat as cold for approximated LRU.
    EvictCandidate {
        key,
        key_type,
        size,
        last_access: cold_instant(),
        lfu_freq: 0,
        expires_at,
        search_index: None,
    }
}

fn sample_map_keys<V, F>(map: &RwLock<HashMap<Bytes, V>>, n: usize, mut push: F)
where
    V: Clone,
    F: FnMut(Bytes, V),
{
    use rand::Rng;
    use std::collections::HashSet;

    let guard = map.read();
    let len = guard.len();
    if len == 0 || n == 0 {
        return;
    }
    let mut rng = rand::thread_rng();
    let mut seen = HashSet::new();
    let attempts = n.saturating_mul(5).max(n);
    for _ in 0..attempts {
        if seen.len() >= n {
            break;
        }
        let idx = rng.gen_range(0..len);
        if let Some((k, v)) = guard.iter().nth(idx) {
            if seen.insert(k.clone()) {
                push(k.clone(), v.clone());
            }
        }
    }
}
