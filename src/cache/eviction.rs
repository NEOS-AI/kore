use crate::entry::SharedEntry;
use crate::error::{Error, Result};
use crate::memory::MemoryCategory;
use bytes::Bytes;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::time;

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
    /// approximately `needed` bytes are freed (string KV pool only).
    pub(super) fn evict_memory(&self, needed: usize) -> Result<()> {
        let policy = self.eviction_policy();
        if policy == EvictionPolicy::NoEviction {
            return Err(Error::OutOfMemory);
        }

        let sample_size = self.eviction_sample_size.load(Ordering::Relaxed).max(1);
        let mut freed = 0;
        let mut empty_rounds = 0;

        while freed < needed {
            let candidates = self.sample_eviction_candidates(policy, sample_size);
            if candidates.is_empty() {
                empty_rounds += 1;
                // No eligible keys (e.g. volatile policy with no TTLs)
                if empty_rounds >= 3 {
                    return Err(Error::OutOfMemory);
                }
                continue;
            }
            empty_rounds = 0;

            let victim = Self::pick_victim(candidates, policy);
            if let Some((key, entry)) = victim {
                tracing::debug!(
                    "Evicting key {:?} policy={} last_access={:?} lfu={} ttl={:?}",
                    key,
                    policy.as_str(),
                    entry.last_access_time(),
                    entry.lfu_freq(),
                    entry.expires_at
                );

                let size = entry.size();
                if self.map.remove(&key).is_some() {
                    freed += size;
                    self.memory_usage.fetch_sub(size, Ordering::Relaxed);
                    self.memory_tracker.deallocate(size, MemoryCategory::Cache);
                    self.stats.incr(&self.stats.evicted_lru);
                    self.touch_watch_key(&key);
                }
            } else {
                return Err(Error::OutOfMemory);
            }
        }

        Ok(())
    }

    fn sample_eviction_candidates(
        &self,
        policy: EvictionPolicy,
        sample_size: usize,
    ) -> Vec<(Bytes, SharedEntry)> {
        let volatile_only = policy.volatile_only();
        // Oversample when filtering to volatile keys.
        let draw = if volatile_only {
            (sample_size * 4).max(sample_size)
        } else {
            sample_size
        };

        let mut out = Vec::with_capacity(sample_size);
        // A few draws if first batch is sparse (volatile filter).
        for _ in 0..4 {
            for (k, e) in self.map.get_n_random(draw) {
                if e.is_expired() {
                    continue;
                }
                if volatile_only && e.expires_at.is_none() {
                    continue;
                }
                out.push((k, e));
                if out.len() >= sample_size {
                    return out;
                }
            }
            if !volatile_only || out.len() >= sample_size {
                break;
            }
        }
        out
    }

    fn pick_victim(
        candidates: Vec<(Bytes, SharedEntry)>,
        policy: EvictionPolicy,
    ) -> Option<(Bytes, SharedEntry)> {
        use rand::seq::SliceRandom;
        match policy {
            EvictionPolicy::NoEviction => None,
            EvictionPolicy::AllKeysLru | EvictionPolicy::VolatileLru => candidates
                .into_iter()
                .min_by_key(|(_, e)| e.last_access_time()),
            EvictionPolicy::AllKeysLfu | EvictionPolicy::VolatileLfu => {
                candidates.into_iter().min_by_key(|(_, e)| e.lfu_freq())
            }
            EvictionPolicy::AllKeysRandom | EvictionPolicy::VolatileRandom => {
                let mut rng = rand::thread_rng();
                candidates.choose(&mut rng).cloned()
            }
            EvictionPolicy::VolatileTtl => candidates.into_iter().min_by_key(|(_, e)| {
                e.expires_at
                    .unwrap_or_else(|| Instant::now() + Duration::from_secs(u64::MAX / 4))
            }),
        }
    }

    /// Background task: Redis-style *active expire sampling* (not full-shard retain).
    ///
    /// Runs ~10 Hz with a short time budget so large keyspaces never stall on a
    /// full `HashMap::retain` pass. Manual [`Self::sweep`] still does a full scan.
    pub(super) async fn background_sweep(&self) {
        // 100ms tick ≈ Redis hz=10 active-expire cadence
        let mut interval = time::interval(Duration::from_millis(100));

        loop {
            interval.tick().await;

            if !self.autosweep_enabled.load(Ordering::Relaxed) {
                continue;
            }

            // Sampling cycle with 1ms budget (scaled by load via continue-ratio).
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
    /// Prefer [`Self::active_expire`] for incremental cleanup.
    pub fn sweep(&self) -> usize {
        let result = self.map.sweep_expired();
        if result.count > 0 {
            self.apply_expire_accounting(result.count, result.bytes_freed);
        }
        result.count
    }

    /// Run one Redis-style active expire cycle (sampling). Returns keys deleted.
    pub fn active_expire(&self) -> usize {
        let result = self.map.active_expire_default();
        if result.count > 0 {
            self.apply_expire_accounting(result.count, result.bytes_freed);
        }
        result.count
    }

    /// Active expire with explicit parameters (for tests / tuning).
    pub fn active_expire_cycle(
        &self,
        samples_per_pass: usize,
        max_passes: usize,
        time_budget: Duration,
    ) -> crate::hashmap::ActiveExpireResult {
        let result = self
            .map
            .active_expire_cycle(samples_per_pass, max_passes, time_budget);
        if result.count > 0 {
            self.apply_expire_accounting(result.count, result.bytes_freed);
        }
        result
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
