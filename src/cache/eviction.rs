use crate::error::{Error, Result};
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time;

use super::Cache;

impl Cache {
    /// Evict entries using approximated LRU algorithm.
    /// Samples N random entries (default 5) and evicts the least recently used one.
    /// This is much better than 2-random and is similar to what Redis uses.
    pub(super) fn evict_lru(&self, needed: usize) -> Result<()> {
        let sample_size = self.eviction_sample_size.load(Ordering::Relaxed);
        let mut freed = 0;

        while freed < needed {
            // Get N random candidates for eviction
            let candidates = self.map.get_n_random(sample_size);

            if candidates.is_empty() {
                return Err(Error::OutOfMemory);
            }

            // Evict the least recently used one
            let lru = candidates
                .into_iter()
                .min_by_key(|(_, entry)| entry.last_access_time());

            if let Some((key, entry)) = lru {
                // log eviction
                tracing::debug!(
                    "Evicting key {:?} (last accessed at {:?})",
                    key,
                    entry.last_access_time()
                );

                self.map.remove(&key); // Remove from map
                freed += entry.size(); // Keep track of freed memory

                // Update memory usage
                self.memory_usage.fetch_sub(entry.size(), Ordering::Relaxed);
                self.stats.incr(&self.stats.evicted_lru);
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Background task to sweep expired entries
    pub(super) async fn background_sweep(&self) {
        let mut interval = time::interval(Duration::from_secs(1));

        loop {
            interval.tick().await;

            if !self.autosweep_enabled.load(Ordering::Relaxed) {
                continue;
            }

            // Sweep expired entries
            let removed = self.map.sweep_expired();

            if removed > 0 {
                self.stats
                    .evicted_expired
                    .fetch_add(removed as u64, Ordering::Relaxed);
                tracing::debug!("Swept {} expired entries", removed);
            }
        }
    }

    /// Manually trigger a sweep
    pub fn sweep(&self) -> usize {
        let removed = self.map.sweep_expired();
        if removed > 0 {
            self.stats
                .evicted_expired
                .fetch_add(removed as u64, Ordering::Relaxed);
        }
        removed
    }

    /// Set autosweep enabled
    pub fn set_autosweep(&self, enabled: bool) {
        self.autosweep_enabled.store(enabled, Ordering::Relaxed);
    }

    /// Set eviction enabled
    pub fn set_evict(&self, enabled: bool) {
        self.evict_enabled.store(enabled, Ordering::Relaxed);
    }
}
