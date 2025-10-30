use crate::entry::{Entry, LoadOptions, SharedEntry, StoreOptions};
use crate::error::{Error, Result};
use crate::hashmap::ShardedHashMap;
use crate::stats::Stats;
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time;

/// The main cache structure
pub struct Cache {
    /// Sharded hashmap for storing entries
    map: ShardedHashMap,
    /// Statistics
    pub stats: Arc<Stats>,
    /// Maximum memory in bytes
    pub max_memory: usize,
    /// Current memory usage
    memory_usage: AtomicUsize,
    /// Maximum entry size in bytes
    max_entry_size: AtomicUsize,
    /// Enable eviction when memory is full
    evict_enabled: AtomicBool,
    /// Enable automatic sweeping
    autosweep_enabled: AtomicBool,
}

impl Cache {
    pub fn new(num_shards: usize, max_memory: usize) -> Arc<Self> {
        Self::new_with_sweep(num_shards, max_memory, 500 * 1024 * 1024, true)
    }

    pub fn new_with_sweep(num_shards: usize, max_memory: usize, max_entry_size: usize, start_sweep: bool) -> Arc<Self> {
        let cache = Arc::new(Self {
            map: ShardedHashMap::new(num_shards, 1024),
            stats: Arc::new(Stats::new()),
            max_memory,
            memory_usage: AtomicUsize::new(0),
            max_entry_size: AtomicUsize::new(max_entry_size),
            evict_enabled: AtomicBool::new(true),
            autosweep_enabled: AtomicBool::new(true),
        });

        // Start background sweep task if requested
        if start_sweep {
            let cache_clone = cache.clone();
            tokio::spawn(async move {
                cache_clone.background_sweep().await;
            });
        }

        cache
    }

    /// Store a key-value pair
    pub fn store(
        &self,
        key: Bytes,
        value: Bytes,
        opts: StoreOptions,
    ) -> Result<Option<SharedEntry>> {
        // Check entry size
        let entry_size = key.len() + value.len();
        let max_entry_size = self.max_entry_size.load(Ordering::Relaxed);
        if entry_size > max_entry_size {
            self.stats.incr(&self.stats.store_too_large);
            return Err(Error::EntryTooLarge);
        }

        // Check memory before insert
        let current_memory = self.memory_usage.load(Ordering::Relaxed);

        // If key already exists, we need to account for the memory that will be freed
        let existing_size = self.map.get(&key)
            .map(|e| e.size())
            .unwrap_or(0);

        let net_memory_change = entry_size.saturating_sub(existing_size);

        if current_memory + net_memory_change > self.max_memory {
            if self.evict_enabled.load(Ordering::Relaxed) {
                // Try to evict entries to make space
                self.evict_lru(net_memory_change)?;
            } else {
                self.stats.incr(&self.stats.store_no_memory);
                return Err(Error::OutOfMemory);
            }
        }

        // Handle NX (only if not exists)
        if opts.nx {
            if let Some(existing) = self.map.get(&key) {
                if !existing.is_expired() {
                    return if opts.get {
                        Ok(Some(existing))
                    } else {
                        Ok(None)
                    };
                }
            }
        }

        // Handle XX (only if exists)
        if opts.xx {
            match self.map.get(&key) {
                Some(existing) if !existing.is_expired() => {
                    // OK to proceed
                }
                _ => return Ok(None),
            }
        }

        // Get old value if requested
        let old_value = if opts.get {
            self.map.get(&key).filter(|e| !e.is_expired())
        } else {
            None
        };

        // Handle CAS (compare-and-swap)
        if let Some(expected_cas) = opts.cas {
            match self.map.get(&key) {
                Some(existing) if !existing.is_expired() => {
                    if existing.cas != expected_cas {
                        self.stats.incr(&self.stats.cas_badval);
                        return Err(Error::CasMismatch);
                    }
                }
                _ => {
                    self.stats.incr(&self.stats.cas_misses);
                    return Err(Error::KeyNotFound);
                }
            }
            self.stats.incr(&self.stats.cas_hits);
        }

        // Create new entry
        let mut entry = Entry::new(key.clone(), value);

        // Set expiration
        if let Some(ttl_ms) = opts.ttl_ms {
            entry = entry.with_expiration(Duration::from_millis(ttl_ms));
        } else if let Some(exat_ms) = opts.exat_ms {
            let expires_at = Instant::now() + Duration::from_millis(exat_ms);
            entry = entry.with_expiration_at(expires_at);
        } else if opts.keepttl {
            // Keep existing TTL if present
            if let Some(existing) = self.map.get(&key) {
                entry.expires_at = existing.expires_at;
            }
        }

        // Set flags
        entry = entry.with_flags(opts.flags);

        // Set CAS value
        let cas = self.map.next_cas(&key);
        entry = entry.with_cas(cas);

        let entry = Arc::new(entry);

        // Insert into map
        let old_entry = self.map.insert(key, entry);

        // Update memory usage
        if let Some(ref old) = old_entry {
            self.memory_usage
                .fetch_sub(old.size(), Ordering::Relaxed);
        }
        self.memory_usage
            .fetch_add(entry_size, Ordering::Relaxed);

        // Update stats
        self.stats.incr(&self.stats.cmd_set);

        Ok(old_value)
    }

    /// Load a key
    pub fn load(&self, key: &Bytes, _opts: LoadOptions) -> Result<Option<SharedEntry>> {
        self.stats.incr(&self.stats.cmd_get);

        match self.map.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    // Remove expired entry
                    self.map.remove(key);
                    self.memory_usage
                        .fetch_sub(entry.size(), Ordering::Relaxed);
                    self.stats.incr(&self.stats.evicted_expired);
                    self.stats.incr(&self.stats.misses);
                    Ok(None)
                } else {
                    self.stats.incr(&self.stats.hits);
                    Ok(Some(entry))
                }
            }
            None => {
                self.stats.incr(&self.stats.misses);
                Ok(None)
            }
        }
    }

    /// Delete a key
    pub fn delete(&self, key: &Bytes) -> Result<bool> {
        self.stats.incr(&self.stats.cmd_del);

        match self.map.remove(key) {
            Some(entry) => {
                self.memory_usage
                    .fetch_sub(entry.size(), Ordering::Relaxed);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Delete multiple keys
    pub fn delete_many(&self, keys: &[Bytes]) -> Result<usize> {
        let mut count = 0;
        for key in keys {
            if self.delete(key)? {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Check if key exists
    pub fn exists(&self, key: &Bytes) -> bool {
        self.map
            .get(key)
            .map(|e| !e.is_expired())
            .unwrap_or(false)
    }

    /// Get database size
    pub fn dbsize(&self) -> usize {
        self.map.len()
    }

    /// Get memory usage
    pub fn memory_usage(&self) -> usize {
        self.memory_usage.load(Ordering::Relaxed)
    }

    /// Clear all entries
    pub fn flush(&self) {
        self.map.clear();
        self.memory_usage.store(0, Ordering::Relaxed);
    }

    /// Get all keys matching a pattern
    pub fn keys(&self, pattern: Option<&str>) -> Vec<Bytes> {
        self.map.keys(pattern)
    }

    /// Atomic increment
    pub fn incr(&self, key: &Bytes, delta: i64) -> Result<i64> {
        self.stats.incr(&self.stats.cmd_incr);

        // Get current value
        let current = match self.map.get(key) {
            Some(entry) if !entry.is_expired() => {
                // Parse as integer
                let val_str = std::str::from_utf8(&entry.value)
                    .map_err(|_| Error::InvalidArgument("value is not a valid integer".into()))?;
                val_str
                    .parse::<i64>()
                    .map_err(|_| Error::InvalidArgument("value is not a valid integer".into()))?
            }
            _ => 0,
        };

        let new_value = current + delta;
        let value_bytes = Bytes::from(new_value.to_string());

        // Store new value
        self.store(
            key.clone(),
            value_bytes,
            StoreOptions::default(),
        )?;

        Ok(new_value)
    }

    /// Atomic decrement
    pub fn decr(&self, key: &Bytes, delta: i64) -> Result<i64> {
        self.stats.incr(&self.stats.cmd_decr);
        self.incr(key, -delta)
    }

    /// Set expiration on a key (in milliseconds)
    pub fn expire(&self, key: &Bytes, ttl_ms: u64) -> Result<bool> {
        match self.map.get(key) {
            Some(entry) if !entry.is_expired() => {
                // Create new entry with updated expiration
                let mut new_entry = (*entry).clone();
                new_entry.expires_at = Some(Instant::now() + Duration::from_millis(ttl_ms));
                let new_entry = Arc::new(new_entry);

                self.map.insert(key.clone(), new_entry);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Get TTL in milliseconds (-1 = no expiration, -2 = expired/not found)
    pub fn ttl(&self, key: &Bytes) -> i64 {
        match self.map.get(key) {
            Some(entry) if !entry.is_expired() => entry.ttl_millis().unwrap_or(-1),
            _ => -2,
        }
    }

    /// Evict entries using 2-random algorithm.
    /// When memory is full, randomly pick two entries and evict the older one.
    fn evict_lru(&self, needed: usize) -> Result<()> {
        let mut freed = 0;

        while freed < needed {
            let candidates = self.map.get_two_random();

            if candidates.is_empty() {
                return Err(Error::OutOfMemory);
            }

            // Evict the oldest one
            let oldest = candidates
                .into_iter()
                .min_by_key(|(_, entry)| entry.created_at);

            if let Some((key, entry)) = oldest {
                // log eviction
                tracing::debug!("Evicting key {:?} (created at {:?})", key, entry.created_at);

                self.map.remove(&key);  // Remove from map
                freed += entry.size();  // Keep track of freed memory

                // Update memory usage
                self.memory_usage
                    .fetch_sub(entry.size(), Ordering::Relaxed);
                self.stats.incr(&self.stats.evicted_lru);
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Background task to sweep expired entries
    async fn background_sweep(&self) {
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

    /// Get maximum entry size
    pub fn get_max_entry_size(&self) -> usize {
        self.max_entry_size.load(Ordering::Relaxed)
    }

    /// Set maximum entry size with validation
    pub fn set_max_entry_size(&self, size: usize) -> Result<()> {
        // Minimum 1KB
        if size < 1024 {
            return Err(Error::InvalidArgument("max entry size too small (minimum 1KB)".into()));
        }
        // Cannot exceed max memory
        if size > self.max_memory {
            return Err(Error::InvalidArgument("max entry size cannot exceed max memory".into()));
        }
        self.max_entry_size.store(size, Ordering::Relaxed);
        Ok(())
    }
}
