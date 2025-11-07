use crate::entry::{Entry, LoadOptions, SharedEntry, StoreOptions};
use crate::error::{Error, Result};
use crate::memory::MemoryCategory;
use bytes::Bytes;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::Cache;

impl Cache {
    /// Store a key-value pair
    pub fn store(
        &self,
        key: Bytes,
        value: Bytes,
        opts: StoreOptions,
    ) -> Result<Option<SharedEntry>> {
        // Check entry size (including struct overhead)
        let entry_size = key.len() + value.len() + std::mem::size_of::<Entry>();
        let max_entry_size = self.max_entry_size.load(Ordering::Relaxed);
        if entry_size > max_entry_size {
            self.stats.incr(&self.stats.store_too_large);
            return Err(Error::EntryTooLarge);
        }

        // Check memory before insert
        let current_memory = self.memory_usage.load(Ordering::Relaxed);

        // If key already exists, we need to account for the memory that will be freed
        let existing_size = self.map.get(&key).map(|e| e.size()).unwrap_or(0);

        let net_memory_change = entry_size.saturating_sub(existing_size);

        // Check MemoryTracker for Cache category
        if !self.memory_tracker.can_allocate(net_memory_change, MemoryCategory::Cache) {
            if self.evict_enabled.load(Ordering::Relaxed) {
                // Try to evict entries to make space
                self.evict_lru(net_memory_change)?;
            } else {
                self.stats.incr(&self.stats.store_no_memory);
                return Err(Error::OutOfMemory);
            }
        }

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
                    // Key exists, return the existing value to indicate failure
                    return Ok(Some(existing));
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
            let old_size = old.size();
            self.memory_usage.fetch_sub(old_size, Ordering::Relaxed);
            self.memory_tracker.deallocate(old_size, MemoryCategory::Cache);
        }
        
        // Allocate new memory
        if !self.memory_tracker.allocate(entry_size, MemoryCategory::Cache) {
            return Err(Error::OutOfMemory);
        }
        self.memory_usage.fetch_add(entry_size, Ordering::Relaxed);

        // Update stats
        self.stats.incr(&self.stats.cmd_set);

        Ok(old_value)
    }

    /// Load a key
    pub fn load(&self, key: &Bytes, opts: LoadOptions) -> Result<Option<SharedEntry>> {
        self.stats.incr(&self.stats.cmd_get);

        match self.map.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    // Remove expired entry
                    self.map.remove(key);
                    self.memory_usage.fetch_sub(entry.size(), Ordering::Relaxed);
                    self.stats.incr(&self.stats.evicted_expired);
                    self.stats.incr(&self.stats.misses);
                    Ok(None)
                } else {
                    // Update last access time for LRU
                    if opts.touch {
                        entry.touch();
                    }
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
                let size = entry.size();
                self.memory_usage.fetch_sub(size, Ordering::Relaxed);
                self.memory_tracker.deallocate(size, MemoryCategory::Cache);
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
        self.map.get(key).map(|e| !e.is_expired()).unwrap_or(false)
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
}
