use crate::error::{Error, Result};
use std::sync::atomic::Ordering;

use super::Cache;

impl Cache {
    /// Get maximum entry size
    pub fn get_max_entry_size(&self) -> usize {
        self.max_entry_size.load(Ordering::Relaxed)
    }

    /// Set maximum entry size with validation
    pub fn set_max_entry_size(&self, size: usize) -> Result<()> {
        // Minimum 1KB
        if size < 1024 {
            return Err(Error::InvalidArgument(
                "max entry size too small (minimum 1KB)".into(),
            ));
        }
        // Cannot exceed max memory
        if size > self.max_memory() {
            return Err(Error::InvalidArgument(
                "max entry size cannot exceed max memory".into(),
            ));
        }
        self.max_entry_size.store(size, Ordering::Relaxed);
        Ok(())
    }

    /// Set maximum memory limit (live). Updates both the cache limit and MemoryTracker.
    /// If current usage exceeds the new limit and eviction is enabled, attempts to free space.
    pub fn set_max_memory(&self, size: usize) -> Result<()> {
        // 0 means unlimited; otherwise require at least 1MB (matches CLI validation)
        if size > 0 && size < 1024 * 1024 {
            return Err(Error::InvalidArgument(
                "max memory must be at least 1MB (or 0 for unlimited)".into(),
            ));
        }

        self.max_memory.store(size, Ordering::Relaxed);
        self.memory_tracker.set_max_memory(size);

        // If max entry size now exceeds the limit, clamp it
        let entry_max = self.max_entry_size.load(Ordering::Relaxed);
        if size > 0 && entry_max > size {
            self.max_entry_size.store(size, Ordering::Relaxed);
        }

        // Best-effort: free memory if we're over the new limit (all categories).
        if size > 0 {
            let tracked = self.memory_tracker.total_memory();
            let over = tracked.saturating_sub(size);
            if over > 0 && self.eviction_allowed() {
                let _ = self.evict_memory(over);
            }
        }

        Ok(())
    }

    /// Get eviction sample size
    pub fn get_eviction_sample_size(&self) -> usize {
        self.eviction_sample_size.load(Ordering::Relaxed)
    }

    /// Set eviction sample size with validation
    pub fn set_eviction_sample_size(&self, size: usize) -> Result<()> {
        // Minimum 1, maximum 100
        if size < 1 {
            return Err(Error::InvalidArgument(
                "eviction sample size too small (minimum 1)".into(),
            ));
        }
        if size > 100 {
            return Err(Error::InvalidArgument(
                "eviction sample size too large (maximum 100)".into(),
            ));
        }
        self.eviction_sample_size.store(size, Ordering::Relaxed);
        Ok(())
    }

    /// Redis `lfu-log-factor` (default 10).
    pub fn lfu_log_factor(&self) -> u8 {
        self.lfu_log_factor.load(Ordering::Relaxed)
    }

    /// Set `lfu-log-factor` (0..=255; Redis uses 0..100 typically, we allow full range).
    pub fn set_lfu_log_factor(&self, factor: u8) -> Result<()> {
        self.lfu_log_factor.store(factor, Ordering::Relaxed);
        Ok(())
    }

    /// Redis `lfu-decay-time` in minutes (default 1; 0 = never decay).
    pub fn lfu_decay_time(&self) -> u8 {
        self.lfu_decay_time.load(Ordering::Relaxed)
    }

    /// Set `lfu-decay-time` minutes (0 disables decay).
    pub fn set_lfu_decay_time(&self, minutes: u8) -> Result<()> {
        self.lfu_decay_time.store(minutes, Ordering::Relaxed);
        Ok(())
    }
}
