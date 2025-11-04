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
        if size > self.max_memory {
            return Err(Error::InvalidArgument(
                "max entry size cannot exceed max memory".into(),
            ));
        }
        self.max_entry_size.store(size, Ordering::Relaxed);
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
}
