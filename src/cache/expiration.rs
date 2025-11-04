use crate::error::Result;
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::Cache;

impl Cache {
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
}
