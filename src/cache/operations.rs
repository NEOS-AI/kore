use crate::entry::StoreOptions;
use crate::error::{Error, Result};
use bytes::Bytes;

use super::Cache;

impl Cache {
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
        self.store(key.clone(), value_bytes, StoreOptions::default())?;

        Ok(new_value)
    }

    /// Atomic decrement
    pub fn decr(&self, key: &Bytes, delta: i64) -> Result<i64> {
        self.stats.incr(&self.stats.cmd_decr);
        self.incr(key, -delta)
    }
}
