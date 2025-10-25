use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Cache entry with reference counting and optional expiration
#[derive(Clone, Debug)]
pub struct Entry {
    /// The key
    pub key: Bytes,
    /// The value
    pub value: Bytes,
    /// Creation time
    pub created_at: Instant,
    /// Expiration time (None = no expiration)
    pub expires_at: Option<Instant>,
    /// User-defined flags (for Memcache compatibility)
    pub flags: u32,
    /// CAS (Compare-And-Swap) value
    pub cas: u64,
}

impl Entry {
    pub fn new(key: Bytes, value: Bytes) -> Self {
        Self {
            key,
            value,
            created_at: Instant::now(),
            expires_at: None,
            flags: 0,
            cas: 0,
        }
    }

    pub fn with_expiration(mut self, ttl: Duration) -> Self {
        self.expires_at = Some(self.created_at + ttl);
        self
    }

    pub fn with_expiration_at(mut self, expires_at: Instant) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    pub fn with_flags(mut self, flags: u32) -> Self {
        self.flags = flags;
        self
    }

    pub fn with_cas(mut self, cas: u64) -> Self {
        self.cas = cas;
        self
    }

    /// Check if the entry is expired
    pub fn is_expired(&self) -> bool {
        self.expires_at
            .map(|exp| Instant::now() >= exp)
            .unwrap_or(false)
    }

    /// Get remaining TTL in milliseconds
    pub fn ttl_millis(&self) -> Option<i64> {
        self.expires_at.map(|exp| {
            let now = Instant::now();
            if now >= exp {
                -2 // Expired
            } else {
                exp.duration_since(now).as_millis() as i64
            }
        })
    }

    /// Get the size of this entry in bytes
    pub fn size(&self) -> usize {
        self.key.len() + self.value.len() + std::mem::size_of::<Self>()
    }
}

/// Reference-counted entry wrapper
pub type SharedEntry = Arc<Entry>;

/// Options for storing an entry
#[derive(Default, Clone, Debug)]
pub struct StoreOptions {
    /// Only set if key does not exist (NX)
    pub nx: bool,
    /// Only set if key exists (XX)
    pub xx: bool,
    /// Return the old value (GET)
    pub get: bool,
    /// Time-to-live in milliseconds
    pub ttl_ms: Option<u64>,
    /// Absolute expiration timestamp (milliseconds since epoch)
    pub exat_ms: Option<u64>,
    /// User flags
    pub flags: u32,
    /// Keep existing TTL
    pub keepttl: bool,
    /// Expected CAS value (for compare-and-swap)
    pub cas: Option<u64>,
}

/// Options for loading an entry
#[derive(Default, Clone, Debug)]
pub struct LoadOptions {
    /// Update access time (for LRU)
    pub touch: bool,
    /// Return CAS value
    pub with_cas: bool,
}
