use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Cache entry with reference counting and optional expiration
#[derive(Debug)]
pub struct Entry {
    /// The key
    pub key: Bytes,
    /// The value
    pub value: Bytes,
    /// Creation time
    pub created_at: Instant,
    /// Last access time (for LRU eviction) - stored as micros since creation
    last_access_micros: AtomicU64,
    /// Access frequency counter for LFU eviction (incremented on touch).
    lfu_freq: AtomicU64,
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
            last_access_micros: AtomicU64::new(0),
            lfu_freq: AtomicU64::new(0),
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

    /// Update the last access time to now and bump LFU frequency.
    pub fn touch(&self) {
        let micros_since_creation = self.created_at.elapsed().as_micros() as u64;
        self.last_access_micros
            .store(micros_since_creation, Ordering::Relaxed);
        // Saturating-ish: cap so counters stay comparable under long uptime
        let _ = self.lfu_freq.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |f| {
            Some(f.saturating_add(1).min(u32::MAX as u64))
        });
    }

    /// Get the last access time as an Instant
    pub fn last_access_time(&self) -> Instant {
        let micros = self.last_access_micros.load(Ordering::Relaxed);
        if micros == 0 {
            // Never accessed, return creation time
            self.created_at
        } else {
            self.created_at + Duration::from_micros(micros)
        }
    }

    /// LFU frequency (higher = more frequently accessed).
    pub fn lfu_freq(&self) -> u64 {
        self.lfu_freq.load(Ordering::Relaxed)
    }
}

impl Clone for Entry {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            value: self.value.clone(),
            created_at: self.created_at,
            last_access_micros: AtomicU64::new(self.last_access_micros.load(Ordering::Relaxed)),
            lfu_freq: AtomicU64::new(self.lfu_freq.load(Ordering::Relaxed)),
            expires_at: self.expires_at,
            flags: self.flags,
            cas: self.cas,
        }
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
#[derive(Clone, Debug)]
pub struct LoadOptions {
    /// Update access time (for LRU)
    pub touch: bool,
    /// Return CAS value
    pub with_cas: bool,
}

impl Default for LoadOptions {
    fn default() -> Self {
        Self {
            touch: true, // Enable touch by default for LRU tracking
            with_cas: false,
        }
    }
}
