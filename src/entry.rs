use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Cache entry with reference counting.
///
/// # TTL (Batch GA)
///
/// Key-level expiration lives on [`crate::cache::keyspace::KeySlot::expires_at`]
/// only. This struct no longer carries `expires_at` — use
/// [`crate::Cache::ttl`] / slot expire for TTL queries. Load returns the bare
/// entry; callers that need remaining TTL should call `cache.ttl(key)`.
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
    /// Redis-style LFU word: high 16 bits = minute stamp, low 8 = log counter.
    lfu: AtomicU64,
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
            lfu: AtomicU64::new(crate::lfu::initial()),
            flags: 0,
            cas: 0,
        }
    }

    pub fn with_flags(mut self, flags: u32) -> Self {
        self.flags = flags;
        self
    }

    pub fn with_cas(mut self, cas: u64) -> Self {
        self.cas = cas;
        self
    }

    /// Accounted heap size of this entry (includes map/allocator overhead).
    pub fn size(&self) -> usize {
        crate::memory::estimate_string_entry(
            self.key.len(),
            self.value.len(),
            std::mem::size_of::<Self>(),
        )
    }

    /// Logical size for maxentrysize checks (no allocator tax).
    pub fn logical_size(&self) -> usize {
        crate::memory::logical_string_entry(
            self.key.len(),
            self.value.len(),
            std::mem::size_of::<Self>(),
        )
    }

    /// Update last-access time (LRU) and Redis-style LFU on read/write touch.
    ///
    /// `log_factor` / `decay_time` come from cache config (`lfu-log-factor`,
    /// `lfu-decay-time`). Defaults match Redis (10 and 1 minute).
    pub fn touch(&self, log_factor: u8, decay_time: u8) {
        let micros_since_creation = self.created_at.elapsed().as_micros() as u64;
        self.last_access_micros
            .store(micros_since_creation, Ordering::Relaxed);
        let _ = self.lfu.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
            Some(crate::lfu::on_access(cur, log_factor, decay_time))
        });
    }

    /// Convenience touch with Redis defaults (tests / call sites without config).
    pub fn touch_default(&self) {
        self.touch(
            crate::lfu::LFU_LOG_FACTOR_DEFAULT,
            crate::lfu::LFU_DECAY_TIME_DEFAULT,
        );
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

    /// Effective LFU frequency for eviction (decayed log counter; higher = hotter).
    pub fn lfu_freq(&self, decay_time: u8) -> u64 {
        let packed = self.lfu.load(Ordering::Relaxed);
        crate::lfu::effective_counter(packed, decay_time) as u64
    }

    /// Raw packed LFU word (tests / debug).
    pub fn lfu_raw(&self) -> u64 {
        self.lfu.load(Ordering::Relaxed)
    }
}

impl Clone for Entry {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            value: self.value.clone(),
            created_at: self.created_at,
            last_access_micros: AtomicU64::new(self.last_access_micros.load(Ordering::Relaxed)),
            lfu: AtomicU64::new(self.lfu.load(Ordering::Relaxed)),
            flags: self.flags,
            cas: self.cas,
        }
    }
}

/// Reference-counted entry wrapper
pub type SharedEntry = Arc<Entry>;

/// Options for storing an entry
#[derive(Default, Clone, Copy, Debug)]
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
