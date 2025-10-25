use std::sync::atomic::{AtomicU64, Ordering};

/// Statistics for the cache
#[derive(Debug, Default)]
pub struct Stats {
    // Command counts
    pub cmd_get: AtomicU64,
    pub cmd_set: AtomicU64,
    pub cmd_del: AtomicU64,
    pub cmd_incr: AtomicU64,
    pub cmd_decr: AtomicU64,

    // Cache hits and misses
    pub hits: AtomicU64,
    pub misses: AtomicU64,

    // Eviction counts
    pub evicted_expired: AtomicU64,
    pub evicted_lru: AtomicU64,

    // CAS operations
    pub cas_hits: AtomicU64,
    pub cas_misses: AtomicU64,
    pub cas_badval: AtomicU64,

    // Store errors
    pub store_too_large: AtomicU64,
    pub store_no_memory: AtomicU64,

    // Authentication
    pub auth_cmds: AtomicU64,
    pub auth_errors: AtomicU64,
}

impl Stats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn incr(&self, counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    pub fn reset(&self) {
        self.cmd_get.store(0, Ordering::Relaxed);
        self.cmd_set.store(0, Ordering::Relaxed);
        self.cmd_del.store(0, Ordering::Relaxed);
        self.cmd_incr.store(0, Ordering::Relaxed);
        self.cmd_decr.store(0, Ordering::Relaxed);
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.evicted_expired.store(0, Ordering::Relaxed);
        self.evicted_lru.store(0, Ordering::Relaxed);
        self.cas_hits.store(0, Ordering::Relaxed);
        self.cas_misses.store(0, Ordering::Relaxed);
        self.cas_badval.store(0, Ordering::Relaxed);
        self.store_too_large.store(0, Ordering::Relaxed);
        self.store_no_memory.store(0, Ordering::Relaxed);
        self.auth_cmds.store(0, Ordering::Relaxed);
        self.auth_errors.store(0, Ordering::Relaxed);
    }
}
