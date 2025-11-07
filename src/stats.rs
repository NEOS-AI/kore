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

    // Sorted Set command counts
    pub cmd_zadd: AtomicU64,
    pub cmd_zrange: AtomicU64,
    pub cmd_zrevrange: AtomicU64,
    pub cmd_zcard: AtomicU64,
    pub cmd_zscore: AtomicU64,
    pub cmd_zrem: AtomicU64,
    pub cmd_zrank: AtomicU64,
    pub cmd_zrevrank: AtomicU64,

    // Geospatial command counts
    pub cmd_geoadd: AtomicU64,
    pub cmd_geosearch: AtomicU64,

    // Pub/Sub command counts
    pub cmd_publish: AtomicU64,
    pub cmd_subscribe: AtomicU64,
    pub cmd_unsubscribe: AtomicU64,
    pub cmd_psubscribe: AtomicU64,
    pub cmd_punsubscribe: AtomicU64,
    pub cmd_pubsub: AtomicU64,

    // Pub/Sub metrics
    pub pubsub_messages_sent: AtomicU64,
    pub pubsub_channels_active: AtomicU64,
    pub pubsub_patterns_active: AtomicU64,
    pub pubsub_clients_active: AtomicU64,

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

    // Network statistics
    pub bytes_sent: AtomicU64,
    pub bytes_received: AtomicU64,
    pub total_connections: AtomicU64,
    pub active_connections: AtomicU64,
}

impl Stats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn incr(&self, counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn incr_bytes_sent(&self, bytes: usize) {
        self.bytes_sent.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn incr_bytes_received(&self, bytes: usize) {
        self.bytes_received.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn incr_connections(&self) {
        self.total_connections.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decr_active_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
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
        self.cmd_zadd.store(0, Ordering::Relaxed);
        self.cmd_zrange.store(0, Ordering::Relaxed);
        self.cmd_zrevrange.store(0, Ordering::Relaxed);
        self.cmd_zcard.store(0, Ordering::Relaxed);
        self.cmd_zscore.store(0, Ordering::Relaxed);
        self.cmd_zrem.store(0, Ordering::Relaxed);
        self.cmd_zrank.store(0, Ordering::Relaxed);
        self.cmd_zrevrank.store(0, Ordering::Relaxed);
        self.cmd_geoadd.store(0, Ordering::Relaxed);
        self.cmd_geosearch.store(0, Ordering::Relaxed);
        self.cmd_publish.store(0, Ordering::Relaxed);
        self.cmd_subscribe.store(0, Ordering::Relaxed);
        self.cmd_unsubscribe.store(0, Ordering::Relaxed);
        self.cmd_psubscribe.store(0, Ordering::Relaxed);
        self.cmd_punsubscribe.store(0, Ordering::Relaxed);
        self.cmd_pubsub.store(0, Ordering::Relaxed);
        self.pubsub_messages_sent.store(0, Ordering::Relaxed);
        self.pubsub_channels_active.store(0, Ordering::Relaxed);
        self.pubsub_patterns_active.store(0, Ordering::Relaxed);
        self.pubsub_clients_active.store(0, Ordering::Relaxed);
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
        self.bytes_sent.store(0, Ordering::Relaxed);
        self.bytes_received.store(0, Ordering::Relaxed);
        self.total_connections.store(0, Ordering::Relaxed);
        self.active_connections.store(0, Ordering::Relaxed);
    }
}
