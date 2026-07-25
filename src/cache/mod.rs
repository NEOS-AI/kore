mod storage;
mod operations;
mod expiration;
mod eviction;
mod sorted_sets;
mod geo_sets;
mod hashes;
mod lists;
mod sets;
mod streams;
mod config;
mod search;
mod bitmap;
mod hyperloglog;
mod key_xfer;
mod keyspace;

pub use bitmap::{BitOpKind, BitfieldOp, BitfieldOverflow};
pub use keyspace::KeyValue;

use crate::hashmap::{ShardedHashMap, ShardedKeyMap};
use crate::sorted_set::SharedSortedSet;
use crate::geospatial::GeoSet;
use crate::hash_type::SharedHash;
use crate::list_block::ListBlockers;
use crate::list_type::SharedList;
use crate::set_type::SharedSet;
use crate::stream_type::SharedStream;
use crate::stats::Stats;
use crate::pubsub::PubSub;
use crate::memory::MemoryTracker;
use crate::search_index::SearchIndexManager;
use crate::slowlog::SlowLog;
use crate::acl_log::AclLog;
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize};
use std::sync::Arc;
use std::time::Instant;

pub use storage::KeyType;
pub(crate) use storage::KeyspacePayload;
pub use eviction::EvictionPolicy;

/// The main cache structure.
///
/// # Keyspace layout (Batch FG)
///
/// Logical Redis keyspace is **one name → one typed value**. Today that is
/// implemented as a **multi-map** (fields below) with a facade
/// ([`KeyValue`], [`Cache::get_key_value`]) so TYPE / DEL / EXISTS share one
/// lookup path. See `keyspace` module docs and `docs/module_architectures.md`
/// for the migration plan toward a single `KeyValue` map (FG-2+).
pub struct Cache {
    /// Sharded hashmap for string entries (`KeyValue::String`)
    pub(super) map: ShardedHashMap,
    /// Sharded map for sorted sets (key → SharedSortedSet)
    pub(super) sorted_sets: ShardedKeyMap<SharedSortedSet>,
    /// Sharded map for geospatial sets (key → SharedGeoSet)
    pub(super) geo_sets: ShardedKeyMap<Arc<RwLock<GeoSet>>>,
    /// Redis Hash keys
    pub(super) hashes: Arc<RwLock<HashMap<Bytes, SharedHash>>>,
    /// Redis List keys
    pub(super) lists: Arc<RwLock<HashMap<Bytes, SharedList>>>,
    /// Clients blocked on empty lists (BLPOP / BRPOP) for this keyspace.
    pub list_blockers: ListBlockers,
    /// Clients blocked on streams (XREAD / XREADGROUP BLOCK) for this keyspace.
    pub stream_blockers: ListBlockers,
    /// Redis Set keys
    pub(super) sets: Arc<RwLock<HashMap<Bytes, SharedSet>>>,
    /// Redis Stream keys
    pub(super) streams: Arc<RwLock<HashMap<Bytes, SharedStream>>>,
    /// Absolute Instant expiry for non-string keys (Redis expires-dict style).
    /// Strings keep TTL on `Entry`; typed keys store it here.
    pub(super) typed_expires: RwLock<HashMap<Bytes, Instant>>,
    /// Pub/Sub system
    pub pubsub: Arc<PubSub>,
    /// Search index manager
    pub(super) search_index_manager: Arc<SearchIndexManager>,
    /// Memory tracker for category-based allocation
    pub(super) memory_tracker: Arc<MemoryTracker>,
    /// Statistics
    pub stats: Arc<Stats>,
    /// Server-wide slow log (shared across logical DBs).
    pub slowlog: Arc<SlowLog>,
    /// Server-wide ACL security log (shared across logical DBs).
    pub acl_log: Arc<AclLog>,
    /// Maximum memory in bytes (live-updatable via CONFIG SET maxmemory)
    pub(super) max_memory: AtomicUsize,
    /// Current memory usage
    pub(super) memory_usage: AtomicUsize,
    /// Maximum entry size in bytes
    pub(super) max_entry_size: AtomicUsize,
    /// Enable eviction when memory is full (synced with eviction_policy)
    pub(super) evict_enabled: AtomicBool,
    /// Redis maxmemory-policy (see `EvictionPolicy`)
    pub(super) eviction_policy: AtomicU8,
    /// Enable automatic sweeping
    pub(super) autosweep_enabled: AtomicBool,
    /// Held for the duration of one background expire cycle so
    /// [`Self::with_autosweep_paused`] can wait for in-flight work to finish.
    pub(super) autosweep_cycle_lock: Mutex<()>,
    /// Number of samples for approximated LRU/LFU eviction (default: 5)
    pub(super) eviction_sample_size: AtomicUsize,
    /// Redis `lfu-log-factor` (default 10): higher slows counter growth.
    pub(super) lfu_log_factor: AtomicU8,
    /// Redis `lfu-decay-time` in minutes (default 1); 0 disables decay.
    pub(super) lfu_decay_time: AtomicU8,
    /// Per-key generation counters for WATCH / optimistic locking.
    /// Only keys that have been WATCHed (or modified while watched) appear here.
    pub(super) watch_gens: Mutex<HashMap<Bytes, u64>>,
    /// Create-time loadfactor used for initial per-shard HashMap capacity.
    /// Preserved so scratch keyspaces (`empty_keyspace_like`) match the
    /// process sizing policy rather than hardcoding 0.75.
    pub(super) loadfactor: f64,
}

impl Cache {
    pub fn new(num_shards: usize, max_memory: usize) -> Arc<Self> {
        Self::new_with_sweep(num_shards, max_memory, 500 * 1024 * 1024, true)
    }

    pub fn new_with_sweep(
        num_shards: usize,
        max_memory: usize,
        max_entry_size: usize,
        start_sweep: bool,
    ) -> Arc<Self> {
        Self::new_with_sweep_loadfactor(num_shards, max_memory, max_entry_size, start_sweep, 0.75)
    }

    /// Create a cache with an explicit load factor used for initial shard capacity.
    /// Higher loadfactor → smaller initial HashMap capacity per shard.
    pub fn new_with_sweep_loadfactor(
        num_shards: usize,
        max_memory: usize,
        max_entry_size: usize,
        start_sweep: bool,
        loadfactor: f64,
    ) -> Arc<Self> {
        // Create memory tracker with total memory limit and 1MB max message size
        let memory_tracker = Arc::new(MemoryTracker::new(
            max_memory,
            1024 * 1024, // 1MB max message size
        ));

        // capacity_per_shard ≈ 1024 / loadfactor (clamped); denser tables start smaller
        let cap = ((1024.0 / loadfactor.max(0.55)) as usize).max(16);

        let cache = Arc::new(Self {
            map: ShardedHashMap::new(num_shards, cap),
            sorted_sets: ShardedKeyMap::new(num_shards),
            geo_sets: ShardedKeyMap::new(num_shards),
            hashes: Arc::new(RwLock::new(HashMap::new())),
            lists: Arc::new(RwLock::new(HashMap::new())),
            list_blockers: ListBlockers::new(),
            stream_blockers: ListBlockers::new(),
            sets: Arc::new(RwLock::new(HashMap::new())),
            streams: Arc::new(RwLock::new(HashMap::new())),
            typed_expires: RwLock::new(HashMap::new()),
            pubsub: PubSub::new(),
            search_index_manager: Arc::new(SearchIndexManager::new()),
            memory_tracker,
            stats: Arc::new(Stats::new()),
            slowlog: Arc::new(SlowLog::new()),
            acl_log: Arc::new(AclLog::new()),
            max_memory: AtomicUsize::new(max_memory),
            memory_usage: AtomicUsize::new(0),
            max_entry_size: AtomicUsize::new(max_entry_size),
            evict_enabled: AtomicBool::new(true),
            eviction_policy: AtomicU8::new(EvictionPolicy::AllKeysLru as u8),
            autosweep_enabled: AtomicBool::new(true),
            autosweep_cycle_lock: Mutex::new(()),
            eviction_sample_size: AtomicUsize::new(5), // Redis default
            lfu_log_factor: AtomicU8::new(crate::lfu::LFU_LOG_FACTOR_DEFAULT),
            lfu_decay_time: AtomicU8::new(crate::lfu::LFU_DECAY_TIME_DEFAULT),
            watch_gens: Mutex::new(HashMap::new()),
            loadfactor,
        });

        if start_sweep {
            cache.start_background_sweep();
        }

        cache
    }

    /// Create an empty keyspace that shares pub/sub and stats with `shared`.
    ///
    /// Used for logical multi-DB (`SELECT`): keys are isolated, but Redis pub/sub
    /// and connection stats remain process-global (anchored on DB 0).
    pub fn new_keyspace_sharing(
        shared: &Self,
        num_shards: usize,
        max_memory: usize,
        max_entry_size: usize,
        start_sweep: bool,
        loadfactor: f64,
    ) -> Arc<Self> {
        Self::new_keyspace_sharing_with_stats(
            shared,
            num_shards,
            max_memory,
            max_entry_size,
            start_sweep,
            loadfactor,
            Arc::clone(&shared.stats),
        )
    }

    /// Like [`new_keyspace_sharing`], but with an explicit stats Arc.
    ///
    /// Logical multi-DB siblings pass `shared.stats`; scratch-load uses a fresh
    /// `Stats` so RDB/AOF apply does not pollute live INFO counters.
    fn new_keyspace_sharing_with_stats(
        shared: &Self,
        num_shards: usize,
        max_memory: usize,
        max_entry_size: usize,
        start_sweep: bool,
        loadfactor: f64,
        stats: Arc<Stats>,
    ) -> Arc<Self> {
        let memory_tracker = Arc::new(MemoryTracker::new(max_memory, 1024 * 1024));
        let cap = ((1024.0 / loadfactor.max(0.55)) as usize).max(16);

        let cache = Arc::new(Self {
            map: ShardedHashMap::new(num_shards, cap),
            sorted_sets: ShardedKeyMap::new(num_shards),
            geo_sets: ShardedKeyMap::new(num_shards),
            hashes: Arc::new(RwLock::new(HashMap::new())),
            lists: Arc::new(RwLock::new(HashMap::new())),
            list_blockers: ListBlockers::new(),
            stream_blockers: ListBlockers::new(),
            sets: Arc::new(RwLock::new(HashMap::new())),
            streams: Arc::new(RwLock::new(HashMap::new())),
            typed_expires: RwLock::new(HashMap::new()),
            pubsub: Arc::clone(&shared.pubsub),
            search_index_manager: Arc::new(SearchIndexManager::new()),
            memory_tracker,
            stats,
            slowlog: Arc::clone(&shared.slowlog),
            acl_log: Arc::clone(&shared.acl_log),
            max_memory: AtomicUsize::new(max_memory),
            memory_usage: AtomicUsize::new(0),
            max_entry_size: AtomicUsize::new(max_entry_size),
            evict_enabled: AtomicBool::new(
                shared.evict_enabled.load(std::sync::atomic::Ordering::Relaxed),
            ),
            eviction_policy: AtomicU8::new(
                shared
                    .eviction_policy
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            autosweep_enabled: AtomicBool::new(
                shared
                    .autosweep_enabled
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            autosweep_cycle_lock: Mutex::new(()),
            eviction_sample_size: AtomicUsize::new(
                shared
                    .eviction_sample_size
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            lfu_log_factor: AtomicU8::new(
                shared
                    .lfu_log_factor
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            lfu_decay_time: AtomicU8::new(
                shared
                    .lfu_decay_time
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            watch_gens: Mutex::new(HashMap::new()),
            loadfactor,
        });

        if start_sweep {
            cache.start_background_sweep();
        }

        cache
    }

    /// Empty keyspace sibling for scratch-load (AOF/RDB).
    ///
    /// Same shard count / maxmemory / max-entry-size / create-time loadfactor
    /// as `self`, shares pubsub + slowlog / acl_log (multi-DB sibling pattern),
    /// but has **independent** maps, search manager, zeroed memory, and a
    /// private `Stats` (so load apply does not inflate live INFO). Background
    /// sweep is **not** started — callers must use this only under exclusive
    /// access (no concurrent client commands against the scratch). Autosweep
    /// is forced off.
    pub fn empty_keyspace_like(&self) -> Arc<Self> {
        let scratch = Self::new_keyspace_sharing_with_stats(
            self,
            self.map.num_shards(),
            self.max_memory.load(std::sync::atomic::Ordering::Relaxed),
            self.max_entry_size
                .load(std::sync::atomic::Ordering::Relaxed),
            false, // start_sweep: load-time exclusive use
            self.loadfactor,
            Arc::new(Stats::new()),
        );
        scratch.set_autosweep(false);
        scratch
    }

    /// Spawn the background active-expire task (idempotent only if callers
    /// ensure a single spawn; used after startup load when create used
    /// `start_sweep: false`).
    pub fn start_background_sweep(self: &Arc<Self>) {
        let cache_clone = self.clone();
        tokio::spawn(async move {
            cache_clone.background_sweep().await;
        });
    }

    /// Current autosweep flag.
    pub fn autosweep_enabled(&self) -> bool {
        self.autosweep_enabled
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Run `f` with autosweep disabled and no in-flight expire cycle running.
    ///
    /// Disables the flag, waits to acquire [`Self::autosweep_cycle_lock`] (held
    /// for the whole background expire body), runs `f`, then restores the
    /// previous flag even if `f` panics. Used around keyspace replace commit.
    pub fn with_autosweep_paused<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let prev = self.autosweep_enabled();
        self.set_autosweep(false);
        struct Restore<'a> {
            cache: &'a Cache,
            prev: bool,
        }
        impl Drop for Restore<'_> {
            fn drop(&mut self) {
                self.cache.set_autosweep(self.prev);
            }
        }
        let _restore = Restore {
            cache: self,
            prev,
        };
        // Wait for any in-flight expire cycle to finish; hold lock so a new
        // cycle cannot start while `f` runs.
        let _cycle = self.autosweep_cycle_lock.lock();
        f()
    }

    /// Current max memory limit in bytes.
    pub fn max_memory(&self) -> usize {
        self.max_memory.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Ensure `key` is tracked for WATCH and return its current generation.
    pub fn watch_generation(&self, key: &Bytes) -> u64 {
        let mut gens = self.watch_gens.lock();
        *gens.entry(key.clone()).or_insert(0)
    }

    /// Bump generation for a key if it is (or was) under WATCH tracking.
    /// No-op when the key has never been watched — keeps the map small.
    pub fn touch_watch_key(&self, key: &Bytes) {
        let mut gens = self.watch_gens.lock();
        if let Some(g) = gens.get_mut(key) {
            *g = g.wrapping_add(1);
        }
    }

    /// Bump generation for every currently tracked watch key (e.g. FLUSHDB/FLUSHALL).
    pub fn touch_all_watch_keys(&self) {
        let mut gens = self.watch_gens.lock();
        for g in gens.values_mut() {
            *g = g.wrapping_add(1);
        }
    }
}
