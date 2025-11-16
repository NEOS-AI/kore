mod storage;
mod operations;
mod expiration;
mod eviction;
mod sorted_sets;
mod geo_sets;
mod config;
mod search;

use crate::hashmap::ShardedHashMap;
use crate::sorted_set::SharedSortedSet;
use crate::geospatial::GeoSet;
use crate::stats::Stats;
use crate::pubsub::PubSub;
use crate::memory::MemoryTracker;
use crate::search_index::SearchIndexManager;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Arc, RwLock};

/// The main cache structure
pub struct Cache {
    /// Sharded hashmap for storing entries
    pub(super) map: ShardedHashMap,
    /// HashMap for storing sorted sets
    pub(super) sorted_sets: Arc<RwLock<HashMap<Bytes, SharedSortedSet>>>,
    /// HashMap for storing geospatial sets
    pub(super) geo_sets: Arc<RwLock<HashMap<Bytes, Arc<RwLock<GeoSet>>>>>,
    /// Pub/Sub system
    pub pubsub: Arc<PubSub>,
    /// Search index manager
    pub(super) search_index_manager: Arc<SearchIndexManager>,
    /// Memory tracker for category-based allocation
    pub(super) memory_tracker: Arc<MemoryTracker>,
    /// Statistics
    pub stats: Arc<Stats>,
    /// Maximum memory in bytes
    pub(super) max_memory: usize,
    /// Current memory usage
    pub(super) memory_usage: AtomicUsize,
    /// Maximum entry size in bytes
    pub(super) max_entry_size: AtomicUsize,
    /// Enable eviction when memory is full
    pub(super) evict_enabled: AtomicBool,
    /// Enable automatic sweeping
    pub(super) autosweep_enabled: AtomicBool,
    /// Number of samples for approximated LRU eviction (default: 5)
    pub(super) eviction_sample_size: AtomicUsize,
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
        // Create memory tracker with total memory limit and 1MB max message size
        let memory_tracker = Arc::new(MemoryTracker::new(
            max_memory,
            1024 * 1024, // 1MB max message size
        ));
        
        let cache = Arc::new(Self {
            map: ShardedHashMap::new(num_shards, 1024),
            sorted_sets: Arc::new(RwLock::new(HashMap::new())),
            geo_sets: Arc::new(RwLock::new(HashMap::new())),
            pubsub: PubSub::new(),
            search_index_manager: Arc::new(SearchIndexManager::new()),
            memory_tracker,
            stats: Arc::new(Stats::new()),
            max_memory,
            memory_usage: AtomicUsize::new(0),
            max_entry_size: AtomicUsize::new(max_entry_size),
            evict_enabled: AtomicBool::new(true),
            autosweep_enabled: AtomicBool::new(true),
            eviction_sample_size: AtomicUsize::new(5), // Redis default
        });

        // Start background sweep task if requested
        if start_sweep {
            let cache_clone = cache.clone();
            tokio::spawn(async move {
                cache_clone.background_sweep().await;
            });
        }

        cache
    }
}
