use crate::entry::{Entry, LoadOptions, SharedEntry, StoreOptions};
use crate::error::{Error, Result};
use crate::hashmap::EntryAction;
use crate::memory::MemoryCategory;
use bytes::Bytes;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::Cache;

/// Redis-style key type across string / zset / geo / hash / list / set / stream namespaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    None,
    String,
    ZSet,
    Geo,
    Hash,
    List,
    Set,
    Stream,
}

impl KeyType {
    /// Redis TYPE command string. Geo keys report as "zset" (Redis-compatible).
    pub fn as_redis_str(&self) -> &'static str {
        match self {
            KeyType::None => "none",
            KeyType::String => "string",
            KeyType::ZSet | KeyType::Geo => "zset",
            KeyType::Hash => "hash",
            KeyType::List => "list",
            KeyType::Set => "set",
            KeyType::Stream => "stream",
        }
    }
}

/// Outcome of the atomic store mutate path.
enum StoreOutcome {
    /// NX failed — key already exists
    Exists(SharedEntry),
    /// XX failed — key does not exist
    NotExists,
    /// CAS value mismatch
    CasMismatch,
    /// CAS target key missing
    CasMiss,
    /// Successfully stored
    Stored {
        /// Old value when GET option was set
        old_for_get: Option<SharedEntry>,
        old_size: usize,
        new_size: usize,
    },
}

impl Cache {
    /// Determine the type of value stored at `key`.
    pub fn key_type(&self, key: &Bytes) -> KeyType {
        if let Some(entry) = self.map.get(key) {
            if !entry.is_expired() {
                return KeyType::String;
            }
        }
        // Lazy expire for typed keys with TTL.
        if self.purge_typed_if_expired(key) {
            return KeyType::None;
        }
        if self.sorted_sets.contains_key(key) {
            return KeyType::ZSet;
        }
        if self.geo_sets.contains_key(key) {
            return KeyType::Geo;
        }
        if self.hashes.read().contains_key(key) {
            return KeyType::Hash;
        }
        if self.lists.read().contains_key(key) {
            return KeyType::List;
        }
        if self.sets.read().contains_key(key) {
            return KeyType::Set;
        }
        if self.streams.read().contains_key(key) {
            return KeyType::Stream;
        }
        KeyType::None
    }

    /// Ensure `key` is either absent or already of `expected` type.
    pub fn ensure_type(&self, key: &Bytes, expected: KeyType) -> Result<()> {
        match self.key_type(key) {
            KeyType::None => Ok(()),
            actual if actual == expected => Ok(()),
            _ => Err(Error::WrongType),
        }
    }

    /// Ensure `key` is absent or a string (for SET/GET-family commands).
    pub fn ensure_string_or_absent(&self, key: &Bytes) -> Result<()> {
        self.ensure_type(key, KeyType::String)
    }

    /// Update both memory_usage and memory_tracker after a successful map mutation.
    pub(super) fn account_replace(&self, old_size: usize, new_size: usize) {
        if old_size > 0 {
            self.memory_usage.fetch_sub(old_size, Ordering::Relaxed);
            self.memory_tracker
                .deallocate(old_size, MemoryCategory::Cache);
        }
        if new_size > 0 {
            self.memory_usage.fetch_add(new_size, Ordering::Relaxed);
            // Unconditional — entry is already in the map; never fail accounting mid-flight.
            self.memory_tracker
                .account(new_size, MemoryCategory::Cache);
        }
    }

    /// Convert StoreOptions expiration fields into an absolute Instant, if any.
    /// EXAT/PXAT are absolute Unix epoch milliseconds.
    fn resolve_expiration(opts: &StoreOptions) -> Option<Instant> {
        if let Some(ttl_ms) = opts.ttl_ms {
            Some(Instant::now() + Duration::from_millis(ttl_ms))
        } else if let Some(exat_ms) = opts.exat_ms {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            if exat_ms <= now_ms {
                // Timestamp in the past — store as immediately expired
                Some(Instant::now())
            } else {
                Some(Instant::now() + Duration::from_millis(exat_ms - now_ms))
            }
        } else {
            None
        }
    }

    /// Ensure there is capacity for `needed` additional bytes, optionally evicting.
    ///
    /// Uses total tracked memory (all categories) so string stores respect
    /// hash/list/zset/search usage under the same maxmemory budget.
    pub(super) fn ensure_capacity(&self, needed: usize) -> Result<()> {
        if needed == 0 {
            return Ok(());
        }

        let tracker_ok = self
            .memory_tracker
            .can_allocate(needed, MemoryCategory::Cache);
        let max_memory = self.max_memory.load(Ordering::Relaxed);
        // 0 = unlimited (Redis-compatible CONFIG SET maxmemory 0)
        let total = self.memory_tracker.total_memory();
        let usage_ok = max_memory == 0 || total.saturating_add(needed) <= max_memory;

        if tracker_ok && usage_ok {
            return Ok(());
        }

        if self.eviction_allowed() {
            match self.evict_memory(needed) {
                Ok(()) => Ok(()),
                Err(e) => {
                    self.stats.incr(&self.stats.store_no_memory);
                    Err(e)
                }
            }
        } else {
            self.stats.incr(&self.stats.store_no_memory);
            Err(Error::OutOfMemory)
        }
    }

    /// Store a key-value pair
    pub fn store(
        &self,
        key: Bytes,
        value: Bytes,
        opts: StoreOptions,
    ) -> Result<Option<SharedEntry>> {
        // maxentrysize: logical payload (no allocator tax)
        let logical = crate::memory::logical_string_entry(
            key.len(),
            value.len(),
            std::mem::size_of::<Entry>(),
        );
        let max_entry_size = self.max_entry_size.load(Ordering::Relaxed);
        if logical > max_entry_size {
            self.stats.incr(&self.stats.store_too_large);
            return Err(Error::EntryTooLarge);
        }

        // Accounted size includes map slot + allocator overhead (Batch AA).
        let entry_size = crate::memory::estimate_string_entry(
            key.len(),
            value.len(),
            std::mem::size_of::<Entry>(),
        );

        // Rough pre-check: account for memory that would be freed on replace
        let existing_size = self.map.get(&key).map(|e| e.size()).unwrap_or(0);
        let net_memory_change = entry_size.saturating_sub(existing_size);
        self.ensure_capacity(net_memory_change)?;

        // Resolve absolute expiration outside the lock (keepttl handled under lock)
        let expires_at = Self::resolve_expiration(&opts);
        let keepttl = opts.keepttl && expires_at.is_none();
        let nx = opts.nx;
        let xx = opts.xx;
        let get = opts.get;
        let flags = opts.flags;
        let cas_expected = opts.cas;

        let outcome = self.map.mutate(&key, |current, next_cas| {
            // NX: only set if not exists (treat expired as absent)
            if nx {
                if let Some(existing) = current {
                    if !existing.is_expired() {
                        return (
                            EntryAction::Keep,
                            StoreOutcome::Exists(existing.clone()),
                        );
                    }
                }
            }

            // XX: only set if exists and not expired
            if xx {
                match current {
                    Some(existing) if !existing.is_expired() => {}
                    _ => return (EntryAction::Keep, StoreOutcome::NotExists),
                }
            }

            // CAS compare-and-swap
            if let Some(expected_cas) = cas_expected {
                match current {
                    Some(existing) if !existing.is_expired() => {
                        if existing.cas != expected_cas {
                            return (EntryAction::Keep, StoreOutcome::CasMismatch);
                        }
                    }
                    _ => return (EntryAction::Keep, StoreOutcome::CasMiss),
                }
            }

            let old_for_get = if get {
                current.filter(|e| !e.is_expired()).cloned()
            } else {
                None
            };

            let old_size = current.map(|e| e.size()).unwrap_or(0);

            let mut entry = Entry::new(key.clone(), value.clone());

            if let Some(exp) = expires_at {
                entry.expires_at = Some(exp);
            } else if keepttl {
                if let Some(existing) = current {
                    entry.expires_at = existing.expires_at;
                }
            }

            entry = entry.with_flags(flags).with_cas(next_cas);
            let entry = Arc::new(entry);
            let new_size = entry.size();

            (
                EntryAction::Set(entry),
                StoreOutcome::Stored {
                    old_for_get,
                    old_size,
                    new_size,
                },
            )
        });

        match outcome {
            StoreOutcome::Exists(existing) => Ok(Some(existing)),
            StoreOutcome::NotExists => Ok(None),
            StoreOutcome::CasMismatch => {
                self.stats.incr(&self.stats.cas_badval);
                Err(Error::CasMismatch)
            }
            StoreOutcome::CasMiss => {
                self.stats.incr(&self.stats.cas_misses);
                Err(Error::KeyNotFound)
            }
            StoreOutcome::Stored {
                old_for_get,
                old_size,
                new_size,
            } => {
                // Entry is in the map — always keep counters consistent (no OOM after insert).
                self.account_replace(old_size, new_size);
                if cas_expected.is_some() {
                    self.stats.incr(&self.stats.cas_hits);
                }
                self.stats.incr(&self.stats.cmd_set);
                Ok(old_for_get)
            }
        }
    }

    /// Load a key
    pub fn load(&self, key: &Bytes, opts: LoadOptions) -> Result<Option<SharedEntry>> {
        self.stats.incr(&self.stats.cmd_get);

        match self.map.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    // Remove expired entry and free both counters
                    let size = entry.size();
                    if self.map.remove(key).is_some() {
                        self.memory_usage.fetch_sub(size, Ordering::Relaxed);
                        self.memory_tracker
                            .deallocate(size, MemoryCategory::Cache);
                        self.stats.incr(&self.stats.evicted_expired);
                    }
                    self.stats.incr(&self.stats.misses);
                    Ok(None)
                } else {
                    // Update last access (LRU) and Redis-style LFU
                    if opts.touch {
                        entry.touch(
                            self.lfu_log_factor.load(Ordering::Relaxed),
                            self.lfu_decay_time.load(Ordering::Relaxed),
                        );
                    }
                    self.stats.incr(&self.stats.hits);
                    Ok(Some(entry))
                }
            }
            None => {
                self.stats.incr(&self.stats.misses);
                Ok(None)
            }
        }
    }

    /// Delete a key (string, sorted set, geo, hash, list, or set)
    pub fn delete(&self, key: &Bytes) -> Result<bool> {
        self.stats.incr(&self.stats.cmd_del);

        let deleted = if let Some(entry) = self.map.remove(key) {
            let size = entry.size();
            self.memory_usage.fetch_sub(size, Ordering::Relaxed);
            self.memory_tracker.deallocate(size, MemoryCategory::Cache);
            true
        } else if self.remove_sorted_set(key) {
            true
        } else if self.remove_geo_set(key) {
            true
        } else if self.remove_hash(key) {
            true
        } else if self.remove_list(key) {
            true
        } else if self.remove_set(key) {
            true
        } else if self.remove_stream(key) {
            true
        } else {
            false
        };

        // Always drop typed expire metadata (no-op if absent).
        self.clear_typed_expire(key);

        // DEL/UNLINK: remove key from any matching search indices
        if deleted {
            self.auto_remove_from_indices(key);
        }

        Ok(deleted)
    }

    /// Delete multiple keys
    pub fn delete_many(&self, keys: &[Bytes]) -> Result<usize> {
        let mut count = 0;
        for key in keys {
            if self.delete(key)? {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Check if key exists (any type)
    pub fn exists(&self, key: &Bytes) -> bool {
        self.key_type(key) != KeyType::None
    }

    /// Get database size (all key types)
    pub fn dbsize(&self) -> usize {
        let strings = self.map.len();
        let zsets = self.sorted_sets.len();
        let geos = self.geo_sets.len();
        let hashes = self.hashes.read().len();
        let lists = self.lists.read().len();
        let sets = self.sets.read().len();
        let streams = self.streams.read().len();
        strings + zsets + geos + hashes + lists + sets + streams
    }

    /// String-KV atomic counter (kept for replace/evict paths; prefer `tracked_memory`).
    pub fn string_memory_usage(&self) -> usize {
        self.memory_usage.load(Ordering::Relaxed)
    }

    /// Total accounted memory (all categories) — Redis-compatible `used_memory`.
    pub fn memory_usage(&self) -> usize {
        self.memory_tracker.total_memory()
    }

    /// Total memory tracked across all categories (alias of `memory_usage`).
    pub fn tracked_memory(&self) -> usize {
        self.memory_tracker.total_memory()
    }

    /// Memory tracked for a specific category (Cache, Search, Hashes, …).
    pub fn category_memory(&self, category: MemoryCategory) -> usize {
        self.memory_tracker.category_memory(category)
    }

    /// Release Pub/Sub pending-buffer memory after delivery, lag drop, or disconnect.
    pub fn release_pubsub_memory(&self, size: usize) {
        if size > 0 {
            self.memory_tracker
                .deallocate(size, MemoryCategory::PubSub);
        }
    }

    /// Mark one pub/sub message as delivered to `client_id` and free its pending bytes.
    pub async fn note_pubsub_delivered(&self, client_id: crate::pubsub::ClientId) {
        let size = self.pubsub.note_delivered(client_id).await;
        self.release_pubsub_memory(size);
    }

    /// Unregister a pub/sub client and free any remaining pending buffer accounting.
    pub async fn unregister_pubsub_client(&self, client_id: crate::pubsub::ClientId) {
        let pending = self.pubsub.unregister_client(client_id).await;
        self.release_pubsub_memory(pending);
    }

    /// Memory tracked for the Cache (string KV) category only
    pub fn tracked_cache_memory(&self) -> usize {
        self.memory_tracker
            .category_memory(MemoryCategory::Cache)
    }

    /// Clear all keyspace entries (KV, zset, geo, hash, list, set, stream) and
    /// search *documents*, then reset memory accounting.
    ///
    /// FT index definitions and aliases are kept (RediSearch-style FLUSHDB:
    /// docs gone, schema remains). For a full wipe including schema, use
    /// [`flush_all_including_search`].
    pub fn flush(&self) {
        self.flush_keyspace();
        // Drop indexed docs so FT.SEARCH cannot return deleted keys; keep schema.
        self.search_index_manager.clear_documents();
        self.memory_usage.store(0, Ordering::Relaxed);
        self.memory_tracker.reset();
    }

    /// Full wipe: keyspace + every search index definition and alias.
    ///
    /// Used when a hard reset of definitions is required. Live FLUSHDB/FLUSHALL
    /// use [`flush`] instead. AOF/RDB public load paths use scratch-load +
    /// [`replace_keyspace_from`] and do **not** wipe the target on failure.
    pub fn flush_all_including_search(&self) {
        self.flush_keyspace();
        self.search_index_manager.clear();
        self.memory_usage.store(0, Ordering::Relaxed);
        self.memory_tracker.reset();
    }

    /// Move full keyspace state from `other` into `self` (map-level swap).
    ///
    /// **Exclusive access required** on both caches for the whole call: no
    /// concurrent client commands. Callers must also pause autosweep on `self`
    /// (see [`Cache::with_autosweep_paused`] / public load wrappers) so expire
    /// cannot race map/counter install. Intended only for AOF/RDB scratch-load
    /// commit after a successful decode/replay into `other`.
    ///
    /// Swaps: string map, sorted_sets, geo_sets, hashes, lists, sets, streams,
    /// typed_expires, watch_gens, search indices/aliases, MemoryTracker
    /// keyspace category counts, and `memory_usage`. Memory is moved only via
    /// tracker take/install + `memory_usage` store (never per-key `account`
    /// after map replace).
    ///
    /// Pre-swap WATCH keys on `self` are re-tracked and bumped after install so
    /// live clients with WATCH fail EXEC (same idea as FLUSHDB).
    ///
    /// Leaves **unchanged** on `self`: pubsub, connection stats, list/stream
    /// blockers, maxmemory / eviction config, slowlog, acl_log.
    ///
    /// After return, `self` holds `other`'s keyspace; `other` is empty (safe to
    /// drop). Drain-then-replace: scratch is fully drained first, then the
    /// target is drained into discard and filled — so a panic while preparing
    /// scratch leaves `self` intact. Discard locals are dropped immediately
    /// after install to shorten the dual-residency window.
    pub fn replace_keyspace_from(&self, other: &Self) {
        // Capture pre-swap WATCH keys before drain (for post-install bump).
        let pre_watch_keys: Vec<Bytes> = self.watch_gens.lock().keys().cloned().collect();

        // 1. Drain scratch completely first (self still intact if this panics).
        let other_map = other.map.drain_all();
        let other_zsets = other.sorted_sets.drain_all();
        let other_geos = other.geo_sets.drain_all();
        let other_hashes = std::mem::take(&mut *other.hashes.write());
        let other_lists = std::mem::take(&mut *other.lists.write());
        let other_sets = std::mem::take(&mut *other.sets.write());
        let other_streams = std::mem::take(&mut *other.streams.write());
        let other_expires = std::mem::take(&mut *other.typed_expires.write());
        let other_watch = std::mem::take(&mut *other.watch_gens.lock());
        let (other_indices, other_aliases) = other.search_index_manager.take_all();
        let other_counts = other.memory_tracker.take_keyspace_counts();
        let other_mem = other.memory_usage.swap(0, Ordering::Relaxed);

        // 2. Drain target into discard, then install scratch state.
        let discard_map = self.map.drain_all();
        let discard_zsets = self.sorted_sets.drain_all();
        let discard_geos = self.geo_sets.drain_all();
        let discard_hashes = std::mem::take(&mut *self.hashes.write());
        let discard_lists = std::mem::take(&mut *self.lists.write());
        let discard_sets = std::mem::take(&mut *self.sets.write());
        let discard_streams = std::mem::take(&mut *self.streams.write());
        let discard_expires = std::mem::take(&mut *self.typed_expires.write());
        let discard_watch = std::mem::take(&mut *self.watch_gens.lock());
        let (discard_indices, discard_aliases) = self.search_index_manager.take_all();
        let discard_counts = self.memory_tracker.take_keyspace_counts();

        // Install maps first, then memory counters (autosweep must be paused).
        self.map.replace_all(other_map);
        self.sorted_sets.replace_all(other_zsets);
        self.geo_sets.replace_all(other_geos);
        *self.hashes.write() = other_hashes;
        *self.lists.write() = other_lists;
        *self.sets.write() = other_sets;
        *self.streams.write() = other_streams;
        *self.typed_expires.write() = other_expires;
        *self.watch_gens.lock() = other_watch;
        self.search_index_manager
            .install(other_indices, other_aliases);
        self.memory_tracker.install_keyspace_counts(&other_counts);
        self.memory_usage.store(other_mem, Ordering::Relaxed);

        // Free discarded target state ASAP (shorten ~2× peak window).
        drop(discard_map);
        drop(discard_zsets);
        drop(discard_geos);
        drop(discard_hashes);
        drop(discard_lists);
        drop(discard_sets);
        drop(discard_streams);
        drop(discard_expires);
        drop(discard_watch);
        drop(discard_indices);
        drop(discard_aliases);
        let _ = discard_counts;

        // Bump pre-swap WATCH gens so live EXEC aborts after dataset replace.
        if !pre_watch_keys.is_empty() {
            let mut gens = self.watch_gens.lock();
            for k in pre_watch_keys {
                let g = gens.entry(k).or_insert(0);
                *g = g.wrapping_add(1);
            }
        }
    }

    /// Non-mutating export of non-expired string entries for RDB snapshot /
    /// scratch seed. Does **not** touch LRU/LFU, bump stats, or lazy-delete
    /// expired keys (expired are simply skipped).
    pub fn export_strings(&self) -> Vec<(Bytes, Bytes, u32, i64)> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let mut out = Vec::new();
        for key in self.map.keys(None) {
            let Some(entry) = self.map.get(&key) else {
                continue;
            };
            if entry.is_expired() {
                continue;
            }
            let expire_unix_ms = match entry.ttl_millis() {
                Some(ttl) if ttl > 0 => now + ttl,
                _ => -1,
            };
            out.push((
                entry.key.clone(),
                entry.value.clone(),
                entry.flags,
                expire_unix_ms,
            ));
        }
        out
    }

    /// Clear all typed key maps / expires (not search schema).
    fn flush_keyspace(&self) {
        self.map.clear();
        self.sorted_sets.clear();
        self.geo_sets.clear();
        self.hashes.write().clear();
        self.lists.write().clear();
        self.sets.write().clear();
        self.streams.write().clear();
        self.typed_expires.write().clear();
    }

    /// All non-expired string keys in the sharded map (for persistence).
    pub fn map_keys_all(&self) -> Vec<Bytes> {
        self.map
            .keys(None)
            .into_iter()
            .filter(|k| self.map.get(k).map(|e| !e.is_expired()).unwrap_or(false))
            .collect()
    }

    /// Get all keys matching a pattern across all key-type maps.
    pub fn keys(&self, pattern: Option<&str>) -> Vec<Bytes> {
        use crate::hashmap::pattern_match;
        use std::collections::HashSet;

        let mut seen: HashSet<Bytes> = HashSet::new();
        let mut result = Vec::new();

        // String keys (skip expired)
        for key in self.map.keys(pattern) {
            if let Some(entry) = self.map.get(&key) {
                if entry.is_expired() {
                    continue;
                }
            }
            if seen.insert(key.clone()) {
                result.push(key);
            }
        }

        let matches = |key: &Bytes| -> bool {
            match pattern {
                Some(pat) => pattern_match(pat, std::str::from_utf8(key).unwrap_or("")),
                None => true,
            }
        };

        let push_typed = |key: Bytes, seen: &mut HashSet<Bytes>, result: &mut Vec<Bytes>| {
            if self.purge_typed_if_expired(&key) {
                return;
            }
            if seen.insert(key.clone()) {
                result.push(key);
            }
        };

        for key in self.sorted_sets.keys(pattern) {
            push_typed(key, &mut seen, &mut result);
        }
        for key in self.geo_sets.keys(pattern) {
            push_typed(key, &mut seen, &mut result);
        }
        {
            let keys: Vec<Bytes> = self
                .hashes
                .read()
                .keys()
                .filter(|k| matches(k))
                .cloned()
                .collect();
            for key in keys {
                push_typed(key, &mut seen, &mut result);
            }
        }
        {
            let keys: Vec<Bytes> = self
                .lists
                .read()
                .keys()
                .filter(|k| matches(k))
                .cloned()
                .collect();
            for key in keys {
                push_typed(key, &mut seen, &mut result);
            }
        }
        {
            let keys: Vec<Bytes> = self
                .sets
                .read()
                .keys()
                .filter(|k| matches(k))
                .cloned()
                .collect();
            for key in keys {
                push_typed(key, &mut seen, &mut result);
            }
        }
        {
            let keys: Vec<Bytes> = self
                .streams
                .read()
                .keys()
                .filter(|k| matches(k))
                .cloned()
                .collect();
            for key in keys {
                push_typed(key, &mut seen, &mut result);
            }
        }

        result
    }

    /// Cursor-based SCAN across all key types (string, zset, geo, hash, list, set).
    ///
    /// Collects all matching keys, sorts them for a stable cursor, then returns
    /// up to `count` keys starting at `cursor` (treated as a start index).
    /// Next cursor is `start + returned_len`, or `0` when iteration is complete.
    pub fn scan(
        &self,
        cursor: u64,
        pattern: Option<&str>,
        count: usize,
    ) -> (u64, Vec<Bytes>) {
        let mut keys = self.keys(pattern);
        keys.sort();

        let start = cursor as usize;
        if start >= keys.len() {
            return (0, Vec::new());
        }

        let end = (start + count).min(keys.len());
        let batch = keys[start..end].to_vec();
        let next = if end >= keys.len() { 0 } else { end as u64 };
        (next, batch)
    }
}
