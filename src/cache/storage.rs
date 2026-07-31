use crate::entry::{Entry, LoadOptions, SharedEntry, StoreOptions};
use crate::error::{Error, Result};
use crate::hashmap::{EntryAction, MapAction};
use crate::memory::MemoryCategory;
use crate::search_index::SearchIndex;
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::keyspace::{KeySlot, KeyValue};
use super::Cache;

/// Whether a key with the given slot expire is still live (no expire → live).
///
/// Batch FU/GA: string RMW call sites use this with the `slot_expires` argument
/// from [`Cache::mutate_string`]. `Entry` has no expire field (Batch GA).
#[inline]
pub(super) fn slot_ttl_live(slot_expires: Option<Instant>) -> bool {
    match slot_expires {
        Some(exp) => Instant::now() < exp,
        None => true,
    }
}

/// Drained keyspace held between multi-DB stage and install.
///
/// Built by [`Cache::take_keyspace_payload`]; consumed by
/// [`Cache::install_keyspace_payload`]. Not part of the public API.
///
/// **FG-4 / FP / FQ:** one `key_values` stream holds every type as [`KeySlot`]
/// (value + optional key-level expire). WATCH / search schema / memory counters
/// remain sibling fields. TTL lives on the slot for all types (no side map).
pub(crate) struct KeyspacePayload {
    /// All keys from [`Cache::key_values`] as slots (String + typed + expire).
    key_values: Vec<(Bytes, KeySlot)>,
    watch: HashMap<Bytes, u64>,
    indices: HashMap<String, Arc<RwLock<SearchIndex>>>,
    aliases: HashMap<String, String>,
    counts: [(MemoryCategory, usize); 8],
    mem: usize,
}

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
    ///
    /// Routed through [`Cache::get_key_value`] (unified keyspace facade).
    pub fn key_type(&self, key: &Bytes) -> KeyType {
        self.get_key_value(key)
            .map(|v| v.key_type())
            .unwrap_or(KeyType::None)
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
    ///
    /// Hot-path optimized (Batch FI / FG-4): probes `key_values` without a full
    /// facade walk. Non-string variants → WRONGTYPE; expired strings count as
    /// absent (same as SET overwrite semantics).
    pub fn ensure_string_or_absent(&self, key: &Bytes) -> Result<()> {
        if self.purge_if_expired(key) {
            return Ok(());
        }
        match self.key_values.get(key) {
            None => Ok(()),
            Some(slot) if matches!(slot.value, KeyValue::String(_)) => Ok(()),
            Some(_) => Err(Error::WrongType),
        }
    }

    /// Atomic string RMW under the key_values shard lock.
    ///
    /// If a non-string typed value occupies `key`, returns `Err(WrongType)`
    /// without calling `f` (no dual-residence / silent overwrite).
    ///
    /// # Batch FU / GA — slot-only TTL
    ///
    /// Callback signature:
    /// ```ignore
    /// FnOnce(
    ///     current: Option<&SharedEntry>,   // present string (even if expired)
    ///     slot_expires: Option<Instant>,   // key-level SoT (KeySlot only)
    ///     next_cas: u64,
    /// ) -> (EntryAction, Option<Instant>, R)
    /// ```
    /// For [`EntryAction::Set`], the second tuple element is the **new** slot
    /// expire (`KEEPTTL` = previous live `slot_expires`). `Entry` has no expire
    /// field (Batch GA). Use [`slot_ttl_live`] on `slot_expires` for
    /// live-vs-expired checks.
    pub(super) fn mutate_string<F, R>(&self, key: &Bytes, f: F) -> Result<R>
    where
        F: FnOnce(
            Option<&SharedEntry>,
            Option<Instant>,
            u64,
        ) -> (EntryAction, Option<Instant>, R),
    {
        self.key_values.mutate(key, |current, next_cas| {
            match current {
                Some(KeySlot {
                    value: KeyValue::String(entry),
                    expires_at,
                }) => {
                    let (action, new_exp, r) = f(Some(entry), *expires_at, next_cas);
                    (Self::string_map_action(action, new_exp), Ok(r))
                }
                Some(_) => (MapAction::Keep, Err(Error::WrongType)),
                None => {
                    let (action, new_exp, r) = f(None, None, next_cas);
                    (Self::string_map_action(action, new_exp), Ok(r))
                }
            }
        })
    }

    /// Lift string `EntryAction` onto a [`KeySlot`] with explicit slot expire.
    ///
    /// Batch FU/GA: `new_expires` is the sole TTL SoT on the slot.
    #[inline]
    fn string_map_action(
        action: EntryAction,
        new_expires: Option<Instant>,
    ) -> MapAction<KeySlot> {
        match action {
            EntryAction::Keep => MapAction::Keep,
            EntryAction::Set(e) => {
                MapAction::Set(KeySlot::with_expire(KeyValue::String(e), new_expires))
            }
            EntryAction::Remove => MapAction::Remove,
        }
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

        let max_memory = self.max_memory.load(Ordering::Relaxed);
        // 0 = unlimited (Redis-compatible CONFIG SET maxmemory 0) — skip total scan.
        if max_memory == 0 {
            return Ok(());
        }

        let tracker_ok = self
            .memory_tracker
            .can_allocate(needed, MemoryCategory::Cache);
        let total = self.memory_tracker.total_memory();
        let usage_ok = total.saturating_add(needed) <= max_memory;

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

        // Rough pre-check: account for memory that would be freed on replace.
        // When maxmemory is unlimited, skip the extra shard read (Batch FI).
        let max_memory = self.max_memory.load(Ordering::Relaxed);
        let net_memory_change = if max_memory == 0 {
            entry_size
        } else {
            let existing_size = self
                .get_string_entry(&key)
                .map(|e| e.size())
                .unwrap_or(0);
            entry_size.saturating_sub(existing_size)
        };
        self.ensure_capacity(net_memory_change)?;

        // Resolve absolute expiration outside the lock (keepttl handled under lock)
        let expires_at = Self::resolve_expiration(&opts);
        let keepttl = opts.keepttl && expires_at.is_none();
        let nx = opts.nx;
        let xx = opts.xx;
        let get = opts.get;
        let flags = opts.flags;
        let cas_expected = opts.cas;

        // Build entry shell outside the shard lock when we do not need keepttl
        // (expires resolved above). CAS/NX/XX still decide under the lock.
        // Moving `value` in avoids a second clone under the write lock (Batch FI).
        // Batch FU/GA: expire intent is returned separately (slot only).
        let outcome = match self.mutate_string(&key, |current, slot_exp, next_cas| {
            let live = slot_ttl_live(slot_exp);

            // NX: only set if not exists (treat expired as absent)
            if nx {
                if let Some(existing) = current {
                    if live {
                        return (
                            EntryAction::Keep,
                            None,
                            StoreOutcome::Exists(existing.clone()),
                        );
                    }
                }
            }

            // XX: only set if exists and not expired
            if xx {
                match current {
                    Some(_) if live => {}
                    _ => return (EntryAction::Keep, None, StoreOutcome::NotExists),
                }
            }

            // CAS compare-and-swap
            if let Some(expected_cas) = cas_expected {
                match current {
                    Some(existing) if live => {
                        if existing.cas != expected_cas {
                            return (EntryAction::Keep, None, StoreOutcome::CasMismatch);
                        }
                    }
                    _ => return (EntryAction::Keep, None, StoreOutcome::CasMiss),
                }
            }

            let old_for_get = if get {
                current.filter(|_| live).cloned()
            } else {
                None
            };

            let old_size = current.map(|e| e.size()).unwrap_or(0);

            // key still cloned for Entry + map slot; value is moved (single owner).
            // Slot owns TTL (Batch GA: Entry has no expires_at).
            let entry = Entry::new(key.clone(), value)
                .with_flags(flags)
                .with_cas(next_cas);
            let entry = Arc::new(entry);
            let new_size = entry.size();

            // New slot expire: explicit EX/PX/…, else KEEPTTL from previous slot, else clear.
            let new_slot_exp = if let Some(exp) = expires_at {
                Some(exp)
            } else if keepttl && live {
                slot_exp
            } else {
                None
            };

            (
                EntryAction::Set(entry),
                new_slot_exp,
                StoreOutcome::Stored {
                    old_for_get,
                    old_size,
                    new_size,
                },
            )
        }) {
            Ok(o) => o,
            Err(Error::WrongType) => return Err(Error::WrongType),
            Err(e) => return Err(e),
        };

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

        match self.key_values.get(key) {
            Some(slot) => {
                let expired = slot.is_expired();
                match slot.value {
                    KeyValue::String(entry) => {
                        if expired {
                            // Remove expired entry and free both counters
                            let size = entry.size();
                            if let Some(KeySlot {
                                value: KeyValue::String(_),
                                ..
                            }) = self.key_values.remove(key)
                            {
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
                            // Batch GA: Entry has no expire field. Remaining TTL
                            // is on the slot — callers use `Cache::ttl(key)`.
                            Ok(Some(entry))
                        }
                    }
                    _ => {
                        // Key exists as a non-string type — GET-family returns WrongType
                        // at the command layer; treat as miss here for raw load.
                        self.stats.incr(&self.stats.misses);
                        Ok(None)
                    }
                }
            }
            None => {
                self.stats.incr(&self.stats.misses);
                Ok(None)
            }
        }
    }

    /// Delete a key of any type (unified keyspace facade).
    ///
    /// Removes the slot via [`Cache::remove_key_value_raw`] (value + expire)
    /// and drops search-index documents for the name.
    pub fn delete(&self, key: &Bytes) -> Result<bool> {
        self.stats.incr(&self.stats.cmd_del);

        // Slot remove drops expire with the value (Batch FP / FQ).
        let deleted = self.remove_key_value_raw(key);

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

    /// Check if key exists (any type). Uses the unified keyspace facade.
    pub fn exists(&self, key: &Bytes) -> bool {
        self.get_key_value(key).is_some()
    }

    /// Get database size (all key types)
    pub fn dbsize(&self) -> usize {
        // FG-4: single unified map (includes strings)
        self.key_values.len()
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

    /// Drain this keyspace into a staged payload (maps left empty).
    ///
    /// Used by multi-DB replace so every source DB can be fully prepared before
    /// any target is mutated — a panic while draining later DBs leaves all
    /// targets intact. Single-DB [`replace_keyspace_from`] uses the same path.
    pub(crate) fn take_keyspace_payload(&self) -> KeyspacePayload {
        // FG-4 / FP / FQ: one stream for all types; key-level expire on each KeySlot.
        let key_values = self.key_values.drain_all();
        let watch = std::mem::take(&mut *self.watch_gens.lock());
        let (indices, aliases) = self.search_index_manager.take_all();
        let counts = self.memory_tracker.take_keyspace_counts();
        let mem = self.memory_usage.swap(0, Ordering::Relaxed);
        KeyspacePayload {
            key_values,
            watch,
            indices,
            aliases,
            counts,
            mem,
        }
    }

    /// Install a staged keyspace payload into `self`, returning the prior state.
    ///
    /// Pre-swap WATCH keys on `self` are re-tracked and bumped **atomically**
    /// with watch map install. Autosweep must be paused for the whole call.
    ///
    /// The returned payload is the drained pre-install keyspace. Single-DB
    /// callers may drop it immediately; multi-DB
    /// [`Databases::replace_keyspaces_from`] retains discards until every DB
    /// is installed so a panic mid-loop can reinstall olds (Batch DS).
    ///
    /// # Residual
    ///
    /// A panic **inside** this method (after drain, mid-fill) still leaves a
    /// single-DB tear; discards are only returned after fill completes.
    /// Command path relies on `-LOADING` for that window. FG-4 / FP / FQ: single
    /// `key_values` fill (slots carry key-level expire; no side expires map).
    pub(crate) fn install_keyspace_payload_retaining_discard(
        &self,
        payload: KeyspacePayload,
    ) -> KeyspacePayload {
        let pre_watch_keys: Vec<Bytes> = self.watch_gens.lock().keys().cloned().collect();

        // Drain target into discard, then install staged state.
        let discard_key_values = self.key_values.drain_all();
        let discard_watch = std::mem::take(&mut *self.watch_gens.lock());
        let (discard_indices, discard_aliases) = self.search_index_manager.take_all();
        let discard_counts = self.memory_tracker.take_keyspace_counts();
        let discard_mem = self.memory_usage.load(Ordering::Relaxed);

        // Map already drained into discard_* above — fill only (no second drain).
        self.key_values.fill_all(payload.key_values);
        // Install scratch watch map and bump pre-swap keys under one lock so
        // `watch_generation` cannot observe a clean empty map mid-replace.
        {
            let mut gens = self.watch_gens.lock();
            *gens = payload.watch;
            for k in pre_watch_keys {
                let g = gens.entry(k).or_insert(0);
                *g = g.wrapping_add(1);
            }
        }
        self.search_index_manager
            .install(payload.indices, payload.aliases);
        self.memory_tracker
            .install_keyspace_counts(&payload.counts);
        self.memory_usage.store(payload.mem, Ordering::Relaxed);

        KeyspacePayload {
            key_values: discard_key_values,
            watch: discard_watch,
            indices: discard_indices,
            aliases: discard_aliases,
            counts: discard_counts,
            mem: discard_mem,
        }
    }

    /// Install a staged keyspace payload into `self`, discarding prior data.
    ///
    /// See [`Self::install_keyspace_payload_retaining_discard`]. Drops the
    /// prior state immediately to shorten the dual-residency window.
    pub(crate) fn install_keyspace_payload(&self, payload: KeyspacePayload) {
        let _discard = self.install_keyspace_payload_retaining_discard(payload);
    }

    /// Move full keyspace state from `other` into `self` (map-level swap).
    ///
    /// **Exclusive access required** on both caches for the whole call: no
    /// concurrent client commands. Callers must also pause autosweep on `self`
    /// (see [`Cache::with_autosweep_paused`] / public load wrappers) so expire
    /// cannot race map/counter install. Intended only for AOF/RDB scratch-load
    /// commit after a successful decode/replay into `other`.
    ///
    /// Swaps: `key_values` (all types including strings; key-level expire on slots),
    /// watch_gens, search indices/aliases, MemoryTracker keyspace category
    /// counts, and `memory_usage`. Memory is moved only via tracker take/install
    /// + `memory_usage` store (never per-key `account` after map replace).
    ///
    /// Pre-swap WATCH keys on `self` are re-tracked and bumped **atomically**
    /// with watch map install so live clients with WATCH fail EXEC (same idea
    /// as FLUSHDB) without a clean-gen window.
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
        let payload = other.take_keyspace_payload();
        self.install_keyspace_payload(payload);
    }

    /// Non-mutating export of non-expired string entries for RDB snapshot /
    /// scratch seed. Does **not** touch LRU/LFU, bump stats, or lazy-delete
    /// expired keys (expired are simply skipped).
    pub fn export_strings(&self) -> Vec<(Bytes, Bytes, u32, i64)> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let now_instant = Instant::now();
        let mut out = Vec::new();
        self.key_values.for_each(|_key, slot| {
            let KeyValue::String(entry) = &slot.value else {
                return;
            };
            if slot.is_expired() {
                return;
            }
            let expire_unix_ms = match slot.expires() {
                Some(exp) if exp > now_instant => {
                    now + exp.duration_since(now_instant).as_millis() as i64
                }
                Some(_) => return, // past due
                None => -1,
            };
            out.push((
                entry.key.clone(),
                entry.value.clone(),
                entry.flags,
                expire_unix_ms,
            ));
        });
        out
    }

    /// Clear the unified key map (slots include key-level expires; not search schema).
    fn flush_keyspace(&self) {
        self.key_values.clear();
    }

    /// All non-expired string keys in the unified map (for persistence / migrate).
    pub fn map_keys_all(&self) -> Vec<Bytes> {
        let mut out = Vec::new();
        self.key_values.for_each(|key, slot| {
            if matches!(slot.value, KeyValue::String(_)) && !slot.is_expired() {
                out.push(key.clone());
            }
        });
        out
    }

    /// Get all keys matching a pattern across the unified keyspace.
    pub fn keys(&self, pattern: Option<&str>) -> Vec<Bytes> {
        let mut result = Vec::new();

        // FG-4 / FP / FQ: single map — purge expired keys (all types) via slot TTL.
        for key in self.key_values.keys(pattern) {
            if self.purge_if_expired(&key) {
                continue;
            }
            if self.key_values.get(&key).is_none() {
                continue;
            }
            result.push(key);
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
