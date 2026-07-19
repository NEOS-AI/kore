//! Logical Redis databases (SELECT / multi-DB keyspaces).
//!
//! Each logical DB is an independent `Cache` keyspace. Pub/Sub and connection
//! stats are shared via DB 0 (Redis semantics: pub/sub is not DB-scoped).

use crate::cache::{Cache, KeyspacePayload};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// Default number of logical databases (Redis default).
pub const DEFAULT_DATABASES: usize = 16;

/// Collection of logical databases.
pub struct Databases {
    dbs: Vec<Arc<Cache>>,
    /// Bumped once when a multi-DB keyspace replace **finishes** (success or
    /// panic via drop guard). Frozen for the whole install so observers cannot
    /// treat a mid-loop gen as a published epoch. Pair with
    /// [`Self::load_in_progress`] / [`Self::with_stable_keyspace_view`].
    load_generation: AtomicU64,
    /// True while [`Self::replace_keyspaces_from`] is running.
    load_in_progress: AtomicBool,
    /// Multi-DB keyspace epoch lock.
    ///
    /// - **Write**: held for the whole multi-DB install loop (after staging).
    /// - **Read**: held by multi-DB exporters / consistent multi-DB observers
    ///   ([`Self::with_stable_keyspace_view`]) so they never sample DB0-new +
    ///   DB1-old mid-install.
    ///
    /// Does **not** block per-DB command paths that already hold `Arc<Cache>`
    /// (those are gated by `-LOADING` on the public command path).
    keyspace_epoch_lock: RwLock<()>,
    /// Optional probe invoked after each DB is installed during
    /// [`Self::replace_keyspaces_from`] (0-based DB index). Held **under** the
    /// epoch write lock — hooks must not call [`Self::with_stable_keyspace_view`]
    /// (would deadlock). Use [`Self::try_with_stable_keyspace_view`] to observe
    /// exclusion.
    ///
    /// **Production always leaves this `None`.** Kept on the type (not
    /// `#[cfg(test)]`) because integration tests under `tests/` link the
    /// library without `cfg(test)` on the crate; a `test-hooks` feature would
    /// also force CI to pass `--features`. Harmless when unset (empty mutex).
    after_install_db: parking_lot::Mutex<Option<Arc<dyn Fn(usize) + Send + Sync>>>,
}

impl Databases {
    /// Wrap a single cache as DB 0 only (unit tests / simple embeds).
    pub fn single(cache: Arc<Cache>) -> Arc<Self> {
        Arc::new(Self {
            dbs: vec![cache],
            load_generation: AtomicU64::new(0),
            load_in_progress: AtomicBool::new(false),
            keyspace_epoch_lock: RwLock::new(()),
            after_install_db: parking_lot::Mutex::new(None),
        })
    }

    /// Build `num_dbs` keyspaces. DB 0 is primary; siblings share pubsub + stats.
    pub fn create(
        num_dbs: usize,
        num_shards: usize,
        max_memory: usize,
        max_entry_size: usize,
        start_sweep: bool,
        loadfactor: f64,
    ) -> Arc<Self> {
        let n = num_dbs.max(1);
        let db0 = Cache::new_with_sweep_loadfactor(
            num_shards,
            max_memory,
            max_entry_size,
            start_sweep,
            loadfactor,
        );
        let mut dbs = Vec::with_capacity(n);
        dbs.push(db0.clone());
        for _ in 1..n {
            dbs.push(Cache::new_keyspace_sharing(
                &db0,
                num_shards,
                max_memory,
                max_entry_size,
                start_sweep,
                loadfactor,
            ));
        }
        Arc::new(Self {
            dbs,
            load_generation: AtomicU64::new(0),
            load_in_progress: AtomicBool::new(false),
            keyspace_epoch_lock: RwLock::new(()),
            after_install_db: parking_lot::Mutex::new(None),
        })
    }

    pub fn len(&self) -> usize {
        self.dbs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.dbs.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<Arc<Cache>> {
        self.dbs.get(index).cloned()
    }

    /// Primary database (DB 0) — used for persistence, pub/sub registration, stats.
    pub fn db0(&self) -> Arc<Cache> {
        self.dbs[0].clone()
    }

    /// Iterate logical DBs. **Not** a multi-DB consistent snapshot by itself —
    /// concurrent [`replace_keyspaces_from`] can make a raw walk see DB0-new +
    /// DB1-old. Multi-DB keyspace exporters must wrap the walk in
    /// [`Self::with_stable_keyspace_view`] (see RDB `from_databases`, AOF
    /// `rewrite_databases`). Config / blocker / stats walks that do not sample
    /// key contents are fine without the epoch lock.
    pub fn iter(&self) -> impl Iterator<Item = &Arc<Cache>> {
        self.dbs.iter()
    }

    /// FLUSHALL: clear every logical database (keys + search docs; keep FT schema).
    pub fn flush_all(&self) {
        for db in &self.dbs {
            db.flush();
        }
    }

    /// Full wipe of every logical database including FT index definitions/aliases.
    /// Hard reset helper — not live FLUSHALL. AOF/RDB public load paths use
    /// scratch-load + [`replace_keyspaces_from`] instead of wiping on failure.
    pub fn flush_all_including_search(&self) {
        for db in &self.dbs {
            db.flush_all_including_search();
        }
    }

    /// Empty multi-DB collection matching this instance's DB count and per-DB
    /// shard / memory config. Shares pubsub (via each DB's `empty_keyspace_like`)
    /// and does **not** start background sweeps — exclusive load-time use only.
    pub fn empty_like(&self) -> Arc<Self> {
        let mut dbs = Vec::with_capacity(self.dbs.len());
        for db in &self.dbs {
            dbs.push(db.empty_keyspace_like());
        }
        Arc::new(Self {
            dbs,
            load_generation: AtomicU64::new(0),
            load_in_progress: AtomicBool::new(false),
            keyspace_epoch_lock: RwLock::new(()),
            after_install_db: parking_lot::Mutex::new(None),
        })
    }

    /// Monotonic generation bumped when multi-DB keyspace replace finishes.
    ///
    /// Increases once per replace attempt (success or panic via drop guard).
    /// Frozen during install (no mid-loop publish). Useful for tests /
    /// diagnostics together with [`Self::load_in_progress`].
    pub fn load_generation(&self) -> u64 {
        self.load_generation.load(Ordering::Acquire)
    }

    /// True while a multi-DB keyspace replace is in progress.
    pub fn load_in_progress(&self) -> bool {
        self.load_in_progress.load(Ordering::Acquire)
    }

    /// Run `f` while holding the multi-DB keyspace epoch **read** lock.
    ///
    /// Blocks if a multi-DB install is mid-loop, then observes a consistent
    /// all-old or all-new multi-DB view (no DB0-new + DB1-old tear). Use for
    /// multi-DB export (`MultiDbSnapshot::from_databases`, SAVE internals).
    pub fn with_stable_keyspace_view<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let _guard: RwLockReadGuard<'_, ()> = self.keyspace_epoch_lock.read();
        f()
    }

    /// Non-blocking variant of [`Self::with_stable_keyspace_view`].
    ///
    /// Returns `None` if a multi-DB install currently holds the epoch write
    /// lock. Intended for probes / tests that must not deadlock under an
    /// `after_install_db` hook.
    pub fn try_with_stable_keyspace_view<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce() -> R,
    {
        let _guard = self.keyspace_epoch_lock.try_read()?;
        Some(f())
    }

    /// Install-time probe hook (tests). See [`Self::after_install_db`].
    pub fn set_after_install_db_hook(
        &self,
        hook: Option<Arc<dyn Fn(usize) + Send + Sync>>,
    ) {
        *self.after_install_db.lock() = hook;
    }

    /// Run `f` while [`load_in_progress`] is forced true (tests / probes).
    ///
    /// Restores the previous flag on drop (panic-safe). Does **not** bump
    /// [`load_generation`] (unlike a real replace) and does **not** take the
    /// epoch write lock.
    pub fn with_load_in_progress_flag<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let prev = self.load_in_progress.load(Ordering::Acquire);
        self.load_in_progress.store(true, Ordering::Release);
        struct Restore<'a> {
            dbs: &'a Databases,
            prev: bool,
        }
        impl Drop for Restore<'_> {
            fn drop(&mut self) {
                self.dbs
                    .load_in_progress
                    .store(self.prev, Ordering::Release);
            }
        }
        let _g = Restore {
            dbs: self,
            prev,
        };
        f()
    }

    /// Move full keyspace state of every DB from `other` into `self`.
    ///
    /// **Exclusive access required** (see [`Cache::replace_keyspace_from`]).
    /// Callers should pause autosweep on all DBs for the whole call (see
    /// [`with_autosweep_paused_all`]); public load wrappers do this.
    /// DB count is matched by index; extra DBs on either side are ignored.
    ///
    /// Sets [`load_in_progress`] for the duration. Bumps [`load_generation`]
    /// once when the call finishes (success **or** panic via drop guard).
    ///
    /// # Consistency under concurrent readers
    ///
    /// **Lock-step install (Batch DR):** after staging every source payload,
    /// all target DBs are installed under a single `keyspace_epoch_lock`
    /// write section. Multi-DB observers that take
    /// [`Self::with_stable_keyspace_view`] (e.g. RDB `from_databases` / SAVE,
    /// AOF `rewrite_databases`) either finish before that section or block
    /// until **all** DBs are new — they never sample DB0-new + DB1-old.
    ///
    /// Public command path still returns **`-LOADING`** while
    /// [`load_in_progress`] is true (data plane + SYNC/PSYNC). Peak dual-
    /// residency during stage is ~old multi-DB + full staged scratch (~2×);
    /// during install, discarded olds of already-installed DBs are retained
    /// until the loop commits (extra peak for panic rollback — Batch DS).
    ///
    /// # Panic safety
    ///
    /// - **Staging:** all source DBs are fully drained into staged payloads
    ///   **before** any target is mutated. A panic while preparing source DBs
    ///   leaves every target intact.
    /// - **Install loop (Batch DS):** each DB install retains the prior
    ///   payload. If install panics after DB *i* is fully swapped (e.g. test
    ///   hook, later DB install), a drop guard **reinstalls** olds for
    ///   `0..=i` while still holding the epoch write lock. Survivors then see
    ///   the pre-replace multi-DB dataset (plus a bumped [`load_generation`]).
    ///
    /// # Residuals
    ///
    /// - **Panic mid-single-DB `install_keyspace_payload_*`** (after drain,
    ///   mid multi-map fill) is not rolled back — that DB stays torn. True
    ///   all-or-nothing single-DB install needs Arc-swap of maps (Option C).
    /// - **Raw per-DB `Arc<Cache>` access** that bypasses the epoch lock and
    ///   the command gate can still observe a mid-loop multi-DB tear while
    ///   install is in progress (and mid-payload single-DB map tear).
    ///   Privileged allowlisted commands that only touch the selected DB or
    ///   non-keyspace metadata (e.g. blocked-client counts) are not multi-DB
    ///   exporters; document if expanded.
    pub fn replace_keyspaces_from(&self, other: &Self) {
        self.load_in_progress.store(true, Ordering::Release);
        struct LoadFlag<'a>(&'a Databases);
        impl Drop for LoadFlag<'_> {
            fn drop(&mut self) {
                // Publish generation only at end (frozen during install).
                self.0.load_generation.fetch_add(1, Ordering::AcqRel);
                self.0.load_in_progress.store(false, Ordering::Release);
            }
        }
        let _flag = LoadFlag(self);

        let n = self.dbs.len().min(other.dbs.len());
        // Stage: drain every source first so a panic preparing later DBs does
        // not leave earlier targets already swapped. Staging is outside the
        // epoch write lock so multi-DB exporters can still finish an all-old
        // snapshot while sources are prepared.
        let mut staged = Vec::with_capacity(n);
        for i in 0..n {
            staged.push(other.dbs[i].take_keyspace_payload());
        }

        // Lock-step install: one exclusive section for every DB. Retain
        // discarded olds so a panic mid-loop can restore already-swapped DBs
        // before the epoch write is released (Drop order: rollback then epoch).
        {
            let _epoch: RwLockWriteGuard<'_, ()> = self.keyspace_epoch_lock.write();

            /// Restores already-installed DBs from retained discards on panic.
            ///
            /// Declared after `_epoch` so it drops first while the write lock
            /// is still held — rollback never races multi-DB exporters.
            struct InstallRollback<'a> {
                dbs: &'a Databases,
                /// `(db_index, pre-install payload)` for DBs fully installed.
                installed: Vec<(usize, KeyspacePayload)>,
                committed: bool,
            }
            impl Drop for InstallRollback<'_> {
                fn drop(&mut self) {
                    if self.committed {
                        return;
                    }
                    // Reverse order is fine; whole restore is under epoch write.
                    while let Some((i, old)) = self.installed.pop() {
                        // Drop the half-new maps; reinstall pre-replace state.
                        self.dbs.dbs[i].install_keyspace_payload(old);
                    }
                }
            }

            let mut rollback = InstallRollback {
                dbs: self,
                installed: Vec::with_capacity(n),
                committed: false,
            };
            for (i, payload) in staged.into_iter().enumerate() {
                let old = self.dbs[i].install_keyspace_payload_retaining_discard(payload);
                rollback.installed.push((i, old));
                if let Some(ref hook) = *self.after_install_db.lock() {
                    hook(i);
                }
            }
            rollback.committed = true;
            // Drop retained discards after successful commit (shorten dual-residency).
        }
    }

    /// Run `f` with autosweep disabled on every DB; restore each DB's previous
    /// flag afterward (panic-safe). Used around multi-DB keyspace replace.
    pub fn with_autosweep_paused_all<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let prev: Vec<bool> = self.dbs.iter().map(|db| db.autosweep_enabled()).collect();
        for db in &self.dbs {
            db.set_autosweep(false);
        }
        struct Restore<'a> {
            dbs: &'a Databases,
            prev: Vec<bool>,
        }
        impl Drop for Restore<'_> {
            fn drop(&mut self) {
                for (db, &was) in self.dbs.dbs.iter().zip(self.prev.iter()) {
                    db.set_autosweep(was);
                }
            }
        }
        let _guard = Restore {
            dbs: self,
            prev,
        };
        f()
    }

    /// Enable or disable autosweep on every logical DB.
    pub fn set_autosweep_all(&self, enabled: bool) {
        for db in &self.dbs {
            db.set_autosweep(enabled);
        }
    }

    /// Spawn background sweep tasks on every DB (after startup load).
    pub fn start_background_sweep_all(self: &Arc<Self>) {
        for db in &self.dbs {
            db.start_background_sweep();
        }
    }

    /// SWAPDB: exchange all keys (all types + TTLs) between two logical DBs.
    /// Content swap so existing `Arc<Cache>` holders observe the new data.
    pub fn swap_db(&self, a: usize, b: usize) -> Result<(), String> {
        if a >= self.dbs.len() || b >= self.dbs.len() {
            return Err("ERR DB index is out of range".into());
        }
        if a == b {
            return Ok(());
        }
        let da = &self.dbs[a];
        let db = &self.dbs[b];

        let keys_a = da.keys(None);
        let keys_b = db.keys(None);
        let mut payloads_a = Vec::with_capacity(keys_a.len());
        for k in &keys_a {
            if let Some(p) = da.dump_key(k) {
                payloads_a.push((k.clone(), p));
            }
        }
        let mut payloads_b = Vec::with_capacity(keys_b.len());
        for k in &keys_b {
            if let Some(p) = db.dump_key(k) {
                payloads_b.push((k.clone(), p));
            }
        }

        da.flush();
        db.flush();

        for (k, p) in payloads_b {
            da.restore_key(&k, &p, true)
                .map_err(|e| e.to_resp_string())?;
        }
        for (k, p) in payloads_a {
            db.restore_key(&k, &p, true)
                .map_err(|e| e.to_resp_string())?;
        }
        Ok(())
    }

    /// Apply eviction policy to every DB (CONFIG SET maxmemory-policy).
    pub fn set_eviction_policy_all(&self, policy: crate::cache::EvictionPolicy) {
        for db in &self.dbs {
            db.set_eviction_policy(policy);
        }
    }

    /// Apply maxmemory to every DB.
    pub fn set_max_memory_all(&self, max_memory: usize) {
        for db in &self.dbs {
            let _ = db.set_max_memory(max_memory);
        }
    }
}
