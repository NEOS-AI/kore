//! Logical Redis databases (SELECT / multi-DB keyspaces).
//!
//! Each logical DB is an independent `Cache` keyspace. Pub/Sub and connection
//! stats are shared via DB 0 (Redis semantics: pub/sub is not DB-scoped).

use crate::cache::Cache;
use std::sync::Arc;

/// Default number of logical databases (Redis default).
pub const DEFAULT_DATABASES: usize = 16;

/// Collection of logical databases.
pub struct Databases {
    dbs: Vec<Arc<Cache>>,
}

impl Databases {
    /// Wrap a single cache as DB 0 only (unit tests / simple embeds).
    pub fn single(cache: Arc<Cache>) -> Arc<Self> {
        Arc::new(Self { dbs: vec![cache] })
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
        Arc::new(Self { dbs })
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
        Arc::new(Self { dbs })
    }

    /// Move full keyspace state of every DB from `other` into `self`.
    ///
    /// **Exclusive access required** (see [`Cache::replace_keyspace_from`]).
    /// Callers should pause autosweep on all DBs for the whole call (see
    /// [`with_autosweep_paused_all`]); public load wrappers do this.
    /// DB count is matched by index; extra DBs on either side are ignored.
    ///
    /// # Consistency under concurrent readers
    ///
    /// This is **not** atomic across DBs from a concurrent reader's point of
    /// view. Install is still per-DB: a client SELECT'ing across DBs (or a
    /// FULLRESYNC observer) can see DB0 already replaced while DB1 is still
    /// empty (after a prior `flush=true` wipe) or still holding old data.
    /// True multi-DB consistency requires exclusive access — no concurrent
    /// client readers for the duration of this call. A server-wide load
    /// barrier / cross-DB atomic publish remains open (TODO).
    ///
    /// # Panic safety (partial staging)
    ///
    /// All source DBs are fully drained into staged payloads **before** any
    /// target is mutated. A panic while preparing source DBs leaves every
    /// target intact. A panic mid-install after DB *i* is committed still
    /// leaves DBs `0..=i` new and `i+1..` old/empty — not fully atomic.
    pub fn replace_keyspaces_from(&self, other: &Self) {
        let n = self.dbs.len().min(other.dbs.len());
        // Stage: drain every source first so a panic preparing later DBs does
        // not leave earlier targets already swapped.
        let mut staged = Vec::with_capacity(n);
        for i in 0..n {
            staged.push(other.dbs[i].take_keyspace_payload());
        }
        for (i, payload) in staged.into_iter().enumerate() {
            self.dbs[i].install_keyspace_payload(payload);
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
