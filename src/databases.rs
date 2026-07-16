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

    /// FLUSHALL: clear every logical database.
    pub fn flush_all(&self) {
        for db in &self.dbs {
            db.flush();
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
