//! Persistence and replication: RDB snapshots, AOF, async replica feeds.

pub mod aof;
pub mod rdb;
pub mod replication;

use crate::cache::Cache;
use crate::databases::Databases;
use crate::error::{Error, Result};
use parking_lot::Mutex;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;
use tracing::{info, warn};

use self::aof::AofWriter;
use self::replication::ReplicationManager;

/// One Redis-style `save <seconds> <changes>` rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SaveRule {
    /// Minimum seconds since last successful save.
    pub seconds: u64,
    /// Minimum number of write changes since last successful save.
    pub changes: u64,
}

impl SaveRule {
    pub fn new(seconds: u64, changes: u64) -> Self {
        Self { seconds, changes }
    }
}

/// Default Redis-compatible save rules: `900 1`, `300 10`, `60 10000`.
pub fn default_save_rules() -> Vec<SaveRule> {
    vec![
        SaveRule::new(900, 1),
        SaveRule::new(300, 10),
        SaveRule::new(60, 10000),
    ]
}

/// Parse save policy string.
///
/// Accepts:
/// - empty / `""` → no rules (auto-save disabled)
/// - comma form: `900,1 300,10 60,10000`
/// - Redis form: `900 1 300 10 60 10000`
pub fn parse_save_rules(s: &str) -> std::result::Result<Vec<SaveRule>, String> {
    let s = s.trim();
    if s.is_empty() || s == "\"\"" {
        return Ok(Vec::new());
    }

    let tokens: Vec<&str> = s.split_whitespace().collect();
    let mut rules = Vec::with_capacity(tokens.len());
    let mut i = 0;
    while i < tokens.len() {
        let tok = tokens[i];
        if tok.contains(',') {
            let mut parts = tok.split(',');
            let sec = parts
                .next()
                .and_then(|p| p.parse::<u64>().ok())
                .ok_or_else(|| format!("invalid save rule '{}'", tok))?;
            let chg = parts
                .next()
                .and_then(|p| p.parse::<u64>().ok())
                .ok_or_else(|| format!("invalid save rule '{}'", tok))?;
            if parts.next().is_some() {
                return Err(format!("invalid save rule '{}'", tok));
            }
            if sec == 0 && chg == 0 {
                return Err("save rule seconds and changes cannot both be 0".into());
            }
            rules.push(SaveRule::new(sec, chg));
            i += 1;
        } else {
            if i + 1 >= tokens.len() {
                return Err(format!(
                    "invalid save directive near '{}': expected seconds changes pairs",
                    tok
                ));
            }
            let sec: u64 = tokens[i]
                .parse()
                .map_err(|_| format!("invalid save seconds '{}'", tokens[i]))?;
            let chg: u64 = tokens[i + 1]
                .parse()
                .map_err(|_| format!("invalid save changes '{}'", tokens[i + 1]))?;
            if sec == 0 && chg == 0 {
                return Err("save rule seconds and changes cannot both be 0".into());
            }
            rules.push(SaveRule::new(sec, chg));
            i += 2;
        }
    }
    Ok(rules)
}

/// Format rules as Redis CONFIG GET save string: `"900 1 300 10"`.
pub fn format_save_rules(rules: &[SaveRule]) -> String {
    rules
        .iter()
        .map(|r| format!("{} {}", r.seconds, r.changes))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Runtime persistence configuration (derived from CLI Config).
#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    pub dir: PathBuf,
    pub dbfilename: String,
    pub appendonly: bool,
    pub appendfilename: String,
    /// Timed RDB save policies (empty = disabled).
    pub save_rules: Vec<SaveRule>,
}

impl PersistenceConfig {
    pub fn rdb_path(&self) -> PathBuf {
        self.dir.join(&self.dbfilename)
    }

    pub fn aof_path(&self) -> PathBuf {
        self.dir.join(&self.appendfilename)
    }
}

/// Live AOF writer + last selected DB for lazy SELECT emission.
///
/// Bundled under one mutex so decide-select, append SELECT, append command,
/// and update-selected are atomic — concurrent writers on different DBs cannot
/// interleave SELECT and write commands in the AOF / replication stream.
struct AofLiveState {
    writer: Option<AofWriter>,
    /// Last DB index written to the live AOF.
    /// `None` means unknown — next write will emit SELECT if needed.
    selected_db: Option<usize>,
}

/// Coordinates RDB / AOF and exposes hooks for the command layer.
pub struct PersistenceManager {
    config: PersistenceConfig,
    last_save_unix: AtomicU64,
    /// Wall-clock mark for policy elapsed time (process start or last successful save).
    last_save_at: Mutex<Instant>,
    /// Dataset mutations since last successful RDB save.
    dirty_changes: AtomicU64,
    /// Live-updatable save rules (CONFIG SET save).
    save_rules: Mutex<Vec<SaveRule>>,
    bgsave_in_progress: AtomicBool,
    aof: Mutex<AofLiveState>,
    pub replication: Arc<ReplicationManager>,
}

impl PersistenceManager {
    pub fn new(config: PersistenceConfig) -> Result<Arc<Self>> {
        let aof = if config.appendonly {
            Some(AofWriter::open(config.aof_path())?)
        } else {
            None
        };

        let save_rules = config.save_rules.clone();

        Ok(Arc::new(Self {
            config,
            last_save_unix: AtomicU64::new(0),
            last_save_at: Mutex::new(Instant::now()),
            dirty_changes: AtomicU64::new(0),
            save_rules: Mutex::new(save_rules),
            bgsave_in_progress: AtomicBool::new(false),
            aof: Mutex::new(AofLiveState {
                writer: aof,
                selected_db: None,
            }),
            replication: ReplicationManager::new(),
        }))
    }

    pub fn config(&self) -> &PersistenceConfig {
        &self.config
    }

    pub fn last_save_unix(&self) -> u64 {
        self.last_save_unix.load(Ordering::Relaxed)
    }

    pub fn bgsave_in_progress(&self) -> bool {
        self.bgsave_in_progress.load(Ordering::Relaxed)
    }

    pub fn appendonly(&self) -> bool {
        self.config.appendonly
    }

    /// Changes since last successful RDB save.
    pub fn dirty_changes(&self) -> u64 {
        self.dirty_changes.load(Ordering::Relaxed)
    }

    /// Increment dirty counter (dataset mutation).
    pub fn mark_dirty(&self) {
        self.dirty_changes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn save_rules(&self) -> Vec<SaveRule> {
        self.save_rules.lock().clone()
    }

    /// Replace save rules (CONFIG SET save). Empty disables auto-save.
    pub fn set_save_rules(&self, rules: Vec<SaveRule>) {
        *self.save_rules.lock() = rules;
    }

    /// Parse and set save rules from a CONFIG-style string.
    pub fn set_save_rules_from_str(&self, s: &str) -> Result<()> {
        let rules = parse_save_rules(s).map_err(Error::ConfigError)?;
        self.set_save_rules(rules);
        Ok(())
    }

    pub fn save_rules_string(&self) -> String {
        format_save_rules(&self.save_rules())
    }

    /// Seconds elapsed since last successful save (or manager creation).
    pub fn seconds_since_last_save(&self) -> u64 {
        self.last_save_at.lock().elapsed().as_secs()
    }

    /// Pretend the last save was `ago` in the past (used by tests / diagnostics).
    pub fn set_last_save_age(&self, ago: Duration) {
        *self.last_save_at.lock() = Instant::now()
            .checked_sub(ago)
            .unwrap_or_else(Instant::now);
    }

    /// Load data at startup into multi-DB keyspaces: prefer AOF if appendonly, else RDB.
    pub fn load_at_startup(&self, databases: &Arc<Databases>) -> Result<()> {
        if self.config.appendonly {
            let path = self.config.aof_path();
            if path.exists() {
                info!("Loading AOF from {}", path.display());
                let n = aof::load_into_databases(databases, &path)?;
                info!("AOF loaded ({} commands)", n);
                return Ok(());
            }
            // Fall through to RDB if AOF missing
        }

        let path = self.config.rdb_path();
        if path.exists() {
            info!("Loading RDB from {}", path.display());
            let n = rdb::load_databases(databases, &path, true)?;
            info!("RDB loaded ({} keys)", n);
            self.touch_last_save();
            self.dirty_changes.store(0, Ordering::Relaxed);
        } else {
            info!("No RDB/AOF found at startup — empty dataset");
        }
        Ok(())
    }

    /// Load into a single cache (DB 0 / first DB). Kept for tests and embeds.
    pub fn load_at_startup_cache(&self, cache: &Arc<Cache>) -> Result<()> {
        if self.config.appendonly {
            let path = self.config.aof_path();
            if path.exists() {
                info!("Loading AOF from {}", path.display());
                let n = aof::load_into_cache(cache, &path)?;
                info!("AOF loaded ({} commands)", n);
                return Ok(());
            }
        }

        let path = self.config.rdb_path();
        if path.exists() {
            info!("Loading RDB from {}", path.display());
            let n = rdb::load_file(cache, &path, true)?;
            info!("RDB loaded ({} keys)", n);
            self.touch_last_save();
            self.dirty_changes.store(0, Ordering::Relaxed);
        } else {
            info!("No RDB/AOF found at startup — empty dataset");
        }
        Ok(())
    }

    fn touch_last_save(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.last_save_unix.store(now, Ordering::Relaxed);
        *self.last_save_at.lock() = Instant::now();
    }

    /// Synchronous SAVE of all non-empty logical databases.
    pub fn save(&self, databases: &Databases) -> Result<()> {
        let path = self.config.rdb_path();
        rdb::save_databases(databases, &path)?;
        self.touch_last_save();
        self.dirty_changes.store(0, Ordering::Relaxed);
        info!("RDB saved to {}", path.display());
        Ok(())
    }

    /// Synchronous SAVE of a single cache (DB 0 snapshot with streams). Tests / embeds.
    pub fn save_cache(&self, cache: &Cache) -> Result<()> {
        let path = self.config.rdb_path();
        rdb::save_file(cache, &path)?;
        self.touch_last_save();
        self.dirty_changes.store(0, Ordering::Relaxed);
        info!("RDB saved to {}", path.display());
        Ok(())
    }

    /// Background SAVE of all databases. Returns false if already running.
    pub fn bgsave(self: &Arc<Self>, databases: Arc<Databases>) -> bool {
        if self
            .bgsave_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return false;
        }
        let mgr = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let path = mgr.config.rdb_path();
            match rdb::save_databases(&databases, &path) {
                Ok(()) => {
                    mgr.touch_last_save();
                    mgr.dirty_changes.store(0, Ordering::Relaxed);
                    info!("BGSAVE completed: {}", path.display());
                }
                Err(e) => warn!("BGSAVE failed: {}", e),
            }
            mgr.bgsave_in_progress.store(false, Ordering::SeqCst);
        });
        true
    }

    /// Background SAVE of a single cache. Returns false if already running.
    pub fn bgsave_cache(self: &Arc<Self>, cache: Arc<Cache>) -> bool {
        if self
            .bgsave_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return false;
        }
        let mgr = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let path = mgr.config.rdb_path();
            match rdb::save_file(&cache, &path) {
                Ok(()) => {
                    mgr.touch_last_save();
                    mgr.dirty_changes.store(0, Ordering::Relaxed);
                    info!("BGSAVE completed: {}", path.display());
                }
                Err(e) => warn!("BGSAVE failed: {}", e),
            }
            mgr.bgsave_in_progress.store(false, Ordering::SeqCst);
        });
        true
    }

    /// If any save rule matches (elapsed + dirty), start BGSAVE for all databases.
    /// Returns true if a background save was started.
    pub fn maybe_auto_save(self: &Arc<Self>, databases: &Arc<Databases>) -> bool {
        if self.bgsave_in_progress() {
            return false;
        }
        let dirty = self.dirty_changes();
        if dirty == 0 {
            return false;
        }
        let elapsed = self.seconds_since_last_save();
        let rules = self.save_rules();
        for rule in rules {
            if elapsed >= rule.seconds && dirty >= rule.changes {
                if self.bgsave(Arc::clone(databases)) {
                    info!(
                        "Auto BGSAVE triggered (save {} {}): dirty={} elapsed={}s",
                        rule.seconds, rule.changes, dirty, elapsed
                    );
                    return true;
                }
                return false;
            }
        }
        false
    }

    /// Single-cache auto-save (tests / embeds).
    pub fn maybe_auto_save_cache(self: &Arc<Self>, cache: &Arc<Cache>) -> bool {
        if self.bgsave_in_progress() {
            return false;
        }
        let dirty = self.dirty_changes();
        if dirty == 0 {
            return false;
        }
        let elapsed = self.seconds_since_last_save();
        let rules = self.save_rules();
        for rule in rules {
            if elapsed >= rule.seconds && dirty >= rule.changes {
                if self.bgsave_cache(Arc::clone(cache)) {
                    info!(
                        "Auto BGSAVE triggered (save {} {}): dirty={} elapsed={}s",
                        rule.seconds, rule.changes, dirty, elapsed
                    );
                    return true;
                }
                return false;
            }
        }
        false
    }

    /// Spawn a 1s-tick task that evaluates save policies until shutdown.
    pub fn spawn_auto_save_scheduler(
        self: &Arc<Self>,
        databases: Arc<Databases>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mgr = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Skip the immediate first tick so we don't race with startup load.
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        let _ = mgr.maybe_auto_save(&databases);
                    }
                }
            }
        });
    }

    /// Rewrite AOF from all non-empty databases.
    pub fn rewrite_aof(&self, databases: &Databases) -> Result<()> {
        let path = self.config.aof_path();
        aof::rewrite_databases(databases, &path)?;
        // Re-open writer at end of new file; force SELECT on next write.
        // Same mutex as on_write_command — no lock-order deadlock with concurrent writers.
        let mut state = self.aof.lock();
        if self.config.appendonly {
            state.writer = Some(AofWriter::open(&path)?);
        }
        state.selected_db = None;
        info!("AOF rewrite completed: {}", path.display());
        Ok(())
    }

    /// Rewrite AOF from a single cache (tests / embeds).
    pub fn rewrite_aof_cache(&self, cache: &Cache) -> Result<()> {
        let path = self.config.aof_path();
        aof::rewrite(cache, &path)?;
        let mut state = self.aof.lock();
        if self.config.appendonly {
            state.writer = Some(AofWriter::open(&path)?);
        }
        state.selected_db = None;
        info!("AOF rewrite completed: {}", path.display());
        Ok(())
    }

    /// Log a write command to AOF (if enabled), mark dirty, and propagate to replicas.
    ///
    /// When the selected DB changes relative to the last recorded write, a
    /// `SELECT n` command is emitted first (and also propagated to replicas).
    ///
    /// **AOF enabled:** decide-select, AOF append(s), and AOF `selected_db`
    /// update run under the AOF mutex. Replication uses
    /// [`ReplicationManager::propagate_write`], which decides stream SELECT and
    /// appends under the repl publish lock (still called while holding AOF so
    /// disk and stream order match for concurrent writers).
    ///
    /// **AOF disabled (common bench / standalone):** the AOF mutex is **not**
    /// taken. Command encode runs outside the repl lock; SELECT decision +
    /// backlog append are atomic inside `propagate_write` so concurrent multi-DB
    /// writers cannot interleave a SELECT-less command ahead of another thread’s
    /// SELECT (Batch FI-2).
    ///
    /// When SELECT is needed, SELECT+cmd are one contiguous `propagate` payload
    /// so a concurrent PSYNC cannot register a feed between SELECT and the write.
    /// AOF still records SELECT and the write as two separate appends.
    pub fn on_write_command(&self, selected_db: usize, args: &[bytes::Bytes]) {
        self.mark_dirty();

        // ── Fast path: no AOF file — stream SELECT owned by replication ──
        if !self.config.appendonly {
            // No aof.lock: selected_db for the repl stream is tracked inside
            // propagate_write under the same critical section as backlog append.
            self.replication.propagate_write(selected_db, args);
            return;
        }

        // ── AOF path: hold lock across SELECT + command appends for disk order ──
        let mut state = self.aof.lock();

        let emit_select = match state.selected_db {
            Some(db) => db != selected_db,
            // Unknown (startup / post-rewrite): emit SELECT only when not DB 0.
            None => selected_db != 0,
        };

        if emit_select {
            let select_args = [
                bytes::Bytes::from_static(b"SELECT"),
                bytes::Bytes::from(selected_db.to_string()),
            ];
            // AOF: SELECT then command as separate appends (atomic under this lock).
            if let Some(ref mut writer) = state.writer {
                if let Err(e) = writer.append_command(&select_args) {
                    warn!("AOF SELECT append failed: {}", e);
                }
                if let Err(e) = writer.append_command(args) {
                    warn!("AOF append failed: {}", e);
                }
            }
        } else if let Some(ref mut writer) = state.writer {
            if let Err(e) = writer.append_command(args) {
                warn!("AOF append failed: {}", e);
            }
        }
        state.selected_db = Some(selected_db);
        // Replication owns stream SELECT under its own lock; holding aof here
        // keeps AOF disk order aligned with stream order across writers.
        self.replication.propagate_write(selected_db, args);
    }

    pub fn rdb_path(&self) -> PathBuf {
        self.config.rdb_path()
    }

    pub fn aof_path(&self) -> PathBuf {
        self.config.aof_path()
    }

    /// Ensure data directory exists.
    pub fn ensure_dir(&self) -> Result<()> {
        if !self.config.dir.as_os_str().is_empty() {
            std::fs::create_dir_all(&self.config.dir)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_save_rules_comma_and_redis_forms() {
        let a = parse_save_rules("900,1 300,10 60,10000").unwrap();
        assert_eq!(a, default_save_rules());

        let b = parse_save_rules("900 1 300 10 60 10000").unwrap();
        assert_eq!(b, default_save_rules());

        assert!(parse_save_rules("").unwrap().is_empty());
        assert!(parse_save_rules("\"\"").unwrap().is_empty());
        assert!(parse_save_rules("900").is_err());
    }

    #[test]
    fn format_roundtrip() {
        let s = format_save_rules(&default_save_rules());
        assert_eq!(s, "900 1 300 10 60 10000");
        assert_eq!(parse_save_rules(&s).unwrap(), default_save_rules());
    }
}
