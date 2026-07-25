//! Redis Sentinel–compatible **lite** (Batch EW) + multi-Sentinel **ODOWN** (Batch EX)
//! + **config persistence** (Batch EZ) + **hello bus lite** (Batch FA)
//! + **promote-success gate** (Batch FC) + **leader election** (Batch FE)
//! + **promote ranking** (Batch FK) + **INFO priority + failover cooldown** (Batch FM).
//!
//! - EW: subjective-down (`s_down`), MONITOR, GET-MASTER-ADDR, FAILOVER, auto-failover
//! - EX: peer `SENTINEL MEET`, `IS-MASTER-DOWN-BY-ADDR` votes, `o_down` when votes ≥ quorum
//! - EZ: `SENTINEL FLUSHCONFIG` / load `dir/sentinel.conf` on boot; autosave on topology change
//! - FA: Redis-style hello CSV; `PUBLISH __sentinel__:hello` on masters; peer `SENTINEL HELLO`
//! - FC: `promote_replica` requires real promote (FAILOVER / REPLICAOF / ROLE=master), never PING alone
//! - FE: voted-leader on `IS-MASTER-DOWN-BY-ADDR` (epoch/runid); only elected leader auto-failovers
//! - FK: promote rank = highest priority (0 never), then highest ROLE offset, then greatest `ip:port`
//! - FM: live `INFO replication` `slave_priority` refresh; auto-failover cooldown after `try_failover`
//!
//! # Honesty vs full Redis Sentinel
//!
//! - **Leader election (FE):** first-seen sticky vote per epoch; higher epoch can re-vote.
//!   Winner needs `max(quorum, floor(N/2)+1)` votes among self+peers. Manual `SENTINEL FAILOVER`
//!   bypasses election (operator force). Not a full Raft/Sentinel state machine (no election
//!   timeout abort, no subjective-leader lex-min runid pre-filter).
//! - **Hello bus:** tick `PUBLISH __sentinel__:hello` + peer `SENTINEL HELLO` exchange.
//!   No long-lived master `SUBSCRIBE` fan-in (residual).
//! - **CKQUORUM:** peer-table size (`1 + peers`), not live reachability probes (residual).
//! - **Promote order (FK/FM):** mirrors cluster EA/EB — highest `priority` (0 never promote), then
//!   highest `repl_offset` (from ROLE master slave list), then lexicographically greatest `ip:port`.
//!   ROLE discovery defaults priority to 100; probe / `try_failover` refresh each replica via
//!   `INFO replication` `slave_priority` (Batch FM). Note: Redis Sentinel prefers **lower**
//!   priority numbers; Kore mirrors cluster (**higher** wins) for EA/EB consistency.
//! - **Failover cooldown (FM):** after a completed or failed `try_failover`, auto path suppresses
//!   re-entry for [`FAILOVER_COOLDOWN`] (15s, Redis failover-timeout ballpark lite). Manual
//!   `SENTINEL FAILOVER` always forces (bypasses cooldown).

use crate::protocol::{RespParser, RespValue};
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tracing::{debug, info, warn};

// ── Batch FC: test inject for promote_replica ────────────────────────────────
// 0 = normal; 1 = force fail after PING (no ROLE fallback); 2 = force OK after PING
static PROMOTE_INJECT: AtomicU32 = AtomicU32::new(0);

/// Force-fail promote after PING (cmds + ROLE treated as failed). Batch FC tests.
pub const PROMOTE_INJECT_FORCE_FAIL: u32 = 1;
/// Force-success promote after PING. Batch FC tests.
pub const PROMOTE_INJECT_FORCE_OK: u32 = 2;

/// Set promote inject mode (`0` / [`PROMOTE_INJECT_FORCE_FAIL`] / [`PROMOTE_INJECT_FORCE_OK`]).
pub fn test_set_promote_inject(mode: u32) {
    PROMOTE_INJECT.store(mode, Ordering::SeqCst);
}

/// Clears promote inject on drop (Batch FC).
pub struct PromoteInjectGuard;

impl Drop for PromoteInjectGuard {
    fn drop(&mut self) {
        PROMOTE_INJECT.store(0, Ordering::SeqCst);
    }
}

/// Acquire a promote-inject guard that resets mode when dropped.
pub fn test_promote_inject() -> PromoteInjectGuard {
    PromoteInjectGuard
}

/// Default subjective-down timeout (Redis-like 30s).
pub const DEFAULT_DOWN_AFTER_MS: u64 = 30_000;

/// Auto-failover re-entry cooldown after `try_failover` completes or fails (Batch FM).
///
/// Redis `failover-timeout` defaults to 180s; lite uses **15s** so ops recover without
/// thrashing every tick. Manual `SENTINEL FAILOVER` bypasses this gate.
pub const FAILOVER_COOLDOWN: Duration = Duration::from_secs(15);

/// Redis Sentinel hello pub/sub channel (Batch FA).
pub const HELLO_CHANNEL: &str = "__sentinel__:hello";

/// Probe interval for the background loop.
const SENTINEL_TICK: Duration = Duration::from_secs(1);

const IO_TIMEOUT: Duration = Duration::from_millis(800);

/// Test override for [`FAILOVER_COOLDOWN`] duration (ms). `0` = use default 15s.
static FAILOVER_COOLDOWN_MS_OVERRIDE: AtomicU64 = AtomicU64::new(0);

/// Set failover cooldown override for tests (`0` restores [`FAILOVER_COOLDOWN`]).
pub fn test_set_failover_cooldown_ms(ms: u64) {
    FAILOVER_COOLDOWN_MS_OVERRIDE.store(ms, Ordering::SeqCst);
}

/// Effective auto-failover cooldown (Batch FM).
pub fn failover_cooldown_duration() -> Duration {
    let ov = FAILOVER_COOLDOWN_MS_OVERRIDE.load(Ordering::SeqCst);
    if ov == 0 {
        FAILOVER_COOLDOWN
    } else {
        Duration::from_millis(ov)
    }
}

/// Parsed Redis-style hello payload (Batch FA).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelloMsg {
    pub sentinel_ip: String,
    pub sentinel_port: u16,
    pub runid: String,
    pub current_epoch: u64,
    pub master_name: String,
    pub master_ip: String,
    pub master_port: u16,
    pub master_config_epoch: u64,
}

/// Known replica of a monitored master (Batch EW + FK rank fields).
///
/// Ranking for promote (Batch FK, mirrors cluster EA/EB):
/// 1. highest [`Self::priority`] (**0 = never promote**)
/// 2. then highest [`Self::repl_offset`] (ROLE master slave list)
/// 3. then lexicographically greatest `ip:port`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaInfo {
    pub ip: String,
    pub port: u16,
    /// Replication offset from ROLE master slave entry (Batch FK). Default 0.
    pub repl_offset: u64,
    /// Promote priority (Batch FK). Default 100; **0 = never promote**.
    pub priority: u32,
}

impl ReplicaInfo {
    /// Default priority 100, offset 0 (discovery placeholder / inject helper).
    pub fn new(ip: impl Into<String>, port: u16) -> Self {
        Self {
            ip: ip.into(),
            port,
            repl_offset: 0,
            priority: 100,
        }
    }

    /// Set rank fields used by [`rank_replicas_for_promote`].
    pub fn with_rank(mut self, priority: u32, repl_offset: u64) -> Self {
        self.priority = priority;
        self.repl_offset = repl_offset;
        self
    }

    /// Stable address key for tie-break and logging.
    pub fn addr_key(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }
}

/// Sort eligible replicas for promote attempts (Batch FK).
///
/// Filters out `priority == 0` (never promote). Order: highest priority, then
/// highest offset, then lexicographically greatest `ip:port` (mirrors
/// [`crate::cluster::ClusterState::failover_election_winner`]).
pub fn rank_replicas_for_promote(replicas: &[ReplicaInfo]) -> Vec<ReplicaInfo> {
    let mut eligible: Vec<ReplicaInfo> = replicas
        .iter()
        .filter(|r| r.priority > 0)
        .cloned()
        .collect();
    eligible.sort_by(|a, b| {
        b.priority
            .cmp(&a.priority)
            .then_with(|| b.repl_offset.cmp(&a.repl_offset))
            .then_with(|| b.addr_key().cmp(&a.addr_key()))
    });
    eligible
}

/// Another Sentinel process known via MEET (Batch EX).
#[derive(Debug, Clone)]
pub struct PeerSentinel {
    pub id: String,
    pub ip: String,
    pub port: u16,
}

#[derive(Debug, Clone)]
pub struct MasterInfo {
    pub name: String,
    pub ip: String,
    pub port: u16,
    pub quorum: u32,
    pub down_after_ms: u64,
    /// Subjectively down (this Sentinel cannot reach the master).
    pub s_down: bool,
    /// Objectively down (quorum of s_down votes; Batch EX).
    pub o_down: bool,
    /// Last known vote count for o_down (self + peers).
    pub down_votes: u32,
    pub last_ok: Instant,
    pub replicas: Vec<ReplicaInfo>,
    /// When true, background loop will attempt promote after o_down.
    pub auto_failover: bool,
    /// In-progress / completed failover epoch (monotonic counter for tests).
    pub failover_epoch: u64,
    /// True while manual or auto `try_failover` runs for this master (Batch FC).
    /// Prevents overlapping SENTINEL FAILOVER and tick auto-failover.
    pub failover_in_progress: bool,
    /// Runid we voted for as failover leader this epoch (Batch FE). Empty = no vote yet.
    pub leader_runid: String,
    /// Epoch of [`Self::leader_runid`] (Batch FE). Sticky until a higher epoch arrives.
    pub leader_epoch: u64,
    /// When the last `try_failover` finished (success or fail). Batch FM cooldown clock.
    pub last_failover_attempt: Option<Instant>,
}

impl MasterInfo {
    pub fn flags(&self) -> String {
        let mut f = Vec::new();
        if self.o_down {
            f.push("o_down");
        }
        if self.s_down {
            f.push("s_down");
        }
        f.push("master");
        f.join(",")
    }
}

/// Shared Sentinel state for one Kore process.
#[derive(Debug)]
pub struct SentinelState {
    my_id: RwLock<String>,
    /// Client-facing address for MEETPEER announce (Batch EX).
    listen_ip: RwLock<String>,
    listen_port: RwLock<u16>,
    masters: RwLock<HashMap<String, MasterInfo>>,
    peers: RwLock<HashMap<String, PeerSentinel>>,
    /// Config epoch for is-master-down-by-addr (lite).
    current_epoch: AtomicU64,
    /// Directory for `sentinel.conf` autosave (Batch EZ). `None` disables autosave.
    conf_dir: RwLock<Option<String>>,
}

fn generate_sentinel_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    // 40 hex chars Redis-style runid
    format!("{:040x}", nanos ^ (nanos >> 17))
}

impl SentinelState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            my_id: RwLock::new(generate_sentinel_id()),
            listen_ip: RwLock::new("127.0.0.1".into()),
            listen_port: RwLock::new(26379),
            masters: RwLock::new(HashMap::new()),
            peers: RwLock::new(HashMap::new()),
            current_epoch: AtomicU64::new(0),
            conf_dir: RwLock::new(None),
        })
    }

    /// Best-effort load of `{dir}/sentinel.conf` (Batch EZ); else empty new state.
    pub fn load_or_new(dir: &str) -> Arc<Self> {
        let path = std::path::Path::new(if dir.is_empty() { "." } else { dir }).join("sentinel.conf");
        match std::fs::read_to_string(&path) {
            Ok(text) => match Self::from_conf_text(&text) {
                Ok(s) => {
                    s.set_conf_dir(dir);
                    tracing::info!("sentinel: loaded config from {}", path.display());
                    s
                }
                Err(e) => {
                    tracing::warn!(
                        "sentinel: failed to load {}: {} — starting empty",
                        path.display(),
                        e
                    );
                    let s = Self::new();
                    s.set_conf_dir(dir);
                    s
                }
            },
            Err(_) => {
                let s = Self::new();
                s.set_conf_dir(dir);
                s
            }
        }
    }

    pub fn my_id(&self) -> String {
        self.my_id.read().clone()
    }

    /// Directory for autosave / FLUSHCONFIG (Batch EZ).
    pub fn set_conf_dir(&self, dir: impl Into<String>) {
        let d = dir.into();
        *self.conf_dir.write() = if d.is_empty() { None } else { Some(d) };
    }

    pub fn conf_dir(&self) -> Option<String> {
        self.conf_dir.read().clone()
    }

    pub fn current_epoch(&self) -> u64 {
        self.current_epoch.load(Ordering::Relaxed)
    }

    /// Bind/announce address used when introducing ourselves via MEETPEER.
    pub fn set_listen_addr(&self, ip: impl Into<String>, port: u16) {
        *self.listen_ip.write() = ip.into();
        *self.listen_port.write() = port;
    }

    pub fn listen_addr(&self) -> (String, u16) {
        (self.listen_ip.read().clone(), *self.listen_port.read())
    }

    /// `SENTINEL MONITOR <name> <ip> <port> <quorum>`
    pub fn monitor(
        &self,
        name: impl Into<String>,
        ip: impl Into<String>,
        port: u16,
        quorum: u32,
    ) -> Result<(), String> {
        let name = name.into();
        if name.is_empty() {
            return Err("ERR master name cannot be empty".into());
        }
        if port == 0 {
            return Err("ERR Invalid port".into());
        }
        if quorum == 0 {
            return Err("ERR quorum must be at least 1".into());
        }
        let mut g = self.masters.write();
        if g.contains_key(&name) {
            return Err(format!("ERR Duplicated master name: {}", name));
        }
        g.insert(
            name.clone(),
            MasterInfo {
                name,
                ip: ip.into(),
                port,
                quorum,
                down_after_ms: DEFAULT_DOWN_AFTER_MS,
                s_down: false,
                o_down: false,
                down_votes: 0,
                last_ok: Instant::now(),
                replicas: Vec::new(),
                auto_failover: true,
                failover_epoch: 0,
                failover_in_progress: false,
                leader_runid: String::new(),
                leader_epoch: 0,
                last_failover_attempt: None,
            },
        );
        drop(g);
        self.autosave_conf();
        Ok(())
    }

    pub fn remove(&self, name: &str) -> Result<(), String> {
        let mut g = self.masters.write();
        if g.remove(name).is_none() {
            return Err(format!("ERR No such master with name '{}'", name));
        }
        drop(g);
        self.autosave_conf();
        Ok(())
    }

    /// `(ip, port)` when known; `None` if name missing.
    pub fn get_master_addr(&self, name: &str) -> Option<(String, u16)> {
        let g = self.masters.read();
        g.get(name).map(|m| (m.ip.clone(), m.port))
    }

    pub fn master(&self, name: &str) -> Option<MasterInfo> {
        self.masters.read().get(name).cloned()
    }

    pub fn masters(&self) -> Vec<MasterInfo> {
        let g = self.masters.read();
        let mut v: Vec<_> = g.values().cloned().collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }

    pub fn peers(&self) -> Vec<PeerSentinel> {
        let g = self.peers.read();
        let mut v: Vec<_> = g.values().cloned().collect();
        v.sort_by(|a, b| a.id.cmp(&b.id));
        v
    }

    /// Learn or update a peer Sentinel. Returns `true` if the peer table changed
    /// (new peer or ip/port update). Autosaves only on real change (Batch FE).
    pub fn add_peer(&self, id: impl Into<String>, ip: impl Into<String>, port: u16) -> bool {
        let id = id.into();
        if id == self.my_id() {
            return false;
        }
        let ip = ip.into();
        {
            let mut g = self.peers.write();
            if let Some(existing) = g.get(&id) {
                if existing.ip == ip && existing.port == port {
                    return false;
                }
            }
            g.insert(
                id.clone(),
                PeerSentinel {
                    id,
                    ip,
                    port,
                },
            );
        }
        self.autosave_conf();
        true
    }

    pub fn set_option(&self, name: &str, option: &str, value: &str) -> Result<(), String> {
        let mut g = self.masters.write();
        let m = g
            .get_mut(name)
            .ok_or_else(|| format!("ERR No such master with name '{}'", name))?;
        let opt = option.to_ascii_lowercase();
        match opt.as_str() {
            "down-after-milliseconds" | "down_after_milliseconds" => {
                let ms: u64 = value
                    .parse()
                    .map_err(|_| "ERR Invalid down-after-milliseconds".to_string())?;
                if ms < 10 {
                    return Err("ERR down-after-milliseconds must be >= 10".into());
                }
                m.down_after_ms = ms;
            }
            "quorum" => {
                let q: u32 = value
                    .parse()
                    .map_err(|_| "ERR Invalid quorum".to_string())?;
                if q == 0 {
                    return Err("ERR quorum must be at least 1".into());
                }
                m.quorum = q;
            }
            "auto-failover" | "auto_failover" => {
                let v = value.to_ascii_lowercase();
                m.auto_failover = matches!(v.as_str(), "yes" | "true" | "1" | "on");
            }
            _ => {
                return Err(format!("ERR Unknown option '{}'", option));
            }
        }
        drop(g);
        self.autosave_conf();
        Ok(())
    }

    /// Apply a successful health probe.
    pub fn note_ok(&self, name: &str, replicas: Option<Vec<ReplicaInfo>>) {
        let mut g = self.masters.write();
        if let Some(m) = g.get_mut(name) {
            m.last_ok = Instant::now();
            if m.s_down || m.o_down {
                info!("sentinel: master {} is back up ({}:{})", name, m.ip, m.port);
            }
            m.s_down = false;
            m.o_down = false;
            m.down_votes = 0;
            // Clear election vote when master is healthy again (Batch FE).
            m.leader_runid.clear();
            m.leader_epoch = 0;
            if let Some(r) = replicas {
                m.replicas = r;
            }
        }
    }

    /// Mark s_down if last_ok is older than down_after_ms. Returns whether newly s_down.
    pub fn maybe_sdown(&self, name: &str) -> bool {
        let mut g = self.masters.write();
        let Some(m) = g.get_mut(name) else {
            return false;
        };
        if m.last_ok.elapsed() >= Duration::from_millis(m.down_after_ms) {
            if !m.s_down {
                m.s_down = true;
                warn!(
                    "sentinel: +sdown master {} {}:{} (down-after {}ms)",
                    name, m.ip, m.port, m.down_after_ms
                );
                return true;
            }
        }
        false
    }

    /// Local answer for `IS-MASTER-DOWN-BY-ADDR` (Batch EX + FE).
    ///
    /// Returns `(down: 0|1, leader_runid, leader_epoch)`.
    ///
    /// # Voting (Batch FE)
    ///
    /// When the master is subjectively down (`s_down`) and `req_runid` is a real
    /// candidate (not `"*"` / empty), this Sentinel casts a **sticky** vote for
    /// that candidate at `req_epoch` if `req_epoch` is greater than any prior
    /// vote epoch (Redis-style first-seen per epoch; higher epoch may re-vote).
    ///
    /// When `req_runid` is `"*"` (probe only): returns the existing vote if any;
    /// if `s_down` and no vote yet, returns **self** as leader (sole-sentinel
    /// convenience — Redis often returns `"*"` here; we document the difference).
    ///
    /// When not `s_down`: `(0, "", 0)`.
    pub fn is_master_down_by_addr(
        &self,
        ip: &str,
        port: u16,
        req_epoch: u64,
        req_runid: &str,
    ) -> (i64, String, u64) {
        // Find master name under a short read lock, then vote under write.
        let master_name = {
            let g = self.masters.read();
            g.values()
                .find(|m| m.ip == ip && m.port == port)
                .map(|m| m.name.clone())
        };
        let Some(name) = master_name else {
            return (0, String::new(), 0);
        };
        let Some(m) = self.master(&name) else {
            return (0, String::new(), 0);
        };
        if !m.s_down {
            return (0, String::new(), 0);
        }
        let seeking_vote = !req_runid.is_empty() && req_runid != "*";
        if seeking_vote {
            let (leader, epoch) = self.vote_leader(&name, req_epoch, req_runid);
            return (1, leader, epoch);
        }
        // Probe only: existing vote, or self for lite sole-sentinel UX.
        if !m.leader_runid.is_empty() {
            return (1, m.leader_runid, m.leader_epoch);
        }
        let epoch = self.current_epoch().max(1);
        (1, self.my_id(), epoch)
    }

    /// Cast or read sticky failover-leader vote for `name` (Batch FE).
    ///
    /// If `req_epoch` is greater than the stored `leader_epoch`, vote for
    /// `candidate` and advance process `current_epoch` when needed. Otherwise
    /// return the prior vote unchanged.
    pub fn vote_leader(&self, name: &str, req_epoch: u64, candidate: &str) -> (String, u64) {
        if candidate.is_empty() || candidate == "*" {
            if let Some(m) = self.master(name) {
                return (m.leader_runid, m.leader_epoch);
            }
            return (String::new(), 0);
        }
        // Advance process epoch when peer requests a higher one.
        let cur = self.current_epoch();
        if req_epoch > cur {
            self.current_epoch.store(req_epoch, Ordering::Relaxed);
        }
        let mut g = self.masters.write();
        let Some(m) = g.get_mut(name) else {
            return (String::new(), 0);
        };
        if req_epoch > m.leader_epoch {
            m.leader_epoch = req_epoch;
            m.leader_runid = candidate.to_string();
            debug!(
                "sentinel: +vote-for-leader {} epoch={} leader={}",
                name, req_epoch, candidate
            );
        }
        (m.leader_runid.clone(), m.leader_epoch)
    }

    /// Bump process config epoch and return the new value (election attempt).
    pub fn next_election_epoch(&self) -> u64 {
        self.current_epoch.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// True when this Sentinel holds the failover-leader vote for `name`
    /// (or there is no competing vote yet / sole sentinel). Batch FE.
    pub fn is_failover_leader(&self, name: &str) -> bool {
        let Some(m) = self.master(name) else {
            return false;
        };
        if m.leader_runid.is_empty() {
            // No vote cast — sole sentinel path treats self as leader.
            return self.peers().is_empty();
        }
        m.leader_runid == self.my_id()
    }

    /// Votes required to win leadership: `max(quorum, floor(N/2)+1)` (Batch FE).
    pub fn leader_votes_needed(&self, name: &str) -> u32 {
        let quorum = self.master(name).map(|m| m.quorum).unwrap_or(1);
        let voters = self.known_sentinel_count() as u32;
        let majority = voters / 2 + 1;
        quorum.max(majority).max(1)
    }

    /// Recompute o_down from local s_down + peer votes (Batch EX).
    ///
    /// `peer_down_votes` is the number of other sentinels that report down.
    /// Returns true if newly o_down.
    pub fn apply_down_votes(&self, name: &str, peer_down_votes: u32) -> bool {
        let mut g = self.masters.write();
        let Some(m) = g.get_mut(name) else {
            return false;
        };
        let self_vote = if m.s_down { 1u32 } else { 0 };
        let votes = self_vote.saturating_add(peer_down_votes);
        m.down_votes = votes;
        if votes >= m.quorum {
            if !m.o_down {
                m.o_down = true;
                warn!(
                    "sentinel: +odown master {} {}:{} votes={}/{}",
                    name, m.ip, m.port, votes, m.quorum
                );
                return true;
            }
            m.o_down = true;
        } else if !m.s_down {
            m.o_down = false;
        } else {
            // Still s_down but below quorum — clear o_down.
            m.o_down = false;
        }
        false
    }

    /// After successful promote: point monitor at the new master.
    pub fn switch_master(&self, name: &str, ip: String, port: u16) {
        let mut g = self.masters.write();
        if let Some(m) = g.get_mut(name) {
            info!(
                "sentinel: +switch-master {} {}:{} -> {}:{}",
                name, m.ip, m.port, ip, port
            );
            m.ip = ip;
            m.port = port;
            m.s_down = false;
            m.o_down = false;
            m.down_votes = 0;
            m.last_ok = Instant::now();
            m.failover_epoch = m.failover_epoch.saturating_add(1);
            m.replicas.clear();
            // Clear election state after successful switch (Batch FE).
            m.leader_runid.clear();
            m.leader_epoch = 0;
            // Leave failover_in_progress to the outer try_failover guard.
            self.current_epoch.fetch_add(1, Ordering::Relaxed);
        }
        drop(g);
        self.autosave_conf();
    }

    /// Mark failover in-progress for `name` (Batch FC). Err if already running.
    pub fn begin_failover(&self, name: &str) -> Result<(), String> {
        let mut g = self.masters.write();
        let m = g
            .get_mut(name)
            .ok_or_else(|| format!("ERR No such master with name '{}'", name))?;
        if m.failover_in_progress {
            return Err("ERR Failover already in progress for this master".into());
        }
        m.failover_in_progress = true;
        Ok(())
    }

    /// Clear in-progress flag (Batch FC). Idempotent if master missing.
    pub fn end_failover(&self, name: &str) {
        if let Some(m) = self.masters.write().get_mut(name) {
            m.failover_in_progress = false;
        }
    }

    /// Record that `try_failover` just finished (success or fail). Starts auto cooldown (Batch FM).
    pub fn note_failover_attempt(&self, name: &str) {
        if let Some(m) = self.masters.write().get_mut(name) {
            m.last_failover_attempt = Some(Instant::now());
        }
    }

    /// True while auto-failover should wait after a recent `try_failover` (Batch FM).
    ///
    /// Manual `SENTINEL FAILOVER` does **not** consult this (operator force).
    pub fn in_failover_cooldown(&self, name: &str) -> bool {
        let Some(m) = self.master(name) else {
            return false;
        };
        let Some(t) = m.last_failover_attempt else {
            return false;
        };
        t.elapsed() < failover_cooldown_duration()
    }

    /// Clear cooldown (tests / operator recovery). Batch FM.
    pub fn clear_failover_cooldown(&self, name: &str) {
        if let Some(m) = self.masters.write().get_mut(name) {
            m.last_failover_attempt = None;
        }
    }

    /// Replace known replica list without touching s_down / last_ok (Batch FM priority refresh).
    pub fn set_replicas(&self, name: &str, replicas: Vec<ReplicaInfo>) {
        if let Some(m) = self.masters.write().get_mut(name) {
            m.replicas = replicas;
        }
    }

    pub fn names(&self) -> Vec<String> {
        self.masters.read().keys().cloned().collect()
    }

    /// Usable sentinels for CKQUORUM: self + known peers (table size, not live probe).
    ///
    /// Residual (Batch FE): does **not** PING peers; count is `1 + peers.len()`.
    pub fn known_sentinel_count(&self) -> usize {
        1 + self.peers.read().len()
    }

    // ── Batch FA: hello bus lite ─────────────────────────────────────────────

    /// Build Redis-compatible hello CSV for one monitored master.
    pub fn format_hello(&self, master_name: &str) -> Option<String> {
        let m = self.master(master_name)?;
        let (sip, sport) = self.listen_addr();
        Some(format!(
            "{},{},{},{},{},{},{},{}",
            sip,
            sport,
            self.my_id(),
            self.current_epoch(),
            m.name,
            m.ip,
            m.port,
            m.failover_epoch
        ))
    }

    /// Parse Redis-style hello CSV (8 comma-separated fields).
    pub fn parse_hello(payload: &str) -> Option<HelloMsg> {
        let parts: Vec<&str> = payload.trim().split(',').collect();
        if parts.len() < 8 {
            return None;
        }
        Some(HelloMsg {
            sentinel_ip: parts[0].to_string(),
            sentinel_port: parts[1].parse().ok()?,
            runid: parts[2].to_string(),
            current_epoch: parts[3].parse().ok()?,
            master_name: parts[4].to_string(),
            master_ip: parts[5].to_string(),
            master_port: parts[6].parse().ok()?,
            master_config_epoch: parts[7].parse().ok()?,
        })
    }

    /// Apply a peer hello: learn peer + optional higher-epoch master switch (Batch FA).
    ///
    /// Returns true if master address was switched.
    pub fn apply_hello(&self, msg: &HelloMsg) -> bool {
        if msg.runid == self.my_id() {
            return false;
        }
        if msg.sentinel_port == 0 || msg.runid.len() < 8 {
            return false;
        }
        // Learn peer; add_peer autosaves only when ip/port actually change (Batch FE).
        self.add_peer(&msg.runid, &msg.sentinel_ip, msg.sentinel_port);

        // Bump local epoch if peer is ahead (lite config epoch tracking).
        let cur = self.current_epoch();
        if msg.current_epoch > cur {
            self.current_epoch
                .store(msg.current_epoch, Ordering::Relaxed);
        }

        let Some(local) = self.master(&msg.master_name) else {
            return false;
        };
        // Higher master config epoch wins (switch-master after remote failover).
        if msg.master_config_epoch > local.failover_epoch
            && (msg.master_ip != local.ip || msg.master_port != local.port)
        {
            info!(
                "sentinel: hello switch-master {} {}:{} -> {}:{} (epoch {}->{})",
                msg.master_name,
                local.ip,
                local.port,
                msg.master_ip,
                msg.master_port,
                local.failover_epoch,
                msg.master_config_epoch
            );
            // Update without full switch_master clear of replicas mid-flight:
            // reuse switch_master then restore epoch from hello.
            self.switch_master(
                &msg.master_name,
                msg.master_ip.clone(),
                msg.master_port,
            );
            // Align failover_epoch to peer's reported master config epoch.
            if let Some(m) = self.masters.write().get_mut(&msg.master_name) {
                m.failover_epoch = msg.master_config_epoch;
            }
            return true;
        }
        false
    }

    // ── Batch EZ: sentinel.conf persistence ─────────────────────────────────

    /// Serialize current topology to conf text.
    pub fn format_conf(&self) -> String {
        let mut out = String::new();
        out.push_str("# Kore sentinel.conf generated by SENTINEL FLUSHCONFIG\n");
        out.push_str(&format!("# epoch {}\n", self.current_epoch()));
        out.push_str(&format!("sentinel myid {}\n", self.my_id()));
        let masters = self.masters();
        for m in &masters {
            out.push_str(&format!(
                "sentinel monitor {} {} {} {}\n",
                m.name, m.ip, m.port, m.quorum
            ));
            out.push_str(&format!(
                "sentinel down-after-milliseconds {} {}\n",
                m.name, m.down_after_ms
            ));
            out.push_str(&format!(
                "sentinel auto-failover {} {}\n",
                m.name,
                if m.auto_failover { "yes" } else { "no" }
            ));
        }
        for p in self.peers() {
            out.push_str(&format!(
                "sentinel known-peer {} {} {}\n",
                p.id, p.ip, p.port
            ));
        }
        out
    }

    /// Write conf to `{dir}/sentinel.conf` (Batch EZ).
    pub fn save_conf_to(&self, dir: &str) -> Result<std::path::PathBuf, String> {
        use std::fs;
        use std::io::Write;
        use std::path::PathBuf;

        let dir = if dir.is_empty() { "." } else { dir };
        let path = PathBuf::from(dir).join("sentinel.conf");
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| format!("cannot create dir: {}", e))?;
        }
        let body = self.format_conf();
        let mut f = fs::File::create(&path)
            .map_err(|e| format!("cannot create {}: {}", path.display(), e))?;
        f.write_all(body.as_bytes())
            .map_err(|e| format!("write error: {}", e))?;
        f.sync_all().map_err(|e| format!("sync error: {}", e))?;
        Ok(path)
    }

    /// Best-effort autosave when conf_dir is set (Batch EZ).
    pub fn autosave_conf(&self) {
        let Some(dir) = self.conf_dir.read().clone() else {
            return;
        };
        match self.save_conf_to(&dir) {
            Ok(p) => debug!("sentinel: autosaved config to {}", p.display()),
            Err(e) => warn!("sentinel: autosave sentinel.conf failed: {}", e),
        }
    }

    /// Parse conf text into a new state (Batch EZ).
    pub fn from_conf_text(text: &str) -> Result<Arc<Self>, String> {
        let s = Self::new();
        let mut epoch = 0u64;
        let mut myid: Option<String> = None;
        for raw in text.lines() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                if let Some(rest) = line.strip_prefix("# epoch ") {
                    if let Ok(e) = rest.trim().parse::<u64>() {
                        epoch = e;
                    }
                }
                continue;
            }
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 2 || parts[0] != "sentinel" {
                continue;
            }
            match parts[1] {
                "myid" if parts.len() >= 3 => {
                    myid = Some(parts[2].to_string());
                }
                "monitor" if parts.len() >= 6 => {
                    let name = parts[2];
                    let ip = parts[3];
                    let port: u16 = parts[4]
                        .parse()
                        .map_err(|_| format!("invalid port in: {}", line))?;
                    let quorum: u32 = parts[5]
                        .parse()
                        .map_err(|_| format!("invalid quorum in: {}", line))?;
                    // Ignore duplicate errors on reload of same name.
                    let _ = s.monitor(name, ip, port, quorum);
                }
                "down-after-milliseconds" if parts.len() >= 4 => {
                    let _ = s.set_option(parts[2], "down-after-milliseconds", parts[3]);
                }
                "auto-failover" if parts.len() >= 4 => {
                    let _ = s.set_option(parts[2], "auto-failover", parts[3]);
                }
                "known-peer" if parts.len() >= 5 => {
                    let id = parts[2];
                    let ip = parts[3];
                    let port: u16 = parts[4]
                        .parse()
                        .map_err(|_| format!("invalid peer port in: {}", line))?;
                    // Avoid autosave during load.
                    {
                        let mut g = s.peers.write();
                        g.insert(
                            id.to_string(),
                            PeerSentinel {
                                id: id.to_string(),
                                ip: ip.to_string(),
                                port,
                            },
                        );
                    }
                }
                _ => {}
            }
        }
        if let Some(id) = myid {
            if id.len() >= 8 {
                *s.my_id.write() = id;
            }
        }
        s.current_epoch.store(epoch, Ordering::Relaxed);
        Ok(s)
    }
}

impl Default for SentinelState {
    fn default() -> Self {
        Self {
            my_id: RwLock::new(generate_sentinel_id()),
            listen_ip: RwLock::new("127.0.0.1".into()),
            listen_port: RwLock::new(26379),
            masters: RwLock::new(HashMap::new()),
            peers: RwLock::new(HashMap::new()),
            current_epoch: AtomicU64::new(0),
            conf_dir: RwLock::new(None),
        }
    }
}

/// Background sentinel health + ODOWN vote + auto-failover loop.
pub async fn run_sentinel_loop(
    sentinel: Arc<SentinelState>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
            }
            _ = tokio::time::sleep(SENTINEL_TICK) => {
                tick_all(&sentinel).await;
            }
        }
    }
}

async fn tick_all(sentinel: &SentinelState) {
    let names = sentinel.names();
    for name in names {
        probe_one(sentinel, &name).await;
        let _ = sentinel.maybe_sdown(&name);
        // Batch FA: announce ourselves on the master hello channel (if master up).
        // Residual: tick PUBLISH only — no long-lived master SUBSCRIBE fan-in.
        publish_hello_to_master(sentinel, &name).await;
        // Batch EX: poll peers for is-master-down votes (probe runid="*", no election).
        let peer_votes = collect_peer_down_votes(sentinel, &name).await;
        let _ = sentinel.apply_down_votes(&name, peer_votes);
        if let Some(m) = sentinel.master(&name) {
            // Auto-failover on o_down only; only elected leader promotes (Batch FE).
            // Batch FM: suppress re-entry during post-try_failover cooldown (manual FAILOVER bypasses).
            if m.o_down
                && m.auto_failover
                && !m.failover_in_progress
                && !sentinel.in_failover_cooldown(&name)
            {
                if try_elect_leader(sentinel, &name).await {
                    let _ = try_failover(sentinel, &name).await;
                }
            }
        }
    }
    // Batch FA: exchange HELLO with known peers (primary discovery path).
    hello_peers(sentinel).await;
}

/// PUBLISH Redis-style hello to a monitored master when it is reachable (Batch FA).
async fn publish_hello_to_master(sentinel: &SentinelState, name: &str) {
    let Some(payload) = sentinel.format_hello(name) else {
        return;
    };
    let Some(m) = sentinel.master(name) else {
        return;
    };
    if m.s_down {
        return;
    }
    let addr = format!("{}:{}", m.ip, m.port);
    match publish_channel(&addr, HELLO_CHANNEL, &payload).await {
        Ok(n) => debug!(
            "sentinel: published hello for {} to {} ({} receivers)",
            name, addr, n
        ),
        Err(e) => debug!("sentinel: hello publish to {} failed: {}", addr, e),
    }
}

/// Send `SENTINEL HELLO <csv>` to each known peer (Batch FA).
async fn hello_peers(sentinel: &SentinelState) {
    let peers = sentinel.peers();
    if peers.is_empty() {
        return;
    }
    let names = sentinel.names();
    if names.is_empty() {
        return;
    }
    // One hello per master, to each peer.
    for name in names {
        let Some(payload) = sentinel.format_hello(&name) else {
            continue;
        };
        for p in &peers {
            let addr = format!("{}:{}", p.ip, p.port);
            match send_sentinel_hello(&addr, &payload).await {
                Ok(()) => {}
                Err(e) => debug!("sentinel: HELLO to peer {} failed: {}", addr, e),
            }
        }
    }
}

async fn publish_channel(addr: &str, channel: &str, message: &str) -> Result<i64, String> {
    let mut stream = connect(addr).await?;
    match resp_command(
        &mut stream,
        &["PUBLISH", channel, message],
        IO_TIMEOUT,
    )
    .await
    {
        Ok(RespValue::Integer(n)) => Ok(n),
        Ok(RespValue::Error(e)) => Err(String::from_utf8_lossy(&e).into_owned()),
        Ok(other) => Err(format!("unexpected PUBLISH reply: {:?}", other)),
        Err(e) => Err(e),
    }
}

async fn send_sentinel_hello(addr: &str, payload: &str) -> Result<(), String> {
    let mut stream = connect(addr).await?;
    match resp_command(
        &mut stream,
        &["SENTINEL", "HELLO", payload],
        IO_TIMEOUT,
    )
    .await
    {
        Ok(RespValue::SimpleString(s)) if s.as_ref() == b"OK" => Ok(()),
        Ok(RespValue::Integer(_)) => Ok(()), // Redis sometimes returns 1
        Ok(RespValue::Error(e)) => Err(String::from_utf8_lossy(&e).into_owned()),
        Ok(other) => Err(format!("unexpected HELLO reply: {:?}", other)),
        Err(e) => Err(e),
    }
}

/// Query peer sentinels: how many report this master's addr as down.
///
/// Uses runid `"*"` so ODOWN collection does **not** request a leader vote (Batch FE).
async fn collect_peer_down_votes(sentinel: &SentinelState, name: &str) -> u32 {
    let Some(m) = sentinel.master(name) else {
        return 0;
    };
    let peers = sentinel.peers();
    if peers.is_empty() {
        return 0;
    }
    let epoch = sentinel.current_epoch().to_string();
    let mut votes = 0u32;
    for p in peers {
        let addr = format!("{}:{}", p.ip, p.port);
        match query_is_master_down(&addr, &m.ip, m.port, &epoch, "*").await {
            Ok((true, _, _)) => votes += 1,
            Ok((false, _, _)) => {}
            Err(e) => debug!("sentinel: peer {} is-master-down failed: {}", addr, e),
        }
    }
    votes
}

/// Full `IS-MASTER-DOWN-BY-ADDR` reply: `(down, leader_runid, leader_epoch)`.
async fn query_is_master_down(
    peer_addr: &str,
    ip: &str,
    port: u16,
    epoch: &str,
    runid: &str,
) -> Result<(bool, String, u64), String> {
    let mut stream = connect(peer_addr).await?;
    let port_s = port.to_string();
    match resp_command(
        &mut stream,
        &[
            "SENTINEL",
            "IS-MASTER-DOWN-BY-ADDR",
            ip,
            &port_s,
            epoch,
            runid,
        ],
        IO_TIMEOUT,
    )
    .await
    {
        Ok(RespValue::Array(a)) if !a.is_empty() => {
            let down = match a.first() {
                Some(RespValue::Integer(n)) => *n != 0,
                Some(RespValue::BulkString(Some(b))) => b.as_ref() != b"0",
                _ => false,
            };
            let leader = match a.get(1) {
                Some(RespValue::BulkString(Some(b))) => {
                    String::from_utf8_lossy(b).into_owned()
                }
                Some(RespValue::SimpleString(b)) => String::from_utf8_lossy(b).into_owned(),
                _ => String::new(),
            };
            let leader_epoch = match a.get(2) {
                Some(RespValue::Integer(n)) if *n >= 0 => *n as u64,
                Some(RespValue::BulkString(Some(b))) => {
                    String::from_utf8_lossy(b).parse().unwrap_or(0)
                }
                _ => 0,
            };
            Ok((down, leader, leader_epoch))
        }
        Ok(RespValue::Integer(n)) => Ok((n != 0, String::new(), 0)),
        Ok(other) => Err(format!("unexpected is-master-down reply: {:?}", other)),
        Err(e) => Err(e),
    }
}

/// Campaign for failover leadership and return whether we won (Batch FE).
///
/// - Sole sentinel (no peers): vote for self and return `true`.
/// - Multi-sentinel: sticky vote for self at a campaign epoch; solicit peers via
///   `IS-MASTER-DOWN-BY-ADDR` with our runid; win if votes ≥ `leader_votes_needed`.
/// - If we already voted for another runid this epoch, abstain (`false`).
///
/// Manual `SENTINEL FAILOVER` does not call this (operator force).
pub async fn try_elect_leader(sentinel: &SentinelState, name: &str) -> bool {
    let Some(m) = sentinel.master(name) else {
        return false;
    };
    let my_id = sentinel.my_id();

    // Already committed to another leader this epoch → abstain.
    if !m.leader_runid.is_empty() && m.leader_runid != my_id {
        debug!(
            "sentinel: abstain failover for {} (voted-leader={} epoch={})",
            name, m.leader_runid, m.leader_epoch
        );
        return false;
    }

    // Sole sentinel: no cross-process race.
    if sentinel.peers().is_empty() {
        let epoch = if m.leader_epoch > 0 {
            m.leader_epoch
        } else {
            sentinel.next_election_epoch().max(1)
        };
        let _ = sentinel.vote_leader(name, epoch, &my_id);
        return true;
    }

    // Reuse campaign epoch if we already vote for ourselves; else open a new one.
    let epoch = if m.leader_epoch > 0 && m.leader_runid == my_id {
        m.leader_epoch
    } else {
        sentinel.next_election_epoch()
    };
    let _ = sentinel.vote_leader(name, epoch, &my_id);

    let mut votes_for_me = 1u32; // self
    let epoch_s = epoch.to_string();
    for p in sentinel.peers() {
        let addr = format!("{}:{}", p.ip, p.port);
        match query_is_master_down(&addr, &m.ip, m.port, &epoch_s, &my_id).await {
            Ok((_down, leader, leader_epoch)) => {
                if leader_epoch == epoch && leader == my_id {
                    votes_for_me = votes_for_me.saturating_add(1);
                } else if leader_epoch == epoch && !leader.is_empty() && leader != "*" {
                    debug!(
                        "sentinel: peer {} voted for {} (epoch {}) not us",
                        addr, leader, leader_epoch
                    );
                }
            }
            Err(e) => debug!("sentinel: elect query {} failed: {}", addr, e),
        }
    }

    let need = sentinel.leader_votes_needed(name);
    if votes_for_me >= need {
        info!(
            "sentinel: +elected-leader {} epoch={} votes={}/{}",
            name, epoch, votes_for_me, need
        );
        true
    } else {
        debug!(
            "sentinel: not elected for {} epoch={} votes={}/{}",
            name, epoch, votes_for_me, need
        );
        false
    }
}

/// `SENTINEL MEET <ip> <port>` — learn peer runid and announce ourselves (Batch EX).
pub async fn meet_sentinel(
    sentinel: &SentinelState,
    ip: &str,
    port: u16,
) -> Result<(), String> {
    let addr = format!("{}:{}", ip, port);
    let mut stream = connect(&addr).await?;
    let peer_id = match resp_command(&mut stream, &["SENTINEL", "MYID"], IO_TIMEOUT).await {
        Ok(RespValue::BulkString(Some(b))) => String::from_utf8_lossy(&b).into_owned(),
        Ok(RespValue::Error(e)) => return Err(String::from_utf8_lossy(&e).into_owned()),
        Ok(other) => return Err(format!("unexpected MYID: {:?}", other)),
        Err(e) => return Err(e),
    };
    if peer_id == sentinel.my_id() {
        return Err("ERR SENTINEL MEET would meet myself".into());
    }
    let (my_ip, my_port) = sentinel.listen_addr();
    let my_id = sentinel.my_id();
    match resp_command(
        &mut stream,
        &[
            "SENTINEL",
            "MEETPEER",
            &my_id,
            &my_ip,
            &my_port.to_string(),
        ],
        IO_TIMEOUT,
    )
    .await
    {
        Ok(RespValue::SimpleString(s)) if s.as_ref() == b"OK" => {}
        Ok(RespValue::Error(e)) => return Err(String::from_utf8_lossy(&e).into_owned()),
        Ok(other) => return Err(format!("unexpected MEETPEER: {:?}", other)),
        Err(e) => return Err(e),
    }
    sentinel.add_peer(peer_id, ip, port);
    info!("sentinel: met peer {}:{}", ip, port);
    Ok(())
}

async fn probe_one(sentinel: &SentinelState, name: &str) {
    let Some(m) = sentinel.master(name) else {
        return;
    };
    let addr = format!("{}:{}", m.ip, m.port);
    match connect_and_ping(&addr).await {
        Ok(()) => {
            let mut replicas = fetch_replicas_role(&addr).await.unwrap_or_default();
            // Batch FM: enrich ROLE list with live INFO slave_priority per replica.
            enrich_replica_priorities(&mut replicas).await;
            sentinel.note_ok(name, Some(replicas));
        }
        Err(e) => {
            debug!("sentinel: ping {} failed: {}", addr, e);
        }
    }
}

/// Manual or auto: promote the best-ranked reachable replica (Batch FK + FM).
///
/// Serializes per master via [`SentinelState::begin_failover`] so manual
/// `SENTINEL FAILOVER` and the tick auto-failover path cannot overlap (Batch FC).
///
/// Auto path only invokes this after [`try_elect_leader`] wins (Batch FE) and
/// outside [`SentinelState::in_failover_cooldown`] (Batch FM). Manual
/// `SENTINEL FAILOVER` skips election **and** cooldown (operator force).
///
/// **Promote order (Batch FK):** walk [`rank_replicas_for_promote`] — highest
/// priority (0 skipped), then highest ROLE offset, then greatest `ip:port`.
/// Continues down the ranked list when [`promote_replica`] fails.
///
/// **Priority source (Batch FM):** before ranking, re-query each known replica's
/// `INFO replication` for `slave_priority` (ROLE still supplies offset; default 100).
///
/// Always records [`SentinelState::note_failover_attempt`] on exit so auto path
/// respects [`FAILOVER_COOLDOWN`].
pub async fn try_failover(sentinel: &SentinelState, name: &str) -> Result<(), String> {
    let m = sentinel
        .master(name)
        .ok_or_else(|| format!("ERR No such master with name '{}'", name))?;
    if m.replicas.is_empty() {
        // Best-effort refresh ROLE once more if master still up (unlikely in s_down).
        let addr = format!("{}:{}", m.ip, m.port);
        if let Ok(mut reps) = fetch_replicas_role(&addr).await {
            if !reps.is_empty() {
                enrich_replica_priorities(&mut reps).await;
                // Master still answered ROLE — mark ok + store replicas.
                sentinel.note_ok(name, Some(reps));
            }
        }
    }
    let m = sentinel
        .master(name)
        .ok_or_else(|| format!("ERR No such master with name '{}'", name))?;
    if m.replicas.is_empty() {
        return Err("ERR No good replica for failover".into());
    }

    // Batch FM: refresh live INFO priorities before ranking (master may be s_down).
    let mut replicas = m.replicas.clone();
    enrich_replica_priorities(&mut replicas).await;
    sentinel.set_replicas(name, replicas.clone());

    // Batch FK: rank before promote; drop priority-0 never-promote replicas.
    let ranked = rank_replicas_for_promote(&replicas);
    if ranked.is_empty() {
        // Still start cooldown so auto path does not thrash "no good replica".
        sentinel.note_failover_attempt(name);
        return Err("ERR No good replica for failover".into());
    }

    // Per-master in-progress gate (Batch FC).
    if let Err(e) = sentinel.begin_failover(name) {
        return Err(e);
    }

    let old_ip = m.ip.clone();
    let old_port = m.port;
    let result = async {
        for r in &ranked {
            let raddr = r.addr_key();
            // Prefer bare FAILOVER (Kore replica promote); fall back to REPLICAOF NO ONE.
            // promote_replica never succeeds on PING alone (Batch FC).
            if promote_replica(&raddr).await.is_ok() {
                sentinel.switch_master(name, r.ip.clone(), r.port);
                // Best-effort re-point old master as replica of new master.
                let _ = replicaof_to(&format!("{}:{}", old_ip, old_port), &r.ip, r.port).await;
                info!(
                    "sentinel: failover complete for {} -> {} (priority={} offset={})",
                    name, raddr, r.priority, r.repl_offset
                );
                return Ok(());
            }
        }
        Err("ERR All replicas failed promote".into())
    }
    .await;

    sentinel.end_failover(name);
    // Batch FM: always arm cooldown after a completed or failed attempt.
    sentinel.note_failover_attempt(name);
    result
}

/// Promote a replica so callers may `switch_master` to it.
///
/// **Batch FC success gate** — `PING` alone is **never** enough. Success only if:
/// 1. `FAILOVER` returns OK, **or**
/// 2. `REPLICAOF NO ONE` returns OK, **or**
/// 3. post-attempt `ROLE` reports `master` (already master / promote race)
///
/// On any other outcome returns `Err` so the failover loop tries the next
/// replica or reports failure without re-pointing clients at a still-replica.
async fn promote_replica(addr: &str) -> Result<(), String> {
    // Reachability first (required, but not sufficient for switch).
    connect_and_ping(addr).await?;

    match PROMOTE_INJECT.load(Ordering::SeqCst) {
        PROMOTE_INJECT_FORCE_FAIL => {
            return Err(format!(
                "promote inject: forced fail for {} (PING ok, no promote)",
                addr
            ));
        }
        PROMOTE_INJECT_FORCE_OK => return Ok(()),
        _ => {}
    }

    let mut stream = connect(addr).await?;
    // Kore / Redis: FAILOVER on replica promotes.
    match resp_command(&mut stream, &["FAILOVER"], IO_TIMEOUT).await {
        Ok(RespValue::SimpleString(s)) if s.as_ref() == b"OK" => return Ok(()),
        Ok(RespValue::Error(_)) | Ok(_) | Err(_) => {}
    }
    // Fallback: REPLICAOF NO ONE (needs persistence configured on target).
    match resp_command(
        &mut stream,
        &["REPLICAOF", "NO", "ONE"],
        IO_TIMEOUT,
    )
    .await
    {
        Ok(RespValue::SimpleString(s)) if s.as_ref() == b"OK" => return Ok(()),
        Ok(RespValue::Error(_)) | Ok(_) | Err(_) => {}
    }
    // Optional hardening: accept already-master (standalone target, or promote
    // that completed without a clean OK on this connection).
    if role_is_master(addr).await {
        return Ok(());
    }
    Err(format!(
        "promote failed for {}: FAILOVER/REPLICAOF did not OK and ROLE is not master",
        addr
    ))
}

/// True when `ROLE` bulk role field is `master`.
async fn role_is_master(addr: &str) -> bool {
    let Ok(mut stream) = connect(addr).await else {
        return false;
    };
    let Ok(reply) = resp_command(&mut stream, &["ROLE"], IO_TIMEOUT).await else {
        return false;
    };
    match reply {
        RespValue::Array(arr) if !arr.is_empty() => match &arr[0] {
            RespValue::BulkString(Some(b)) => b.eq_ignore_ascii_case(b"master"),
            _ => false,
        },
        _ => false,
    }
}

async fn replicaof_to(addr: &str, host: &str, port: u16) -> Result<(), String> {
    let mut stream = connect(addr).await?;
    match resp_command(
        &mut stream,
        &["REPLICAOF", host, &port.to_string()],
        IO_TIMEOUT,
    )
    .await
    {
        Ok(RespValue::SimpleString(s)) if s.as_ref() == b"OK" => Ok(()),
        Ok(RespValue::Error(e)) => Err(String::from_utf8_lossy(&e).into_owned()),
        Ok(other) => Err(format!("unexpected REPLICAOF reply: {:?}", other)),
        Err(e) => Err(e),
    }
}

async fn connect_and_ping(addr: &str) -> Result<(), String> {
    let mut stream = connect(addr).await?;
    match resp_command(&mut stream, &["PING"], IO_TIMEOUT).await {
        Ok(RespValue::SimpleString(s)) if s.eq_ignore_ascii_case(b"PONG") => Ok(()),
        Ok(RespValue::BulkString(Some(b))) if b.eq_ignore_ascii_case(b"PONG") => Ok(()),
        Ok(RespValue::Error(e)) => Err(String::from_utf8_lossy(&e).into_owned()),
        Ok(other) => Err(format!("unexpected PING reply: {:?}", other)),
        Err(e) => Err(e),
    }
}

/// Parse Redis ROLE: master → list of [ip, port, offset] slaves.
async fn fetch_replicas_role(addr: &str) -> Result<Vec<ReplicaInfo>, String> {
    let mut stream = connect(addr).await?;
    let reply = resp_command(&mut stream, &["ROLE"], IO_TIMEOUT).await?;
    parse_role_replicas(&reply)
}

/// Parse `slave_priority` / `replica_priority` from INFO text (Batch FM).
///
/// Returns `None` when the field is absent so callers can keep ROLE / injected
/// priority (no-persistence Kore INFO omits the key). Unparsable values → 100.
pub fn parse_info_slave_priority(info: &str) -> Option<u32> {
    for line in info.lines() {
        let line = line.trim();
        // Accept both Redis names; INFO uses snake_case keys.
        if let Some(rest) = line.strip_prefix("slave_priority:") {
            return Some(rest.trim().parse().unwrap_or(100));
        }
        if let Some(rest) = line.strip_prefix("replica_priority:") {
            return Some(rest.trim().parse().unwrap_or(100));
        }
    }
    None
}

/// Best-effort `INFO replication` → `slave_priority` for one replica address.
///
/// `None` when connect/INFO fails **or** the field is missing from the body.
async fn fetch_slave_priority(addr: &str) -> Option<u32> {
    let mut stream = connect(addr).await.ok()?;
    let reply = resp_command(&mut stream, &["INFO", "replication"], IO_TIMEOUT)
        .await
        .ok()?;
    let text = match reply {
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(&b).into_owned(),
        RespValue::SimpleString(b) => String::from_utf8_lossy(&b).into_owned(),
        _ => return None,
    };
    parse_info_slave_priority(&text)
}

/// Query each replica's INFO for live `slave_priority` (Batch FM).
///
/// On fetch failure or missing field keeps the existing priority (ROLE default
/// 100, inject via `with_rank`, or a prior successful INFO refresh).
async fn enrich_replica_priorities(replicas: &mut [ReplicaInfo]) {
    for r in replicas.iter_mut() {
        let addr = r.addr_key();
        match fetch_slave_priority(&addr).await {
            Some(p) => {
                if r.priority != p {
                    debug!(
                        "sentinel: replica {} slave_priority {} -> {}",
                        addr, r.priority, p
                    );
                }
                r.priority = p;
            }
            None => {
                debug!(
                    "sentinel: INFO slave_priority for {} unavailable; keeping {}",
                    addr, r.priority
                );
            }
        }
    }
}

fn parse_role_replicas(reply: &RespValue) -> Result<Vec<ReplicaInfo>, String> {
    let arr = match reply {
        RespValue::Array(a) => a,
        _ => return Err("ROLE not array".into()),
    };
    if arr.is_empty() {
        return Ok(Vec::new());
    }
    let role = match &arr[0] {
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_ascii_lowercase(),
        _ => return Ok(Vec::new()),
    };
    if role != "master" || arr.len() < 3 {
        return Ok(Vec::new());
    }
    let slaves = match &arr[2] {
        RespValue::Array(s) => s,
        _ => return Ok(Vec::new()),
    };
    let mut out = Vec::new();
    for s in slaves {
        let fields = match s {
            RespValue::Array(f) => f,
            _ => continue,
        };
        if fields.len() < 2 {
            continue;
        }
        let ip = match &fields[0] {
            RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
            _ => continue,
        };
        let port = match &fields[1] {
            RespValue::BulkString(Some(b)) => {
                String::from_utf8_lossy(b).parse::<u16>().unwrap_or(0)
            }
            RespValue::Integer(n) if *n > 0 && *n <= u16::MAX as i64 => *n as u16,
            _ => 0,
        };
        // ROLE master slave entry: [ip, port, offset] (Batch FK).
        let repl_offset = if fields.len() >= 3 {
            match &fields[2] {
                RespValue::BulkString(Some(b)) => {
                    String::from_utf8_lossy(b).parse::<u64>().unwrap_or(0)
                }
                RespValue::Integer(n) if *n >= 0 => *n as u64,
                _ => 0,
            }
        } else {
            0
        };
        if port > 0 {
            out.push(ReplicaInfo {
                ip,
                port,
                repl_offset,
                priority: 100, // default; 0 = never promote (Batch FK)
            });
        }
    }
    Ok(out)
}

async fn connect(addr: &str) -> Result<TcpStream, String> {
    match tokio::time::timeout(IO_TIMEOUT, TcpStream::connect(addr)).await {
        Ok(Ok(s)) => {
            let _ = s.set_nodelay(true);
            Ok(s)
        }
        Ok(Err(e)) => Err(format!("connect {}: {}", addr, e)),
        Err(_) => Err(format!("connect timeout {}", addr)),
    }
}

async fn resp_command(
    stream: &mut TcpStream,
    parts: &[&str],
    timeout: Duration,
) -> Result<RespValue, String> {
    let args: Vec<RespValue> = parts
        .iter()
        .map(|p| RespValue::BulkString(Some(Bytes::from(p.to_string()))))
        .collect();
    let payload = RespValue::Array(args).serialize();
    match tokio::time::timeout(timeout, stream.write_all(&payload)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(format!("write: {}", e)),
        Err(_) => return Err("write timeout".into()),
    }
    let mut parser = RespParser::new();
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        if let Some(val) = parser
            .parse()
            .map_err(|e| format!("parse: {}", e))?
        {
            return Ok(val);
        }
        let n = match tokio::time::timeout(timeout, stream.read(&mut buf)).await {
            Ok(Ok(0)) => return Err("connection closed".into()),
            Ok(Ok(n)) => n,
            Ok(Err(e)) => return Err(format!("read: {}", e)),
            Err(_) => return Err("read timeout".into()),
        };
        parser.feed(&buf[..n]);
    }
}

/// Flatten master info into Redis SENTINEL MASTER field array.
///
/// `other_sentinels` is the count of peer Sentinels (Batch EX).
pub fn master_fields(m: &MasterInfo, other_sentinels: usize) -> RespValue {
    let mut f = Vec::new();
    let push = |f: &mut Vec<RespValue>, k: &str, v: String| {
        f.push(RespValue::BulkString(Some(Bytes::from(k.to_string()))));
        f.push(RespValue::BulkString(Some(Bytes::from(v))));
    };
    push(&mut f, "name", m.name.clone());
    push(&mut f, "ip", m.ip.clone());
    push(&mut f, "port", m.port.to_string());
    push(&mut f, "runid", "".into());
    push(&mut f, "flags", m.flags());
    push(&mut f, "link-pending-commands", "0".into());
    push(&mut f, "link-refcount", "1".into());
    push(&mut f, "last-ping-sent", "0".into());
    push(
        &mut f,
        "last-ok-ping-reply",
        m.last_ok.elapsed().as_millis().to_string(),
    );
    push(
        &mut f,
        "last-ping-reply",
        m.last_ok.elapsed().as_millis().to_string(),
    );
    push(
        &mut f,
        "down-after-milliseconds",
        m.down_after_ms.to_string(),
    );
    push(&mut f, "info-refresh", "0".into());
    push(&mut f, "role-reported", "master".into());
    push(&mut f, "role-reported-time", "0".into());
    push(&mut f, "config-epoch", m.failover_epoch.to_string());
    push(&mut f, "num-slaves", m.replicas.len().to_string());
    push(
        &mut f,
        "num-other-sentinels",
        other_sentinels.to_string(),
    );
    push(&mut f, "quorum", m.quorum.to_string());
    push(&mut f, "failover-timeout", "180000".into());
    push(&mut f, "parallel-syncs", "1".into());
    push(&mut f, "down-votes", m.down_votes.to_string());
    // Batch FE: surface local voted-leader for this master.
    push(&mut f, "voted-leader", m.leader_runid.clone());
    push(
        &mut f,
        "voted-leader-epoch",
        m.leader_epoch.to_string(),
    );
    RespValue::Array(f)
}

/// Peer sentinel field array for `SENTINEL SENTINELS`.
pub fn peer_fields(p: &PeerSentinel) -> RespValue {
    let pairs = [
        ("name", format!("sentinel-{}", &p.id[..p.id.len().min(8)])),
        ("ip", p.ip.clone()),
        ("port", p.port.to_string()),
        ("runid", p.id.clone()),
        ("flags", "sentinel".into()),
        ("link-pending-commands", "0".into()),
        ("last-ping-sent", "0".into()),
        ("last-ok-ping-reply", "0".into()),
        ("last-ping-reply", "0".into()),
        ("down-after-milliseconds", "0".into()),
        ("last-hello-message", "0".into()),
        ("voted-leader", "".into()),
        ("voted-leader-epoch", "0".into()),
    ];
    let mut out = Vec::new();
    for (k, v) in pairs {
        out.push(RespValue::BulkString(Some(Bytes::from(k.to_string()))));
        out.push(RespValue::BulkString(Some(Bytes::from(v))));
    }
    RespValue::Array(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monitor_and_get_addr() {
        let s = SentinelState::new();
        s.monitor("mymaster", "127.0.0.1", 6379, 1).unwrap();
        assert_eq!(
            s.get_master_addr("mymaster"),
            Some(("127.0.0.1".into(), 6379))
        );
        assert!(s.monitor("mymaster", "10.0.0.1", 1, 1).is_err());
        s.remove("mymaster").unwrap();
        assert!(s.get_master_addr("mymaster").is_none());
    }

    #[test]
    fn set_down_after_and_sdown() {
        let s = SentinelState::new();
        s.monitor("m", "127.0.0.1", 1, 1).unwrap();
        s.set_option("m", "down-after-milliseconds", "50").unwrap();
        // Force last_ok into the past by re-writing — use sleep.
        std::thread::sleep(Duration::from_millis(60));
        assert!(s.maybe_sdown("m"));
        assert!(s.master("m").unwrap().s_down);
        // Quorum 1 + self vote → o_down.
        assert!(s.apply_down_votes("m", 0));
        assert!(s.master("m").unwrap().o_down);
        s.note_ok("m", None);
        assert!(!s.master("m").unwrap().s_down);
        assert!(!s.master("m").unwrap().o_down);
    }

    #[test]
    fn failover_in_progress_gate() {
        let s = SentinelState::new();
        s.monitor("m", "127.0.0.1", 1, 1).unwrap();
        assert!(!s.master("m").unwrap().failover_in_progress);
        s.begin_failover("m").unwrap();
        assert!(s.master("m").unwrap().failover_in_progress);
        let err = s.begin_failover("m").unwrap_err();
        assert!(
            err.to_ascii_lowercase().contains("already in progress"),
            "err={}",
            err
        );
        s.end_failover("m");
        assert!(!s.master("m").unwrap().failover_in_progress);
        s.begin_failover("m").unwrap();
        s.end_failover("m");
    }

    #[test]
    fn odown_needs_quorum_votes() {
        let s = SentinelState::new();
        s.monitor("m", "10.0.0.1", 6379, 2).unwrap();
        s.set_option("m", "down-after-milliseconds", "20").unwrap();
        std::thread::sleep(Duration::from_millis(30));
        assert!(s.maybe_sdown("m"));
        // Only self vote (1) < quorum 2.
        assert!(!s.apply_down_votes("m", 0));
        assert!(!s.master("m").unwrap().o_down);
        // One peer agrees.
        assert!(s.apply_down_votes("m", 1));
        assert!(s.master("m").unwrap().o_down);
        assert_eq!(s.master("m").unwrap().down_votes, 2);
    }

    #[test]
    fn is_master_down_by_addr_local() {
        let s = SentinelState::new();
        s.monitor("m", "10.0.0.9", 7000, 1).unwrap();
        let (d, _, _) = s.is_master_down_by_addr("10.0.0.9", 7000, 0, "*");
        assert_eq!(d, 0);
        s.set_option("m", "down-after-milliseconds", "20").unwrap();
        std::thread::sleep(Duration::from_millis(30));
        s.maybe_sdown("m");
        // Probe-only when s_down: lite returns self as leader.
        let (d, leader, epoch) = s.is_master_down_by_addr("10.0.0.9", 7000, 0, "*");
        assert_eq!(d, 1);
        assert_eq!(leader, s.my_id());
        assert!(epoch >= 1);
    }

    /// Batch FE: sticky vote per epoch; higher epoch may re-vote.
    #[test]
    fn leader_vote_sticky_and_higher_epoch() {
        let s = SentinelState::new();
        s.monitor("m", "10.0.0.9", 7000, 2).unwrap();
        s.set_option("m", "down-after-milliseconds", "20").unwrap();
        std::thread::sleep(Duration::from_millis(30));
        s.maybe_sdown("m");

        let cand_a = "aa".repeat(20);
        let cand_b = "bb".repeat(20);

        let (d, leader, epoch) =
            s.is_master_down_by_addr("10.0.0.9", 7000, 5, &cand_a);
        assert_eq!(d, 1);
        assert_eq!(leader, cand_a);
        assert_eq!(epoch, 5);

        // Same epoch, different candidate → sticky first vote.
        let (_, leader2, epoch2) =
            s.is_master_down_by_addr("10.0.0.9", 7000, 5, &cand_b);
        assert_eq!(leader2, cand_a);
        assert_eq!(epoch2, 5);

        // Higher epoch → re-vote for B.
        let (_, leader3, epoch3) =
            s.is_master_down_by_addr("10.0.0.9", 7000, 6, &cand_b);
        assert_eq!(leader3, cand_b);
        assert_eq!(epoch3, 6);

        assert!(!s.is_failover_leader("m"));
        assert_eq!(s.master("m").unwrap().leader_runid, cand_b);
    }

    /// Batch FE: sole sentinel is failover leader; voted-for-other abstains.
    #[test]
    fn is_failover_leader_sole_and_voted_other() {
        let s = SentinelState::new();
        s.monitor("m", "10.0.0.1", 6379, 1).unwrap();
        // No peers, no vote yet → sole leader.
        assert!(s.is_failover_leader("m"));
        assert_eq!(s.leader_votes_needed("m"), 1);

        s.set_option("m", "down-after-milliseconds", "20").unwrap();
        std::thread::sleep(Duration::from_millis(30));
        s.maybe_sdown("m");
        let other = "cc".repeat(20);
        let _ = s.vote_leader("m", 3, &other);
        assert!(!s.is_failover_leader("m"));
        // Inject a peer so N=2 → majority 2, quorum 1 → need 2.
        s.add_peer("dd".repeat(20), "10.0.0.2", 26380);
        assert_eq!(s.leader_votes_needed("m"), 2);
    }

    /// Batch FE: add_peer only changes (and would autosave) when new/updated.
    #[test]
    fn add_peer_no_change_returns_false() {
        let s = SentinelState::new();
        let id = "ee".repeat(20);
        assert!(s.add_peer(&id, "10.0.0.3", 26379));
        assert!(!s.add_peer(&id, "10.0.0.3", 26379));
        assert!(s.add_peer(&id, "10.0.0.4", 26379)); // ip change
        assert_eq!(s.peers().len(), 1);
        assert_eq!(s.peers()[0].ip, "10.0.0.4");
    }

    #[test]
    fn parse_role_master_with_slaves() {
        let reply = RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"master"))),
            RespValue::Integer(100),
            RespValue::Array(vec![RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"10.0.0.2"))),
                RespValue::BulkString(Some(Bytes::from_static(b"6380"))),
                RespValue::BulkString(Some(Bytes::from_static(b"50"))),
            ])]),
        ]);
        let r = parse_role_replicas(&reply).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].ip, "10.0.0.2");
        assert_eq!(r[0].port, 6380);
        assert_eq!(r[0].repl_offset, 50); // Batch FK: ROLE offset
        assert_eq!(r[0].priority, 100);
    }

    /// Batch FK: rank by priority then offset then greatest ip:port; priority 0 dropped.
    #[test]
    fn rank_replicas_priority_offset_and_skip_zero() {
        let a = ReplicaInfo::new("10.0.0.1", 6381).with_rank(100, 500);
        let b = ReplicaInfo::new("10.0.0.2", 6382).with_rank(200, 10); // higher pri
        let c = ReplicaInfo::new("10.0.0.3", 6383).with_rank(0, 9_999); // never
        let d = ReplicaInfo::new("10.0.0.4", 6384).with_rank(100, 900); // same pri, higher off
        let ranked = rank_replicas_for_promote(&[a.clone(), b.clone(), c.clone(), d.clone()]);
        assert_eq!(ranked.len(), 3, "priority 0 excluded");
        assert_eq!(ranked[0].port, 6382, "highest priority first");
        assert_eq!(ranked[1].port, 6384, "higher offset when priority ties");
        assert_eq!(ranked[2].port, 6381);
        // Sole priority-0 list → empty.
        assert!(rank_replicas_for_promote(&[c]).is_empty());
    }

    #[test]
    fn rank_replicas_tiebreak_greatest_addr() {
        let lo = ReplicaInfo::new("10.0.0.1", 6380).with_rank(100, 100);
        let hi = ReplicaInfo::new("10.0.0.9", 6380).with_rank(100, 100);
        let ranked = rank_replicas_for_promote(&[lo, hi]);
        assert_eq!(ranked[0].ip, "10.0.0.9");
    }

    /// Batch FM: INFO slave_priority parse (missing → None; present → value).
    #[test]
    fn parse_info_slave_priority_fields() {
        assert_eq!(parse_info_slave_priority("role:slave\r\n"), None);
        assert_eq!(
            parse_info_slave_priority("role:slave\r\nslave_priority:50\r\n"),
            Some(50)
        );
        assert_eq!(
            parse_info_slave_priority("slave_priority:0\r\nrole:slave\r\n"),
            Some(0)
        );
        assert_eq!(
            parse_info_slave_priority("replica_priority:150\r\n"),
            Some(150)
        );
        assert_eq!(
            parse_info_slave_priority("slave_priority:notanumber\r\n"),
            Some(100)
        );
    }

    /// Batch FM: auto cooldown after note_failover_attempt; clear restores.
    #[test]
    fn failover_cooldown_gates_auto_reentry() {
        let s = SentinelState::new();
        s.monitor("m", "10.0.0.1", 6379, 1).unwrap();
        assert!(!s.in_failover_cooldown("m"));
        // Short override so unit test is fast.
        test_set_failover_cooldown_ms(200);
        s.note_failover_attempt("m");
        assert!(s.in_failover_cooldown("m"));
        s.clear_failover_cooldown("m");
        assert!(!s.in_failover_cooldown("m"));
        // Expired cooldown.
        s.note_failover_attempt("m");
        assert!(s.in_failover_cooldown("m"));
        std::thread::sleep(Duration::from_millis(220));
        assert!(!s.in_failover_cooldown("m"));
        test_set_failover_cooldown_ms(0); // restore default
        assert_eq!(failover_cooldown_duration(), FAILOVER_COOLDOWN);
    }

    #[test]
    fn hello_parse_format_and_apply() {
        let a = SentinelState::new();
        a.set_listen_addr("127.0.0.1", 26379);
        a.monitor("mymaster", "10.0.0.1", 6379, 1).unwrap();
        let csv = a.format_hello("mymaster").unwrap();
        let msg = SentinelState::parse_hello(&csv).unwrap();
        assert_eq!(msg.master_name, "mymaster");
        assert_eq!(msg.master_ip, "10.0.0.1");
        assert_eq!(msg.master_port, 6379);
        assert_eq!(msg.runid, a.my_id());

        let b = SentinelState::new();
        b.monitor("mymaster", "10.0.0.1", 6379, 1).unwrap();
        // Peer hello with higher master config epoch and new address.
        let peer_hello = format!(
            "10.0.0.9,26380,{},5,mymaster,10.0.0.2,6380,3",
            "bb".repeat(20)
        );
        let msg = SentinelState::parse_hello(&peer_hello).unwrap();
        assert!(b.apply_hello(&msg));
        assert_eq!(b.peers().len(), 1);
        assert_eq!(b.peers()[0].port, 26380);
        let m = b.master("mymaster").unwrap();
        assert_eq!(m.ip, "10.0.0.2");
        assert_eq!(m.port, 6380);
        assert_eq!(m.failover_epoch, 3);
        // Self hello is ignored.
        let self_csv = b.format_hello("mymaster").unwrap();
        let self_msg = SentinelState::parse_hello(&self_csv).unwrap();
        assert!(!b.apply_hello(&self_msg));
    }

    #[test]
    fn sentinel_conf_round_trip() {
        let dir = std::env::temp_dir().join(format!(
            "kore-sent-conf-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let a = SentinelState::new();
        // No conf_dir yet so monitor won't write until we set it after building body.
        a.monitor("mymaster", "10.0.0.1", 6379, 2).unwrap();
        a.set_option("mymaster", "down-after-milliseconds", "5000")
            .unwrap();
        a.set_option("mymaster", "auto-failover", "no").unwrap();
        // insert peer without autosave side effects beyond empty conf_dir
        {
            let mut g = a.peers.write();
            g.insert(
                "pp".repeat(20),
                PeerSentinel {
                    id: "pp".repeat(20),
                    ip: "10.0.0.2".into(),
                    port: 26380,
                },
            );
        }
        let my = a.my_id();
        let path = a.save_conf_to(dir.to_str().unwrap()).unwrap();
        assert!(path.exists());
        let body = std::fs::read_to_string(&path).unwrap();
        assert!(body.contains("sentinel monitor mymaster"));
        assert!(body.contains(&my));

        let b = SentinelState::from_conf_text(&body).unwrap();
        assert_eq!(b.my_id(), my);
        let m = b.master("mymaster").unwrap();
        assert_eq!(m.ip, "10.0.0.1");
        assert_eq!(m.port, 6379);
        assert_eq!(m.quorum, 2);
        assert_eq!(m.down_after_ms, 5000);
        assert!(!m.auto_failover);
        assert_eq!(b.peers().len(), 1);
        assert_eq!(b.peers()[0].port, 26380);

        let loaded = SentinelState::load_or_new(dir.to_str().unwrap());
        assert!(loaded.master("mymaster").is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
