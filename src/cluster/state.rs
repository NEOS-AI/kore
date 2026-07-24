//! Cluster topology: slot ownership, migrating/importing, membership, fail flags.
//!
//! **Ownership epochs (Batch DU):** each slot has a config epoch. Local
//! `SETSLOT NODE` / reassign bumps `current_epoch` and stamps the slot.
//! Peers exchange compressed ownership ranges via `CLUSTER OWNERS` on gossip;
//! higher per-slot epoch wins. Slots in local MIGRATING/IMPORTING are not
//! overwritten by peer gossip (transition safety). Not Redis binary bus / 2PC.
//!
//! **nodes.conf (Batch EM/EN/EO):** explicit `CLUSTER SAVECONFIG` and best-effort
//! autosave after topology-mutating ops / failover claim when a dir is configured.
//!
//! **Fail quorum (Batch DW):** unreachable peers start as `pfail`. Full `fail`
//! requires a vote quorum among known masters (`masters/2+1`). Clusters with
//! ≤2 masters keep **single-observer** fail (back-compat / small deploy).
//! Votes = local pfail/fail + peer `CLUSTER FAILREPORTS` lists.
//!
//! **Replica election (Batch DY/EA):** on master FAIL, the known replica with
//! the highest replication offset wins; **node id** breaks ties (Batch EA).
//! Roles + offsets propagate via `CLUSTER ROLEMAP` / MEETPEER role fields.

use super::crc16::SLOT_COUNT;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Default Redis-like node timeout (ms). Tests may lower this.
pub const DEFAULT_NODE_TIMEOUT_MS: u64 = 15_000;

/// Information about a known cluster node.
#[derive(Debug, Clone)]
pub struct ClusterNode {
    pub id: String,
    pub ip: String,
    pub port: u16,
    /// Bus port (Redis default: client port + 10000). Informational only for MVP.
    pub cport: u16,
    pub myself: bool,
    pub master: bool,
    /// When `master == false`, id of the master this node replicates.
    pub master_id: Option<String>,
    /// Possible fail (timeout observed; not yet quorum-confirmed). Batch DW.
    pub pfail: bool,
    /// Confirmed fail (quorum or small-cluster single-observer).
    pub fail: bool,
    /// Last known replication offset for election (Batch EA). 0 if unknown.
    pub repl_offset: u64,
    /// Replica priority for election (Batch EB). Higher wins; **0 = never promote**.
    /// Default 100 (Redis SLAVE-PRIORITY style).
    pub repl_priority: u32,
}

/// Redirect target for MOVED / ASK replies.
#[derive(Debug, Clone)]
pub struct RedirectTarget {
    pub slot: u16,
    pub ip: String,
    pub port: u16,
    pub node_id: String,
}

/// Compressed ownership range for gossip / `CLUSTER OWNERS` (Batch DU).
///
/// Consecutive slots with the same owner **and** epoch are collapsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipRange {
    pub start: u16,
    pub end: u16,
    pub owner_id: String,
    pub ip: String,
    pub port: u16,
    pub epoch: u64,
}

/// One node's role for `CLUSTER ROLEMAP` (Batch DY/EA).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleMapEntry {
    pub id: String,
    pub master: bool,
    /// Master id when `master == false`; empty when master.
    pub master_id: String,
    pub ip: String,
    pub port: u16,
    /// Replication offset for failover election (Batch EA).
    pub repl_offset: u64,
    /// Replica priority for election (Batch EB). Higher wins; 0 = never promote.
    pub repl_priority: u32,
}

/// Manual `CLUSTER FAILOVER` modes (Batch EC).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManualFailoverMode {
    /// Master must be fail/pfail; caller must be election winner.
    Safe,
    /// Mark master fail; still require election winner.
    Force,
    /// Mark master fail and claim regardless of election (Redis TAKEOVER).
    Takeover,
}

/// Result of applying one peer ownership range (or slot).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnershipApplyResult {
    /// Peer epoch higher — owner/epoch updated.
    Applied,
    /// Local epoch ≥ peer, or equal-epoch conflict kept local.
    RejectedStale,
    /// Slot is local MIGRATING or IMPORTING — not clobbered by gossip.
    SkippedTransition,
    /// Invalid slot / empty range.
    Invalid,
}

/// Cluster topology for this process.
#[derive(Debug)]
pub struct ClusterState {
    inner: RwLock<Inner>,
    /// Directory for `nodes.conf` autosave (Batch EO). `None` disables autosave
    /// (explicit `save_nodes_conf_to` still works with a caller-supplied dir).
    nodes_conf_dir: RwLock<Option<String>>,
    /// Test-only: remaining forced failures for local `SETSLOT NODE` (Batch EP).
    /// Per-instance so parallel integration tests do not race.
    source_node_fail_inject: AtomicU32,
    /// When true (default), cluster is `fail` unless every slot has a non-fail
    /// owner (Batch EQ / Redis `cluster-require-full-coverage`).
    require_full_coverage: AtomicBool,
    /// When true, allow read key commands while `cluster_state` is fail (Batch ES).
    allow_reads_when_down: AtomicBool,
    /// Client-facing IP override for myself (Batch EU / cluster-announce-ip).
    announce_ip: RwLock<Option<String>>,
    /// Client-facing port override for myself (Batch EU / cluster-announce-port).
    announce_port: RwLock<Option<u16>>,
}

#[derive(Debug)]
struct Inner {
    my_id: String,
    ip: String,
    port: u16,
    /// slot → owning node id
    slot_owner: Vec<String>,
    /// Per-slot config epoch (Batch DU). Higher wins on gossip merge.
    slot_config_epoch: Vec<u64>,
    /// slot → destination node id (we are migrating this slot away)
    migrating: HashMap<u16, String>,
    /// slot → source node id (we are importing this slot)
    importing: HashMap<u16, String>,
    /// known nodes by id
    nodes: HashMap<String, ClusterNode>,
    current_epoch: u64,
    /// Fail detection timeout (ms). Heartbeat marks pfail/fail after this without pong.
    node_timeout_ms: u64,
    /// Last successful heartbeat / meet time per peer id.
    last_pong: HashMap<String, Instant>,
    /// peer_id → node ids that peer reports as pfail/fail (Batch DW).
    fail_reports: HashMap<String, HashSet<String>>,
    /// This process's replication offset for election (Batch EA).
    local_repl_offset: u64,
    /// This process's replica priority for election (Batch EB). Default 100.
    local_repl_priority: u32,
}

impl ClusterState {
    /// Create a single-node cluster that owns every slot.
    pub fn single_node(ip: impl Into<String>, port: u16) -> Arc<Self> {
        Self::with_identity(ip, port, generate_node_id(), true, None, true)
    }

    /// Best-effort load of `{dir}/nodes.conf` (Batch EN).
    ///
    /// On missing file or parse error, falls back to [`Self::single_node`].
    pub fn load_or_single_node(ip: impl Into<String>, port: u16, dir: &str) -> Arc<Self> {
        let ip = ip.into();
        let path = std::path::Path::new(if dir.is_empty() { "." } else { dir }).join("nodes.conf");
        match std::fs::read_to_string(&path) {
            Ok(text) => match Self::from_nodes_conf(ip.clone(), port, &text) {
                Ok(cs) => {
                    tracing::info!("cluster: loaded topology from {}", path.display());
                    cs
                }
                Err(e) => {
                    tracing::warn!(
                        "cluster: failed to load {}: {} — starting single-node",
                        path.display(),
                        e
                    );
                    Self::single_node(ip, port)
                }
            },
            Err(_) => Self::single_node(ip, port),
        }
    }

    /// Build cluster state from `CLUSTER NODES` / `SAVECONFIG` text (Batch EN).
    ///
    /// Resolves **myself** via `myself` flag, else matching `ip:port`. Restores
    /// node id, peers, slot ownership, and config epoch. Migrating/importing
    /// annotations in the file are ignored (start stable).
    pub fn from_nodes_conf(
        ip: impl Into<String>,
        port: u16,
        text: &str,
    ) -> std::result::Result<Arc<Self>, String> {
        let ip = ip.into();
        let header_epoch = parse_nodes_conf_header_epoch(text);
        let lines = parse_nodes_conf_lines(text)?;
        if lines.is_empty() {
            return Err("no node lines in nodes.conf".into());
        }
        let myself = lines
            .iter()
            .find(|l| l.myself)
            .or_else(|| lines.iter().find(|l| l.ip == ip && l.port == port))
            .ok_or_else(|| {
                "no myself line and no node matching this process ip:port".to_string()
            })?;

        let cs = Self::with_identity(
            ip,
            port,
            myself.id.clone(),
            myself.master,
            myself.master_id.clone(),
            false, // start with unbound slots; apply file ownership
        );

        let file_epoch = lines.iter().map(|l| l.epoch).max().unwrap_or(1);
        let epoch = header_epoch.unwrap_or(file_epoch).max(1);

        {
            let mut g = cs.inner.write();
            g.current_epoch = epoch;
            let my_id = g.my_id.clone();
            for line in &lines {
                if line.id == my_id {
                    if let Some(me) = g.nodes.get_mut(&my_id) {
                        me.master = line.master;
                        me.master_id = line.master_id.clone();
                        me.fail = false;
                        me.pfail = false;
                    }
                } else {
                    let cport = line.port.saturating_add(10000);
                    g.nodes.insert(
                        line.id.clone(),
                        ClusterNode {
                            id: line.id.clone(),
                            ip: line.ip.clone(),
                            port: line.port,
                            cport,
                            myself: false,
                            master: line.master,
                            master_id: line.master_id.clone(),
                            pfail: line.pfail,
                            fail: line.fail,
                            repl_offset: 0,
                            repl_priority: 100,
                        },
                    );
                    g.last_pong
                        .entry(line.id.clone())
                        .or_insert_with(Instant::now);
                }
                for &(start, end) in &line.slots {
                    for slot in start..=end {
                        if slot < SLOT_COUNT {
                            g.slot_owner[slot as usize] = line.id.clone();
                            g.slot_config_epoch[slot as usize] = epoch;
                        }
                    }
                }
            }
        }
        Ok(cs)
    }

    fn with_identity(
        ip: impl Into<String>,
        port: u16,
        my_id: String,
        master: bool,
        master_id: Option<String>,
        own_all_slots: bool,
    ) -> Arc<Self> {
        let ip = ip.into();
        let cport = port.saturating_add(10000);
        let mut nodes = HashMap::new();
        nodes.insert(
            my_id.clone(),
            ClusterNode {
                id: my_id.clone(),
                ip: ip.clone(),
                port,
                cport,
                myself: true,
                master,
                master_id,
                pfail: false,
                fail: false,
                repl_offset: 0,
                repl_priority: 100,
            },
        );
        let slot_owner = if own_all_slots {
            vec![my_id.clone(); SLOT_COUNT as usize]
        } else {
            vec![String::new(); SLOT_COUNT as usize]
        };
        let slot_config_epoch = vec![1u64; SLOT_COUNT as usize];
        Arc::new(Self {
            inner: RwLock::new(Inner {
                my_id,
                ip,
                port,
                slot_owner,
                slot_config_epoch,
                migrating: HashMap::new(),
                importing: HashMap::new(),
                nodes,
                current_epoch: 1,
                node_timeout_ms: DEFAULT_NODE_TIMEOUT_MS,
                last_pong: HashMap::new(),
                fail_reports: HashMap::new(),
                local_repl_offset: 0,
                local_repl_priority: 100,
            }),
            nodes_conf_dir: RwLock::new(None),
            source_node_fail_inject: AtomicU32::new(0),
            require_full_coverage: AtomicBool::new(true),
            allow_reads_when_down: AtomicBool::new(false),
            announce_ip: RwLock::new(None),
            announce_port: RwLock::new(None),
        })
    }

    /// Redis `cluster-require-full-coverage` (Batch EQ). Default `true`.
    pub fn set_require_full_coverage(&self, require: bool) {
        self.require_full_coverage.store(require, Ordering::Relaxed);
    }

    /// Whether full slot coverage is required for `cluster_state:ok`.
    pub fn require_full_coverage(&self) -> bool {
        self.require_full_coverage.load(Ordering::Relaxed)
    }

    /// Redis `cluster-allow-reads-when-down` (Batch ES). Default `false`.
    pub fn set_allow_reads_when_down(&self, allow: bool) {
        self.allow_reads_when_down.store(allow, Ordering::Relaxed);
    }

    /// Whether read commands are allowed when `cluster_state` is fail.
    pub fn allow_reads_when_down(&self) -> bool {
        self.allow_reads_when_down.load(Ordering::Relaxed)
    }

    /// Redis `cluster-announce-ip` (Batch EU). Empty clears override.
    pub fn set_announce_ip(&self, ip: Option<String>) {
        {
            *self.announce_ip.write() = ip.filter(|s| !s.is_empty());
        } // drop write before refresh (non-reentrant RwLock)
        self.refresh_myself_advertised();
    }

    /// Configured announce IP, if any.
    pub fn announce_ip(&self) -> Option<String> {
        self.announce_ip.read().clone()
    }

    /// Redis `cluster-announce-port` (Batch EU). `None` or 0 clears override.
    pub fn set_announce_port(&self, port: Option<u16>) {
        {
            *self.announce_port.write() = port.filter(|&p| p > 0);
        }
        self.refresh_myself_advertised();
    }

    /// Configured announce port, if any.
    pub fn announce_port(&self) -> Option<u16> {
        *self.announce_port.read()
    }

    /// Client-facing address for this node (announce override or bind addr).
    ///
    /// Used for MEETPEER, CLUSTER NODES/SLOTS, OWNERS gossip, and MOVED when we
    /// are the owner (via the myself entry in the nodes table).
    pub fn advertised_addr(&self) -> (String, u16) {
        let g = self.inner.read();
        let ip = self
            .announce_ip
            .read()
            .clone()
            .unwrap_or_else(|| g.ip.clone());
        let port = self.announce_port.read().unwrap_or(g.port);
        (ip, port)
    }

    /// Re-stamp the myself node with the current advertised address (Batch EU).
    fn refresh_myself_advertised(&self) {
        let (ip, port) = {
            let g = self.inner.read();
            let ip = self
                .announce_ip
                .read()
                .clone()
                .unwrap_or_else(|| g.ip.clone());
            let port = self.announce_port.read().unwrap_or(g.port);
            (ip, port)
        };
        let mut g = self.inner.write();
        let my_id = g.my_id.clone();
        if let Some(me) = g.nodes.get_mut(&my_id) {
            me.ip = ip;
            me.port = port;
            me.cport = port.saturating_add(10000);
        }
    }

    /// True when every hash slot has a known, non-`fail` owner.
    ///
    /// Unbound slots and slots owned by fail-marked nodes are not covered.
    pub fn has_full_coverage(&self) -> bool {
        let g = self.inner.read();
        for id in &g.slot_owner {
            if id.is_empty() {
                return false;
            }
            match g.nodes.get(id.as_str()) {
                Some(n) if !n.fail => {}
                _ => return false,
            }
        }
        true
    }

    /// Redis `cluster_state:ok` vs `fail` (Batch EQ).
    ///
    /// With `require_full_coverage`, returns false when any slot is unserved.
    /// When coverage is not required, always true (per-slot CLUSTERDOWN still
    /// applies for unbound slots).
    pub fn cluster_state_ok(&self) -> bool {
        if !self.require_full_coverage() {
            return true;
        }
        self.has_full_coverage()
    }

    /// Configure the directory used by [`Self::autosave_nodes_conf`] (Batch EO).
    ///
    /// Empty string clears the dir (disables autosave). Call on boot after
    /// [`Self::load_or_single_node`] so failover claims can persist topology.
    pub fn set_nodes_conf_dir(&self, dir: impl Into<String>) {
        let d = dir.into();
        *self.nodes_conf_dir.write() = if d.is_empty() { None } else { Some(d) };
    }

    /// Directory configured for autosave, if any.
    pub fn nodes_conf_dir(&self) -> Option<String> {
        self.nodes_conf_dir.read().clone()
    }

    /// Write `CLUSTER NODES` text to `{dir}/nodes.conf` (Batch EM/EO).
    ///
    /// Header includes current config epoch for operators / loaders (Batch EN).
    pub fn save_nodes_conf_to(
        &self,
        dir: &str,
    ) -> std::result::Result<std::path::PathBuf, String> {
        use std::fs;
        use std::io::Write;
        use std::path::PathBuf;

        let dir = if dir.is_empty() { "." } else { dir };
        let path = PathBuf::from(dir).join("nodes.conf");
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| format!("cannot create dir: {}", e))?;
        }
        let body = self.format_nodes();
        let mut f = fs::File::create(&path)
            .map_err(|e| format!("cannot create {}: {}", path.display(), e))?;
        writeln!(f, "# Kore cluster nodes.conf generated by CLUSTER SAVECONFIG")
            .map_err(|e| format!("write error: {}", e))?;
        writeln!(f, "# epoch {}", self.current_epoch())
            .map_err(|e| format!("write error: {}", e))?;
        f.write_all(body.as_bytes())
            .map_err(|e| format!("write error: {}", e))?;
        f.sync_all().map_err(|e| format!("sync error: {}", e))?;
        Ok(path)
    }

    /// Best-effort persist when [`Self::set_nodes_conf_dir`] was configured (Batch EO).
    ///
    /// No-op when dir is unset. Logs at `warn` on I/O failure (does not surface
    /// to clients — matching Redis-style best-effort cluster config rewrite).
    pub fn autosave_nodes_conf(&self) {
        let Some(dir) = self.nodes_conf_dir.read().clone() else {
            return;
        };
        match self.save_nodes_conf_to(&dir) {
            Ok(path) => {
                tracing::debug!("cluster: autosaved config to {}", path.display());
            }
            Err(e) => {
                tracing::warn!("cluster: autosave nodes.conf failed: {}", e);
            }
        }
    }

    /// Update this node's replication offset used for failover election (Batch EA).
    pub fn set_local_repl_offset(&self, offset: u64) {
        let mut g = self.inner.write();
        g.local_repl_offset = offset;
        let my_id = g.my_id.clone();
        if let Some(me) = g.nodes.get_mut(&my_id) {
            me.repl_offset = offset;
        }
    }

    /// Local election offset (also stored on the myself node entry).
    pub fn local_repl_offset(&self) -> u64 {
        self.inner.read().local_repl_offset
    }

    /// Set replica priority for failover election (Batch EB). 0 = never promote.
    pub fn set_local_repl_priority(&self, priority: u32) {
        let mut g = self.inner.write();
        g.local_repl_priority = priority;
        let my_id = g.my_id.clone();
        if let Some(me) = g.nodes.get_mut(&my_id) {
            me.repl_priority = priority;
        }
    }

    /// Local replica priority (default 100).
    pub fn local_repl_priority(&self) -> u32 {
        self.inner.read().local_repl_priority
    }

    /// Process-wide config epoch (bumped on local ownership changes).
    pub fn current_epoch(&self) -> u64 {
        self.inner.read().current_epoch
    }

    /// Force-bump config epoch (`CLUSTER BUMPEPOCH`, Batch ED). Returns new epoch.
    pub fn bump_epoch(&self) -> u64 {
        let mut g = self.inner.write();
        g.current_epoch = g.current_epoch.saturating_add(1);
        g.current_epoch
    }

    /// `CLUSTER SET-CONFIG-EPOCH <epoch>` (Batch EK).
    ///
    /// Sets `current_epoch` only when `epoch` is **strictly greater** than the
    /// current value (Redis-compatible). Does not rewrite per-slot epochs.
    pub fn set_config_epoch(&self, epoch: u64) -> Result<(), String> {
        let mut g = self.inner.write();
        if epoch <= g.current_epoch {
            return Err(format!(
                "ERR Node config epoch ({}) is not greater than the current local config epoch ({})",
                epoch, g.current_epoch
            ));
        }
        g.current_epoch = epoch;
        Ok(())
    }

    /// `CLUSTER FORGET <node-id>` — drop a peer from the nodes table (Batch EG).
    ///
    /// Cannot forget ourselves or a node that still owns slots.
    pub fn forget_node(&self, node_id: &str) -> Result<(), String> {
        let mut g = self.inner.write();
        if node_id == g.my_id {
            return Err("ERR I tried hard but I can't forget myself".into());
        }
        if !g.nodes.contains_key(node_id) {
            return Err(format!("ERR Unknown node {}", node_id));
        }
        if g.slot_owner.iter().any(|o| o == node_id) {
            return Err(
                "ERR Can't forget a master with assigned slots. Delete slots first.".into(),
            );
        }
        g.nodes.remove(node_id);
        g.last_pong.remove(node_id);
        g.fail_reports.remove(node_id);
        for set in g.fail_reports.values_mut() {
            set.remove(node_id);
        }
        // Drop migrating/importing edges that named this peer.
        g.migrating.retain(|_, dest| dest != node_id);
        g.importing.retain(|_, src| src != node_id);
        Ok(())
    }

    /// `CLUSTER RESET [SOFT|HARD]` topology half (Batch EG).
    ///
    /// Soft/Hard both clear slots, peers, and replica role. **Hard** keyspace
    /// wipe is performed by the command handler (`FLUSHALL`-style).
    pub fn reset_cluster_config(&self) {
        let mut g = self.inner.write();
        let my_id = g.my_id.clone();
        g.current_epoch = g.current_epoch.saturating_add(1);
        let epoch = g.current_epoch;
        for i in 0..SLOT_COUNT as usize {
            g.slot_owner[i].clear();
            g.slot_config_epoch[i] = epoch;
        }
        g.migrating.clear();
        g.importing.clear();
        g.fail_reports.clear();
        g.last_pong.clear();
        // Keep only myself; force master.
        g.nodes.retain(|id, _| id == &my_id);
        if let Some(me) = g.nodes.get_mut(&my_id) {
            me.myself = true;
            me.master = true;
            me.master_id = None;
            me.fail = false;
            me.pfail = false;
        }
    }

    /// Config epoch for a single slot (`0` if out of range).
    pub fn slot_epoch(&self, slot: u16) -> u64 {
        let g = self.inner.read();
        g.slot_config_epoch
            .get(slot as usize)
            .copied()
            .unwrap_or(0)
    }

    /// Owner node id for a slot, if known.
    pub fn owner_id_of(&self, slot: u16) -> Option<String> {
        let g = self.inner.read();
        g.slot_owner.get(slot as usize).cloned()
    }

    pub fn my_id(&self) -> String {
        self.inner.read().my_id.clone()
    }

    /// Bind / process address (listen). Prefer [`Self::advertised_addr`] for
    /// client-facing topology (Batch EU).
    pub fn bind_addr(&self) -> (String, u16) {
        let g = self.inner.read();
        (g.ip.clone(), g.port)
    }

    /// Client-facing address (announce override when set). Alias of
    /// [`Self::advertised_addr`] for call sites that used `addr` for MEETPEER.
    pub fn addr(&self) -> (String, u16) {
        self.advertised_addr()
    }

    pub fn node_timeout_ms(&self) -> u64 {
        self.inner.read().node_timeout_ms
    }

    /// Override fail-detection timeout (tests use short values).
    pub fn set_node_timeout_ms(&self, ms: u64) {
        self.inner.write().node_timeout_ms = ms.max(20);
    }

    pub fn owns_slot(&self, slot: u16) -> bool {
        let g = self.inner.read();
        g.slot_owner
            .get(slot as usize)
            .map(|id| id == &g.my_id)
            .unwrap_or(false)
    }

    pub fn owner_of(&self, slot: u16) -> Option<ClusterNode> {
        let g = self.inner.read();
        let id = g.slot_owner.get(slot as usize)?;
        if id.is_empty() {
            return None; // unbound (Batch EE FLUSHSLOTS/DELSLOTS)
        }
        g.nodes.get(id).cloned()
    }

    /// Whether the slot has no stable owner (unbound).
    pub fn slot_unbound(&self, slot: u16) -> bool {
        let g = self.inner.read();
        g.slot_owner
            .get(slot as usize)
            .map(|id| id.is_empty())
            .unwrap_or(true)
    }

    /// `CLUSTER ADDSLOTS` — assign slots to this node (Batch EE).
    ///
    /// Errors if any slot is already owned by a different non-empty owner.
    /// Idempotent when we already own the slot. Bumps config epoch once.
    pub fn add_slots(&self, slots: &[u16]) -> Result<(), String> {
        if slots.is_empty() {
            return Err("ERR wrong number of arguments for 'cluster|addslots' command".into());
        }
        let mut g = self.inner.write();
        let my_id = g.my_id.clone();
        for &slot in slots {
            if slot >= SLOT_COUNT {
                return Err(format!("ERR Invalid or out of range slot {}", slot));
            }
            let cur = &g.slot_owner[slot as usize];
            if !cur.is_empty() && cur != &my_id {
                return Err(format!(
                    "ERR Slot {} is already busy (owned by {})",
                    slot, cur
                ));
            }
        }
        g.current_epoch = g.current_epoch.saturating_add(1);
        let epoch = g.current_epoch;
        for &slot in slots {
            g.slot_owner[slot as usize] = my_id.clone();
            g.slot_config_epoch[slot as usize] = epoch;
            g.migrating.remove(&slot);
            g.importing.remove(&slot);
        }
        Ok(())
    }

    /// `CLUSTER DELSLOTS` — unbind slots this node owns (Batch EE).
    pub fn del_slots(&self, slots: &[u16]) -> Result<(), String> {
        if slots.is_empty() {
            return Err("ERR wrong number of arguments for 'cluster|delslots' command".into());
        }
        let mut g = self.inner.write();
        let my_id = g.my_id.clone();
        for &slot in slots {
            if slot >= SLOT_COUNT {
                return Err(format!("ERR Invalid or out of range slot {}", slot));
            }
            let cur = &g.slot_owner[slot as usize];
            if cur.is_empty() {
                return Err(format!("ERR Slot {} is already unassigned", slot));
            }
            if cur != &my_id {
                return Err(format!(
                    "ERR Slot {} is not served by this node (owned by {})",
                    slot, cur
                ));
            }
        }
        g.current_epoch = g.current_epoch.saturating_add(1);
        let epoch = g.current_epoch;
        for &slot in slots {
            g.slot_owner[slot as usize].clear();
            g.slot_config_epoch[slot as usize] = epoch;
            g.migrating.remove(&slot);
            g.importing.remove(&slot);
        }
        Ok(())
    }

    /// Expand inclusive slot ranges into a flat list (Batch EF).
    ///
    /// Each pair is `(start, end)` with `start <= end < SLOT_COUNT`.
    pub fn expand_slot_ranges(ranges: &[(u16, u16)]) -> Result<Vec<u16>, String> {
        let mut out = Vec::new();
        for &(start, end) in ranges {
            if start > end {
                return Err(format!(
                    "ERR start slot number {} is greater than end slot number {}",
                    start, end
                ));
            }
            if end >= SLOT_COUNT {
                return Err(format!("ERR Invalid or out of range slot {}", end));
            }
            for s in start..=end {
                out.push(s);
            }
        }
        // Dedup while preserving order (Redis rejects overlaps on ADDSLOTSRANGE).
        let mut seen = std::collections::HashSet::new();
        let mut deduped = Vec::with_capacity(out.len());
        for s in out {
            if !seen.insert(s) {
                return Err(format!("ERR Slot {} specified multiple times", s));
            }
            deduped.push(s);
        }
        Ok(deduped)
    }

    /// `CLUSTER ADDSLOTSRANGE start end [start end ...]` (Batch EF).
    pub fn add_slot_ranges(&self, ranges: &[(u16, u16)]) -> Result<(), String> {
        let slots = Self::expand_slot_ranges(ranges)?;
        if slots.is_empty() {
            return Err(
                "ERR wrong number of arguments for 'cluster|addslotsrange' command".into(),
            );
        }
        self.add_slots(&slots)
    }

    /// `CLUSTER DELSLOTSRANGE start end [start end ...]` (Batch EF).
    pub fn del_slot_ranges(&self, ranges: &[(u16, u16)]) -> Result<(), String> {
        let slots = Self::expand_slot_ranges(ranges)?;
        if slots.is_empty() {
            return Err(
                "ERR wrong number of arguments for 'cluster|delslotsrange' command".into(),
            );
        }
        self.del_slots(&slots)
    }

    /// `CLUSTER FLUSHSLOTS` — unbind every slot owned by this node (Batch EE).
    pub fn flush_slots(&self) {
        let mut g = self.inner.write();
        let my_id = g.my_id.clone();
        let mut any = false;
        for owner in g.slot_owner.iter() {
            if owner == &my_id {
                any = true;
                break;
            }
        }
        if !any {
            g.migrating.clear();
            g.importing.clear();
            return;
        }
        g.current_epoch = g.current_epoch.saturating_add(1);
        let epoch = g.current_epoch;
        for i in 0..SLOT_COUNT as usize {
            if g.slot_owner[i] == my_id {
                g.slot_owner[i].clear();
                g.slot_config_epoch[i] = epoch;
            }
        }
        g.migrating.clear();
        g.importing.clear();
    }

    pub fn migrating_dest(&self, slot: u16) -> Option<String> {
        self.inner.read().migrating.get(&slot).cloned()
    }

    pub fn importing_source(&self, slot: u16) -> Option<String> {
        self.inner.read().importing.get(&slot).cloned()
    }

    pub fn is_migrating(&self, slot: u16) -> bool {
        self.inner.read().migrating.contains_key(&slot)
    }

    pub fn is_importing(&self, slot: u16) -> bool {
        self.inner.read().importing.contains_key(&slot)
    }

    /// Lookup a known node by id.
    pub fn get_node(&self, id: &str) -> Option<ClusterNode> {
        self.inner.read().nodes.get(id).cloned()
    }

    /// Snapshots of all peers (not myself), for gossip.
    pub fn peer_snapshots(&self) -> Vec<ClusterNode> {
        let g = self.inner.read();
        g.nodes
            .values()
            .filter(|n| !n.myself)
            .cloned()
            .collect()
    }

    /// Whether this process is configured as a cluster replica of `master_id`.
    pub fn is_replica_of(&self, master_id: &str) -> bool {
        let g = self.inner.read();
        g.nodes
            .get(&g.my_id)
            .map(|n| !n.master && n.master_id.as_deref() == Some(master_id))
            .unwrap_or(false)
    }

    /// Whether this process is a cluster replica (not a master).
    pub fn is_cluster_replica(&self) -> bool {
        let g = self.inner.read();
        g.nodes
            .get(&g.my_id)
            .map(|n| !n.master)
            .unwrap_or(false)
    }

    /// Master id we replicate, when this process is a cluster replica.
    pub fn local_master_id(&self) -> Option<String> {
        let g = self.inner.read();
        g.nodes
            .get(&g.my_id)
            .and_then(|n| {
                if n.master {
                    None
                } else {
                    n.master_id.clone()
                }
            })
    }

    /// Batch ER: replica may serve **reads** for slots owned by its master when
    /// the connection issued `READONLY` (not writes — those stay MOVED).
    pub fn can_serve_readonly(&self, slot: u16) -> bool {
        if slot >= SLOT_COUNT {
            return false;
        }
        let g = self.inner.read();
        let me = match g.nodes.get(&g.my_id) {
            Some(n) => n,
            None => return false,
        };
        if me.master {
            return false;
        }
        let Some(mid) = me.master_id.as_ref() else {
            return false;
        };
        g.slot_owner
            .get(slot as usize)
            .map(|o| o == mid)
            .unwrap_or(false)
    }

    pub fn node_is_master(&self, id: &str) -> bool {
        self.inner
            .read()
            .nodes
            .get(id)
            .map(|n| n.master)
            .unwrap_or(false)
    }

    pub fn node_has_slots(&self, id: &str) -> bool {
        let g = self.inner.read();
        g.slot_owner.iter().any(|owner| owner == id)
    }

    pub fn node_is_fail(&self, id: &str) -> bool {
        self.inner
            .read()
            .nodes
            .get(id)
            .map(|n| n.fail)
            .unwrap_or(false)
    }

    /// Add or update a peer node (MEET / SETSLOT targets / tests).
    ///
    /// Preserves known role flags when the peer is re-added without role info.
    pub fn add_node(&self, id: impl Into<String>, ip: impl Into<String>, port: u16) {
        self.add_node_with_role(id, ip, port, None, None);
    }

    /// Add/update peer with optional role (Batch DY MEETPEER / ROLEMAP).
    ///
    /// `role_master`: `Some(true|false)` overwrites; `None` keeps existing or defaults master.
    /// `role_master_id`: only applied when setting slave (`Some(id)` or `Some("")` clears).
    pub fn add_node_with_role(
        &self,
        id: impl Into<String>,
        ip: impl Into<String>,
        port: u16,
        role_master: Option<bool>,
        role_master_id: Option<Option<String>>,
    ) {
        let id = id.into();
        let ip = ip.into();
        let cport = port.saturating_add(10000);
        let mut g = self.inner.write();
        let myself = id == g.my_id;
        if myself {
            return;
        }
        let existing = g.nodes.get(&id);
        let master = role_master
            .unwrap_or_else(|| existing.map(|n| n.master).unwrap_or(true));
        let master_id = match role_master_id {
            Some(v) => v,
            None => existing.and_then(|n| n.master_id.clone()),
        };
        let pfail = existing.map(|n| n.pfail).unwrap_or(false);
        let fail = existing.map(|n| n.fail).unwrap_or(false);
        let repl_offset = existing.map(|n| n.repl_offset).unwrap_or(0);
        let repl_priority = existing.map(|n| n.repl_priority).unwrap_or(100);
        g.nodes.insert(
            id.clone(),
            ClusterNode {
                id: id.clone(),
                ip,
                port,
                cport,
                myself: false,
                master,
                master_id,
                pfail,
                fail,
                repl_offset,
                repl_priority,
            },
        );
        g.last_pong.entry(id).or_insert_with(Instant::now);
    }

    /// Snapshot of known node roles for gossip (`CLUSTER ROLEMAP`).
    pub fn role_map_snapshot(&self) -> Vec<RoleMapEntry> {
        let g = self.inner.read();
        let mut out: Vec<RoleMapEntry> = g
            .nodes
            .values()
            .map(|n| {
                let (repl_offset, repl_priority) = if n.myself {
                    (g.local_repl_offset, g.local_repl_priority)
                } else {
                    (n.repl_offset, n.repl_priority)
                };
                RoleMapEntry {
                    id: n.id.clone(),
                    master: n.master,
                    master_id: n.master_id.clone().unwrap_or_default(),
                    ip: n.ip.clone(),
                    port: n.port,
                    repl_offset,
                    repl_priority,
                }
            })
            .collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }

    /// Merge peer ROLEMAP entries (address + role + offset + priority). Does not clear pfail/fail.
    pub fn merge_role_map(&self, entries: &[RoleMapEntry]) {
        for e in entries {
            if e.id == self.my_id() {
                continue;
            }
            let master_id = if e.master || e.master_id.is_empty() {
                None
            } else {
                Some(e.master_id.clone())
            };
            self.add_node_with_role(
                &e.id,
                &e.ip,
                e.port,
                Some(e.master),
                Some(master_id),
            );
            // Apply election offset + priority after role insert.
            let mut g = self.inner.write();
            if let Some(n) = g.nodes.get_mut(&e.id) {
                n.repl_offset = e.repl_offset;
                n.repl_priority = e.repl_priority;
            }
        }
    }

    /// Known replica node ids of `master_id` (not failed), including self if applicable.
    pub fn replicas_of(&self, master_id: &str) -> Vec<String> {
        let g = self.inner.read();
        let mut ids: Vec<String> = g
            .nodes
            .values()
            .filter(|n| {
                !n.fail
                    && !n.master
                    && n.master_id.as_deref() == Some(master_id)
            })
            .map(|n| n.id.clone())
            .collect();
        ids.sort();
        ids.dedup();
        ids
    }

    /// Failover winner among known replicas of `failed_master_id`.
    ///
    /// Ranking (Batch EB/EA/DY): highest `repl_priority` (0 never promotes),
    /// then highest `repl_offset`, then lexicographically greatest node id.
    /// `None` if we are not a replica of the failed master, or all candidates
    /// have priority 0.
    pub fn failover_election_winner(&self, failed_master_id: &str) -> Option<String> {
        if !self.is_replica_of(failed_master_id) {
            return None;
        }
        let candidates = self.replicas_of(failed_master_id);
        if candidates.is_empty() {
            // Sole self: still respect priority 0 (never promote).
            return if self.local_repl_priority() == 0 {
                None
            } else {
                Some(self.my_id())
            };
        }
        let g = self.inner.read();
        let my_id = g.my_id.clone();
        let local_off = g.local_repl_offset;
        let local_pri = g.local_repl_priority;
        let pri_off = |id: &str| -> (u32, u64) {
            if id == my_id {
                (local_pri, local_off)
            } else {
                let n = g.nodes.get(id);
                (
                    n.map(|x| x.repl_priority).unwrap_or(100),
                    n.map(|x| x.repl_offset).unwrap_or(0),
                )
            }
        };
        // Filter priority 0 (never promote).
        let eligible: Vec<String> = candidates
            .into_iter()
            .filter(|id| pri_off(id).0 > 0)
            .collect();
        if eligible.is_empty() {
            return None;
        }
        eligible.into_iter().max_by(|a, b| {
            let (pa, oa) = pri_off(a);
            let (pb, ob) = pri_off(b);
            pa.cmp(&pb)
                .then_with(|| oa.cmp(&ob))
                .then_with(|| a.cmp(b))
        })
    }

    /// Replication offset used for election for `node_id` (0 if unknown).
    pub fn election_repl_offset(&self, node_id: &str) -> u64 {
        let g = self.inner.read();
        if node_id == g.my_id {
            g.local_repl_offset
        } else {
            g.nodes.get(node_id).map(|n| n.repl_offset).unwrap_or(0)
        }
    }

    /// Another live master that already holds slots (post-failover winner).
    ///
    /// Used so losers do not re-elect themselves after the winner promotes and
    /// leaves the "replica of failed" set (Batch EA/DZ).
    pub fn other_master_with_slots(&self) -> Option<String> {
        let g = self.inner.read();
        let my_id = g.my_id.clone();
        let mut best: Option<(usize, String)> = None;
        for n in g.nodes.values() {
            if n.myself || n.fail || !n.master || n.id == my_id {
                continue;
            }
            let count = g.slot_owner.iter().filter(|o| *o == &n.id).count();
            if count == 0 {
                continue;
            }
            match &best {
                None => best = Some((count, n.id.clone())),
                Some((c, id)) if count > *c || (count == *c && n.id > *id) => {
                    best = Some((count, n.id.clone()));
                }
                _ => {}
            }
        }
        best.map(|(_, id)| id)
    }

    /// Whether this node should claim slots after `failed_master_id` is FAIL.
    pub fn should_claim_on_failover(&self, failed_master_id: &str) -> bool {
        match self.failover_election_winner(failed_master_id) {
            Some(w) => w == self.my_id(),
            None => false,
        }
    }

    /// `CLUSTER MYSHARDID` — stable id for this node's shard (Batch EJ).
    ///
    /// Master with slots → own id; replica → master id; otherwise own id.
    pub fn my_shard_id(&self) -> String {
        let g = self.inner.read();
        if let Some(me) = g.nodes.get(&g.my_id) {
            if !me.master {
                if let Some(ref mid) = me.master_id {
                    if !mid.is_empty() {
                        return mid.clone();
                    }
                }
            }
        }
        // Prefer id of any slot we own (covers masters after FLUSHSLOTS+partial).
        for owner in &g.slot_owner {
            if owner == &g.my_id {
                return g.my_id.clone();
            }
        }
        g.my_id.clone()
    }

    /// Master id we replicate, if any.
    pub fn my_master_id(&self) -> Option<String> {
        let g = self.inner.read();
        g.nodes
            .get(&g.my_id)
            .and_then(|n| {
                if n.master {
                    None
                } else {
                    n.master_id.clone()
                }
            })
    }

    /// Operator-driven failover (Batch EC). Returns slots claimed.
    ///
    /// Caller should promote replication (`promote_to_master`) when persistence
    /// is present **before** or **after** this call; this only updates cluster
    /// topology via [`Self::claim_slots_from`].
    pub fn manual_failover(&self, mode: ManualFailoverMode) -> Result<usize, String> {
        let master_id = self
            .my_master_id()
            .ok_or_else(|| "You should send CLUSTER FAILOVER to a replica".to_string())?;

        match mode {
            ManualFailoverMode::Safe => {
                if !self.node_is_fail(&master_id) && !self.node_is_pfail(&master_id) {
                    return Err(
                        "Master node is not marked as fail/pfail — use FORCE or TAKEOVER".into(),
                    );
                }
                if !self.should_claim_on_failover(&master_id) {
                    return Err(
                        "This replica is not the failover election winner".into(),
                    );
                }
            }
            ManualFailoverMode::Force => {
                if !self.should_claim_on_failover(&master_id) {
                    return Err(
                        "This replica is not the failover election winner — use TAKEOVER".into(),
                    );
                }
                self.mark_fail(&master_id);
            }
            ManualFailoverMode::Takeover => {
                self.mark_fail(&master_id);
            }
        }

        self.claim_slots_from(&master_id)
    }

    /// Batch DZ: non-winner replica re-points topology at the election winner.
    ///
    /// Marks `winner_id` as master (they claim slots), then
    /// [`configure_as_replica_of`]. No-op if already replica of winner.
    pub fn reconfigure_as_replica_of_failover_winner(
        &self,
        winner_id: &str,
    ) -> Result<(), String> {
        if winner_id == self.my_id() {
            return Err("cannot reconfigure as replica of myself".into());
        }
        if self.is_replica_of(winner_id) {
            return Ok(());
        }
        // Ensure winner is known and treated as master for REPLICATE.
        {
            let mut g = self.inner.write();
            if !g.nodes.contains_key(winner_id) {
                return Err(format!("Unknown node {}", winner_id));
            }
            if let Some(w) = g.nodes.get_mut(winner_id) {
                w.master = true;
                w.master_id = None;
                w.fail = false;
                w.pfail = false;
            }
        }
        self.configure_as_replica_of(winner_id)
    }

    /// Our role strings for MEETPEER announce: (`master`|`slave`, master_id or `-`).
    pub fn my_role_wire(&self) -> (String, String) {
        let g = self.inner.read();
        match g.nodes.get(&g.my_id) {
            Some(n) if !n.master => (
                "slave".into(),
                n.master_id.clone().unwrap_or_else(|| "-".into()),
            ),
            _ => ("master".into(), "-".into()),
        }
    }

    /// Record a successful heartbeat / meet exchange.
    pub fn touch_pong(&self, id: &str) {
        let mut g = self.inner.write();
        g.last_pong.insert(id.to_string(), Instant::now());
        if let Some(n) = g.nodes.get_mut(id) {
            // Clear pfail/fail on recovery (best-effort; Redis needs more).
            n.pfail = false;
            n.fail = false;
        }
        // This peer is reachable again: drop it as a *suspect* in every report
        // set. Keep `fail_reports[id]` (what this peer reports about others) —
        // wiping that broke multi-master quorum (Batch DW).
        for set in g.fail_reports.values_mut() {
            set.remove(id);
        }
    }

    /// Time since last pong; if never touched, returns a large duration so first
    /// failed probes can eventually fail the node after timeout from add time.
    pub fn elapsed_since_pong(&self, id: &str) -> Duration {
        let g = self.inner.read();
        match g.last_pong.get(id) {
            Some(t) => t.elapsed(),
            None => Duration::from_millis(g.node_timeout_ms.saturating_mul(2)),
        }
    }

    /// Mark a peer as confirmed failed (forces FAIL; used by tests / force path).
    pub fn mark_fail(&self, id: &str) {
        let mut g = self.inner.write();
        if id == g.my_id {
            return;
        }
        if let Some(n) = g.nodes.get_mut(id) {
            n.pfail = false;
            n.fail = true;
        }
    }

    /// Whether a peer is in possible-fail (timeout observed, not quorum-confirmed).
    pub fn node_is_pfail(&self, id: &str) -> bool {
        self.inner
            .read()
            .nodes
            .get(id)
            .map(|n| n.pfail)
            .unwrap_or(false)
    }

    /// Count known masters (master flag, not myself-only). Includes pfail masters.
    pub fn master_count(&self) -> usize {
        let g = self.inner.read();
        g.nodes
            .values()
            .filter(|n| n.master && !n.fail)
            .count()
            .max(1)
    }

    /// Votes required to escalate pfail → fail.
    ///
    /// ≤2 masters: **1** (single-observer, small-cluster back-compat).
    /// ≥3 masters: `masters/2 + 1` (thin quorum MVP, Batch DW).
    pub fn fail_quorum_size(&self) -> usize {
        let m = self.master_count();
        if m <= 2 {
            1
        } else {
            m / 2 + 1
        }
    }

    /// Node ids we currently report as pfail or fail (`CLUSTER FAILREPORTS`).
    pub fn local_fail_reports(&self) -> Vec<String> {
        let g = self.inner.read();
        let mut ids: Vec<String> = g
            .nodes
            .values()
            .filter(|n| !n.myself && (n.pfail || n.fail))
            .map(|n| n.id.clone())
            .collect();
        ids.sort();
        ids
    }

    /// `CLUSTER COUNT-FAILURE-REPORTS <node-id>` (Batch EH).
    ///
    /// Counts how many peers listed `node_id` in their last FAILREPORTS digest,
    /// plus 1 if **this** node currently marks it pfail/fail.
    pub fn count_failure_reports(&self, node_id: &str) -> i64 {
        let g = self.inner.read();
        let mut n = 0i64;
        if g.nodes
            .get(node_id)
            .map(|x| x.pfail || x.fail)
            .unwrap_or(false)
        {
            n += 1;
        }
        for (reporter, set) in &g.fail_reports {
            if reporter == node_id {
                continue;
            }
            if set.contains(node_id) {
                n += 1;
            }
        }
        n
    }

    /// Ingest a peer's FAILREPORTS list (suspect node ids).
    pub fn ingest_fail_reports(&self, from_peer: &str, suspects: &[String]) {
        if from_peer.is_empty() {
            return;
        }
        let mut g = self.inner.write();
        let set: HashSet<String> = suspects.iter().cloned().collect();
        g.fail_reports.insert(from_peer.to_string(), set);
    }

    /// Note that `id` is unreachable after node-timeout.
    ///
    /// Marks `pfail`, then escalates to `fail` when vote count ≥ quorum.
    /// Returns `true` if the node **newly** became `fail` (caller may failover).
    pub fn note_unreachable(&self, id: &str) -> bool {
        if id.is_empty() {
            return false;
        }
        {
            let mut g = self.inner.write();
            if id == g.my_id {
                return false;
            }
            let Some(n) = g.nodes.get_mut(id) else {
                return false;
            };
            if n.fail {
                return false;
            }
            n.pfail = true;
        }
        self.escalate_fails().contains(&id.to_string())
    }

    /// Recompute FAIL from local pfail + peer reports. Returns newly failed ids.
    pub fn escalate_fails(&self) -> Vec<String> {
        let quorum = self.fail_quorum_size();
        let mut g = self.inner.write();
        let my_id = g.my_id.clone();

        // Candidates: any non-self node currently pfail/fail-reported.
        let mut candidates: HashSet<String> = HashSet::new();
        for n in g.nodes.values() {
            if !n.myself && (n.pfail || n.fail) {
                candidates.insert(n.id.clone());
            }
        }
        for set in g.fail_reports.values() {
            candidates.extend(set.iter().cloned());
        }
        candidates.remove(&my_id);

        let mut newly = Vec::new();
        for cand in candidates {
            if g.nodes.get(&cand).map(|n| n.fail).unwrap_or(false) {
                continue;
            }
            let mut votes = 0usize;
            // Self vote
            if g.nodes.get(&cand).map(|n| n.pfail || n.fail).unwrap_or(false) {
                votes += 1;
            }
            // Peer votes (each reporting peer counts once)
            for (reporter, set) in &g.fail_reports {
                if reporter == &cand {
                    continue;
                }
                // Only count votes from masters that are not themselves failed.
                let reporter_ok = g
                    .nodes
                    .get(reporter)
                    .map(|n| n.master && !n.fail)
                    .unwrap_or(false);
                if reporter_ok && set.contains(&cand) {
                    votes += 1;
                }
            }
            // When self is a replica, still count self pfail vote above; for
            // small-cluster quorum=1 that is enough.
            if votes >= quorum {
                if let Some(n) = g.nodes.get_mut(&cand) {
                    if !n.fail {
                        n.pfail = false;
                        n.fail = true;
                        newly.push(cand);
                    }
                }
            }
        }
        newly
    }

    /// Configure this node as a cluster replica of `master_id`.
    /// Reassigns any slots we own to the master and sets slave flags.
    pub fn configure_as_replica_of(&self, master_id: &str) -> Result<(), String> {
        let mut g = self.inner.write();
        if master_id == g.my_id {
            return Err("cannot replicate myself".into());
        }
        if !g.nodes.contains_key(master_id) {
            return Err(format!("Unknown node {}", master_id));
        }
        // Give our slots to the master (single epoch bump for the handoff).
        let my_id = g.my_id.clone();
        let mut any = false;
        for owner in g.slot_owner.iter() {
            if owner == &my_id {
                any = true;
                break;
            }
        }
        if any {
            g.current_epoch = g.current_epoch.saturating_add(1);
            let epoch = g.current_epoch;
            let idxs: Vec<usize> = g
                .slot_owner
                .iter()
                .enumerate()
                .filter(|(_, owner)| *owner == &my_id)
                .map(|(i, _)| i)
                .collect();
            for i in idxs {
                g.slot_owner[i] = master_id.to_string();
                g.slot_config_epoch[i] = epoch;
            }
        }
        g.migrating.clear();
        g.importing.clear();
        if let Some(me) = g.nodes.get_mut(&my_id) {
            me.master = false;
            me.master_id = Some(master_id.to_string());
            me.fail = false;
        }
        // Ensure master flag on the target.
        if let Some(m) = g.nodes.get_mut(master_id) {
            m.master = true;
            m.master_id = None;
        }
        Ok(())
    }

    /// After master fail: claim all slots owned by `failed_id`, become master.
    /// Returns number of slots claimed.
    pub fn claim_slots_from(&self, failed_id: &str) -> Result<usize, String> {
        let mut g = self.inner.write();
        if !g.nodes.contains_key(failed_id) {
            return Err(format!("Unknown node {}", failed_id));
        }
        let my_id = g.my_id.clone();
        let mut claimed = 0usize;
        for owner in g.slot_owner.iter() {
            if owner == failed_id {
                claimed += 1;
            }
        }
        if claimed > 0 {
            g.current_epoch = g.current_epoch.saturating_add(1);
            let epoch = g.current_epoch;
            let idxs: Vec<usize> = g
                .slot_owner
                .iter()
                .enumerate()
                .filter(|(_, owner)| *owner == failed_id)
                .map(|(i, _)| i)
                .collect();
            for i in idxs {
                g.slot_owner[i] = my_id.clone();
                g.slot_config_epoch[i] = epoch;
            }
        }
        g.migrating.clear();
        g.importing.clear();
        if let Some(me) = g.nodes.get_mut(&my_id) {
            me.master = true;
            me.master_id = None;
            me.fail = false;
        }
        Ok(claimed)
    }

    /// Reassign a single slot to `node_id` (test helper / SETSLOT NODE).
    ///
    /// Bumps `current_epoch` and stamps the slot so gossip peers accept the
    /// new owner (Batch DU).
    pub fn reassign_slot(&self, slot: u16, node_id: &str) -> Result<(), String> {
        if slot >= SLOT_COUNT {
            return Err(format!("slot out of range: {}", slot));
        }
        let mut g = self.inner.write();
        if !g.nodes.contains_key(node_id) {
            return Err(format!("Unknown node {}", node_id));
        }
        g.current_epoch = g.current_epoch.saturating_add(1);
        g.slot_owner[slot as usize] = node_id.to_string();
        g.slot_config_epoch[slot as usize] = g.current_epoch;
        g.migrating.remove(&slot);
        g.importing.remove(&slot);
        Ok(())
    }

    /// Reassign an inclusive slot range (test helper).
    pub fn reassign_slot_range(&self, start: u16, end: u16, node_id: &str) -> Result<(), String> {
        if start > end || end >= SLOT_COUNT {
            return Err("invalid slot range".into());
        }
        for s in start..=end {
            self.reassign_slot(s, node_id)?;
        }
        Ok(())
    }

    /// CLUSTER SETSLOT <slot> MIGRATING <node-id>
    pub fn set_migrating(&self, slot: u16, dest_node_id: &str) -> Result<(), String> {
        if slot >= SLOT_COUNT {
            return Err("Slot out of range".into());
        }
        let mut g = self.inner.write();
        if !g.nodes.contains_key(dest_node_id) {
            return Err(format!("I don't know about node {}", dest_node_id));
        }
        if g.slot_owner.get(slot as usize).map(|s| s.as_str()) != Some(g.my_id.as_str()) {
            return Err("I'm not the owner of hash slot".into());
        }
        g.importing.remove(&slot);
        g.migrating.insert(slot, dest_node_id.to_string());
        Ok(())
    }

    /// CLUSTER SETSLOT <slot> IMPORTING <node-id>
    pub fn set_importing(&self, slot: u16, source_node_id: &str) -> Result<(), String> {
        if slot >= SLOT_COUNT {
            return Err("Slot out of range".into());
        }
        let mut g = self.inner.write();
        if !g.nodes.contains_key(source_node_id) {
            return Err(format!("I don't know about node {}", source_node_id));
        }
        // Importing means we do not stably own it yet.
        g.migrating.remove(&slot);
        g.importing.insert(slot, source_node_id.to_string());
        Ok(())
    }

    /// CLUSTER SETSLOT <slot> STABLE
    pub fn set_stable(&self, slot: u16) -> Result<(), String> {
        if slot >= SLOT_COUNT {
            return Err("Slot out of range".into());
        }
        let mut g = self.inner.write();
        g.migrating.remove(&slot);
        g.importing.remove(&slot);
        Ok(())
    }

    /// CLUSTER SETSLOT <slot> NODE <node-id>
    ///
    /// Always allowed for operators (bumps epoch). Stale **gossip** ownership
    /// with a lower epoch is rejected in [`Self::apply_ownership_range`] (Batch
    /// DV fence) — not here, so `RESHARD FINISH` / manual recovery still work.
    pub fn set_node(&self, slot: u16, node_id: &str) -> Result<(), String> {
        // Batch EP test hook: force local NODE failures (per ClusterState instance).
        if self.take_source_node_inject_fail() {
            return Err("injected source NODE failure".into());
        }
        self.reassign_slot(slot, node_id)
    }

    /// Force the next `n` local `SETSLOT NODE` attempts to fail (Batch EP tests).
    pub fn test_inject_source_node_failures(&self, n: u32) {
        self.source_node_fail_inject.store(n, Ordering::SeqCst);
    }

    /// Clear source NODE failure injection (Batch EP tests).
    pub fn test_clear_source_node_inject(&self) {
        self.source_node_fail_inject.store(0, Ordering::SeqCst);
    }

    fn take_source_node_inject_fail(&self) -> bool {
        self.source_node_fail_inject
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                if n > 0 {
                    Some(n - 1)
                } else {
                    None
                }
            })
            .is_ok()
    }

    /// Compressed ownership map for gossip / `CLUSTER OWNERS` (Batch DU).
    ///
    /// Ranges share owner id **and** config epoch. Unknown owners still emit
    /// the range with empty ip/port (peer merge will skip add_node).
    pub fn ownership_snapshot(&self) -> Vec<OwnershipRange> {
        let g = self.inner.read();
        if g.slot_owner.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::new();
        let mut start = 0u16;
        let mut cur_owner = g.slot_owner[0].clone();
        let mut cur_epoch = g.slot_config_epoch[0];
        for slot in 1..SLOT_COUNT {
            let owner = &g.slot_owner[slot as usize];
            let epoch = g.slot_config_epoch[slot as usize];
            if owner != &cur_owner || epoch != cur_epoch {
                let (ip, port) = node_addr(&g, &cur_owner);
                out.push(OwnershipRange {
                    start,
                    end: slot - 1,
                    owner_id: cur_owner.clone(),
                    ip,
                    port,
                    epoch: cur_epoch,
                });
                start = slot;
                cur_owner = owner.clone();
                cur_epoch = epoch;
            }
        }
        let (ip, port) = node_addr(&g, &cur_owner);
        out.push(OwnershipRange {
            start,
            end: SLOT_COUNT - 1,
            owner_id: cur_owner,
            ip,
            port,
            epoch: cur_epoch,
        });
        out
    }

    /// Apply a peer ownership range (higher epoch wins). See [`OwnershipApplyResult`].
    ///
    /// Equal epoch + different owner: keep local (no flip-flop). Local MIGRATING
    /// or IMPORTING slots are never overwritten by gossip.
    pub fn apply_ownership_range(&self, range: &OwnershipRange) -> OwnershipApplyResult {
        if range.start > range.end || range.end >= SLOT_COUNT {
            return OwnershipApplyResult::Invalid;
        }
        if range.owner_id.is_empty() {
            return OwnershipApplyResult::Invalid;
        }

        let mut g = self.inner.write();

        // Ensure we know how to reach the owner (MEET may not have run for C→B).
        if !g.nodes.contains_key(&range.owner_id) {
            if range.ip.is_empty() || range.port == 0 {
                // Cannot install owner without address; reject this range.
                return OwnershipApplyResult::RejectedStale;
            }
            let cport = range.port.saturating_add(10000);
            g.nodes.insert(
                range.owner_id.clone(),
                ClusterNode {
                    id: range.owner_id.clone(),
                    ip: range.ip.clone(),
                    port: range.port,
                    cport,
                    myself: false,
                    master: true,
                    master_id: None,
                    pfail: false,
                    fail: false,
                    repl_offset: 0,
                    repl_priority: 100,
                },
            );
            g.last_pong
                .entry(range.owner_id.clone())
                .or_insert_with(Instant::now);
        } else if !range.ip.is_empty() && range.port != 0 {
            // Refresh address if peer supplied one (best-effort).
            if let Some(n) = g.nodes.get_mut(&range.owner_id) {
                if !n.myself {
                    n.ip = range.ip.clone();
                    n.port = range.port;
                    n.cport = range.port.saturating_add(10000);
                }
            }
        }

        let mut applied = 0u32;
        let mut skipped_transition = 0u32;
        let mut rejected = 0u32;

        for slot in range.start..=range.end {
            let idx = slot as usize;
            if g.migrating.contains_key(&slot) || g.importing.contains_key(&slot) {
                skipped_transition += 1;
                continue;
            }
            let local_epoch = g.slot_config_epoch[idx];
            if range.epoch > local_epoch {
                g.slot_owner[idx] = range.owner_id.clone();
                g.slot_config_epoch[idx] = range.epoch;
                applied += 1;
            } else {
                // Stale or equal-epoch conflict — keep local.
                rejected += 1;
            }
        }

        if applied > 0 && range.epoch > g.current_epoch {
            g.current_epoch = range.epoch;
        }

        if applied > 0 {
            OwnershipApplyResult::Applied
        } else if skipped_transition > 0 && rejected == 0 {
            OwnershipApplyResult::SkippedTransition
        } else if skipped_transition > 0 && applied == 0 {
            // Mixed skip/reject still surfaces transition preference when any skipped.
            OwnershipApplyResult::SkippedTransition
        } else {
            OwnershipApplyResult::RejectedStale
        }
    }

    /// Merge a full peer ownership snapshot. Returns counts of applied ranges.
    pub fn merge_ownership_snapshot(&self, ranges: &[OwnershipRange]) -> (u32, u32, u32) {
        let mut applied = 0u32;
        let mut rejected = 0u32;
        let mut skipped = 0u32;
        for r in ranges {
            match self.apply_ownership_range(r) {
                OwnershipApplyResult::Applied => applied += 1,
                OwnershipApplyResult::RejectedStale | OwnershipApplyResult::Invalid => {
                    rejected += 1
                }
                OwnershipApplyResult::SkippedTransition => skipped += 1,
            }
        }
        (applied, rejected, skipped)
    }

    /// Build MOVED redirect for a slot.
    pub fn moved_target(&self, slot: u16) -> Option<RedirectTarget> {
        let node = self.owner_of(slot)?;
        Some(RedirectTarget {
            slot,
            ip: node.ip,
            port: node.port,
            node_id: node.id,
        })
    }

    /// Build ASK redirect toward the migration destination (or owner if unknown).
    pub fn ask_target(&self, slot: u16) -> Option<RedirectTarget> {
        let g = self.inner.read();
        let dest_id = g.migrating.get(&slot)?;
        let node = g.nodes.get(dest_id)?;
        Some(RedirectTarget {
            slot,
            ip: node.ip.clone(),
            port: node.port,
            node_id: node.id.clone(),
        })
    }

    /// Format CLUSTER NODES reply (Redis text bulk).
    pub fn format_nodes(&self) -> String {
        let g = self.inner.read();
        let mut lines = Vec::new();
        // Emit myself first, then others
        let mut ids: Vec<_> = g.nodes.keys().cloned().collect();
        ids.sort_by(|a, b| {
            let am = a == &g.my_id;
            let bm = b == &g.my_id;
            bm.cmp(&am).then(a.cmp(b))
        });

        for id in ids {
            let node = match g.nodes.get(&id) {
                Some(n) => n,
                None => continue,
            };
            let mut flags = Vec::new();
            if node.myself {
                flags.push("myself");
            }
            if node.fail {
                flags.push("fail");
            } else if node.pfail {
                flags.push("fail?"); // Redis-style possible fail (Batch DW)
            }
            if node.master {
                flags.push("master");
            } else {
                flags.push("slave");
            }
            let flags = flags.join(",");
            let master = node
                .master_id
                .as_deref()
                .unwrap_or("-");
            let ping = 0;
            let pong = 0;
            let epoch = g.current_epoch;
            let link = if node.fail { "disconnected" } else { "connected" };

            // Slot ranges owned by this node (excluding pure importing-only)
            let ranges = slot_ranges_for(&g.slot_owner, &id);
            let mut range_str = String::new();
            for (start, end) in ranges {
                if !range_str.is_empty() {
                    range_str.push(' ');
                }
                if start == end {
                    range_str.push_str(&start.to_string());
                } else {
                    range_str.push_str(&format!("{}-{}", start, end));
                }
            }

            // Annotate migrating/importing on myself
            if node.myself {
                for (&slot, dest) in &g.migrating {
                    if !range_str.is_empty() {
                        range_str.push(' ');
                    }
                    range_str.push_str(&format!("[{}->-{}]", slot, dest));
                }
                for (&slot, src) in &g.importing {
                    if !range_str.is_empty() {
                        range_str.push(' ');
                    }
                    range_str.push_str(&format!("[{}-<-{}]", slot, src));
                }
            }

            let line = format!(
                "{} {}:{}@{} {} {} {} {} {} {}{}",
                node.id,
                node.ip,
                node.port,
                node.cport,
                flags,
                master,
                ping,
                pong,
                epoch,
                link,
                if range_str.is_empty() {
                    String::new()
                } else {
                    format!(" {}", range_str)
                }
            );
            lines.push(line);
        }
        let mut out = lines.join("\n");
        out.push('\n');
        out
    }

    /// CLUSTER SLOTS nested array data (start, end, **master** owner node).
    ///
    /// Consecutive slots with the same owner collapse into one range. Unbound
    /// (empty owner) runs are omitted. Replica endpoints are attached by the
    /// command layer ([`crate::commands`] / Batch ET).
    pub fn slots_ranges(&self) -> Vec<(u16, u16, ClusterNode)> {
        let g = self.inner.read();
        let mut out = Vec::new();
        if g.slot_owner.is_empty() {
            return out;
        }
        let mut start = 0u16;
        let mut cur = g.slot_owner[0].clone();
        for slot in 1..SLOT_COUNT {
            let owner = &g.slot_owner[slot as usize];
            if owner != &cur {
                if !cur.is_empty() {
                    if let Some(node) = g.nodes.get(&cur) {
                        out.push((start, slot - 1, node.clone()));
                    }
                }
                start = slot;
                cur = owner.clone();
            }
        }
        if !cur.is_empty() {
            if let Some(node) = g.nodes.get(&cur) {
                out.push((start, SLOT_COUNT - 1, node.clone()));
            }
        }
        out
    }

    /// CLUSTER INFO text.
    pub fn format_info(&self) -> String {
        let state_ok = self.cluster_state_ok();
        let g = self.inner.read();
        let assigned = g
            .slot_owner
            .iter()
            .filter(|id| !id.is_empty() && g.nodes.contains_key(id.as_str()))
            .count();
        let known = g.nodes.len();
        let fail_slots = g
            .slot_owner
            .iter()
            .filter(|id| g.nodes.get(id.as_str()).map(|n| n.fail).unwrap_or(false))
            .count();
        // cluster_size = number of masters with slots
        let mut masters_with_slots = std::collections::HashSet::new();
        for id in &g.slot_owner {
            if id.is_empty() {
                continue;
            }
            if let Some(n) = g.nodes.get(id) {
                if n.master && !n.fail {
                    masters_with_slots.insert(id.clone());
                }
            }
        }
        let state = if state_ok { "ok" } else { "fail" };
        format!(
            "cluster_state:{}\r\n\
             cluster_slots_assigned:{}\r\n\
             cluster_slots_ok:{}\r\n\
             cluster_slots_pfail:0\r\n\
             cluster_slots_fail:{}\r\n\
             cluster_known_nodes:{}\r\n\
             cluster_size:{}\r\n\
             cluster_current_epoch:{}\r\n\
             cluster_my_epoch:{}\r\n\
             cluster_stats_messages_ping_sent:0\r\n\
             cluster_stats_messages_pong_sent:0\r\n\
             cluster_stats_messages_sent:0\r\n\
             cluster_stats_messages_ping_received:0\r\n\
             cluster_stats_messages_pong_received:0\r\n\
             cluster_stats_messages_received:0\r\n",
            state,
            assigned,
            assigned.saturating_sub(fail_slots),
            fail_slots,
            known,
            masters_with_slots.len().max(if assigned > 0 { 1 } else { 0 }),
            g.current_epoch,
            g.current_epoch,
        )
    }
}

fn node_addr(g: &Inner, owner_id: &str) -> (String, u16) {
    match g.nodes.get(owner_id) {
        Some(n) => (n.ip.clone(), n.port),
        None => (String::new(), 0),
    }
}

/// One parsed line of `CLUSTER NODES` / `nodes.conf` (Batch EN).
struct ParsedNodesLine {
    id: String,
    ip: String,
    port: u16,
    myself: bool,
    master: bool,
    fail: bool,
    pfail: bool,
    master_id: Option<String>,
    epoch: u64,
    slots: Vec<(u16, u16)>,
}

fn parse_nodes_conf_header_epoch(text: &str) -> Option<u64> {
    for line in text.lines() {
        let t = line.trim();
        if let Some(rest) = t.strip_prefix("# epoch ") {
            if let Ok(n) = rest.trim().parse::<u64>() {
                return Some(n);
            }
        }
    }
    None
}

fn parse_nodes_conf_lines(text: &str) -> std::result::Result<Vec<ParsedNodesLine>, String> {
    let mut out = Vec::new();
    for (lineno, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        // id ip:port@cport flags master ping pong epoch link [slots...]
        if parts.len() < 8 {
            return Err(format!("line {}: expected at least 8 fields", lineno + 1));
        }
        let id = parts[0].to_string();
        let addr = parts[1];
        let (ip_port, _cport) = match addr.split_once('@') {
            Some((a, c)) => (a, c),
            None => (addr, "0"),
        };
        let (ip, port_s) = ip_port
            .rsplit_once(':')
            .ok_or_else(|| format!("line {}: bad address {}", lineno + 1, addr))?;
        let port: u16 = port_s
            .parse()
            .map_err(|_| format!("line {}: bad port", lineno + 1))?;
        let flags = parts[2];
        let myself = flags.split(',').any(|f| f == "myself");
        let master = flags.split(',').any(|f| f == "master")
            || !flags.split(',').any(|f| f == "slave");
        let fail = flags.split(',').any(|f| f == "fail");
        let pfail = flags.split(',').any(|f| f == "fail?");
        let master_id = match parts[3] {
            "-" => None,
            s => Some(s.to_string()),
        };
        let epoch: u64 = parts
            .get(6)
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);
        // parts[7] = link; slots from 8..
        let mut slots = Vec::new();
        for tok in parts.iter().skip(8) {
            if tok.starts_with('[') {
                continue; // migrating/importing annotations
            }
            if let Some((a, b)) = tok.split_once('-') {
                let start: u16 = a
                    .parse()
                    .map_err(|_| format!("line {}: bad slot range {}", lineno + 1, tok))?;
                let end: u16 = b
                    .parse()
                    .map_err(|_| format!("line {}: bad slot range {}", lineno + 1, tok))?;
                if start <= end && end < SLOT_COUNT {
                    slots.push((start, end));
                }
            } else {
                let s: u16 = tok
                    .parse()
                    .map_err(|_| format!("line {}: bad slot {}", lineno + 1, tok))?;
                if s < SLOT_COUNT {
                    slots.push((s, s));
                }
            }
        }
        out.push(ParsedNodesLine {
            id,
            ip: ip.to_string(),
            port,
            myself,
            master,
            fail,
            pfail,
            master_id,
            epoch,
            slots,
        });
    }
    Ok(out)
}

fn slot_ranges_for(slot_owner: &[String], node_id: &str) -> Vec<(u16, u16)> {
    let mut ranges = Vec::new();
    let mut i = 0u16;
    while i < SLOT_COUNT {
        if slot_owner.get(i as usize).map(|s| s.as_str()) != Some(node_id) {
            i += 1;
            continue;
        }
        let start = i;
        i += 1;
        while i < SLOT_COUNT && slot_owner.get(i as usize).map(|s| s.as_str()) == Some(node_id) {
            i += 1;
        }
        ranges.push((start, i - 1));
    }
    ranges
}

/// Generate a Redis-style 40-char hex node id.
fn generate_node_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    // Mix time + address bits for uniqueness without extra deps
    let mixed = nanos
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(0xDEAD_BEEF_CAFE_BABE);
    // Expand to 40 hex chars
    format!("{:032x}{:08x}", mixed, (mixed >> 64) as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_node_owns_all_slots() {
        let cs = ClusterState::single_node("127.0.0.1", 6379);
        assert!(cs.owns_slot(0));
        assert!(cs.owns_slot(12182));
        assert!(cs.owns_slot(16383));
        assert_eq!(cs.slots_ranges().len(), 1);
        let (s, e, n) = &cs.slots_ranges()[0];
        assert_eq!((*s, *e), (0, 16383));
        assert_eq!(n.id, cs.my_id());
        assert_eq!(cs.my_shard_id(), cs.my_id());
    }

    #[test]
    fn can_serve_readonly_only_for_master_slots() {
        let master = ClusterState::single_node("127.0.0.1", 7000);
        let mid = master.my_id();
        let r = ClusterState::single_node("127.0.0.1", 7001);
        r.add_node(&mid, "127.0.0.1", 7000);
        r.configure_as_replica_of(&mid).unwrap();
        assert!(r.is_cluster_replica());
        assert_eq!(r.local_master_id().as_deref(), Some(mid.as_str()));
        // All slots given to master → readonly serve allowed for any slot.
        assert!(r.can_serve_readonly(0));
        assert!(r.can_serve_readonly(16383));
        assert!(!r.owns_slot(0));
        // Master never "readonly-serves" (it owns or MOVED).
        assert!(!master.can_serve_readonly(0));
        // Unbound slot on replica (after flush on a copy): no.
        let r2 = ClusterState::single_node("127.0.0.1", 7002);
        r2.add_node(&mid, "127.0.0.1", 7000);
        r2.configure_as_replica_of(&mid).unwrap();
        r2.flush_slots(); // unbinds all — but wait, flush clears our ownership only
        // After configure_as_replica_of, we own nothing; flush_slots unbinds slots we own (none).
        // Reassign one slot to a third party.
        let other = "oo".repeat(20);
        r2.add_node(&other, "10.0.0.3", 7003);
        r2.reassign_slot(5, &other).unwrap();
        assert!(!r2.can_serve_readonly(5)); // not our master's slot
        assert!(r2.can_serve_readonly(6)); // still master's
    }

    #[test]
    fn require_full_coverage_drives_cluster_state() {
        let cs = ClusterState::single_node("127.0.0.1", 6379);
        assert!(cs.require_full_coverage());
        assert!(cs.has_full_coverage());
        assert!(cs.cluster_state_ok());
        assert!(cs.format_info().contains("cluster_state:ok"));

        cs.flush_slots();
        assert!(!cs.has_full_coverage());
        assert!(!cs.cluster_state_ok());
        assert!(cs.format_info().contains("cluster_state:fail"));

        // Disable require → state ok even without coverage (per-slot CLUSTERDOWN still applies).
        cs.set_require_full_coverage(false);
        assert!(!cs.has_full_coverage());
        assert!(cs.cluster_state_ok());
        assert!(cs.format_info().contains("cluster_state:ok"));

        cs.set_require_full_coverage(true);
        cs.add_slots(&[0]).unwrap();
        // Still incomplete coverage.
        assert!(!cs.has_full_coverage());
        assert!(!cs.cluster_state_ok());
    }

    #[test]
    fn allow_reads_when_down_flag() {
        let cs = ClusterState::single_node("127.0.0.1", 6379);
        assert!(!cs.allow_reads_when_down());
        cs.set_allow_reads_when_down(true);
        assert!(cs.allow_reads_when_down());
        // Flag is independent of coverage / cluster_state.
        cs.flush_slots();
        assert!(!cs.cluster_state_ok());
        assert!(cs.allow_reads_when_down());
    }

    #[test]
    fn slots_ranges_omits_unbound() {
        let cs = ClusterState::single_node("127.0.0.1", 6379);
        cs.del_slots(&[0, 1, 2]).unwrap();
        let ranges = cs.slots_ranges();
        // First range should start at 3, not include unbound 0-2.
        assert!(!ranges.is_empty());
        assert_eq!(ranges[0].0, 3);
        assert_eq!(ranges[0].2.id, cs.my_id());
    }

    #[test]
    fn announce_ip_port_update_myself_and_addr() {
        let cs = ClusterState::single_node("127.0.0.1", 7000);
        assert_eq!(cs.addr(), ("127.0.0.1".into(), 7000));
        assert_eq!(cs.bind_addr(), ("127.0.0.1".into(), 7000));

        cs.set_announce_ip(Some("10.9.8.7".into()));
        cs.set_announce_port(Some(17000));
        assert_eq!(cs.addr(), ("10.9.8.7".into(), 17000));
        assert_eq!(cs.bind_addr(), ("127.0.0.1".into(), 7000));
        assert_eq!(cs.announce_ip().as_deref(), Some("10.9.8.7"));
        assert_eq!(cs.announce_port(), Some(17000));

        let me = cs.get_node(&cs.my_id()).unwrap();
        assert_eq!(me.ip, "10.9.8.7");
        assert_eq!(me.port, 17000);

        let nodes = cs.format_nodes();
        assert!(
            nodes.contains("10.9.8.7:17000"),
            "NODES should advertise announce addr: {}",
            nodes
        );

        // Clear overrides.
        cs.set_announce_ip(None);
        cs.set_announce_port(None);
        assert_eq!(cs.addr(), ("127.0.0.1".into(), 7000));
        let me = cs.get_node(&cs.my_id()).unwrap();
        assert_eq!(me.ip, "127.0.0.1");
        assert_eq!(me.port, 7000);
    }

    #[test]
    fn nodes_conf_round_trip() {
        let a = ClusterState::single_node("127.0.0.1", 7000);
        let peer = "pp".repeat(20);
        a.add_node(&peer, "10.0.0.2", 7001);
        a.reassign_slot_range(0, 99, &peer).unwrap();
        a.set_config_epoch(a.current_epoch() + 5).unwrap();
        let text = format!(
            "# Kore cluster nodes.conf generated by CLUSTER SAVECONFIG\n# epoch {}\n{}",
            a.current_epoch(),
            a.format_nodes()
        );
        let b = ClusterState::from_nodes_conf("127.0.0.1", 7000, &text).unwrap();
        assert_eq!(b.my_id(), a.my_id());
        assert_eq!(b.current_epoch(), a.current_epoch());
        assert!(b.get_node(&peer).is_some());
        assert!(!b.owns_slot(0)); // peer owns 0-99
        assert_eq!(b.owner_id_of(50).as_deref(), Some(peer.as_str()));
        assert!(b.owns_slot(100));
    }

    #[test]
    fn save_nodes_conf_to_and_autosave() {
        let dir = std::env::temp_dir().join(format!(
            "kore-autosave-ut-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let cs = ClusterState::single_node("127.0.0.1", 7000);
        let path = cs
            .save_nodes_conf_to(dir.to_str().unwrap())
            .expect("save");
        assert!(path.exists());
        let body = std::fs::read_to_string(&path).unwrap();
        assert!(body.contains(&cs.my_id()));
        assert!(body.contains("Kore cluster nodes.conf"));

        // Autosave no-op until dir configured.
        let _ = std::fs::remove_file(&path);
        cs.autosave_nodes_conf();
        assert!(!path.exists());
        cs.set_nodes_conf_dir(dir.to_str().unwrap());
        cs.autosave_nodes_conf();
        assert!(path.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn set_config_epoch_only_increases() {
        let cs = ClusterState::single_node("127.0.0.1", 6379);
        let cur = cs.current_epoch();
        assert!(cs.set_config_epoch(cur).is_err());
        assert!(cs.set_config_epoch(cur.saturating_sub(1)).is_err());
        cs.set_config_epoch(cur + 10).unwrap();
        assert_eq!(cs.current_epoch(), cur + 10);
        assert!(cs.set_config_epoch(cur + 5).is_err());
    }

    #[test]
    fn myshardid_follows_replica_master() {
        let master_id = "m0".repeat(20);
        let r = ClusterState::single_node("127.0.0.1", 7500);
        r.add_node(&master_id, "127.0.0.1", 7000);
        r.configure_as_replica_of(&master_id).unwrap();
        assert_eq!(r.my_shard_id(), master_id);
    }

    #[test]
    fn forget_and_reset_cluster() {
        let cs = ClusterState::single_node("127.0.0.1", 6379);
        let peer = "fg".repeat(20);
        cs.add_node(&peer, "10.0.0.2", 7001);
        assert!(cs.get_node(&peer).is_some());
        // Peer owns no slots → forget ok.
        cs.forget_node(&peer).unwrap();
        assert!(cs.get_node(&peer).is_none());

        let peer2 = "fh".repeat(20);
        cs.add_node(&peer2, "10.0.0.3", 7002);
        cs.reassign_slot(5, &peer2).unwrap();
        assert!(cs.forget_node(&peer2).is_err());
        assert!(cs.forget_node(&cs.my_id()).is_err());

        cs.reset_cluster_config();
        assert!(cs.slot_unbound(0));
        assert!(cs.slot_unbound(5)); // ownership cleared even if was peer's
        assert!(cs.get_node(&peer2).is_none());
        let me = cs.get_node(&cs.my_id()).unwrap();
        assert!(me.master);
        assert!(me.master_id.is_none());
    }

    #[test]
    fn add_del_slot_ranges() {
        let cs = ClusterState::single_node("127.0.0.1", 6379);
        cs.flush_slots();
        cs.add_slot_ranges(&[(0, 2), (10, 12)]).unwrap();
        assert!(cs.owns_slot(0) && cs.owns_slot(2) && cs.owns_slot(11));
        assert!(cs.slot_unbound(3));
        // Overlap in one call is rejected.
        assert!(cs.add_slot_ranges(&[(0, 1), (1, 2)]).is_err());
        cs.del_slot_ranges(&[(0, 2)]).unwrap();
        assert!(cs.slot_unbound(0) && cs.slot_unbound(2));
        assert!(cs.owns_slot(10));
    }

    #[test]
    fn add_del_flush_slots() {
        let cs = ClusterState::single_node("127.0.0.1", 6379);
        let epoch0 = cs.current_epoch();
        cs.del_slots(&[0, 1, 2]).unwrap();
        assert!(cs.slot_unbound(0));
        assert!(!cs.owns_slot(0));
        assert!(cs.owner_of(0).is_none());
        assert!(cs.current_epoch() > epoch0);

        cs.add_slots(&[0, 1]).unwrap();
        assert!(cs.owns_slot(0));
        assert!(cs.owns_slot(1));
        assert!(cs.slot_unbound(2));

        // Idempotent re-add of our slots.
        cs.add_slots(&[0]).unwrap();

        let other = "xx".repeat(20);
        cs.add_node(&other, "10.0.0.2", 7001);
        cs.reassign_slot(10, &other).unwrap();
        assert!(cs.add_slots(&[10]).is_err());

        cs.flush_slots();
        assert!(cs.slot_unbound(0));
        assert!(cs.slot_unbound(1));
        // Slots owned by others are left alone.
        assert_eq!(cs.owner_id_of(10).as_deref(), Some(other.as_str()));
    }

    #[test]
    fn reassign_produces_moved_target() {
        let cs = ClusterState::single_node("127.0.0.1", 6379);
        cs.add_node("abcd".repeat(10), "10.0.0.2", 7001);
        let other = "abcd".repeat(10);
        cs.reassign_slot(100, &other).unwrap();
        assert!(!cs.owns_slot(100));
        let t = cs.moved_target(100).unwrap();
        assert_eq!(t.port, 7001);
        assert_eq!(t.ip, "10.0.0.2");
    }

    #[test]
    fn mark_fail_shows_in_nodes() {
        let cs = ClusterState::single_node("127.0.0.1", 6379);
        let other = "ab".repeat(20);
        cs.add_node(&other, "10.0.0.2", 7001);
        cs.mark_fail(&other);
        assert!(cs.node_is_fail(&other));
        let nodes = cs.format_nodes();
        assert!(nodes.contains("fail"), "nodes={}", nodes);
    }

    #[test]
    fn failover_election_picks_max_replica_id() {
        let master = ClusterState::single_node("127.0.0.1", 7000);
        let master_id = master.my_id();
        let r = ClusterState::single_node("127.0.0.1", 7001);
        let low = "aa".repeat(20);
        let high = "zz".repeat(20);
        r.add_node(&master_id, "127.0.0.1", 7000);
        r.configure_as_replica_of(&master_id).unwrap();
        r.add_node_with_role(
            &low,
            "10.0.0.2",
            7002,
            Some(false),
            Some(Some(master_id.clone())),
        );
        r.add_node_with_role(
            &high,
            "10.0.0.3",
            7003,
            Some(false),
            Some(Some(master_id.clone())),
        );
        // Equal offsets → max id wins (Batch DY tie-break).
        r.set_local_repl_offset(0);
        let winner = r.failover_election_winner(&master_id).unwrap();
        let mut cands = r.replicas_of(&master_id);
        cands.sort();
        cands.dedup();
        assert_eq!(winner, cands.into_iter().max().unwrap());
        assert_eq!(r.should_claim_on_failover(&master_id), winner == r.my_id());
    }

    #[test]
    fn failover_election_prefers_higher_priority() {
        let master_id = "m0".repeat(20);
        let r = ClusterState::single_node("127.0.0.1", 7300);
        let peer = "zz".repeat(20); // high id but lower priority
        r.add_node(&master_id, "127.0.0.1", 7000);
        r.configure_as_replica_of(&master_id).unwrap();
        r.set_local_repl_priority(50);
        r.set_local_repl_offset(9_999_999); // high offset must not beat priority
        r.merge_role_map(&[RoleMapEntry {
            id: peer.clone(),
            master: false,
            master_id: master_id.clone(),
            ip: "10.0.0.2".into(),
            port: 7002,
            repl_offset: 1,
            repl_priority: 200,
        }]);
        assert_eq!(
            r.failover_election_winner(&master_id).as_deref(),
            Some(peer.as_str())
        );
        assert!(!r.should_claim_on_failover(&master_id));
    }

    #[test]
    fn failover_election_priority_zero_never_promotes() {
        let master_id = "m0".repeat(20);
        let r = ClusterState::single_node("127.0.0.1", 7301);
        r.add_node(&master_id, "127.0.0.1", 7000);
        r.configure_as_replica_of(&master_id).unwrap();
        r.set_local_repl_priority(0);
        assert!(r.failover_election_winner(&master_id).is_none());
        assert!(!r.should_claim_on_failover(&master_id));

        // Peer with priority > 0 wins; self at 0 never.
        let peer = "bb".repeat(20);
        r.merge_role_map(&[RoleMapEntry {
            id: peer.clone(),
            master: false,
            master_id: master_id.clone(),
            ip: "10.0.0.3".into(),
            port: 7003,
            repl_offset: 0,
            repl_priority: 1,
        }]);
        assert_eq!(
            r.failover_election_winner(&master_id).as_deref(),
            Some(peer.as_str())
        );
    }

    #[test]
    fn failover_election_prefers_higher_repl_offset() {
        let master_id = "m0".repeat(20);
        let r = ClusterState::single_node("127.0.0.1", 7200);
        let peer = "aa".repeat(20); // id lower than most random self ids
        r.add_node(&master_id, "127.0.0.1", 7000);
        r.configure_as_replica_of(&master_id).unwrap();
        // Peer has much higher offset via ROLEMAP → wins even if id is smaller.
        r.merge_role_map(&[RoleMapEntry {
            id: peer.clone(),
            master: false,
            master_id: master_id.clone(),
            ip: "10.0.0.2".into(),
            port: 7002,
            repl_offset: 1_000_000,
            repl_priority: 100,
        }]);
        r.set_local_repl_offset(10);
        assert_eq!(r.election_repl_offset(&peer), 1_000_000);
        assert_eq!(
            r.failover_election_winner(&master_id).as_deref(),
            Some(peer.as_str())
        );
        assert!(!r.should_claim_on_failover(&master_id));

        // Self catches up past peer → self wins.
        r.set_local_repl_offset(2_000_000);
        assert_eq!(
            r.failover_election_winner(&master_id).as_deref(),
            Some(r.my_id().as_str())
        );
        assert!(r.should_claim_on_failover(&master_id));
    }

    #[test]
    fn manual_failover_safe_requires_fail_and_winner() {
        use super::ManualFailoverMode;
        let master_id = "m0".repeat(20);
        let r = ClusterState::single_node("127.0.0.1", 7400);
        r.add_node(&master_id, "127.0.0.1", 7000);
        r.configure_as_replica_of(&master_id).unwrap();
        // Master still healthy → Safe fails.
        assert!(r.manual_failover(ManualFailoverMode::Safe).is_err());
        r.mark_fail(&master_id);
        // Sole replica with default priority → winner.
        let n = r.manual_failover(ManualFailoverMode::Safe).unwrap();
        assert_eq!(n, SLOT_COUNT as usize);
        assert!(r.owns_slot(0));
    }

    #[test]
    fn manual_failover_takeover_ignores_election() {
        use super::ManualFailoverMode;
        let master_id = "m0".repeat(20);
        let r = ClusterState::single_node("127.0.0.1", 7401);
        r.add_node(&master_id, "127.0.0.1", 7000);
        r.configure_as_replica_of(&master_id).unwrap();
        r.set_local_repl_priority(0); // would never win election
        assert!(r.failover_election_winner(&master_id).is_none());
        let n = r.manual_failover(ManualFailoverMode::Takeover).unwrap();
        assert_eq!(n, SLOT_COUNT as usize);
        assert!(r.owns_slot(0));
        assert!(r.node_is_fail(&master_id));
    }

    #[test]
    fn manual_failover_force_requires_winner() {
        use super::ManualFailoverMode;
        let master_id = "m0".repeat(20);
        let r = ClusterState::single_node("127.0.0.1", 7402);
        r.add_node(&master_id, "127.0.0.1", 7000);
        r.configure_as_replica_of(&master_id).unwrap();
        r.set_local_repl_priority(0);
        assert!(r.manual_failover(ManualFailoverMode::Force).is_err());
        r.set_local_repl_priority(100);
        let n = r.manual_failover(ManualFailoverMode::Force).unwrap();
        assert_eq!(n, SLOT_COUNT as usize);
    }

    #[test]
    fn non_replica_should_not_claim() {
        let cs = ClusterState::single_node("127.0.0.1", 7000);
        let other = "mm".repeat(20);
        cs.add_node(&other, "10.0.0.2", 7001);
        assert!(!cs.should_claim_on_failover(&other));
        assert!(cs.failover_election_winner(&other).is_none());
    }

    #[test]
    fn loser_reconfigures_as_replica_of_winner() {
        let master_id = "m0".repeat(20);
        let winner = "zz".repeat(20);
        let loser = ClusterState::single_node("127.0.0.1", 7100);
        loser.add_node(&master_id, "127.0.0.1", 7000);
        loser.configure_as_replica_of(&master_id).unwrap();
        loser.add_node_with_role(
            &winner,
            "10.0.0.9",
            7009,
            Some(false),
            Some(Some(master_id.clone())),
        );
        // Winner id > loser's random id in most cases; force winner as max by
        // using zz… and ensuring loser is not that id.
        assert_ne!(loser.my_id(), winner);
        // Make sure winner would win if both are candidates (zz prefix is high).
        let w = loser.failover_election_winner(&master_id);
        // If self id > winner, reconfig path still must work when called directly.
        loser
            .reconfigure_as_replica_of_failover_winner(&winner)
            .unwrap();
        assert!(loser.is_replica_of(&winner));
        assert!(!loser.is_replica_of(&master_id));
        let wn = loser.get_node(&winner).unwrap();
        assert!(wn.master);
        let _ = w; // election may pick self if id > winner; reconfig API is independent
    }

    #[test]
    fn small_cluster_unreachable_is_single_observer_fail() {
        let cs = ClusterState::single_node("127.0.0.1", 7000);
        let other = "op".repeat(20);
        cs.add_node(&other, "10.0.0.2", 7001);
        assert_eq!(cs.fail_quorum_size(), 1);
        assert!(cs.note_unreachable(&other));
        assert!(cs.node_is_fail(&other));
        assert!(!cs.node_is_pfail(&other));
        assert!(cs.local_fail_reports().contains(&other));
    }

    #[test]
    fn count_failure_reports_includes_local_and_peers() {
        let a = ClusterState::single_node("127.0.0.1", 7000);
        let b = "bb".repeat(20);
        let c = "cc".repeat(20);
        a.add_node(&b, "10.0.0.2", 7001);
        a.add_node(&c, "10.0.0.3", 7002);
        assert_eq!(a.count_failure_reports(&b), 0);
        a.note_unreachable(&b); // small cluster → fail immediately
        assert!(a.count_failure_reports(&b) >= 1);
        a.ingest_fail_reports(&c, &[b.clone()]);
        assert!(a.count_failure_reports(&b) >= 2);
    }

    #[test]
    fn multi_master_needs_quorum_to_fail() {
        let a = ClusterState::single_node("127.0.0.1", 7000);
        let b = "qr".repeat(20);
        let c = "st".repeat(20);
        let d = "uv".repeat(20);
        a.add_node(&b, "10.0.0.2", 7001);
        a.add_node(&c, "10.0.0.3", 7002);
        a.add_node(&d, "10.0.0.4", 7003);
        // 4 masters → quorum = 3
        assert_eq!(a.master_count(), 4);
        assert_eq!(a.fail_quorum_size(), 3);

        // Local observe only → pfail, not fail.
        assert!(!a.note_unreachable(&b));
        assert!(a.node_is_pfail(&b));
        assert!(!a.node_is_fail(&b));
        let nodes = a.format_nodes();
        assert!(nodes.contains("fail?"), "expected pfail flag: {}", nodes);

        // One peer report → still 2 votes < 3.
        a.ingest_fail_reports(&c, &[b.clone()]);
        assert!(a.escalate_fails().is_empty());
        assert!(a.node_is_pfail(&b));

        // Second peer report → 3 votes → fail.
        a.ingest_fail_reports(&d, &[b.clone()]);
        let newly = a.escalate_fails();
        assert_eq!(newly, vec![b.clone()]);
        assert!(a.node_is_fail(&b));
        assert!(!a.node_is_pfail(&b));
    }

    #[test]
    fn touch_pong_clears_pfail_and_reports() {
        let cs = ClusterState::single_node("127.0.0.1", 7010);
        let x = "x1".repeat(20);
        let y = "y1".repeat(20);
        cs.add_node(&x, "10.0.0.8", 7011);
        cs.add_node(&y, "10.0.0.9", 7012);
        // 3 masters → quorum 2; local observe alone stays pfail.
        assert_eq!(cs.fail_quorum_size(), 2);
        assert!(!cs.note_unreachable(&x));
        assert!(cs.node_is_pfail(&x));
        cs.touch_pong(&x);
        assert!(!cs.node_is_pfail(&x));
        assert!(!cs.node_is_fail(&x));
        assert!(cs.local_fail_reports().is_empty());
    }

    #[test]
    fn replica_claim_slots_on_failover() {
        let cs = ClusterState::single_node("127.0.0.1", 7000);
        let master_id = "cd".repeat(20);
        cs.add_node(&master_id, "127.0.0.1", 7001);
        cs.configure_as_replica_of(&master_id).unwrap();
        assert!(!cs.owns_slot(0));
        assert!(cs.is_replica_of(&master_id));
        cs.mark_fail(&master_id);
        let n = cs.claim_slots_from(&master_id).unwrap();
        assert_eq!(n, SLOT_COUNT as usize);
        assert!(cs.owns_slot(0));
        assert!(cs.owns_slot(16383));
        let me = cs.get_node(&cs.my_id()).unwrap();
        assert!(me.master);
        assert!(me.master_id.is_none());
        // Failover stamps a higher epoch on claimed slots.
        assert!(cs.slot_epoch(0) > 1);
        assert_eq!(cs.slot_epoch(0), cs.current_epoch());
    }

    #[test]
    fn reassign_bumps_slot_epoch() {
        let cs = ClusterState::single_node("127.0.0.1", 6379);
        let other = "ab".repeat(20);
        cs.add_node(&other, "10.0.0.2", 7001);
        let before = cs.slot_epoch(100);
        cs.reassign_slot(100, &other).unwrap();
        assert!(cs.slot_epoch(100) > before);
        assert_eq!(cs.owner_id_of(100).as_deref(), Some(other.as_str()));
    }

    #[test]
    fn ownership_merge_higher_epoch_wins() {
        let local = ClusterState::single_node("127.0.0.1", 7000);
        let peer_id = "ef".repeat(20);
        // Peer claims slot 42 with epoch 5 (local starts at 1).
        let range = OwnershipRange {
            start: 42,
            end: 42,
            owner_id: peer_id.clone(),
            ip: "10.0.0.9".into(),
            port: 7009,
            epoch: 5,
        };
        assert_eq!(
            local.apply_ownership_range(&range),
            OwnershipApplyResult::Applied
        );
        assert_eq!(local.owner_id_of(42).as_deref(), Some(peer_id.as_str()));
        assert_eq!(local.slot_epoch(42), 5);
        assert!(local.get_node(&peer_id).is_some());

        // Stale peer epoch must not clobber.
        let stale = OwnershipRange {
            epoch: 2,
            owner_id: "st".repeat(20),
            ip: "10.0.0.8".into(),
            port: 7008,
            ..range.clone()
        };
        assert_eq!(
            local.apply_ownership_range(&stale),
            OwnershipApplyResult::RejectedStale
        );
        assert_eq!(local.owner_id_of(42).as_deref(), Some(peer_id.as_str()));
    }

    #[test]
    fn ownership_merge_skips_migrating_slot() {
        let cs = ClusterState::single_node("127.0.0.1", 7000);
        let dest = "gh".repeat(20);
        cs.add_node(&dest, "10.0.0.3", 7003);
        cs.set_migrating(7, &dest).unwrap();
        let range = OwnershipRange {
            start: 7,
            end: 7,
            owner_id: dest.clone(),
            ip: "10.0.0.3".into(),
            port: 7003,
            epoch: 99,
        };
        assert_eq!(
            cs.apply_ownership_range(&range),
            OwnershipApplyResult::SkippedTransition
        );
        // Still ourselves as owner (MIGRATING does not change owner).
        assert_eq!(cs.owner_id_of(7).as_deref(), Some(cs.my_id().as_str()));
        assert!(cs.is_migrating(7));
    }

    #[test]
    fn ownership_snapshot_compresses_ranges() {
        let cs = ClusterState::single_node("127.0.0.1", 7000);
        let snap = cs.ownership_snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].start, 0);
        assert_eq!(snap[0].end, SLOT_COUNT - 1);
        assert_eq!(snap[0].owner_id, cs.my_id());
        assert_eq!(snap[0].epoch, 1);

        let other = "ij".repeat(20);
        cs.add_node(&other, "10.0.0.4", 7004);
        cs.reassign_slot(100, &other).unwrap();
        let snap2 = cs.ownership_snapshot();
        assert!(snap2.len() >= 3, "split around slot 100: {:?}", snap2.len());
        let mid = snap2.iter().find(|r| r.start == 100 && r.end == 100).unwrap();
        assert_eq!(mid.owner_id, other);
        assert!(mid.epoch > 1);
    }

    #[test]
    fn equal_epoch_conflict_keeps_local() {
        let a = ClusterState::single_node("127.0.0.1", 7000);
        let b_id = "kl".repeat(20);
        // Same epoch as initial (1), different owner — must not flip.
        let range = OwnershipRange {
            start: 0,
            end: 0,
            owner_id: b_id,
            ip: "10.0.0.5".into(),
            port: 7005,
            epoch: 1,
        };
        assert_eq!(
            a.apply_ownership_range(&range),
            OwnershipApplyResult::RejectedStale
        );
        assert_eq!(a.owner_id_of(0).as_deref(), Some(a.my_id().as_str()));
    }
}
