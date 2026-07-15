//! Cluster topology: slot ownership, migrating/importing, membership, fail flags.

use super::crc16::SLOT_COUNT;
use parking_lot::RwLock;
use std::collections::HashMap;
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
    /// Single-observer fail flag (not Redis quorum PFAIL/FAIL).
    pub fail: bool,
}

/// Redirect target for MOVED / ASK replies.
#[derive(Debug, Clone)]
pub struct RedirectTarget {
    pub slot: u16,
    pub ip: String,
    pub port: u16,
    pub node_id: String,
}

/// Cluster topology for this process.
#[derive(Debug)]
pub struct ClusterState {
    inner: RwLock<Inner>,
}

#[derive(Debug)]
struct Inner {
    my_id: String,
    ip: String,
    port: u16,
    /// slot → owning node id
    slot_owner: Vec<String>,
    /// slot → destination node id (we are migrating this slot away)
    migrating: HashMap<u16, String>,
    /// slot → source node id (we are importing this slot)
    importing: HashMap<u16, String>,
    /// known nodes by id
    nodes: HashMap<String, ClusterNode>,
    current_epoch: u64,
    /// Fail detection timeout (ms). Heartbeat marks fail after this without pong.
    node_timeout_ms: u64,
    /// Last successful heartbeat / meet time per peer id.
    last_pong: HashMap<String, Instant>,
}

impl ClusterState {
    /// Create a single-node cluster that owns every slot.
    pub fn single_node(ip: impl Into<String>, port: u16) -> Arc<Self> {
        let ip = ip.into();
        let my_id = generate_node_id();
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
                master: true,
                master_id: None,
                fail: false,
            },
        );
        let slot_owner = vec![my_id.clone(); SLOT_COUNT as usize];
        Arc::new(Self {
            inner: RwLock::new(Inner {
                my_id,
                ip,
                port,
                slot_owner,
                migrating: HashMap::new(),
                importing: HashMap::new(),
                nodes,
                current_epoch: 1,
                node_timeout_ms: DEFAULT_NODE_TIMEOUT_MS,
                last_pong: HashMap::new(),
            }),
        })
    }

    pub fn my_id(&self) -> String {
        self.inner.read().my_id.clone()
    }

    pub fn addr(&self) -> (String, u16) {
        let g = self.inner.read();
        (g.ip.clone(), g.port)
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
        g.nodes.get(id).cloned()
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
    pub fn add_node(&self, id: impl Into<String>, ip: impl Into<String>, port: u16) {
        let id = id.into();
        let ip = ip.into();
        let cport = port.saturating_add(10000);
        let mut g = self.inner.write();
        let myself = id == g.my_id;
        if myself {
            // Do not overwrite myself entry via peer path.
            return;
        }
        let existing = g.nodes.get(&id);
        let master = existing.map(|n| n.master).unwrap_or(true);
        let master_id = existing.and_then(|n| n.master_id.clone());
        let fail = existing.map(|n| n.fail).unwrap_or(false);
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
                fail,
            },
        );
        g.last_pong.entry(id).or_insert_with(Instant::now);
    }

    /// Record a successful heartbeat / meet exchange.
    pub fn touch_pong(&self, id: &str) {
        let mut g = self.inner.write();
        g.last_pong.insert(id.to_string(), Instant::now());
        if let Some(n) = g.nodes.get_mut(id) {
            // Clear fail on recovery (best-effort; Redis needs more).
            n.fail = false;
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

    /// Mark a peer as failed (single-observer).
    pub fn mark_fail(&self, id: &str) {
        let mut g = self.inner.write();
        if id == g.my_id {
            return;
        }
        if let Some(n) = g.nodes.get_mut(id) {
            n.fail = true;
        }
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
        // Give our slots to the master.
        let my_id = g.my_id.clone();
        for owner in g.slot_owner.iter_mut() {
            if owner == &my_id {
                *owner = master_id.to_string();
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
        for owner in g.slot_owner.iter_mut() {
            if owner == failed_id {
                *owner = my_id.clone();
                claimed += 1;
            }
        }
        g.migrating.clear();
        g.importing.clear();
        if let Some(me) = g.nodes.get_mut(&my_id) {
            me.master = true;
            me.master_id = None;
            me.fail = false;
        }
        // Bump epoch on failover
        g.current_epoch = g.current_epoch.saturating_add(1);
        Ok(claimed)
    }

    /// Reassign a single slot to `node_id` (test helper / SETSLOT NODE).
    pub fn reassign_slot(&self, slot: u16, node_id: &str) -> Result<(), String> {
        if slot >= SLOT_COUNT {
            return Err(format!("slot out of range: {}", slot));
        }
        let mut g = self.inner.write();
        if !g.nodes.contains_key(node_id) {
            return Err(format!("Unknown node {}", node_id));
        }
        g.slot_owner[slot as usize] = node_id.to_string();
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
    pub fn set_node(&self, slot: u16, node_id: &str) -> Result<(), String> {
        self.reassign_slot(slot, node_id)
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

    /// CLUSTER SLOTS nested array data (start, end, master node).
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
                if let Some(node) = g.nodes.get(&cur) {
                    out.push((start, slot - 1, node.clone()));
                }
                start = slot;
                cur = owner.clone();
            }
        }
        if let Some(node) = g.nodes.get(&cur) {
            out.push((start, SLOT_COUNT - 1, node.clone()));
        }
        out
    }

    /// CLUSTER INFO text.
    pub fn format_info(&self) -> String {
        let g = self.inner.read();
        let assigned = g
            .slot_owner
            .iter()
            .filter(|id| g.nodes.contains_key(id.as_str()))
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
            if let Some(n) = g.nodes.get(id) {
                if n.master && !n.fail {
                    masters_with_slots.insert(id.clone());
                }
            }
        }
        format!(
            "cluster_state:ok\r\n\
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
            assigned,
            assigned.saturating_sub(fail_slots),
            fail_slots,
            known,
            masters_with_slots.len().max(1),
            g.current_epoch,
            g.current_epoch,
        )
    }
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
    }
}
