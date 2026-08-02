//! Thin cluster gossip: RESP on the client port, with peer-bus prefer (Batch HA).
//!
//! - MEET: prefer KORB bus `MEET` on cport; fall back to CLUSTER MYID / MEETPEER
//! - Heartbeat: prefer bus identity `PING`; fall back to RESP PING; timeout → pfail/fail
//! - After successful PING: pull `CLUSTER OWNERS` + `CLUSTER FAILREPORTS` (RESP)
//! - On master FAIL: replica election (Batch DY); winner claims; losers re-point
//!   at winner (Batch DZ)
//!
//! **Fail detection (Batch DW):** multi-master vote quorum (`masters/2+1`);
//! ≤2 masters keep single-observer fail.
//! **Replica election (Batch DY/EA/EB):** highest priority (0 never), then
//! offset, then max id; ROLEMAP carries offset+priority.
//! **Loser reconfig (Batch DZ):** non-winners follow the winner.

use super::bus::{self, MeetBody};
use super::state::{OwnershipRange, RoleMapEntry};
use super::ClusterState;
use crate::persistence::PersistenceManager;
use crate::protocol::{RespParser, RespValue};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tracing::{debug, info, warn};

/// Default connect/command timeout for meet / heartbeat probes.
const PROBE_IO_TIMEOUT: Duration = Duration::from_millis(500);

/// Run MEET against `ip:port`: learn peer id, announce ourselves, add peer locally.
///
/// Batch HA: prefer KORB bus MEET on `port+10000`; fall back to RESP MYID/MEETPEER.
pub async fn meet_peer(
    cluster: &ClusterState,
    ip: &str,
    port: u16,
) -> Result<(), String> {
    let timeout = Duration::from_millis(cluster.node_timeout_ms().max(500));
    let (my_ip, my_port) = cluster.addr();
    let my_id = cluster.my_id();
    let (role, master_id_wire) = cluster.my_role_wire();

    // ── Bus MEET (Batch HA) ────────────────────────────────────────────────
    let announce = MeetBody {
        id: my_id.clone(),
        ip: my_ip.clone(),
        port: my_port,
        role: role.clone(),
        master_id: master_id_wire.clone(),
    };
    match bus::bus_meet(ip, port, &announce).await {
        Ok(peer_id) => {
            if peer_id == my_id {
                return Err("ERR CLUSTER MEET would meet myself".into());
            }
            cluster.add_node(&peer_id, ip, port);
            cluster.touch_pong(&peer_id);
            // Topology pull still over RESP when client port is up.
            let addr = format!("{}:{}", ip, port);
            if let Ok(Ok(mut stream)) =
                tokio::time::timeout(timeout, TcpStream::connect(&addr)).await
            {
                let _ = stream.set_nodelay(true);
                pull_and_merge_owners(cluster, &mut stream, timeout).await;
                pull_and_merge_rolemap(cluster, &mut stream, timeout).await;
            }
            debug!("cluster MEET via peer bus to {}:{}", ip, port);
            return Ok(());
        }
        Err(e) if e.is_transport() => {
            debug!(
                "cluster MEET bus miss {}:{} ({}); falling back to RESP",
                ip, port, e
            );
        }
        Err(e) => {
            // Remote application error (e.g. meet myself) — surface to caller.
            return Err(format!("ERR CLUSTER MEET {}", e.message()));
        }
    }

    // ── RESP fallback (pre-HA path) ────────────────────────────────────────
    let addr = format!("{}:{}", ip, port);
    let mut stream = match tokio::time::timeout(timeout, TcpStream::connect(&addr)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(format!("ERR CLUSTER MEET unable to connect to {}: {}", addr, e)),
        Err(_) => return Err(format!("ERR CLUSTER MEET timed out connecting to {}", addr)),
    };
    let _ = stream.set_nodelay(true);

    // Learn peer identity
    let peer_id = match resp_command(&mut stream, &["CLUSTER", "MYID"], timeout).await {
        Ok(RespValue::BulkString(Some(b))) => String::from_utf8_lossy(&b).into_owned(),
        Ok(RespValue::Error(e)) => {
            return Err(format!(
                "ERR CLUSTER MEET peer error: {}",
                String::from_utf8_lossy(&e)
            ));
        }
        Ok(other) => {
            return Err(format!(
                "ERR CLUSTER MEET unexpected MYID reply: {:?}",
                other
            ));
        }
        Err(e) => return Err(format!("ERR CLUSTER MEET {}", e)),
    };

    if peer_id == my_id {
        return Err("ERR CLUSTER MEET would meet myself".into());
    }

    // Announce ourselves so the peer adds us to its nodes table (role fields Batch DY).
    match resp_command(
        &mut stream,
        &[
            "CLUSTER",
            "MEETPEER",
            &my_id,
            &my_ip,
            &my_port.to_string(),
            &role,
            &master_id_wire,
        ],
        timeout,
    )
    .await
    {
        Ok(RespValue::SimpleString(s)) if s.as_ref() == b"OK" => {}
        Ok(RespValue::Error(e)) => {
            return Err(format!(
                "ERR CLUSTER MEET peer rejected MEETPEER: {}",
                String::from_utf8_lossy(&e)
            ));
        }
        Ok(other) => {
            return Err(format!(
                "ERR CLUSTER MEET unexpected MEETPEER reply: {:?}",
                other
            ));
        }
        Err(e) => return Err(format!("ERR CLUSTER MEET {}", e)),
    }

    cluster.add_node(&peer_id, ip, port);
    cluster.touch_pong(&peer_id);
    // Best-effort: pull OWNERS + ROLEMAP so MEET learns topology (DU/DY).
    if let Ok(Ok(mut stream)) = tokio::time::timeout(timeout, TcpStream::connect(&addr)).await {
        let _ = stream.set_nodelay(true);
        pull_and_merge_owners(cluster, &mut stream, timeout).await;
        pull_and_merge_rolemap(cluster, &mut stream, timeout).await;
    }
    Ok(())
}

/// One gossip / heartbeat cycle: PING peers; pull OWNERS + FAILREPORTS; escalate fails.
pub async fn gossip_tick(
    cluster: &Arc<ClusterState>,
    persistence: Option<&Arc<PersistenceManager>>,
) {
    let peers = cluster.peer_snapshots();
    let timeout = Duration::from_millis(cluster.node_timeout_ms());
    let probe_timeout = timeout.min(PROBE_IO_TIMEOUT).max(Duration::from_millis(50));

    for peer in peers {
        if peer.fail {
            continue;
        }
        let addr = format!("{}:{}", peer.ip, peer.port);
        match ping_and_sync(
            cluster,
            &peer.id,
            &peer.ip,
            peer.port,
            &addr,
            probe_timeout,
        )
        .await
        {
            true => {
                cluster.touch_pong(&peer.id);
            }
            false => {
                if cluster.elapsed_since_pong(&peer.id) >= timeout {
                    let newly = cluster.note_unreachable(&peer.id);
                    if newly {
                        info!(
                            "cluster: node {} ({}:{}) → fail (node-timeout {}ms, quorum={})",
                            &peer.id[..peer.id.len().min(8)],
                            peer.ip,
                            peer.port,
                            cluster.node_timeout_ms(),
                            cluster.fail_quorum_size()
                        );
                        // Batch HA: best-effort FAIL fan-out over peer bus (ignore errors).
                        bus_announce_fail_to_others(cluster, &peer.id).await;
                        maybe_failover_on_master_fail(cluster, persistence, &peer.id);
                    } else if cluster.node_is_pfail(&peer.id) {
                        debug!(
                            "cluster: node {} ({}:{}) → pfail (waiting quorum={})",
                            &peer.id[..peer.id.len().min(8)],
                            peer.ip,
                            peer.port,
                            cluster.fail_quorum_size()
                        );
                    }
                }
            }
        }
    }

    // Escalate using reports gathered this tick (multi-master path).
    for id in cluster.escalate_fails() {
        info!(
            "cluster: node {} escalated to fail via quorum (quorum={})",
            &id[..id.len().min(8)],
            cluster.fail_quorum_size()
        );
        maybe_failover_on_master_fail(cluster, persistence, &id);
    }

    // Batch DZ/EA: retry loser re-point if still attached to a failed master.
    try_follow_failed_masters(cluster, persistence);
    // Heal dual-master races (two replicas both claimed before ROLEMAP met).
    try_demote_if_superseded(cluster, persistence);
}

/// If we are still a replica of a failed master, follow the new master / winner.
///
/// Does **not** late-claim: after the real winner promotes, they leave the
/// "replica of failed" set and a naive re-election would make us the sole
/// candidate and wrongly promote (Batch EA/DZ fix).
fn try_follow_failed_masters(
    cluster: &Arc<ClusterState>,
    persistence: Option<&Arc<PersistenceManager>>,
) {
    let peers = cluster.peer_snapshots();
    let my_failed_masters: Vec<String> = peers
        .into_iter()
        .filter(|p| p.fail && cluster.is_replica_of(&p.id))
        .map(|p| p.id)
        .collect();
    if my_failed_masters.is_empty() {
        return;
    }
    if let Some(p) = persistence {
        let off = p
            .replication
            .replica_offset()
            .max(p.replication.master_repl_offset());
        cluster.set_local_repl_offset(off);
    }

    // Prefer a live master that already holds slots (winner already claimed).
    if let Some(new_master) = cluster.other_master_with_slots() {
        if !cluster.is_replica_of(&new_master) {
            follow_new_master(cluster, persistence, &new_master);
        }
        return;
    }

    for failed_id in my_failed_masters {
        if let Some(winner) = cluster.failover_election_winner(&failed_id) {
            if winner != cluster.my_id() && !cluster.is_replica_of(&winner) {
                follow_new_master(cluster, persistence, &winner);
            }
        }
    }
}

fn follow_new_master(
    cluster: &ClusterState,
    persistence: Option<&Arc<PersistenceManager>>,
    winner: &str,
) {
    match cluster.reconfigure_as_replica_of_failover_winner(winner) {
        Ok(()) => {
            if let Some(p) = persistence {
                if let Some(w) = cluster.get_node(winner) {
                    p.replication
                        .set_replicaof(Some(format!("{}:{}", w.ip, w.port)));
                }
            }
            info!(
                "cluster: re-pointed to failover master {}",
                &winner[..winner.len().min(8)]
            );
        }
        Err(e) => debug!("cluster: re-point to {} failed: {}", winner, e),
    }
}

/// If we are a master but another live master has more (or equal+higher id) slots,
/// demote and follow them — recovers dual-claim races when ROLEMAP lagged.
fn try_demote_if_superseded(
    cluster: &Arc<ClusterState>,
    persistence: Option<&Arc<PersistenceManager>>,
) {
    let me = cluster.get_node(&cluster.my_id());
    let Some(me) = me else {
        return;
    };
    if !me.master || me.fail {
        return;
    }
    let my_slots = {
        // Count slots we own.
        let mut n = 0usize;
        for s in 0..crate::cluster::SLOT_COUNT {
            if cluster.owns_slot(s) {
                n += 1;
            }
        }
        n
    };
    let Some(other) = cluster.other_master_with_slots() else {
        return;
    };
    let other_slots = {
        let mut n = 0usize;
        for s in 0..crate::cluster::SLOT_COUNT {
            if cluster.owner_id_of(s).as_deref() == Some(other.as_str()) {
                n += 1;
            }
        }
        n
    };
    let demote = other_slots > my_slots
        || (other_slots == my_slots && other > cluster.my_id())
        || (my_slots == 0 && other_slots > 0);
    if !demote {
        return;
    }
    info!(
        "cluster: demoting to follow master {} (slots me={} other={})",
        &other[..other.len().min(8)],
        my_slots,
        other_slots
    );
    follow_new_master(cluster, persistence, &other);
}

/// Best-effort FAIL announce to other live peers over the bus (Batch HA).
async fn bus_announce_fail_to_others(cluster: &ClusterState, failed_id: &str) {
    for p in cluster.peer_snapshots() {
        if p.id == failed_id || p.fail {
            continue;
        }
        let _ = bus::bus_fail_announce(&p.ip, p.port, failed_id).await;
    }
}

/// PING peer; on success pull OWNERS + FAILREPORTS.
///
/// Batch HA: try KORB identity PING on cport first; fall back to RESP PING.
async fn ping_and_sync(
    cluster: &ClusterState,
    peer_id: &str,
    peer_ip: &str,
    peer_port: u16,
    addr: &str,
    timeout: Duration,
) -> bool {
    let my_id = cluster.my_id();
    let bus_ok = match bus::bus_ping_id(peer_ip, peer_port, &my_id).await {
        Ok(_pong_id) => true,
        Err(e) if e.is_transport() => false,
        Err(_) => false,
    };

    let connect = TcpStream::connect(addr);
    let mut stream = match tokio::time::timeout(timeout, connect).await {
        Ok(Ok(s)) => s,
        _ => return bus_ok, // bus-only success still counts as reachable
    };
    let _ = stream.set_nodelay(true);

    if !bus_ok {
        match resp_command(&mut stream, &["PING"], timeout).await {
            Ok(RespValue::SimpleString(s)) if s.as_ref() == b"PONG" => {}
            Ok(RespValue::BulkString(Some(b))) if b.as_ref() == b"PONG" => {}
            _ => return false,
        }
    }
    // Topology / fail reports still over RESP when client port is up.
    pull_and_merge_owners(cluster, &mut stream, timeout).await;
    pull_and_merge_rolemap(cluster, &mut stream, timeout).await;
    pull_and_ingest_fail_reports(cluster, peer_id, &mut stream, timeout).await;
    true
}

async fn pull_and_merge_rolemap(
    cluster: &ClusterState,
    stream: &mut TcpStream,
    timeout: Duration,
) {
    match resp_command(stream, &["CLUSTER", "ROLEMAP"], timeout).await {
        Ok(val) => match parse_rolemap_reply(&val) {
            Ok(entries) => {
                if !entries.is_empty() {
                    cluster.merge_role_map(&entries);
                    debug!("cluster: merged ROLEMAP entries={}", entries.len());
                }
            }
            Err(e) => debug!("cluster: ROLEMAP parse failed: {}", e),
        },
        Err(e) => debug!("cluster: ROLEMAP fetch failed: {}", e),
    }
}

/// Parse `CLUSTER ROLEMAP` [id, role, master_id, ip, port, offset?, priority?].
pub fn parse_rolemap_reply(val: &RespValue) -> Result<Vec<RoleMapEntry>, String> {
    let rows = match val {
        RespValue::Array(a) => a,
        _ => return Err("ROLEMAP reply is not an array".into()),
    };
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let fields = match row {
            RespValue::Array(f) => f,
            _ => return Err("ROLEMAP row is not an array".into()),
        };
        if fields.len() < 5 {
            return Err(format!("ROLEMAP row needs ≥5 fields, got {}", fields.len()));
        }
        let id = match fields[0].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Err("ROLEMAP id not bulk".into()),
        };
        let role = match fields[1].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).to_ascii_lowercase(),
            None => return Err("ROLEMAP role not bulk".into()),
        };
        let master = role == "master";
        let master_id = match fields[2].as_bulk_string() {
            Some(b) => {
                let s = String::from_utf8_lossy(b);
                if s == "-" {
                    String::new()
                } else {
                    s.into_owned()
                }
            }
            None => return Err("ROLEMAP master_id not bulk".into()),
        };
        let ip = match fields[3].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Err("ROLEMAP ip not bulk".into()),
        };
        let port = match fields[4] {
            RespValue::Integer(n) if n >= 0 && n <= u16::MAX as i64 => n as u16,
            _ => return Err("ROLEMAP port not integer".into()),
        };
        let repl_offset = if fields.len() >= 6 {
            match fields[5] {
                RespValue::Integer(n) if n >= 0 => n as u64,
                _ => 0,
            }
        } else {
            0
        };
        let repl_priority = if fields.len() >= 7 {
            match fields[6] {
                RespValue::Integer(n) if n >= 0 && n <= u32::MAX as i64 => n as u32,
                _ => 100,
            }
        } else {
            100
        };
        out.push(RoleMapEntry {
            id,
            master,
            master_id,
            ip,
            port,
            repl_offset,
            repl_priority,
        });
    }
    Ok(out)
}

async fn pull_and_ingest_fail_reports(
    cluster: &ClusterState,
    peer_id: &str,
    stream: &mut TcpStream,
    timeout: Duration,
) {
    match resp_command(stream, &["CLUSTER", "FAILREPORTS"], timeout).await {
        Ok(val) => match parse_fail_reports_reply(&val) {
            Ok(suspects) => {
                cluster.ingest_fail_reports(peer_id, &suspects);
            }
            Err(e) => debug!("cluster: FAILREPORTS parse failed: {}", e),
        },
        Err(e) => debug!("cluster: FAILREPORTS fetch failed: {}", e),
    }
}

/// Parse `CLUSTER FAILREPORTS` array of bulk node ids.
pub fn parse_fail_reports_reply(val: &RespValue) -> Result<Vec<String>, String> {
    let rows = match val {
        RespValue::Array(a) => a,
        _ => return Err("FAILREPORTS reply is not an array".into()),
    };
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        match row.as_bulk_string() {
            Some(b) => out.push(String::from_utf8_lossy(b).into_owned()),
            None => return Err("FAILREPORTS entry not bulk".into()),
        }
    }
    Ok(out)
}

async fn pull_and_merge_owners(
    cluster: &ClusterState,
    stream: &mut TcpStream,
    timeout: Duration,
) {
    match resp_command(stream, &["CLUSTER", "OWNERS"], timeout).await {
        Ok(val) => match parse_owners_reply(&val) {
            Ok(ranges) => {
                let (applied, rejected, skipped) = cluster.merge_ownership_snapshot(&ranges);
                if applied > 0 {
                    debug!(
                        "cluster: merged OWNERS applied={} rejected={} skipped_transition={}",
                        applied, rejected, skipped
                    );
                }
            }
            Err(e) => debug!("cluster: OWNERS parse failed: {}", e),
        },
        Err(e) => debug!("cluster: OWNERS fetch failed: {}", e),
    }
}

/// Parse `CLUSTER OWNERS` array reply into [`OwnershipRange`] values.
pub fn parse_owners_reply(val: &RespValue) -> Result<Vec<OwnershipRange>, String> {
    let rows = match val {
        RespValue::Array(a) => a,
        _ => return Err("OWNERS reply is not an array".into()),
    };
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let fields = match row {
            RespValue::Array(f) => f,
            _ => return Err("OWNERS row is not an array".into()),
        };
        if fields.len() < 6 {
            return Err(format!("OWNERS row needs 6 fields, got {}", fields.len()));
        }
        let start = match fields[0] {
            RespValue::Integer(n) if n >= 0 && n <= u16::MAX as i64 => n as u16,
            _ => return Err("OWNERS start not integer".into()),
        };
        let end = match fields[1] {
            RespValue::Integer(n) if n >= 0 && n <= u16::MAX as i64 => n as u16,
            _ => return Err("OWNERS end not integer".into()),
        };
        let owner_id = match fields[2].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Err("OWNERS owner_id not bulk".into()),
        };
        let ip = match fields[3].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Err("OWNERS ip not bulk".into()),
        };
        let port = match fields[4] {
            RespValue::Integer(n) if n >= 0 && n <= u16::MAX as i64 => n as u16,
            _ => return Err("OWNERS port not integer".into()),
        };
        let epoch = match fields[5] {
            RespValue::Integer(n) if n >= 0 => n as u64,
            _ => return Err("OWNERS epoch not integer".into()),
        };
        out.push(OwnershipRange {
            start,
            end,
            owner_id,
            ip,
            port,
            epoch,
        });
    }
    Ok(out)
}

/// Background gossip loop. Interval ≈ node_timeout / 3 (min 20ms).
pub async fn run_cluster_gossip(
    cluster: Arc<ClusterState>,
    persistence: Option<Arc<PersistenceManager>>,
    mut shutdown: watch::Receiver<bool>,
) {
    info!(
        "Cluster gossip started (node-timeout={}ms, single-observer fail detection)",
        cluster.node_timeout_ms()
    );
    loop {
        let interval = Duration::from_millis((cluster.node_timeout_ms() / 3).max(20));
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("Cluster gossip shutting down");
                    break;
                }
            }
            _ = tokio::time::sleep(interval) => {
                gossip_tick(&cluster, persistence.as_ref()).await;
            }
        }
    }
}

fn maybe_failover_on_master_fail(
    cluster: &Arc<ClusterState>,
    persistence: Option<&Arc<PersistenceManager>>,
    failed_id: &str,
) {
    // Only act if we are a cluster replica of the failed node.
    if !cluster.is_replica_of(failed_id) {
        return;
    }

    // Refresh election offset from replication before ranking (Batch EA).
    if let Some(p) = persistence {
        let off = p
            .replication
            .replica_offset()
            .max(p.replication.master_repl_offset());
        cluster.set_local_repl_offset(off);
    }

    // Batch DY/EA: only the election winner claims (offset, then max id).
    if !cluster.should_claim_on_failover(failed_id) {
        let Some(winner) = cluster.failover_election_winner(failed_id) else {
            return;
        };
        info!(
            "cluster: master {} failed — not election winner (winner={}); re-pointing (Batch DZ)",
            &failed_id[..failed_id.len().min(8)],
            &winner[..winner.len().min(8)]
        );
        // Batch DZ: follow the winner so topology converges without dual masters.
        match cluster.reconfigure_as_replica_of_failover_winner(&winner) {
            Ok(()) => {
                if let Some(p) = persistence {
                    if let Some(w) = cluster.get_node(&winner) {
                        p.replication
                            .set_replicaof(Some(format!("{}:{}", w.ip, w.port)));
                        info!(
                            "cluster: now replica of failover winner {} ({}:{})",
                            &winner[..winner.len().min(8)],
                            w.ip,
                            w.port
                        );
                    }
                }
            }
            Err(e) => {
                warn!(
                    "cluster: failed to reconfigure as replica of winner {}: {}",
                    &winner[..winner.len().min(8)],
                    e
                );
            }
        }
        return;
    }

    info!(
        "cluster: master {} failed — election winner; promoting myself and claiming slots",
        &failed_id[..failed_id.len().min(8)]
    );

    if let Some(p) = persistence {
        p.replication.promote_to_master();
    }

    match cluster.claim_slots_from(failed_id) {
        Ok(n) => {
            info!("cluster: claimed {} slots from failed master", n);
            // Batch EO: persist claimed ownership for next boot.
            cluster.autosave_nodes_conf();
        }
        Err(e) => {
            warn!("cluster: claim_slots_from failed: {}", e);
        }
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
        Ok(Err(e)) => return Err(format!("write error: {}", e)),
        Err(_) => return Err("write timed out".into()),
    }

    let mut parser = RespParser::new();
    let mut buf = vec![0u8; 8192];
    loop {
        if let Some(val) = parser
            .parse()
            .map_err(|e| format!("parse error: {}", e))?
        {
            return Ok(val);
        }
        let n = match tokio::time::timeout(timeout, stream.read(&mut buf)).await {
            Ok(Ok(0)) => return Err("connection closed".into()),
            Ok(Ok(n)) => n,
            Ok(Err(e)) => return Err(format!("read error: {}", e)),
            Err(_) => return Err("read timed out".into()),
        };
        parser.feed(&buf[..n]);
    }
}

/// Test / admin helper: force a fail mark (same path as timeout).
pub fn force_mark_fail(
    cluster: &Arc<ClusterState>,
    persistence: Option<&Arc<PersistenceManager>>,
    node_id: &str,
) {
    cluster.mark_fail(node_id);
    maybe_failover_on_master_fail(cluster, persistence, node_id);
    debug!("cluster: force_mark_fail {}", node_id);
}
