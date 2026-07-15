//! Thin cluster gossip over the client RESP port (not Redis binary bus).
//!
//! - MEET: TCP connect + CLUSTER MYID / CLUSTER MEETPEER exchange
//! - Heartbeat: periodic PING; single-observer timeout → mark `fail`
//! - On master FAIL: if we are that master's replica, promote + claim slots
//!
//! **Not Redis-compatible quorum fail detection** — one local observer is enough.

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
pub async fn meet_peer(
    cluster: &ClusterState,
    ip: &str,
    port: u16,
) -> Result<(), String> {
    let addr = format!("{}:{}", ip, port);
    let timeout = Duration::from_millis(cluster.node_timeout_ms().max(500));

    let mut stream = match tokio::time::timeout(timeout, TcpStream::connect(&addr)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(format!("ERR CLUSTER MEET unable to connect to {}: {}", addr, e)),
        Err(_) => return Err(format!("ERR CLUSTER MEET timed out connecting to {}", addr)),
    };
    let _ = stream.set_nodelay(true);

    // Learn peer identity
    let myid = match resp_command(&mut stream, &["CLUSTER", "MYID"], timeout).await {
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

    if myid == cluster.my_id() {
        return Err("ERR CLUSTER MEET would meet myself".into());
    }

    // Announce ourselves so the peer adds us to its nodes table
    let (my_ip, my_port) = cluster.addr();
    let my_id = cluster.my_id();
    match resp_command(
        &mut stream,
        &[
            "CLUSTER",
            "MEETPEER",
            &my_id,
            &my_ip,
            &my_port.to_string(),
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

    cluster.add_node(&myid, ip, port);
    cluster.touch_pong(&myid);
    Ok(())
}

/// One gossip / heartbeat cycle: PING peers; mark fail on timeout; maybe failover.
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
        let ok = ping_peer(&addr, probe_timeout).await;
        if ok {
            cluster.touch_pong(&peer.id);
            continue;
        }
        // No successful pong recently → mark fail (single-observer).
        if cluster.elapsed_since_pong(&peer.id) >= timeout {
            info!(
                "cluster: marking node {} ({}:{}) as fail (node-timeout {}ms, single-observer)",
                &peer.id[..peer.id.len().min(8)],
                peer.ip,
                peer.port,
                cluster.node_timeout_ms()
            );
            cluster.mark_fail(&peer.id);
            maybe_failover_on_master_fail(cluster, persistence, &peer.id);
        }
    }
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

    info!(
        "cluster: master {} failed — promoting myself and claiming slots",
        &failed_id[..failed_id.len().min(8)]
    );

    if let Some(p) = persistence {
        p.replication.promote_to_master();
    }

    match cluster.claim_slots_from(failed_id) {
        Ok(n) => {
            info!("cluster: claimed {} slots from failed master", n);
        }
        Err(e) => {
            warn!("cluster: claim_slots_from failed: {}", e);
        }
    }
}

async fn ping_peer(addr: &str, timeout: Duration) -> bool {
    let connect = TcpStream::connect(addr);
    let mut stream = match tokio::time::timeout(timeout, connect).await {
        Ok(Ok(s)) => s,
        _ => return false,
    };
    let _ = stream.set_nodelay(true);
    match resp_command(&mut stream, &["PING"], timeout).await {
        Ok(RespValue::SimpleString(s)) if s.as_ref() == b"PONG" => true,
        Ok(RespValue::BulkString(Some(b))) if b.as_ref() == b"PONG" => true,
        _ => false,
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
