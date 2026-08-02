//! Kore peer bus lite for dual-end NODE 2PC (Batch **GP**).
//!
//! Optional length-prefixed binary frames for `PREPARE` / `COMMIT` / `ABORT` /
//! `PING` on the Redis-style cluster bus port (`client_port + 10000`). This is
//! **not** the Redis cluster bus: no gossip opcodes, no MEET over the bus,
//! operator `SETSLOT NODE` still goes over RESP.
//!
//! Wire layout (little-endian body fields; magic is four ASCII bytes `KORB`):
//! ```text
//! magic u32 bytes = b"KORB"
//! version u8 = 1
//! type u8: PING=1 PONG=2 PREPARE=10 COMMIT=11 ABORT=12 OK=20 ERR=21
//! flags u8 = 0
//! reserved u8 = 0
//! body_len u32
//! body...
//! ```
//! PREPARE / COMMIT / ABORT body: `slot u16` + length-prefixed `target_id`
//! (`u16` length + UTF-8 bytes). ERR body: same length-prefixed string.
//!
//! Dual-end reshard prefers the bus when connect succeeds, and falls back to
//! the existing RESP `SETSLOT` path on transport failure (bus down / bind miss).

use super::state::ClusterState;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tracing::{debug, info, warn};

/// Wire magic: four bytes `K` `O` `R` `B` (ASCII).
pub const MAGIC: [u8; 4] = *b"KORB";
/// Protocol version.
pub const VERSION: u8 = 1;
/// Fixed header size in bytes.
pub const HEADER_LEN: usize = 12;
/// Soft cap on frame body (DoS guard).
pub const MAX_BODY_LEN: u32 = 64 * 1024;

pub const TYPE_PING: u8 = 1;
pub const TYPE_PONG: u8 = 2;
pub const TYPE_PREPARE: u8 = 10;
pub const TYPE_COMMIT: u8 = 11;
pub const TYPE_ABORT: u8 = 12;
pub const TYPE_OK: u8 = 20;
pub const TYPE_ERR: u8 = 21;

/// Default I/O timeout for bus client RPCs (shorter than RESP migrate).
const BUS_IO_TIMEOUT: Duration = Duration::from_secs(2);

/// Redis / Kore convention: bus port = client port + 10000.
#[inline]
pub fn peer_cport(client_port: u16) -> u16 {
    client_port.saturating_add(10000)
}

/// Encode a full frame (`type` + body).
pub fn encode_frame(msg_type: u8, body: &[u8]) -> Vec<u8> {
    let body_len = body.len() as u32;
    let mut out = Vec::with_capacity(HEADER_LEN + body.len());
    out.extend_from_slice(&MAGIC);
    out.push(VERSION);
    out.push(msg_type);
    out.push(0); // flags
    out.push(0); // reserved
    out.extend_from_slice(&body_len.to_le_bytes());
    out.extend_from_slice(body);
    out
}

/// Decode header from the first [`HEADER_LEN`] bytes.
///
/// Returns `(msg_type, body_len)`.
pub fn decode_header(buf: &[u8]) -> Result<(u8, u32), String> {
    if buf.len() < HEADER_LEN {
        return Err(format!(
            "header too short: {} < {}",
            buf.len(),
            HEADER_LEN
        ));
    }
    if buf[0..4] != MAGIC {
        return Err(format!(
            "wrong magic: got {:02x?} want KORB",
            &buf[0..4.min(buf.len())]
        ));
    }
    let version = buf[4];
    if version != VERSION {
        return Err(format!("unsupported bus version {}", version));
    }
    let msg_type = buf[5];
    // flags = buf[6], reserved = buf[7] — ignored (must accept 0)
    let body_len = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
    if body_len > MAX_BODY_LEN {
        return Err(format!("body_len {} exceeds max {}", body_len, MAX_BODY_LEN));
    }
    Ok((msg_type, body_len))
}

/// PREPARE / COMMIT / ABORT body: slot + length-prefixed target id.
pub fn encode_slot_target(slot: u16, target_id: &str) -> Vec<u8> {
    let id_bytes = target_id.as_bytes();
    let id_len = id_bytes.len() as u16;
    let mut out = Vec::with_capacity(2 + 2 + id_bytes.len());
    out.extend_from_slice(&slot.to_le_bytes());
    out.extend_from_slice(&id_len.to_le_bytes());
    out.extend_from_slice(id_bytes);
    out
}

/// Decode PREPARE / COMMIT / ABORT body.
pub fn decode_slot_target(body: &[u8]) -> Result<(u16, String), String> {
    if body.len() < 4 {
        return Err(format!("slot-target body too short: {}", body.len()));
    }
    let slot = u16::from_le_bytes([body[0], body[1]]);
    let id_len = u16::from_le_bytes([body[2], body[3]]) as usize;
    if body.len() < 4 + id_len {
        return Err(format!(
            "slot-target id truncated: need {} have {}",
            4 + id_len,
            body.len()
        ));
    }
    let id = String::from_utf8_lossy(&body[4..4 + id_len]).into_owned();
    Ok((slot, id))
}

/// ERR body: length-prefixed UTF-8 message.
pub fn encode_string_body(msg: &str) -> Vec<u8> {
    let b = msg.as_bytes();
    let len = b.len() as u16;
    let mut out = Vec::with_capacity(2 + b.len());
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(b);
    out
}

/// Decode length-prefixed string body (ERR / optional OK text).
pub fn decode_string_body(body: &[u8]) -> String {
    if body.len() < 2 {
        return String::from_utf8_lossy(body).into_owned();
    }
    let len = u16::from_le_bytes([body[0], body[1]]) as usize;
    if body.len() < 2 + len {
        return String::from_utf8_lossy(body).into_owned();
    }
    String::from_utf8_lossy(&body[2..2 + len]).into_owned()
}

/// Client-side bus RPC error: transport (fall back RESP) vs remote application.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BusRpcError {
    /// Connect / I/O / framing — caller should fall back to RESP.
    Transport(String),
    /// Peer returned ERR or rejected the op — do **not** silently RESP-retry
    /// the same logical op as if the bus were down (preserves inject / fences).
    Remote(String),
}

impl BusRpcError {
    pub fn is_transport(&self) -> bool {
        matches!(self, BusRpcError::Transport(_))
    }

    pub fn message(&self) -> &str {
        match self {
            BusRpcError::Transport(s) | BusRpcError::Remote(s) => s.as_str(),
        }
    }
}

impl std::fmt::Display for BusRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BusRpcError::Transport(s) => write!(f, "bus-transport:{}", s),
            BusRpcError::Remote(s) => write!(f, "{}", s),
        }
    }
}

/// Send one PREPARE / COMMIT / ABORT and wait for OK / ERR.
pub async fn bus_rpc(
    ip: &str,
    cport: u16,
    msg_type: u8,
    slot: u16,
    target_id: &str,
) -> Result<(), BusRpcError> {
    let addr = format!("{}:{}", ip, cport);
    let mut stream = match tokio::time::timeout(BUS_IO_TIMEOUT, TcpStream::connect(&addr)).await {
        Ok(Ok(s)) => {
            let _ = s.set_nodelay(true);
            s
        }
        Ok(Err(e)) => {
            return Err(BusRpcError::Transport(format!(
                "connect {}: {}",
                addr, e
            )));
        }
        Err(_) => {
            return Err(BusRpcError::Transport(format!(
                "connect timeout {}",
                addr
            )));
        }
    };

    let body = encode_slot_target(slot, target_id);
    let frame = encode_frame(msg_type, &body);
    match tokio::time::timeout(BUS_IO_TIMEOUT, stream.write_all(&frame)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            return Err(BusRpcError::Transport(format!("write: {}", e)));
        }
        Err(_) => return Err(BusRpcError::Transport("write timeout".into())),
    }

    let (rty, rbody) = read_frame(&mut stream).await?;
    match rty {
        TYPE_OK => Ok(()),
        TYPE_ERR => Err(BusRpcError::Remote(decode_string_body(&rbody))),
        other => Err(BusRpcError::Remote(format!(
            "unexpected bus reply type {}",
            other
        ))),
    }
}

/// Prefer-bus helpers used by dual-end reshard (Batch GP).
pub async fn bus_prepare(
    ip: &str,
    client_port: u16,
    slot: u16,
    target_id: &str,
) -> Result<(), BusRpcError> {
    bus_rpc(ip, peer_cport(client_port), TYPE_PREPARE, slot, target_id).await
}

pub async fn bus_commit(
    ip: &str,
    client_port: u16,
    slot: u16,
    target_id: &str,
) -> Result<(), BusRpcError> {
    bus_rpc(ip, peer_cport(client_port), TYPE_COMMIT, slot, target_id).await
}

pub async fn bus_abort(
    ip: &str,
    client_port: u16,
    slot: u16,
    target_id: &str,
) -> Result<(), BusRpcError> {
    bus_rpc(ip, peer_cport(client_port), TYPE_ABORT, slot, target_id).await
}

async fn read_frame(stream: &mut TcpStream) -> Result<(u8, Vec<u8>), BusRpcError> {
    let mut header = [0u8; HEADER_LEN];
    match tokio::time::timeout(BUS_IO_TIMEOUT, stream.read_exact(&mut header)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            return Err(BusRpcError::Transport(format!("read header: {}", e)));
        }
        Err(_) => return Err(BusRpcError::Transport("read header timeout".into())),
    }
    let (msg_type, body_len) =
        decode_header(&header).map_err(BusRpcError::Transport)?;
    let mut body = vec![0u8; body_len as usize];
    if body_len > 0 {
        match tokio::time::timeout(BUS_IO_TIMEOUT, stream.read_exact(&mut body)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                return Err(BusRpcError::Transport(format!("read body: {}", e)));
            }
            Err(_) => return Err(BusRpcError::Transport("read body timeout".into())),
        }
    }
    Ok((msg_type, body))
}

/// Accept loop on `client_port + 10000`. Soft-fails (logs + returns) if bind fails.
///
/// Handles PREPARE / COMMIT / ABORT via existing [`ClusterState`] prepare APIs
/// and replies OK / ERR. PING → PONG. Not a Redis cluster-bus reimplementation.
pub async fn run_cluster_bus(
    cluster: Arc<ClusterState>,
    mut shutdown: watch::Receiver<bool>,
) {
    let (ip, port) = cluster.bind_addr();
    let cport = peer_cport(port);
    let addr = format!("{}:{}", ip, cport);
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            warn!(
                "Cluster peer bus bind failed on {}: {}; dual-end NODE uses RESP only",
                addr, e
            );
            return;
        }
    };
    info!(
        "Cluster peer bus lite listening on {} (NODE 2PC only; not Redis bus)",
        addr
    );

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("Cluster peer bus shutting down");
                    break;
                }
            }
            accept = listener.accept() => {
                match accept {
                    Ok((stream, peer)) => {
                        let _ = stream.set_nodelay(true);
                        let cs = Arc::clone(&cluster);
                        tokio::spawn(async move {
                            if let Err(e) = handle_bus_conn(stream, cs).await {
                                debug!("cluster bus conn from {}: {}", peer, e);
                            }
                        });
                    }
                    Err(e) => {
                        warn!("cluster bus accept error: {}", e);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        }
    }
}

async fn handle_bus_conn(
    mut stream: TcpStream,
    cluster: Arc<ClusterState>,
) -> Result<(), String> {
    // Serve multiple RPCs on one connection (reshard may pipeline-ish).
    loop {
        let mut header = [0u8; HEADER_LEN];
        match stream.read_exact(&mut header).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(format!("read header: {}", e)),
        }
        let (msg_type, body_len) = decode_header(&header)?;
        let mut body = vec![0u8; body_len as usize];
        if body_len > 0 {
            stream
                .read_exact(&mut body)
                .await
                .map_err(|e| format!("read body: {}", e))?;
        }

        let reply = match msg_type {
            TYPE_PING => encode_frame(TYPE_PONG, &[]),
            TYPE_PREPARE => handle_prepare(&cluster, &body),
            TYPE_COMMIT => handle_commit(&cluster, &body),
            TYPE_ABORT => handle_abort(&cluster, &body),
            other => encode_frame(
                TYPE_ERR,
                &encode_string_body(&format!("unknown bus type {}", other)),
            ),
        };
        stream
            .write_all(&reply)
            .await
            .map_err(|e| format!("write reply: {}", e))?;
    }
}

fn handle_prepare(cluster: &ClusterState, body: &[u8]) -> Vec<u8> {
    match decode_slot_target(body) {
        Ok((slot, target)) => match cluster.set_prepare_node(slot, &target) {
            Ok(()) => encode_frame(TYPE_OK, &[]),
            Err(e) => encode_frame(TYPE_ERR, &encode_string_body(&e)),
        },
        Err(e) => encode_frame(TYPE_ERR, &encode_string_body(&e)),
    }
}

fn handle_commit(cluster: &ClusterState, body: &[u8]) -> Vec<u8> {
    match decode_slot_target(body) {
        Ok((slot, target)) => match cluster.commit_prepare_node(slot, &target) {
            Ok(()) => encode_frame(TYPE_OK, &[]),
            Err(e) => encode_frame(TYPE_ERR, &encode_string_body(&e)),
        },
        Err(e) => encode_frame(TYPE_ERR, &encode_string_body(&e)),
    }
}

fn handle_abort(cluster: &ClusterState, body: &[u8]) -> Vec<u8> {
    match decode_slot_target(body) {
        Ok((slot, _target)) => match cluster.abort_prepare_node(slot) {
            Ok(()) => encode_frame(TYPE_OK, &[]),
            Err(e) => encode_frame(TYPE_ERR, &encode_string_body(&e)),
        },
        Err(e) => encode_frame(TYPE_ERR, &encode_string_body(&e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::watch;

    #[test]
    fn encode_decode_header_roundtrip() {
        let body = encode_slot_target(42, "node-abc");
        let frame = encode_frame(TYPE_PREPARE, &body);
        assert_eq!(&frame[0..4], b"KORB");
        assert_eq!(frame[4], VERSION);
        assert_eq!(frame[5], TYPE_PREPARE);
        let (ty, blen) = decode_header(&frame).unwrap();
        assert_eq!(ty, TYPE_PREPARE);
        assert_eq!(blen as usize, body.len());
        let (slot, id) = decode_slot_target(&frame[HEADER_LEN..]).unwrap();
        assert_eq!(slot, 42);
        assert_eq!(id, "node-abc");
    }

    #[test]
    fn wrong_magic_rejected() {
        let mut frame = encode_frame(TYPE_PING, &[]);
        frame[0] = b'X';
        let err = decode_header(&frame).unwrap_err();
        assert!(
            err.contains("wrong magic"),
            "expected wrong magic, got {}",
            err
        );
    }

    #[test]
    fn string_body_roundtrip() {
        let b = encode_string_body("prepare vote expired");
        assert_eq!(decode_string_body(&b), "prepare vote expired");
    }

    #[test]
    fn empty_ping_frame_size() {
        let f = encode_frame(TYPE_PING, &[]);
        assert_eq!(f.len(), HEADER_LEN);
        let (ty, blen) = decode_header(&f).unwrap();
        assert_eq!(ty, TYPE_PING);
        assert_eq!(blen, 0);
    }

    /// Handler path: PREPARE then COMMIT applies ownership via ClusterState APIs.
    #[tokio::test]
    async fn bus_prepare_then_commit_applies_ownership() {
        let dest = ClusterState::single_node("127.0.0.1", 19001);
        let dest_id = dest.my_id();
        // Source-shaped peer known so prepare topology is ready when we own.
        // Dest already owns all slots in new_single_node — dest-side prepare
        // accepts when myself is the target (already owns / ready to take).
        dest.set_prepare_node(0, &dest_id).unwrap();
        assert!(dest.is_prepared(0));

        // Simulate bus COMMIT body handling.
        let body = encode_slot_target(0, &dest_id);
        let reply = handle_commit(&dest, &body);
        let (ty, blen) = decode_header(&reply).unwrap();
        assert_eq!(ty, TYPE_OK, "commit should OK");
        assert_eq!(blen, 0);
        assert_eq!(dest.owner_id_of(0).as_deref(), Some(dest_id.as_str()));
        assert!(!dest.is_prepared(0));
    }

    /// End-to-end: real TCP bus accept + client PREPARE/COMMIT.
    #[tokio::test]
    async fn bus_tcp_prepare_commit_e2e() {
        // Bind an ephemeral client port so cport = port+10000 is stable for this test.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let cluster = ClusterState::single_node("127.0.0.1", port);
        let dest_id = cluster.my_id();
        let (tx, rx) = watch::channel(false);
        let bus_cs = Arc::clone(&cluster);
        let handle = tokio::spawn(async move {
            run_cluster_bus(bus_cs, rx).await;
        });
        // Wait until bus accepts (retry connect).
        let mut ready = false;
        for _ in 0..50 {
            if TcpStream::connect(("127.0.0.1", peer_cport(port)))
                .await
                .is_ok()
            {
                ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(ready, "bus did not accept on cport {}", peer_cport(port));

        bus_prepare("127.0.0.1", port, 7, &dest_id)
            .await
            .expect("prepare");
        assert!(cluster.is_prepared(7));

        bus_commit("127.0.0.1", port, 7, &dest_id)
            .await
            .expect("commit");
        assert_eq!(cluster.owner_id_of(7).as_deref(), Some(dest_id.as_str()));
        assert!(!cluster.is_prepared(7));

        // PING / PONG path via raw frame.
        let mut s = TcpStream::connect(("127.0.0.1", peer_cport(port)))
            .await
            .unwrap();
        s.write_all(&encode_frame(TYPE_PING, &[])).await.unwrap();
        let mut hdr = [0u8; HEADER_LEN];
        s.read_exact(&mut hdr).await.unwrap();
        let (ty, blen) = decode_header(&hdr).unwrap();
        assert_eq!(ty, TYPE_PONG);
        assert_eq!(blen, 0);

        let _ = tx.send(true);
        let _ = handle.await;
    }

    #[tokio::test]
    async fn bus_transport_error_when_down() {
        // Nothing listening on this cport.
        let err = bus_prepare("127.0.0.1", 1, 0, "x")
            .await
            .unwrap_err();
        assert!(err.is_transport(), "expected transport, got {:?}", err);
    }
}
