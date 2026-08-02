//! Kore peer bus for dual-end NODE 2PC (Batch **GP**) and membership lite
//! (Batch **HA**).
//!
//! Length-prefixed binary frames on the Redis-style cluster bus port
//! (`client_port + 10000`). Still **not** the Redis binary cluster bus wire
//! (no Redis gossip packet layout); Kore uses magic `KORB` with its own types.
//!
//! Wire layout (little-endian body fields; magic is four ASCII bytes `KORB`):
//! ```text
//! magic u32 bytes = b"KORB"
//! version u8 = 1
//! type u8: PING=1 PONG=2 MEET=3 MEET_ACK=4 FAIL=5
//!          PREPARE=10 COMMIT=11 ABORT=12 OK=20 ERR=21
//! flags u8 = 0
//! reserved u8 = 0
//! body_len u32
//! body...
//! ```
//! PREPARE / COMMIT / ABORT body: `slot u16` + length-prefixed `target_id`.
//! MEET body: id, ip, port, role, master_id (length-prefixed strings + u16 port).
//! MEET_ACK body: peer id (length-prefixed).
//! PING body (HA): optional peer identity (length-prefixed node id); empty OK.
//! PONG body (HA): optional our node id.
//! FAIL body: failed node id (length-prefixed).
//!
//! Dual-end reshard prefers the bus when connect succeeds, and falls back to
//! the existing RESP `SETSLOT` path on transport failure (bus down / bind miss).
//! MEET / heartbeat prefer the bus then fall back to RESP (Batch HA).

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
/// Membership join request (Batch HA).
pub const TYPE_MEET: u8 = 3;
/// Membership join reply with peer id (Batch HA).
pub const TYPE_MEET_ACK: u8 = 4;
/// Announce that a node is failed (Batch HA).
pub const TYPE_FAIL: u8 = 5;
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

/// MEET announce payload (Batch HA).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeetBody {
    pub id: String,
    pub ip: String,
    pub port: u16,
    /// `"master"` or `"slave"`.
    pub role: String,
    /// Master id when role is slave; `"-"` when master.
    pub master_id: String,
}

fn write_lp_str(out: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    let len = b.len() as u16;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(b);
}

fn read_lp_str(body: &[u8], off: &mut usize) -> Result<String, String> {
    if *off + 2 > body.len() {
        return Err("meet body truncated (len)".into());
    }
    let len = u16::from_le_bytes([body[*off], body[*off + 1]]) as usize;
    *off += 2;
    if *off + len > body.len() {
        return Err("meet body truncated (data)".into());
    }
    let s = String::from_utf8_lossy(&body[*off..*off + len]).into_owned();
    *off += len;
    Ok(s)
}

/// Encode MEET body.
pub fn encode_meet_body(m: &MeetBody) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    write_lp_str(&mut out, &m.id);
    write_lp_str(&mut out, &m.ip);
    out.extend_from_slice(&m.port.to_le_bytes());
    write_lp_str(&mut out, &m.role);
    write_lp_str(&mut out, &m.master_id);
    out
}

/// Decode MEET body.
pub fn decode_meet_body(body: &[u8]) -> Result<MeetBody, String> {
    let mut off = 0;
    let id = read_lp_str(body, &mut off)?;
    let ip = read_lp_str(body, &mut off)?;
    if off + 2 > body.len() {
        return Err("meet body truncated (port)".into());
    }
    let port = u16::from_le_bytes([body[off], body[off + 1]]);
    off += 2;
    let role = read_lp_str(body, &mut off)?;
    let master_id = read_lp_str(body, &mut off)?;
    Ok(MeetBody {
        id,
        ip,
        port,
        role,
        master_id,
    })
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

/// MEET over the peer bus (Batch HA). Returns peer node id on success.
///
/// Caller should fall back to RESP `CLUSTER MEET` when this returns transport error.
pub async fn bus_meet(
    ip: &str,
    client_port: u16,
    announce: &MeetBody,
) -> Result<String, BusRpcError> {
    let cport = peer_cport(client_port);
    let addr = format!("{}:{}", ip, cport);
    let mut stream = match tokio::time::timeout(BUS_IO_TIMEOUT, TcpStream::connect(&addr)).await {
        Ok(Ok(s)) => {
            let _ = s.set_nodelay(true);
            s
        }
        Ok(Err(e)) => {
            return Err(BusRpcError::Transport(format!("connect {}: {}", addr, e)));
        }
        Err(_) => {
            return Err(BusRpcError::Transport(format!("connect timeout {}", addr)));
        }
    };
    let frame = encode_frame(TYPE_MEET, &encode_meet_body(announce));
    match tokio::time::timeout(BUS_IO_TIMEOUT, stream.write_all(&frame)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(BusRpcError::Transport(format!("write: {}", e))),
        Err(_) => return Err(BusRpcError::Transport("write timeout".into())),
    }
    let (rty, rbody) = read_frame(&mut stream).await?;
    match rty {
        TYPE_MEET_ACK => {
            let peer_id = decode_string_body(&rbody);
            if peer_id.is_empty() {
                Err(BusRpcError::Remote("empty MEET_ACK id".into()))
            } else {
                Ok(peer_id)
            }
        }
        TYPE_ERR => Err(BusRpcError::Remote(decode_string_body(&rbody))),
        other => Err(BusRpcError::Remote(format!(
            "unexpected MEET reply type {}",
            other
        ))),
    }
}

/// Identity PING over the bus (Batch HA). Returns peer id from PONG when present.
pub async fn bus_ping_id(
    ip: &str,
    client_port: u16,
    my_id: &str,
) -> Result<String, BusRpcError> {
    let cport = peer_cport(client_port);
    let addr = format!("{}:{}", ip, cport);
    let mut stream = match tokio::time::timeout(BUS_IO_TIMEOUT, TcpStream::connect(&addr)).await {
        Ok(Ok(s)) => {
            let _ = s.set_nodelay(true);
            s
        }
        Ok(Err(e)) => {
            return Err(BusRpcError::Transport(format!("connect {}: {}", addr, e)));
        }
        Err(_) => {
            return Err(BusRpcError::Transport(format!("connect timeout {}", addr)));
        }
    };
    let frame = encode_frame(TYPE_PING, &encode_string_body(my_id));
    match tokio::time::timeout(BUS_IO_TIMEOUT, stream.write_all(&frame)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(BusRpcError::Transport(format!("write: {}", e))),
        Err(_) => return Err(BusRpcError::Transport("write timeout".into())),
    }
    let (rty, rbody) = read_frame(&mut stream).await?;
    match rty {
        TYPE_PONG => Ok(decode_string_body(&rbody)),
        TYPE_ERR => Err(BusRpcError::Remote(decode_string_body(&rbody))),
        other => Err(BusRpcError::Remote(format!(
            "unexpected PING reply type {}",
            other
        ))),
    }
}

/// Announce FAIL for `failed_id` to a peer over the bus (best-effort Batch HA).
pub async fn bus_fail_announce(
    ip: &str,
    client_port: u16,
    failed_id: &str,
) -> Result<(), BusRpcError> {
    let cport = peer_cport(client_port);
    let addr = format!("{}:{}", ip, cport);
    let mut stream = match tokio::time::timeout(BUS_IO_TIMEOUT, TcpStream::connect(&addr)).await {
        Ok(Ok(s)) => {
            let _ = s.set_nodelay(true);
            s
        }
        Ok(Err(e)) => {
            return Err(BusRpcError::Transport(format!("connect {}: {}", addr, e)));
        }
        Err(_) => {
            return Err(BusRpcError::Transport(format!("connect timeout {}", addr)));
        }
    };
    let frame = encode_frame(TYPE_FAIL, &encode_string_body(failed_id));
    match tokio::time::timeout(BUS_IO_TIMEOUT, stream.write_all(&frame)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(BusRpcError::Transport(format!("write: {}", e))),
        Err(_) => return Err(BusRpcError::Transport("write timeout".into())),
    }
    let (rty, rbody) = read_frame(&mut stream).await?;
    match rty {
        TYPE_OK => Ok(()),
        TYPE_ERR => Err(BusRpcError::Remote(decode_string_body(&rbody))),
        other => Err(BusRpcError::Remote(format!(
            "unexpected FAIL reply type {}",
            other
        ))),
    }
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
        "Cluster peer bus listening on {} (NODE 2PC + MEET/PING/FAIL; KORB not Redis wire)",
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
            TYPE_PING => handle_ping(&cluster, &body),
            TYPE_MEET => handle_meet(&cluster, &body),
            TYPE_FAIL => handle_fail(&cluster, &body),
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

/// Identity PING: optional body = remote id (touch_pong); reply PONG with our id.
fn handle_ping(cluster: &ClusterState, body: &[u8]) -> Vec<u8> {
    if !body.is_empty() {
        let remote = decode_string_body(body);
        if !remote.is_empty() && remote != cluster.my_id() {
            // Known peer → refresh liveness; unknown id is ignored (MEET first).
            cluster.touch_pong(&remote);
        }
    }
    encode_frame(TYPE_PONG, &encode_string_body(&cluster.my_id()))
}

/// MEET: add peer from announce body; reply MEET_ACK with our id.
fn handle_meet(cluster: &ClusterState, body: &[u8]) -> Vec<u8> {
    match decode_meet_body(body) {
        Ok(m) => {
            if m.id == cluster.my_id() {
                return encode_frame(
                    TYPE_ERR,
                    &encode_string_body("ERR CLUSTER MEET would meet myself"),
                );
            }
            if m.id.is_empty() || m.ip.is_empty() || m.port == 0 {
                return encode_frame(
                    TYPE_ERR,
                    &encode_string_body("ERR invalid MEET body"),
                );
            }
            let role_master = Some(!m.role.eq_ignore_ascii_case("slave"));
            let role_master_id = if m.role.eq_ignore_ascii_case("slave") {
                let mid = if m.master_id == "-" || m.master_id.is_empty() {
                    None
                } else {
                    Some(m.master_id.clone())
                };
                Some(mid)
            } else {
                Some(None)
            };
            cluster.add_node_with_role(&m.id, &m.ip, m.port, role_master, role_master_id);
            cluster.touch_pong(&m.id);
            encode_frame(TYPE_MEET_ACK, &encode_string_body(&cluster.my_id()))
        }
        Err(e) => encode_frame(TYPE_ERR, &encode_string_body(&e)),
    }
}

/// FAIL announce: mark the named peer failed (Batch HA).
fn handle_fail(cluster: &ClusterState, body: &[u8]) -> Vec<u8> {
    let failed = decode_string_body(body);
    if failed.is_empty() || failed == cluster.my_id() {
        return encode_frame(
            TYPE_ERR,
            &encode_string_body("ERR invalid FAIL target"),
        );
    }
    cluster.mark_fail(&failed);
    encode_frame(TYPE_OK, &[])
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
        // Identity PONG body is non-empty (handled in e2e tests).
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

        // PING / PONG path via raw frame (Batch HA: PONG carries our node id).
        let mut s = TcpStream::connect(("127.0.0.1", peer_cport(port)))
            .await
            .unwrap();
        s.write_all(&encode_frame(TYPE_PING, &[])).await.unwrap();
        let mut hdr = [0u8; HEADER_LEN];
        s.read_exact(&mut hdr).await.unwrap();
        let (ty, blen) = decode_header(&hdr).unwrap();
        assert_eq!(ty, TYPE_PONG);
        let mut pong_body = vec![0u8; blen as usize];
        if blen > 0 {
            s.read_exact(&mut pong_body).await.unwrap();
        }
        assert_eq!(decode_string_body(&pong_body), dest_id);

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

    #[test]
    fn meet_body_roundtrip() {
        let m = MeetBody {
            id: "node-a".into(),
            ip: "10.0.0.1".into(),
            port: 7000,
            role: "master".into(),
            master_id: "-".into(),
        };
        let b = encode_meet_body(&m);
        let d = decode_meet_body(&b).unwrap();
        assert_eq!(d, m);
    }

    #[tokio::test]
    async fn bus_meet_and_identity_ping_e2e() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let cluster = ClusterState::single_node("127.0.0.1", port);
        let my_id = cluster.my_id();
        let (tx, rx) = watch::channel(false);
        let bus_cs = Arc::clone(&cluster);
        let handle = tokio::spawn(async move {
            run_cluster_bus(bus_cs, rx).await;
        });
        for _ in 0..50 {
            if TcpStream::connect(("127.0.0.1", peer_cport(port)))
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let announce = MeetBody {
            id: "peer-meet-1".into(),
            ip: "127.0.0.1".into(),
            port: 19999,
            role: "master".into(),
            master_id: "-".into(),
        };
        let ack_id = bus_meet("127.0.0.1", port, &announce)
            .await
            .expect("bus meet");
        assert_eq!(ack_id, my_id);
        // Peer should be in the table.
        let peers = cluster.peer_snapshots();
        assert!(
            peers.iter().any(|p| p.id == "peer-meet-1"),
            "expected peer after MEET: {:?}",
            peers
        );

        let pong_id = bus_ping_id("127.0.0.1", port, "peer-meet-1")
            .await
            .expect("bus ping");
        assert_eq!(pong_id, my_id);

        bus_fail_announce("127.0.0.1", port, "peer-meet-1")
            .await
            .expect("bus fail");
        assert!(
            cluster.peer_snapshots().iter().any(|p| p.id == "peer-meet-1" && p.fail),
            "peer should be fail after FAIL announce"
        );

        let _ = tx.send(true);
        let _ = handle.await;
    }
}
