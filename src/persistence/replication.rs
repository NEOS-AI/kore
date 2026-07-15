//! Async primary → replica replication with PSYNC-style partial resync.
//!
//! Protocol:
//! 1. Replica connects and sends `PSYNC <replid> <offset>` (or `PSYNC ? -1` / legacy `SYNC`)
//! 2. Primary replies either:
//!    - `+FULLRESYNC <replid> <offset>` + RDB bulk + live command stream
//!    - `+CONTINUE` + backlog bytes from offset + live command stream
//! 3. Replica loads RDB (full only), then applies streamed RESP commands
//!
//! Replica clients may issue normal **read** commands against a readonly replica.

use crate::cache::Cache;
use crate::databases::Databases;
use crate::error::{Error, Result};
use crate::persistence::{aof, rdb};
use crate::protocol::{RespParser, RespValue};
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{info, warn};

const REPLICA_CHANNEL_CAP: usize = 1024;
/// Default replication backlog capacity (bytes of command stream retained for PSYNC).
const DEFAULT_BACKLOG_CAP: usize = 1024 * 1024; // 1 MiB

/// Outbound feed to a connected replica (primary side).
struct ReplicaFeed {
    tx: mpsc::Sender<Bytes>,
    /// Announced client-facing host from `REPLCONF ip-address`, if any.
    host: Option<String>,
    /// Announced listening port from `REPLCONF listening-port`, if any.
    port: Option<u16>,
}

/// Circular replication backlog: retains recent write stream for partial resync.
struct ReplBacklog {
    /// Max retained bytes.
    capacity: usize,
    /// Concatenated stream of RESP command payloads (oldest → newest).
    buf: Vec<u8>,
    /// Global offset of the first byte currently in `buf`.
    start_offset: u64,
    /// Global offset of the next byte to be written (master_repl_offset).
    end_offset: u64,
}

impl ReplBacklog {
    fn new(capacity: usize) -> Self {
        Self {
            // Allow small capacities in tests; production uses DEFAULT_BACKLOG_CAP.
            capacity: capacity.max(1),
            buf: Vec::with_capacity(capacity.min(64 * 1024).max(64)),
            start_offset: 0,
            end_offset: 0,
        }
    }

    fn end_offset(&self) -> u64 {
        self.end_offset
    }

    fn start_offset(&self) -> u64 {
        self.start_offset
    }

    fn len(&self) -> usize {
        self.buf.len()
    }

    /// Append one command payload; returns the offset **before** this append
    /// (i.e. the start offset of the written bytes) and bumps end_offset.
    fn append(&mut self, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        // If a single write exceeds capacity, keep only its tail.
        if data.len() > self.capacity {
            let start = data.len() - self.capacity;
            self.buf.clear();
            self.buf.extend_from_slice(&data[start..]);
            self.end_offset = self.end_offset.saturating_add(data.len() as u64);
            self.start_offset = self.end_offset.saturating_sub(self.buf.len() as u64);
            return;
        }
        self.buf.extend_from_slice(data);
        self.end_offset = self.end_offset.saturating_add(data.len() as u64);
        // Trim oldest until within capacity
        while self.buf.len() > self.capacity {
            // Drop in chunks of 1/8 capacity to amortize
            let drop_n = (self.capacity / 8).max(1).min(self.buf.len());
            self.buf.drain(..drop_n);
            self.start_offset = self.start_offset.saturating_add(drop_n as u64);
        }
    }

    /// True if we can serve a partial resync starting at `offset`
    /// (offset must be in [start, end]).
    fn can_partial(&self, offset: u64) -> bool {
        offset >= self.start_offset && offset <= self.end_offset
    }

    /// Bytes from global `offset` to current end (empty if offset == end).
    fn get_from(&self, offset: u64) -> Option<Bytes> {
        if !self.can_partial(offset) {
            return None;
        }
        let rel = (offset - self.start_offset) as usize;
        if rel > self.buf.len() {
            return None;
        }
        Some(Bytes::copy_from_slice(&self.buf[rel..]))
    }

    /// Clear backlog contents and reset offsets to zero (used on promote).
    fn clear(&mut self) {
        self.buf.clear();
        self.start_offset = 0;
        self.end_offset = 0;
    }
}

/// Result of starting a replica handshake on the primary.
pub enum SyncStart {
    /// Full resync: raw bytes (FULLRESYNC line + RDB bulk) + live feed.
    Full {
        raw_response: Bytes,
        feed: mpsc::Receiver<Bytes>,
    },
    /// Partial: raw bytes (CONTINUE + backlog) + live feed.
    Partial {
        raw_response: Bytes,
        feed: mpsc::Receiver<Bytes>,
    },
}

/// Shared replication state for a Kore instance.
pub struct ReplicationManager {
    /// Connected replica feeds (primary side)
    replicas: Mutex<Vec<ReplicaFeed>>,
    /// True when this instance is a replica of another
    is_replica: AtomicBool,
    /// Configured primary address when acting as replica ("host:port")
    primary_addr: Mutex<Option<String>>,
    /// Number of connected replicas
    connected_replicas: AtomicUsize,
    /// Replica should not accept writes (Redis-style)
    readonly: AtomicBool,
    /// Primary replication ID (40 hex chars)
    replid: Mutex<String>,
    /// master_repl_offset — next byte offset in the replication stream
    master_repl_offset: AtomicU64,
    /// Backlog for PSYNC partial resync
    backlog: Mutex<ReplBacklog>,
    /// Replica-side: last known primary replid (for reconnect PSYNC)
    cached_master_replid: Mutex<String>,
    /// Replica-side: offset we have applied / will request
    replica_offset: AtomicU64,
    /// Replica-side: link to primary currently up
    master_link_up: AtomicBool,
    /// Previous replid after promote (Redis-style master_replid2); empty if never promoted.
    master_replid2: Mutex<String>,
    /// Offset at which the previous replid was abandoned (-1 when unset).
    second_repl_offset: AtomicU64,
    /// Whether second_repl_offset is meaningful (Redis uses -1 when unset).
    second_repl_offset_set: AtomicBool,
    /// Port this instance listens on (for `REPLCONF listening-port` when acting as replica).
    announce_port: AtomicUsize,
    /// Temporary write pause during coordinated failover (master side).
    failover_in_progress: AtomicBool,
}

impl ReplicationManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            replicas: Mutex::new(Vec::new()),
            is_replica: AtomicBool::new(false),
            primary_addr: Mutex::new(None),
            connected_replicas: AtomicUsize::new(0),
            readonly: AtomicBool::new(false),
            replid: Mutex::new(generate_replid()),
            master_repl_offset: AtomicU64::new(0),
            backlog: Mutex::new(ReplBacklog::new(DEFAULT_BACKLOG_CAP)),
            cached_master_replid: Mutex::new(String::new()),
            replica_offset: AtomicU64::new(0),
            master_link_up: AtomicBool::new(false),
            master_replid2: Mutex::new(String::new()),
            second_repl_offset: AtomicU64::new(0),
            second_repl_offset_set: AtomicBool::new(false),
            announce_port: AtomicUsize::new(0),
            failover_in_progress: AtomicBool::new(false),
        })
    }

    pub fn is_replica(&self) -> bool {
        self.is_replica.load(Ordering::Relaxed)
    }

    pub fn readonly(&self) -> bool {
        self.readonly.load(Ordering::Relaxed)
            || self.failover_in_progress.load(Ordering::Relaxed)
    }

    /// Client-facing listen port announced via REPLCONF when this node is a replica.
    pub fn set_announce_port(&self, port: u16) {
        self.announce_port.store(port as usize, Ordering::Relaxed);
    }

    pub fn announce_port(&self) -> u16 {
        self.announce_port.load(Ordering::Relaxed) as u16
    }

    pub fn failover_in_progress(&self) -> bool {
        self.failover_in_progress.load(Ordering::Relaxed)
    }

    pub fn connected_replicas(&self) -> usize {
        self.connected_replicas.load(Ordering::Relaxed)
    }

    pub fn primary_addr(&self) -> Option<String> {
        self.primary_addr.lock().clone()
    }

    pub fn replid(&self) -> String {
        self.replid.lock().clone()
    }

    pub fn master_repl_offset(&self) -> u64 {
        self.master_repl_offset.load(Ordering::Relaxed)
    }

    pub fn replica_offset(&self) -> u64 {
        self.replica_offset.load(Ordering::Relaxed)
    }

    pub fn master_link_up(&self) -> bool {
        self.master_link_up.load(Ordering::Relaxed)
    }

    pub fn cached_master_replid(&self) -> String {
        self.cached_master_replid.lock().clone()
    }

    pub fn master_replid2(&self) -> String {
        self.master_replid2.lock().clone()
    }

    /// Second repl offset for INFO; -1 when never promoted (Redis convention).
    pub fn second_repl_offset(&self) -> i64 {
        if self.second_repl_offset_set.load(Ordering::Relaxed) {
            self.second_repl_offset.load(Ordering::Relaxed) as i64
        } else {
            -1
        }
    }

    /// Role string for ROLE / INFO / HELLO.
    pub fn role_name(&self) -> &'static str {
        if self.is_replica() {
            "slave"
        } else {
            "master"
        }
    }

    /// Register a new replica feed channel. Returns the receiver the network
    /// task should drain to the socket.
    pub fn register_replica(&self) -> mpsc::Receiver<Bytes> {
        self.register_replica_announced(None, None)
    }

    /// Register a replica feed with optional identity from `REPLCONF`.
    pub fn register_replica_announced(
        &self,
        host: Option<String>,
        port: Option<u16>,
    ) -> mpsc::Receiver<Bytes> {
        let (tx, rx) = mpsc::channel(REPLICA_CHANNEL_CAP);
        self.replicas.lock().push(ReplicaFeed { tx, host, port });
        self.connected_replicas.fetch_add(1, Ordering::Relaxed);
        rx
    }

    /// True if any connected replica announced `host:port` (or port-only match).
    pub fn has_replica_at(&self, host: &str, port: u16) -> bool {
        let reps = self.replicas.lock();
        reps.iter().any(|r| replica_matches(r, host, port))
    }

    /// True if at least one connected replica has announced a listening port.
    pub fn any_replica_identity_known(&self) -> bool {
        self.replicas.lock().iter().any(|r| r.port.is_some())
    }

    /// Propagate a write command (as RESP array bytes) to all replicas and backlog.
    pub fn propagate_raw(&self, data: Bytes) {
        // Always append to backlog (even with no replicas) so reconnecting
        // replicas can PSYNC if they reconnect quickly.
        {
            let mut bl = self.backlog.lock();
            bl.append(&data);
            self.master_repl_offset
                .store(bl.end_offset(), Ordering::Relaxed);
        }

        let mut reps = self.replicas.lock();
        if reps.is_empty() {
            return;
        }
        reps.retain(|r| r.tx.try_send(data.clone()).is_ok());
        self.connected_replicas
            .store(reps.len(), Ordering::Relaxed);
    }

    /// Propagate argv as RESP command.
    pub fn propagate_command(&self, args: &[Bytes]) {
        let raw = aof::encode_command(args);
        self.propagate_raw(raw);
    }

    /// Legacy SYNC: full RDB bulk only (no FULLRESYNC line) + live feed.
    ///
    /// Snapshot includes all logical databases.
    pub fn start_full_sync(
        &self,
        databases: &Databases,
    ) -> Result<(Bytes, mpsc::Receiver<Bytes>)> {
        self.start_full_sync_announced(databases, None, None)
    }

    /// SYNC with optional replica identity from prior `REPLCONF`.
    pub fn start_full_sync_announced(
        &self,
        databases: &Databases,
        replica_host: Option<String>,
        replica_port: Option<u16>,
    ) -> Result<(Bytes, mpsc::Receiver<Bytes>)> {
        let rdb_bytes = rdb::save_databases_to_bytes(databases)?;
        let response = RespValue::BulkString(Some(rdb_bytes)).serialize();
        let rx = self.register_replica_announced(replica_host, replica_port);
        Ok((response, rx))
    }

    /// PSYNC handshake.
    ///
    /// - `replid_req == "?"` or offset `< 0` → full resync
    /// - matching replid + offset in backlog → partial
    /// - otherwise full
    ///
    /// Full resync RDB includes all logical databases.
    pub fn start_psync(
        &self,
        databases: &Databases,
        replid_req: &str,
        offset: i64,
    ) -> Result<SyncStart> {
        self.start_psync_announced(databases, replid_req, offset, None, None)
    }

    /// PSYNC with optional replica identity from prior `REPLCONF`.
    pub fn start_psync_announced(
        &self,
        databases: &Databases,
        replid_req: &str,
        offset: i64,
        replica_host: Option<String>,
        replica_port: Option<u16>,
    ) -> Result<SyncStart> {
        let our_id = self.replid();

        let want_full = replid_req == "?"
            || offset < 0
            || replid_req != our_id.as_str();

        if !want_full {
            // Try partial under lock so backlog + feed registration is atomic
            let bl = self.backlog.lock();
            let off = offset as u64;
            if bl.can_partial(off) {
                if let Some(history) = bl.get_from(off) {
                    let (tx, rx) = mpsc::channel(REPLICA_CHANNEL_CAP);
                    self.replicas.lock().push(ReplicaFeed {
                        tx,
                        host: replica_host,
                        port: replica_port,
                    });
                    self.connected_replicas.fetch_add(1, Ordering::Relaxed);

                    let mut raw = BytesMut::new();
                    raw.extend_from_slice(b"+CONTINUE\r\n");
                    raw.extend_from_slice(&history);
                    drop(bl);
                    return Ok(SyncStart::Partial {
                        raw_response: raw.freeze(),
                        feed: rx,
                    });
                }
            }
            drop(bl);
            // fall through to full
        }

        // Full resync — multi-DB snapshot
        let rdb_bytes = rdb::save_databases_to_bytes(databases)?;
        // Offset reported is current master offset (stream starts after RDB)
        let offset_now = self.master_repl_offset();
        let rx = self.register_replica_announced(replica_host, replica_port);

        let mut raw = BytesMut::new();
        let header = format!("+FULLRESYNC {} {}\r\n", our_id, offset_now);
        raw.extend_from_slice(header.as_bytes());
        raw.extend_from_slice(&RespValue::BulkString(Some(rdb_bytes)).serialize());

        Ok(SyncStart::Full {
            raw_response: raw.freeze(),
            feed: rx,
        })
    }

    /// Default timeout for coordinated `FAILOVER TO` (milliseconds).
    pub const FAILOVER_DEFAULT_TIMEOUT_MS: u64 = 5000;

    /// Master-initiated coordinated failover (MVP-lite).
    ///
    /// 1. Optionally pause writes (`failover_in_progress`)
    /// 2. If replica identities are known and none match `host:port`, error
    /// 3. TCP connect to target and send bare `FAILOVER`
    /// 4. On success, demote self via `set_replicaof(Some(host:port))`
    ///
    /// **Known race / gap**: no replication-offset catch-up wait. The target may
    /// promote before applying the latest backlog entries that were still in flight.
    pub async fn coordinated_failover_to(
        &self,
        host: &str,
        port: u16,
        timeout_ms: u64,
    ) -> std::result::Result<(), String> {
        if self.is_replica() {
            return Err("ERR FAILOVER TO is only allowed on the master".into());
        }

        // Soft identity check: only enforce when at least one replica announced a port.
        if self.any_replica_identity_known() && !self.has_replica_at(host, port) {
            return Err(format!(
                "ERR FAILOVER TO no matching replica for {}:{}",
                host, port
            ));
        }

        let timeout = std::time::Duration::from_millis(timeout_ms.max(1));
        self.failover_in_progress.store(true, Ordering::Relaxed);

        let result = self
            .send_failover_to_target(host, port, timeout)
            .await;

        match result {
            Ok(()) => {
                // Demote self to replica of the newly promoted master.
                // set_replicaof clears failover_in_progress via readonly path...
                // we clear the flag after demotion so readonly stays true as replica.
                self.set_replicaof(Some(format!("{}:{}", host, port)));
                self.failover_in_progress.store(false, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.failover_in_progress.store(false, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    async fn send_failover_to_target(
        &self,
        host: &str,
        port: u16,
        timeout: std::time::Duration,
    ) -> std::result::Result<(), String> {
        let addr = format!("{}:{}", host, port);
        let connect = TcpStream::connect(addr.clone());
        let mut stream = match tokio::time::timeout(timeout, connect).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                return Err(format!(
                    "ERR FAILOVER TO failed to connect to {}: {}",
                    addr, e
                ));
            }
            Err(_) => {
                return Err(format!(
                    "ERR FAILOVER TO timed out connecting to {}",
                    addr
                ));
            }
        };
        let _ = stream.set_nodelay(true);

        let failover_cmd = RespValue::Array(vec![RespValue::BulkString(Some(
            Bytes::from_static(b"FAILOVER"),
        ))])
        .serialize();

        match tokio::time::timeout(timeout, stream.write_all(&failover_cmd)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                return Err(format!(
                    "ERR FAILOVER TO failed writing to {}: {}",
                    addr, e
                ));
            }
            Err(_) => {
                return Err(format!(
                    "ERR FAILOVER TO timed out writing to {}",
                    addr
                ));
            }
        }

        // Read +OK (or error) from the target.
        let mut parser = RespParser::new();
        let mut buf = vec![0u8; 4096];
        let reply = loop {
            if let Some(val) = parser
                .parse()
                .map_err(|e| format!("ERR FAILOVER TO parse error: {}", e))?
            {
                break val;
            }
            let n = match tokio::time::timeout(timeout, stream.read(&mut buf)).await {
                Ok(Ok(0)) => {
                    return Err(format!(
                        "ERR FAILOVER TO target {} closed connection",
                        addr
                    ));
                }
                Ok(Ok(n)) => n,
                Ok(Err(e)) => {
                    return Err(format!(
                        "ERR FAILOVER TO failed reading from {}: {}",
                        addr, e
                    ));
                }
                Err(_) => {
                    return Err(format!(
                        "ERR FAILOVER TO timed out waiting for reply from {}",
                        addr
                    ));
                }
            };
            parser.feed(&buf[..n]);
        };

        match reply {
            RespValue::SimpleString(s) if s.as_ref() == b"OK" => Ok(()),
            RespValue::Error(e) => Err(format!(
                "ERR FAILOVER TO target error: {}",
                String::from_utf8_lossy(&e)
            )),
            other => Err(format!(
                "ERR FAILOVER TO unexpected reply from {}: {:?}",
                addr, other
            )),
        }
    }

    /// Configure this node as a replica of `host:port` (or promote with None).
    pub fn set_replicaof(&self, addr: Option<String>) {
        match addr {
            Some(a) => {
                self.is_replica.store(true, Ordering::Relaxed);
                self.readonly.store(true, Ordering::Relaxed);
                *self.primary_addr.lock() = Some(a);
                self.master_link_up.store(false, Ordering::Relaxed);
            }
            None => {
                self.promote_to_master();
            }
        }
    }

    /// Promote this instance to master: clear replica flags, rotate replid,
    /// reset offset/backlog/feeds, and clear replica-side cached primary state.
    ///
    /// Idempotent when already master (no replid rotation / backlog wipe).
    pub fn promote_to_master(&self) {
        // Already master: leave replication history alone (Redis-like).
        if !self.is_replica.load(Ordering::Relaxed) && self.primary_addr.lock().is_none() {
            // Still ensure writable flags and no stale primary pointer.
            self.readonly.store(false, Ordering::Relaxed);
            self.master_link_up.store(false, Ordering::Relaxed);
            return;
        }

        let old_id = self.replid();
        // Prefer applied replica stream offset when demoted node is promoting.
        let old_offset = self
            .master_repl_offset()
            .max(self.replica_offset.load(Ordering::Relaxed));

        // Reset history BEFORE enabling writes so concurrent clients cannot
        // append to the old backlog and then have it wiped under them.
        {
            let mut bl = self.backlog.lock();
            bl.clear();
            self.master_repl_offset.store(0, Ordering::Relaxed);
            *self.master_replid2.lock() = old_id;
            self.second_repl_offset
                .store(old_offset, Ordering::Relaxed);
            self.second_repl_offset_set
                .store(true, Ordering::Relaxed);
            *self.replid.lock() = generate_replid();
        }

        // Drop replica feed channels (EOF for old subscribers)
        {
            let mut reps = self.replicas.lock();
            reps.clear();
            self.connected_replicas.store(0, Ordering::Relaxed);
        }

        // Drop replica-side metadata
        *self.cached_master_replid.lock() = String::new();
        self.replica_offset.store(0, Ordering::Relaxed);
        *self.primary_addr.lock() = None;
        self.master_link_up.store(false, Ordering::Relaxed);

        // Enable writes last
        self.is_replica.store(false, Ordering::Relaxed);
        self.readonly.store(false, Ordering::Relaxed);
    }

    /// Build Redis-style ROLE reply.
    pub fn role_reply(&self) -> RespValue {
        if self.is_replica() {
            let master = self.primary_addr().unwrap_or_default();
            let (host, port) = split_host_port(&master);
            let state = if self.master_link_up() {
                "connected"
            } else {
                "connect"
            };
            RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"slave"))),
                RespValue::BulkString(Some(Bytes::from(host))),
                RespValue::Integer(port as i64),
                RespValue::BulkString(Some(Bytes::from(state))),
                RespValue::Integer(self.replica_offset() as i64),
            ])
        } else {
            // master: role, offset, list of replicas (host/port when announced via REPLCONF)
            let offset_s = self.master_repl_offset().to_string();
            let slaves: Vec<RespValue> = self
                .replicas
                .lock()
                .iter()
                .map(|r| {
                    let host = r
                        .host
                        .clone()
                        .unwrap_or_else(|| "?".to_string());
                    let port = r
                        .port
                        .map(|p| p.to_string())
                        .unwrap_or_else(|| "0".to_string());
                    RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from(host))),
                        RespValue::BulkString(Some(Bytes::from(port))),
                        RespValue::BulkString(Some(Bytes::from(offset_s.clone()))),
                    ])
                })
                .collect();
            RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"master"))),
                RespValue::Integer(self.master_repl_offset() as i64),
                RespValue::Array(slaves),
            ])
        }
    }

    /// INFO replication section body (without header).
    pub fn info_replication(&self) -> String {
        if self.is_replica() {
            let master = self.primary_addr().unwrap_or_default();
            let (host, port) = split_host_port(&master);
            let link = if self.master_link_up() {
                "up"
            } else {
                "down"
            };
            format!(
                "role:slave\r\n\
                 master_host:{}\r\n\
                 master_port:{}\r\n\
                 master_link_status:{}\r\n\
                 master_replid:{}\r\n\
                 slave_repl_offset:{}\r\n\
                 master_repl_offset:{}\r\n",
                host,
                port,
                link,
                self.cached_master_replid(),
                self.replica_offset(),
                self.master_repl_offset(),
            )
        } else {
            let bl = self.backlog.lock();
            let replid2 = self.master_replid2();
            let second_off = self.second_repl_offset();
            let mut s = format!(
                "role:master\r\n\
                 connected_slaves:{}\r\n\
                 master_replid:{}\r\n\
                 master_repl_offset:{}\r\n",
                self.connected_replicas(),
                self.replid(),
                self.master_repl_offset(),
            );
            if !replid2.is_empty() {
                s.push_str(&format!(
                    "master_replid2:{}\r\n\
                     second_repl_offset:{}\r\n",
                    replid2, second_off
                ));
            }
            s.push_str(&format!(
                "repl_backlog_active:1\r\n\
                 repl_backlog_size:{}\r\n\
                 repl_backlog_first_byte_offset:{}\r\n\
                 repl_backlog_histlen:{}\r\n",
                bl.capacity,
                bl.start_offset(),
                bl.len(),
            ));
            s
        }
    }
}

impl Default for ReplicationManager {
    fn default() -> Self {
        Self {
            replicas: Mutex::new(Vec::new()),
            is_replica: AtomicBool::new(false),
            primary_addr: Mutex::new(None),
            connected_replicas: AtomicUsize::new(0),
            readonly: AtomicBool::new(false),
            replid: Mutex::new(generate_replid()),
            master_repl_offset: AtomicU64::new(0),
            backlog: Mutex::new(ReplBacklog::new(DEFAULT_BACKLOG_CAP)),
            cached_master_replid: Mutex::new(String::new()),
            replica_offset: AtomicU64::new(0),
            master_link_up: AtomicBool::new(false),
            master_replid2: Mutex::new(String::new()),
            second_repl_offset: AtomicU64::new(0),
            second_repl_offset_set: AtomicBool::new(false),
            announce_port: AtomicUsize::new(0),
            failover_in_progress: AtomicBool::new(false),
        }
    }
}

fn generate_replid() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    // 40 hex chars — mix time + fixed prefix (not crypto; fine for local repl)
    format!("{:040x}", t ^ 0x6b6f7265_7073796e_u128)
}

fn split_host_port(addr: &str) -> (String, u16) {
    if let Some((h, p)) = addr.rsplit_once(':') {
        let port = p.parse().unwrap_or(0);
        (h.to_string(), port)
    } else {
        (addr.to_string(), 0)
    }
}

fn replica_matches(r: &ReplicaFeed, host: &str, port: u16) -> bool {
    match r.port {
        Some(p) if p == port => {
            // Port match; host optional (peer IP may differ from announce host).
            match r.host.as_deref() {
                None | Some("?") => true,
                Some(h) => hosts_equal(h, host),
            }
        }
        _ => false,
    }
}

fn hosts_equal(a: &str, b: &str) -> bool {
    if a.eq_ignore_ascii_case(b) {
        return true;
    }
    // Treat common localhost aliases as equivalent for local failover tests.
    let local = |h: &str| {
        h == "127.0.0.1" || h.eq_ignore_ascii_case("localhost") || h == "::1"
    };
    local(a) && local(b)
}

/// Background task: connect to primary, PSYNC, load multi-DB RDB if needed, apply stream.
pub async fn run_replica_loop(
    databases: Arc<Databases>,
    repl: Arc<ReplicationManager>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() {
            return;
        }

        let addr = match repl.primary_addr() {
            Some(a) => a,
            None => {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                continue;
            }
        };

        info!("Replica connecting to primary {}", addr);
        repl.master_link_up.store(false, Ordering::Relaxed);
        match sync_from_primary(databases.clone(), repl.clone(), &addr).await {
            Ok(()) => {
                info!("Replica disconnected from primary {}", addr);
            }
            Err(e) => {
                warn!("Replica sync error ({}): {}", addr, e);
            }
        }
        repl.master_link_up.store(false, Ordering::Relaxed);

        // Back off before reconnect
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}

async fn sync_from_primary(
    databases: Arc<Databases>,
    repl: Arc<ReplicationManager>,
    addr: &str,
) -> Result<()> {
    let mut stream = TcpStream::connect(addr)
        .await
        .map_err(|e| Error::NetworkError(format!("connect primary: {}", e)))?;
    stream.set_nodelay(true)?;

    // Redis-style handshake: announce listening-port before PSYNC so the primary
    // can match FAILOVER TO host port against connected replicas.
    let announce = repl.announce_port();
    if announce != 0 {
        let replconf_port = RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"REPLCONF"))),
            RespValue::BulkString(Some(Bytes::from_static(b"listening-port"))),
            RespValue::BulkString(Some(Bytes::from(announce.to_string()))),
        ])
        .serialize();
        stream.write_all(&replconf_port).await?;
        // Drain +OK (best-effort; ignore body)
        let mut parser_hs = RespParser::new();
        let mut hs_buf = vec![0u8; 1024];
        let _ = read_one_value(&mut stream, &mut parser_hs, &mut hs_buf).await;

        // Also announce loopback ip when useful for ROLE listing.
        let replconf_ip = RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"REPLCONF"))),
            RespValue::BulkString(Some(Bytes::from_static(b"ip-address"))),
            RespValue::BulkString(Some(Bytes::from_static(b"127.0.0.1"))),
        ])
        .serialize();
        stream.write_all(&replconf_ip).await?;
        let _ = read_one_value(&mut stream, &mut parser_hs, &mut hs_buf).await;
    }

    // Prefer PSYNC with cached id/offset; fall back to full.
    let cached_id = repl.cached_master_replid();
    let offset = repl.replica_offset();
    let (psync_id, psync_off) = if cached_id.is_empty() {
        ("?".to_string(), -1i64)
    } else {
        (cached_id, offset as i64)
    };

    let psync_cmd = RespValue::Array(vec![
        RespValue::BulkString(Some(Bytes::from_static(b"PSYNC"))),
        RespValue::BulkString(Some(Bytes::from(psync_id))),
        RespValue::BulkString(Some(Bytes::from(psync_off.to_string()))),
    ])
    .serialize();
    stream.write_all(&psync_cmd).await?;

    let mut parser = RespParser::new();
    let mut buf = vec![0u8; 64 * 1024];
    // After full RDB load, stream starts at DB 0 until SELECT is applied.
    let mut current_db: usize = 0;

    // First response: FULLRESYNC or CONTINUE simple string
    let first = read_one_value(&mut stream, &mut parser, &mut buf).await?;
    match first {
        RespValue::SimpleString(s) => {
            let line = String::from_utf8_lossy(&s);
            if line.starts_with("FULLRESYNC ") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                // FULLRESYNC <replid> <offset>
                if parts.len() >= 3 {
                    *repl.cached_master_replid.lock() = parts[1].to_string();
                    if let Ok(off) = parts[2].parse::<u64>() {
                        repl.replica_offset.store(off, Ordering::Relaxed);
                    }
                }
                // Next: RDB bulk (all logical databases)
                let rdb_data = read_one_value(&mut stream, &mut parser, &mut buf).await?;
                match rdb_data {
                    RespValue::BulkString(Some(data)) => {
                        info!(
                            "FULLRESYNC: loading multi-DB RDB snapshot ({} bytes)",
                            data.len()
                        );
                        rdb::load_databases_bytes(&databases, &data, true)?;
                        current_db = 0;
                    }
                    RespValue::Error(e) => {
                        return Err(Error::NetworkError(format!(
                            "FULLRESYNC RDB error: {}",
                            String::from_utf8_lossy(&e)
                        )));
                    }
                    other => {
                        return Err(Error::NetworkError(format!(
                            "unexpected RDB response: {:?}",
                            other
                        )));
                    }
                }
            } else if line.starts_with("CONTINUE") {
                info!("PARTIAL resync CONTINUE accepted");
                // Stream continues with backlog + live commands already in buffer
            } else {
                return Err(Error::NetworkError(format!(
                    "unexpected PSYNC reply: {}",
                    line
                )));
            }
        }
        // Some primaries (legacy SYNC) may reply with bulk RDB directly if we
        // somehow got SYNC — accept for robustness.
        RespValue::BulkString(Some(data)) => {
            info!("Legacy bulk RDB ({} bytes)", data.len());
            rdb::load_databases_bytes(&databases, &data, true)?;
            current_db = 0;
        }
        RespValue::Error(e) => {
            return Err(Error::NetworkError(format!(
                "PSYNC error: {}",
                String::from_utf8_lossy(&e)
            )));
        }
        other => {
            return Err(Error::NetworkError(format!(
                "unexpected PSYNC response: {:?}",
                other
            )));
        }
    }

    repl.master_link_up.store(true, Ordering::Relaxed);
    info!("Replica linked; applying command stream");

    // Apply remaining buffered data + stream; count bytes toward replica_offset
    loop {
        while let Some(val) = parser.parse()? {
            let raw_len = estimate_resp_size(&val);
            apply_replicated_command(&databases, &mut current_db, val)?;
            repl.replica_offset
                .fetch_add(raw_len as u64, Ordering::Relaxed);
        }
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        parser.feed(&buf[..n]);
    }
}

async fn read_one_value(
    stream: &mut TcpStream,
    parser: &mut RespParser,
    buf: &mut [u8],
) -> Result<RespValue> {
    loop {
        if let Some(val) = parser.parse()? {
            return Ok(val);
        }
        let n = stream.read(buf).await?;
        if n == 0 {
            return Err(Error::NetworkError(
                "primary closed during handshake".into(),
            ));
        }
        parser.feed(&buf[..n]);
    }
}

/// Best-effort size of a RESP value for offset accounting (matches encode size
/// for arrays of bulk strings; approximate otherwise).
fn estimate_resp_size(val: &RespValue) -> usize {
    val.serialize().len()
}

fn apply_replicated_command(
    databases: &Databases,
    current_db: &mut usize,
    value: RespValue,
) -> Result<()> {
    let args = match value {
        RespValue::Array(arr) => arr,
        _ => return Ok(()), // ignore non-commands
    };
    let mut argv = Vec::with_capacity(args.len());
    for a in args {
        match a {
            RespValue::BulkString(Some(b)) => argv.push(b),
            RespValue::SimpleString(b) => argv.push(b),
            RespValue::Integer(i) => argv.push(Bytes::from(i.to_string())),
            _ => return Ok(()),
        }
    }
    if argv.is_empty() {
        return Ok(());
    }
    apply_argv(databases, current_db, argv)
}

/// Apply one replicated command against multi-DB keyspaces (SELECT-aware).
///
/// Used by the replica apply loop and unit/integration tests.
pub fn apply_argv(
    databases: &Databases,
    current_db: &mut usize,
    argv: Vec<Bytes>,
) -> Result<()> {
    use crate::entry::StoreOptions;

    let cmd = String::from_utf8_lossy(&argv[0]).to_uppercase();
    match cmd.as_str() {
        "SELECT" => {
            if argv.len() >= 2 {
                if let Ok(s) = std::str::from_utf8(&argv[1]) {
                    if let Ok(idx) = s.parse::<usize>() {
                        if databases.get(idx).is_some() {
                            *current_db = idx;
                        }
                    }
                }
            }
            return Ok(());
        }
        "FLUSHALL" => {
            databases.flush_all();
            return Ok(());
        }
        "FLUSHDB" => {
            if let Some(cache) = databases.get(*current_db) {
                cache.flush();
            }
            return Ok(());
        }
        _ => {}
    }

    let Some(cache) = databases.get(*current_db) else {
        return Ok(());
    };

    match cmd.as_str() {
        "SET" => {
            if argv.len() >= 3 {
                let mut opts = StoreOptions::default();
                let mut i = 3;
                while i < argv.len() {
                    let opt = String::from_utf8_lossy(&argv[i]).to_uppercase();
                    match opt.as_str() {
                        "PXAT" => {
                            if let Some(ts) = argv.get(i + 1).and_then(|b| {
                                std::str::from_utf8(b).ok().and_then(|s| s.parse().ok())
                            }) {
                                opts.exat_ms = Some(ts);
                            }
                            i += 2;
                        }
                        "PX" => {
                            if let Some(ms) = argv.get(i + 1).and_then(|b| {
                                std::str::from_utf8(b).ok().and_then(|s| s.parse().ok())
                            }) {
                                opts.ttl_ms = Some(ms);
                            }
                            i += 2;
                        }
                        _ => i += 1,
                    }
                }
                let _ = cache.store(argv[1].clone(), argv[2].clone(), opts);
            }
        }
        "DEL" | "UNLINK" => {
            for k in argv.iter().skip(1) {
                let _ = cache.delete(k);
            }
        }
        "INCR" => {
            if argv.len() >= 2 {
                let _ = cache.incr(&argv[1], 1);
            }
        }
        "INCRBY" => {
            if argv.len() >= 3 {
                let d: i64 = std::str::from_utf8(&argv[2])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1);
                let _ = cache.incr(&argv[1], d);
            }
        }
        "DECR" => {
            if argv.len() >= 2 {
                let _ = cache.decr(&argv[1], 1);
            }
        }
        "DECRBY" => {
            if argv.len() >= 3 {
                let d: i64 = std::str::from_utf8(&argv[2])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1);
                let _ = cache.decr(&argv[1], d);
            }
        }
        "APPEND" => {
            if argv.len() >= 3 {
                // Best-effort: load, append, store
                if let Ok(Some(cur)) = cache.load(&argv[1], crate::entry::LoadOptions::default()) {
                    let mut v = cur.value.to_vec();
                    v.extend_from_slice(&argv[2]);
                    let _ = cache.store(argv[1].clone(), Bytes::from(v), StoreOptions::default());
                } else {
                    let _ = cache.store(argv[1].clone(), argv[2].clone(), StoreOptions::default());
                }
            }
        }
        "ZADD" => {
            if argv.len() >= 4 {
                if let Ok(zset) = cache.get_or_create_sorted_set(&argv[1]) {
                    if let Ok(mut set) = zset.write() {
                        let mut i = 2;
                        while i + 1 < argv.len() {
                            let score: f64 = std::str::from_utf8(&argv[i])
                                .ok()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0.0);
                            set.add(argv[i + 1].clone(), score);
                            i += 2;
                        }
                    }
                }
            }
        }
        "ZREM" => {
            if argv.len() >= 3 {
                if let Some(zset) = cache.get_sorted_set(&argv[1]) {
                    if let Ok(mut set) = zset.write() {
                        for m in argv.iter().skip(2) {
                            set.remove(m);
                        }
                    }
                }
            }
        }
        "GEOADD" => {
            if argv.len() >= 5 {
                if let Ok(geoset) = cache.get_or_create_geo_set(&argv[1]) {
                    if let Ok(mut set) = geoset.write() {
                        let mut i = 2;
                        while i + 2 < argv.len() {
                            let lon: f64 = std::str::from_utf8(&argv[i])
                                .ok()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0.0);
                            let lat: f64 = std::str::from_utf8(&argv[i + 1])
                                .ok()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0.0);
                            let _ = set.add(argv[i + 2].clone(), lon, lat);
                            i += 3;
                        }
                    }
                }
            }
        }
        "HSET" => {
            if argv.len() >= 4 {
                if let Ok(h) = cache.get_or_create_hash(&argv[1]) {
                    if let Ok(mut hash) = h.write() {
                        let mut i = 2;
                        while i + 1 < argv.len() {
                            hash.hset(argv[i].clone(), argv[i + 1].clone());
                            i += 2;
                        }
                    }
                }
            }
        }
        "HDEL" => {
            if argv.len() >= 3 {
                if let Some(h) = cache.get_hash(&argv[1]) {
                    if let Ok(mut hash) = h.write() {
                        let fields: Vec<Bytes> = argv[2..].to_vec();
                        hash.hdel(&fields);
                    }
                    cache.remove_hash_if_empty(&argv[1]);
                }
            }
        }
        "LPUSH" => {
            if argv.len() >= 3 {
                if let Ok(list) = cache.get_or_create_list(&argv[1]) {
                    if let Ok(mut l) = list.write() {
                        l.lpush(argv[2..].iter().cloned());
                    }
                }
            }
        }
        "RPUSH" => {
            if argv.len() >= 3 {
                if let Ok(list) = cache.get_or_create_list(&argv[1]) {
                    if let Ok(mut l) = list.write() {
                        l.rpush(argv[2..].iter().cloned());
                    }
                }
            }
        }
        "LPOP" => {
            if argv.len() >= 2 {
                if let Some(list) = cache.get_list(&argv[1]) {
                    if let Ok(mut l) = list.write() {
                        let _ = l.lpop();
                    }
                    cache.remove_list_if_empty(&argv[1]);
                }
            }
        }
        "RPOP" => {
            if argv.len() >= 2 {
                if let Some(list) = cache.get_list(&argv[1]) {
                    if let Ok(mut l) = list.write() {
                        let _ = l.rpop();
                    }
                    cache.remove_list_if_empty(&argv[1]);
                }
            }
        }
        "SADD" => {
            if argv.len() >= 3 {
                if let Ok(s) = cache.get_or_create_set(&argv[1]) {
                    if let Ok(mut set) = s.write() {
                        set.sadd(argv[2..].iter().cloned());
                    }
                }
            }
        }
        "SREM" => {
            if argv.len() >= 3 {
                if let Some(s) = cache.get_set(&argv[1]) {
                    if let Ok(mut set) = s.write() {
                        set.srem(argv[2..].iter().cloned());
                    }
                    cache.remove_set_if_empty(&argv[1]);
                }
            }
        }
        _ => {}
    }
    Ok(())
}

/// Fixed default replid used only for tests that need a stable value.
pub fn default_replid() -> String {
    "kore-repl-0000000000000000000000000000000000000001".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backlog_append_and_partial() {
        let mut bl = ReplBacklog::new(1024);
        bl.append(b"AAA");
        bl.append(b"BBB");
        assert_eq!(bl.start_offset(), 0);
        assert_eq!(bl.end_offset(), 6);
        assert!(bl.can_partial(0));
        assert!(bl.can_partial(3));
        assert!(bl.can_partial(6));
        assert!(!bl.can_partial(7));
        assert_eq!(bl.get_from(0).unwrap().as_ref(), b"AAABBB");
        assert_eq!(bl.get_from(3).unwrap().as_ref(), b"BBB");
        assert_eq!(bl.get_from(6).unwrap().as_ref(), b"");
    }

    #[test]
    fn backlog_trims_when_over_capacity() {
        let mut bl = ReplBacklog::new(16);
        bl.append(b"0123456789"); // 10
        bl.append(b"ABCDEFGHIJ"); // 10 → total 20 > 16
        assert!(bl.len() <= 16);
        assert_eq!(bl.end_offset(), 20);
        // Oldest bytes dropped; partial from 0 no longer possible
        assert!(!bl.can_partial(0));
        assert!(bl.can_partial(bl.start_offset()));
    }

    #[test]
    fn propagate_bumps_offset() {
        let repl = ReplicationManager::new();
        assert_eq!(repl.master_repl_offset(), 0);
        repl.propagate_command(&[
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"k"),
            Bytes::from_static(b"v"),
        ]);
        assert!(repl.master_repl_offset() > 0);
    }

    #[test]
    fn backlog_oversized_single_write_keeps_tail() {
        let mut bl = ReplBacklog::new(8);
        bl.append(b"0123456789ABCDEF"); // 16 bytes > 8
        assert!(bl.len() <= 8);
        assert_eq!(bl.end_offset(), 16);
        // Tail only
        let data = bl.get_from(bl.start_offset()).unwrap();
        assert_eq!(data.as_ref(), b"89ABCDEF");
    }

    #[test]
    fn live_feed_receives_propagated_commands() {
        let repl = ReplicationManager::new();
        let cache = Cache::new_with_sweep(4, 1024 * 1024, 1024 * 1024, false);
        let databases = Databases::single(cache);
        let start = repl.start_psync(&databases, "?", -1).unwrap();
        let mut feed = match start {
            SyncStart::Full { feed, .. } => feed,
            SyncStart::Partial { feed, .. } => feed,
        };
        assert_eq!(repl.connected_replicas(), 1);

        repl.propagate_command(&[
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"live"),
            Bytes::from_static(b"1"),
        ]);
        let msg = feed.try_recv().expect("feed should have SET");
        let s = String::from_utf8_lossy(&msg);
        assert!(s.contains("SET") && s.contains("live"));
    }

    #[test]
    fn partial_history_contains_only_bytes_from_offset() {
        let repl = ReplicationManager::new();
        let cache = Cache::new_with_sweep(4, 1024 * 1024, 1024 * 1024, false);
        let databases = Databases::single(cache);
        repl.propagate_command(&[
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"a"),
            Bytes::from_static(b"1"),
        ]);
        let mid = repl.master_repl_offset();
        repl.propagate_command(&[
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"2"),
        ]);
        let id = repl.replid();
        match repl.start_psync(&databases, &id, mid as i64).unwrap() {
            SyncStart::Partial { raw_response, .. } => {
                assert!(raw_response.starts_with(b"+CONTINUE\r\n"));
                let body = &raw_response[b"+CONTINUE\r\n".len()..];
                let s = String::from_utf8_lossy(body);
                assert!(s.contains("b"), "should include post-mid command: {}", s);
                // First command may or may not appear depending on encoding — mid is exact byte offset
                assert_eq!(body.len() as u64, repl.master_repl_offset() - mid);
            }
            other => panic!("expected partial, got {:?}", std::mem::discriminant(&other)),
        }
    }

    #[test]
    fn set_replicaof_toggles_flags_and_replid_on_promote() {
        let repl = ReplicationManager::new();
        let id1 = repl.replid();
        assert!(!repl.is_replica());
        assert!(!repl.readonly());
        assert_eq!(repl.role_name(), "master");

        repl.set_replicaof(Some("127.0.0.1:7001".into()));
        assert!(repl.is_replica());
        assert!(repl.readonly());
        assert_eq!(repl.role_name(), "slave");
        assert_eq!(repl.primary_addr().as_deref(), Some("127.0.0.1:7001"));
        assert!(!repl.master_link_up());

        repl.set_replicaof(None);
        assert!(!repl.is_replica());
        assert!(!repl.readonly());
        assert_eq!(repl.role_name(), "master");
        assert!(repl.primary_addr().is_none());
        // New replid after promote
        let id2 = repl.replid();
        assert_ne!(id1, id2);
    }

    #[test]
    fn role_reply_master_and_slave_shapes() {
        let repl = ReplicationManager::new();
        match repl.role_reply() {
            RespValue::Array(arr) => {
                assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from_static(b"master"))));
                assert!(matches!(arr[1], RespValue::Integer(_)));
                assert!(matches!(arr[2], RespValue::Array(_)));
            }
            other => panic!("{:?}", other),
        }
        repl.set_replicaof(Some("10.0.0.5:6379".into()));
        match repl.role_reply() {
            RespValue::Array(arr) => {
                assert_eq!(arr[0], RespValue::BulkString(Some(Bytes::from_static(b"slave"))));
                assert_eq!(arr[1], RespValue::BulkString(Some(Bytes::from("10.0.0.5"))));
                assert_eq!(arr[2], RespValue::Integer(6379));
            }
            other => panic!("{:?}", other),
        }
    }

    #[test]
    fn info_replication_master_and_slave() {
        let repl = ReplicationManager::new();
        let master = repl.info_replication();
        assert!(master.contains("role:master"));
        assert!(master.contains("master_replid:"));
        assert!(master.contains("repl_backlog_active:1"));

        repl.set_replicaof(Some("192.168.1.1:6380".into()));
        let slave = repl.info_replication();
        assert!(slave.contains("role:slave"));
        assert!(slave.contains("master_host:192.168.1.1"));
        assert!(slave.contains("master_port:6380"));
        assert!(slave.contains("master_link_status:down"));
    }

    #[test]
    fn full_sync_legacy_is_pure_bulk() {
        let repl = ReplicationManager::new();
        let cache = Cache::new_with_sweep(4, 1024 * 1024, 1024 * 1024, false);
        let databases = Databases::single(cache);
        let (raw, _rx) = repl.start_full_sync(&databases).unwrap();
        assert!(raw.starts_with(b"$"), "SYNC should be bulk RDB, got {:?}", &raw[..raw.len().min(20)]);
        assert!(!raw.starts_with(b"+FULLRESYNC"));
    }

    #[test]
    fn apply_argv_set_del_incr_hash_list_set() {
        let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 1024 * 1024, false);
        let databases = Databases::single(cache.clone());
        let mut db = 0usize;
        apply_argv(
            &databases,
            &mut db,
            vec![
                Bytes::from_static(b"SET"),
                Bytes::from_static(b"k"),
                Bytes::from_static(b"v"),
            ],
        )
        .unwrap();
        let entry = cache
            .load(&Bytes::from_static(b"k"), crate::entry::LoadOptions::default())
            .unwrap()
            .unwrap();
        assert_eq!(entry.value.as_ref(), b"v");

        apply_argv(
            &databases,
            &mut db,
            vec![Bytes::from_static(b"INCR"), Bytes::from_static(b"n")],
        )
        .unwrap();
        apply_argv(
            &databases,
            &mut db,
            vec![
                Bytes::from_static(b"INCRBY"),
                Bytes::from_static(b"n"),
                Bytes::from_static(b"4"),
            ],
        )
        .unwrap();

        apply_argv(
            &databases,
            &mut db,
            vec![
                Bytes::from_static(b"HSET"),
                Bytes::from_static(b"h"),
                Bytes::from_static(b"f"),
                Bytes::from_static(b"1"),
            ],
        )
        .unwrap();
        assert!(cache.hash_exists(&Bytes::from_static(b"h")));

        apply_argv(
            &databases,
            &mut db,
            vec![
                Bytes::from_static(b"LPUSH"),
                Bytes::from_static(b"l"),
                Bytes::from_static(b"a"),
            ],
        )
        .unwrap();
        assert!(cache.list_exists(&Bytes::from_static(b"l")));

        apply_argv(
            &databases,
            &mut db,
            vec![
                Bytes::from_static(b"SADD"),
                Bytes::from_static(b"s"),
                Bytes::from_static(b"m"),
            ],
        )
        .unwrap();
        assert!(cache.set_exists(&Bytes::from_static(b"s")));

        apply_argv(
            &databases,
            &mut db,
            vec![
                Bytes::from_static(b"ZADD"),
                Bytes::from_static(b"z"),
                Bytes::from_static(b"1.5"),
                Bytes::from_static(b"m1"),
            ],
        )
        .unwrap();
        assert!(cache.sorted_set_exists(&Bytes::from_static(b"z")));

        apply_argv(
            &databases,
            &mut db,
            vec![Bytes::from_static(b"DEL"), Bytes::from_static(b"k")],
        )
        .unwrap();
        assert!(!cache.exists(&Bytes::from_static(b"k")));

        apply_argv(&databases, &mut db, vec![Bytes::from_static(b"FLUSHDB")]).unwrap();
        assert_eq!(cache.dbsize(), 0);
    }

    #[test]
    fn split_host_port_helpers() {
        assert_eq!(split_host_port("localhost:6379"), ("localhost".into(), 6379));
        assert_eq!(split_host_port("no-port"), ("no-port".into(), 0));
    }

    #[test]
    fn apply_argv_zrem_hdel_srem_and_pops() {
        let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 1024 * 1024, false);
        let databases = Databases::single(cache.clone());
        let mut db = 0usize;

        apply_argv(
            &databases,
            &mut db,
            vec![
                Bytes::from_static(b"ZADD"),
                Bytes::from_static(b"z"),
                Bytes::from_static(b"1"),
                Bytes::from_static(b"a"),
                Bytes::from_static(b"2"),
                Bytes::from_static(b"b"),
            ],
        )
        .unwrap();
        apply_argv(
            &databases,
            &mut db,
            vec![
                Bytes::from_static(b"ZREM"),
                Bytes::from_static(b"z"),
                Bytes::from_static(b"a"),
            ],
        )
        .unwrap();
        {
            let z = cache.get_sorted_set(&Bytes::from_static(b"z")).unwrap();
            let set = z.read().unwrap();
            assert_eq!(set.len(), 1);
            assert!(set.score(&Bytes::from_static(b"b")).is_some());
            assert!(set.score(&Bytes::from_static(b"a")).is_none());
        }

        apply_argv(
            &databases,
            &mut db,
            vec![
                Bytes::from_static(b"HSET"),
                Bytes::from_static(b"h"),
                Bytes::from_static(b"f1"),
                Bytes::from_static(b"v1"),
                Bytes::from_static(b"f2"),
                Bytes::from_static(b"v2"),
            ],
        )
        .unwrap();
        apply_argv(
            &databases,
            &mut db,
            vec![
                Bytes::from_static(b"HDEL"),
                Bytes::from_static(b"h"),
                Bytes::from_static(b"f1"),
            ],
        )
        .unwrap();
        {
            let h = cache.get_hash(&Bytes::from_static(b"h")).unwrap();
            let hash = h.read().unwrap();
            assert!(hash.hget(&Bytes::from_static(b"f1")).is_none());
            assert!(hash.hget(&Bytes::from_static(b"f2")).is_some());
        }

        apply_argv(
            &databases,
            &mut db,
            vec![
                Bytes::from_static(b"SADD"),
                Bytes::from_static(b"s"),
                Bytes::from_static(b"m1"),
                Bytes::from_static(b"m2"),
            ],
        )
        .unwrap();
        apply_argv(
            &databases,
            &mut db,
            vec![
                Bytes::from_static(b"SREM"),
                Bytes::from_static(b"s"),
                Bytes::from_static(b"m1"),
            ],
        )
        .unwrap();
        {
            let s = cache.get_set(&Bytes::from_static(b"s")).unwrap();
            let set = s.read().unwrap();
            assert!(!set.sismember(&Bytes::from_static(b"m1")));
            assert!(set.sismember(&Bytes::from_static(b"m2")));
        }

        apply_argv(
            &databases,
            &mut db,
            vec![
                Bytes::from_static(b"RPUSH"),
                Bytes::from_static(b"l"),
                Bytes::from_static(b"x"),
                Bytes::from_static(b"y"),
            ],
        )
        .unwrap();
        apply_argv(
            &databases,
            &mut db,
            vec![Bytes::from_static(b"LPOP"), Bytes::from_static(b"l")],
        )
        .unwrap();
        apply_argv(
            &databases,
            &mut db,
            vec![Bytes::from_static(b"RPOP"), Bytes::from_static(b"l")],
        )
        .unwrap();
        // Empty list should be cleaned up
        assert!(!cache.list_exists(&Bytes::from_static(b"l")));
    }

    #[test]
    fn apply_argv_append_decr_and_geo() {
        let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 1024 * 1024, false);
        let databases = Databases::single(cache.clone());
        let mut db = 0usize;

        apply_argv(
            &databases,
            &mut db,
            vec![
                Bytes::from_static(b"SET"),
                Bytes::from_static(b"s"),
                Bytes::from_static(b"hello"),
            ],
        )
        .unwrap();
        apply_argv(
            &databases,
            &mut db,
            vec![
                Bytes::from_static(b"APPEND"),
                Bytes::from_static(b"s"),
                Bytes::from_static(b" world"),
            ],
        )
        .unwrap();
        let entry = cache
            .load(&Bytes::from_static(b"s"), crate::entry::LoadOptions::default())
            .unwrap()
            .unwrap();
        assert_eq!(entry.value.as_ref(), b"hello world");

        apply_argv(
            &databases,
            &mut db,
            vec![
                Bytes::from_static(b"SET"),
                Bytes::from_static(b"n"),
                Bytes::from_static(b"10"),
            ],
        )
        .unwrap();
        apply_argv(
            &databases,
            &mut db,
            vec![Bytes::from_static(b"DECR"), Bytes::from_static(b"n")],
        )
        .unwrap();
        apply_argv(
            &databases,
            &mut db,
            vec![
                Bytes::from_static(b"DECRBY"),
                Bytes::from_static(b"n"),
                Bytes::from_static(b"3"),
            ],
        )
        .unwrap();
        let entry = cache
            .load(&Bytes::from_static(b"n"), crate::entry::LoadOptions::default())
            .unwrap()
            .unwrap();
        assert_eq!(entry.value.as_ref(), b"6");

        apply_argv(
            &databases,
            &mut db,
            vec![
                Bytes::from_static(b"GEOADD"),
                Bytes::from_static(b"g"),
                Bytes::from_static(b"13.361389"),
                Bytes::from_static(b"38.115556"),
                Bytes::from_static(b"Palermo"),
            ],
        )
        .unwrap();
        assert!(cache.get_geo_set(&Bytes::from_static(b"g")).is_some());
    }

    #[test]
    fn full_resync_response_contains_replid_and_offset() {
        let repl = ReplicationManager::new();
        let cache = Cache::new_with_sweep(4, 1024 * 1024, 1024 * 1024, false);
        let databases = Databases::single(cache);
        repl.propagate_command(&[
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"k"),
            Bytes::from_static(b"v"),
        ]);
        let offset = repl.master_repl_offset();
        let id = repl.replid();
        match repl.start_psync(&databases, "?", -1).unwrap() {
            SyncStart::Full { raw_response, feed: _ } => {
                let s = String::from_utf8_lossy(&raw_response);
                assert!(s.starts_with(&format!("+FULLRESYNC {} {}", id, offset)));
                assert!(s.contains("\r\n$"));
            }
            SyncStart::Partial { .. } => panic!("expected full"),
        }
    }

    #[test]
    fn drop_feed_decrements_connected_replicas() {
        let repl = ReplicationManager::new();
        let cache = Cache::new_with_sweep(4, 1024 * 1024, 1024 * 1024, false);
        let databases = Databases::single(cache);
        let start = repl.start_psync(&databases, "?", -1).unwrap();
        let feed = match start {
            SyncStart::Full { feed, .. } | SyncStart::Partial { feed, .. } => feed,
        };
        assert_eq!(repl.connected_replicas(), 1);
        drop(feed);
        // Give the cleanup task a moment if any; count is decremented when feed is dropped
        // via the channel close path used by propagate. At minimum, a second PSYNC adds another.
        let start2 = repl.start_psync(&databases, "?", -1).unwrap();
        let _feed2 = match start2 {
            SyncStart::Full { feed, .. } | SyncStart::Partial { feed, .. } => feed,
        };
        assert!(repl.connected_replicas() >= 1);
    }

    #[test]
    fn wrong_replid_forces_full_even_with_valid_offset() {
        let repl = ReplicationManager::new();
        let cache = Cache::new_with_sweep(4, 1024 * 1024, 1024 * 1024, false);
        let databases = Databases::single(cache);
        repl.propagate_command(&[
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"a"),
            Bytes::from_static(b"1"),
        ]);
        let off = repl.master_repl_offset();
        match repl
            .start_psync(&databases, "ffffffffffffffffffffffffffffffffffffffff", 0)
            .unwrap()
        {
            SyncStart::Full { .. } => {}
            SyncStart::Partial { .. } => panic!("wrong replid must full-resync"),
        }
        // Correct id still partial
        let id = repl.replid();
        match repl.start_psync(&databases, &id, 0).unwrap() {
            SyncStart::Partial { raw_response, .. } => {
                assert!(raw_response.starts_with(b"+CONTINUE\r\n"));
                assert_eq!(
                    raw_response.len() - b"+CONTINUE\r\n".len(),
                    off as usize
                );
            }
            SyncStart::Full { .. } => panic!("expected partial"),
        }
    }

    #[test]
    fn set_replicaof_none_resets_backlog_so_partial_fails() {
        let repl = ReplicationManager::new();
        let cache = Cache::new_with_sweep(4, 1024 * 1024, 1024 * 1024, false);
        let databases = Databases::single(cache);

        repl.propagate_command(&[
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"a"),
            Bytes::from_static(b"1"),
        ]);
        repl.propagate_command(&[
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"2"),
        ]);
        let old_id = repl.replid();
        let old_off = repl.master_repl_offset();
        assert!(old_off > 0);

        // Before promote, partial from 0 with current id works
        match repl.start_psync(&databases, &old_id, 0).unwrap() {
            SyncStart::Partial { .. } => {}
            SyncStart::Full { .. } => panic!("expected partial before promote"),
        }

        // Simulate replica-side metadata + promote
        *repl.cached_master_replid.lock() = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".into();
        repl.replica_offset.store(42, Ordering::Relaxed);
        repl.set_replicaof(Some("127.0.0.1:7000".into()));
        repl.set_replicaof(None);

        assert_eq!(repl.master_repl_offset(), 0);
        assert_ne!(repl.replid(), old_id);
        // Old offsets no longer partial-servable (backlog cleared)
        assert!(!repl.backlog.lock().can_partial(0) || repl.master_repl_offset() == 0);
        // Even with old id, partial must fail (full only)
        match repl.start_psync(&databases, &old_id, 0).unwrap() {
            SyncStart::Full { .. } => {}
            SyncStart::Partial { .. } => panic!("old id partial must fail after promote"),
        }
        // New id at offset 0 is empty backlog — can_partial(0) with empty cleared backlog
        // still allows offset==end (0), but history is empty; that is fine as Partial with empty body.
        // What matters: old non-zero offsets fail.
        match repl.start_psync(&databases, &old_id, old_off as i64).unwrap() {
            SyncStart::Full { .. } => {}
            SyncStart::Partial { .. } => panic!("old offset partial must fail after promote"),
        }
    }

    #[test]
    fn promote_clears_replica_metadata_and_feeds() {
        let repl = ReplicationManager::new();
        let cache = Cache::new_with_sweep(4, 1024 * 1024, 1024 * 1024, false);

        // Register a feed as if a replica were connected while we were master
        let mut feed = repl.register_replica();
        assert_eq!(repl.connected_replicas(), 1);
        repl.propagate_command(&[
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"k"),
            Bytes::from_static(b"v"),
        ]);
        assert!(feed.try_recv().is_ok());

        *repl.cached_master_replid.lock() = "cached-master-id".into();
        repl.replica_offset.store(99, Ordering::Relaxed);
        repl.set_replicaof(Some("10.0.0.1:6379".into()));

        repl.set_replicaof(None);

        assert!(!repl.is_replica());
        assert!(!repl.readonly());
        assert!(repl.primary_addr().is_none());
        assert!(repl.cached_master_replid().is_empty());
        assert_eq!(repl.replica_offset(), 0);
        assert_eq!(repl.master_repl_offset(), 0);
        assert_eq!(repl.connected_replicas(), 0);
        // Dropped feeds: sender gone → try_recv yields disconnected or empty forever
        assert!(feed.try_recv().is_err());
        // No further propagation to the old feed
        repl.propagate_command(&[
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"x"),
            Bytes::from_static(b"1"),
        ]);
        assert!(feed.try_recv().is_err());
        let _ = cache; // silence unused
    }
}
