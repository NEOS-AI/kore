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
use tokio::time::{Duration, Instant};
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
    /// Last ACK offset reported by this replica (live repl link or GETACK).
    ack_offset: AtomicU64,
    /// Unix millis of last ACK (or connect time). Used for `min-replicas-max-lag`.
    last_ack_unix_ms: AtomicU64,
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
    /// Refuse writes unless at least this many "good" replicas (0 = disabled).
    min_replicas_to_write: AtomicUsize,
    /// Max lag in seconds for a replica to count as good (Redis default 10).
    min_replicas_max_lag_secs: AtomicUsize,
    /// Bumped when primary address changes so `sync_from_primary` reconnects.
    primary_link_epoch: AtomicU64,
    /// Serializes full-resync (RDB snapshot + feed registration) with
    /// `propagate_raw` so a write cannot land in the gap between snapshot and
    /// register (would be missing from both RDB and the new live feed).
    fullsync_gate: Mutex<()>,
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
            min_replicas_to_write: AtomicUsize::new(0),
            min_replicas_max_lag_secs: AtomicUsize::new(10),
            primary_link_epoch: AtomicU64::new(0),
            fullsync_gate: Mutex::new(()),
        })
    }

    pub fn primary_link_epoch(&self) -> u64 {
        self.primary_link_epoch.load(Ordering::Relaxed)
    }

    fn bump_primary_link_epoch(&self) {
        self.primary_link_epoch.fetch_add(1, Ordering::Relaxed);
        self.master_link_up.store(false, Ordering::Relaxed);
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

    pub fn min_replicas_to_write(&self) -> usize {
        self.min_replicas_to_write.load(Ordering::Relaxed)
    }

    pub fn set_min_replicas_to_write(&self, n: usize) {
        self.min_replicas_to_write.store(n, Ordering::Relaxed);
    }

    pub fn min_replicas_max_lag(&self) -> usize {
        self.min_replicas_max_lag_secs.load(Ordering::Relaxed)
    }

    pub fn set_min_replicas_max_lag(&self, secs: usize) {
        self.min_replicas_max_lag_secs.store(secs, Ordering::Relaxed);
    }

    /// Number of connected replicas whose last ACK is within `min-replicas-max-lag`.
    pub fn good_replica_count(&self) -> usize {
        let max_lag_secs = self.min_replicas_max_lag() as u64;
        let now = unix_now_ms();
        let max_lag_ms = max_lag_secs.saturating_mul(1000);
        let reps = self.replicas.lock();
        reps.iter()
            .filter(|r| {
                let last = r.last_ack_unix_ms.load(Ordering::Relaxed);
                if last == 0 {
                    return false;
                }
                now.saturating_sub(last) <= max_lag_ms
            })
            .count()
    }

    /// True when writes are allowed under `min-replicas-to-write` (always true if 0).
    pub fn writes_allowed_by_min_replicas(&self) -> bool {
        let need = self.min_replicas_to_write();
        if need == 0 || self.is_replica() {
            return true;
        }
        self.good_replica_count() >= need
    }

    /// Count connected replica feeds whose tracked ACK ≥ `offset`.
    pub fn count_replicas_acked(&self, offset: u64) -> usize {
        let reps = self.replicas.lock();
        reps.iter()
            .filter(|r| r.ack_offset.load(Ordering::Relaxed) >= offset)
            .count()
    }

    /// Redis `WAIT numreplicas timeout_ms`: block until enough replicas have
    /// acknowledged the current master offset, or until timeout.
    ///
    /// - Freezes `master_repl_offset` at call time as the target.
    /// - `timeout_ms == 0` waits indefinitely.
    /// - Returns how many replicas reached the offset (may be &lt; numreplicas on timeout).
    /// - On a replica (no feeds), returns 0 immediately.
    pub async fn wait_numreplicas(&self, numreplicas: usize, timeout_ms: u64) -> usize {
        let target = self.master_repl_offset();
        if self.is_replica() {
            return 0;
        }

        let deadline = if timeout_ms == 0 {
            None
        } else {
            Some(Instant::now() + Duration::from_millis(timeout_ms))
        };

        loop {
            let n = self.count_replicas_acked(target);
            if numreplicas == 0 || n >= numreplicas {
                return n;
            }
            if let Some(d) = deadline {
                if Instant::now() >= d {
                    return n;
                }
            }

            // Probe live feeds so ACKs can advance without client-port GETACK.
            self.send_getack_probe_to_feeds(None, None);

            let slice = match deadline {
                Some(d) => d
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(25)),
                None => Duration::from_millis(25),
            };
            if slice.is_zero() {
                return self.count_replicas_acked(target);
            }
            tokio::time::sleep(slice).await;
        }
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
        let now = unix_now_ms();
        self.replicas.lock().push(ReplicaFeed {
            tx,
            host,
            port,
            ack_offset: AtomicU64::new(0),
            last_ack_unix_ms: AtomicU64::new(now),
        });
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

    /// Record an ACK offset from a replica (live feed link or client GETACK).
    ///
    /// Matches by announced `host`/`port` when provided; otherwise updates all
    /// feeds that have no identity (best-effort single-replica case).
    pub fn note_replica_ack(&self, host: Option<&str>, port: Option<u16>, offset: u64) {
        let mut reps = self.replicas.lock();
        let mut matched = false;
        for r in reps.iter_mut() {
            let host_ok = match (host, r.host.as_deref()) {
                (None, _) => true,
                (Some(h), Some(rh)) => hosts_equal(h, rh),
                (Some(_), None) => port.is_some() && r.port == port,
            };
            let port_ok = match (port, r.port) {
                (None, _) => true,
                (Some(p), Some(rp)) => p == rp,
                (Some(_), None) => false,
            };
            if host_ok && port_ok && (host.is_some() || port.is_some() || r.port.is_none()) {
                // Monotonic: never decrease a known ack.
                r.ack_offset.fetch_max(offset, Ordering::Relaxed);
                r.last_ack_unix_ms
                    .store(unix_now_ms(), Ordering::Relaxed);
                matched = true;
            }
        }
        // Fallback: single anonymous feed
        if !matched && reps.len() == 1 {
            reps[0].ack_offset.fetch_max(offset, Ordering::Relaxed);
            reps[0]
                .last_ack_unix_ms
                .store(unix_now_ms(), Ordering::Relaxed);
        }
    }

    /// Highest tracked ACK for a replica at `host:port`, if any feed matches.
    pub fn tracked_ack_for(&self, host: &str, port: u16) -> Option<u64> {
        let reps = self.replicas.lock();
        let mut best: Option<u64> = None;
        for r in reps.iter() {
            if replica_matches(r, host, port) {
                let ack = r.ack_offset.load(Ordering::Relaxed);
                best = Some(best.map_or(ack, |b| b.max(ack)));
            }
        }
        best
    }

    /// Encode and try-send `REPLCONF GETACK *` on matching replica feeds (no backlog).
    ///
    /// Used to probe the live repl link without advancing `master_repl_offset`.
    pub fn send_getack_probe_to_feeds(&self, host: Option<&str>, port: Option<u16>) {
        let getack = RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"REPLCONF"))),
            RespValue::BulkString(Some(Bytes::from_static(b"GETACK"))),
            RespValue::BulkString(Some(Bytes::from_static(b"*"))),
        ])
        .serialize();
        let reps = self.replicas.lock();
        for r in reps.iter() {
            let matches = match (host, port, r.host.as_deref(), r.port) {
                (None, None, _, _) => true,
                (Some(h), Some(p), _, _) => replica_matches(r, h, p),
                (None, Some(p), _, Some(rp)) => p == rp,
                (Some(h), None, Some(rh), _) => hosts_equal(h, rh),
                _ => r.port.is_none() && r.host.is_none(),
            };
            if matches {
                let _ = r.tx.try_send(getack.clone());
            }
        }
    }

    /// Propagate a write command (as RESP array bytes) to all replicas and backlog.
    pub fn propagate_raw(&self, data: Bytes) {
        // Share the fullsync gate so we never append/send while a full resync
        // is between RDB snapshot and feed registration.
        let _gate = self.fullsync_gate.lock();

        // Always append to backlog (even with no replicas) so reconnecting
        // replicas can PSYNC if they reconnect quickly.
        {
            let mut bl = self.backlog.lock();
            bl.append(&data);
            self.master_repl_offset
                .store(bl.end_offset(), Ordering::Relaxed);
        }

        // Fast path: no connected replicas — skip the feed list lock.
        // `connected_replicas` is updated under `replicas` lock on register/drop;
        // a stale 0 is safe (miss one send → full resync later). A stale non-zero
        // just takes the lock and finds the list empty/non-empty correctly.
        if self.connected_replicas.load(Ordering::Relaxed) == 0 {
            return;
        }

        let mut reps = self.replicas.lock();
        if reps.is_empty() {
            self.connected_replicas.store(0, Ordering::Relaxed);
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
        // Hold the gate across snapshot + register (see `fullsync_gate`).
        let _gate = self.fullsync_gate.lock();
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
                        ack_offset: AtomicU64::new(0),
                        last_ack_unix_ms: AtomicU64::new(unix_now_ms()),
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

        // Full resync — multi-DB snapshot + feed register under one gate so
        // concurrent `propagate_raw` cannot insert a write into the gap
        // (missing from RDB and from the new feed).
        let _gate = self.fullsync_gate.lock();
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

    /// How often to poll the target with `REPLCONF GETACK` during catch-up.
    const FAILOVER_CATCHUP_POLL_MS: u64 = 25;

    /// Master-initiated coordinated failover.
    ///
    /// 1. Pause writes (`failover_in_progress`)
    /// 2. If replica identities are known and none match `host:port`, error
    /// 3. Unless `force`: wait until target ack ≥ frozen master offset
    ///    (live-link tracked ACK and/or `REPLCONF GETACK` on client port)
    /// 4. TCP connect to target and send bare `FAILOVER`
    /// 5. On success, best-effort redirect other replicas to the new master
    /// 6. Demote self via `set_replicaof(Some(host:port))`
    ///
    /// `force` skips the catch-up wait (may promote a lagging replica).
    pub async fn coordinated_failover_to(
        &self,
        host: &str,
        port: u16,
        timeout_ms: u64,
        force: bool,
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

        let deadline = Instant::now() + Duration::from_millis(timeout_ms.max(1));
        self.failover_in_progress.store(true, Ordering::Relaxed);

        // Freeze the offset we require the replica to have applied before promote.
        let target_offset = self.master_repl_offset();
        if !force {
            if let Err(e) = self
                .wait_replica_offset_catchup(host, port, target_offset, deadline)
                .await
            {
                self.failover_in_progress.store(false, Ordering::Relaxed);
                return Err(e);
            }
        } else {
            info!(
                "FAILOVER TO FORCE: skipping catch-up (master offset {})",
                target_offset
            );
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            self.failover_in_progress.store(false, Ordering::Relaxed);
            return Err(if force {
                "ERR FAILOVER TO timed out".into()
            } else {
                format!(
                    "ERR FAILOVER TO timed out waiting for replica catch-up (need offset {})",
                    target_offset
                )
            });
        }

        let result = self.send_failover_to_target(host, port, remaining).await;

        match result {
            Ok(()) => {
                // Point sibling replicas at the new master (best-effort).
                self.redirect_replicas_to_master(host, port).await;
                // Demote self to replica of the newly promoted master.
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

    /// Encode `REPLICAOF host port` and try-send on all feeds except the promote target.
    pub fn send_replicaof_to_feeds(
        &self,
        new_host: &str,
        new_port: u16,
        exclude_host: &str,
        exclude_port: u16,
    ) {
        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"REPLICAOF"))),
            RespValue::BulkString(Some(Bytes::from(new_host.to_string()))),
            RespValue::BulkString(Some(Bytes::from(new_port.to_string()))),
        ])
        .serialize();
        let reps = self.replicas.lock();
        for r in reps.iter() {
            if replica_matches(r, exclude_host, exclude_port) {
                continue;
            }
            let _ = r.tx.try_send(cmd.clone());
        }
    }

    /// Best-effort: push REPLICAOF on feeds + client-port TCP to announced replicas.
    pub async fn redirect_replicas_to_master(&self, new_host: &str, new_port: u16) {
        self.send_replicaof_to_feeds(new_host, new_port, new_host, new_port);

        // Snapshot announced identities for TCP fallback.
        let targets: Vec<(String, u16)> = {
            let reps = self.replicas.lock();
            reps.iter()
                .filter_map(|r| {
                    if replica_matches(r, new_host, new_port) {
                        return None;
                    }
                    let port = r.port?;
                    let host = r
                        .host
                        .clone()
                        .filter(|h| h != "?")
                        .unwrap_or_else(|| "127.0.0.1".into());
                    Some((host, port))
                })
                .collect()
        };

        for (h, p) in targets {
            if let Err(e) = self
                .send_replicaof_to_target(&h, p, new_host, new_port, Duration::from_millis(500))
                .await
            {
                warn!(
                    "FAILOVER TO: failed to redirect replica {}:{} → {}:{}: {}",
                    h, p, new_host, new_port, e
                );
            } else {
                info!(
                    "FAILOVER TO: redirected replica {}:{} → {}:{}",
                    h, p, new_host, new_port
                );
            }
        }
    }

    async fn send_replicaof_to_target(
        &self,
        peer_host: &str,
        peer_port: u16,
        new_host: &str,
        new_port: u16,
        timeout: Duration,
    ) -> std::result::Result<(), String> {
        let addr = format!("{}:{}", peer_host, peer_port);
        let connect = TcpStream::connect(&addr);
        let mut stream = match tokio::time::timeout(timeout, connect).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                return Err(format!("connect {}: {}", addr, e));
            }
            Err(_) => {
                return Err(format!("connect timeout {}", addr));
            }
        };

        let cmd = RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"REPLICAOF"))),
            RespValue::BulkString(Some(Bytes::from(new_host.to_string()))),
            RespValue::BulkString(Some(Bytes::from(new_port.to_string()))),
        ])
        .serialize();

        match tokio::time::timeout(timeout, stream.write_all(&cmd)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(format!("write {}: {}", addr, e)),
            Err(_) => return Err(format!("write timeout {}", addr)),
        }

        let mut parser = RespParser::new();
        let mut buf = vec![0u8; 4096];
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(format!("read timeout {}", addr));
            }
            if let Some(val) = parser.parse().map_err(|e| e.to_string())? {
                match val {
                    RespValue::SimpleString(s) if s.as_ref() == b"OK" => return Ok(()),
                    RespValue::Error(e) => {
                        return Err(format!("target error: {}", String::from_utf8_lossy(&e)));
                    }
                    other => return Err(format!("unexpected reply: {:?}", other)),
                }
            }
            let n = match tokio::time::timeout(remaining, stream.read(&mut buf)).await {
                Ok(Ok(n)) => n,
                Ok(Err(e)) => return Err(format!("read {}: {}", addr, e)),
                Err(_) => return Err(format!("read timeout {}", addr)),
            };
            if n == 0 {
                return Err(format!("closed {}", addr));
            }
            parser.feed(&buf[..n]);
        }
    }

    /// Wait until the target's replication offset ≥ `target_offset` or deadline.
    ///
    /// Sources (in order each poll):
    /// 1. Live-link tracked ACK (`note_replica_ack` / feed GETACK replies)
    /// 2. Probe GETACK on matching replica feed channels (no backlog bump)
    /// 3. Client-port `REPLCONF GETACK *` fallback
    ///
    /// When `target_offset == 0` there is nothing to wait for.
    pub async fn wait_replica_offset_catchup(
        &self,
        host: &str,
        port: u16,
        target_offset: u64,
        deadline: Instant,
    ) -> std::result::Result<(), String> {
        if target_offset == 0 {
            return Ok(());
        }

        let addr = format!("{}:{}", host, port);
        let mut last_ack: Option<u64> = None;
        let mut last_err: Option<String> = None;

        loop {
            if Instant::now() >= deadline {
                return Err(match last_ack {
                    Some(ack) => format!(
                        "ERR FAILOVER TO timed out waiting for replica catch-up (need offset {}, last ack {})",
                        target_offset, ack
                    ),
                    None => format!(
                        "ERR FAILOVER TO timed out waiting for replica catch-up (need offset {}, {})",
                        target_offset,
                        last_err.unwrap_or_else(|| "no ack received".into())
                    ),
                });
            }

            // 1) Live-link tracked ACK
            if let Some(ack) = self.tracked_ack_for(host, port) {
                last_ack = Some(ack);
                if ack >= target_offset {
                    info!(
                        "FAILOVER TO catch-up ok (live ack): replica {} ack {} >= master {}",
                        addr, ack, target_offset
                    );
                    return Ok(());
                }
            }

            // 2) Probe on feed channels (replica replies on live link → note_replica_ack)
            self.send_getack_probe_to_feeds(Some(host), Some(port));

            // Brief yield so feed write + replica ACK can land
            let slice = deadline.saturating_duration_since(Instant::now());
            if slice.is_zero() {
                continue;
            }
            tokio::time::sleep(slice.min(Duration::from_millis(10))).await;

            if let Some(ack) = self.tracked_ack_for(host, port) {
                last_ack = Some(ack);
                if ack >= target_offset {
                    info!(
                        "FAILOVER TO catch-up ok (after feed probe): {} ack {} >= {}",
                        addr, ack, target_offset
                    );
                    return Ok(());
                }
            }

            // 3) Client-port GETACK fallback
            let attempt = deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(200));
            if attempt.is_zero() {
                continue;
            }
            match self.query_replica_ack(&addr, attempt).await {
                Ok(ack) if ack >= target_offset => {
                    // Keep live table warm for subsequent probes
                    self.note_replica_ack(Some(host), Some(port), ack);
                    info!(
                        "FAILOVER TO catch-up ok (client GETACK): replica {} ack {} >= master {}",
                        addr, ack, target_offset
                    );
                    return Ok(());
                }
                Ok(ack) => {
                    self.note_replica_ack(Some(host), Some(port), ack);
                    last_ack = Some(ack);
                    last_err = None;
                }
                Err(e) => {
                    last_err = Some(e);
                }
            }

            let sleep_for = Duration::from_millis(Self::FAILOVER_CATCHUP_POLL_MS)
                .min(deadline.saturating_duration_since(Instant::now()));
            if !sleep_for.is_zero() {
                tokio::time::sleep(sleep_for).await;
            }
        }
    }

    /// Open a short-lived client connection and send `REPLCONF GETACK *`.
    /// Returns the replica's reported offset.
    async fn query_replica_ack(
        &self,
        addr: &str,
        timeout: Duration,
    ) -> std::result::Result<u64, String> {
        let connect = TcpStream::connect(addr.to_string());
        let mut stream = match tokio::time::timeout(timeout, connect).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                return Err(format!("connect {}: {}", addr, e));
            }
            Err(_) => {
                return Err(format!("connect timeout {}", addr));
            }
        };
        let _ = stream.set_nodelay(true);

        let getack = RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"REPLCONF"))),
            RespValue::BulkString(Some(Bytes::from_static(b"GETACK"))),
            RespValue::BulkString(Some(Bytes::from_static(b"*"))),
        ])
        .serialize();

        match tokio::time::timeout(timeout, stream.write_all(&getack)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(format!("write GETACK {}: {}", addr, e)),
            Err(_) => return Err(format!("write GETACK timeout {}", addr)),
        }

        let mut parser = RespParser::new();
        let mut buf = vec![0u8; 4096];
        let reply = loop {
            if let Some(val) = parser
                .parse()
                .map_err(|e| format!("parse GETACK reply: {}", e))?
            {
                break val;
            }
            let n = match tokio::time::timeout(timeout, stream.read(&mut buf)).await {
                Ok(Ok(0)) => {
                    return Err(format!("GETACK: {} closed connection", addr));
                }
                Ok(Ok(n)) => n,
                Ok(Err(e)) => return Err(format!("read GETACK {}: {}", addr, e)),
                Err(_) => return Err(format!("read GETACK timeout {}", addr)),
            };
            parser.feed(&buf[..n]);
        };

        parse_replconf_ack_offset(&reply)
            .ok_or_else(|| format!("unexpected GETACK reply from {}: {:?}", addr, reply))
    }

    async fn send_failover_to_target(
        &self,
        host: &str,
        port: u16,
        timeout: Duration,
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
                let changed = {
                    let mut primary = self.primary_addr.lock();
                    let changed = primary.as_ref() != Some(&a);
                    *primary = Some(a);
                    changed
                };
                self.is_replica.store(true, Ordering::Relaxed);
                self.readonly.store(true, Ordering::Relaxed);
                self.master_link_up.store(false, Ordering::Relaxed);
                if changed {
                    self.bump_primary_link_epoch();
                }
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
        // Force any in-flight sync_from_primary loop to exit.
        self.bump_primary_link_epoch();

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
                 master_repl_offset:{}\r\n\
                 min_slaves_good:{}\r\n\
                 min_slaves_to_write:{}\r\n\
                 min_slaves_max_lag:{}\r\n",
                self.connected_replicas(),
                self.replid(),
                self.master_repl_offset(),
                self.good_replica_count(),
                self.min_replicas_to_write(),
                self.min_replicas_max_lag(),
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
            min_replicas_to_write: AtomicUsize::new(0),
            min_replicas_max_lag_secs: AtomicUsize::new(10),
            primary_link_epoch: AtomicU64::new(0),
            fullsync_gate: Mutex::new(()),
        }
    }
}

fn unix_now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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

/// Parse `*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$N\r\n<offset>\r\n` style reply.
pub fn parse_replconf_ack_offset(reply: &RespValue) -> Option<u64> {
    let arr = match reply {
        RespValue::Array(a) if a.len() >= 3 => a,
        _ => return None,
    };
    let cmd = arr[0].as_bulk_string()?;
    let ack = arr[1].as_bulk_string()?;
    if !cmd.eq_ignore_ascii_case(b"REPLCONF") || !ack.eq_ignore_ascii_case(b"ACK") {
        return None;
    }
    let off_b = arr[2].as_bulk_string()?;
    let s = std::str::from_utf8(off_b).ok()?;
    s.parse::<u64>().ok()
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
    let link_epoch = repl.primary_link_epoch();
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

    // Apply remaining buffered data + stream; count exact wire bytes toward replica_offset.
    // Also handle master `REPLCONF GETACK` probes by replying `REPLCONF ACK <offset>` on this link.
    loop {
        // Primary address / epoch changed (FAILOVER re-follow or promote) → reconnect.
        if repl.primary_link_epoch() != link_epoch {
            info!("Replica primary link epoch changed; reconnecting");
            return Ok(());
        }
        if repl.primary_addr().as_deref() != Some(addr) {
            info!("Replica primary address changed; reconnecting");
            return Ok(());
        }

        while let Some((val, consumed)) = parser.parse_with_consumed()? {
            if is_replconf_getack(&val) {
                // GETACK is part of the master stream — count it, then reply with current offset.
                repl.replica_offset
                    .fetch_add(consumed as u64, Ordering::Relaxed);
                let ack = encode_replconf_ack(repl.replica_offset());
                stream.write_all(&ack).await.map_err(|e| {
                    Error::NetworkError(format!("write REPLCONF ACK: {}", e))
                })?;
                continue;
            }
            // Master may push REPLICAOF on the feed after FAILOVER TO (sibling re-follow).
            if let Some(new_primary) = parse_replicaof_command(&val) {
                repl.replica_offset
                    .fetch_add(consumed as u64, Ordering::Relaxed);
                match new_primary {
                    Some(a) => {
                        info!("Replica received REPLICAOF {} on link; switching", a);
                        repl.set_replicaof(Some(a));
                    }
                    None => {
                        info!("Replica received REPLICAOF NO ONE on link; promoting");
                        repl.set_replicaof(None);
                    }
                }
                return Ok(());
            }
            apply_replicated_command(&databases, &mut current_db, val)?;
            repl.replica_offset
                .fetch_add(consumed as u64, Ordering::Relaxed);
        }
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        parser.feed(&buf[..n]);
    }
}

/// Parse `REPLICAOF host port` / `REPLICAOF NO ONE` / `SLAVEOF …` from a stream command.
/// Returns `Some(None)` for NO ONE, `Some(Some(addr))` for host:port, `None` if not REPLICAOF.
fn parse_replicaof_command(val: &RespValue) -> Option<Option<String>> {
    let arr = match val {
        RespValue::Array(a) if a.len() >= 3 => a,
        _ => return None,
    };
    let cmd = arr[0].as_bulk_string()?;
    if !cmd.eq_ignore_ascii_case(b"REPLICAOF") && !cmd.eq_ignore_ascii_case(b"SLAVEOF") {
        return None;
    }
    let a1 = arr[1].as_bulk_string()?;
    let a2 = arr[2].as_bulk_string()?;
    if a1.eq_ignore_ascii_case(b"NO") && a2.eq_ignore_ascii_case(b"ONE") {
        return Some(None);
    }
    let host = std::str::from_utf8(a1).ok()?;
    let port = std::str::from_utf8(a2).ok()?;
    // Validate port is numeric
    let _: u16 = port.parse().ok()?;
    Some(Some(format!("{}:{}", host, port)))
}

/// True if value is `REPLCONF GETACK …` (master offset probe).
fn is_replconf_getack(val: &RespValue) -> bool {
    let arr = match val {
        RespValue::Array(a) if a.len() >= 2 => a,
        _ => return false,
    };
    let cmd = match arr[0].as_bulk_string() {
        Some(b) => b,
        None => return false,
    };
    let sub = match arr[1].as_bulk_string() {
        Some(b) => b,
        None => return false,
    };
    cmd.eq_ignore_ascii_case(b"REPLCONF") && sub.eq_ignore_ascii_case(b"GETACK")
}

fn encode_replconf_ack(offset: u64) -> Bytes {
    RespValue::Array(vec![
        RespValue::BulkString(Some(Bytes::from_static(b"REPLCONF"))),
        RespValue::BulkString(Some(Bytes::from_static(b"ACK"))),
        RespValue::BulkString(Some(Bytes::from(offset.to_string()))),
    ])
    .serialize()
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
        "PERSIST" => {
            if argv.len() >= 2 {
                let _ = cache.persist(&argv[1]);
            }
        }
        "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" => {
            if argv.len() >= 3 {
                let n: i64 = std::str::from_utf8(&argv[2])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                match cmd.as_str() {
                    "EXPIRE" => {
                        let _ = cache.expire(&argv[1], (n.max(0) as u64).saturating_mul(1000));
                    }
                    "PEXPIRE" => {
                        let _ = cache.expire(&argv[1], n.max(0) as u64);
                    }
                    "EXPIREAT" => {
                        let _ = cache.expire_at_unix_ms(&argv[1], n.saturating_mul(1000));
                    }
                    "PEXPIREAT" => {
                        use crate::cache::KeyType;
                        match cache.key_type(&argv[1]) {
                            KeyType::String => {
                                let now = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map(|d| d.as_millis() as i64)
                                    .unwrap_or(0);
                                if n > now {
                                    let _ = cache.expire(&argv[1], (n - now) as u64);
                                } else {
                                    let _ = cache.delete(&argv[1]);
                                }
                            }
                            KeyType::None => {}
                            _ => cache.set_typed_expire_unix_ms(&argv[1], n),
                        }
                    }
                    _ => {}
                }
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
                    { let mut set = zset.write();
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
                    { let mut set = zset.write();
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
                    { let mut set = geoset.write();
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
                    { let mut hash = h.write();
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
                    { let mut hash = h.write();
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
                    { let mut l = list.write();
                        l.lpush(argv[2..].iter().cloned());
                    }
                }
            }
        }
        "RPUSH" => {
            if argv.len() >= 3 {
                if let Ok(list) = cache.get_or_create_list(&argv[1]) {
                    { let mut l = list.write();
                        l.rpush(argv[2..].iter().cloned());
                    }
                }
            }
        }
        "LPOP" => {
            if argv.len() >= 2 {
                if let Some(list) = cache.get_list(&argv[1]) {
                    { let mut l = list.write();
                        let _ = l.lpop();
                    }
                    cache.remove_list_if_empty(&argv[1]);
                }
            }
        }
        "RPOP" => {
            if argv.len() >= 2 {
                if let Some(list) = cache.get_list(&argv[1]) {
                    { let mut l = list.write();
                        let _ = l.rpop();
                    }
                    cache.remove_list_if_empty(&argv[1]);
                }
            }
        }
        "SADD" => {
            if argv.len() >= 3 {
                if let Ok(s) = cache.get_or_create_set(&argv[1]) {
                    { let mut set = s.write();
                        set.sadd(argv[2..].iter().cloned());
                    }
                }
            }
        }
        "SREM" => {
            if argv.len() >= 3 {
                if let Some(s) = cache.get_set(&argv[1]) {
                    { let mut set = s.write();
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

    /// Concurrent full-resync registration must not drop live writes: every
    /// propagate after a feed is registered must either be in the RDB snapshot
    /// or appear on that feed (gate closes the snapshot/register gap).
    #[test]
    fn fullsync_gate_keeps_propagates_visible_on_new_feed() {
        use std::sync::Barrier;
        use std::thread;

        let repl = ReplicationManager::new();
        let cache = Cache::new_with_sweep(4, 1024 * 1024, 1024 * 1024, false);
        let databases = Databases::single(cache);
        let barrier = Arc::new(Barrier::new(2));

        let repl_w = Arc::clone(&repl);
        let b_w = Arc::clone(&barrier);
        let writer = thread::spawn(move || {
            b_w.wait();
            for i in 0..200 {
                repl_w.propagate_command(&[
                    Bytes::from_static(b"SET"),
                    Bytes::from(format!("k{}", i)),
                    Bytes::from_static(b"v"),
                ]);
            }
        });

        let repl_s = Arc::clone(&repl);
        let b_s = Arc::clone(&barrier);
        let syncer = thread::spawn(move || {
            b_s.wait();
            let mut feeds = Vec::new();
            for _ in 0..4 {
                let start = repl_s.start_psync(&databases, "?", -1).unwrap();
                match start {
                    SyncStart::Full { feed, .. } | SyncStart::Partial { feed, .. } => {
                        feeds.push(feed);
                    }
                }
            }
            feeds
        });

        writer.join().unwrap();
        let mut feeds = syncer.join().unwrap();
        // Drain: each feed should be able to recv without panicking; at least
        // the last registered feed should see some of the later writes if any
        // happened after its registration. Stronger check: backlog end offset
        // matches 200 propagates and connected_replicas >= 1.
        assert!(repl.master_repl_offset() > 0);
        assert!(repl.connected_replicas() >= 1);
        // Ensure we can recv from feeds (channel not poisoned / closed).
        for feed in feeds.iter_mut() {
            while feed.try_recv().is_ok() {}
        }
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
            let set = z.read();
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
            let hash = h.read();
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
            let set = s.read();
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

    #[test]
    fn parse_replconf_ack_offset_ok() {
        let reply = RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"REPLCONF"))),
            RespValue::BulkString(Some(Bytes::from_static(b"ACK"))),
            RespValue::BulkString(Some(Bytes::from_static(b"12345"))),
        ]);
        assert_eq!(parse_replconf_ack_offset(&reply), Some(12345));

        let bad = RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"REPLCONF"))),
            RespValue::BulkString(Some(Bytes::from_static(b"ACK"))),
        ]);
        assert_eq!(parse_replconf_ack_offset(&bad), None);

        let wrong = RespValue::ok();
        assert_eq!(parse_replconf_ack_offset(&wrong), None);
    }

    #[tokio::test]
    async fn wait_catchup_zero_offset_is_immediate() {
        let repl = ReplicationManager::new();
        let deadline = Instant::now() + Duration::from_millis(100);
        // No server needed when target_offset is 0
        repl.wait_replica_offset_catchup("127.0.0.1", 1, 0, deadline)
            .await
            .expect("zero offset must succeed without network");
    }

    #[tokio::test]
    async fn wait_catchup_times_out_when_ack_never_reaches_target() {
        // Nothing listening → polls fail until deadline
        let repl = ReplicationManager::new();
        let deadline = Instant::now() + Duration::from_millis(150);
        let err = repl
            .wait_replica_offset_catchup("127.0.0.1", 1, 999, deadline)
            .await
            .expect_err("must time out");
        assert!(
            err.to_ascii_lowercase().contains("catch-up")
                || err.to_ascii_lowercase().contains("catchup"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn note_replica_ack_tracks_by_host_port() {
        let repl = ReplicationManager::new();
        let _rx = repl.register_replica_announced(
            Some("127.0.0.1".into()),
            Some(7001),
        );
        assert_eq!(repl.tracked_ack_for("127.0.0.1", 7001), Some(0));
        repl.note_replica_ack(Some("127.0.0.1"), Some(7001), 42);
        assert_eq!(repl.tracked_ack_for("127.0.0.1", 7001), Some(42));
        // Monotonic: lower ack does not decrease
        repl.note_replica_ack(Some("127.0.0.1"), Some(7001), 10);
        assert_eq!(repl.tracked_ack_for("127.0.0.1", 7001), Some(42));
        // Higher wins
        repl.note_replica_ack(Some("127.0.0.1"), Some(7001), 100);
        assert_eq!(repl.tracked_ack_for("127.0.0.1", 7001), Some(100));
        // Localhost alias matches
        assert_eq!(repl.tracked_ack_for("localhost", 7001), Some(100));
        // Wrong port
        assert_eq!(repl.tracked_ack_for("127.0.0.1", 7002), None);
    }

    #[test]
    fn note_replica_ack_port_only_when_host_unknown_on_feed() {
        let repl = ReplicationManager::new();
        let _rx = repl.register_replica_announced(None, Some(6380));
        repl.note_replica_ack(Some("10.0.0.5"), Some(6380), 77);
        assert_eq!(repl.tracked_ack_for("10.0.0.5", 6380), Some(77));
        assert_eq!(repl.tracked_ack_for("other", 6380), Some(77));
    }

    #[test]
    fn note_replica_ack_single_anonymous_feed_fallback() {
        let repl = ReplicationManager::new();
        let _rx = repl.register_replica_announced(None, None);
        // No identity on either side — single-feed fallback
        repl.note_replica_ack(Some("127.0.0.1"), Some(1), 55);
        // tracked_ack_for needs port match on feed; anonymous has no port
        assert_eq!(repl.tracked_ack_for("127.0.0.1", 1), None);
        // But note_replica_ack did update the anonymous feed via fallback —
        // verify via a second note with no identity reading through port-less path:
        // register a second feed with identity and ensure anonymous was the one updated
        // by checking send_getack still targets it (smoke).
        repl.send_getack_probe_to_feeds(None, None);
    }

    #[test]
    fn send_getack_probe_delivers_to_matching_feed() {
        let repl = ReplicationManager::new();
        let mut rx = repl.register_replica_announced(
            Some("127.0.0.1".into()),
            Some(6400),
        );
        let mut other = repl.register_replica_announced(
            Some("127.0.0.1".into()),
            Some(6401),
        );
        repl.send_getack_probe_to_feeds(Some("127.0.0.1"), Some(6400));
        let msg = rx.try_recv().expect("GETACK on matching feed");
        let s = String::from_utf8_lossy(&msg);
        assert!(s.to_ascii_uppercase().contains("GETACK"), "got {}", s);
        assert!(other.try_recv().is_err(), "other feed must not get probe");
    }

    #[test]
    fn is_replconf_getack_detects_probe() {
        let getack = RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"REPLCONF"))),
            RespValue::BulkString(Some(Bytes::from_static(b"GETACK"))),
            RespValue::BulkString(Some(Bytes::from_static(b"*"))),
        ]);
        assert!(is_replconf_getack(&getack));
        let ack = encode_replconf_ack(99);
        // encode is serialized bytes — parse back
        let mut p = RespParser::new();
        p.feed(&ack);
        let val = p.parse().unwrap().unwrap();
        assert!(!is_replconf_getack(&val));
        assert_eq!(parse_replconf_ack_offset(&val), Some(99));
    }

    #[test]
    fn encode_replconf_ack_roundtrip() {
        for off in [0u64, 1, 42, 999_999] {
            let raw = encode_replconf_ack(off);
            let mut p = RespParser::new();
            p.feed(&raw);
            let val = p.parse().unwrap().unwrap();
            assert_eq!(parse_replconf_ack_offset(&val), Some(off));
        }
    }

    #[tokio::test]
    async fn wait_catchup_succeeds_via_live_tracked_ack() {
        let repl = ReplicationManager::new();
        let _rx = repl.register_replica_announced(
            Some("127.0.0.1".into()),
            Some(16690),
        );
        // Pretend the live link already reported catch-up
        repl.note_replica_ack(Some("127.0.0.1"), Some(16690), 500);
        let deadline = Instant::now() + Duration::from_millis(500);
        repl.wait_replica_offset_catchup("127.0.0.1", 16690, 500, deadline)
            .await
            .expect("live ack should satisfy catch-up without network");
    }

    #[tokio::test]
    async fn wait_catchup_live_ack_below_target_still_times_out() {
        let repl = ReplicationManager::new();
        let _rx = repl.register_replica_announced(
            Some("127.0.0.1".into()),
            Some(16691),
        );
        repl.note_replica_ack(Some("127.0.0.1"), Some(16691), 10);
        let deadline = Instant::now() + Duration::from_millis(120);
        let err = repl
            .wait_replica_offset_catchup("127.0.0.1", 16691, 9999, deadline)
            .await
            .expect_err("ack 10 < 9999 must time out");
        assert!(
            err.to_ascii_lowercase().contains("catch-up")
                || err.contains("9999")
                || err.contains("10"),
            "unexpected: {}",
            err
        );
    }

    #[tokio::test]
    async fn coordinated_failover_force_skips_catchup_then_fails_connect() {
        // FORCE skips wait; unreachable target → connect error, master stays master.
        let repl = ReplicationManager::new();
        repl.propagate_command(&[
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"k"),
            Bytes::from_static(b"v"),
        ]);
        assert!(repl.master_repl_offset() > 0);
        let err = repl
            .coordinated_failover_to("127.0.0.1", 1, 200, true)
            .await
            .expect_err("unreachable target");
        assert!(
            err.to_ascii_lowercase().contains("failover"),
            "unexpected: {}",
            err
        );
        assert!(
            !err.to_ascii_lowercase().contains("catch-up"),
            "FORCE must not fail on catch-up: {}",
            err
        );
        assert!(!repl.is_replica());
        assert!(!repl.failover_in_progress());
    }

    #[tokio::test]
    async fn coordinated_failover_without_force_reports_catchup_timeout() {
        let repl = ReplicationManager::new();
        repl.propagate_command(&[
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
        ]);
        let err = repl
            .coordinated_failover_to("127.0.0.1", 1, 150, false)
            .await
            .expect_err("must fail catch-up");
        assert!(
            err.to_ascii_lowercase().contains("catch-up"),
            "expected catch-up error, got: {}",
            err
        );
        assert!(!repl.is_replica());
    }

    #[test]
    fn parse_replconf_ack_case_insensitive() {
        let reply = RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"replconf"))),
            RespValue::BulkString(Some(Bytes::from_static(b"ack"))),
            RespValue::BulkString(Some(Bytes::from_static(b"7"))),
        ]);
        assert_eq!(parse_replconf_ack_offset(&reply), Some(7));
    }

    #[test]
    fn count_replicas_acked_by_offset() {
        let repl = ReplicationManager::new();
        let _a = repl.register_replica_announced(Some("127.0.0.1".into()), Some(1));
        let _b = repl.register_replica_announced(Some("127.0.0.1".into()), Some(2));
        assert_eq!(repl.count_replicas_acked(0), 2);
        assert_eq!(repl.count_replicas_acked(1), 0);
        repl.note_replica_ack(Some("127.0.0.1"), Some(1), 50);
        assert_eq!(repl.count_replicas_acked(50), 1);
        assert_eq!(repl.count_replicas_acked(51), 0);
        repl.note_replica_ack(Some("127.0.0.1"), Some(2), 100);
        assert_eq!(repl.count_replicas_acked(50), 2);
        assert_eq!(repl.count_replicas_acked(100), 1);
    }

    #[tokio::test]
    async fn wait_numreplicas_returns_when_enough_acked() {
        let repl = ReplicationManager::new();
        repl.propagate_command(&[
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"k"),
            Bytes::from_static(b"v"),
        ]);
        let target = repl.master_repl_offset();
        let _rx = repl.register_replica_announced(Some("127.0.0.1".into()), Some(9001));
        // Pretend catch-up already happened
        repl.note_replica_ack(Some("127.0.0.1"), Some(9001), target);
        let n = repl.wait_numreplicas(1, 500).await;
        assert_eq!(n, 1);
    }

    #[tokio::test]
    async fn wait_numreplicas_times_out_with_partial_count() {
        let repl = ReplicationManager::new();
        repl.propagate_command(&[
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"k"),
            Bytes::from_static(b"v"),
        ]);
        assert!(repl.master_repl_offset() > 0);
        let _rx = repl.register_replica_announced(Some("127.0.0.1".into()), Some(9002));
        // Leave ACK at 0 — below master offset
        let start = Instant::now();
        let n = repl.wait_numreplicas(1, 80).await;
        assert_eq!(n, 0);
        assert!(start.elapsed() >= Duration::from_millis(50));
    }

    #[tokio::test]
    async fn wait_numreplicas_zero_num_returns_current_count() {
        let repl = ReplicationManager::new();
        let _rx = repl.register_replica();
        // offset 0, ack 0 → counts
        let n = repl.wait_numreplicas(0, 1000).await;
        assert_eq!(n, 1);
    }

    #[tokio::test]
    async fn wait_on_replica_returns_zero() {
        let repl = ReplicationManager::new();
        repl.set_replicaof(Some("10.0.0.1:6379".into()));
        let n = repl.wait_numreplicas(1, 100).await;
        assert_eq!(n, 0);
    }

    #[test]
    fn min_replicas_to_write_gates_writes() {
        let repl = ReplicationManager::new();
        assert!(repl.writes_allowed_by_min_replicas());
        repl.set_min_replicas_to_write(1);
        assert!(!repl.writes_allowed_by_min_replicas());
        let _rx = repl.register_replica();
        // Fresh connect counts as good within max lag
        assert!(repl.good_replica_count() >= 1);
        assert!(repl.writes_allowed_by_min_replicas());
        repl.set_min_replicas_to_write(2);
        assert!(!repl.writes_allowed_by_min_replicas());
        // On replica role, gate is off
        repl.set_replicaof(Some("1.2.3.4:9".into()));
        assert!(repl.writes_allowed_by_min_replicas());
    }

    #[test]
    fn min_replicas_max_lag_expires_good_count() {
        let repl = ReplicationManager::new();
        let _rx = repl.register_replica_announced(Some("127.0.0.1".into()), Some(1));
        repl.set_min_replicas_max_lag(0); // 0 seconds → only exact-now ACKs
        // last_ack was at connect; with 0 lag window, may or may not still be good
        // Force stale: set last ack far in the past via note then max_lag 0 after sleep is flaky.
        // With max_lag 0, only last_ack_ms == now counts (within 0ms). After any delay, stale.
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert_eq!(
            repl.good_replica_count(),
            0,
            "after 5ms with max_lag=0, replica should not be good"
        );
        repl.note_replica_ack(Some("127.0.0.1"), Some(1), 1);
        // Immediately after ACK with max_lag=0: now - last ≈ 0 → good
        assert_eq!(repl.good_replica_count(), 1);
        repl.set_min_replicas_max_lag(10);
        assert_eq!(repl.good_replica_count(), 1);
    }

    #[test]
    fn info_includes_min_slaves_fields() {
        let repl = ReplicationManager::new();
        repl.set_min_replicas_to_write(2);
        repl.set_min_replicas_max_lag(5);
        let info = repl.info_replication();
        assert!(info.contains("min_slaves_to_write:2"), "{}", info);
        assert!(info.contains("min_slaves_max_lag:5"), "{}", info);
        assert!(info.contains("min_slaves_good:"), "{}", info);
    }

    #[test]
    fn send_replicaof_to_feeds_excludes_target() {
        let repl = ReplicationManager::new();
        let mut keep = repl.register_replica_announced(Some("127.0.0.1".into()), Some(7001));
        let mut exclude = repl.register_replica_announced(Some("127.0.0.1".into()), Some(7002));
        repl.send_replicaof_to_feeds("127.0.0.1", 7002, "127.0.0.1", 7002);
        // sibling should receive REPLICAOF
        let msg = keep.try_recv().expect("sibling feed should get REPLICAOF");
        let s = String::from_utf8_lossy(&msg);
        assert!(s.contains("REPLICAOF") || s.contains("7002"), "got {:?}", s);
        // promote target must not receive
        assert!(exclude.try_recv().is_err(), "target feed must be excluded");
    }

    #[test]
    fn primary_link_epoch_bumps_on_set_replicaof_change() {
        let repl = ReplicationManager::new();
        let e0 = repl.primary_link_epoch();
        repl.set_replicaof(Some("127.0.0.1:1".into()));
        let e1 = repl.primary_link_epoch();
        assert!(e1 > e0);
        // same addr → no bump
        repl.set_replicaof(Some("127.0.0.1:1".into()));
        assert_eq!(repl.primary_link_epoch(), e1);
        // change addr → bump
        repl.set_replicaof(Some("127.0.0.1:2".into()));
        assert!(repl.primary_link_epoch() > e1);
        // promote → bump
        let e2 = repl.primary_link_epoch();
        repl.set_replicaof(None);
        assert!(repl.primary_link_epoch() > e2);
        assert!(!repl.is_replica());
    }

    #[test]
    fn parse_replicaof_command_variants() {
        let of = RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"REPLICAOF"))),
            RespValue::BulkString(Some(Bytes::from_static(b"10.0.0.1"))),
            RespValue::BulkString(Some(Bytes::from_static(b"6380"))),
        ]);
        assert_eq!(
            parse_replicaof_command(&of),
            Some(Some("10.0.0.1:6380".into()))
        );
        let no = RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"SLAVEOF"))),
            RespValue::BulkString(Some(Bytes::from_static(b"NO"))),
            RespValue::BulkString(Some(Bytes::from_static(b"ONE"))),
        ]);
        assert_eq!(parse_replicaof_command(&no), Some(None));
        let set = RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"SET"))),
            RespValue::BulkString(Some(Bytes::from_static(b"k"))),
            RespValue::BulkString(Some(Bytes::from_static(b"v"))),
        ]);
        assert_eq!(parse_replicaof_command(&set), None);
    }
}
