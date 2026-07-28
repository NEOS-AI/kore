//! Thin slot migration + Redis key-level `MIGRATE` (shared RESP recreate path).
//!
//! Operator flow (Redis-like slot reshard):
//! 1. dest:  CLUSTER SETSLOT <s> IMPORTING <source-id>
//! 2. source: CLUSTER SETSLOT <s> MIGRATING <dest-id>
//! 3. source: CLUSTER MIGRATEKEYS <s> <dest-ip> <dest-port>
//! 4. both:   CLUSTER SETSLOT <s> NODE <dest-id>
//!
//! `CLUSTER RESHARD` runs steps 1–4 on the source for one slot or an inclusive
//! range. Dual-end NODE uses a **RESP prepare/vote then commit** path (Batch FB
//! + FH 2PC slices) — not Redis binary cluster-bus 2PC. After each side's NODE,
//! ownership is re-checked and failed NODE is retried a few times (Batch DN).
//! Partial failures leave honest status fields; operators can complete with
//! `CLUSTER RESHARD FINISH` or manual SETSLOT.
//!
//! **Partial key moves:** `migrate_slot_keys` deletes each source key only after
//! dest accepts it. On mid-slot failure, earlier keys already live on dest;
//! `MigrateSlotError::partial` (and RESHARD `migrated`/`skipped` under
//! `failed_keys`) report how many succeeded. **Retry re-runs MIGRATEKEYS /
//! RESHARD for leftover source keys only** — already-moved keys stay on dest.
//!
//! **Range abort:** multi-slot RESHARD stops after the first non-`complete`
//! status (`failed_*`, `failed_prepare`, or `partial_*_node`) so operators do
//! not cascade mixed ownership across a range.
//!
//! **Dual-end NODE 2PC (Batch FB/FH + DV/EH/EJ/EP/EY):**
//! 1. **Prepare/vote** on source + dest (`SETSLOT PREPARE <dest>`): MYID,
//!    ownership / MIGRATING / IMPORTING sanity; votes stamp slot config epoch
//!    + TTL (FH). Fail → `failed_prepare` with **no** NODE (ABORTPREPARE).
//! 2. **Commit re-check (FH):** both sides re-validate prepare (epoch/TTL/
//!    topology / MYID) via local + `SETSLOT CHECKPREPARE` before any NODE.
//!    Fail → `failed_prepare:recheck:…` without half-apply.
//! 3. **Commit** only after re-check: **dest** `SETSLOT COMMITPREPARE <dest>`
//!    first (atomic check+NODE, Batch FO), then **source**
//!    `commit_prepare_node` (Batch DV — no MOVED-to-IMPORTING if dest fails).
//!    Source re-checks prepare again atomically at commit.
//! 4. If dest owns but source NODE fails: EH re-asserts MIGRATING; EP rolls
//!    dest back to source (`NODE <source>` + `IMPORTING`) → `rolled_back`.
//! 5. Both ok → EJ post-commit dual verify (`partial_verify` on drift).
//! Ownership epochs (DU) fence stale gossip after NODE. Prepare votes are durable
//! in `nodes.conf` (`# prepare …`; Batch FO) with wall-clock TTL — expired /
//! missing votes fail closed on commit re-check.
//!
//! **Redis `MIGRATE` (Batch DP/DQ):** key-level transfer reuses
//! [`snapshot_key`] / [`recreate_commands`] / ASKING / RESP I/O. Options:
//! `COPY`, `REPLACE`, `AUTH`/`AUTH2`, multi-key via `KEYS`, `timeout` ms,
//! `destination-db` (SELECT on dest). No DUMP/RESTORE wire format.
//!
//! Supports string, hash, list, set, zset, geo, and stream keys. Dest writes use
//! ASKING so IMPORTING slots accept the transfer. Complex types are recreated
//! with the same RESP commands as AOF rewrite (no DUMP/RESTORE).
//!
//! **TTL (Batch DT):** expire is snapshotted as **absolute Unix-ms** end time
//! (`Cache::expire_time_unix_ms`) and applied on dest via string `SET … PXAT` or
//! trailing `PEXPIREAT`. This preserves the wall-clock end time under migrate
//! RTT/processing delay (Batch DQ used remaining-ms `PX`/`PEXPIRE`, which could
//! shrink lifetime). Multi-key mid-batch failure after ≥1 success returns
//! Redis-style `IOERR` including `migrated=` / `skipped=` counts (Batch DQ).

use super::crc16::{key_hash_slot, SLOT_COUNT};
use super::state::ClusterState;
use crate::cache::{Cache, KeyType};
use crate::entry::LoadOptions;
use crate::protocol::{RespParser, RespValue};
use crate::stream_type::{StreamId, StreamStateSnapshot};
use bytes::Bytes;
use std::fmt;
use std::sync::atomic::{AtomicU16, AtomicU32, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, MutexGuard};

/// Default I/O timeout for migrate RESP commands.
const MIGRATE_IO_TIMEOUT: Duration = Duration::from_secs(5);

/// Attempts per side for dual-end `SETSLOT NODE` (1 try + retries).
const NODE_SET_ATTEMPTS: u32 = 3;

/// Backoff between dual-end NODE retries (transient blips).
const NODE_RETRY_DELAY: Duration = Duration::from_millis(40);

/// Serializes tests that inject dest NODE failures (shared counter + port).
static DEST_NODE_INJECT_LOCK: Mutex<()> = Mutex::const_new(());

/// Only dual-end NODE toward this dest port is affected (0 = disabled).
static DEST_NODE_INJECT_PORT: AtomicU16 = AtomicU16::new(0);

/// Remaining forced failures for dest dual-end `SETSLOT NODE` (test hook).
static DEST_NODE_INJECT_FAILS: AtomicU32 = AtomicU32::new(0);

/// Serializes tests that inject dest PREPARE failures (Batch FB).
static DEST_PREPARE_INJECT_LOCK: Mutex<()> = Mutex::const_new(());

/// Only dual-end PREPARE toward this dest port is affected (0 = disabled).
static DEST_PREPARE_INJECT_PORT: AtomicU16 = AtomicU16::new(0);

/// Remaining forced failures for dest dual-end PREPARE (test hook).
static DEST_PREPARE_INJECT_FAILS: AtomicU32 = AtomicU32::new(0);

/// Serializes tests that inject mid-slot key-migrate failures.
static MIGRATE_KEY_INJECT_LOCK: Mutex<()> = Mutex::const_new(());

/// Fail the next key attempt after this many successful migrations (`u32::MAX` = off).
static MIGRATE_KEY_FAIL_AFTER: AtomicU32 = AtomicU32::new(u32::MAX);

/// Guard holding exclusive access to dest-NODE failure injection.
///
/// Injection is **port-scoped** so parallel RESHARD tests on other ports are
/// unaffected. Drop clears port + counter.
pub struct DestNodeInjectGuard {
    _lock: MutexGuard<'static, ()>,
}

impl DestNodeInjectGuard {
    /// Force the next `n` dest `SETSLOT NODE` attempts toward `dest_port` to fail.
    pub fn set_failures_for_port(&self, dest_port: u16, n: u32) {
        DEST_NODE_INJECT_PORT.store(dest_port, Ordering::SeqCst);
        DEST_NODE_INJECT_FAILS.store(n, Ordering::SeqCst);
    }
}

impl Drop for DestNodeInjectGuard {
    fn drop(&mut self) {
        DEST_NODE_INJECT_PORT.store(0, Ordering::SeqCst);
        DEST_NODE_INJECT_FAILS.store(0, Ordering::SeqCst);
    }
}

/// Acquire exclusive access for dest-NODE failure injection tests.
pub async fn test_acquire_dest_node_inject() -> DestNodeInjectGuard {
    DestNodeInjectGuard {
        _lock: DEST_NODE_INJECT_LOCK.lock().await,
    }
}

/// RAII clear for per-`ClusterState` source NODE injection (Batch EP).
///
/// Inject is stored on the cluster instance (not process-global) so parallel
/// integration tests cannot race.
pub struct SourceNodeInjectGuard {
    cluster: std::sync::Arc<ClusterState>,
}

impl SourceNodeInjectGuard {
    /// Force the next `n` local `SETSLOT NODE` attempts to fail on this cluster.
    pub fn set_failures(&self, n: u32) {
        self.cluster.test_inject_source_node_failures(n);
    }
}

impl Drop for SourceNodeInjectGuard {
    fn drop(&mut self) {
        self.cluster.test_clear_source_node_inject();
    }
}

/// Bind source-NODE failure injection to a specific [`ClusterState`] (Batch EP).
pub fn test_source_node_inject(cluster: std::sync::Arc<ClusterState>) -> SourceNodeInjectGuard {
    SourceNodeInjectGuard { cluster }
}

/// Guard holding exclusive access to dest-PREPARE failure injection (Batch FB).
pub struct DestPrepareInjectGuard {
    _lock: MutexGuard<'static, ()>,
}

impl DestPrepareInjectGuard {
    /// Force the next `n` dest PREPARE attempts toward `dest_port` to fail.
    pub fn set_failures_for_port(&self, dest_port: u16, n: u32) {
        DEST_PREPARE_INJECT_PORT.store(dest_port, Ordering::SeqCst);
        DEST_PREPARE_INJECT_FAILS.store(n, Ordering::SeqCst);
    }
}

impl Drop for DestPrepareInjectGuard {
    fn drop(&mut self) {
        DEST_PREPARE_INJECT_PORT.store(0, Ordering::SeqCst);
        DEST_PREPARE_INJECT_FAILS.store(0, Ordering::SeqCst);
    }
}

/// Acquire exclusive access for dest-PREPARE failure injection tests (Batch FB).
pub async fn test_acquire_dest_prepare_inject() -> DestPrepareInjectGuard {
    DestPrepareInjectGuard {
        _lock: DEST_PREPARE_INJECT_LOCK.lock().await,
    }
}

/// RAII clear for per-`ClusterState` source PREPARE injection (Batch FB).
pub struct SourcePrepareInjectGuard {
    cluster: std::sync::Arc<ClusterState>,
}

impl SourcePrepareInjectGuard {
    /// Force the next `n` local `SETSLOT PREPARE` attempts to fail.
    pub fn set_failures(&self, n: u32) {
        self.cluster.test_inject_prepare_failures(n);
    }
}

impl Drop for SourcePrepareInjectGuard {
    fn drop(&mut self) {
        self.cluster.test_clear_prepare_inject();
    }
}

/// Bind source-PREPARE failure injection to a specific [`ClusterState`] (Batch FB).
pub fn test_source_prepare_inject(
    cluster: std::sync::Arc<ClusterState>,
) -> SourcePrepareInjectGuard {
    SourcePrepareInjectGuard { cluster }
}

/// RAII clear for per-`ClusterState` commit re-check clear injection (Batch FH).
pub struct CommitRecheckInjectGuard {
    cluster: std::sync::Arc<ClusterState>,
}

impl CommitRecheckInjectGuard {
    /// Force the next `n` local commit re-checks to clear prepare first.
    pub fn set_clear_count(&self, n: u32) {
        self.cluster.test_inject_commit_recheck_clear(n);
    }
}

impl Drop for CommitRecheckInjectGuard {
    fn drop(&mut self) {
        self.cluster.test_clear_commit_recheck_inject();
    }
}

/// Bind commit-recheck clear injection to a specific [`ClusterState`] (Batch FH).
pub fn test_commit_recheck_inject(
    cluster: std::sync::Arc<ClusterState>,
) -> CommitRecheckInjectGuard {
    CommitRecheckInjectGuard { cluster }
}

/// Set inject for any dest port (prefer the port-scoped guard in suites).
pub fn test_inject_dest_node_failures(n: u32) {
    DEST_NODE_INJECT_PORT.store(u16::MAX, Ordering::SeqCst);
    DEST_NODE_INJECT_FAILS.store(n, Ordering::SeqCst);
}

/// Consume one injected failure if `dest_port` matches the active inject port.
fn take_dest_node_inject_fail(dest_port: u16) -> bool {
    let inject_port = DEST_NODE_INJECT_PORT.load(Ordering::SeqCst);
    if inject_port == 0 || (inject_port != u16::MAX && inject_port != dest_port) {
        return false;
    }
    DEST_NODE_INJECT_FAILS
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
            if n > 0 {
                Some(n - 1)
            } else {
                None
            }
        })
        .is_ok()
}

/// Consume one injected dest PREPARE failure if `dest_port` matches (Batch FB).
fn take_dest_prepare_inject_fail(dest_port: u16) -> bool {
    let inject_port = DEST_PREPARE_INJECT_PORT.load(Ordering::SeqCst);
    if inject_port == 0 || (inject_port != u16::MAX && inject_port != dest_port) {
        return false;
    }
    DEST_PREPARE_INJECT_FAILS
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
            if n > 0 {
                Some(n - 1)
            } else {
                None
            }
        })
        .is_ok()
}

/// Guard for mid-slot key-migrate failure injection (Batch DO tests).
///
/// Drop disables the inject (`fail_after = u32::MAX`).
pub struct MigrateKeyInjectGuard {
    _lock: MutexGuard<'static, ()>,
}

impl MigrateKeyInjectGuard {
    /// After `n` successful key migrations, force the next key attempt to fail.
    ///
    /// `n = 1` → first key moves, second returns `MigrateSlotError` with
    /// `partial.migrated == 1`.
    pub fn fail_after_successes(&self, n: u32) {
        MIGRATE_KEY_FAIL_AFTER.store(n, Ordering::SeqCst);
    }
}

impl Drop for MigrateKeyInjectGuard {
    fn drop(&mut self) {
        MIGRATE_KEY_FAIL_AFTER.store(u32::MAX, Ordering::SeqCst);
    }
}

/// Acquire exclusive access for mid-slot key-migrate failure injection tests.
pub async fn test_acquire_migrate_key_inject() -> MigrateKeyInjectGuard {
    MigrateKeyInjectGuard {
        _lock: MIGRATE_KEY_INJECT_LOCK.lock().await,
    }
}

/// Result of migrating keys for one slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrateSlotResult {
    /// Keys successfully moved (dest recreate + source DEL).
    pub migrated: usize,
    /// Keys skipped (gone mid-flight, empty, or unsupported).
    pub skipped: usize,
}

/// Mid-slot (or early) failure from [`migrate_slot_keys`], carrying partial progress.
///
/// Keys counted in `partial.migrated` already live on dest and were deleted from
/// source. Retry only needs to move remaining source keys in the slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrateSlotError {
    /// Progress before the failure (`migrated` / `skipped` so far).
    pub partial: MigrateSlotResult,
    /// Full error message (typically `ERR CLUSTER MIGRATEKEYS …`).
    pub message: String,
}

impl fmt::Display for MigrateSlotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for MigrateSlotError {}

fn migrate_err(partial: MigrateSlotResult, message: impl Into<String>) -> MigrateSlotError {
    MigrateSlotError {
        partial,
        message: message.into(),
    }
}

/// Outcome of orchestrated reshard for one slot (`CLUSTER RESHARD`).
///
/// Dual-end ownership is not atomic: `source_node` / `dest_node` report each
/// side's `SETSLOT NODE` independently. `status` summarizes recovery needs.
///
/// On `failed_keys`, `migrated`/`skipped` reflect partial progress (keys already
/// on dest). Retry MIGRATEKEYS/RESHARD moves only leftovers on source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReshardSlotResult {
    pub slot: u16,
    pub migrated: usize,
    pub skipped: usize,
    /// `"ok"` or error text for local `SETSLOT NODE <dest>`.
    pub source_node: String,
    /// `"ok"` or error text for remote `SETSLOT NODE <dest>`.
    pub dest_node: String,
    /// `complete` | `partial_dest_node` | `partial_source_node` | `failed_*`
    pub status: String,
    /// Optional operator note (e.g. FINISH when source still holds keys in slot).
    pub warning: Option<String>,
}

impl ReshardSlotResult {
    /// RESP2 array of flat field pairs for operator introspection.
    ///
    /// When [`Self::warning`] is set, appends `warning` / text after `status`.
    pub fn to_resp_array(&self) -> RespValue {
        let mut fields = vec![
            bulk_static(b"slot"),
            RespValue::Integer(self.slot as i64),
            bulk_static(b"migrated"),
            RespValue::Integer(self.migrated as i64),
            bulk_static(b"skipped"),
            RespValue::Integer(self.skipped as i64),
            bulk_static(b"source_node"),
            bulk_owned(self.source_node.clone()),
            bulk_static(b"dest_node"),
            bulk_owned(self.dest_node.clone()),
            bulk_static(b"status"),
            bulk_owned(self.status.clone()),
        ];
        if let Some(w) = &self.warning {
            fields.push(bulk_static(b"warning"));
            fields.push(bulk_owned(w.clone()));
        }
        RespValue::Array(fields)
    }
}

/// Back-compat alias field name used by older call sites.
impl MigrateSlotResult {
    /// Previously counted non-string keys left behind; now always 0 (all types migrate).
    #[deprecated(note = "use skipped; multi-type migrate no longer leaves non-string keys")]
    pub fn skipped_non_string(&self) -> usize {
        self.skipped
    }
}

/// Snapshot of one key ready for RESP recreate on the destination.
///
/// `expire_unix_ms` is absolute Unix-epoch milliseconds end time (`-1` = none),
/// from [`Cache::expire_time_unix_ms`]. Applied on dest via `SET … PXAT` (string)
/// or trailing `PEXPIREAT` (typed keys) so migrate RTT does not shrink lifetime
/// (Batch DT; previously remaining-ms `PX`/`PEXPIRE`).
enum KeySnapshot {
    String {
        value: Bytes,
        expire_unix_ms: i64,
    },
    Hash {
        fields: Vec<(Bytes, Bytes)>,
        expire_unix_ms: i64,
    },
    List {
        items: Vec<Bytes>,
        expire_unix_ms: i64,
    },
    Set {
        members: Vec<Bytes>,
        expire_unix_ms: i64,
    },
    ZSet {
        members: Vec<(Bytes, f64)>,
        expire_unix_ms: i64,
    },
    Geo {
        members: Vec<(Bytes, f64, f64)>,
        expire_unix_ms: i64,
    },
    Stream {
        state: StreamStateSnapshot,
        expire_unix_ms: i64,
    },
}

/// Return all non-expired keys currently stored whose hash slot equals `slot`.
///
/// Covers all key types (string, hash, list, …). Order is not guaranteed.
pub fn keys_in_slot(cache: &Cache, slot: u16) -> Vec<Bytes> {
    if slot >= SLOT_COUNT {
        return Vec::new();
    }
    cache
        .keys(None)
        .into_iter()
        .filter(|k| key_hash_slot(k) == slot)
        .collect()
}

/// String keys only in `slot` (non-expired).
pub fn string_keys_in_slot(cache: &Cache, slot: u16) -> Vec<Bytes> {
    if slot >= SLOT_COUNT {
        return Vec::new();
    }
    cache
        .map_keys_all()
        .into_iter()
        .filter(|k| key_hash_slot(k) == slot)
        .collect()
}

/// Snapshot a single key for migration. Returns `None` if the key is gone or empty.
fn snapshot_key(cache: &Cache, key: &Bytes) -> Option<KeySnapshot> {
    // Absolute Unix-ms end time (`-1` none, `-2` missing/expired). Prefer this
    // over remaining-ms so recreate can use PXAT/PEXPIREAT (Batch DT).
    let expire_unix_ms = cache.expire_time_unix_ms(key);
    if expire_unix_ms == -2 {
        return None;
    }
    match cache.key_type(key) {
        KeyType::None => None,
        KeyType::String => {
            let entry = cache
                .load(
                    key,
                    LoadOptions {
                        touch: false,
                        with_cas: false,
                    },
                )
                .ok()
                .flatten()?;
            Some(KeySnapshot::String {
                value: entry.value.clone(),
                expire_unix_ms,
            })
        }
        KeyType::Hash => {
            let h = cache.get_hash(key)?;
            let fields: Vec<_> = h.read().iter_fields().collect();
            if fields.is_empty() {
                return None;
            }
            Some(KeySnapshot::Hash {
                fields,
                expire_unix_ms,
            })
        }
        KeyType::List => {
            let l = cache.get_list(key)?;
            let items: Vec<_> = l.read().iter_items().collect();
            if items.is_empty() {
                return None;
            }
            Some(KeySnapshot::List {
                items,
                expire_unix_ms,
            })
        }
        KeyType::Set => {
            let s = cache.get_set(key)?;
            let members: Vec<_> = s.read().iter_members().collect();
            if members.is_empty() {
                return None;
            }
            Some(KeySnapshot::Set {
                members,
                expire_unix_ms,
            })
        }
        KeyType::ZSet => {
            let z = cache.get_sorted_set(key)?;
            let members: Vec<_> = z.read().iter_members().collect();
            if members.is_empty() {
                return None;
            }
            Some(KeySnapshot::ZSet {
                members,
                expire_unix_ms,
            })
        }
        KeyType::Geo => {
            let g = cache.get_geo_set(key)?;
            let members: Vec<_> = g.read().iter_members().collect();
            if members.is_empty() {
                return None;
            }
            Some(KeySnapshot::Geo {
                members,
                expire_unix_ms,
            })
        }
        KeyType::Stream => {
            let s = cache.get_stream(key)?;
            let state = s.read().export_state();
            // Allow empty streams when groups exist (Redis keeps them).
            if state.entries.is_empty() && state.groups.is_empty() {
                return None;
            }
            Some(KeySnapshot::Stream {
                state,
                expire_unix_ms,
            })
        }
    }
}

/// Append `PEXPIREAT key <unix-ms>` when an absolute expire is set (typed keys).
fn push_pexpireat_cmd(cmds: &mut Vec<Vec<RespValue>>, key: &Bytes, expire_unix_ms: i64) {
    if expire_unix_ms > 0 {
        cmds.push(vec![
            bulk_static(b"PEXPIREAT"),
            RespValue::BulkString(Some(key.clone())),
            bulk_owned(expire_unix_ms.to_string()),
        ]);
    }
}

/// Build the sequence of RESP command arrays needed to recreate `snap` at `key`.
///
/// String expire uses `SET … PXAT`; other types recreate value then `PEXPIREAT`
/// so the wall-clock end time is preserved under migrate latency (Batch DT).
fn recreate_commands(key: &Bytes, snap: &KeySnapshot) -> Vec<Vec<RespValue>> {
    match snap {
        KeySnapshot::String {
            value,
            expire_unix_ms,
        } => {
            let mut parts = vec![
                bulk_static(b"SET"),
                RespValue::BulkString(Some(key.clone())),
                RespValue::BulkString(Some(value.clone())),
            ];
            if *expire_unix_ms > 0 {
                parts.push(bulk_static(b"PXAT"));
                parts.push(bulk_owned(expire_unix_ms.to_string()));
            }
            vec![parts]
        }
        KeySnapshot::Hash {
            fields,
            expire_unix_ms,
        } => {
            let mut parts = vec![
                bulk_static(b"HSET"),
                RespValue::BulkString(Some(key.clone())),
            ];
            for (f, v) in fields {
                parts.push(RespValue::BulkString(Some(f.clone())));
                parts.push(RespValue::BulkString(Some(v.clone())));
            }
            let mut cmds = vec![parts];
            push_pexpireat_cmd(&mut cmds, key, *expire_unix_ms);
            cmds
        }
        KeySnapshot::List {
            items,
            expire_unix_ms,
        } => {
            let mut parts = vec![
                bulk_static(b"RPUSH"),
                RespValue::BulkString(Some(key.clone())),
            ];
            for e in items {
                parts.push(RespValue::BulkString(Some(e.clone())));
            }
            let mut cmds = vec![parts];
            push_pexpireat_cmd(&mut cmds, key, *expire_unix_ms);
            cmds
        }
        KeySnapshot::Set {
            members,
            expire_unix_ms,
        } => {
            let mut parts = vec![
                bulk_static(b"SADD"),
                RespValue::BulkString(Some(key.clone())),
            ];
            for m in members {
                parts.push(RespValue::BulkString(Some(m.clone())));
            }
            let mut cmds = vec![parts];
            push_pexpireat_cmd(&mut cmds, key, *expire_unix_ms);
            cmds
        }
        KeySnapshot::ZSet {
            members,
            expire_unix_ms,
        } => {
            let mut parts = vec![
                bulk_static(b"ZADD"),
                RespValue::BulkString(Some(key.clone())),
            ];
            for (m, score) in members {
                parts.push(bulk_owned(score_string(*score)));
                parts.push(RespValue::BulkString(Some(m.clone())));
            }
            let mut cmds = vec![parts];
            push_pexpireat_cmd(&mut cmds, key, *expire_unix_ms);
            cmds
        }
        KeySnapshot::Geo {
            members,
            expire_unix_ms,
        } => {
            let mut parts = vec![
                bulk_static(b"GEOADD"),
                RespValue::BulkString(Some(key.clone())),
            ];
            for (m, lon, lat) in members {
                parts.push(bulk_owned(score_string(*lon)));
                parts.push(bulk_owned(score_string(*lat)));
                parts.push(RespValue::BulkString(Some(m.clone())));
            }
            let mut cmds = vec![parts];
            push_pexpireat_cmd(&mut cmds, key, *expire_unix_ms);
            cmds
        }
        KeySnapshot::Stream {
            state,
            expire_unix_ms,
        } => {
            let mut cmds = stream_recreate_commands(key, state);
            push_pexpireat_cmd(&mut cmds, key, *expire_unix_ms);
            cmds
        }
    }
}

fn stream_recreate_commands(key: &Bytes, state: &StreamStateSnapshot) -> Vec<Vec<RespValue>> {
    let mut cmds = Vec::new();
    for (id, fields) in &state.entries {
        let mut parts = vec![
            bulk_static(b"XADD"),
            RespValue::BulkString(Some(key.clone())),
            bulk_owned(id.to_string_id()),
        ];
        for (f, v) in fields {
            parts.push(RespValue::BulkString(Some(f.clone())));
            parts.push(RespValue::BulkString(Some(v.clone())));
        }
        if parts.len() > 3 {
            cmds.push(parts);
        }
    }
    let max_entry = state
        .entries
        .iter()
        .map(|(id, _)| *id)
        .max()
        .unwrap_or(StreamId::ZERO);
    if state.last_generated_id > max_entry {
        cmds.push(vec![
            bulk_static(b"XSETID"),
            RespValue::BulkString(Some(key.clone())),
            bulk_owned(state.last_generated_id.to_string_id()),
        ]);
    }
    for g in &state.groups {
        cmds.push(vec![
            bulk_static(b"XGROUP"),
            bulk_static(b"CREATE"),
            RespValue::BulkString(Some(key.clone())),
            RespValue::BulkString(Some(g.name.clone())),
            bulk_owned(g.last_delivered_id.to_string_id()),
            bulk_static(b"MKSTREAM"),
        ]);
        cmds.push(vec![
            bulk_static(b"XGROUP"),
            bulk_static(b"SETID"),
            RespValue::BulkString(Some(key.clone())),
            RespValue::BulkString(Some(g.name.clone())),
            bulk_owned(g.last_delivered_id.to_string_id()),
        ]);
        for pe in &g.pending {
            cmds.push(vec![
                bulk_static(b"XCLAIM"),
                RespValue::BulkString(Some(key.clone())),
                RespValue::BulkString(Some(g.name.clone())),
                RespValue::BulkString(Some(pe.consumer.clone())),
                bulk_static(b"0"),
                bulk_owned(pe.id.to_string_id()),
                bulk_static(b"FORCE"),
                bulk_static(b"TIME"),
                bulk_owned(pe.delivery_time_ms.to_string()),
                bulk_static(b"RETRYCOUNT"),
                bulk_owned(pe.delivery_count.max(1).to_string()),
            ]);
        }
    }
    cmds
}

fn score_string(v: f64) -> String {
    // Compact Redis-friendly float formatting
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{}", v)
    }
}

/// Expect a successful write reply (OK / integer ≥ 0 / bulk id / array).
fn accept_write_reply(reply: &RespValue, cmd: &str, ctx: &str) -> Result<(), String> {
    match reply {
        RespValue::SimpleString(s) if s.as_ref() == b"OK" => Ok(()),
        RespValue::Integer(n) if *n >= 0 => Ok(()),
        // XADD returns the entry id as a bulk string
        RespValue::BulkString(Some(_)) => Ok(()),
        // XCLAIM returns array of claimed entries
        RespValue::Array(_) => Ok(()),
        RespValue::Error(e) => Err(format!(
            "ERR {} {} failed: {}",
            ctx,
            cmd,
            String::from_utf8_lossy(e)
        )),
        other => Err(format!(
            "ERR {} unexpected {} reply: {:?}",
            ctx, cmd, other
        )),
    }
}

/// Options for transferring one or more keys over RESP (shared by MIGRATEKEYS + MIGRATE).
#[derive(Debug, Clone)]
pub struct MigrateKeyOpts {
    /// Leave the source key in place after dest accepts the recreate.
    pub copy: bool,
    /// If true, `DEL` the dest key before recreate (overwrite). If false, fail with
    /// `BUSYKEY` when the dest already has the key.
    pub replace: bool,
    /// Issue `ASKING` before each dest command (needed for IMPORTING slots).
    pub asking: bool,
    /// Connect / read / write timeout.
    pub io_timeout: Duration,
}

impl Default for MigrateKeyOpts {
    fn default() -> Self {
        Self {
            copy: false,
            replace: true,
            asking: true,
            io_timeout: MIGRATE_IO_TIMEOUT,
        }
    }
}

/// Per-key outcome from [`migrate_one_key_on_stream`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrateOneOutcome {
    /// Key recreated on dest; deleted on source unless `copy`.
    Migrated,
    /// Source key missing/empty — nothing transferred.
    Missing,
}

/// Redis-style `MIGRATE` result (success path).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrateCommandResult {
    /// At least one key moved/copied successfully.
    Ok,
    /// None of the requested keys existed on the source.
    NoKey,
}

/// Destination credentials / logical DB for Redis `MIGRATE`.
#[derive(Debug, Clone, Default)]
pub struct MigrateDestAuth {
    /// `AUTH password` (no username).
    pub password: Option<String>,
    /// `AUTH username password` (AUTH2). Takes precedence over bare password when set.
    pub username: Option<String>,
    /// Destination logical database index (`SELECT` when non-zero).
    pub dest_db: i64,
}

/// Transfer a single key over an open destination stream.
///
/// Steps: snapshot → (optional EXISTS/DEL) → ASKING + recreate cmds → source DEL
/// unless `copy`. Returns [`MigrateOneOutcome::Missing`] when the key is gone or empty.
pub async fn migrate_one_key_on_stream(
    cache: &Cache,
    stream: &mut TcpStream,
    key: &Bytes,
    opts: &MigrateKeyOpts,
) -> Result<MigrateOneOutcome, String> {
    let ctx = "MIGRATE";
    let snap = match snapshot_key(cache, key) {
        Some(s) => s,
        None => return Ok(MigrateOneOutcome::Missing),
    };
    let cmds = recreate_commands(key, &snap);
    if cmds.is_empty() {
        return Ok(MigrateOneOutcome::Missing);
    }

    // Without REPLACE: refuse if dest already holds the key (Redis BUSYKEY).
    if !opts.replace {
        issue_asking(stream, opts).await?;
        match resp_command_bytes(
            stream,
            &[
                bulk_static(b"EXISTS"),
                RespValue::BulkString(Some(key.clone())),
            ],
            opts.io_timeout,
        )
        .await
        {
            Ok(RespValue::Integer(n)) if n > 0 => {
                return Err("BUSYKEY Target key name already exists.".into());
            }
            Ok(RespValue::Integer(_)) => {}
            Ok(RespValue::Error(e)) => {
                return Err(format!(
                    "ERR {} EXISTS failed: {}",
                    ctx,
                    String::from_utf8_lossy(&e)
                ));
            }
            Ok(other) => {
                return Err(format!("ERR {} unexpected EXISTS reply: {:?}", ctx, other));
            }
            Err(e) => return Err(format!("ERR {} {}", ctx, e)),
        }
    } else {
        // REPLACE: clear dest key first so complex types do not merge into leftovers.
        issue_asking(stream, opts).await?;
        match resp_command_bytes(
            stream,
            &[
                bulk_static(b"DEL"),
                RespValue::BulkString(Some(key.clone())),
            ],
            opts.io_timeout,
        )
        .await
        {
            Ok(RespValue::Integer(_)) => {}
            Ok(RespValue::Error(e)) => {
                return Err(format!(
                    "ERR {} DEL (replace) failed: {}",
                    ctx,
                    String::from_utf8_lossy(&e)
                ));
            }
            Ok(other) => {
                return Err(format!(
                    "ERR {} unexpected DEL (replace) reply: {:?}",
                    ctx, other
                ));
            }
            Err(e) => return Err(format!("ERR {} {}", ctx, e)),
        }
    }

    for (i, parts) in cmds.iter().enumerate() {
        issue_asking(stream, opts).await?;

        let cmd_name = parts
            .first()
            .and_then(|p| p.as_bulk_string())
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .unwrap_or_else(|| "CMD".into());
        match resp_command_bytes(stream, parts, opts.io_timeout).await {
            Ok(reply) => {
                if let Err(e) = accept_write_reply(&reply, &cmd_name, ctx) {
                    return Err(format!(
                        "{} (key={}, step={})",
                        e,
                        String::from_utf8_lossy(key),
                        i
                    ));
                }
            }
            Err(e) => {
                return Err(format!(
                    "ERR {} {} I/O for key {}: {}",
                    ctx,
                    cmd_name,
                    String::from_utf8_lossy(key),
                    e
                ));
            }
        }
    }

    if !opts.copy {
        cache
            .delete(key)
            .map_err(|e| format!("ERR {} DEL failed after migrate: {}", ctx, e))?;
    }
    Ok(MigrateOneOutcome::Migrated)
}

async fn issue_asking(stream: &mut TcpStream, opts: &MigrateKeyOpts) -> Result<(), String> {
    if !opts.asking {
        return Ok(());
    }
    match resp_command_bytes(stream, &[bulk_static(b"ASKING")], opts.io_timeout).await {
        Ok(RespValue::SimpleString(s)) if s.as_ref() == b"OK" => Ok(()),
        // Standalone / non-cluster dests reject ASKING — treat as no-op so MIGRATE
        // still works without requiring --cluster-enabled on the destination.
        Ok(RespValue::Error(e))
            if String::from_utf8_lossy(&e).contains("cluster support disabled") =>
        {
            Ok(())
        }
        Ok(RespValue::Error(e)) => Err(format!(
            "ERR MIGRATE ASKING failed: {}",
            String::from_utf8_lossy(&e)
        )),
        Ok(other) => Err(format!("ERR MIGRATE unexpected ASKING reply: {:?}", other)),
        Err(e) => Err(format!("ERR MIGRATE {}", e)),
    }
}

/// Connect to dest, optionally AUTH + SELECT, and migrate `keys` via RESP recreate.
///
/// Redis-compatible success replies are expressed as [`MigrateCommandResult`].
/// On mid-batch failure after one or more keys succeeded, returns an `IOERR`
/// string that includes `migrated=` / `skipped=` counts (Batch DQ); already-migrated
/// keys stay on dest (and are gone from source unless `copy`). Retry only leftover
/// source keys.
///
/// Keys that were successfully deleted from source (not `copy`) are returned in
/// `deleted_keys` so the command handler can AOF/repl-propagate `DEL`.
pub async fn migrate_keys_to(
    cache: &Cache,
    dest_ip: &str,
    dest_port: u16,
    keys: &[Bytes],
    opts: &MigrateKeyOpts,
    auth: &MigrateDestAuth,
) -> Result<(MigrateCommandResult, Vec<Bytes>), String> {
    if keys.is_empty() {
        return Ok((MigrateCommandResult::NoKey, Vec::new()));
    }

    let addr = format!("{}:{}", dest_ip, dest_port);
    let mut stream = connect_dest_with_timeout(dest_ip, dest_port, opts.io_timeout)
        .await
        .map_err(|e| format!("ERR MIGRATE {}", e))?;

    // Probe ASKING once: disable for standalone dests (cluster support disabled).
    let mut opts = opts.clone();
    if opts.asking {
        match resp_command_bytes(&mut stream, &[bulk_static(b"ASKING")], opts.io_timeout).await {
            Ok(RespValue::SimpleString(s)) if s.as_ref() == b"OK" => {
                // Dest accepted ASKING; keep issuing before each write (one-shot).
            }
            Ok(RespValue::Error(e))
                if String::from_utf8_lossy(&e).contains("cluster support disabled") =>
            {
                opts.asking = false;
            }
            Ok(RespValue::Error(e)) => {
                return Err(format!(
                    "ERR MIGRATE ASKING failed: {}",
                    String::from_utf8_lossy(&e)
                ));
            }
            Ok(other) => {
                return Err(format!("ERR MIGRATE unexpected ASKING reply: {:?}", other));
            }
            Err(e) => return Err(format!("ERR MIGRATE {}", e)),
        }
    }
    let opts = &opts;

    // AUTH / AUTH2
    if let Some(user) = auth.username.as_ref() {
        let pass = auth.password.as_deref().unwrap_or("");
        match resp_command_bytes(
            &mut stream,
            &[
                bulk_static(b"AUTH"),
                bulk_owned(user.clone()),
                bulk_owned(pass.to_string()),
            ],
            opts.io_timeout,
        )
        .await
        {
            Ok(RespValue::SimpleString(s)) if s.as_ref() == b"OK" => {}
            Ok(RespValue::Error(e)) => {
                return Err(format!(
                    "ERR MIGRATE AUTH failed: {}",
                    String::from_utf8_lossy(&e)
                ));
            }
            Ok(other) => {
                return Err(format!("ERR MIGRATE unexpected AUTH reply: {:?}", other));
            }
            Err(e) => return Err(format!("ERR MIGRATE AUTH I/O: {}", e)),
        }
    } else if let Some(pass) = auth.password.as_ref() {
        match resp_command_bytes(
            &mut stream,
            &[bulk_static(b"AUTH"), bulk_owned(pass.clone())],
            opts.io_timeout,
        )
        .await
        {
            Ok(RespValue::SimpleString(s)) if s.as_ref() == b"OK" => {}
            Ok(RespValue::Error(e)) => {
                return Err(format!(
                    "ERR MIGRATE AUTH failed: {}",
                    String::from_utf8_lossy(&e)
                ));
            }
            Ok(other) => {
                return Err(format!("ERR MIGRATE unexpected AUTH reply: {:?}", other));
            }
            Err(e) => return Err(format!("ERR MIGRATE AUTH I/O: {}", e)),
        }
    }

    // destination-db: SELECT when non-zero (cluster dest rejects SELECT).
    if auth.dest_db != 0 {
        if auth.dest_db < 0 {
            return Err("ERR DB index is out of range".into());
        }
        match resp_command_bytes(
            &mut stream,
            &[
                bulk_static(b"SELECT"),
                bulk_owned(auth.dest_db.to_string()),
            ],
            opts.io_timeout,
        )
        .await
        {
            Ok(RespValue::SimpleString(s)) if s.as_ref() == b"OK" => {}
            Ok(RespValue::Error(e)) => {
                return Err(format!(
                    "ERR MIGRATE SELECT failed: {}",
                    String::from_utf8_lossy(&e)
                ));
            }
            Ok(other) => {
                return Err(format!("ERR MIGRATE unexpected SELECT reply: {:?}", other));
            }
            Err(e) => return Err(format!("ERR MIGRATE SELECT I/O: {}", e)),
        }
    }

    let mut migrated = 0usize;
    let mut skipped = 0usize;
    let mut deleted_keys = Vec::new();

    for key in keys {
        // Test inject: fail after N successful migrations (shared with MIGRATEKEYS).
        let fail_after = MIGRATE_KEY_FAIL_AFTER.load(Ordering::SeqCst);
        if fail_after != u32::MAX && (migrated as u32) >= fail_after {
            if migrated > 0 {
                return Err(format_partial_ioerr(&addr, "injected mid-batch failure", migrated, skipped));
            }
            return Err("ERR MIGRATE injected mid-batch failure".into());
        }

        match migrate_one_key_on_stream(cache, &mut stream, key, opts).await {
            Ok(MigrateOneOutcome::Migrated) => {
                migrated += 1;
                if !opts.copy {
                    deleted_keys.push(key.clone());
                }
            }
            Ok(MigrateOneOutcome::Missing) => {
                skipped += 1;
            }
            Err(e) => {
                // Redis uses IOERR when the link fails after partial progress.
                if migrated > 0 {
                    return Err(format_partial_ioerr(&addr, &e, migrated, skipped));
                }
                return Err(e);
            }
        }
    }

    if migrated > 0 {
        Ok((MigrateCommandResult::Ok, deleted_keys))
    } else {
        Ok((MigrateCommandResult::NoKey, deleted_keys))
    }
}

/// Redis-ish multi-key partial failure: IOERR + honest progress counts.
fn format_partial_ioerr(addr: &str, detail: &str, migrated: usize, skipped: usize) -> String {
    format!(
        "IOERR error or timeout transferring key to {} ({}). \
         Partial keys may have moved: migrated={} skipped={}.",
        addr, detail, migrated, skipped
    )
}

async fn connect_dest_with_timeout(
    dest_ip: &str,
    dest_port: u16,
    timeout: Duration,
) -> Result<TcpStream, String> {
    let addr = format!("{}:{}", dest_ip, dest_port);
    match tokio::time::timeout(timeout, TcpStream::connect(&addr)).await {
        Ok(Ok(s)) => {
            let _ = s.set_nodelay(true);
            Ok(s)
        }
        Ok(Err(e)) => Err(format!("unable to connect to {}: {}", addr, e)),
        Err(_) => Err(format!("timed out connecting to {}", addr)),
    }
}

/// Migrate all keys in `slot` from `cache` to `dest_ip:dest_port` via RESP.
///
/// For each key:
/// 1. Snapshot local value (all types)
/// 2. ASKING + recreate command(s) on dest
/// 3. DEL on source after successful recreate
///
/// # Partial failure
///
/// On error after one or more keys succeeded, returns [`MigrateSlotError`] with
/// `partial.migrated` / `partial.skipped` set to progress so far. Those migrated
/// keys already live on dest and are gone from source. **Retry** this function
/// (or RESHARD) for the same slot: only leftover source keys are moved again.
pub async fn migrate_slot_keys(
    cache: &Cache,
    slot: u16,
    dest_ip: &str,
    dest_port: u16,
) -> Result<MigrateSlotResult, MigrateSlotError> {
    let empty = MigrateSlotResult {
        migrated: 0,
        skipped: 0,
    };
    if slot >= SLOT_COUNT {
        return Err(migrate_err(
            empty,
            format!("ERR Invalid or out of range slot: {}", slot),
        ));
    }

    let keys = keys_in_slot(cache, slot);
    if keys.is_empty() {
        return Ok(empty);
    }

    let addr = format!("{}:{}", dest_ip, dest_port);
    let mut stream = match connect_dest_with_timeout(dest_ip, dest_port, MIGRATE_IO_TIMEOUT).await {
        Ok(s) => s,
        Err(e) => {
            return Err(migrate_err(
                empty,
                format!("ERR CLUSTER MIGRATEKEYS {}", e),
            ))
        }
    };

    let opts = MigrateKeyOpts {
        copy: false,
        replace: true,
        asking: true,
        io_timeout: MIGRATE_IO_TIMEOUT,
    };

    let mut migrated = 0usize;
    let mut skipped = 0usize;

    for key in keys {
        let progress = || MigrateSlotResult { migrated, skipped };

        // Test inject: fail after N successful migrations (see MigrateKeyInjectGuard).
        let fail_after = MIGRATE_KEY_FAIL_AFTER.load(Ordering::SeqCst);
        if fail_after != u32::MAX && (migrated as u32) >= fail_after {
            return Err(migrate_err(
                progress(),
                "ERR CLUSTER MIGRATEKEYS injected mid-slot failure",
            ));
        }

        match migrate_one_key_on_stream(cache, &mut stream, &key, &opts).await {
            Ok(MigrateOneOutcome::Migrated) => migrated += 1,
            Ok(MigrateOneOutcome::Missing) => skipped += 1,
            Err(e) => {
                // Rewrite generic MIGRATE prefix to CLUSTER MIGRATEKEYS for operators.
                let msg = e.replace("ERR MIGRATE", "ERR CLUSTER MIGRATEKEYS");
                let msg = if msg.starts_with("BUSYKEY") {
                    format!("ERR CLUSTER MIGRATEKEYS {}", msg)
                } else if msg.starts_with("ERR ") {
                    msg
                } else {
                    format!("ERR CLUSTER MIGRATEKEYS {} (key={}, dest={})", msg, String::from_utf8_lossy(&key), addr)
                };
                return Err(migrate_err(progress(), msg));
            }
        }
    }

    Ok(MigrateSlotResult { migrated, skipped })
}

/// Back-compat name: now migrates **all** key types (not only strings).
pub async fn migrate_slot_string_keys(
    cache: &Cache,
    slot: u16,
    dest_ip: &str,
    dest_port: u16,
) -> Result<MigrateSlotResult, MigrateSlotError> {
    migrate_slot_keys(cache, slot, dest_ip, dest_port).await
}

/// Orchestrate the documented 4-step reshard for slots `start..=end` to `dest_node_id`.
///
/// Runs on the **source** (this node must own each slot). For every slot:
/// 0. Best-effort: dest `SETSLOT NODE <source-id>` so dest does not claim ownership
/// 1. dest `SETSLOT IMPORTING <source-id>`
/// 2. source `SETSLOT MIGRATING <dest-id>`
/// 3. `MIGRATEKEYS` (all types)
/// 4. dual-end NODE with **prepare/vote then commit** (Batch FB 2PC slice) +
///    verify+retry (DN); dest-first commit (DV); EP rollback on source commit fail
///
/// On key-move failure the slot is left MIGRATING/IMPORTING for operator recovery;
/// `migrated`/`skipped` on `failed_keys` report partial progress (retry leftover keys only).
/// Dual-end NODE failures are reported in `ReshardSlotResult` rather than rolled back;
/// use [`finish_slot_node`] / `CLUSTER RESHARD FINISH` to complete NODE without re-migrating.
///
/// **Range policy (Batch DO/FB):** abort further slots on any non-`complete` status
/// (`failed_*`, `failed_prepare`, or `partial_*_node`) so a mid-range prepare or
/// commit failure does not cascade mixed ownership.
pub async fn reshard_slots(
    cache: &Cache,
    cluster: &ClusterState,
    start: u16,
    end: u16,
    dest_node_id: &str,
) -> Result<Vec<ReshardSlotResult>, String> {
    if start > end || end >= SLOT_COUNT {
        return Err(format!(
            "ERR Invalid or out of range slot range: {}-{}",
            start, end
        ));
    }
    if dest_node_id == cluster.my_id() {
        return Err("ERR CLUSTER RESHARD destination cannot be myself".into());
    }
    let dest = cluster.get_node(dest_node_id).ok_or_else(|| {
        format!(
            "ERR CLUSTER RESHARD I don't know about node {}",
            dest_node_id
        )
    })?;
    if dest.fail {
        return Err(format!(
            "ERR CLUSTER RESHARD destination node {} is marked fail",
            dest_node_id
        ));
    }

    let mut out = Vec::with_capacity((end - start + 1) as usize);
    for slot in start..=end {
        // Hard stop on slots we do not own (before mutating remote state).
        if !cluster.owns_slot(slot) {
            return Err(format!(
                "ERR I'm not the owner of hash slot {} (cannot RESHARD)",
                slot
            ));
        }
        match reshard_one_slot(cache, cluster, slot, dest_node_id, &dest.ip, dest.port).await {
            Ok(r) => {
                // Abort remaining range slots after any non-complete outcome
                // (failed_* orchestration errors or partial_* dual-end NODE).
                let abort_range = reshard_range_should_abort(&r.status);
                out.push(r);
                if abort_range {
                    break;
                }
            }
            Err(e) => return Err(e),
        }
    }
    Ok(out)
}

/// Multi-slot RESHARD stops after the first slot whose status is not `complete`.
fn reshard_range_should_abort(status: &str) -> bool {
    // Includes partial_verify (EJ), rolled_back (EP), failed_prepare (FB/EY).
    status != "complete"
}

/// Single-slot convenience wrapper.
pub async fn reshard_slot(
    cache: &Cache,
    cluster: &ClusterState,
    slot: u16,
    dest_node_id: &str,
) -> Result<ReshardSlotResult, String> {
    let mut v = reshard_slots(cache, cluster, slot, slot, dest_node_id).await?;
    v.pop()
        .ok_or_else(|| "ERR CLUSTER RESHARD internal: empty result".to_string())
}

/// One planned slot move for `CLUSTER RESHARD PLAN` / `AUTO` (Batch DX).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReshardPlanEntry {
    pub slot: u16,
    pub source_id: String,
    pub source_ip: String,
    pub source_port: u16,
}

impl ReshardPlanEntry {
    pub fn to_resp_array(&self) -> RespValue {
        RespValue::Array(vec![
            bulk_static(b"slot"),
            RespValue::Integer(self.slot as i64),
            bulk_static(b"source_id"),
            RespValue::BulkString(Some(Bytes::from(self.source_id.clone()))),
            bulk_static(b"source_ip"),
            RespValue::BulkString(Some(Bytes::from(self.source_ip.clone()))),
            bulk_static(b"source_port"),
            RespValue::Integer(self.source_port as i64),
        ])
    }
}

/// Build a greedy reshard plan: take up to `num_slots` from masters that have
/// the most slots (excluding dest and failed nodes). Stable slot order within
/// each source. Does **not** move data.
///
/// Empty when dest already owns enough of the map or no donor slots exist.
pub fn plan_reshard(
    cluster: &ClusterState,
    dest_node_id: &str,
    num_slots: usize,
) -> Result<Vec<ReshardPlanEntry>, String> {
    if num_slots == 0 {
        return Err("ERR CLUSTER RESHARD PLAN num-slots must be > 0".into());
    }
    if num_slots > SLOT_COUNT as usize {
        return Err(format!(
            "ERR CLUSTER RESHARD PLAN num-slots must be <= {}",
            SLOT_COUNT
        ));
    }
    let dest = cluster.get_node(dest_node_id).ok_or_else(|| {
        format!(
            "ERR CLUSTER RESHARD PLAN I don't know about node {}",
            dest_node_id
        )
    })?;
    if dest.fail {
        return Err(format!(
            "ERR CLUSTER RESHARD PLAN destination node {} is marked fail",
            dest_node_id
        ));
    }

    // owner_id → list of slots (ascending).
    let mut by_owner: std::collections::HashMap<String, Vec<u16>> =
        std::collections::HashMap::new();
    for slot in 0..SLOT_COUNT {
        let Some(owner_id) = cluster.owner_id_of(slot) else {
            continue;
        };
        if owner_id == dest_node_id {
            continue;
        }
        let Some(owner) = cluster.get_node(&owner_id) else {
            continue;
        };
        if owner.fail || !owner.master {
            continue;
        }
        by_owner.entry(owner_id).or_default().push(slot);
    }

    // Greedy: donors with the most slots first; tie-break by id for stability.
    let mut donors: Vec<(String, Vec<u16>)> = by_owner.into_iter().collect();
    donors.sort_by(|a, b| {
        b.1.len()
            .cmp(&a.1.len())
            .then_with(|| a.0.cmp(&b.0))
    });

    let mut plan = Vec::with_capacity(num_slots.min(SLOT_COUNT as usize));
    for (source_id, slots) in donors {
        if plan.len() >= num_slots {
            break;
        }
        let Some(src) = cluster.get_node(&source_id) else {
            continue;
        };
        for slot in slots {
            if plan.len() >= num_slots {
                break;
            }
            plan.push(ReshardPlanEntry {
                slot,
                source_id: source_id.clone(),
                source_ip: src.ip.clone(),
                source_port: src.port,
            });
        }
    }
    Ok(plan)
}

/// Execute a reshard plan: local slots via [`reshard_slot`]; remote sources via
/// RESP `CLUSTER RESHARD <slot> <dest>` on the source (Batch DX coordinator).
///
/// Aborts further entries after the first non-`complete` status (same policy as
/// multi-slot RESHARD). Not 2PC; partial progress is honest per entry.
pub async fn execute_reshard_plan(
    cache: &Cache,
    cluster: &ClusterState,
    dest_node_id: &str,
    plan: &[ReshardPlanEntry],
) -> Result<Vec<ReshardSlotResult>, String> {
    if dest_node_id == cluster.my_id() {
        return Err("ERR CLUSTER RESHARD AUTO destination cannot be myself".into());
    }
    let dest = cluster.get_node(dest_node_id).ok_or_else(|| {
        format!(
            "ERR CLUSTER RESHARD AUTO I don't know about node {}",
            dest_node_id
        )
    })?;
    if dest.fail {
        return Err(format!(
            "ERR CLUSTER RESHARD AUTO destination node {} is marked fail",
            dest_node_id
        ));
    }

    let my_id = cluster.my_id();
    let mut out = Vec::with_capacity(plan.len());
    for entry in plan {
        let result = if entry.source_id == my_id {
            if !cluster.owns_slot(entry.slot) {
                ReshardSlotResult {
                    slot: entry.slot,
                    migrated: 0,
                    skipped: 0,
                    source_node: "n/a".into(),
                    dest_node: "n/a".into(),
                    status: "failed_not_owner".into(),
                    warning: Some(format!(
                        "plan source is self but slot {} not owned locally",
                        entry.slot
                    )),
                }
            } else {
                reshard_slot(cache, cluster, entry.slot, dest_node_id).await?
            }
        } else {
            remote_reshard_one_slot(entry, dest_node_id).await
        };
        let abort = reshard_range_should_abort(&result.status);
        out.push(result);
        if abort {
            break;
        }
    }
    Ok(out)
}

/// Ask a remote source to run `CLUSTER RESHARD <slot> <dest-id>`.
async fn remote_reshard_one_slot(
    entry: &ReshardPlanEntry,
    dest_node_id: &str,
) -> ReshardSlotResult {
    let slot_s = entry.slot.to_string();
    let mut stream = match connect_dest(&entry.source_ip, entry.source_port).await {
        Ok(s) => s,
        Err(e) => {
            return ReshardSlotResult {
                slot: entry.slot,
                migrated: 0,
                skipped: 0,
                source_node: "n/a".into(),
                dest_node: "n/a".into(),
                status: format!("failed_connect:{}", e),
                warning: Some(format!(
                    "remote source {}:{}",
                    entry.source_ip, entry.source_port
                )),
            };
        }
    };

    let args = vec![
        RespValue::BulkString(Some(Bytes::from_static(b"CLUSTER"))),
        RespValue::BulkString(Some(Bytes::from_static(b"RESHARD"))),
        RespValue::BulkString(Some(Bytes::from(slot_s))),
        RespValue::BulkString(Some(Bytes::from(dest_node_id.to_string()))),
    ];
    match resp_command_bytes(&mut stream, &args, MIGRATE_IO_TIMEOUT).await {
        Ok(reply) => parse_remote_reshard_reply(entry.slot, &reply),
        Err(e) => ReshardSlotResult {
            slot: entry.slot,
            migrated: 0,
            skipped: 0,
            source_node: "n/a".into(),
            dest_node: "n/a".into(),
            status: format!("failed_remote:{}", e),
            warning: Some(format!(
                "remote source {}:{}",
                entry.source_ip, entry.source_port
            )),
        },
    }
}

/// Parse `CLUSTER RESHARD` array reply from a remote source into one result.
fn parse_remote_reshard_reply(slot: u16, reply: &RespValue) -> ReshardSlotResult {
    if let RespValue::Error(e) = reply {
        return ReshardSlotResult {
            slot,
            migrated: 0,
            skipped: 0,
            source_node: "n/a".into(),
            dest_node: "n/a".into(),
            status: format!("failed_remote:{}", String::from_utf8_lossy(e)),
            warning: None,
        };
    }
    let outer = match reply {
        RespValue::Array(a) if !a.is_empty() => a,
        _ => {
            return ReshardSlotResult {
                slot,
                migrated: 0,
                skipped: 0,
                source_node: "n/a".into(),
                dest_node: "n/a".into(),
                status: "failed_remote:unexpected reply".into(),
                warning: None,
            };
        }
    };
    // First element is the per-slot field array.
    let fields = match &outer[0] {
        RespValue::Array(f) => f,
        _ => {
            return ReshardSlotResult {
                slot,
                migrated: 0,
                skipped: 0,
                source_node: "n/a".into(),
                dest_node: "n/a".into(),
                status: "failed_remote:bad row".into(),
                warning: None,
            };
        }
    };
    let mut migrated = 0usize;
    let mut skipped = 0usize;
    let mut source_node = String::from("n/a");
    let mut dest_node = String::from("n/a");
    let mut status = String::from("failed_remote:missing status");
    let mut warning = None;
    let mut i = 0;
    while i + 1 < fields.len() {
        let key = match fields[i].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => {
                i += 2;
                continue;
            }
        };
        match key.as_str() {
            "migrated" => {
                if let RespValue::Integer(n) = fields[i + 1] {
                    migrated = n.max(0) as usize;
                }
            }
            "skipped" => {
                if let RespValue::Integer(n) = fields[i + 1] {
                    skipped = n.max(0) as usize;
                }
            }
            "source_node" => {
                if let Some(b) = fields[i + 1].as_bulk_string() {
                    source_node = String::from_utf8_lossy(b).into_owned();
                }
            }
            "dest_node" => {
                if let Some(b) = fields[i + 1].as_bulk_string() {
                    dest_node = String::from_utf8_lossy(b).into_owned();
                }
            }
            "status" => {
                if let Some(b) = fields[i + 1].as_bulk_string() {
                    status = String::from_utf8_lossy(b).into_owned();
                }
            }
            "warning" => {
                if let Some(b) = fields[i + 1].as_bulk_string() {
                    warning = Some(String::from_utf8_lossy(b).into_owned());
                }
            }
            _ => {}
        }
        i += 2;
    }
    ReshardSlotResult {
        slot,
        migrated,
        skipped,
        source_node,
        dest_node,
        status,
        warning,
    }
}

async fn reshard_one_slot(
    cache: &Cache,
    cluster: &ClusterState,
    slot: u16,
    dest_node_id: &str,
    dest_ip: &str,
    dest_port: u16,
) -> Result<ReshardSlotResult, String> {
    let source_id = cluster.my_id();
    let slot_s = slot.to_string();

    let mut stream = match connect_dest(dest_ip, dest_port).await {
        Ok(s) => s,
        Err(e) => {
            return Ok(ReshardSlotResult {
                slot,
                migrated: 0,
                skipped: 0,
                source_node: "n/a".into(),
                dest_node: "n/a".into(),
                status: format!("failed_connect:{}", e),
                warning: None,
            });
        }
    };

    // Step 0: align dest ownership away from dest (best-effort) so clients hitting
    // dest get MOVED to source until final NODE. Ignore errors if already aligned.
    let _ = dest_setslot(
        &mut stream,
        &["CLUSTER", "SETSLOT", &slot_s, "NODE", &source_id],
    )
    .await;

    // Step 1: dest IMPORTING <source>
    if let Err(e) = dest_setslot(
        &mut stream,
        &["CLUSTER", "SETSLOT", &slot_s, "IMPORTING", &source_id],
    )
    .await
    {
        return Ok(ReshardSlotResult {
            slot,
            migrated: 0,
            skipped: 0,
            source_node: "n/a".into(),
            dest_node: "n/a".into(),
            status: format!("failed_importing:{}", e),
            warning: None,
        });
    }

    // Step 2: source MIGRATING <dest>
    if let Err(e) = cluster.set_migrating(slot, dest_node_id) {
        // Best-effort clear dest IMPORTING so we do not leave a dangling import.
        let _ = dest_setslot(&mut stream, &["CLUSTER", "SETSLOT", &slot_s, "STABLE"]).await;
        return Ok(ReshardSlotResult {
            slot,
            migrated: 0,
            skipped: 0,
            source_node: "n/a".into(),
            dest_node: "n/a".into(),
            status: format!("failed_migrating:{}", e),
            warning: None,
        });
    }

    // Drop the control connection before key migrate (migrate opens its own).
    drop(stream);

    // Step 3: move keys (partial progress surfaced on failed_keys).
    let key_result = match migrate_slot_keys(cache, slot, dest_ip, dest_port).await {
        Ok(r) => r,
        Err(e) => {
            // Leave MIGRATING / IMPORTING for operator retry (MIGRATEKEYS / SETSLOT).
            // Already-migrated keys live on dest; retry only leftover source keys.
            return Ok(ReshardSlotResult {
                slot,
                migrated: e.partial.migrated,
                skipped: e.partial.skipped,
                source_node: "n/a".into(),
                dest_node: "n/a".into(),
                status: format!("failed_keys:{}", strip_err_prefix(&e.message)),
                warning: None,
            });
        }
    };

    // Step 4: dual-end SETSLOT NODE — prepare/vote then commit (Batch FB).
    let (source_node, dest_node, status, warning) =
        dual_end_setslot_node(cluster, slot, dest_node_id, dest_ip, dest_port).await;

    Ok(ReshardSlotResult {
        slot,
        migrated: key_result.migrated,
        skipped: key_result.skipped,
        source_node,
        dest_node,
        status,
        warning,
    })
}

/// Complete dual-end `SETSLOT NODE` for one slot without migrating keys.
///
/// Operator recovery after `partial_*_node` from RESHARD (or manual SETSLOT half-done).
/// Idempotent when both sides already own/claim `dest_node_id` for `slot`.
///
/// Does **not** move keys. Soft-checks `keys_in_slot` on the calling (source) node:
/// if keys remain, sets [`ReshardSlotResult::warning`] but still applies NODE so
/// operators can recover ownership when they know placement is intentional.
/// Prefer re-running MIGRATEKEYS/RESHARD after `failed_keys` before FINISH.
pub async fn finish_slot_node(
    cache: &Cache,
    cluster: &ClusterState,
    slot: u16,
    dest_node_id: &str,
) -> Result<ReshardSlotResult, String> {
    if slot >= SLOT_COUNT {
        return Err("ERR Invalid or out of range slot".into());
    }
    if dest_node_id == cluster.my_id() {
        return Err("ERR CLUSTER RESHARD FINISH destination cannot be myself".into());
    }
    let dest = cluster.get_node(dest_node_id).ok_or_else(|| {
        format!(
            "ERR CLUSTER RESHARD FINISH I don't know about node {}",
            dest_node_id
        )
    })?;
    if dest.fail {
        return Err(format!(
            "ERR CLUSTER RESHARD FINISH destination node {} is marked fail",
            dest_node_id
        ));
    }

    let remaining = keys_in_slot(cache, slot).len();
    let warning = if remaining > 0 {
        Some(format!(
            "source still holds {} key(s) in slot {}; FINISH only updates ownership — re-run MIGRATEKEYS/RESHARD for leftover keys before relying on placement",
            remaining, slot
        ))
    } else {
        None
    };

    let (source_node, dest_node, status, node_warning) =
        dual_end_setslot_node(cluster, slot, dest_node_id, &dest.ip, dest.port).await;

    // Prefer keys-remaining warning; else NODE partial warning (Batch EH).
    let warning = warning.or(node_warning);

    Ok(ReshardSlotResult {
        slot,
        migrated: 0,
        skipped: 0,
        source_node,
        dest_node,
        status,
        warning,
    })
}

/// Summarize dual-end NODE outcomes into an operator-facing status string.
///
/// Dest-first (Batch DV): when dest fails, source is skipped
/// (`skipped:dest NODE incomplete`) → still reported as `partial_dest_node`
/// so operators use the same recovery path (`RESHARD FINISH`).
fn summarize_dual_end_status(source_node: &str, dest_node: &str) -> String {
    match (source_node == "ok", dest_node == "ok") {
        (true, true) => "complete".to_string(),
        (true, false) => "partial_dest_node".to_string(),
        (false, true) => "partial_source_node".to_string(),
        (false, false) => {
            if source_node.starts_with("skipped:") {
                "partial_dest_node".to_string()
            } else {
                "partial_both_node".to_string()
            }
        }
    }
}

/// Dest then source `SETSLOT NODE <dest>` with prepare/vote then commit (Batch FB/FH).
///
/// Returns `(source_node, dest_node, status, warning)`.
///
/// **Prepare (Batch FB/EY/FH):** source + dest vote via `SETSLOT PREPARE <dest>`
/// (local + remote RESP); votes carry slot-epoch + TTL. Fail closed →
/// `failed_prepare` without NODE; both sides ABORTPREPARE.
///
/// **Commit re-check (Batch FH):** source `check_prepare_valid` + dest
/// `SETSLOT CHECKPREPARE` before any NODE. Fail → `failed_prepare:recheck:…`
/// without half-apply.
///
/// **Commit (Batch FO/DV):** dest `SETSLOT COMMITPREPARE` (atomic check+NODE)
/// first, then source `commit_prepare_node`. Source is skipped when dest does
/// not verify as owner — avoids MOVED while dest is still IMPORTING.
///
/// **Batch EH:** if dest NODE ok but source NODE fails while we still own the
/// slot, re-assert `MIGRATING → dest` so clients receive ASK (keys may already
/// live on dest after MIGRATEKEYS).
///
/// **Batch EP:** after EH, best-effort **compensate** by rolling dest ownership
/// back to the source (`SETSLOT NODE <source>` + `IMPORTING <source>`). Success →
/// status `rolled_back` (consistent dual view; retry FINISH). Failure → keep
/// `partial_source_node` with rollback error in the warning.
///
/// **Batch EJ:** when both sides report ok, re-check local owner and remote
/// `CLUSTER SLOTS` owner; downgrade status if either side drifted.
async fn dual_end_setslot_node(
    cluster: &ClusterState,
    slot: u16,
    dest_node_id: &str,
    dest_ip: &str,
    dest_port: u16,
) -> (String, String, String, Option<String>) {
    // Idempotent complete: both sides already report dest as owner (skip 2PC).
    if source_owns_as(cluster, slot, dest_node_id) {
        if let Ok(mut stream) = connect_dest(dest_ip, dest_port).await {
            if verify_remote_slot_owner(&mut stream, slot, dest_node_id)
                .await
                .is_ok()
            {
                // Clear any stale prepare votes from a prior partial attempt.
                let _ = cluster.abort_prepare_node(slot);
                return ("ok".into(), "ok".into(), "complete".into(), None);
            }
        }
    }

    // Phase 1: prepare / vote on both ends (no NODE yet).
    if let Err(e) =
        prepare_dual_end_node(cluster, slot, dest_node_id, dest_ip, dest_port).await
    {
        return (
            format!("prepare:{}", e),
            "n/a".into(),
            format!("failed_prepare:{}", e),
            Some(format!(
                "dual-end NODE prepare failed for slot {}: {} — fix topology then RESHARD FINISH",
                slot, e
            )),
        );
    }

    // Phase 1b: commit re-check — both sides still prepared (Batch FH).
    if let Err(e) =
        recheck_prepare_before_commit(cluster, slot, dest_node_id, dest_ip, dest_port).await
    {
        let _ = cluster.abort_prepare_node(slot);
        let _ = abort_dest_prepare(slot, dest_ip, dest_port).await;
        return (
            format!("recheck:{}", e),
            "n/a".into(),
            format!("failed_prepare:recheck:{}", e),
            Some(format!(
                "dual-end NODE commit re-check failed for slot {}: {} — fix topology then RESHARD FINISH",
                slot, e
            )),
        );
    }

    // Phase 2: commit — dest COMMITPREPARE first, then source (Batch DV/FO).
    let mut dest_node =
        apply_dest_node_with_retry(slot, dest_node_id, dest_ip, dest_port).await;
    let mut source_node = if dest_node == "ok" {
        // Atomic re-check + NODE on source (Batch FO; closes FH recheck→NODE race).
        apply_source_node_with_retry(cluster, slot, dest_node_id).await
    } else {
        // Do not flip source ownership if dest never took the slot stably.
        String::from("skipped:dest NODE incomplete")
    };
    let mut status = summarize_dual_end_status(&source_node, &dest_node);
    let mut warning = if dest_node == "ok" && source_node != "ok" && cluster.owns_slot(slot) {
        match cluster.set_migrating(slot, dest_node_id) {
            Ok(()) => Some(format!(
                "source NODE failed after dest owned slot {}; left MIGRATING→{} for ASK",
                slot, dest_node_id
            )),
            Err(e) => Some(format!(
                "source NODE failed; could not re-assert MIGRATING: {}",
                e
            )),
        }
    } else {
        None
    };

    // Batch EP: compensate dest when source NODE failed after dest took ownership.
    if dest_node == "ok" && source_node != "ok" && cluster.owns_slot(slot) {
        let source_id = cluster.my_id();
        match rollback_dest_ownership_to_source(slot, &source_id, dest_ip, dest_port).await {
            Ok(()) => {
                dest_node = "rolled_back".to_string();
                status = "rolled_back".to_string();
                warning = Some(format!(
                    "source NODE failed for slot {}; dest ownership rolled back to source + IMPORTING — retry CLUSTER RESHARD or FINISH",
                    slot
                ));
            }
            Err(e) => {
                // Keep partial_source_node; surface rollback failure for ops.
                let rb = format!(
                    "dest rollback failed: {} — run CLUSTER RESHARD FINISH or SETSLOT NODE",
                    e
                );
                warning = Some(match warning.take() {
                    Some(w) => format!("{}; {}", w, rb),
                    None => rb,
                });
            }
        }
    }

    // After commit path: clear local prepare (NODE already cleared it on success;
    // abort leftover vote after partial/rollback so state does not stick).
    let _ = cluster.abort_prepare_node(slot);
    // Best-effort clear dest prepare if commit did not land as complete.
    if status != "complete" {
        let _ = abort_dest_prepare(slot, dest_ip, dest_port).await;
    }

    // Batch EJ: post-commit dual verify when both sides claimed success.
    if status == "complete" {
        if let Some(w) =
            post_commit_verify_dual_end(cluster, slot, dest_node_id, dest_ip, dest_port).await
        {
            // Downgrade: keep sides' last apply strings but mark incomplete.
            if !source_owns_as(cluster, slot, dest_node_id) {
                source_node = format!("verify:{}", w);
            }
            dest_node = format!("verify:{}", w);
            status = "partial_verify".to_string();
            warning = Some(format!(
                "post-commit ownership verify failed for slot {}: {} — run CLUSTER RESHARD FINISH or SETSLOT NODE",
                slot, w
            ));
        }
    }

    (source_node, dest_node, status, warning)
}

/// Prepare/vote phase for dual-end NODE (Batch FB/FH; extends EY preflight).
///
/// 1. Source `SETSLOT PREPARE <dest>` (local vote — owns/already-dest + MIGRATING;
///    stamps slot epoch + wall time)
/// 2. Dest reachable + `CLUSTER MYID` matches `dest_node_id`
/// 3. Dest `CLUSTER SLOTS` owner is source or dest (or unbound/missing)
/// 4. Dest `SETSLOT PREPARE <dest>` (remote vote)
///
/// On any failure: abort local prepare + best-effort dest ABORTPREPARE.
/// Does **not** apply SETSLOT NODE.
async fn prepare_dual_end_node(
    cluster: &ClusterState,
    slot: u16,
    dest_node_id: &str,
    dest_ip: &str,
    dest_port: u16,
) -> Result<(), String> {
    let source_id = cluster.my_id();

    // Source vote first (fail closed before touching dest prepare state).
    if let Err(e) = cluster.set_prepare_node(slot, dest_node_id) {
        return Err(format!("source {}", e));
    }

    let result = prepare_dest_vote(slot, dest_node_id, &source_id, dest_ip, dest_port).await;
    if let Err(ref e) = result {
        let _ = cluster.abort_prepare_node(slot);
        let _ = abort_dest_prepare(slot, dest_ip, dest_port).await;
        return Err(e.clone());
    }
    Ok(())
}

/// Commit-phase re-check of prepare votes on source + dest (Batch FH).
///
/// Ensures epoch fence / TTL / topology still hold and dest MYID still matches
/// before either side applies NODE. Fail closed — caller aborts prepares.
async fn recheck_prepare_before_commit(
    cluster: &ClusterState,
    slot: u16,
    dest_node_id: &str,
    dest_ip: &str,
    dest_port: u16,
) -> Result<(), String> {
    // Source local re-check first (may consume commit-recheck inject).
    if let Err(e) = cluster.check_prepare_valid(slot, dest_node_id) {
        return Err(format!("source {}", e));
    }

    let mut stream = connect_dest(dest_ip, dest_port)
        .await
        .map_err(|e| format!("dest {}", e))?;

    let remote_id = remote_cluster_myid(&mut stream).await?;
    if remote_id != dest_node_id {
        return Err(format!(
            "dest MYID is {} want {} (commit re-check)",
            remote_id, dest_node_id
        ));
    }

    let slot_s = slot.to_string();
    dest_setslot(
        &mut stream,
        &[
            "CLUSTER",
            "SETSLOT",
            &slot_s,
            "CHECKPREPARE",
            dest_node_id,
        ],
    )
    .await
    .map_err(|e| format!("dest {}", strip_err_prefix(&e)))
}

/// Dest half of prepare: MYID + owner sanity + SETSLOT PREPARE (Batch FB/EY).
async fn prepare_dest_vote(
    slot: u16,
    dest_node_id: &str,
    source_id: &str,
    dest_ip: &str,
    dest_port: u16,
) -> Result<(), String> {
    if take_dest_prepare_inject_fail(dest_port) {
        return Err("injected dest PREPARE failure".into());
    }

    let mut stream = connect_dest(dest_ip, dest_port)
        .await
        .map_err(|e| format!("dest {}", e))?;

    let remote_id = remote_cluster_myid(&mut stream).await?;
    if remote_id != dest_node_id {
        return Err(format!(
            "dest MYID is {} want {}",
            remote_id, dest_node_id
        ));
    }

    match remote_slot_owner_id(&mut stream, slot).await {
        Ok(owner) if owner == dest_node_id || owner == source_id => {}
        Ok(owner) => {
            return Err(format!(
                "dest reports unexpected owner {} for slot {} (want source or dest)",
                owner, slot
            ));
        }
        // Unbound / missing from SLOTS: allow (dest may only have IMPORTING).
        Err(e) if e.contains("missing") || e.contains("unbound") => {}
        Err(e) => return Err(format!("dest slots:{}", e)),
    }

    let slot_s = slot.to_string();
    dest_setslot(
        &mut stream,
        &["CLUSTER", "SETSLOT", &slot_s, "PREPARE", dest_node_id],
    )
    .await
    .map_err(|e| format!("dest prepare:{}", strip_err_prefix(&e)))
}

/// Best-effort dest ABORTPREPARE (clear prepare vote without NODE).
async fn abort_dest_prepare(slot: u16, dest_ip: &str, dest_port: u16) -> Result<(), String> {
    let slot_s = slot.to_string();
    let mut stream = connect_dest(dest_ip, dest_port).await?;
    dest_setslot(
        &mut stream,
        &["CLUSTER", "SETSLOT", &slot_s, "ABORTPREPARE"],
    )
    .await
    .map_err(|e| strip_err_prefix(&e).to_string())
}

async fn remote_cluster_myid(stream: &mut TcpStream) -> Result<String, String> {
    let args = vec![
        RespValue::BulkString(Some(Bytes::from_static(b"CLUSTER"))),
        RespValue::BulkString(Some(Bytes::from_static(b"MYID"))),
    ];
    match resp_command_bytes(stream, &args, MIGRATE_IO_TIMEOUT).await? {
        RespValue::BulkString(Some(b)) => Ok(String::from_utf8_lossy(&b).into_owned()),
        RespValue::Error(e) => Err(String::from_utf8_lossy(&e).into_owned()),
        other => Err(format!("unexpected MYID reply: {:?}", other)),
    }
}

/// Owner id for `slot` on remote, or Err if missing.
async fn remote_slot_owner_id(stream: &mut TcpStream, slot: u16) -> Result<String, String> {
    let args = vec![
        RespValue::BulkString(Some(Bytes::from_static(b"CLUSTER"))),
        RespValue::BulkString(Some(Bytes::from_static(b"SLOTS"))),
    ];
    let reply = resp_command_bytes(stream, &args, MIGRATE_IO_TIMEOUT).await?;
    slot_owner_id_from_slots_reply(&reply, slot)
        .ok_or_else(|| format!("slot {} missing from CLUSTER SLOTS", slot))
}

/// Best-effort reverse of dest `SETSLOT NODE <dest>` (Batch EP compensating step).
///
/// Restores `NODE <source>` then `IMPORTING <source>` so a later RESHARD/FINISH
/// can complete dual-end NODE without a dual-ownership window.
async fn rollback_dest_ownership_to_source(
    slot: u16,
    source_id: &str,
    dest_ip: &str,
    dest_port: u16,
) -> Result<(), String> {
    let slot_s = slot.to_string();
    let mut last_err = String::from("dest rollback not attempted");
    for attempt in 0..NODE_SET_ATTEMPTS {
        match connect_dest(dest_ip, dest_port).await {
            Ok(mut stream) => {
                match dest_setslot(
                    &mut stream,
                    &["CLUSTER", "SETSLOT", &slot_s, "NODE", source_id],
                )
                .await
                {
                    Ok(()) => match verify_remote_slot_owner(&mut stream, slot, source_id).await {
                        Ok(()) => {
                            // Resume import state for ASKING / retry (best-effort).
                            let _ = dest_setslot(
                                &mut stream,
                                &["CLUSTER", "SETSLOT", &slot_s, "IMPORTING", source_id],
                            )
                            .await;
                            return Ok(());
                        }
                        Err(e) => last_err = format!("verify:{}", e),
                    },
                    Err(e) => last_err = e,
                }
            }
            Err(e) => last_err = format!("connect:{}", e),
        }
        if attempt + 1 < NODE_SET_ATTEMPTS {
            tokio::time::sleep(NODE_RETRY_DELAY).await;
        }
    }
    Err(last_err)
}

/// Returns `Some(reason)` if post-commit ownership is inconsistent.
async fn post_commit_verify_dual_end(
    cluster: &ClusterState,
    slot: u16,
    dest_node_id: &str,
    dest_ip: &str,
    dest_port: u16,
) -> Option<String> {
    if !source_owns_as(cluster, slot, dest_node_id) {
        return Some("local owner is not dest after dual NODE".into());
    }
    match connect_dest(dest_ip, dest_port).await {
        Ok(mut stream) => match verify_remote_slot_owner(&mut stream, slot, dest_node_id).await {
            Ok(()) => None,
            Err(e) => Some(format!("remote {}", e)),
        },
        Err(e) => Some(format!("remote connect {}", e)),
    }
}

/// Local `SETSLOT COMMITPREPARE` + ownership verify, with retries (Batch FO).
///
/// Uses atomic check+apply ([`ClusterState::commit_prepare_node`]). Source NODE
/// inject lives on that path (Batch EP) so retries share the dual-end failure
/// path. Operator bare `SETSLOT NODE` still bypasses prepare (FINISH/recovery).
async fn apply_source_node_with_retry(
    cluster: &ClusterState,
    slot: u16,
    dest_node_id: &str,
) -> String {
    let mut last_err = String::from("source NODE not attempted");
    for attempt in 0..NODE_SET_ATTEMPTS {
        match cluster.commit_prepare_node(slot, dest_node_id) {
            Ok(()) => {
                if source_owns_as(cluster, slot, dest_node_id) {
                    return "ok".to_string();
                }
                last_err = "verify: local owner is not dest after COMMITPREPARE".into();
            }
            Err(e) => last_err = e,
        }
        if attempt + 1 < NODE_SET_ATTEMPTS {
            tokio::time::sleep(NODE_RETRY_DELAY).await;
        }
    }
    last_err
}

fn source_owns_as(cluster: &ClusterState, slot: u16, node_id: &str) -> bool {
    cluster
        .owner_of(slot)
        .map(|n| n.id == node_id)
        .unwrap_or(false)
}

/// Remote dest `SETSLOT COMMITPREPARE` + CLUSTER SLOTS verify, with retries (Batch FO).
///
/// Falls back to bare `SETSLOT NODE` only if dest rejects unknown subcommand
/// (older peer without FO) — still verifies ownership after either path.
async fn apply_dest_node_with_retry(
    slot: u16,
    dest_node_id: &str,
    dest_ip: &str,
    dest_port: u16,
) -> String {
    let slot_s = slot.to_string();
    let mut last_err = String::from("dest NODE not attempted");
    for attempt in 0..NODE_SET_ATTEMPTS {
        // Test injection: simulate transient (or permanent) dest NODE failures.
        if take_dest_node_inject_fail(dest_port) {
            last_err = "injected dest NODE failure".into();
            if attempt + 1 < NODE_SET_ATTEMPTS {
                tokio::time::sleep(NODE_RETRY_DELAY).await;
            }
            continue;
        }

        match connect_dest(dest_ip, dest_port).await {
            Ok(mut stream) => {
                // Prefer atomic COMMITPREPARE (Batch FO); fall back to NODE.
                let applied = match dest_setslot(
                    &mut stream,
                    &[
                        "CLUSTER",
                        "SETSLOT",
                        &slot_s,
                        "COMMITPREPARE",
                        dest_node_id,
                    ],
                )
                .await
                {
                    Ok(()) => Ok(()),
                    Err(e) if e.to_ascii_uppercase().contains("UNKNOWN SUBCOMMAND") => {
                        dest_setslot(
                            &mut stream,
                            &["CLUSTER", "SETSLOT", &slot_s, "NODE", dest_node_id],
                        )
                        .await
                    }
                    Err(e) => Err(e),
                };
                match applied {
                    Ok(()) => match verify_remote_slot_owner(&mut stream, slot, dest_node_id).await
                    {
                        Ok(()) => return "ok".to_string(),
                        Err(e) => last_err = format!("verify:{}", e),
                    },
                    Err(e) => last_err = e,
                }
            }
            Err(e) => last_err = format!("connect:{}", e),
        }
        if attempt + 1 < NODE_SET_ATTEMPTS {
            tokio::time::sleep(NODE_RETRY_DELAY).await;
        }
    }
    last_err
}

/// Confirm remote node reports `dest_node_id` as owner of `slot` via CLUSTER SLOTS.
async fn verify_remote_slot_owner(
    stream: &mut TcpStream,
    slot: u16,
    dest_node_id: &str,
) -> Result<(), String> {
    let args = vec![
        RespValue::BulkString(Some(Bytes::from_static(b"CLUSTER"))),
        RespValue::BulkString(Some(Bytes::from_static(b"SLOTS"))),
    ];
    let reply = resp_command_bytes(stream, &args, MIGRATE_IO_TIMEOUT).await?;
    let owner = slot_owner_id_from_slots_reply(&reply, slot)
        .ok_or_else(|| format!("slot {} missing from CLUSTER SLOTS", slot))?;
    if owner == dest_node_id {
        Ok(())
    } else {
        Err(format!(
            "remote owner is {} want {}",
            owner, dest_node_id
        ))
    }
}

/// Parse Redis-style CLUSTER SLOTS array and return owner id for `slot`.
fn slot_owner_id_from_slots_reply(reply: &RespValue, slot: u16) -> Option<String> {
    let ranges = match reply {
        RespValue::Array(a) => a,
        _ => return None,
    };
    for range in ranges {
        let parts = match range {
            RespValue::Array(p) => p,
            _ => continue,
        };
        if parts.len() < 3 {
            continue;
        }
        let start = match &parts[0] {
            RespValue::Integer(n) if *n >= 0 => *n as u16,
            _ => continue,
        };
        let end = match &parts[1] {
            RespValue::Integer(n) if *n >= 0 => *n as u16,
            _ => continue,
        };
        if slot < start || slot > end {
            continue;
        }
        let node = match &parts[2] {
            RespValue::Array(n) if n.len() >= 3 => n,
            _ => continue,
        };
        if let RespValue::BulkString(Some(id)) = &node[2] {
            return Some(String::from_utf8_lossy(id).into_owned());
        }
    }
    None
}

fn strip_err_prefix(e: &str) -> &str {
    e.strip_prefix("ERR ").unwrap_or(e)
}

async fn connect_dest(dest_ip: &str, dest_port: u16) -> Result<TcpStream, String> {
    let addr = format!("{}:{}", dest_ip, dest_port);
    match tokio::time::timeout(MIGRATE_IO_TIMEOUT, TcpStream::connect(&addr)).await {
        Ok(Ok(s)) => {
            let _ = s.set_nodelay(true);
            Ok(s)
        }
        Ok(Err(e)) => Err(format!("unable to connect to {}: {}", addr, e)),
        Err(_) => Err(format!("timed out connecting to {}", addr)),
    }
}

async fn dest_setslot(stream: &mut TcpStream, parts: &[&str]) -> Result<(), String> {
    let args: Vec<RespValue> = parts
        .iter()
        .map(|p| RespValue::BulkString(Some(Bytes::from(p.to_string()))))
        .collect();
    match resp_command_bytes(stream, &args, MIGRATE_IO_TIMEOUT).await {
        Ok(RespValue::SimpleString(s)) if s.as_ref() == b"OK" => Ok(()),
        Ok(RespValue::Error(e)) => Err(String::from_utf8_lossy(&e).into_owned()),
        Ok(other) => Err(format!("unexpected SETSLOT reply: {:?}", other)),
        Err(e) => Err(e),
    }
}

fn bulk_static(s: &'static [u8]) -> RespValue {
    RespValue::BulkString(Some(Bytes::from_static(s)))
}

fn bulk_owned(s: String) -> RespValue {
    RespValue::BulkString(Some(Bytes::from(s)))
}

async fn resp_command_bytes(
    stream: &mut TcpStream,
    parts: &[RespValue],
    timeout: Duration,
) -> Result<RespValue, String> {
    let payload = RespValue::Array(parts.to_vec()).serialize();

    match tokio::time::timeout(timeout, stream.write_all(&payload)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(format!("write error: {}", e)),
        Err(_) => return Err("write timed out".into()),
    }

    let mut parser = RespParser::new();
    let mut buf = vec![0u8; 64 * 1024];
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::StoreOptions;

    #[test]
    fn reshard_result_resp_array_fields() {
        let r = ReshardSlotResult {
            slot: 12182,
            migrated: 3,
            skipped: 1,
            source_node: "ok".into(),
            dest_node: "connect:timeout".into(),
            status: "partial_dest_node".into(),
            warning: None,
        };
        match r.to_resp_array() {
            RespValue::Array(a) => {
                assert_eq!(a.len(), 12);
                assert_eq!(a[1], RespValue::Integer(12182));
                assert_eq!(a[3], RespValue::Integer(3));
                assert_eq!(a[5], RespValue::Integer(1));
                match &a[11] {
                    RespValue::BulkString(Some(b)) => {
                        assert_eq!(b.as_ref(), b"partial_dest_node");
                    }
                    other => panic!("{:?}", other),
                }
            }
            other => panic!("{:?}", other),
        }
    }

    #[test]
    fn reshard_result_resp_includes_warning_when_set() {
        let r = ReshardSlotResult {
            slot: 1,
            migrated: 0,
            skipped: 0,
            source_node: "ok".into(),
            dest_node: "ok".into(),
            status: "complete".into(),
            warning: Some("source still holds 2 key(s)".into()),
        };
        match r.to_resp_array() {
            RespValue::Array(a) => {
                assert_eq!(a.len(), 14);
                match &a[12] {
                    RespValue::BulkString(Some(b)) => assert_eq!(b.as_ref(), b"warning"),
                    other => panic!("{:?}", other),
                }
                match &a[13] {
                    RespValue::BulkString(Some(b)) => {
                        assert!(b.as_ref().starts_with(b"source still holds"));
                    }
                    other => panic!("{:?}", other),
                }
            }
            other => panic!("{:?}", other),
        }
    }

    #[test]
    fn reshard_range_aborts_on_partial_and_failed() {
        assert!(!reshard_range_should_abort("complete"));
        assert!(reshard_range_should_abort("partial_dest_node"));
        assert!(reshard_range_should_abort("partial_source_node"));
        assert!(reshard_range_should_abort("failed_keys:boom"));
        assert!(reshard_range_should_abort("failed_connect:x"));
    }

    #[test]
    fn plan_reshard_greedy_from_largest_donor() {
        let a = ClusterState::single_node("127.0.0.1", 7000);
        let b_id = "bb".repeat(20);
        a.add_node(&b_id, "10.0.0.2", 7001);
        // Give B a few slots so A remains largest donor.
        a.reassign_slot_range(0, 9, &b_id).unwrap();
        let plan = plan_reshard(&a, &b_id, 5).unwrap();
        assert_eq!(plan.len(), 5);
        for e in &plan {
            assert_eq!(e.source_id, a.my_id());
            assert!(e.slot >= 10, "should not re-plan dest-owned slots");
        }
        // Dest already owns 10; planning 0 slots is error.
        assert!(plan_reshard(&a, &b_id, 0).is_err());
    }

    #[test]
    fn plan_reshard_unknown_dest_errors() {
        let a = ClusterState::single_node("127.0.0.1", 7000);
        assert!(plan_reshard(&a, "no-such-node", 1).is_err());
    }

    #[test]
    fn partial_source_reasserts_migrating() {
        let cs = ClusterState::single_node("127.0.0.1", 7000);
        let dest = "dd".repeat(20);
        cs.add_node(&dest, "10.0.0.2", 7001);
        // Simulate dest NODE ok but source NODE not applied: we still own slot.
        assert!(cs.owns_slot(0));
        // Directly exercise the MIGRATING restore path used after partial source NODE.
        cs.set_migrating(0, &dest).unwrap();
        assert!(cs.is_migrating(0));
        assert_eq!(cs.migrating_dest(0).as_deref(), Some(dest.as_str()));
    }

    #[test]
    fn summarize_dual_end_status_matrix() {
        assert_eq!(summarize_dual_end_status("ok", "ok"), "complete");
        assert_eq!(
            summarize_dual_end_status("ok", "connect:timeout"),
            "partial_dest_node"
        );
        assert_eq!(
            summarize_dual_end_status("Unknown node x", "ok"),
            "partial_source_node"
        );
        assert_eq!(
            summarize_dual_end_status("err", "err"),
            "partial_both_node"
        );
        // Dest-first: source skipped when dest fails → still partial_dest_node.
        assert_eq!(
            summarize_dual_end_status("skipped:dest NODE incomplete", "injected dest NODE failure"),
            "partial_dest_node"
        );
        // Batch EJ: post-commit verify failure aborts multi-slot ranges.
        assert!(reshard_range_should_abort("partial_verify"));
        // Batch EP: compensating dest rollback is not complete — abort ranges.
        assert!(reshard_range_should_abort("rolled_back"));
        assert!(reshard_range_should_abort("partial_source_node"));
        // Batch FB/EY: prepare failure aborts without applying NODE.
        assert!(reshard_range_should_abort(
            "failed_prepare:dest MYID is x want y"
        ));
        assert!(reshard_range_should_abort(
            "failed_preflight:dest MYID is x want y"
        ));
    }

    #[test]
    fn prepare_source_vote_records_and_abort_clears() {
        let cs = ClusterState::single_node("127.0.0.1", 7000);
        let dest = "dd".repeat(20);
        cs.add_node(&dest, "10.0.0.2", 7001);
        cs.set_migrating(0, &dest).unwrap();
        cs.set_prepare_node(0, &dest).unwrap();
        assert!(cs.is_prepared(0));
        assert_eq!(cs.prepared_node(0).as_deref(), Some(dest.as_str()));
        assert!(cs.check_prepare_valid(0, &dest).is_ok());
        cs.abort_prepare_node(0).unwrap();
        assert!(!cs.is_prepared(0));
        assert!(cs.check_prepare_valid(0, &dest).is_err());
    }

    #[test]
    fn prepare_source_rejects_unowned() {
        let cs = ClusterState::single_node("127.0.0.1", 7000);
        let other = "oo".repeat(20);
        let dest = "dd".repeat(20);
        cs.add_node(&other, "10.0.0.2", 7001);
        cs.add_node(&dest, "10.0.0.3", 7002);
        cs.reassign_slot(0, &other).unwrap();
        assert!(cs.set_prepare_node(0, &dest).is_err());
    }

    /// Batch FH: epoch fence rejects commit re-check after slot epoch moves.
    #[test]
    fn prepare_stale_epoch_fails_commit_recheck() {
        let cs = ClusterState::single_node("127.0.0.1", 7000);
        let dest = "dd".repeat(20);
        cs.add_node(&dest, "10.0.0.2", 7001);
        cs.set_migrating(0, &dest).unwrap();
        cs.set_prepare_node(0, &dest).unwrap();
        let stamped = cs.prepared_slot_epoch(0).expect("stamped epoch");
        assert_eq!(stamped, cs.slot_epoch(0));
        assert!(cs.check_prepare_valid(0, &dest).is_ok());

        // Ownership/epoch moves under the vote (without NODE clearing prepare).
        cs.test_bump_slot_epoch_keep_prepare(0);
        assert!(cs.is_prepared(0), "vote still in memory until re-check");
        let err = cs.check_prepare_valid(0, &dest).unwrap_err();
        assert!(
            err.contains("epoch stale") || err.contains("stale"),
            "expected epoch stale error, got {}",
            err
        );
        // No half-apply: we still own the slot.
        assert!(cs.owns_slot(0));
        assert_ne!(cs.owner_id_of(0).as_deref(), Some(dest.as_str()));
    }

    /// Batch FH: cleared prepare fails commit re-check (no NODE).
    #[test]
    fn prepare_cleared_fails_commit_recheck() {
        let cs = ClusterState::single_node("127.0.0.1", 7000);
        let dest = "dd".repeat(20);
        cs.add_node(&dest, "10.0.0.2", 7001);
        cs.set_migrating(0, &dest).unwrap();
        cs.set_prepare_node(0, &dest).unwrap();
        cs.abort_prepare_node(0).unwrap();
        let err = cs.check_prepare_valid(0, &dest).unwrap_err();
        assert!(
            err.contains("no prepare") || err.contains("cleared"),
            "expected cleared prepare error, got {}",
            err
        );
        assert!(cs.owns_slot(0));
    }

    /// Batch FH/FO: soft-reset clears prepares; legacy conf without `# prepare` stays empty.
    #[test]
    fn prepare_boot_clear_fail_closed() {
        let cs = ClusterState::single_node("127.0.0.1", 7000);
        let dest = "dd".repeat(20);
        cs.add_node(&dest, "10.0.0.2", 7001);
        cs.set_migrating(0, &dest).unwrap();
        cs.set_prepare_node(0, &dest).unwrap();
        assert!(cs.is_prepared(0));
        cs.clear_all_prepares();
        assert!(!cs.is_prepared(0));
        assert!(cs.check_prepare_valid(0, &dest).is_err());
        // Pre-FO / no prepare lines → empty map (FO restores only `# prepare` lines).
        let conf = format!(
            "# Kore cluster nodes.conf\n# epoch {}\n{}",
            cs.current_epoch(),
            cs.format_nodes()
        );
        let loaded =
            ClusterState::from_nodes_conf("127.0.0.1", 7000, &conf).expect("load conf");
        assert!(!loaded.is_prepared(0));
    }

    /// Batch FH: TTL-expired prepare fails re-check and is purged.
    #[test]
    fn prepare_ttl_expired_fails_recheck() {
        let cs = ClusterState::single_node("127.0.0.1", 7000);
        let dest = "dd".repeat(20);
        cs.add_node(&dest, "10.0.0.2", 7001);
        cs.set_migrating(0, &dest).unwrap();
        cs.set_prepare_node(0, &dest).unwrap();
        cs.test_expire_prepare(0);
        let err = cs.check_prepare_valid(0, &dest).unwrap_err();
        assert!(
            err.contains("expired") || err.contains("no prepare"),
            "expected expired prepare, got {}",
            err
        );
        assert!(!cs.is_prepared(0));
    }

    /// Batch FH: range abort covers recheck failure statuses.
    #[test]
    fn reshard_range_aborts_on_failed_prepare_recheck() {
        assert!(reshard_range_should_abort(
            "failed_prepare:recheck:source no prepare vote for slot 0"
        ));
    }

    #[test]
    fn preflight_local_rejects_unowned() {
        let cs = ClusterState::single_node("127.0.0.1", 7000);
        let other = "oo".repeat(20);
        cs.add_node(&other, "10.0.0.2", 7001);
        cs.reassign_slot(0, &other).unwrap();
        // Local does not own 0 and is not dest.
        assert!(!cs.owns_slot(0));
        // Sync preflight piece: local_ready would fail (async preflight needs network).
        assert!(!cs.owns_slot(0) && cs.owner_id_of(0).as_deref() != Some(cs.my_id().as_str()));
    }

    #[test]
    fn slot_owner_id_from_slots_reply_finds_covering_range() {
        let reply = RespValue::Array(vec![
            RespValue::Array(vec![
                RespValue::Integer(0),
                RespValue::Integer(100),
                RespValue::Array(vec![
                    bulk_static(b"127.0.0.1"),
                    RespValue::Integer(7000),
                    bulk_owned("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()),
                ]),
            ]),
            RespValue::Array(vec![
                RespValue::Integer(101),
                RespValue::Integer(16383),
                RespValue::Array(vec![
                    bulk_static(b"127.0.0.1"),
                    RespValue::Integer(7001),
                    bulk_owned("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into()),
                ]),
            ]),
        ]);
        assert_eq!(
            slot_owner_id_from_slots_reply(&reply, 50).as_deref(),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
        assert_eq!(
            slot_owner_id_from_slots_reply(&reply, 101).as_deref(),
            Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        );
        assert_eq!(slot_owner_id_from_slots_reply(&reply, 20000), None);
    }

    #[test]
    fn source_node_retry_succeeds_when_dest_known() {
        let cs = ClusterState::single_node("127.0.0.1", 1);
        let peer = "cccccccccccccccccccccccccccccccccccccccc";
        cs.add_node(peer, "127.0.0.1", 2);
        // Synchronous path via set_node is enough; ownership verify uses owner_of.
        cs.set_node(42, peer).unwrap();
        assert!(source_owns_as(&cs, 42, peer));
        assert!(!source_owns_as(&cs, 42, &cs.my_id()));
    }

    #[test]
    fn keys_in_slot_filters_by_crc16() {
        let cache = Cache::new_with_sweep(4, 1024 * 1024, 1024 * 1024, false);
        // "foo" → slot 12182
        let opts = StoreOptions::default();
        cache
            .store(Bytes::from_static(b"foo"), Bytes::from_static(b"1"), opts.clone())
            .unwrap();
        cache
            .store(Bytes::from_static(b"bar"), Bytes::from_static(b"2"), opts)
            .unwrap();

        let slot_foo = key_hash_slot(b"foo");
        let in_foo = keys_in_slot(&cache, slot_foo);
        assert!(in_foo.iter().any(|k| k.as_ref() == b"foo"));
        assert!(!in_foo.iter().any(|k| k.as_ref() == b"bar"));
    }

    #[test]
    fn snapshot_hash_and_list() {
        let cache = Cache::new_with_sweep(4, 1024 * 1024, 1024 * 1024, false);
        let k = Bytes::from_static(b"{t}.h");
        let h = cache.get_or_create_hash(&k).unwrap();
        h.write()
            .hset(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
        match snapshot_key(&cache, &k).unwrap() {
            KeySnapshot::Hash {
                fields,
                expire_unix_ms,
            } => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].0.as_ref(), b"f");
                assert_eq!(expire_unix_ms, -1);
            }
            _ => panic!("expected hash"),
        }

        let kl = Bytes::from_static(b"{t}.l");
        let l = cache.get_or_create_list(&kl).unwrap();
        l.write().rpush([Bytes::from_static(b"a")]);
        l.write().rpush([Bytes::from_static(b"b")]);
        match snapshot_key(&cache, &kl).unwrap() {
            KeySnapshot::List {
                items,
                expire_unix_ms,
            } => {
                assert_eq!(items.len(), 2);
                assert_eq!(expire_unix_ms, -1);
            }
            _ => panic!("expected list"),
        }
    }

    #[test]
    fn snapshot_and_recreate_preserve_typed_absolute_expire() {
        let cache = Cache::new_with_sweep(4, 1024 * 1024, 1024 * 1024, false);
        let k = Bytes::from_static(b"{t}.ht");
        let h = cache.get_or_create_hash(&k).unwrap();
        h.write()
            .hset(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
        cache.expire(&k, 30_000).unwrap();
        let expected_abs = cache.expire_time_unix_ms(&k);
        assert!(expected_abs > 0, "expected absolute expire");

        let snap = snapshot_key(&cache, &k).unwrap();
        match &snap {
            KeySnapshot::Hash { expire_unix_ms, .. } => {
                // Snapshotted absolute should match source within 1ms clock skew.
                assert!(
                    (*expire_unix_ms - expected_abs).abs() <= 1,
                    "expire_unix_ms={} expected={}",
                    expire_unix_ms,
                    expected_abs
                );
            }
            _ => panic!("expected hash"),
        }
        let cmds = recreate_commands(&k, &snap);
        assert_eq!(cmds.len(), 2, "HSET + PEXPIREAT");
        let pexpireat = &cmds[1];
        assert_eq!(
            pexpireat[0].as_bulk_string().map(|b| b.as_ref()),
            Some(b"PEXPIREAT".as_slice())
        );
        let wire_abs: i64 = String::from_utf8_lossy(
            pexpireat[2]
                .as_bulk_string()
                .expect("PEXPIREAT timestamp bulk"),
        )
        .parse()
        .expect("timestamp");
        assert_eq!(wire_abs, snap_expire(&snap));
        assert!((snap_expire(&snap) - expected_abs).abs() <= 1);
    }

    fn snap_expire(snap: &KeySnapshot) -> i64 {
        match snap {
            KeySnapshot::String { expire_unix_ms, .. }
            | KeySnapshot::Hash { expire_unix_ms, .. }
            | KeySnapshot::List { expire_unix_ms, .. }
            | KeySnapshot::Set { expire_unix_ms, .. }
            | KeySnapshot::ZSet { expire_unix_ms, .. }
            | KeySnapshot::Geo { expire_unix_ms, .. }
            | KeySnapshot::Stream { expire_unix_ms, .. } => *expire_unix_ms,
        }
    }

    #[test]
    fn snapshot_string_uses_set_pxat() {
        let cache = Cache::new_with_sweep(4, 1024 * 1024, 1024 * 1024, false);
        let k = Bytes::from_static(b"s-ttl");
        let mut opts = StoreOptions::default();
        opts.ttl_ms = Some(12_000);
        cache
            .store(k.clone(), Bytes::from_static(b"v"), opts)
            .unwrap();
        let expected_abs = cache.expire_time_unix_ms(&k);
        let snap = snapshot_key(&cache, &k).unwrap();
        let cmds = recreate_commands(&k, &snap);
        assert_eq!(cmds.len(), 1);
        let parts = &cmds[0];
        assert!(
            parts
                .iter()
                .any(|p| p.as_bulk_string().map(|b| b.as_ref()) == Some(b"PXAT")),
            "expected SET PXAT in {:?}",
            parts
        );
        // PXAT argument follows the PXAT token.
        let pxat_idx = parts
            .iter()
            .position(|p| p.as_bulk_string().map(|b| b.as_ref()) == Some(b"PXAT"))
            .unwrap();
        let wire_abs: i64 = String::from_utf8_lossy(
            parts[pxat_idx + 1]
                .as_bulk_string()
                .expect("PXAT timestamp"),
        )
        .parse()
        .expect("timestamp");
        assert!(
            (wire_abs - expected_abs).abs() <= 1,
            "wire={} expected={}",
            wire_abs,
            expected_abs
        );
    }

    /// Absolute expire on the wire is frozen at snapshot time: a delay before
    /// recreate must not rewrite the timestamp to a later remaining-ms end.
    #[test]
    fn recreate_absolute_expire_stable_under_delay() {
        let cache = Cache::new_with_sweep(4, 1024 * 1024, 1024 * 1024, false);
        let k = Bytes::from_static(b"delay-ttl");
        let mut opts = StoreOptions::default();
        opts.ttl_ms = Some(60_000);
        cache
            .store(k.clone(), Bytes::from_static(b"v"), opts)
            .unwrap();

        let snap = snapshot_key(&cache, &k).unwrap();
        let frozen = snap_expire(&snap);
        assert!(frozen > 0);

        std::thread::sleep(std::time::Duration::from_millis(150));

        // Remaining-ms path would emit a smaller end after delay; absolute must not.
        let cmds = recreate_commands(&k, &snap);
        let parts = &cmds[0];
        let pxat_idx = parts
            .iter()
            .position(|p| p.as_bulk_string().map(|b| b.as_ref()) == Some(b"PXAT"))
            .expect("PXAT present");
        let wire_abs: i64 = String::from_utf8_lossy(
            parts[pxat_idx + 1]
                .as_bulk_string()
                .expect("PXAT timestamp"),
        )
        .parse()
        .expect("timestamp");
        assert_eq!(wire_abs, frozen, "absolute expire must be frozen at snapshot");

        // Typed path: same freeze property for PEXPIREAT.
        let hk = Bytes::from_static(b"delay-h");
        let h = cache.get_or_create_hash(&hk).unwrap();
        h.write()
            .hset(Bytes::from_static(b"f"), Bytes::from_static(b"1"));
        cache.expire(&hk, 45_000).unwrap();
        let hsnap = snapshot_key(&cache, &hk).unwrap();
        let hfrozen = snap_expire(&hsnap);
        std::thread::sleep(std::time::Duration::from_millis(100));
        let hcmds = recreate_commands(&hk, &hsnap);
        assert_eq!(
            hcmds[1][0].as_bulk_string().map(|b| b.as_ref()),
            Some(b"PEXPIREAT".as_slice())
        );
        let hwire: i64 = String::from_utf8_lossy(
            hcmds[1][2]
                .as_bulk_string()
                .expect("PEXPIREAT ts"),
        )
        .parse()
        .expect("timestamp");
        assert_eq!(hwire, hfrozen);
    }

    #[test]
    fn format_partial_ioerr_includes_counts() {
        let s = format_partial_ioerr("127.0.0.1:1", "boom", 2, 1);
        assert!(s.starts_with("IOERR"), "{}", s);
        assert!(s.contains("migrated=2"), "{}", s);
        assert!(s.contains("skipped=1"), "{}", s);
        assert!(s.contains("Partial keys may have moved"), "{}", s);
    }
}
