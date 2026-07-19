//! Thin slot migration + Redis key-level `MIGRATE` (shared RESP recreate path).
//!
//! Operator flow (Redis-like slot reshard):
//! 1. dest:  CLUSTER SETSLOT <s> IMPORTING <source-id>
//! 2. source: CLUSTER SETSLOT <s> MIGRATING <dest-id>
//! 3. source: CLUSTER MIGRATEKEYS <s> <dest-ip> <dest-port>
//! 4. both:   CLUSTER SETSLOT <s> NODE <dest-id>
//!
//! `CLUSTER RESHARD` runs steps 1–4 on the source for one slot or an inclusive
//! range. Dual-end NODE is **best-effort** (not atomic / no 2PC). After each
//! side's NODE, ownership is re-checked and failed NODE is retried a few times
//! (Batch DN). Partial failures leave honest status fields; operators can
//! complete with `CLUSTER RESHARD FINISH` or manual SETSLOT.
//!
//! **Partial key moves:** `migrate_slot_keys` deletes each source key only after
//! dest accepts it. On mid-slot failure, earlier keys already live on dest;
//! `MigrateSlotError::partial` (and RESHARD `migrated`/`skipped` under
//! `failed_keys`) report how many succeeded. **Retry re-runs MIGRATEKEYS /
//! RESHARD for leftover source keys only** — already-moved keys stay on dest.
//!
//! **Range abort:** multi-slot RESHARD stops after the first non-`complete`
//! status (`failed_*` or `partial_*_node`) so operators do not cascade mixed
//! ownership across a range.
//!
//! **Dual-end NODE order:** source applies `SETSLOT NODE <dest>` before dest.
//! Between a successful source NODE and dest NODE, clients may receive MOVED
//! to dest while dest is still IMPORTING (ASK-only) — a transient window under
//! `partial_dest_node`. Retries cover blips; permanent dest failure needs
//! `RESHARD FINISH` or manual SETSLOT. Dest-first ordering is intentionally
//! not used (low-risk residual; Redis client expectations favor source MOVED).
//!
//! **Redis `MIGRATE` (Batch DP):** key-level transfer reuses
//! [`snapshot_key`] / [`recreate_commands`] / ASKING / RESP I/O. Options:
//! `COPY`, `REPLACE`, `AUTH`/`AUTH2`, multi-key via `KEYS`, `timeout` ms,
//! `destination-db` (SELECT on dest). No DUMP/RESTORE wire format.
//!
//! Supports string, hash, list, set, zset, geo, and stream keys. Dest writes use
//! ASKING so IMPORTING slots accept the transfer. Complex types are recreated
//! with the same RESP commands as AOF rewrite (no DUMP/RESTORE).

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
enum KeySnapshot {
    String {
        value: Bytes,
        pttl: i64,
    },
    Hash(Vec<(Bytes, Bytes)>),
    List(Vec<Bytes>),
    Set(Vec<Bytes>),
    ZSet(Vec<(Bytes, f64)>),
    Geo(Vec<(Bytes, f64, f64)>),
    Stream(StreamStateSnapshot),
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
                pttl: entry.ttl_millis().unwrap_or(-1),
            })
        }
        KeyType::Hash => {
            let h = cache.get_hash(key)?;
            let fields: Vec<_> = h.read().iter_fields().collect();
            if fields.is_empty() {
                return None;
            }
            Some(KeySnapshot::Hash(fields))
        }
        KeyType::List => {
            let l = cache.get_list(key)?;
            let items: Vec<_> = l.read().iter_items().collect();
            if items.is_empty() {
                return None;
            }
            Some(KeySnapshot::List(items))
        }
        KeyType::Set => {
            let s = cache.get_set(key)?;
            let members: Vec<_> = s.read().iter_members().collect();
            if members.is_empty() {
                return None;
            }
            Some(KeySnapshot::Set(members))
        }
        KeyType::ZSet => {
            let z = cache.get_sorted_set(key)?;
            let members: Vec<_> = z.read().iter_members().collect();
            if members.is_empty() {
                return None;
            }
            Some(KeySnapshot::ZSet(members))
        }
        KeyType::Geo => {
            let g = cache.get_geo_set(key)?;
            let members: Vec<_> = g.read().iter_members().collect();
            if members.is_empty() {
                return None;
            }
            Some(KeySnapshot::Geo(members))
        }
        KeyType::Stream => {
            let s = cache.get_stream(key)?;
            let state = s.read().export_state();
            // Allow empty streams when groups exist (Redis keeps them).
            if state.entries.is_empty() && state.groups.is_empty() {
                return None;
            }
            Some(KeySnapshot::Stream(state))
        }
    }
}

/// Build the sequence of RESP command arrays needed to recreate `snap` at `key`.
fn recreate_commands(key: &Bytes, snap: &KeySnapshot) -> Vec<Vec<RespValue>> {
    match snap {
        KeySnapshot::String { value, pttl } => {
            let mut parts = vec![
                bulk_static(b"SET"),
                RespValue::BulkString(Some(key.clone())),
                RespValue::BulkString(Some(value.clone())),
            ];
            if *pttl > 0 {
                parts.push(bulk_static(b"PX"));
                parts.push(bulk_owned(pttl.to_string()));
            }
            vec![parts]
        }
        KeySnapshot::Hash(fields) => {
            let mut parts = vec![
                bulk_static(b"HSET"),
                RespValue::BulkString(Some(key.clone())),
            ];
            for (f, v) in fields {
                parts.push(RespValue::BulkString(Some(f.clone())));
                parts.push(RespValue::BulkString(Some(v.clone())));
            }
            vec![parts]
        }
        KeySnapshot::List(items) => {
            let mut parts = vec![
                bulk_static(b"RPUSH"),
                RespValue::BulkString(Some(key.clone())),
            ];
            for e in items {
                parts.push(RespValue::BulkString(Some(e.clone())));
            }
            vec![parts]
        }
        KeySnapshot::Set(members) => {
            let mut parts = vec![
                bulk_static(b"SADD"),
                RespValue::BulkString(Some(key.clone())),
            ];
            for m in members {
                parts.push(RespValue::BulkString(Some(m.clone())));
            }
            vec![parts]
        }
        KeySnapshot::ZSet(members) => {
            let mut parts = vec![
                bulk_static(b"ZADD"),
                RespValue::BulkString(Some(key.clone())),
            ];
            for (m, score) in members {
                parts.push(bulk_owned(score_string(*score)));
                parts.push(RespValue::BulkString(Some(m.clone())));
            }
            vec![parts]
        }
        KeySnapshot::Geo(members) => {
            let mut parts = vec![
                bulk_static(b"GEOADD"),
                RespValue::BulkString(Some(key.clone())),
            ];
            for (m, lon, lat) in members {
                parts.push(bulk_owned(score_string(*lon)));
                parts.push(bulk_owned(score_string(*lat)));
                parts.push(RespValue::BulkString(Some(m.clone())));
            }
            vec![parts]
        }
        KeySnapshot::Stream(state) => stream_recreate_commands(key, state),
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
/// On mid-batch failure after one or more keys succeeded, returns an error string
/// (typically `IOERR` style); already-migrated keys stay on dest (and are gone
/// from source unless `copy`).
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

    let mut any_migrated = false;
    let mut deleted_keys = Vec::new();

    for key in keys {
        match migrate_one_key_on_stream(cache, &mut stream, key, opts).await {
            Ok(MigrateOneOutcome::Migrated) => {
                any_migrated = true;
                if !opts.copy {
                    deleted_keys.push(key.clone());
                }
            }
            Ok(MigrateOneOutcome::Missing) => {}
            Err(e) => {
                // Redis uses IOERR when the link fails after partial progress.
                if any_migrated {
                    return Err(format!(
                        "IOERR error or timeout transferring key to {} ({}). Partial keys may have moved.",
                        addr, e
                    ));
                }
                return Err(e);
            }
        }
    }

    if any_migrated {
        Ok((MigrateCommandResult::Ok, deleted_keys))
    } else {
        Ok((MigrateCommandResult::NoKey, deleted_keys))
    }
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
/// 4. source then dest `SETSLOT NODE <dest-id>` (best-effort dual-end with verify+retry;
///    still not atomic / no 2PC)
///
/// On key-move failure the slot is left MIGRATING/IMPORTING for operator recovery;
/// `migrated`/`skipped` on `failed_keys` report partial progress (retry leftover keys only).
/// Dual-end NODE failures are reported in `ReshardSlotResult` rather than rolled back;
/// use [`finish_slot_node`] / `CLUSTER RESHARD FINISH` to complete NODE without re-migrating.
///
/// **Range policy (Batch DO):** abort further slots on any non-`complete` status
/// (`failed_*` **or** `partial_*_node`) so a mid-range partial does not cascade.
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

    // Step 4: dual-end SETSLOT NODE with verify + retry (not atomic).
    let (source_node, dest_node, status) =
        dual_end_setslot_node(cluster, slot, dest_node_id, dest_ip, dest_port).await;

    Ok(ReshardSlotResult {
        slot,
        migrated: key_result.migrated,
        skipped: key_result.skipped,
        source_node,
        dest_node,
        status,
        warning: None,
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

    let (source_node, dest_node, status) =
        dual_end_setslot_node(cluster, slot, dest_node_id, &dest.ip, dest.port).await;

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
fn summarize_dual_end_status(source_node: &str, dest_node: &str) -> String {
    match (source_node, dest_node) {
        ("ok", "ok") => "complete".to_string(),
        ("ok", _) => "partial_dest_node".to_string(),
        (_, "ok") => "partial_source_node".to_string(),
        _ => "partial_both_node".to_string(),
    }
}

/// Source + dest `SETSLOT NODE <dest>` with local/remote verify and short retries.
///
/// Still **not** atomic across nodes (no 2PC). Returns `(source_node, dest_node, status)`.
///
/// **Order:** source NODE first, then dest. After source succeeds, clients may get
/// MOVED to dest while dest is still IMPORTING (ASK-only) until dest NODE lands —
/// the `partial_dest_node` window. Dest-first was considered; left source-first to
/// match Redis operator expectations for MOVED after source ownership flip.
async fn dual_end_setslot_node(
    cluster: &ClusterState,
    slot: u16,
    dest_node_id: &str,
    dest_ip: &str,
    dest_port: u16,
) -> (String, String, String) {
    let source_node = apply_source_node_with_retry(cluster, slot, dest_node_id).await;
    let dest_node =
        apply_dest_node_with_retry(slot, dest_node_id, dest_ip, dest_port).await;
    let status = summarize_dual_end_status(&source_node, &dest_node);
    (source_node, dest_node, status)
}

/// Local `SETSLOT NODE` + ownership verify, with retries.
async fn apply_source_node_with_retry(
    cluster: &ClusterState,
    slot: u16,
    dest_node_id: &str,
) -> String {
    let mut last_err = String::from("source NODE not attempted");
    for attempt in 0..NODE_SET_ATTEMPTS {
        match cluster.set_node(slot, dest_node_id) {
            Ok(()) => {
                if source_owns_as(cluster, slot, dest_node_id) {
                    return "ok".to_string();
                }
                last_err = "verify: local owner is not dest after SETSLOT NODE".into();
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

/// Remote dest `SETSLOT NODE` + CLUSTER SLOTS verify, with retries.
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
                match dest_setslot(
                    &mut stream,
                    &["CLUSTER", "SETSLOT", &slot_s, "NODE", dest_node_id],
                )
                .await
                {
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
            KeySnapshot::Hash(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].0.as_ref(), b"f");
            }
            _ => panic!("expected hash"),
        }

        let kl = Bytes::from_static(b"{t}.l");
        let l = cache.get_or_create_list(&kl).unwrap();
        l.write().rpush([Bytes::from_static(b"a")]);
        l.write().rpush([Bytes::from_static(b"b")]);
        match snapshot_key(&cache, &kl).unwrap() {
            KeySnapshot::List(items) => assert_eq!(items.len(), 2),
            _ => panic!("expected list"),
        }
    }
}
