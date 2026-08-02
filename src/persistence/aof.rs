//! Append-Only File (AOF) persistence.
//!
//! Format: stream of RESP arrays (Redis-compatible command log).
//! Rewrite materializes the current DB as:
//!   0. `FUNCTION FLUSH` + `FUNCTION LOAD REPLACE` (Batch GY — global, before DBs)
//!   1. `FT.CREATE` (search schema) — before key dumps so HSET auto-index repopulates
//!   2. SET / ZADD / GEOADD / HSET / RPUSH / SADD / XADD / XGROUP …
//!   3. `FT._LOADGRAPH` (Batch FX) — durable HNSW levels/edges after docs re-index
//!   4. `FT.ALIASADD` (aliases after data is re-indexed)
//! with SELECT between logical databases.
//!
//! **HNSW honesty (Batch FV/FX):** AOF rewrite emits `FT.CREATE` HNSW params
//! (`M`, `EF_CONSTRUCTION`) + document HSETs + `FT._LOADGRAPH index field <blob>`
//! for every non-empty dual-written HNSW field so load is edge-identical like
//! RDB v6. Old AOF without `FT._LOADGRAPH` still rebuilds by re-`add` (levels
//! re-sampled). `FT._LOADGRAPH` is rewrite/load-only (applied in AOF replay).

use crate::cache::Cache;
use crate::databases::Databases;
use crate::error::{Error, Result};
use crate::persistence::rdb::DbSnapshot;
use crate::protocol::RespValue;
use crate::scripting::FunctionLibraryStore;
use crate::search_index::{DistanceMetric, FieldType, IndexDefinition, VectorAlgorithm};
use crate::stream_type::StreamId;
use crate::vector_search::HnswGraphSnapshot;
use bytes::Bytes;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Serialize a command (argv) as a RESP array.
///
/// Writes bulk strings directly (no intermediate `RespValue` tree / `Bytes`
/// clones of each arg). Hot path for AOF append and replication propagation.
pub fn encode_command(args: &[Bytes]) -> Bytes {
    use bytes::{BufMut, BytesMut};

    // *N\r\n + per-arg $len\r\n<data>\r\n — digit budget is generous (20).
    let mut cap: usize = 1 + 20 + 2;
    for a in args {
        cap = cap
            .saturating_add(1 + 20 + 2)
            .saturating_add(a.len())
            .saturating_add(2);
    }
    let mut buf = BytesMut::with_capacity(cap);
    buf.put_u8(b'*');
    put_usize(&mut buf, args.len());
    buf.put_slice(b"\r\n");
    for a in args {
        buf.put_u8(b'$');
        put_usize(&mut buf, a.len());
        buf.put_slice(b"\r\n");
        buf.extend_from_slice(a);
        buf.put_slice(b"\r\n");
    }
    buf.freeze()
}

/// Write a decimal usize without heap allocation (hot encode path).
#[inline]
fn put_usize(buf: &mut bytes::BytesMut, mut n: usize) {
    use bytes::BufMut;
    if n == 0 {
        buf.put_u8(b'0');
        return;
    }
    let mut tmp = [0u8; 20];
    let mut i = 20;
    while n > 0 {
        i -= 1;
        tmp[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    buf.extend_from_slice(&tmp[i..]);
}

/// Open or create an AOF file for append.
pub struct AofWriter {
    path: PathBuf,
    file: File,
}

impl AofWriter {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        Ok(Self { path, file })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append a RESP-encoded command and fsync (every write — safe, slower).
    pub fn append_raw(&mut self, data: &[u8]) -> Result<()> {
        self.file.write_all(data)?;
        self.file.sync_data()?;
        Ok(())
    }

    pub fn append_command(&mut self, args: &[Bytes]) -> Result<()> {
        let encoded = encode_command(args);
        self.append_raw(&encoded)
    }
}

/// Encode `FT.CREATE` for every search index on this cache.
///
/// Emitted **before** key dumps so replay creates schema first; subsequent HSET
/// commands auto-index documents into the recreated indices.
fn encode_search_create_commands(cache: &Cache, buf: &mut Vec<u8>) {
    for def in cache.list_search_index_definitions() {
        let mut args = vec![
            Bytes::from_static(b"FT.CREATE"),
            Bytes::from(def.name.clone()),
        ];
        if !def.prefix.is_empty() {
            args.push(Bytes::from_static(b"PREFIX"));
            args.push(Bytes::from(def.prefix.len().to_string()));
            for p in &def.prefix {
                args.push(Bytes::from(p.clone()));
            }
        }
        args.push(Bytes::from_static(b"SCHEMA"));
        for field in &def.fields {
            args.push(Bytes::from(field.name.clone()));
            match &field.field_type {
                FieldType::Text { weight, sortable } => {
                    args.push(Bytes::from_static(b"TEXT"));
                    if (*weight - 1.0).abs() > f64::EPSILON {
                        args.push(Bytes::from_static(b"WEIGHT"));
                        args.push(Bytes::from(weight.to_string()));
                    }
                    if *sortable {
                        args.push(Bytes::from_static(b"SORTABLE"));
                    }
                }
                FieldType::Numeric { sortable } => {
                    args.push(Bytes::from_static(b"NUMERIC"));
                    if *sortable {
                        args.push(Bytes::from_static(b"SORTABLE"));
                    }
                }
                FieldType::Tag { separator, sortable } => {
                    args.push(Bytes::from_static(b"TAG"));
                    if separator != "," {
                        args.push(Bytes::from_static(b"SEPARATOR"));
                        args.push(Bytes::from(separator.clone()));
                    }
                    if *sortable {
                        args.push(Bytes::from_static(b"SORTABLE"));
                    }
                }
                FieldType::Vector {
                    algorithm,
                    dimensions,
                    distance_metric,
                } => {
                    args.push(Bytes::from_static(b"VECTOR"));
                    match algorithm {
                        VectorAlgorithm::Flat => {
                            args.push(Bytes::from_static(b"FLAT"));
                        }
                        VectorAlgorithm::HNSW {
                            m,
                            ef_construction,
                        } => {
                            args.push(Bytes::from_static(b"HNSW"));
                            args.push(Bytes::from_static(b"M"));
                            args.push(Bytes::from(m.to_string()));
                            // Round-trip ef_construction (parser default 200 when omitted).
                            args.push(Bytes::from_static(b"EF_CONSTRUCTION"));
                            args.push(Bytes::from(ef_construction.to_string()));
                        }
                    }
                    args.push(Bytes::from_static(b"TYPE"));
                    args.push(Bytes::from_static(b"FLOAT32"));
                    args.push(Bytes::from_static(b"DIM"));
                    args.push(Bytes::from(dimensions.to_string()));
                    args.push(Bytes::from_static(b"DISTANCE_METRIC"));
                    let metric = match distance_metric {
                        DistanceMetric::Cosine => Bytes::from_static(b"COSINE"),
                        DistanceMetric::L2 => Bytes::from_static(b"L2"),
                        DistanceMetric::IP => Bytes::from_static(b"IP"),
                    };
                    args.push(metric);
                }
            }
        }
        buf.extend_from_slice(&encode_command(&args));
    }
}

/// Encode `FT.ALIASADD` for every alias → real index mapping.
///
/// Emitted **after** key dumps so aliases attach once documents are re-indexed.
fn encode_search_alias_commands(cache: &Cache, buf: &mut Vec<u8>) {
    for (alias, index) in cache.list_search_aliases() {
        let args = vec![
            Bytes::from_static(b"FT.ALIASADD"),
            Bytes::from(alias),
            Bytes::from(index),
        ];
        buf.extend_from_slice(&encode_command(&args));
    }
}

/// Encode `FT._LOADGRAPH` for every non-empty HNSW field (Batch FX).
///
/// Emitted **after** key dumps so vectors are re-indexed first, then the durable
/// graph overwrites levels/edges/entry_point (edge-identical like RDB v6).
fn encode_search_hnsw_graph_commands(cache: &Cache, buf: &mut Vec<u8>) {
    for (index_name, field, snap) in cache.export_hnsw_graphs() {
        if snap.is_empty() {
            continue;
        }
        let blob = snap.encode();
        let args = vec![
            Bytes::from_static(b"FT._LOADGRAPH"),
            Bytes::from(index_name),
            Bytes::from(field),
            Bytes::from(blob),
        ];
        buf.extend_from_slice(&encode_command(&args));
    }
}

/// Encode one DB: FT.CREATE → keyspace dump → FT._LOADGRAPH → FT.ALIASADD (no SELECT).
fn encode_db_commands(cache: &Cache, snap: &DbSnapshot, buf: &mut Vec<u8>) {
    encode_search_create_commands(cache, buf);
    encode_snapshot_commands(snap, buf);
    encode_search_hnsw_graph_commands(cache, buf);
    encode_search_alias_commands(cache, buf);
}

/// Encode global Redis Functions for AOF rewrite (Batch GY).
///
/// Emitted once at the start of rewrite (before any SELECT/DB body) so load
/// restores libraries server-wide. Live AOF already appends `FUNCTION` writes.
fn encode_function_commands(libs: &FunctionLibraryStore, buf: &mut Vec<u8>) {
    if libs.is_empty() {
        return;
    }
    buf.extend_from_slice(&encode_command(&[Bytes::from_static(b"FUNCTION"), Bytes::from_static(b"FLUSH")]));
    for lib in libs.list() {
        let args = vec![
            Bytes::from_static(b"FUNCTION"),
            Bytes::from_static(b"LOAD"),
            Bytes::from_static(b"REPLACE"),
            Bytes::from(lib.code),
        ];
        buf.extend_from_slice(&encode_command(&args));
    }
}

/// Encode one DB snapshot as AOF rewrite commands (no SELECT).
fn encode_snapshot_commands(snap: &DbSnapshot, buf: &mut Vec<u8>) {
    for s in &snap.strings {
        let mut args = vec![
            Bytes::from_static(b"SET"),
            s.key.clone(),
            s.value.clone(),
        ];
        // Preserve absolute expiry via PXAT if present
        if s.expire_unix_ms >= 0 {
            args.push(Bytes::from_static(b"PXAT"));
            args.push(Bytes::from(s.expire_unix_ms.to_string()));
        }
        buf.extend_from_slice(&encode_command(&args));
    }

    for z in &snap.zsets {
        // ZADD key score member [score member ...]
        let mut args = vec![Bytes::from_static(b"ZADD"), z.key.clone()];
        for (m, score) in &z.members {
            args.push(Bytes::from(score.to_string()));
            args.push(m.clone());
        }
        if args.len() > 2 {
            buf.extend_from_slice(&encode_command(&args));
        }
    }

    for g in &snap.geos {
        // GEOADD key lon lat member ...
        let mut args = vec![Bytes::from_static(b"GEOADD"), g.key.clone()];
        for (m, lon, lat) in &g.members {
            args.push(Bytes::from(lon.to_string()));
            args.push(Bytes::from(lat.to_string()));
            args.push(m.clone());
        }
        if args.len() > 2 {
            buf.extend_from_slice(&encode_command(&args));
        }
    }

    for h in &snap.hashes {
        // HSET key field value [field value ...]
        let mut args = vec![Bytes::from_static(b"HSET"), h.key.clone()];
        for (f, v) in &h.fields {
            args.push(f.clone());
            args.push(v.clone());
        }
        if args.len() > 2 {
            buf.extend_from_slice(&encode_command(&args));
        }
    }

    for l in &snap.lists {
        // RPUSH preserves left-to-right order of stored elements
        let mut args = vec![Bytes::from_static(b"RPUSH"), l.key.clone()];
        for e in &l.elements {
            args.push(e.clone());
        }
        if args.len() > 2 {
            buf.extend_from_slice(&encode_command(&args));
        }
    }

    for s in &snap.sets {
        // SADD key member [member ...]
        let mut args = vec![Bytes::from_static(b"SADD"), s.key.clone()];
        for m in &s.members {
            args.push(m.clone());
        }
        if args.len() > 2 {
            buf.extend_from_slice(&encode_command(&args));
        }
    }

    for st in &snap.streams {
        // XADD with explicit IDs so rewrite is deterministic.
        for (id, fields) in &st.state.entries {
            let mut args = vec![
                Bytes::from_static(b"XADD"),
                st.key.clone(),
                Bytes::from(id.to_string_id()),
            ];
            for (f, v) in fields {
                args.push(f.clone());
                args.push(v.clone());
            }
            if args.len() > 3 {
                buf.extend_from_slice(&encode_command(&args));
            }
        }
        // Preserve last_generated_id watermark when it exceeds the max remaining entry
        // (e.g. after XDEL of the highest IDs).
        let max_entry = st
            .state
            .entries
            .iter()
            .map(|(id, _)| *id)
            .max()
            .unwrap_or(StreamId::ZERO);
        if st.state.last_generated_id > max_entry {
            let args = vec![
                Bytes::from_static(b"XSETID"),
                st.key.clone(),
                Bytes::from(st.state.last_generated_id.to_string_id()),
            ];
            buf.extend_from_slice(&encode_command(&args));
        }
        // Groups: CREATE (+ MKSTREAM for empty streams), SETID, then XCLAIM FORCE for PEL.
        for g in &st.state.groups {
            let args = vec![
                Bytes::from_static(b"XGROUP"),
                Bytes::from_static(b"CREATE"),
                st.key.clone(),
                g.name.clone(),
                Bytes::from(g.last_delivered_id.to_string_id()),
                Bytes::from_static(b"MKSTREAM"),
            ];
            buf.extend_from_slice(&encode_command(&args));
            // Re-assert cursor (CREATE already sets it; keep explicit for clarity)
            let args = vec![
                Bytes::from_static(b"XGROUP"),
                Bytes::from_static(b"SETID"),
                st.key.clone(),
                g.name.clone(),
                Bytes::from(g.last_delivered_id.to_string_id()),
            ];
            buf.extend_from_slice(&encode_command(&args));
            // XCLAIM key group consumer min-idle id FORCE [TIME ms] [RETRYCOUNT n]
            for pe in &g.pending {
                let args = vec![
                    Bytes::from_static(b"XCLAIM"),
                    st.key.clone(),
                    g.name.clone(),
                    pe.consumer.clone(),
                    Bytes::from_static(b"0"),
                    Bytes::from(pe.id.to_string_id()),
                    Bytes::from_static(b"FORCE"),
                    Bytes::from_static(b"TIME"),
                    Bytes::from(pe.delivery_time_ms.to_string()),
                    Bytes::from_static(b"RETRYCOUNT"),
                    Bytes::from(pe.delivery_count.max(1).to_string()),
                ];
                buf.extend_from_slice(&encode_command(&args));
            }
        }
    }

    // Typed-key TTLs (hash/list/set/zset/geo/stream)
    for (key, exp) in &snap.typed_expires {
        if *exp >= 0 {
            let args = vec![
                Bytes::from_static(b"PEXPIREAT"),
                key.clone(),
                Bytes::from(exp.to_string()),
            ];
            buf.extend_from_slice(&encode_command(&args));
        }
    }
}

fn write_aof_buffer(buf: &[u8], path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let tmp = path.with_extension("aof.tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(buf)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

/// Rewrite AOF from current cache state (atomic replace). Single DB (no SELECT).
pub fn rewrite(cache: &Cache, path: &Path) -> Result<()> {
    rewrite_with_functions(cache, path, None)
}

pub fn rewrite_with_functions(
    cache: &Cache,
    path: &Path,
    function_libs: Option<&FunctionLibraryStore>,
) -> Result<()> {
    let snap = DbSnapshot::from_cache(cache)?;
    let mut buf = Vec::with_capacity(4096);
    if let Some(libs) = function_libs {
        encode_function_commands(libs, &mut buf);
    }
    encode_db_commands(cache, &snap, &mut buf);
    write_aof_buffer(&buf, path)
}

/// Rewrite AOF from all non-empty logical databases, emitting SELECT between them.
///
/// A DB is non-empty if it has keyspace data **or** search indices/aliases.
///
/// Snapshot + search schema encode run under
/// [`Databases::with_stable_keyspace_view`] so a concurrent multi-DB keyspace
/// install cannot produce a torn AOF (DB0-new + DB1-old). Matches RDB
/// [`crate::persistence::rdb::MultiDbSnapshot::from_databases`].
pub fn rewrite_databases(databases: &Databases, path: &Path) -> Result<()> {
    rewrite_databases_with_functions(databases, path, None)
}

pub fn rewrite_databases_with_functions(
    databases: &Databases,
    path: &Path,
    function_libs: Option<&FunctionLibraryStore>,
) -> Result<()> {
    let buf = databases.with_stable_keyspace_view(|| {
        let mut non_empty: Vec<(usize, Arc<Cache>, DbSnapshot)> = Vec::new();
        for (idx, cache) in databases.iter().enumerate() {
            let snap = DbSnapshot::from_cache(cache)?;
            if !snap.is_empty() || cache.has_search_state() {
                non_empty.push((idx, cache.clone(), snap));
            }
        }

        let mut buf = Vec::with_capacity(4096);
        // Batch GY: Functions are global — emit before any SELECT/DB body.
        if let Some(libs) = function_libs {
            encode_function_commands(libs, &mut buf);
        }
        // Still rewrite when only functions exist (empty keyspaces).
        if non_empty.is_empty() {
            return Ok::<Vec<u8>, crate::error::Error>(buf);
        }
        let multi = non_empty.len() > 1;
        for (idx, cache, snap) in &non_empty {
            // Emit SELECT for every DB when multiple are non-empty, and for any
            // non-zero single DB so load restores the correct keyspace.
            if multi || *idx != 0 {
                let args = vec![
                    Bytes::from_static(b"SELECT"),
                    Bytes::from(idx.to_string()),
                ];
                buf.extend_from_slice(&encode_command(&args));
            }
            // Search CREATE/ALIAS still read the live Cache — must stay under
            // the epoch read held by this closure.
            encode_db_commands(cache, snap, &mut buf);
        }
        Ok::<Vec<u8>, crate::error::Error>(buf)
    })?;

    write_aof_buffer(&buf, path)
}

/// Load AOF by replaying RESP commands through a provided apply callback.
///
/// `apply` receives argv as Vec<Bytes> (command + args, uppercased command at [0] optional).
pub fn load_file_with<F>(path: &Path, mut apply: F) -> Result<usize>
where
    F: FnMut(Vec<Bytes>) -> Result<()>,
{
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut data = Vec::new();
    reader.read_to_end(&mut data)?;

    let mut parser = crate::protocol::RespParser::new();
    parser.feed(&data);

    let mut count = 0usize;
    while let Some(value) = parser.parse()? {
        let args = match value {
            RespValue::Array(arr) => arr,
            _ => {
                return Err(Error::ParseError(
                    "AOF entry must be a RESP array".into(),
                ))
            }
        };
        let mut argv = Vec::with_capacity(args.len());
        for a in args {
            match a {
                RespValue::BulkString(Some(b)) => argv.push(b),
                RespValue::SimpleString(b) => argv.push(b),
                RespValue::Integer(i) => argv.push(Bytes::from(i.to_string())),
                _ => {
                    return Err(Error::ParseError(
                        "invalid AOF argument type".into(),
                    ))
                }
            }
        }
        if argv.is_empty() {
            continue;
        }
        apply(argv)?;
        count += 1;
    }
    Ok(count)
}

/// Parse `FT.CREATE` argv (command name at [0]) into an IndexDefinition.
///
/// Thin wrapper around the shared [`IndexDefinition::from_ft_create_argv`] so
/// AOF load and the command path cannot drift. Returns `None` on incomplete /
/// invalid input. Callers treat truncated argv (`len < 4`) as a liberal skip
/// and non-truncated parse failures as load errors.
fn parse_ft_create_definition(argv: &[Bytes]) -> Option<IndexDefinition> {
    if argv.len() < 4 {
        return None;
    }
    IndexDefinition::from_ft_create_argv(argv).ok()
}

/// Apply a single AOF write command against one cache.
/// Apply `FUNCTION` subcommands during AOF load (Batch GY).
fn apply_function_command(libs: &FunctionLibraryStore, argv: &[Bytes]) -> Result<()> {
    if argv.len() < 2 {
        return Ok(());
    }
    let sub = String::from_utf8_lossy(&argv[1]).to_ascii_uppercase();
    match sub.as_str() {
        "FLUSH" => {
            libs.flush();
            Ok(())
        }
        "DELETE" => {
            if argv.len() < 3 {
                return Ok(());
            }
            let name = String::from_utf8_lossy(&argv[2]);
            let _ = libs.delete(&name);
            Ok(())
        }
        "LOAD" => {
            // FUNCTION LOAD [REPLACE] <code>
            let mut i = 2;
            let mut replace = false;
            if i < argv.len()
                && String::from_utf8_lossy(&argv[i]).eq_ignore_ascii_case("REPLACE")
            {
                replace = true;
                i += 1;
            }
            if i >= argv.len() {
                return Ok(());
            }
            let code = String::from_utf8_lossy(&argv[i]);
            libs.load_from_source(&code, replace)
                .map_err(|e| Error::ParseError(format!("AOF FUNCTION LOAD: {}", e)))?;
            Ok(())
        }
        "RESTORE" => {
            // FUNCTION RESTORE <payload> [FLUSH|APPEND|REPLACE]
            if argv.len() < 3 {
                return Ok(());
            }
            let payload = &argv[2];
            let mode = if argv.len() >= 4 {
                String::from_utf8_lossy(&argv[3]).to_ascii_lowercase()
            } else {
                "append".to_string()
            };
            libs.restore_from_dump(payload, &mode)
                .map_err(|e| Error::ParseError(format!("AOF FUNCTION RESTORE: {}", e)))?;
            Ok(())
        }
        _ => Ok(()),
    }
}

pub fn apply_command_to_cache(cache: &Cache, argv: &[Bytes]) -> Result<()> {
    use crate::entry::StoreOptions;
    use crate::search_index::DocumentField;
    use std::collections::HashMap;

    if argv.is_empty() {
        return Ok(());
    }
    let cmd = String::from_utf8_lossy(&argv[0]).to_uppercase();
    match cmd.as_str() {
        "SET" => {
            if argv.len() < 3 {
                return Ok(());
            }
            let mut opts = StoreOptions::default();
            let mut i = 3;
            while i < argv.len() {
                let opt = String::from_utf8_lossy(&argv[i]).to_uppercase();
                match opt.as_str() {
                    "PXAT" | "EXAT" => {
                        if i + 1 >= argv.len() {
                            break;
                        }
                        let ts: i64 = std::str::from_utf8(&argv[i + 1])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(-1);
                        if opt == "EXAT" {
                            opts.exat_ms = Some((ts as u64).saturating_mul(1000));
                        } else {
                            opts.exat_ms = Some(ts as u64);
                        }
                        i += 2;
                    }
                    "PX" => {
                        if i + 1 >= argv.len() {
                            break;
                        }
                        let ms: u64 = std::str::from_utf8(&argv[i + 1])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0);
                        opts.ttl_ms = Some(ms);
                        i += 2;
                    }
                    "EX" => {
                        if i + 1 >= argv.len() {
                            break;
                        }
                        let s: u64 = std::str::from_utf8(&argv[i + 1])
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0);
                        opts.ttl_ms = Some(s.saturating_mul(1000));
                        i += 2;
                    }
                    "NX" => {
                        opts.nx = true;
                        i += 1;
                    }
                    "XX" => {
                        opts.xx = true;
                        i += 1;
                    }
                    _ => i += 1,
                }
            }
            let _ = cache.store(argv[1].clone(), argv[2].clone(), opts);
            Ok(())
        }
        "DEL" => {
            for k in argv.iter().skip(1) {
                let _ = cache.delete(k);
            }
            Ok(())
        }
        "PERSIST" => {
            if argv.len() >= 2 {
                let _ = cache.persist(&argv[1]);
            }
            Ok(())
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
            Ok(())
        }
        "INCR" | "INCRBY" => {
            if argv.len() < 2 {
                return Ok(());
            }
            let delta = if cmd == "INCR" {
                1
            } else {
                std::str::from_utf8(argv.get(2).map(|b| b.as_ref()).unwrap_or(b"1"))
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1)
            };
            let _ = cache.incr(&argv[1], delta);
            Ok(())
        }
        "DECR" | "DECRBY" => {
            if argv.len() < 2 {
                return Ok(());
            }
            let delta = if cmd == "DECR" {
                1
            } else {
                std::str::from_utf8(argv.get(2).map(|b| b.as_ref()).unwrap_or(b"1"))
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(1)
            };
            let _ = cache.decr(&argv[1], delta);
            Ok(())
        }
        "ZADD" => {
            if argv.len() < 4 {
                return Ok(());
            }
            let zset = cache.get_or_create_sorted_set(&argv[1])?;
            let mut set = zset
                .write();
            let mut i = 2;
            while i + 1 < argv.len() {
                let score: f64 = std::str::from_utf8(&argv[i])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                set.add(argv[i + 1].clone(), score);
                i += 2;
            }
            Ok(())
        }
        "ZREM" => {
            if argv.len() < 3 {
                return Ok(());
            }
            if let Some(zset) = cache.get_sorted_set(&argv[1]) {
                let mut set = zset
                    .write();
                for m in argv.iter().skip(2) {
                    set.remove(m);
                }
            }
            Ok(())
        }
        "GEOADD" => {
            if argv.len() < 5 {
                return Ok(());
            }
            let geoset = cache.get_or_create_geo_set(&argv[1])?;
            let mut set = geoset
                .write();
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
            Ok(())
        }
        "HSET" => {
            if argv.len() < 4 {
                return Ok(());
            }
            let key = argv[1].clone();
            let hash = cache.get_or_create_hash(&key)?;
            let index_fields = {
                let mut h = hash.write();
                let mut i = 2;
                while i + 1 < argv.len() {
                    h.hset(argv[i].clone(), argv[i + 1].clone());
                    i += 2;
                }
                // Snapshot for search auto-index (same as command-path HSET).
                let mut index_fields = HashMap::new();
                for (f, v) in h.hgetall() {
                    let fname = String::from_utf8_lossy(&f).into_owned();
                    let fval = String::from_utf8_lossy(&v).into_owned();
                    index_fields.insert(fname, DocumentField::Text(fval));
                }
                index_fields
            };
            cache.auto_index_key(&key, index_fields);
            Ok(())
        }
        "FT.CREATE" => {
            // Truncated argv: match other AOF apply paths (skip liberally).
            // Parsed but mutator fails, or non-truncated unparsable schema: fail load.
            if argv.len() < 4 {
                return Ok(());
            }
            match parse_ft_create_definition(argv) {
                Some(def) => cache
                    .create_search_index(def)
                    .map_err(map_ft_mutator_error)?,
                None => {
                    return Err(Error::ParseError(
                        "invalid or incomplete FT.CREATE in AOF".into(),
                    ));
                }
            }
            Ok(())
        }
        // Batch GY: FUNCTION is server-wide — handled in load_into_databases, not here.
        "FUNCTION" => Ok(()),
        // Batch FX: rewrite-only durable HNSW graph. Applied during AOF load
        // after FT.CREATE + docs so vectors exist; overwrites re-sampled levels.
        "FT._LOADGRAPH" => {
            if argv.len() < 4 {
                // Truncated: skip liberally (matches other AOF apply paths).
                return Ok(());
            }
            let index_name = String::from_utf8_lossy(&argv[1]).into_owned();
            let field = String::from_utf8_lossy(&argv[2]).into_owned();
            let snap = HnswGraphSnapshot::decode(&argv[3]).map_err(|e| {
                Error::ParseError(format!("invalid FT._LOADGRAPH blob: {}", e))
            })?;
            cache
                .apply_hnsw_graphs(&[(index_name, field, snap)])
                .map_err(|e| Error::ParseError(format!("FT._LOADGRAPH apply: {}", e)))?;
            Ok(())
        }
        "FT.DROPINDEX" => {
            if argv.len() >= 2 {
                let name = String::from_utf8_lossy(&argv[1]);
                cache
                    .drop_search_index(&name)
                    .map_err(map_ft_mutator_error)?;
            }
            Ok(())
        }
        "FT.ALIASADD" => {
            if argv.len() >= 3 {
                let alias = String::from_utf8_lossy(&argv[1]);
                let index = String::from_utf8_lossy(&argv[2]);
                cache
                    .alias_add(&alias, &index)
                    .map_err(map_ft_mutator_error)?;
            }
            Ok(())
        }
        "FT.ALIASDEL" => {
            if argv.len() >= 2 {
                let alias = String::from_utf8_lossy(&argv[1]);
                cache.alias_del(&alias).map_err(map_ft_mutator_error)?;
            }
            Ok(())
        }
        "FT.ALIASUPDATE" => {
            if argv.len() >= 3 {
                let alias = String::from_utf8_lossy(&argv[1]);
                let index = String::from_utf8_lossy(&argv[2]);
                cache
                    .alias_update(&alias, &index)
                    .map_err(map_ft_mutator_error)?;
            }
            Ok(())
        }
        "HDEL" => {
            if argv.len() < 3 {
                return Ok(());
            }
            if let Some(hash) = cache.get_hash(&argv[1]) {
                let mut h = hash
                    .write();
                let fields: Vec<_> = argv[2..].to_vec();
                h.hdel(&fields);
                if h.is_empty() {
                    drop(h);
                    cache.remove_hash(&argv[1]);
                }
            }
            Ok(())
        }
        "LPUSH" => {
            if argv.len() < 3 {
                return Ok(());
            }
            let list = cache.get_or_create_list(&argv[1])?;
            let mut l = list
                .write();
            l.lpush(argv[2..].iter().cloned());
            Ok(())
        }
        "RPUSH" => {
            if argv.len() < 3 {
                return Ok(());
            }
            let list = cache.get_or_create_list(&argv[1])?;
            let mut l = list
                .write();
            l.rpush(argv[2..].iter().cloned());
            Ok(())
        }
        "LPOP" => {
            if argv.len() < 2 {
                return Ok(());
            }
            if let Some(list) = cache.get_list(&argv[1]) {
                let mut l = list
                    .write();
                let _ = l.lpop();
                if l.is_empty() {
                    drop(l);
                    cache.remove_list(&argv[1]);
                }
            }
            Ok(())
        }
        "RPOP" => {
            if argv.len() < 2 {
                return Ok(());
            }
            if let Some(list) = cache.get_list(&argv[1]) {
                let mut l = list
                    .write();
                let _ = l.rpop();
                if l.is_empty() {
                    drop(l);
                    cache.remove_list(&argv[1]);
                }
            }
            Ok(())
        }
        "SADD" => {
            if argv.len() < 3 {
                return Ok(());
            }
            let set = cache.get_or_create_set(&argv[1])?;
            let mut s = set
                .write();
            s.sadd(argv[2..].iter().cloned());
            Ok(())
        }
        "SREM" => {
            if argv.len() < 3 {
                return Ok(());
            }
            if let Some(set) = cache.get_set(&argv[1]) {
                let mut s = set
                    .write();
                s.srem(argv[2..].iter().cloned());
                if s.is_empty() {
                    drop(s);
                    cache.remove_set(&argv[1]);
                }
            }
            Ok(())
        }
        "XADD" => {
            // XADD key id field value [field value ...]
            // (rewrite uses explicit IDs; live AOF may use *)
            if argv.len() < 5 {
                return Ok(());
            }
            let key = &argv[1];
            let id_spec = std::str::from_utf8(&argv[2]).unwrap_or("*");
            let mut fields = Vec::new();
            let mut i = 3;
            while i + 1 < argv.len() {
                fields.push((argv[i].clone(), argv[i + 1].clone()));
                i += 2;
            }
            if fields.is_empty() {
                return Ok(());
            }
            let stream = cache.get_or_create_stream(key)?;
            let mut s = stream
                .write();
            let _ = s.xadd(id_spec, fields);
            Ok(())
        }
        "XDEL" => {
            if argv.len() < 3 {
                return Ok(());
            }
            if let Some(stream) = cache.get_stream(&argv[1]) {
                let mut s = stream
                    .write();
                let mut ids = Vec::new();
                for raw in argv.iter().skip(2) {
                    if let Ok(txt) = std::str::from_utf8(raw) {
                        if let Some(id) = StreamId::parse_explicit(txt).or_else(|| StreamId::parse(txt))
                        {
                            ids.push(id);
                        }
                    }
                }
                let _ = s.xdel(&ids);
            }
            Ok(())
        }
        "XTRIM" => {
            // XTRIM key MAXLEN [~] count
            if argv.len() < 4 {
                return Ok(());
            }
            if let Some(stream) = cache.get_stream(&argv[1]) {
                let mut s = stream
                    .write();
                let mut i = 2;
                let sub = String::from_utf8_lossy(&argv[i]).to_uppercase();
                if sub == "MAXLEN" {
                    i += 1;
                    if i < argv.len() && argv[i].as_ref() == b"~" {
                        i += 1;
                    }
                    if i < argv.len() {
                        let max: usize = std::str::from_utf8(&argv[i])
                            .ok()
                            .and_then(|t| t.parse().ok())
                            .unwrap_or(0);
                        let _ = s.trim_maxlen(max);
                    }
                }
            }
            Ok(())
        }
        "XGROUP" => {
            // XGROUP CREATE key groupname id [MKSTREAM]
            // XGROUP DESTROY key groupname
            if argv.len() < 3 {
                return Ok(());
            }
            let sub = String::from_utf8_lossy(&argv[1]).to_uppercase();
            match sub.as_str() {
                "CREATE" => {
                    if argv.len() < 5 {
                        return Ok(());
                    }
                    let key = &argv[2];
                    let gname = argv[3].clone();
                    let id_spec = std::str::from_utf8(&argv[4]).unwrap_or("0");
                    let id = if id_spec == "$" {
                        // last entry id or zero
                        cache
                            .get_stream(key)
                            .map(|s| { let st = s.read(); st.last_id() })
                            .unwrap_or(StreamId::ZERO)
                    } else if id_spec == "0" || id_spec == "0-0" {
                        StreamId::ZERO
                    } else {
                        StreamId::parse_explicit(id_spec)
                            .or_else(|| StreamId::parse(id_spec))
                            .unwrap_or(StreamId::ZERO)
                    };
                    let mkstream = argv.iter().skip(5).any(|a| {
                        String::from_utf8_lossy(a).eq_ignore_ascii_case("MKSTREAM")
                    });
                    if cache.get_stream(key).is_none() {
                        if mkstream {
                            let _ = cache.get_or_create_stream(key)?;
                        } else {
                            return Ok(());
                        }
                    }
                    if let Some(stream) = cache.get_stream(key) {
                        let mut s = stream
                            .write();
                        let _ = s.group_create(gname, id, true, None);
                    }
                    Ok(())
                }
                "DESTROY" => {
                    if argv.len() < 4 {
                        return Ok(());
                    }
                    if let Some(stream) = cache.get_stream(&argv[2]) {
                        let mut s = stream
                            .write();
                        let _ = s.group_destroy(&argv[3]);
                    }
                    Ok(())
                }
                "SETID" => {
                    if argv.len() < 5 {
                        return Ok(());
                    }
                    let id_spec = std::str::from_utf8(&argv[4]).unwrap_or("0-0");
                    let id = StreamId::parse_explicit(id_spec)
                        .or_else(|| StreamId::parse(id_spec))
                        .unwrap_or(StreamId::ZERO);
                    if let Some(stream) = cache.get_stream(&argv[2]) {
                        let mut s = stream
                            .write();
                        let _ = s.group_setid(&argv[3], id, None);
                    }
                    Ok(())
                }
                _ => Ok(()),
            }
        }
        "XSETID" => {
            // XSETID key last-id
            if argv.len() < 3 {
                return Ok(());
            }
            let id_spec = std::str::from_utf8(&argv[2]).unwrap_or("0-0");
            let id = StreamId::parse_explicit(id_spec)
                .or_else(|| StreamId::parse(id_spec))
                .unwrap_or(StreamId::ZERO);
            if let Some(stream) = cache.get_stream(&argv[1]) {
                let mut s = stream
                    .write();
                let _ = s.xsetid(id, None, None);
            }
            Ok(())
        }
        "XCLAIM" => {
            // XCLAIM key group consumer min-idle id [id ...] [FORCE] [TIME ms] [RETRYCOUNT n]
            if argv.len() < 6 {
                return Ok(());
            }
            let key = &argv[1];
            let group = &argv[2];
            let consumer = &argv[3];
            let mut force = false;
            let mut time_ms: Option<u64> = None;
            let mut retry: Option<u64> = None;
            let mut ids: Vec<StreamId> = Vec::new();
            let mut i = 5;
            while i < argv.len() {
                let tok = String::from_utf8_lossy(&argv[i]);
                let upper = tok.to_uppercase();
                match upper.as_str() {
                    "FORCE" => {
                        force = true;
                        i += 1;
                    }
                    "JUSTID" => {
                        i += 1;
                    }
                    "TIME" => {
                        if i + 1 < argv.len() {
                            time_ms = std::str::from_utf8(&argv[i + 1])
                                .ok()
                                .and_then(|s| s.parse().ok());
                        }
                        i += 2;
                    }
                    "RETRYCOUNT" => {
                        if i + 1 < argv.len() {
                            retry = std::str::from_utf8(&argv[i + 1])
                                .ok()
                                .and_then(|s| s.parse().ok());
                        }
                        i += 2;
                    }
                    "IDLE" => {
                        i += 2; // skip IDLE ms
                    }
                    _ => {
                        if let Some(id) = StreamId::parse_explicit(&tok)
                            .or_else(|| StreamId::parse(&tok))
                        {
                            ids.push(id);
                        }
                        i += 1;
                    }
                }
            }
            if !force || ids.is_empty() {
                return Ok(());
            }
            if let Some(stream) = cache.get_stream(key) {
                let mut s = stream
                    .write();
                let _ = s.xclaim_force(group, consumer, &ids, time_ms, retry);
            }
            Ok(())
        }
        "XACK" => {
            // XACK key group id [id ...]
            if argv.len() < 4 {
                return Ok(());
            }
            if let Some(stream) = cache.get_stream(&argv[1]) {
                let mut s = stream
                    .write();
                let ids: Vec<StreamId> = argv[3..]
                    .iter()
                    .filter_map(|b| {
                        let t = std::str::from_utf8(b).ok()?;
                        StreamId::parse_explicit(t).or_else(|| StreamId::parse(t))
                    })
                    .collect();
                let _ = s.xack(&argv[2], &ids);
            }
            Ok(())
        }
        "XREADGROUP" => {
            // Best-effort live AOF apply: XREADGROUP GROUP g c [COUNT n] STREAMS key id
            // Only the `>` form mutates PEL / last_delivered.
            let mut i = 1;
            let mut group = None;
            let mut consumer = None;
            let mut count: Option<usize> = None;
            while i < argv.len() {
                let t = String::from_utf8_lossy(&argv[i]).to_uppercase();
                match t.as_str() {
                    "GROUP" => {
                        if i + 2 < argv.len() {
                            group = Some(argv[i + 1].clone());
                            consumer = Some(argv[i + 2].clone());
                        }
                        i += 3;
                    }
                    "COUNT" => {
                        if i + 1 < argv.len() {
                            count = std::str::from_utf8(&argv[i + 1])
                                .ok()
                                .and_then(|s| s.parse().ok());
                        }
                        i += 2;
                    }
                    "BLOCK" => i += 2,
                    "NOACK" => i += 1,
                    "STREAMS" => {
                        i += 1;
                        break;
                    }
                    _ => i += 1,
                }
            }
            let (Some(group), Some(consumer)) = (group, consumer) else {
                return Ok(());
            };
            // Remaining: keys... ids... (equal split)
            let rest = &argv[i..];
            if rest.len() < 2 || rest.len() % 2 != 0 {
                return Ok(());
            }
            let half = rest.len() / 2;
            for k in 0..half {
                let key = &rest[k];
                let id_spec = std::str::from_utf8(&rest[half + k]).unwrap_or(">");
                if id_spec != ">" {
                    continue; // history reads don't need AOF apply
                }
                if let Some(stream) = cache.get_stream(key) {
                    let mut s = stream
                        .write();
                    let _ = s.xreadgroup(&group, &consumer, ">", count);
                }
            }
            Ok(())
        }
        "FLUSHDB" => {
            cache.flush();
            Ok(())
        }
        // FLUSHALL handled by multi-DB loader
        "FLUSHALL" => {
            cache.flush();
            Ok(())
        }
        // Skip unknown / non-mutating (including SELECT at this layer)
        _ => Ok(()),
    }
}

/// Map FT mutator string errors to typed `Error` (OOM-ish → OutOfMemory).
///
/// Shared by AOF apply and RDB `load_into` so OOM strings from the search
/// layer surface as [`Error::OutOfMemory`] consistently.
///
/// **Call with the raw search-layer message** (e.g. `"OOM: …"`). Do not
/// pre-prefix with `"RDB FT.CREATE: "` — that breaks the OOM prefix match.
pub(crate) fn map_ft_mutator_error(msg: String) -> Error {
    // Search layer emits `"OOM: …"` (see account_search_index_write); match that
    // prefix rather than any substring containing the letters OOM.
    if msg.starts_with("OOM:") || msg.starts_with("OOM ") || msg == "OOM" {
        Error::OutOfMemory
    } else {
        Error::InvalidArgument(msg)
    }
}

/// Map an FT mutator error, then prefix non-OOM messages for RDB context.
///
/// Ensures `"OOM: …"` still becomes [`Error::OutOfMemory`] while other
/// messages become `InvalidArgument("RDB FT.CREATE: …")` etc.
pub(crate) fn map_rdb_ft_mutator_error(context: &str, msg: String) -> Error {
    match map_ft_mutator_error(msg) {
        Error::OutOfMemory => Error::OutOfMemory,
        Error::InvalidArgument(m) => Error::InvalidArgument(format!("{context}: {m}")),
        other => other,
    }
}

#[cfg(test)]
mod ft_error_map_tests {
    use super::*;

    #[test]
    fn map_ft_mutator_error_oom_prefix() {
        assert!(matches!(
            map_ft_mutator_error("OOM: cannot allocate search index memory".into()),
            Error::OutOfMemory
        ));
        assert!(matches!(map_ft_mutator_error("OOM".into()), Error::OutOfMemory));
        assert!(matches!(
            map_ft_mutator_error("OOM overflow".into()),
            Error::OutOfMemory
        ));
    }

    #[test]
    fn map_ft_mutator_error_non_oom() {
        match map_ft_mutator_error("Index 'x' already exists".into()) {
            Error::InvalidArgument(m) => assert!(m.contains("already exists")),
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn map_rdb_ft_maps_oom_before_prefix() {
        // Pre-prefixing would break starts_with("OOM:") — this helper must not.
        assert!(matches!(
            map_rdb_ft_mutator_error("RDB FT.CREATE", "OOM: cannot allocate".into()),
            Error::OutOfMemory
        ));
        match map_rdb_ft_mutator_error("RDB FT.CREATE", "already exists".into()) {
            Error::InvalidArgument(m) => {
                assert!(m.starts_with("RDB FT.CREATE:"), "{m}");
                assert!(m.contains("already exists"));
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
        assert!(matches!(
            map_rdb_ft_mutator_error("RDB FT.ALIASADD", "OOM: x".into()),
            Error::OutOfMemory
        ));
    }

    #[test]
    fn pre_prefixed_oom_would_not_match_raw_helper() {
        // Documents the CH post-ship bug: never call map_ft_mutator_error on
        // already-prefixed strings.
        match map_ft_mutator_error("RDB FT.CREATE: OOM: cannot allocate".into()) {
            Error::InvalidArgument(m) => assert!(m.starts_with("RDB FT.CREATE:")),
            other => panic!("pre-prefixed OOM must NOT map as OutOfMemory, got {other:?}"),
        }
    }
}

/// Replay AOF into a single cache. SELECT is ignored (all commands hit this cache).
///
/// **Scratch-load (transactional):** commands are applied to an empty scratch
/// keyspace (independent stats). On `Ok`, autosweep is paused, the target is
/// flushed (AOF is a full snapshot replace), then scratch is swapped in via
/// [`Cache::replace_keyspace_from`]. On `Err`, `cache` is left completely
/// untouched (scratch is dropped). Requires exclusive access to `cache` for
/// the commit swap (no concurrent client traffic during load).
pub fn load_into_cache(cache: &Arc<Cache>, path: &Path) -> Result<usize> {
    let scratch = cache.empty_keyspace_like();
    let result = load_file_with(path, |argv| {
        let cmd = String::from_utf8_lossy(&argv[0]).to_uppercase();
        if cmd == "SELECT" {
            return Ok(());
        }
        apply_command_to_cache(&scratch, &argv)
    });
    match result {
        Ok(n) => {
            cache.with_autosweep_paused(|| {
                // Dirty WATCH before flush so no clean window while keyspace is empty.
                cache.touch_all_watch_keys();
                // Full replace: free target early to cut dual-residency peak.
                cache.flush_all_including_search();
                cache.replace_keyspace_from(&scratch);
            });
            Ok(n)
        }
        Err(e) => Err(e),
    }
}

/// Replay AOF into multi-DB keyspaces. Handles SELECT and FLUSHALL across DBs.
///
/// **Scratch-load (transactional):** replay targets an empty multi-DB scratch
/// collection. On `Ok`, autosweep is paused on all DBs, WATCH gens are
/// bumped, then each DB is swapped via [`Databases::replace_keyspaces_from`]
/// (lock-step multi-DB install under the keyspace epoch write lock; no multi-DB
/// pre-flush; panic mid-install rolls back already-installed DBs — Batch DS).
/// Peak dual-residency during stage is ~old multi-DB + scratch. On `Err`,
/// `databases` is left completely untouched. Multi-DB exporters take the epoch
/// read lock; command path sees `-LOADING` during commit.
pub fn load_into_databases(databases: &Databases, path: &Path) -> Result<usize> {
    load_into_databases_with_functions(databases, path, None)
}

pub fn load_into_databases_with_functions(
    databases: &Databases,
    path: &Path,
    function_libs: Option<&FunctionLibraryStore>,
) -> Result<usize> {
    let scratch = databases.empty_like();
    let mut current = 0usize;
    let result = load_file_with(path, |argv| {
        let cmd = String::from_utf8_lossy(&argv[0]).to_uppercase();
        match cmd.as_str() {
            "SELECT" => {
                if argv.len() >= 2 {
                    if let Ok(s) = std::str::from_utf8(&argv[1]) {
                        if let Ok(idx) = s.parse::<usize>() {
                            if scratch.get(idx).is_some() {
                                current = idx;
                            }
                        }
                    }
                }
                Ok(())
            }
            "FLUSHALL" => {
                // Live FLUSHALL during AOF replay: keys/docs only, keep schema
                // if any had been created earlier in the file (matches runtime).
                scratch.flush_all();
                Ok(())
            }
            // Batch GY: Redis Functions are server-wide (not per-DB).
            "FUNCTION" => {
                if let Some(libs) = function_libs {
                    apply_function_command(libs, &argv)?;
                }
                Ok(())
            }
            _ => {
                let Some(cache) = scratch.get(current) else {
                    return Ok(());
                };
                apply_command_to_cache(&cache, &argv)
            }
        }
    });
    match result {
        Ok(n) => {
            databases.with_autosweep_paused_all(|| {
                // Dirty WATCH before replace. No multi-DB pre-flush: each DB is
                // fully swapped by replace_keyspaces_from (panic mid-install
                // rolls back already-installed DBs via retained discards).
                for db in databases.iter() {
                    db.touch_all_watch_keys();
                }
                databases.replace_keyspaces_from(&scratch);
            });
            Ok(n)
        }
        Err(e) => Err(e),
    }
}
