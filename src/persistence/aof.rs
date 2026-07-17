//! Append-Only File (AOF) persistence.
//!
//! Format: stream of RESP arrays (Redis-compatible command log).
//! Rewrite materializes the current DB as SET / ZADD / GEOADD / HSET / RPUSH /
//! SADD / XADD / XGROUP CREATE commands, with SELECT between logical databases.

use crate::cache::Cache;
use crate::databases::Databases;
use crate::error::{Error, Result};
use crate::persistence::rdb::DbSnapshot;
use crate::protocol::RespValue;
use crate::stream_type::StreamId;
use bytes::Bytes;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Serialize a command (argv) as a RESP array.
pub fn encode_command(args: &[Bytes]) -> Bytes {
    let arr: Vec<RespValue> = args
        .iter()
        .map(|a| RespValue::BulkString(Some(a.clone())))
        .collect();
    RespValue::Array(arr).serialize()
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
    let snap = DbSnapshot::from_cache(cache)?;
    let mut buf = Vec::with_capacity(4096);
    encode_snapshot_commands(&snap, &mut buf);
    write_aof_buffer(&buf, path)
}

/// Rewrite AOF from all non-empty logical databases, emitting SELECT between them.
pub fn rewrite_databases(databases: &Databases, path: &Path) -> Result<()> {
    let mut non_empty: Vec<(usize, DbSnapshot)> = Vec::new();
    for (idx, cache) in databases.iter().enumerate() {
        let snap = DbSnapshot::from_cache(cache)?;
        if !snap.is_empty() {
            non_empty.push((idx, snap));
        }
    }

    let mut buf = Vec::with_capacity(4096);
    let multi = non_empty.len() > 1;
    for (idx, snap) in &non_empty {
        // Emit SELECT for every DB when multiple are non-empty, and for any
        // non-zero single DB so load restores the correct keyspace.
        if multi || *idx != 0 {
            let args = vec![
                Bytes::from_static(b"SELECT"),
                Bytes::from(idx.to_string()),
            ];
            buf.extend_from_slice(&encode_command(&args));
        }
        encode_snapshot_commands(snap, &mut buf);
    }

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

/// Apply a single AOF write command against one cache.
pub fn apply_command_to_cache(cache: &Cache, argv: &[Bytes]) -> Result<()> {
    use crate::entry::StoreOptions;

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
            let hash = cache.get_or_create_hash(&argv[1])?;
            let mut h = hash
                .write();
            let mut i = 2;
            while i + 1 < argv.len() {
                h.hset(argv[i].clone(), argv[i + 1].clone());
                i += 2;
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
                        let _ = s.group_setid(&argv[3], id);
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
                let _ = s.xsetid(id);
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

/// Replay AOF into a single cache. SELECT is ignored (all commands hit this cache).
pub fn load_into_cache(cache: &Arc<Cache>, path: &Path) -> Result<usize> {
    load_file_with(path, |argv| {
        let cmd = String::from_utf8_lossy(&argv[0]).to_uppercase();
        if cmd == "SELECT" {
            return Ok(());
        }
        apply_command_to_cache(cache, &argv)
    })
}

/// Replay AOF into multi-DB keyspaces. Handles SELECT and FLUSHALL across DBs.
pub fn load_into_databases(databases: &Databases, path: &Path) -> Result<usize> {
    let mut current = 0usize;
    load_file_with(path, |argv| {
        let cmd = String::from_utf8_lossy(&argv[0]).to_uppercase();
        match cmd.as_str() {
            "SELECT" => {
                if argv.len() >= 2 {
                    if let Ok(s) = std::str::from_utf8(&argv[1]) {
                        if let Ok(idx) = s.parse::<usize>() {
                            if databases.get(idx).is_some() {
                                current = idx;
                            }
                        }
                    }
                }
                Ok(())
            }
            "FLUSHALL" => {
                databases.flush_all();
                Ok(())
            }
            _ => {
                let Some(cache) = databases.get(current) else {
                    return Ok(());
                };
                apply_command_to_cache(&cache, &argv)
            }
        }
    })
}
