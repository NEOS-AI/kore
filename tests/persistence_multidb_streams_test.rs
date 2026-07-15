//! Multi-DB RDB/AOF persistence and stream snapshot round-trips.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::databases::Databases;
use kore::persistence::{aof, rdb, PersistenceConfig, PersistenceManager};
use kore::protocol::RespValue;
use kore::Cache;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn tmp_dir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "kore-persist-mdb-{}-{}",
        name,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn make_config(dir: &PathBuf) -> Arc<Config> {
    Arc::new(Config {
        host: "127.0.0.1".to_string(),
        port: 6379,
        threads: 1,
        shards: 8,
        maxmemory: 1024 * 1024 * 50,
        evict: true,
        autosweep: false,
        loadfactor: 0.75,
        maxconns: 10,
        auth: String::new(),
        maxentrysize: 500 * 1024 * 1024,
        verbosity: 0,
        enable_redlock: false,
        redlock_instances: String::new(),
        redlock_retry_count: 3,
        redlock_retry_delay_ms: 200,
        dir: dir.to_string_lossy().to_string(),
        dbfilename: "dump.rdb".to_string(),
        appendonly: false,
        appendfilename: "appendonly.aof".to_string(),
        replicaof: String::new(),
        save: "".to_string(),
        maxmemory_policy: "allkeys-lru".to_string(),
        databases: 16,
        metrics_port: 0,
        tls: false,
        tls_cert: String::new(),
        tls_key: String::new(),
        cluster_enabled: false,
})
}

fn make_databases() -> Arc<Databases> {
    Databases::create(16, 8, 1024 * 1024 * 50, 500 * 1024 * 1024, false, 0.75)
}

fn make_handler(databases: Arc<Databases>, dir: &PathBuf) -> CommandHandler {
    CommandHandler::with_databases(databases, make_config(dir), None)
}

fn make_handler_persist(
    databases: Arc<Databases>,
    dir: &PathBuf,
    mgr: Arc<PersistenceManager>,
) -> CommandHandler {
    CommandHandler::with_databases(databases, make_config(dir), Some(mgr))
}

fn bulk(s: &str) -> RespValue {
    RespValue::BulkString(Some(Bytes::from(s.to_string())))
}

fn cmd(parts: &[&str]) -> RespValue {
    RespValue::Array(parts.iter().map(|p| bulk(p)).collect())
}

fn handle(h: &mut CommandHandler, value: RespValue) -> RespValue {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { h.handle(value).await.unwrap() })
}

fn as_bulk_str(v: &RespValue) -> Option<String> {
    match v {
        RespValue::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    }
}

fn xrange_field_values(range: &RespValue) -> Vec<String> {
    match range {
        RespValue::Array(entries) => entries
            .iter()
            .filter_map(|e| match e {
                RespValue::Array(parts) if parts.len() >= 2 => match &parts[1] {
                    RespValue::Array(fields) if !fields.is_empty() => as_bulk_str(&fields[1]),
                    _ => None,
                },
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

// ── 1. RDB multi-DB ──────────────────────────────────────────────────────────

#[test]
fn rdb_roundtrip_multi_db() {
    let dir = tmp_dir("rdb-mdb");
    let path = dir.join("dump.rdb");
    let databases = make_databases();
    let mut h = make_handler(databases.clone(), &dir);

    assert_eq!(handle(&mut h, cmd(&["SET", "k0", "v0"])), RespValue::ok());
    assert_eq!(handle(&mut h, cmd(&["SELECT", "1"])), RespValue::ok());
    assert_eq!(handle(&mut h, cmd(&["SET", "k1", "v1"])), RespValue::ok());

    rdb::save_databases(&databases, &path).unwrap();
    assert!(path.exists());

    let loaded = make_databases();
    let n = rdb::load_databases(&loaded, &path, true).unwrap();
    assert!(n >= 2, "loaded keys: {n}");

    let mut h2 = make_handler(loaded, &dir);
    assert_eq!(
        as_bulk_str(&handle(&mut h2, cmd(&["GET", "k0"]))).as_deref(),
        Some("v0")
    );
    assert_eq!(handle(&mut h2, cmd(&["SELECT", "1"])), RespValue::ok());
    assert_eq!(
        as_bulk_str(&handle(&mut h2, cmd(&["GET", "k1"]))).as_deref(),
        Some("v1")
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// ── 2. RDB stream entries ────────────────────────────────────────────────────

#[test]
fn rdb_roundtrip_stream_entries() {
    let dir = tmp_dir("rdb-stream");
    let path = dir.join("dump.rdb");
    let databases = make_databases();
    let mut h = make_handler(databases.clone(), &dir);

    let id1 = as_bulk_str(&handle(
        &mut h,
        cmd(&["XADD", "mystream", "*", "f", "a"]),
    ))
    .expect("id1");
    let id2 = as_bulk_str(&handle(
        &mut h,
        cmd(&["XADD", "mystream", "*", "f", "b"]),
    ))
    .expect("id2");
    let id3 = as_bulk_str(&handle(
        &mut h,
        cmd(&["XADD", "mystream", "*", "f", "c"]),
    ))
    .expect("id3");
    assert!(id1 < id2 && id2 < id3);

    rdb::save_databases(&databases, &path).unwrap();

    let loaded = make_databases();
    rdb::load_databases(&loaded, &path, true).unwrap();
    let mut h2 = make_handler(loaded, &dir);

    let range = handle(&mut h2, cmd(&["XRANGE", "mystream", "-", "+"]));
    let values = xrange_field_values(&range);
    assert_eq!(values, vec!["a", "b", "c"]);

    // Auto-ID must continue past last_generated_id
    let id4 = as_bulk_str(&handle(
        &mut h2,
        cmd(&["XADD", "mystream", "*", "f", "d"]),
    ))
    .expect("id4");
    assert!(
        id4 > id3,
        "new auto-id {id4} should be > last saved id {id3}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// ── 3. RDB stream consumer group + PEL ───────────────────────────────────────

#[test]
fn rdb_roundtrip_stream_consumer_group() {
    let dir = tmp_dir("rdb-cg");
    let path = dir.join("dump.rdb");
    let databases = make_databases();
    let mut h = make_handler(databases.clone(), &dir);

    handle(
        &mut h,
        cmd(&["XADD", "s", "1-0", "f", "v1"]),
    );
    handle(
        &mut h,
        cmd(&["XADD", "s", "1-1", "f", "v2"]),
    );
    assert_eq!(
        handle(&mut h, cmd(&["XGROUP", "CREATE", "s", "g", "0"])),
        RespValue::ok()
    );

    // Deliver both entries to consumer c1 → PEL should have 2 pending
    let read = handle(
        &mut h,
        cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c1",
            "COUNT",
            "10",
            "STREAMS",
            "s",
            ">",
        ]),
    );
    match &read {
        RespValue::Array(arr) => assert!(!arr.is_empty(), "expected delivered messages"),
        other => panic!("unexpected XREADGROUP reply: {other:?}"),
    }

    rdb::save_databases(&databases, &path).unwrap();

    let loaded = make_databases();
    rdb::load_databases(&loaded, &path, true).unwrap();
    let mut h2 = make_handler(loaded, &dir);

    // XPENDING summary: total pending should still be 2
    let pending = handle(&mut h2, cmd(&["XPENDING", "s", "g"]));
    match pending {
        RespValue::Array(parts) => {
            assert!(
                parts.len() >= 1,
                "XPENDING summary should have fields, got {parts:?}"
            );
            match &parts[0] {
                RespValue::Integer(n) => assert_eq!(*n, 2, "PEL total"),
                other => panic!("expected pending total integer, got {other:?}"),
            }
        }
        other => panic!("expected XPENDING array, got {other:?}"),
    }

    // History read for c1 should still see pending messages
    let hist = handle(
        &mut h2,
        cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c1",
            "COUNT",
            "10",
            "STREAMS",
            "s",
            "0",
        ]),
    );
    match hist {
        RespValue::Array(arr) => assert!(!arr.is_empty(), "history read should return pending"),
        other => panic!("unexpected history read: {other:?}"),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

// ── 4. AOF rewrite multi-DB with SELECT ──────────────────────────────────────

#[test]
fn aof_rewrite_multi_db_with_select() {
    let dir = tmp_dir("aof-mdb");
    let path = dir.join("appendonly.aof");
    let databases = make_databases();
    let mut h = make_handler(databases.clone(), &dir);

    handle(&mut h, cmd(&["SET", "a", "db0"]));
    handle(&mut h, cmd(&["SELECT", "1"]));
    handle(&mut h, cmd(&["SET", "a", "db1"]));

    aof::rewrite_databases(&databases, &path).unwrap();
    assert!(path.exists());

    // Rewritten AOF should contain SELECT directives
    let text = String::from_utf8_lossy(&std::fs::read(&path).unwrap()).into_owned();
    assert!(
        text.contains("SELECT") || text.contains("select"),
        "AOF rewrite should emit SELECT; got:\n{text}"
    );

    let loaded = make_databases();
    let n = aof::load_into_databases(&loaded, &path).unwrap();
    assert!(n >= 2, "replayed commands: {n}");

    let mut h2 = make_handler(loaded, &dir);
    assert_eq!(
        as_bulk_str(&handle(&mut h2, cmd(&["GET", "a"]))).as_deref(),
        Some("db0")
    );
    handle(&mut h2, cmd(&["SELECT", "1"]));
    assert_eq!(
        as_bulk_str(&handle(&mut h2, cmd(&["GET", "a"]))).as_deref(),
        Some("db1")
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// ── 5. AOF rewrite streams ───────────────────────────────────────────────────

#[test]
fn aof_rewrite_streams() {
    let dir = tmp_dir("aof-stream");
    let path = dir.join("appendonly.aof");
    let databases = make_databases();
    let mut h = make_handler(databases.clone(), &dir);

    handle(
        &mut h,
        cmd(&["XADD", "mystream", "10-0", "name", "Alice"]),
    );
    handle(
        &mut h,
        cmd(&["XADD", "mystream", "*", "name", "Bob"]),
    );
    handle(
        &mut h,
        cmd(&["XGROUP", "CREATE", "mystream", "mygroup", "0"]),
    );

    aof::rewrite_databases(&databases, &path).unwrap();

    let text = String::from_utf8_lossy(&std::fs::read(&path).unwrap()).into_owned();
    assert!(
        text.contains("XADD") || text.contains("xadd"),
        "AOF rewrite should emit XADD; got:\n{text}"
    );

    let loaded = make_databases();
    aof::load_into_databases(&loaded, &path).unwrap();
    let mut h2 = make_handler(loaded, &dir);

    assert_eq!(
        handle(&mut h2, cmd(&["XLEN", "mystream"])),
        RespValue::Integer(2)
    );
    let range = handle(&mut h2, cmd(&["XRANGE", "mystream", "-", "+"]));
    let values = xrange_field_values(&range);
    assert_eq!(values.len(), 2);
    assert_eq!(values[0], "Alice");
    assert_eq!(values[1], "Bob");

    // Group should exist (XGROUP CREATE replayed)
    let pending = handle(&mut h2, cmd(&["XPENDING", "mystream", "mygroup"]));
    match pending {
        RespValue::Array(parts) => match &parts[0] {
            RespValue::Integer(0) => {}
            other => panic!("expected empty PEL after rewrite, got {other:?}"),
        },
        RespValue::Error(e) => panic!("group missing after AOF load: {}", String::from_utf8_lossy(&e)),
        other => panic!("unexpected XPENDING: {other:?}"),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

// ── 5b. AOF rewrite preserves consumer-group PEL ─────────────────────────────

#[test]
fn aof_rewrite_preserves_stream_pel() {
    let dir = tmp_dir("aof-pel");
    let path = dir.join("appendonly.aof");
    let databases = make_databases();
    let mut h = make_handler(databases.clone(), &dir);

    handle(&mut h, cmd(&["XADD", "s", "1-0", "f", "v1"]));
    handle(&mut h, cmd(&["XADD", "s", "1-1", "f", "v2"]));
    assert_eq!(
        handle(&mut h, cmd(&["XGROUP", "CREATE", "s", "g", "0"])),
        RespValue::ok()
    );
    let read = handle(
        &mut h,
        cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c1",
            "COUNT",
            "10",
            "STREAMS",
            "s",
            ">",
        ]),
    );
    assert!(matches!(read, RespValue::Array(_)));

    aof::rewrite_databases(&databases, &path).unwrap();
    let text = String::from_utf8_lossy(&std::fs::read(&path).unwrap()).into_owned();
    assert!(
        text.contains("XCLAIM") || text.contains("xclaim"),
        "rewrite should emit XCLAIM FORCE for PEL; got:\n{text}"
    );

    let loaded = make_databases();
    aof::load_into_databases(&loaded, &path).unwrap();
    let mut h2 = make_handler(loaded, &dir);

    let pending = handle(&mut h2, cmd(&["XPENDING", "s", "g"]));
    match pending {
        RespValue::Array(parts) => match &parts[0] {
            RespValue::Integer(n) => assert_eq!(*n, 2, "PEL must survive AOF rewrite"),
            other => panic!("expected pending total, got {other:?}"),
        },
        other => panic!("XPENDING: {other:?}"),
    }

    // History read still returns pending for c1
    let hist = handle(
        &mut h2,
        cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c1",
            "COUNT",
            "10",
            "STREAMS",
            "s",
            "0",
        ]),
    );
    assert!(
        matches!(hist, RespValue::Array(ref a) if !a.is_empty()),
        "history read should return pending after AOF rewrite"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// ── 6. Live AOF SELECT then write ────────────────────────────────────────────

#[test]
fn aof_live_select_then_write_replay() {
    let dir = tmp_dir("aof-live-select");
    let aof_path = dir.join("appendonly.aof");
    let pconfig = PersistenceConfig {
        dir: dir.clone(),
        dbfilename: "dump.rdb".to_string(),
        appendonly: true,
        appendfilename: "appendonly.aof".to_string(),
        save_rules: vec![],
    };
    let mgr = PersistenceManager::new(pconfig).unwrap();
    mgr.ensure_dir().unwrap();

    let databases = make_databases();
    let mut h = make_handler_persist(databases.clone(), &dir, mgr.clone());

    assert_eq!(handle(&mut h, cmd(&["SET", "k", "on0"])), RespValue::ok());
    assert_eq!(handle(&mut h, cmd(&["SELECT", "1"])), RespValue::ok());
    assert_eq!(handle(&mut h, cmd(&["SET", "k", "on1"])), RespValue::ok());

    assert!(aof_path.exists());
    let raw = std::fs::read(&aof_path).unwrap();
    let text = String::from_utf8_lossy(&raw).into_owned();
    assert!(
        text.contains("SELECT") || text.contains("select"),
        "live AOF should log SELECT before DB1 write; got:\n{text}"
    );

    // Replay into fresh databases
    let loaded = make_databases();
    aof::load_into_databases(&loaded, &aof_path).unwrap();
    let mut h2 = make_handler(loaded, &dir);
    assert_eq!(
        as_bulk_str(&handle(&mut h2, cmd(&["GET", "k"]))).as_deref(),
        Some("on0")
    );
    handle(&mut h2, cmd(&["SELECT", "1"]));
    assert_eq!(
        as_bulk_str(&handle(&mut h2, cmd(&["GET", "k"]))).as_deref(),
        Some("on1")
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// ── Single-cache stream snapshot (replication path) ──────────────────────────

#[test]
fn rdb_single_cache_stream_roundtrip() {
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    {
        let s = cache
            .get_or_create_stream(&Bytes::from("st"))
            .unwrap();
        let mut st = s.write().unwrap();
        st.xadd("5-0", vec![(Bytes::from("x"), Bytes::from("y"))])
            .unwrap();
    }
    let bytes = rdb::save_to_bytes(&cache).unwrap();
    assert!(bytes.starts_with(b"KORDB\0"));

    let cache2 = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    rdb::load_bytes(&cache2, &bytes, true).unwrap();
    let s = cache2.get_stream(&Bytes::from("st")).expect("stream");
    let st = s.read().unwrap();
    assert_eq!(st.len(), 1);
    assert_eq!(st.last_generated_id().to_string_id(), "5-0");
}
