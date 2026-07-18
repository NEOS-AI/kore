//! Batch BY: RDB v5 persists FT index definitions + aliases; load auto-indexes hashes.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::databases::Databases;
use kore::persistence::rdb;
use kore::protocol::RespValue;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn tmp_dir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "kore-by-{}-{}",
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
        enable_fair_queue: false,
        fair_queue_max_size: 1024,
        fair_queue_cleanup_ms: 500,
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
        aclfile: String::new(),
        cluster_enabled: false,
        unixsocket: String::new(),
            log_format: "text".to_string(),
    })
}

fn make_databases() -> Arc<Databases> {
    Databases::create(16, 8, 1024 * 1024 * 50, 500 * 1024 * 1024, false, 0.75)
}

fn make_handler(databases: Arc<Databases>, dir: &PathBuf) -> CommandHandler {
    CommandHandler::with_databases(databases, make_config(dir), None)
}

fn bulk(s: &str) -> RespValue {
    RespValue::BulkString(Some(Bytes::from(s.to_string())))
}

fn cmd(parts: &[&str]) -> RespValue {
    RespValue::Array(parts.iter().map(|p| bulk(p)).collect())
}

fn handle(handler: &mut CommandHandler, value: RespValue) -> RespValue {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async { handler.handle(value).await.unwrap() })
}

fn search_total(resp: &RespValue) -> i64 {
    match resp {
        RespValue::Array(arr) => match arr.first() {
            Some(RespValue::Integer(n)) => *n,
            _ => panic!("FT.SEARCH reply missing total integer: {:?}", resp),
        },
        RespValue::Error(e) => panic!("FT.SEARCH error: {}", String::from_utf8_lossy(e)),
        other => panic!("expected FT.SEARCH array, got {:?}", other),
    }
}

fn search_ids(resp: &RespValue) -> Vec<String> {
    match resp {
        RespValue::Array(arr) => {
            let mut ids = Vec::new();
            let mut i = 1;
            while i < arr.len() {
                if let RespValue::BulkString(Some(id)) = &arr[i] {
                    ids.push(String::from_utf8_lossy(id).into_owned());
                }
                i += 2;
            }
            ids
        }
        other => panic!("expected FT.SEARCH array, got {:?}", other),
    }
}

/// FT.CREATE + HSET + ALIASADD → RDB save → load → search by name and alias.
#[test]
fn rdb_roundtrip_ft_schema_docs_and_alias() {
    let dir = tmp_dir("ft-rdb");
    let path = dir.join("dump.rdb");
    let databases = make_databases();
    let mut h = make_handler(databases.clone(), &dir);

    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "FT.CREATE",
                "articles",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "doc:",
                "SCHEMA",
                "title",
                "TEXT",
                "tags",
                "TAG",
                "SEPARATOR",
                "|",
            ]),
        ),
        RespValue::ok()
    );

    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "HSET",
                "doc:1",
                "title",
                "hello world",
                "tags",
                "rust|redis",
            ]),
        ),
        RespValue::Integer(2)
    );
    assert_eq!(
        handle(
            &mut h,
            cmd(&["HSET", "doc:2", "title", "other note", "tags", "go"]),
        ),
        RespValue::Integer(2)
    );

    assert_eq!(
        handle(&mut h, cmd(&["FT.ALIASADD", "blog", "articles"])),
        RespValue::ok()
    );

    let pre = handle(&mut h, cmd(&["FT.SEARCH", "blog", "hello"]));
    assert_eq!(search_total(&pre), 1);
    assert_eq!(search_ids(&pre), vec!["doc:1".to_string()]);

    rdb::save_databases(&databases, &path).unwrap();
    assert!(path.exists());

    // Version 5 file
    let raw = std::fs::read(&path).unwrap();
    assert!(raw.starts_with(b"KORDB\0"));
    let version = u32::from_le_bytes(raw[6..10].try_into().unwrap());
    assert_eq!(version, 5, "RDB must write version 5 for search section");

    let loaded = make_databases();
    let n = rdb::load_databases(&loaded, &path, true).unwrap();
    assert!(n >= 2, "expected at least 2 hash keys loaded, got {n}");

    let mut h2 = make_handler(loaded, &dir);

    let by_name = handle(&mut h2, cmd(&["FT.SEARCH", "articles", "hello"]));
    assert_eq!(search_total(&by_name), 1);
    assert_eq!(search_ids(&by_name), vec!["doc:1".to_string()]);

    let by_alias = handle(&mut h2, cmd(&["FT.SEARCH", "blog", "hello"]));
    assert_eq!(search_total(&by_alias), 1);
    assert_eq!(search_ids(&by_alias), vec!["doc:1".to_string()]);

    match handle(&mut h2, cmd(&["FT.INFO", "blog"])) {
        RespValue::Array(parts) => assert!(!parts.is_empty(), "FT.INFO should return fields"),
        RespValue::Error(e) => panic!("FT.INFO via alias failed: {}", String::from_utf8_lossy(&e)),
        other => panic!("unexpected FT.INFO: {other:?}"),
    }

    match handle(&mut h2, cmd(&["FT.TAGVALS", "articles", "tags"])) {
        RespValue::Array(a) => {
            let mut vals: Vec<String> = a
                .iter()
                .filter_map(|v| match v {
                    RespValue::BulkString(Some(b)) => {
                        Some(String::from_utf8_lossy(b).into_owned())
                    }
                    _ => None,
                })
                .collect();
            vals.sort();
            assert_eq!(
                vals,
                vec!["go".to_string(), "redis".to_string(), "rust".to_string()],
                "TAG SEPARATOR must survive RDB so values split correctly"
            );
        }
        RespValue::Error(e) => {
            panic!("FT.TAGVALS failed: {}", String::from_utf8_lossy(&e))
        }
        other => panic!("expected TAGVALS array, got {:?}", other),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Index-only DB (schema, no keys) survives RDB round-trip.
#[test]
fn rdb_roundtrip_index_only() {
    let dir = tmp_dir("ft-only");
    let path = dir.join("dump.rdb");
    let databases = make_databases();
    let mut h = make_handler(databases.clone(), &dir);

    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "FT.CREATE",
                "empty_idx",
                "PREFIX",
                "1",
                "x:",
                "SCHEMA",
                "body",
                "TEXT",
            ]),
        ),
        RespValue::ok()
    );

    rdb::save_databases(&databases, &path).unwrap();
    assert!(path.exists(), "index-only DB must produce an RDB file");

    let loaded = make_databases();
    rdb::load_databases(&loaded, &path, true).unwrap();

    let cache = loaded.get(0).unwrap();
    let indices = cache.list_search_indices();
    assert_eq!(indices, vec!["empty_idx".to_string()]);

    let mut h2 = make_handler(loaded, &dir);

    // FT.INFO works on restored empty index
    match handle(&mut h2, cmd(&["FT.INFO", "empty_idx"])) {
        RespValue::Array(parts) => assert!(!parts.is_empty()),
        RespValue::Error(e) => panic!("FT.INFO failed: {}", String::from_utf8_lossy(&e)),
        other => panic!("unexpected FT.INFO: {other:?}"),
    }

    // New HSET after load auto-indexes against restored schema
    assert_eq!(
        handle(
            &mut h2,
            cmd(&["HSET", "x:1", "body", "post rdb doc"]),
        ),
        RespValue::Integer(1)
    );
    let resp = handle(&mut h2, cmd(&["FT.SEARCH", "empty_idx", "rdb"]));
    assert_eq!(search_total(&resp), 1);

    let _ = std::fs::remove_dir_all(&dir);
}

/// Hand-built RDB v4 (no search section) still loads cleanly.
#[test]
fn rdb_v4_without_search_section_still_loads() {
    // Minimal v4 multi-DB file: one string key in DB 0, empty typed-expires, no search.
    fn write_bytes(buf: &mut Vec<u8>, data: &[u8]) {
        let len = data.len() as u32;
        buf.extend_from_slice(&len.to_le_bytes());
        buf.extend_from_slice(data);
    }
    fn write_u32(buf: &mut Vec<u8>, v: u32) {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    fn write_u64(buf: &mut Vec<u8>, v: u64) {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    fn write_i64(buf: &mut Vec<u8>, v: i64) {
        buf.extend_from_slice(&v.to_le_bytes());
    }

    let mut buf = Vec::new();
    buf.extend_from_slice(b"KORDB\0");
    write_u32(&mut buf, 4); // version 4
    write_u64(&mut buf, 1); // 1 database
    write_u32(&mut buf, 0); // db index 0

    // strings
    write_u64(&mut buf, 1);
    write_bytes(&mut buf, b"hello");
    write_bytes(&mut buf, b"world");
    write_u32(&mut buf, 0); // flags
    write_i64(&mut buf, -1); // no expire

    // zsets, geos, hashes, lists, sets, streams — all empty
    for _ in 0..6 {
        write_u64(&mut buf, 0);
    }
    // typed_expires empty
    write_u64(&mut buf, 0);

    buf.push(0xFF); // footer

    let loaded = make_databases();
    let n = rdb::load_databases_bytes(&loaded, &buf, true).unwrap();
    assert_eq!(n, 1);

    let cache = loaded.get(0).unwrap();
    let e = cache
        .load(&Bytes::from("hello"), Default::default())
        .unwrap()
        .unwrap();
    assert_eq!(e.value, Bytes::from("world"));
    assert!(
        cache.list_search_indices().is_empty(),
        "v4 file has no search section"
    );
}

/// Multi-DB: index on DB 1 survives RDB round-trip independently of DB 0.
#[test]
fn rdb_roundtrip_ft_on_secondary_db() {
    let dir = tmp_dir("ft-db1");
    let path = dir.join("dump.rdb");
    let databases = make_databases();
    let mut h = make_handler(databases.clone(), &dir);

    assert_eq!(handle(&mut h, cmd(&["SELECT", "1"])), RespValue::ok());

    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "FT.CREATE",
                "db1idx",
                "PREFIX",
                "1",
                "k:",
                "SCHEMA",
                "body",
                "TEXT",
            ]),
        ),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["HSET", "k:1", "body", "secondary db doc"])),
        RespValue::Integer(1)
    );

    rdb::save_databases(&databases, &path).unwrap();

    let loaded = make_databases();
    rdb::load_databases(&loaded, &path, true).unwrap();

    // DB 0 should not have the index
    assert!(loaded.get(0).unwrap().list_search_indices().is_empty());

    // DB 1 has the index + searchable docs
    let db1 = loaded.get(1).unwrap();
    assert_eq!(db1.list_search_indices(), vec!["db1idx".to_string()]);

    let mut h2 = make_handler(loaded, &dir);
    assert_eq!(handle(&mut h2, cmd(&["SELECT", "1"])), RespValue::ok());
    let resp = handle(&mut h2, cmd(&["FT.SEARCH", "db1idx", "secondary"]));
    assert_eq!(search_total(&resp), 1);
    assert_eq!(search_ids(&resp), vec!["k:1".to_string()]);

    let _ = std::fs::remove_dir_all(&dir);
}

/// NUMERIC + VECTOR field types round-trip through RDB.
#[test]
fn rdb_roundtrip_numeric_and_vector_fields() {
    let dir = tmp_dir("ft-num-vec");
    let path = dir.join("dump.rdb");
    let databases = make_databases();
    let mut h = make_handler(databases.clone(), &dir);

    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "FT.CREATE",
                "mixed",
                "PREFIX",
                "1",
                "item:",
                "SCHEMA",
                "price",
                "NUMERIC",
                "SORTABLE",
                "emb",
                "VECTOR",
                "FLAT",
                "TYPE",
                "FLOAT32",
                "DIM",
                "3",
                "DISTANCE_METRIC",
                "COSINE",
            ]),
        ),
        RespValue::ok()
    );

    rdb::save_databases(&databases, &path).unwrap();

    let loaded = make_databases();
    rdb::load_databases(&loaded, &path, true).unwrap();
    let cache = loaded.get(0).unwrap();
    let def = cache
        .list_search_index_definitions()
        .into_iter()
        .find(|d| d.name == "mixed")
        .expect("mixed index restored");
    assert_eq!(def.fields.len(), 2);
    assert!(matches!(
        def.fields[0].field_type,
        kore::FieldType::Numeric { sortable: true }
    ));
    match &def.fields[1].field_type {
        kore::FieldType::Vector {
            algorithm,
            dimensions,
            distance_metric,
        } => {
            assert!(matches!(algorithm, kore::VectorAlgorithm::Flat));
            assert_eq!(*dimensions, 3);
            assert!(matches!(distance_metric, kore::DistanceMetric::Cosine));
        }
        other => panic!("expected VECTOR field, got {:?}", other),
    }

    let _ = std::fs::remove_dir_all(&dir);
}
