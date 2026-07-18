//! Batch BX: FLUSHDB/FLUSHALL keep FT schema; AOF load failure still full-wipes search.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::databases::Databases;
use kore::persistence::aof;
use kore::protocol::RespValue;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn tmp_dir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "kore-bx-{}-{}",
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

fn argv(parts: &[&str]) -> Vec<Bytes> {
    parts.iter().map(|p| Bytes::from(p.to_string())).collect()
}

/// FLUSHDB drops keys/docs but keeps FT index definitions and aliases.
#[test]
fn flushdb_keeps_ft_schema_and_reindexes() {
    let dir = tmp_dir("flushdb-schema");
    let databases = make_databases();
    let mut h = make_handler(databases.clone(), &dir);

    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "FT.CREATE",
                "articles",
                "PREFIX",
                "1",
                "doc:",
                "SCHEMA",
                "title",
                "TEXT",
            ]),
        ),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["FT.ALIASADD", "blog", "articles"])),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["HSET", "doc:1", "title", "hello world"])),
        RespValue::Integer(1)
    );
    assert_eq!(
        search_total(&handle(&mut h, cmd(&["FT.SEARCH", "articles", "hello"]))),
        1
    );

    assert_eq!(handle(&mut h, cmd(&["FLUSHDB"])), RespValue::ok());

    // Keyspace empty
    assert_eq!(handle(&mut h, cmd(&["DBSIZE"])), RespValue::Integer(0));
    assert!(
        databases
            .get(0)
            .unwrap()
            .get_hash(&Bytes::from_static(b"doc:1"))
            .is_none()
    );

    // Schema + alias remain
    let indices = databases.get(0).unwrap().list_search_indices();
    assert!(
        indices.iter().any(|n| n == "articles"),
        "FLUSHDB must keep FT index definition; got {:?}",
        indices
    );
    let aliases = databases.get(0).unwrap().list_search_aliases();
    assert!(
        aliases.iter().any(|(a, i)| a == "blog" && i == "articles"),
        "FLUSHDB must keep aliases; got {:?}",
        aliases
    );

    // Old docs must not appear in search
    assert_eq!(
        search_total(&handle(&mut h, cmd(&["FT.SEARCH", "articles", "hello"]))),
        0
    );
    assert_eq!(
        search_total(&handle(&mut h, cmd(&["FT.SEARCH", "blog", "hello"]))),
        0
    );

    // FT.INFO still works and reports zero docs
    match handle(&mut h, cmd(&["FT.INFO", "articles"])) {
        RespValue::Array(parts) => assert!(!parts.is_empty()),
        RespValue::Error(e) => panic!("FT.INFO after FLUSHDB: {}", String::from_utf8_lossy(&e)),
        other => panic!("unexpected FT.INFO: {other:?}"),
    }
    let info = databases
        .get(0)
        .unwrap()
        .get_search_index_info("articles")
        .expect("index info");
    assert_eq!(info.num_docs, 0);

    // New HSET under prefix is auto-indexed against surviving schema
    assert_eq!(
        handle(&mut h, cmd(&["HSET", "doc:2", "title", "fresh hello"])),
        RespValue::Integer(1)
    );
    assert_eq!(
        search_total(&handle(&mut h, cmd(&["FT.SEARCH", "articles", "fresh"]))),
        1
    );
    assert_eq!(
        search_total(&handle(&mut h, cmd(&["FT.SEARCH", "blog", "fresh"]))),
        1
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// FLUSHALL multi-DB: clears keys/docs on every DB, keeps FT schema per DB.
#[test]
fn flushall_keeps_ft_schema_multi_db() {
    let dir = tmp_dir("flushall-multi");
    let databases = make_databases();
    let mut h = make_handler(databases.clone(), &dir);

    // DB 0 index + key
    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "FT.CREATE",
                "idx0",
                "PREFIX",
                "1",
                "a:",
                "SCHEMA",
                "t",
                "TEXT",
            ]),
        ),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["HSET", "a:1", "t", "alpha"])),
        RespValue::Integer(1)
    );

    // DB 1 index + key
    assert_eq!(handle(&mut h, cmd(&["SELECT", "1"])), RespValue::ok());
    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "FT.CREATE",
                "idx1",
                "PREFIX",
                "1",
                "b:",
                "SCHEMA",
                "t",
                "TEXT",
            ]),
        ),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["HSET", "b:1", "t", "beta"])),
        RespValue::Integer(1)
    );

    assert_eq!(handle(&mut h, cmd(&["FLUSHALL"])), RespValue::ok());

    // Keys gone on both DBs
    assert!(databases
        .get(0)
        .unwrap()
        .get_hash(&Bytes::from_static(b"a:1"))
        .is_none());
    assert!(databases
        .get(1)
        .unwrap()
        .get_hash(&Bytes::from_static(b"b:1"))
        .is_none());

    // Schemas remain
    assert!(databases
        .get(0)
        .unwrap()
        .list_search_indices()
        .iter()
        .any(|n| n == "idx0"));
    assert!(databases
        .get(1)
        .unwrap()
        .list_search_indices()
        .iter()
        .any(|n| n == "idx1"));

    // Re-index on DB 1 after FLUSHALL
    assert_eq!(
        handle(&mut h, cmd(&["HSET", "b:2", "t", "gamma"])),
        RespValue::Integer(1)
    );
    assert_eq!(
        search_total(&handle(&mut h, cmd(&["FT.SEARCH", "idx1", "gamma"]))),
        1
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Failed AOF load into empty target commits nothing (scratch discarded).
#[test]
fn aof_load_failure_still_wipes_ft_schema() {
    let dir = tmp_dir("aof-fail-wipe");
    let path = dir.join("appendonly.aof");

    let mut writer = aof::AofWriter::open(&path).unwrap();
    writer
        .append_command(&argv(&[
            "FT.CREATE",
            "idx",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
        ]))
        .unwrap();
    writer
        .append_command(&argv(&["HSET", "doc:1", "title", "hello"]))
        .unwrap();
    writer
        .append_command(&argv(&["FT.ALIASADD", "blog", "idx"]))
        .unwrap();
    // Duplicate CREATE fails load after partial apply.
    writer
        .append_command(&argv(&[
            "FT.CREATE",
            "idx",
            "SCHEMA",
            "title",
            "TEXT",
        ]))
        .unwrap();
    drop(writer);

    let loaded = make_databases();
    aof::load_into_databases(&loaded, &path).expect_err("duplicate CREATE must fail");

    let cache = loaded.get(0).unwrap();
    assert!(
        cache.list_search_indices().is_empty(),
        "failed AOF load must drop FT indices"
    );
    assert!(
        cache.list_search_aliases().is_empty(),
        "failed AOF load must drop aliases"
    );
    assert!(
        cache.get_hash(&Bytes::from_static(b"doc:1")).is_none(),
        "failed AOF load must drop keys"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
