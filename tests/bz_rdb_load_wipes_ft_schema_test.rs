//! Batch BZ / CB: RDB load with flush=true replaces FT schema (snapshot swap);
//! mid-load FT failure leaves target untouched (scratch discarded);
//! live FLUSHDB still keeps schema.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::databases::Databases;
use kore::persistence::rdb::{
    self, DbSnapshot, HashRecord, MultiDbSnapshot,
};
use kore::protocol::RespValue;
use kore::search_index::{FieldDefinition, FieldType, IndexDefinition};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn tmp_dir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "kore-bz-{}-{}",
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
        deadlock_ui_port: 0,
        enable_deadlock_detection: false,
        deadlock_max_wait_ms: 30_000,
        deadlock_auto_resolve: false,
        deadlock_victim_strategy: "youngest".to_string(),
        tls: false,
        tls_cert: String::new(),
        tls_key: String::new(),
        aclfile: String::new(),
        cluster_enabled: false,
            cluster_replica_priority: 100,
            cluster_require_full_coverage: true,
            cluster_allow_reads_when_down: false,
            cluster_announce_ip: String::new(),
            cluster_announce_port: 0,
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

/// Create index (+ alias + docs) → SAVE → load with flush into *same* process
/// that still holds the schema → must succeed (not "Index already exists").
#[test]
fn rdb_load_flush_wipes_ft_schema_and_reloads() {
    let dir = tmp_dir("flush-reload");
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
            ]),
        ),
        RespValue::ok()
    );
    assert_eq!(
        handle(
            &mut h,
            cmd(&["HSET", "doc:1", "title", "hello world"]),
        ),
        RespValue::Integer(1)
    );
    assert_eq!(
        handle(&mut h, cmd(&["FT.ALIASADD", "blog", "articles"])),
        RespValue::ok()
    );
    assert_eq!(
        search_total(&handle(&mut h, cmd(&["FT.SEARCH", "blog", "hello"]))),
        1
    );

    rdb::save_databases(&databases, &path).unwrap();

    // Schema still present in the live process (the pre-BY×BX bug).
    assert!(
        databases
            .get(0)
            .unwrap()
            .list_search_indices()
            .iter()
            .any(|n| n == "articles"),
        "precondition: schema still live before reload"
    );

    // Snapshot-replace load into the same DBs (FULLRESYNC-style).
    let n = rdb::load_databases(&databases, &path, true).expect(
        "reload with flush=true must wipe FT schema and recreate; must not fail with Index already exists",
    );
    assert!(n >= 1, "expected at least 1 hash key, got {n}");

    let mut h2 = make_handler(databases.clone(), &dir);

    let by_name = handle(&mut h2, cmd(&["FT.SEARCH", "articles", "hello"]));
    assert_eq!(search_total(&by_name), 1);
    assert_eq!(search_ids(&by_name), vec!["doc:1".to_string()]);

    let by_alias = handle(&mut h2, cmd(&["FT.SEARCH", "blog", "hello"]));
    assert_eq!(search_total(&by_alias), 1);
    assert_eq!(search_ids(&by_alias), vec!["doc:1".to_string()]);

    // Second consecutive flush-load also succeeds (idempotent replace).
    rdb::load_databases(&databases, &path, true)
        .expect("second consecutive flush=true load must also succeed");
    let again = handle(&mut h2, cmd(&["FT.SEARCH", "articles", "hello"]));
    assert_eq!(search_total(&again), 1);

    let _ = std::fs::remove_dir_all(&dir);
}

/// Live FLUSHDB still keeps FT schema (BX regression guard).
#[test]
fn live_flushdb_still_keeps_ft_schema() {
    let dir = tmp_dir("flushdb-keeps");
    let databases = make_databases();
    let mut h = make_handler(databases.clone(), &dir);

    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "FT.CREATE",
                "idx",
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
        handle(&mut h, cmd(&["FT.ALIASADD", "a", "idx"])),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["HSET", "doc:1", "title", "hello"])),
        RespValue::Integer(1)
    );

    assert_eq!(handle(&mut h, cmd(&["FLUSHDB"])), RespValue::ok());

    let cache = databases.get(0).unwrap();
    assert!(
        cache.list_search_indices().iter().any(|n| n == "idx"),
        "FLUSHDB must keep schema; got {:?}",
        cache.list_search_indices()
    );
    assert!(
        cache
            .list_search_aliases()
            .iter()
            .any(|(al, ix)| al == "a" && ix == "idx"),
        "FLUSHDB must keep aliases; got {:?}",
        cache.list_search_aliases()
    );
    assert_eq!(
        search_total(&handle(&mut h, cmd(&["FT.SEARCH", "idx", "hello"]))),
        0,
        "docs must be gone after FLUSHDB"
    );

    // New HSET reindexes against surviving schema
    assert_eq!(
        handle(&mut h, cmd(&["HSET", "doc:2", "title", "fresh hello"])),
        RespValue::Integer(1)
    );
    assert_eq!(
        search_total(&handle(&mut h, cmd(&["FT.SEARCH", "idx", "fresh"]))),
        1
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Mid-load FT.ALIASADD failure → Err; target (incl. pre-existing) untouched.
#[test]
fn rdb_load_mid_ft_failure_wipes_partial_state() {
    // Hand-built snapshot: valid index + hash, then alias to missing index.
    // load_into creates schema + keys first, then fails on ALIASADD (on scratch).
    let def = IndexDefinition::new(
        "idx".to_string(),
        vec!["doc:".to_string()],
        vec![FieldDefinition {
            name: "title".to_string(),
            field_type: FieldType::Text {
                weight: 1.0,
                sortable: false,
            },
        }],
    );
    let snap = MultiDbSnapshot {
        databases: vec![(
            0,
            DbSnapshot {
                strings: Vec::new(),
                zsets: Vec::new(),
                geos: Vec::new(),
                hashes: vec![HashRecord {
                    key: Bytes::from("doc:1"),
                    fields: vec![(Bytes::from("title"), Bytes::from("hello"))],
                }],
                lists: Vec::new(),
                sets: Vec::new(),
                streams: Vec::new(),
                typed_expires: Vec::new(),
                search_indices: vec![def],
                search_aliases: vec![("blog".to_string(), "missing".to_string())],
                hnsw_graphs: Vec::new(),
            },
        )],
    };
    let bytes = Bytes::from(snap.encode().unwrap());

    let loaded = make_databases();
    // Pre-existing key + FT schema must survive failed load (Batch CB).
    loaded
        .get(0)
        .unwrap()
        .store(
            Bytes::from("preexisting"),
            Bytes::from("value"),
            Default::default(),
        )
        .unwrap();
    loaded
        .get(0)
        .unwrap()
        .create_search_index(IndexDefinition::new(
            "keep_me".to_string(),
            vec!["keep:".to_string()],
            vec![FieldDefinition {
                name: "t".to_string(),
                field_type: FieldType::Text {
                    weight: 1.0,
                    sortable: false,
                },
            }],
        ))
        .unwrap();

    let err = rdb::load_databases_bytes(&loaded, &bytes, false)
        .expect_err("bad alias must fail RDB load");
    match err {
        kore::error::Error::InvalidArgument(msg) => {
            let lower = msg.to_lowercase();
            assert!(
                lower.contains("alias")
                    || lower.contains("unknown index")
                    || lower.contains("missing"),
                "expected ALIASADD / unknown-index style error, got: {msg}"
            );
        }
        other => panic!("expected InvalidArgument, got {:?}", other),
    }

    let cache = loaded.get(0).unwrap();
    // Partial scratch must not commit.
    assert!(
        !cache.list_search_indices().iter().any(|n| n == "idx"),
        "partial index from failed load must not commit; got {:?}",
        cache.list_search_indices()
    );
    assert!(
        cache.list_search_aliases().is_empty(),
        "partial aliases from failed load must not commit; got {:?}",
        cache.list_search_aliases()
    );
    assert!(
        cache
            .get_hash(&Bytes::from_static(b"doc:1"))
            .is_none(),
        "partial hash from failed load must not commit"
    );
    // Pre-existing target state preserved.
    let pre = cache
        .load(&Bytes::from("preexisting"), Default::default())
        .unwrap()
        .expect("pre-existing key must survive failed RDB load");
    assert_eq!(pre.value.as_ref(), b"value");
    assert!(
        cache.list_search_indices().iter().any(|n| n == "keep_me"),
        "pre-existing FT schema must survive failed RDB load; got {:?}",
        cache.list_search_indices()
    );
}
