//! Batch CB: failed AOF/RDB load must not destroy pre-existing target data.
//! Successful loads still commit new data (and flush=true snapshot-replaces).

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::databases::Databases;
use kore::entry::LoadOptions;
use kore::persistence::aof;
use kore::persistence::rdb::{self, DbSnapshot, HashRecord, MultiDbSnapshot};
use kore::protocol::RespValue;
use kore::search_index::{FieldDefinition, FieldType, IndexDefinition};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn tmp_dir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "kore-cb-{}-{}",
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

fn argv(parts: &[&str]) -> Vec<Bytes> {
    parts.iter().map(|p| Bytes::from(p.to_string())).collect()
}

fn seed_preexisting(db: &Databases) {
    let cache = db.get(0).unwrap();
    cache
        .store(
            Bytes::from("preexisting"),
            Bytes::from("keep-me"),
            Default::default(),
        )
        .unwrap();
    cache
        .create_search_index(IndexDefinition::new(
            "keep_idx".to_string(),
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
}

fn assert_preexisting(db: &Databases) {
    let cache = db.get(0).unwrap();
    let pre = cache
        .load(
            &Bytes::from("preexisting"),
            LoadOptions {
                touch: false,
                with_cas: false,
            },
        )
        .unwrap()
        .expect("pre-existing key must remain");
    assert_eq!(pre.value.as_ref(), b"keep-me");
    assert!(
        cache
            .list_search_indices()
            .iter()
            .any(|n| n == "keep_idx"),
        "pre-existing FT index must remain; got {:?}",
        cache.list_search_indices()
    );
}

/// Failed AOF load with pre-existing key + FT index → target unchanged.
#[test]
fn aof_load_failure_preserves_preexisting_key_and_ft() {
    let dir = tmp_dir("aof-fail-preserve");
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
    seed_preexisting(&loaded);

    aof::load_into_databases(&loaded, &path).expect_err("duplicate CREATE must fail");

    assert_preexisting(&loaded);
    let cache = loaded.get(0).unwrap();
    assert!(
        !cache.list_search_indices().iter().any(|n| n == "idx"),
        "partial AOF index must not commit"
    );
    assert!(
        cache.get_hash(&Bytes::from_static(b"doc:1")).is_none(),
        "partial AOF hash must not commit"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Failed RDB load mid-apply with pre-existing → target unchanged.
#[test]
fn rdb_load_failure_preserves_preexisting() {
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
            },
        )],
    };
    let bytes = Bytes::from(snap.encode().unwrap());

    let loaded = make_databases();
    seed_preexisting(&loaded);

    rdb::load_databases_bytes(&loaded, &bytes, false)
        .expect_err("bad alias must fail RDB load");

    assert_preexisting(&loaded);
    let cache = loaded.get(0).unwrap();
    assert!(cache.get_hash(&Bytes::from_static(b"doc:1")).is_none());
    assert!(!cache.list_search_indices().iter().any(|n| n == "idx"));

    // flush=true failure path also preserves (scratch never commits).
    rdb::load_databases_bytes(&loaded, &bytes, true)
        .expect_err("bad alias must fail with flush=true too");
    assert_preexisting(&loaded);
}

/// Successful AOF load commits new data (replaces keyspace).
#[test]
fn successful_aof_load_commits_new_data() {
    let dir = tmp_dir("aof-ok");
    let path = dir.join("appendonly.aof");
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
        handle(&mut h, cmd(&["HSET", "doc:1", "title", "hello"])),
        RespValue::Integer(1)
    );
    aof::rewrite_databases(&databases, &path).unwrap();

    let loaded = make_databases();
    // Pre-existing should be replaced on successful AOF load (full replay).
    seed_preexisting(&loaded);
    let n = aof::load_into_databases(&loaded, &path).expect("AOF load must succeed");
    assert!(n >= 2);

    let cache = loaded.get(0).unwrap();
    assert!(
        cache
            .list_search_indices()
            .iter()
            .any(|n| n == "articles"),
        "index from AOF committed"
    );
    assert!(cache.get_hash(&Bytes::from_static(b"doc:1")).is_some());
    // Snapshot-style replace: pre-existing not in AOF is gone.
    assert!(
        cache
            .load(
                &Bytes::from("preexisting"),
                LoadOptions {
                    touch: false,
                    with_cas: false,
                },
            )
            .unwrap()
            .is_none(),
        "successful AOF load replaces keyspace"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Successful RDB load with flush=true replaces target (incl. FT schema).
#[test]
fn successful_rdb_flush_replaces_target() {
    let dir = tmp_dir("rdb-ok");
    let path = dir.join("dump.rdb");
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
        handle(&mut h, cmd(&["HSET", "doc:1", "title", "hello"])),
        RespValue::Integer(1)
    );
    rdb::save_databases(&databases, &path).unwrap();

    let loaded = make_databases();
    seed_preexisting(&loaded);
    let n = rdb::load_databases(&loaded, &path, true).expect("RDB flush load must succeed");
    assert!(n >= 1);

    let cache = loaded.get(0).unwrap();
    assert!(cache
        .list_search_indices()
        .iter()
        .any(|n| n == "articles"));
    assert!(cache.get_hash(&Bytes::from_static(b"doc:1")).is_some());
    assert!(
        cache
            .load(
                &Bytes::from("preexisting"),
                LoadOptions {
                    touch: false,
                    with_cas: false,
                },
            )
            .unwrap()
            .is_none(),
        "flush=true replaces pre-existing keys"
    );
    assert!(
        !cache.list_search_indices().iter().any(|n| n == "keep_idx"),
        "flush=true replaces pre-existing FT schema"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Successful RDB load with flush=false merges while keeping pre-existing keys.
#[test]
fn successful_rdb_merge_keeps_preexisting_keys() {
    let dir = tmp_dir("rdb-merge");
    let path = dir.join("dump.rdb");
    let databases = make_databases();
    databases
        .get(0)
        .unwrap()
        .store(
            Bytes::from("from_rdb"),
            Bytes::from("v"),
            Default::default(),
        )
        .unwrap();
    rdb::save_databases(&databases, &path).unwrap();

    let loaded = make_databases();
    seed_preexisting(&loaded);
    let n = rdb::load_databases(&loaded, &path, false).expect("merge load must succeed");
    assert!(n >= 1);

    assert_preexisting(&loaded);
    let cache = loaded.get(0).unwrap();
    let v = cache
        .load(
            &Bytes::from("from_rdb"),
            LoadOptions {
                touch: false,
                with_cas: false,
            },
        )
        .unwrap()
        .expect("RDB key merged in");
    assert_eq!(v.value.as_ref(), b"v");

    let _ = std::fs::remove_dir_all(&dir);
}
