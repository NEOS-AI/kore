//! Batch BV: AOF load must surface FT mutator failures (not silent Ok).

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::databases::Databases;
use kore::error::Error;
use kore::persistence::aof;
use kore::protocol::RespValue;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn tmp_dir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "kore-bv-{}-{}",
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

fn argv(parts: &[&str]) -> Vec<Bytes> {
    parts.iter().map(|p| Bytes::from(p.to_string())).collect()
}

fn assert_err_variant(err: Error) {
    match err {
        Error::InvalidArgument(msg) | Error::ParseError(msg) => {
            assert!(!msg.is_empty(), "error message should not be empty");
        }
        other => panic!("expected InvalidArgument or ParseError, got {:?}", other),
    }
}

/// Duplicate FT.CREATE for the same index name must fail apply (not Ok + empty search).
#[test]
fn duplicate_ft_create_apply_returns_err() {
    let databases = make_databases();
    let cache = databases.get(0).expect("db 0");

    let create = argv(&[
        "FT.CREATE",
        "articles",
        "PREFIX",
        "1",
        "doc:",
        "SCHEMA",
        "title",
        "TEXT",
    ]);
    aof::apply_command_to_cache(&cache, &create).unwrap();
    assert!(
        cache
            .list_search_indices()
            .iter()
            .any(|n| n == "articles"),
        "first create must register index"
    );

    let err = aof::apply_command_to_cache(&cache, &create).expect_err("duplicate must fail");
    assert_err_variant(err);
    // Still exactly one index — failure must not corrupt state.
    assert_eq!(
        cache
            .list_search_indices()
            .into_iter()
            .filter(|n| n == "articles")
            .count(),
        1
    );
}

/// Same conflict via AOF file load_into_databases.
#[test]
fn duplicate_ft_create_aof_load_returns_err() {
    let dir = tmp_dir("dup-create");
    let path = dir.join("appendonly.aof");

    // Write two identical FT.CREATE commands as RESP.
    let mut writer = aof::AofWriter::open(&path).unwrap();
    let create = argv(&[
        "FT.CREATE",
        "idx",
        "SCHEMA",
        "body",
        "TEXT",
    ]);
    writer.append_command(&create).unwrap();
    writer.append_command(&create).unwrap();
    drop(writer);

    let loaded = make_databases();
    let err = aof::load_into_databases(&loaded, &path).expect_err("load must fail on duplicate");
    assert_err_variant(err);

    // All-or-nothing: partial apply is flushed so no leftover index remains.
    let cache = loaded.get(0).unwrap();
    assert!(
        cache.list_search_indices().is_empty(),
        "failed load must flush partial FT.CREATE state"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// ALIASADD targeting a missing index must fail apply/load.
#[test]
fn aliasadd_missing_index_returns_err() {
    let databases = make_databases();
    let cache = databases.get(0).expect("db 0");

    let err = aof::apply_command_to_cache(
        &cache,
        &argv(&["FT.ALIASADD", "blog", "no_such_index"]),
    )
    .expect_err("ALIASADD to missing index must fail");
    assert_err_variant(err);
    assert!(
        cache.list_search_aliases().is_empty(),
        "failed ALIASADD must not leave alias"
    );
}

#[test]
fn aliasadd_missing_index_aof_load_returns_err() {
    let dir = tmp_dir("alias-missing");
    let path = dir.join("appendonly.aof");

    let mut writer = aof::AofWriter::open(&path).unwrap();
    writer
        .append_command(&argv(&["FT.ALIASADD", "blog", "missing"]))
        .unwrap();
    drop(writer);

    let loaded = make_databases();
    let err = aof::load_into_databases(&loaded, &path).expect_err("load must fail");
    assert_err_variant(err);
    assert!(loaded.get(0).unwrap().list_search_aliases().is_empty());

    let _ = std::fs::remove_dir_all(&dir);
}

/// Successful create then second create in same AOF fails the whole load.
#[test]
fn second_create_after_success_fails_load() {
    let dir = tmp_dir("second-create");
    let path = dir.join("appendonly.aof");
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
                "x:",
                "SCHEMA",
                "t",
                "TEXT",
            ]),
        ),
        RespValue::ok()
    );

    aof::rewrite_databases(&databases, &path).unwrap();

    // Append a conflicting second CREATE for the same name.
    let mut writer = aof::AofWriter::open(&path).unwrap();
    writer
        .append_command(&argv(&[
            "FT.CREATE",
            "idx",
            "SCHEMA",
            "t",
            "TEXT",
        ]))
        .unwrap();
    drop(writer);

    let loaded = make_databases();
    let err = aof::load_into_databases(&loaded, &path).expect_err("duplicate after rewrite");
    assert_err_variant(err);

    // Partial rewrite payload must not remain after failed load.
    let cache = loaded.get(0).unwrap();
    assert!(
        cache.list_search_indices().is_empty(),
        "failed load must flush rewritten FT state"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// DROPINDEX for a missing index fails apply (stricter than DEL no-op).
#[test]
fn dropindex_missing_returns_err() {
    let databases = make_databases();
    let cache = databases.get(0).unwrap();
    let err = aof::apply_command_to_cache(&cache, &argv(&["FT.DROPINDEX", "nope"]))
        .expect_err("DROPINDEX missing must fail");
    assert_err_variant(err);
}

/// ALIASDEL for a missing alias fails apply.
#[test]
fn aliasdel_missing_returns_err() {
    let databases = make_databases();
    let cache = databases.get(0).unwrap();
    let err = aof::apply_command_to_cache(&cache, &argv(&["FT.ALIASDEL", "nope"]))
        .expect_err("ALIASDEL missing must fail");
    assert_err_variant(err);
}

/// Non-truncated but unparsable FT.CREATE (e.g. bad field type) fails load.
#[test]
fn unparsable_ft_create_returns_parse_error() {
    let databases = make_databases();
    let cache = databases.get(0).unwrap();

    // len >= 4 but SCHEMA field type unknown → parse None → ParseError
    let err = aof::apply_command_to_cache(
        &cache,
        &argv(&["FT.CREATE", "idx", "SCHEMA", "f", "NOTATYPE"]),
    )
    .expect_err("unparsable FT.CREATE must fail");
    match err {
        Error::ParseError(msg) => assert!(msg.contains("FT.CREATE"), "{msg}"),
        other => panic!("expected ParseError, got {:?}", other),
    }
}

/// Truncated FT.CREATE still skips liberally (parity with SET/HSET truncated argv).
#[test]
fn truncated_ft_create_skips() {
    let databases = make_databases();
    let cache = databases.get(0).unwrap();
    aof::apply_command_to_cache(&cache, &argv(&["FT.CREATE", "only_name"])).unwrap();
    assert!(cache.list_search_indices().is_empty());
}
