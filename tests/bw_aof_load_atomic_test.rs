//! Batch BW / CB: AOF load is all-or-nothing via scratch-load.
//! Failed apply leaves the target untouched (partial state lives only on scratch).

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
        "kore-bw-{}-{}",
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

/// Successful CREATE + HSET then failing second CREATE → Err; empty target stays empty
/// (partial scratch state is discarded, not committed).
#[test]
fn failed_load_flushes_partial_create_and_hset() {
    let dir = tmp_dir("partial-flush");
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
        .append_command(&argv(&[
            "HSET",
            "doc:1",
            "title",
            "hello",
        ]))
        .unwrap();
    // Duplicate CREATE fails the load after earlier commands applied.
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
    let err = aof::load_into_databases(&loaded, &path).expect_err("duplicate CREATE must fail load");
    match err {
        kore::error::Error::InvalidArgument(msg) => {
            assert!(!msg.is_empty());
        }
        other => panic!("expected InvalidArgument, got {:?}", other),
    }

    let cache = loaded.get(0).unwrap();
    assert!(
        cache.list_search_indices().is_empty(),
        "indices must not be committed after failed load"
    );
    assert!(
        cache.list_search_aliases().is_empty(),
        "aliases must not be committed after failed load"
    );
    // Hash key from partial HSET must not remain (scratch discarded).
    assert!(
        cache.get_hash(&Bytes::from_static(b"doc:1")).is_none(),
        "partial HSET key must not be committed"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Full successful AOF load still restores index + hash + alias.
#[test]
fn successful_load_preserves_search_and_keys() {
    let dir = tmp_dir("success-load");
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
        handle(
            &mut h,
            cmd(&["HSET", "doc:1", "title", "hello"]),
        ),
        RespValue::Integer(1)
    );
    assert_eq!(
        handle(&mut h, cmd(&["FT.ALIASADD", "blog", "articles"])),
        RespValue::ok()
    );

    aof::rewrite_databases(&databases, &path).unwrap();

    let loaded = make_databases();
    let n = aof::load_into_databases(&loaded, &path).expect("full load must succeed");
    assert!(n >= 3, "expected create + hset + alias at least, got {n}");

    let cache = loaded.get(0).unwrap();
    assert!(
        cache
            .list_search_indices()
            .iter()
            .any(|n| n == "articles"),
        "index restored"
    );
    assert!(
        cache
            .list_search_aliases()
            .iter()
            .any(|(a, i)| a == "blog" && i == "articles"),
        "alias restored"
    );
    assert!(
        cache.get_hash(&Bytes::from_static(b"doc:1")).is_some(),
        "hash key restored"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// load_into_cache also discards partial scratch on FT failure.
#[test]
fn failed_load_into_cache_flushes() {
    let dir = tmp_dir("cache-flush");
    let path = dir.join("appendonly.aof");

    let mut writer = aof::AofWriter::open(&path).unwrap();
    writer
        .append_command(&argv(&["SET", "k", "v"]))
        .unwrap();
    writer
        .append_command(&argv(&[
            "FT.CREATE",
            "idx",
            "SCHEMA",
            "t",
            "TEXT",
        ]))
        .unwrap();
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

    let cache = kore::cache::Cache::new_with_sweep(8, 1024 * 1024 * 50, 500 * 1024 * 1024, false);
    aof::load_into_cache(&cache, &path).expect_err("duplicate CREATE");

    assert!(cache.list_search_indices().is_empty());
    assert!(!cache.exists(&Bytes::from_static(b"k")));

    let _ = std::fs::remove_dir_all(&dir);
}
