//! Batch CL: min-replicas-to-write applies to FT.* mutators.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::persistence::{PersistenceConfig, PersistenceManager, SaveRule};
use kore::protocol::RespValue;
use kore::Cache;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("kore-cl-{}-{}", label, nanos));
    let _ = std::fs::create_dir_all(&dir);
    dir
}

fn make_persistence(dir: &PathBuf) -> Arc<PersistenceManager> {
    let pconfig = PersistenceConfig {
        dir: dir.clone(),
        dbfilename: "dump.rdb".to_string(),
        appendonly: false,
        appendfilename: "appendonly.aof".to_string(),
        save_rules: vec![SaveRule::new(900, 1)],
    };
    PersistenceManager::new(pconfig).unwrap()
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
        maxconns: 100,
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
        save: "900,1".to_string(),
        maxmemory_policy: "allkeys-lru".to_string(),
        databases: 16,
        metrics_port: 0,
        tls: false,
        tls_cert: String::new(),
        tls_key: String::new(),
        aclfile: String::new(),
        cluster_enabled: false,
        unixsocket: String::new(),
    })
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

fn err_contains(resp: RespValue, needle: &str) -> bool {
    match resp {
        RespValue::Error(e) => String::from_utf8_lossy(&e).contains(needle),
        _ => false,
    }
}

#[test]
fn min_replicas_blocks_ft_create() {
    let dir = unique_dir("ft-min");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir), Some(mgr.clone()));

    assert!(matches!(
        handle(
            &mut h,
            cmd(&["CONFIG", "SET", "min-replicas-to-write", "1"])
        ),
        RespValue::SimpleString(_)
    ));

    let create = handle(
        &mut h,
        cmd(&[
            "FT.CREATE",
            "idx",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "t",
            "TEXT",
        ]),
    );
    assert!(
        err_contains(create, "NOREPLICAS"),
        "FT.CREATE must respect min-replicas-to-write"
    );

    // Register a replica → FT.CREATE allowed
    let _feed = mgr.replication.register_replica();
    assert!(matches!(
        handle(
            &mut h,
            cmd(&[
                "FT.CREATE",
                "idx",
                "PREFIX",
                "1",
                "doc:",
                "SCHEMA",
                "t",
                "TEXT",
            ])
        ),
        RespValue::SimpleString(_)
    ));

    // FT.SEARCH is not a write
    assert!(matches!(
        handle(&mut h, cmd(&["CONFIG", "SET", "min-replicas-to-write", "1"])),
        RespValue::SimpleString(_)
    ));
    // drop feed? if we need zero replicas again - unregister isn't simple
    // With one feed still registered, search still works
    let search = handle(&mut h, cmd(&["FT.SEARCH", "idx", "*"]));
    assert!(
        !err_contains(search, "NOREPLICAS"),
        "FT.SEARCH must not be gated by min-replicas"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
