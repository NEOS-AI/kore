//! Batch CD: atomic WATCH bump with replace; typed export skips expired keys.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::databases::Databases;
use kore::entry::StoreOptions;
use kore::persistence::rdb::{self, DbSnapshot, MultiDbSnapshot, StringRecord};
use kore::protocol::RespValue;
use kore::Cache;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn tmp_dir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "kore-cd-{}-{}",
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

/// After replace, watched gen is always dirty (no empty-map clean window).
#[test]
fn watch_generation_dirty_immediately_after_replace() {
    let cache = Cache::new_with_sweep(4, 1024 * 1024, 1024 * 1024, false);
    let k = Bytes::from_static(b"w");
    cache
        .store(k.clone(), Bytes::from_static(b"v"), StoreOptions::default())
        .unwrap();
    let gen_before = cache.watch_generation(&k);
    assert_eq!(gen_before, 0);

    let scratch = cache.empty_keyspace_like();
    scratch
        .store(
            Bytes::from_static(b"new"),
            Bytes::from_static(b"1"),
            StoreOptions::default(),
        )
        .unwrap();
    cache.replace_keyspace_from(&scratch);

    let gen_after = cache.watch_generation(&k);
    assert_ne!(gen_after, gen_before);
    assert!(gen_after >= 1);
}

#[test]
fn watch_aborts_exec_after_flush_true_load() {
    let dir = tmp_dir("watch-flush");
    let databases = make_databases();
    let mut handler = make_handler(databases.clone(), &dir);

    assert!(matches!(
        handle(&mut handler, cmd(&["SET", "wk", "1"])),
        RespValue::SimpleString(_)
    ));
    assert!(matches!(
        handle(&mut handler, cmd(&["WATCH", "wk"])),
        RespValue::SimpleString(_)
    ));
    assert!(matches!(
        handle(&mut handler, cmd(&["MULTI"])),
        RespValue::SimpleString(_)
    ));
    assert!(matches!(
        handle(&mut handler, cmd(&["SET", "wk", "2"])),
        RespValue::SimpleString(_)
    ));

    let cache = databases.db0();
    let good = MultiDbSnapshot {
        databases: vec![(
            0,
            DbSnapshot {
                strings: vec![StringRecord {
                    key: Bytes::from_static(b"other"),
                    value: Bytes::from_static(b"x"),
                    flags: 0,
                    expire_unix_ms: -1,
                }],
                zsets: Vec::new(),
                geos: Vec::new(),
                hashes: Vec::new(),
                lists: Vec::new(),
                sets: Vec::new(),
                streams: Vec::new(),
                typed_expires: Vec::new(),
                search_indices: Vec::new(),
                search_aliases: Vec::new(),
            },
        )],
    };
    let bytes = good.encode().unwrap();
    rdb::load_bytes(&cache, &bytes, true).unwrap();

    let exec = handle(&mut handler, cmd(&["EXEC"]));
    assert!(
        matches!(exec, RespValue::Null | RespValue::BulkString(None)),
        "EXEC should abort after load replace, got {:?}",
        exec
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn typed_export_skips_expired_hash() {
    let dir = tmp_dir("typed-export");
    let databases = make_databases();
    let mut handler = make_handler(databases.clone(), &dir);
    let cache = databases.db0();

    handle(&mut handler, cmd(&["HSET", "h:live", "f", "1"]));
    handle(&mut handler, cmd(&["HSET", "h:dead", "f", "2"]));
    // 1 ms TTL; do not touch dead key so body remains until export filter.
    handle(&mut handler, cmd(&["PEXPIRE", "h:dead", "1"]));
    std::thread::sleep(Duration::from_millis(5));

    let live = Bytes::from_static(b"h:live");
    let dead = Bytes::from_static(b"h:dead");
    let exported: Vec<_> = cache
        .export_hashes()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert!(exported.iter().any(|k| k == &live), "live hash missing");
    assert!(
        !exported.iter().any(|k| k == &dead),
        "expired hash should not export"
    );

    let snap = DbSnapshot::from_cache(&cache).unwrap();
    assert!(!snap.hashes.iter().any(|r| r.key == dead));
    assert!(snap.hashes.iter().any(|r| r.key == live));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn with_autosweep_paused_holds_cycle_lock() {
    let cache = Cache::new_with_sweep(4, 1024 * 1024, 1024 * 1024, false);
    cache.set_autosweep(true);
    cache.with_autosweep_paused(|| {
        assert!(!cache.autosweep_enabled());
    });
    assert!(cache.autosweep_enabled());
}
