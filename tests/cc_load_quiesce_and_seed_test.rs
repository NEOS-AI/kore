//! Batch CC: load quiesce, non-mutating merge seed, flush=true replace, WATCH bump.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::databases::Databases;
use kore::entry::{LoadOptions, StoreOptions};
use kore::persistence::rdb::{self, DbSnapshot, MultiDbSnapshot, StringRecord};
use kore::protocol::RespValue;
use kore::Cache;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn tmp_dir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "kore-cc-{}-{}",
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

fn bad_alias_rdb_bytes() -> Bytes {
    use kore::search_index::{FieldDefinition, FieldType, IndexDefinition};
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
                hashes: Vec::new(),
                lists: Vec::new(),
                sets: Vec::new(),
                streams: Vec::new(),
                typed_expires: Vec::new(),
                search_indices: vec![def],
                search_aliases: vec![("blog".to_string(), "missing".to_string())],
            },
        )],
    };
    Bytes::from(snap.encode().unwrap())
}

fn ok_replace_rdb_bytes(key: &str, val: &str) -> Bytes {
    let snap = MultiDbSnapshot {
        databases: vec![(
            0,
            DbSnapshot {
                strings: vec![StringRecord {
                    key: Bytes::from(key.to_string()),
                    value: Bytes::from(val.to_string()),
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
    Bytes::from(snap.encode().unwrap())
}

/// Failed flush=false merge seed must not lazy-delete expired keys or bump GET stats.
#[test]
fn failed_merge_seed_does_not_mutate_target() {
    let loaded = make_databases();
    let cache = loaded.get(0).unwrap();

    // Live key that must remain.
    cache
        .store(
            Bytes::from("live"),
            Bytes::from("ok"),
            StoreOptions::default(),
        )
        .unwrap();

    // Expired key still resident in the map until load/sweep.
    cache
        .store(
            Bytes::from("stale"),
            Bytes::from("gone"),
            StoreOptions {
                ttl_ms: Some(1),
                ..Default::default()
            },
        )
        .unwrap();
    std::thread::sleep(Duration::from_millis(15));

    let cmd_get_before = cache.stats.cmd_get.load(Ordering::Relaxed);
    let hits_before = cache.stats.hits.load(Ordering::Relaxed);
    let expired_before = cache.stats.evicted_expired.load(Ordering::Relaxed);
    let mem_before = cache.string_memory_usage();

    rdb::load_databases_bytes(&loaded, &bad_alias_rdb_bytes(), false)
        .expect_err("bad alias must fail merge");

    // Pre-existing live key remains.
    let live = cache
        .load(
            &Bytes::from("live"),
            LoadOptions {
                touch: false,
                with_cas: false,
            },
        )
        .unwrap()
        .expect("live key must remain after failed merge");
    assert_eq!(live.value.as_ref(), b"ok");

    // Seed must not have walked the map via load() (cmd_get / hits) or lazy-deleted.
    // (The live load above does one cmd_get+hit; account for that.)
    let cmd_get_after = cache.stats.cmd_get.load(Ordering::Relaxed);
    let hits_after = cache.stats.hits.load(Ordering::Relaxed);
    let expired_after = cache.stats.evicted_expired.load(Ordering::Relaxed);
    assert_eq!(
        cmd_get_after,
        cmd_get_before + 1,
        "seed must not bump cmd_get; only our post-check load"
    );
    assert_eq!(
        hits_after,
        hits_before + 1,
        "seed must not bump hits; only our post-check load"
    );
    assert_eq!(
        expired_after, expired_before,
        "seed must not lazy-delete expired keys"
    );

    // Expired entry should still be freeable by sweep (still in map).
    // Memory should not have dropped from a seed-time lazy delete.
    assert!(
        cache.string_memory_usage() >= mem_before.saturating_sub(64),
        "seed must not free expired entry memory"
    );
    let swept = cache.sweep();
    assert!(
        swept >= 1,
        "expired key should still be present for sweep; swept={}",
        swept
    );
}

/// Successful flush=true load replaces pre-existing data.
#[test]
fn successful_flush_true_replaces_data() {
    let loaded = make_databases();
    let cache = loaded.get(0).unwrap();
    cache
        .store(
            Bytes::from("old"),
            Bytes::from("v1"),
            StoreOptions::default(),
        )
        .unwrap();

    let n = rdb::load_databases_bytes(&loaded, &ok_replace_rdb_bytes("new", "v2"), true)
        .expect("flush=true load must succeed");
    assert_eq!(n, 1);

    assert!(
        cache
            .load(
                &Bytes::from("old"),
                LoadOptions {
                    touch: false,
                    with_cas: false,
                },
            )
            .unwrap()
            .is_none(),
        "pre-existing key must be gone after flush=true replace"
    );
    let entry = cache
        .load(
            &Bytes::from("new"),
            LoadOptions {
                touch: false,
                with_cas: false,
            },
        )
        .unwrap()
        .expect("new key from RDB");
    assert_eq!(entry.value.as_ref(), b"v2");
}

/// Autosweep flag is restored after successful and failed load.
#[test]
fn autosweep_restored_after_load() {
    let loaded = make_databases();
    let cache = loaded.get(0).unwrap();
    cache.set_autosweep(true);
    assert!(cache.autosweep_enabled());

    // Failure path.
    rdb::load_databases_bytes(&loaded, &bad_alias_rdb_bytes(), true)
        .expect_err("bad alias");
    assert!(
        cache.autosweep_enabled(),
        "autosweep must remain true after failed load"
    );

    // Success path.
    rdb::load_databases_bytes(&loaded, &ok_replace_rdb_bytes("k", "v"), true)
        .expect("ok load");
    assert!(
        cache.autosweep_enabled(),
        "autosweep must remain true after successful load"
    );

    // Single-cache load path.
    let single = Cache::new_with_sweep(8, 1024 * 1024 * 50, 500 * 1024 * 1024, false);
    single.set_autosweep(true);
    rdb::load_bytes(&single, &ok_replace_rdb_bytes("a", "b"), true).unwrap();
    assert!(single.autosweep_enabled());
}

/// WATCH'd key aborts EXEC after a successful keyspace replace load.
#[test]
fn watch_aborts_exec_after_load_replace() {
    let dir = tmp_dir("watch");
    let databases = make_databases();
    let mut h = make_handler(databases.clone(), &dir);
    let cache = databases.get(0).unwrap();

    assert_eq!(
        handle(&mut h, cmd(&["SET", "watched", "before"])),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["WATCH", "watched"])),
        RespValue::ok()
    );
    assert_eq!(handle(&mut h, cmd(&["MULTI"])), RespValue::ok());
    assert_eq!(
        handle(&mut h, cmd(&["SET", "watched", "from-multi"])),
        RespValue::SimpleString(Bytes::from_static(b"QUEUED"))
    );

    // Dataset replace while MULTI is open (simulates FULLRESYNC / load commit).
    rdb::load_databases_bytes(&databases, &ok_replace_rdb_bytes("other", "x"), true)
        .expect("replace load");

    // EXEC must abort (null) because WATCH gen was bumped on replace.
    let exec = handle(&mut h, cmd(&["EXEC"]));
    assert_eq!(exec, RespValue::null(), "EXEC must abort after load replace");

    // Key from MULTI must not have been applied; RDB key is present.
    assert!(
        cache
            .load(
                &Bytes::from("watched"),
                LoadOptions {
                    touch: false,
                    with_cas: false,
                },
            )
            .unwrap()
            .is_none()
    );
    assert!(
        cache
            .load(
                &Bytes::from("other"),
                LoadOptions {
                    touch: false,
                    with_cas: false,
                },
            )
            .unwrap()
            .is_some()
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Scratch apply must not inflate live INFO cmd_set (independent Stats).
#[test]
fn failed_load_does_not_inflate_target_stats() {
    let loaded = make_databases();
    let cache = loaded.get(0).unwrap();
    cache
        .store(
            Bytes::from("pre"),
            Bytes::from("v"),
            StoreOptions::default(),
        )
        .unwrap();
    let cmd_set_before = cache.stats.cmd_set.load(Ordering::Relaxed);

    rdb::load_databases_bytes(&loaded, &bad_alias_rdb_bytes(), false)
        .expect_err("fail");

    // Merge seed loads strings into scratch via store on scratch only.
    // Target cmd_set must be unchanged.
    assert_eq!(
        cache.stats.cmd_set.load(Ordering::Relaxed),
        cmd_set_before,
        "scratch must use independent Stats"
    );
}
