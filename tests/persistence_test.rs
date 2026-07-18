//! Phase B persistence tests: RDB save/load, AOF rewrite/load, timed SAVE.

use bytes::Bytes;
use kore::entry::StoreOptions;
use kore::persistence::{aof, rdb, parse_save_rules, PersistenceConfig, PersistenceManager, SaveRule};
use kore::Cache;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn tmp_dir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "kore-persist-{}-{}",
        name,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

#[test]
fn test_rdb_save_and_load_strings_zsets() {
    let dir = tmp_dir("rdb");
    let path = dir.join("dump.rdb");

    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 50, 500 * 1024 * 1024, false);
    cache
        .store(
            Bytes::from("hello"),
            Bytes::from("world"),
            StoreOptions::default(),
        )
        .unwrap();
    cache
        .store(
            Bytes::from("num"),
            Bytes::from("42"),
            StoreOptions::default(),
        )
        .unwrap();

    {
        let z = cache
            .get_or_create_sorted_set(&Bytes::from("scores"))
            .unwrap();
        let mut s = z.write();
        s.add(Bytes::from("alice"), 10.0);
        s.add(Bytes::from("bob"), 20.0);
    }

    rdb::save_file(&cache, &path).unwrap();
    assert!(path.exists());

    let cache2 = Cache::new_with_sweep(16, 1024 * 1024 * 50, 500 * 1024 * 1024, false);
    let n = rdb::load_file(&cache2, &path, true).unwrap();
    assert!(n >= 3);

    let e = cache2
        .load(&Bytes::from("hello"), Default::default())
        .unwrap()
        .unwrap();
    assert_eq!(e.value, Bytes::from("world"));

    let z = cache2.get_sorted_set(&Bytes::from("scores")).unwrap();
    let s = z.read();
    assert_eq!(s.score(&Bytes::from("alice")), Some(10.0));
    assert_eq!(s.score(&Bytes::from("bob")), Some(20.0));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_rdb_preserves_ttl() {
    let dir = tmp_dir("rdb-ttl");
    let path = dir.join("dump.rdb");

    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 50, 500 * 1024 * 1024, false);
    let mut opts = StoreOptions::default();
    opts.ttl_ms = Some(60_000);
    cache
        .store(Bytes::from("temp"), Bytes::from("x"), opts)
        .unwrap();

    rdb::save_file(&cache, &path).unwrap();

    let cache2 = Cache::new_with_sweep(16, 1024 * 1024 * 50, 500 * 1024 * 1024, false);
    rdb::load_file(&cache2, &path, true).unwrap();
    let e = cache2
        .load(&Bytes::from("temp"), Default::default())
        .unwrap()
        .unwrap();
    let ttl = e.ttl_millis().unwrap();
    assert!(ttl > 50_000 && ttl <= 60_000, "ttl={}", ttl);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_aof_rewrite_and_load() {
    let dir = tmp_dir("aof");
    let path = dir.join("appendonly.aof");

    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 50, 500 * 1024 * 1024, false);
    cache
        .store(
            Bytes::from("k1"),
            Bytes::from("v1"),
            StoreOptions::default(),
        )
        .unwrap();
    cache.incr(&Bytes::from("counter"), 5).unwrap();

    aof::rewrite(&cache, &path).unwrap();
    assert!(path.exists());

    let cache2 = Cache::new_with_sweep(16, 1024 * 1024 * 50, 500 * 1024 * 1024, false);
    let n = aof::load_into_cache(&cache2, &path).unwrap();
    assert!(n >= 1);

    let e = cache2
        .load(&Bytes::from("k1"), Default::default())
        .unwrap()
        .unwrap();
    assert_eq!(e.value, Bytes::from("v1"));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_persistence_manager_save_load_startup() {
    let dir = tmp_dir("mgr");
    let pconfig = PersistenceConfig {
        dir: dir.clone(),
        dbfilename: "dump.rdb".to_string(),
        appendonly: false,
        appendfilename: "appendonly.aof".to_string(),
        save_rules: vec![],
    };
    let mgr = PersistenceManager::new(pconfig).unwrap();
    mgr.ensure_dir().unwrap();

    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 50, 500 * 1024 * 1024, false);
    cache
        .store(
            Bytes::from("persist"),
            Bytes::from("yes"),
            StoreOptions::default(),
        )
        .unwrap();
    mgr.save_cache(&cache).unwrap();
    assert!(mgr.last_save_unix() > 0);

    let cache2 = Cache::new_with_sweep(16, 1024 * 1024 * 50, 500 * 1024 * 1024, false);
    mgr.load_at_startup_cache(&cache2).unwrap();
    let e = cache2
        .load(&Bytes::from("persist"), Default::default())
        .unwrap()
        .unwrap();
    assert_eq!(e.value, Bytes::from("yes"));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_parse_save_cli_forms() {
    let rules = parse_save_rules("60,1 10,5").unwrap();
    assert_eq!(rules, vec![SaveRule::new(60, 1), SaveRule::new(10, 5)]);
    assert!(parse_save_rules("").unwrap().is_empty());
}

#[tokio::test]
async fn test_auto_save_policy_triggers_bgsave() {
    let dir = tmp_dir("autosave");
    let pconfig = PersistenceConfig {
        dir: dir.clone(),
        dbfilename: "auto.rdb".to_string(),
        appendonly: false,
        appendfilename: "appendonly.aof".to_string(),
        // 1 second + 2 changes
        save_rules: vec![SaveRule::new(1, 2)],
    };
    let mgr = PersistenceManager::new(pconfig).unwrap();
    mgr.ensure_dir().unwrap();

    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    cache
        .store(
            Bytes::from("k"),
            Bytes::from("v"),
            StoreOptions::default(),
        )
        .unwrap();

    // Two dirty writes, last save aged past the 1s threshold
    mgr.mark_dirty();
    mgr.mark_dirty();
    assert_eq!(mgr.dirty_changes(), 2);
    mgr.set_last_save_age(Duration::from_secs(2));

    assert!(mgr.maybe_auto_save_cache(&cache));
    // Wait for spawn_blocking BGSAVE
    for _ in 0..50 {
        if mgr.rdb_path().exists() && !mgr.bgsave_in_progress() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(mgr.rdb_path().exists(), "auto BGSAVE should write RDB");
    assert_eq!(mgr.dirty_changes(), 0, "dirty reset after successful save");
    assert!(mgr.last_save_unix() > 0);

    // Below threshold: no second save triggered immediately with dirty=0
    assert!(!mgr.maybe_auto_save_cache(&cache));

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_auto_save_disabled_with_empty_rules() {
    let dir = tmp_dir("autosave-off");
    let pconfig = PersistenceConfig {
        dir: dir.clone(),
        dbfilename: "off.rdb".to_string(),
        appendonly: false,
        appendfilename: "appendonly.aof".to_string(),
        save_rules: vec![],
    };
    let mgr = PersistenceManager::new(pconfig).unwrap();
    mgr.ensure_dir().unwrap();
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    mgr.mark_dirty();
    mgr.mark_dirty();
    mgr.set_last_save_age(Duration::from_secs(100));
    assert!(!mgr.maybe_auto_save_cache(&cache));
    assert!(!mgr.rdb_path().exists());
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn test_config_set_save_updates_rules() {
    use kore::commands::CommandHandler;
    use kore::config::Config;
    use kore::protocol::RespValue;

    let dir = tmp_dir("cfg-save");
    let pconfig = PersistenceConfig {
        dir: dir.clone(),
        dbfilename: "dump.rdb".to_string(),
        appendonly: false,
        appendfilename: "appendonly.aof".to_string(),
        save_rules: vec![SaveRule::new(900, 1)],
    };
    let mgr = PersistenceManager::new(pconfig).unwrap();
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let config = Config {
        host: "127.0.0.1".to_string(),
        port: 6379,
        threads: 1,
        shards: 8,
        maxmemory: 1024 * 1024 * 10,
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
        save: "900,1".to_string(),
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
    };
    let mut h = CommandHandler::with_persistence(cache, Arc::new(config), Some(mgr.clone()));

    let get = h
        .handle(RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"CONFIG"))),
            RespValue::BulkString(Some(Bytes::from_static(b"GET"))),
            RespValue::BulkString(Some(Bytes::from_static(b"save"))),
        ]))
        .await
        .unwrap();
    match get {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(
                arr[1],
                RespValue::BulkString(Some(Bytes::from("900 1")))
            );
        }
        other => panic!("expected array, got {:?}", other),
    }

    let set = h
        .handle(RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"CONFIG"))),
            RespValue::BulkString(Some(Bytes::from_static(b"SET"))),
            RespValue::BulkString(Some(Bytes::from_static(b"save"))),
            RespValue::BulkString(Some(Bytes::from("1 3"))),
        ]))
        .await
        .unwrap();
    assert_eq!(set, RespValue::ok());
    assert_eq!(mgr.save_rules(), vec![SaveRule::new(1, 3)]);

    // Disable
    let set = h
        .handle(RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"CONFIG"))),
            RespValue::BulkString(Some(Bytes::from_static(b"SET"))),
            RespValue::BulkString(Some(Bytes::from_static(b"save"))),
            RespValue::BulkString(Some(Bytes::from(""))),
        ]))
        .await
        .unwrap();
    assert_eq!(set, RespValue::ok());
    assert!(mgr.save_rules().is_empty());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_rdb_roundtrip_bytes() {
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    cache
        .store(
            Bytes::from("a"),
            Bytes::from("b"),
            StoreOptions::default(),
        )
        .unwrap();
    let bytes = rdb::save_to_bytes(&cache).unwrap();
    assert!(bytes.starts_with(b"KORDB\0"));

    let cache2 = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    rdb::load_bytes(&cache2, &bytes, true).unwrap();
    assert!(cache2.exists(&Bytes::from("a")));
}
