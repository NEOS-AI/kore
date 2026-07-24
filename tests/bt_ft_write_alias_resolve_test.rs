//! Batch BT: FT.* write classification (READONLY/AOF), alias→alias real-name storage,
//! DROPINDEX alias cleanup consistency, Lua SELECT connection side-effect.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::persistence::{PersistenceConfig, PersistenceManager, SaveRule};
use kore::protocol::RespValue;
use kore::Databases;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("kore-bt-{}-{}", label, nanos));
    let _ = std::fs::create_dir_all(&dir);
    dir
}

fn test_config(dir: &PathBuf) -> Config {
    Config {
        host: "127.0.0.1".to_string(),
        port: 6383,
        threads: 1,
        shards: 16,
        maxmemory: 1024 * 1024 * 100,
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
        dbfilename: "bt.rdb".to_string(),
        appendonly: true,
        appendfilename: "appendonly.aof".to_string(),
        replicaof: String::new(),
        save: "900,1".to_string(),
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
    }
}

fn make_persistence(dir: &PathBuf, appendonly: bool) -> Arc<PersistenceManager> {
    let pconfig = PersistenceConfig {
        dir: dir.clone(),
        dbfilename: "bt.rdb".to_string(),
        appendonly,
        appendfilename: "appendonly.aof".to_string(),
        save_rules: vec![SaveRule::new(900, 1)],
    };
    let mgr = PersistenceManager::new(pconfig).unwrap();
    mgr.ensure_dir().unwrap();
    mgr
}

fn make_handler_persist(dir: &PathBuf, mgr: Arc<PersistenceManager>) -> CommandHandler {
    let databases = Databases::create(16, 16, 1024 * 1024 * 100, 500 * 1024 * 1024, false, 0.75);
    CommandHandler::with_databases(databases, Arc::new(test_config(dir)), Some(mgr))
}

fn make_handler() -> CommandHandler {
    let dir = unique_dir("plain");
    let databases = Databases::create(16, 16, 1024 * 1024 * 100, 500 * 1024 * 1024, false, 0.75);
    CommandHandler::with_databases(databases, Arc::new(test_config(&dir)), None)
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

fn assert_readonly(resp: RespValue, label: &str) {
    match resp {
        RespValue::Error(e) => {
            let msg = String::from_utf8_lossy(&e);
            assert!(
                msg.contains("READONLY"),
                "{}: expected READONLY, got {}",
                label,
                msg
            );
        }
        other => panic!("{}: expected READONLY error, got {:?}", label, other),
    }
}

fn create_index(h: &mut CommandHandler, name: &str, prefix: &str) {
    assert_eq!(
        handle(
            h,
            cmd(&[
                "FT.CREATE",
                name,
                "ON",
                "HASH",
                "PREFIX",
                "1",
                prefix,
                "SCHEMA",
                "title",
                "TEXT",
            ])
        ),
        RespValue::ok()
    );
}

// ── P0: FT mutators are writes (READONLY on replica) ─────────────────────────

#[test]
fn bt_ft_mutators_readonly_on_replica() {
    let dir = unique_dir("readonly");
    let mgr = make_persistence(&dir, false);
    let mut h = make_handler_persist(&dir, mgr);

    // Become replica
    assert_eq!(
        handle(&mut h, cmd(&["REPLICAOF", "10.0.0.1", "6379"])),
        RespValue::ok()
    );

    for (label, parts) in [
        (
            "FT.CREATE",
            &["FT.CREATE", "idx", "SCHEMA", "t", "TEXT"][..],
        ),
        ("FT.ALIASADD", &["FT.ALIASADD", "a", "idx"][..]),
        ("FT.ALIASDEL", &["FT.ALIASDEL", "a"][..]),
        ("FT.ALIASUPDATE", &["FT.ALIASUPDATE", "a", "idx"][..]),
        ("FT.DROPINDEX", &["FT.DROPINDEX", "idx"][..]),
    ] {
        assert_readonly(handle(&mut h, cmd(parts)), label);
    }

    // Promote and confirm writes succeed
    assert_eq!(
        handle(&mut h, cmd(&["REPLICAOF", "NO", "ONE"])),
        RespValue::ok()
    );
    create_index(&mut h, "idx", "doc:");
    assert_eq!(
        handle(&mut h, cmd(&["FT.ALIASADD", "blog", "idx"])),
        RespValue::ok()
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// ── P0: FT mutators land in AOF ──────────────────────────────────────────────

#[test]
fn bt_ft_mutators_appended_to_aof() {
    let dir = unique_dir("aof");
    let aof_path = dir.join("appendonly.aof");
    let mgr = make_persistence(&dir, true);
    let mut h = make_handler_persist(&dir, mgr);

    create_index(&mut h, "articles", "doc:");
    assert_eq!(
        handle(&mut h, cmd(&["FT.ALIASADD", "blog", "articles"])),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["FT.ALIASUPDATE", "blog", "articles"])),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["FT.ALIASDEL", "blog"])),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["FT.DROPINDEX", "articles"])),
        RespValue::ok()
    );

    assert!(aof_path.exists(), "AOF file should exist after FT writes");
    let raw = std::fs::read(&aof_path).unwrap();
    let text = String::from_utf8_lossy(&raw).into_owned();
    for needle in [
        "FT.CREATE",
        "FT.ALIASADD",
        "FT.ALIASUPDATE",
        "FT.ALIASDEL",
        "FT.DROPINDEX",
    ] {
        assert!(
            text.contains(needle) || text.contains(&needle.to_ascii_lowercase()),
            "AOF should contain {}; got:\n{}",
            needle,
            text
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

// ── P1: alias → alias stores real index name; DROPINDEX cleans ───────────────

#[test]
fn bt_alias_to_alias_stores_real_name_and_drop_cleans() {
    let mut h = make_handler();
    create_index(&mut h, "articles", "doc:");
    create_index(&mut h, "posts", "post:");

    assert_eq!(
        handle(&mut h, cmd(&["HSET", "doc:1", "title", "hello rust"])),
        RespValue::Integer(1)
    );
    assert_eq!(
        handle(&mut h, cmd(&["HSET", "post:1", "title", "gamma"])),
        RespValue::Integer(1)
    );

    // Primary alias
    assert_eq!(
        handle(&mut h, cmd(&["FT.ALIASADD", "blog", "articles"])),
        RespValue::ok()
    );
    // Alias targeting another alias → must resolve to real index "articles"
    assert_eq!(
        handle(&mut h, cmd(&["FT.ALIASADD", "feed", "blog"])),
        RespValue::ok()
    );

    // Search via secondary alias works
    match handle(&mut h, cmd(&["FT.SEARCH", "feed", "rust"])) {
        RespValue::Array(a) => match a.first() {
            Some(RespValue::Integer(n)) => assert!(*n >= 1),
            other => panic!("FT.SEARCH total missing: {:?}", other),
        },
        other => panic!("expected FT.SEARCH array, got {:?}", other),
    }

    // INFO via secondary alias reports real index name
    match handle(&mut h, cmd(&["FT.INFO", "feed"])) {
        RespValue::Array(a) => {
            let mut found = false;
            let mut i = 0;
            while i + 1 < a.len() {
                if let (RespValue::BulkString(Some(k)), RespValue::BulkString(Some(v))) =
                    (&a[i], &a[i + 1])
                {
                    if k.as_ref() == b"index_name" {
                        assert_eq!(
                            String::from_utf8_lossy(v).as_ref(),
                            "articles",
                            "alias→alias must surface real index name"
                        );
                        found = true;
                        break;
                    }
                }
                i += 2;
            }
            assert!(found, "index_name missing in FT.INFO: {:?}", a);
        }
        other => panic!("expected FT.INFO array, got {:?}", other),
    }

    // ALIASUPDATE retarget via alias name stores real target "posts"
    assert_eq!(
        handle(&mut h, cmd(&["FT.ALIASUPDATE", "feed", "blog"])),
        RespValue::ok()
    );
    // retarget blog first so feed→blog would resolve to posts if blog moved…
    // Explicit: update feed to point at posts by using an alias of posts.
    assert_eq!(
        handle(&mut h, cmd(&["FT.ALIASADD", "stories", "posts"])),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["FT.ALIASUPDATE", "feed", "stories"])),
        RespValue::ok()
    );
    match handle(&mut h, cmd(&["FT.SEARCH", "feed", "gamma"])) {
        RespValue::Array(a) => match a.first() {
            Some(RespValue::Integer(n)) => assert!(*n >= 1, "feed should hit posts"),
            other => panic!("total missing: {:?}", other),
        },
        other => panic!("expected search array, got {:?}", other),
    }
    match handle(&mut h, cmd(&["FT.INFO", "feed"])) {
        RespValue::Array(a) => {
            let mut i = 0;
            while i + 1 < a.len() {
                if let (RespValue::BulkString(Some(k)), RespValue::BulkString(Some(v))) =
                    (&a[i], &a[i + 1])
                {
                    if k.as_ref() == b"index_name" {
                        assert_eq!(String::from_utf8_lossy(v).as_ref(), "posts");
                        break;
                    }
                }
                i += 2;
            }
        }
        other => panic!("expected FT.INFO, got {:?}", other),
    }

    // DROPINDEX by real name must clean aliases that stored the real name
    assert_eq!(
        handle(&mut h, cmd(&["FT.DROPINDEX", "posts"])),
        RespValue::ok()
    );
    match handle(&mut h, cmd(&["FT.SEARCH", "feed", "gamma"])) {
        RespValue::Error(_) => {}
        other => panic!("expected feed alias cleaned after DROPINDEX posts, got {:?}", other),
    }
    match handle(&mut h, cmd(&["FT.ALIASDEL", "stories"])) {
        RespValue::Error(_) => {}
        other => panic!("expected stories alias cleaned on drop, got {:?}", other),
    }
    match handle(&mut h, cmd(&["FT.ALIASDEL", "feed"])) {
        RespValue::Error(_) => {}
        other => panic!("expected feed alias cleaned on drop, got {:?}", other),
    }

    // DROPINDEX via alias also cleans remaining aliases on that real index
    assert_eq!(
        handle(&mut h, cmd(&["FT.DROPINDEX", "blog"])),
        RespValue::ok()
    );
    match handle(&mut h, cmd(&["FT.SEARCH", "articles", "rust"])) {
        RespValue::Error(_) => {}
        other => panic!("expected articles gone after drop via alias, got {:?}", other),
    }
    match handle(&mut h, cmd(&["FT.ALIASDEL", "blog"])) {
        RespValue::Error(_) => {}
        other => panic!("expected blog cleaned on drop, got {:?}", other),
    }
}

// ── P2: post-EVAL connection DB after Lua SELECT ─────────────────────────────

#[test]
fn bt_eval_select_persists_connection_db() {
    let mut h = make_handler();

    // Seed DB 0 and DB 1
    assert_eq!(
        handle(&mut h, cmd(&["SET", "k", "on0"])),
        RespValue::ok()
    );
    assert_eq!(handle(&mut h, cmd(&["SELECT", "1"])), RespValue::ok());
    assert_eq!(
        handle(&mut h, cmd(&["SET", "k", "on1"])),
        RespValue::ok()
    );
    assert_eq!(handle(&mut h, cmd(&["SELECT", "0"])), RespValue::ok());

    // Lua SELECT 1 must stick on the connection after EVAL returns (Redis-compatible).
    assert_eq!(
        handle(
            &mut h,
            cmd(&["EVAL", "return redis.call('SELECT', 1)", "0"])
        ),
        RespValue::ok()
    );

    // Connection should now be on DB 1 without an explicit outer SELECT.
    assert_eq!(
        handle(&mut h, cmd(&["GET", "k"])),
        RespValue::BulkString(Some(Bytes::from_static(b"on1")))
    );

    // Switch back via another script ending on DB 0
    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "EVAL",
                "redis.call('SELECT', 0); return redis.call('GET', 'k')",
                "0",
            ])
        ),
        RespValue::BulkString(Some(Bytes::from_static(b"on0")))
    );
    assert_eq!(
        handle(&mut h, cmd(&["GET", "k"])),
        RespValue::BulkString(Some(Bytes::from_static(b"on0")))
    );
}
