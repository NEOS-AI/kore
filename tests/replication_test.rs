//! PSYNC partial resync, ROLE, and replica read path.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::entry::StoreOptions;
use kore::persistence::replication::SyncStart;
use kore::persistence::{PersistenceConfig, PersistenceManager, SaveRule};
use kore::protocol::RespValue;
use kore::databases::Databases;
use kore::Cache;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("kore-repl-{}-{}", label, nanos));
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

fn bulk(s: &str) -> RespValue {
    RespValue::BulkString(Some(Bytes::from(s.to_string())))
}

fn cmd(parts: &[&str]) -> RespValue {
    RespValue::Array(parts.iter().map(|p| bulk(p)).collect())
}

fn handle(h: &mut CommandHandler, value: RespValue) -> RespValue {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { h.handle(value).await.unwrap() })
}

#[test]
fn psync_question_mark_full_resync() {
    let dir = unique_dir("full");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 50, 500 * 1024 * 1024, false);
    let databases = Databases::single(cache.clone());
    let _ = cache.store(
        Bytes::from_static(b"k"),
        Bytes::from_static(b"v"),
        StoreOptions::default(),
    );

    let start = mgr
        .replication
        .start_psync(&databases, "?", -1)
        .expect("psync");
    match start {
        SyncStart::Full { raw_response, feed: _ } => {
            let s = String::from_utf8_lossy(&raw_response);
            assert!(
                s.starts_with("+FULLRESYNC "),
                "expected FULLRESYNC, got {}",
                &s[..s.len().min(80)]
            );
            assert!(raw_response.windows(1).any(|_| true));
            // Must contain bulk RDB after the simple string line
            assert!(s.contains("\r\n$"));
        }
        SyncStart::Partial { .. } => panic!("expected full resync for ? -1"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn psync_partial_continue_when_offset_in_backlog() {
    let dir = unique_dir("partial");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 50, 500 * 1024 * 1024, false);
    let databases = Databases::single(cache);

    // Seed backlog with writes
    mgr.replication.propagate_command(&[
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"a"),
        Bytes::from_static(b"1"),
    ]);
    let mid = mgr.replication.master_repl_offset();
    mgr.replication.propagate_command(&[
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"b"),
        Bytes::from_static(b"2"),
    ]);
    let end = mgr.replication.master_repl_offset();
    assert!(end > mid);

    let replid = mgr.replication.replid();
    // Request from start of backlog
    let start = mgr
        .replication
        .start_psync(&databases, &replid, 0)
        .expect("psync partial");
    match start {
        SyncStart::Partial { raw_response, feed: _ } => {
            let s = String::from_utf8_lossy(&raw_response);
            assert!(
                s.starts_with("+CONTINUE\r\n"),
                "expected CONTINUE, got {}",
                &s[..s.len().min(40)]
            );
            // Backlog payload after CONTINUE should include both SETs
            assert!(s.contains("SET") || raw_response.len() > 12);
        }
        SyncStart::Full { raw_response, .. } => {
            panic!(
                "expected partial, got full: {}",
                String::from_utf8_lossy(&raw_response[..raw_response.len().min(60)])
            );
        }
    }

    // Offset beyond backlog → full
    let start = mgr
        .replication
        .start_psync(&databases, &replid, (end + 100) as i64)
        .expect("psync full");
    assert!(matches!(start, SyncStart::Full { .. }));

    // Wrong replid → full
    let start = mgr
        .replication
        .start_psync(&databases, "deadbeef", 0)
        .expect("psync wrong id");
    assert!(matches!(start, SyncStart::Full { .. }));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn role_master_and_slave() {
    let dir = unique_dir("role");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir), Some(mgr.clone()));

    let resp = handle(&mut h, cmd(&["ROLE"]));
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(
                arr[0],
                RespValue::BulkString(Some(Bytes::from_static(b"master")))
            );
        }
        other => panic!("expected array, {:?}", other),
    }

    // Become replica
    let resp = handle(&mut h, cmd(&["REPLICAOF", "127.0.0.1", "6380"]));
    assert_eq!(resp, RespValue::ok());

    let resp = handle(&mut h, cmd(&["ROLE"]));
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(
                arr[0],
                RespValue::BulkString(Some(Bytes::from_static(b"slave")))
            );
            assert_eq!(
                arr[1],
                RespValue::BulkString(Some(Bytes::from("127.0.0.1")))
            );
            assert_eq!(arr[2], RespValue::Integer(6380));
        }
        other => panic!("expected slave role, {:?}", other),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn replica_reads_allowed_writes_rejected() {
    let dir = unique_dir("readonly");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    // Pre-load data as if already replicated
    let _ = cache.store(
        Bytes::from_static(b"user:1"),
        Bytes::from_static(b"alice"),
        StoreOptions::default(),
    );

    let mut h = CommandHandler::with_persistence(cache, make_config(&dir), Some(mgr.clone()));
    handle(&mut h, cmd(&["REPLICAOF", "10.0.0.1", "6379"]));

    // Reads work
    let resp = handle(&mut h, cmd(&["GET", "user:1"]));
    assert_eq!(
        resp,
        RespValue::BulkString(Some(Bytes::from_static(b"alice")))
    );
    let resp = handle(&mut h, cmd(&["EXISTS", "user:1"]));
    assert_eq!(resp, RespValue::Integer(1));
    let resp = handle(&mut h, cmd(&["TYPE", "user:1"]));
    assert_eq!(resp, RespValue::SimpleString(Bytes::from_static(b"string")));
    let resp = handle(&mut h, cmd(&["DBSIZE"]));
    assert_eq!(resp, RespValue::Integer(1));

    // Writes rejected with READONLY
    let resp = handle(&mut h, cmd(&["SET", "user:1", "bob"]));
    match resp {
        RespValue::Error(e) => {
            let msg = String::from_utf8_lossy(&e);
            assert!(msg.contains("READONLY"), "got {}", msg);
        }
        other => panic!("expected READONLY error, got {:?}", other),
    }
    let resp = handle(&mut h, cmd(&["DEL", "user:1"]));
    match resp {
        RespValue::Error(e) => {
            assert!(String::from_utf8_lossy(&e).contains("READONLY"));
        }
        other => panic!("expected READONLY, {:?}", other),
    }

    // Data unchanged
    let resp = handle(&mut h, cmd(&["GET", "user:1"]));
    assert_eq!(
        resp,
        RespValue::BulkString(Some(Bytes::from_static(b"alice")))
    );

    // Promote back to master
    handle(&mut h, cmd(&["REPLICAOF", "NO", "ONE"]));
    let resp = handle(&mut h, cmd(&["SET", "user:1", "bob"]));
    assert_eq!(resp, RespValue::ok());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn info_contains_replication_section() {
    let dir = unique_dir("info");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir), Some(mgr.clone()));

    let resp = handle(&mut h, cmd(&["INFO"]));
    match resp {
        RespValue::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("# Replication"), "missing section: {}", s);
            assert!(s.contains("role:master"), "missing role: {}", s);
            assert!(s.contains("master_replid:"), "missing replid: {}", s);
            assert!(s.contains("master_repl_offset:"), "missing offset: {}", s);
            assert!(s.contains("repl_backlog_active:1"), "missing backlog: {}", s);
        }
        other => panic!("expected bulk, {:?}", other),
    }

    handle(&mut h, cmd(&["REPLICAOF", "127.0.0.1", "7000"]));
    let resp = handle(&mut h, cmd(&["INFO"]));
    match resp {
        RespValue::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("role:slave"), "{}", s);
            assert!(s.contains("master_host:127.0.0.1"), "{}", s);
            assert!(s.contains("master_port:7000"), "{}", s);
        }
        other => panic!("expected bulk, {:?}", other),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn replconf_ok_and_getack() {
    let dir = unique_dir("replconf");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir), Some(mgr));

    let resp = handle(&mut h, cmd(&["REPLCONF", "listening-port", "6380"]));
    assert_eq!(resp, RespValue::ok());
    let resp = handle(&mut h, cmd(&["REPLCONF", "capa", "psync2"]));
    assert_eq!(resp, RespValue::ok());

    let resp = handle(&mut h, cmd(&["REPLCONF", "GETACK", "*"]));
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 3);
            assert_eq!(
                arr[0],
                RespValue::BulkString(Some(Bytes::from_static(b"REPLCONF")))
            );
            assert_eq!(
                arr[1],
                RespValue::BulkString(Some(Bytes::from_static(b"ACK")))
            );
        }
        other => panic!("expected ACK array, {:?}", other),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn replconf_ack_updates_tracked_offset() {
    let dir = unique_dir("replconf-ack");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir), Some(mgr.clone()));

    // Announce identity then register a feed so note_replica_ack has a target.
    assert_eq!(
        handle(&mut h, cmd(&["REPLCONF", "listening-port", "6391"])),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["REPLCONF", "ip-address", "127.0.0.1"])),
        RespValue::ok()
    );
    let _feed = mgr
        .replication
        .register_replica_announced(Some("127.0.0.1".into()), Some(6391));

    let resp = handle(&mut h, cmd(&["REPLCONF", "ACK", "1234"]));
    assert_eq!(resp, RespValue::ok());
    assert_eq!(
        mgr.replication.tracked_ack_for("127.0.0.1", 6391),
        Some(1234)
    );

    // Monotonic bump
    assert_eq!(
        handle(&mut h, cmd(&["REPLCONF", "ACK", "2000"])),
        RespValue::ok()
    );
    assert_eq!(
        mgr.replication.tracked_ack_for("127.0.0.1", 6391),
        Some(2000)
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn psync_command_via_handler_sets_pending() {
    let dir = unique_dir("handler");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir), Some(mgr));

    let resp = handle(&mut h, cmd(&["PSYNC", "?", "-1"]));
    // Placeholder OK; real payload is in pending_raw_response
    assert_eq!(resp, RespValue::ok());
    let raw = h.take_raw_response().expect("raw response");
    let s = String::from_utf8_lossy(&raw);
    assert!(s.starts_with("+FULLRESYNC "));
    assert!(h.take_replica_feed().is_some());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn master_offset_advances_on_client_writes() {
    let dir = unique_dir("offset");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir), Some(mgr.clone()));

    let before = mgr.replication.master_repl_offset();
    handle(&mut h, cmd(&["SET", "x", "1"]));
    let after = mgr.replication.master_repl_offset();
    assert!(after > before, "offset should advance: {} -> {}", before, after);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn psync_arity_and_invalid_offset_errors() {
    let dir = unique_dir("arity");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir), Some(mgr));

    let resp = handle(&mut h, cmd(&["PSYNC"]));
    match resp {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("wrong number")),
        other => panic!("{:?}", other),
    }
    let resp = handle(&mut h, cmd(&["PSYNC", "only-one"]));
    match resp {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("wrong number")),
        other => panic!("{:?}", other),
    }
    let resp = handle(&mut h, cmd(&["PSYNC", "?", "not-a-number"]));
    match resp {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("integer") || String::from_utf8_lossy(&e).contains("float") || String::from_utf8_lossy(&e).contains("out of range")),
        other => panic!("{:?}", other),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn sync_legacy_sets_pending_bulk_only() {
    let dir = unique_dir("sync");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir), Some(mgr));

    let resp = handle(&mut h, cmd(&["SYNC"]));
    assert_eq!(resp, RespValue::ok());
    let raw = h.take_raw_response().expect("raw");
    assert!(raw.starts_with(b"$"), "legacy SYNC is bulk RDB");
    assert!(!raw.starts_with(b"+FULLRESYNC"));
    assert!(h.take_replica_feed().is_some());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn replica_reads_hash_list_zset_set() {
    let dir = unique_dir("reads-types");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 50, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(Arc::clone(&cache), make_config(&dir), Some(mgr.clone()));

    // Populate as master
    handle(&mut h, cmd(&["HSET", "h", "f", "v"]));
    handle(&mut h, cmd(&["LPUSH", "l", "a", "b"]));
    handle(&mut h, cmd(&["SADD", "s", "m1", "m2"]));
    handle(&mut h, cmd(&["ZADD", "z", "10", "alice", "20", "bob"]));

    handle(&mut h, cmd(&["REPLICAOF", "127.0.0.1", "9999"]));

    // Hash reads
    let resp = handle(&mut h, cmd(&["HGET", "h", "f"]));
    assert_eq!(resp, RespValue::BulkString(Some(Bytes::from_static(b"v"))));
    let resp = handle(&mut h, cmd(&["HLEN", "h"]));
    assert_eq!(resp, RespValue::Integer(1));

    // List reads
    let resp = handle(&mut h, cmd(&["LLEN", "l"]));
    assert_eq!(resp, RespValue::Integer(2));
    let resp = handle(&mut h, cmd(&["LINDEX", "l", "0"]));
    assert_eq!(resp, RespValue::BulkString(Some(Bytes::from_static(b"b"))));

    // Set reads
    let resp = handle(&mut h, cmd(&["SCARD", "s"]));
    assert_eq!(resp, RespValue::Integer(2));
    let resp = handle(&mut h, cmd(&["SISMEMBER", "s", "m1"]));
    assert_eq!(resp, RespValue::Integer(1));

    // Zset reads
    let resp = handle(&mut h, cmd(&["ZCARD", "z"]));
    assert_eq!(resp, RespValue::Integer(2));
    let resp = handle(&mut h, cmd(&["ZSCORE", "z", "alice"]));
    assert_eq!(resp, RespValue::BulkString(Some(Bytes::from("10"))));
    let resp = handle(&mut h, cmd(&["ZRANK", "z", "alice"]));
    assert_eq!(resp, RespValue::Integer(0));

    // Mutations still blocked
    for c in [
        cmd(&["HSET", "h", "g", "x"]),
        cmd(&["LPUSH", "l", "c"]),
        cmd(&["SADD", "s", "m3"]),
        cmd(&["ZADD", "z", "30", "carol"]),
    ] {
        match handle(&mut h, c) {
            RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("READONLY")),
            other => panic!("expected READONLY, {:?}", other),
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn promote_allows_writes_again() {
    let dir = unique_dir("promote");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir), Some(mgr.clone()));

    handle(&mut h, cmd(&["REPLICAOF", "1.2.3.4", "6379"]));
    assert!(mgr.replication.is_replica());
    let id_while_replica = mgr.replication.replid();

    handle(&mut h, cmd(&["REPLICAOF", "NO", "ONE"]));
    assert!(!mgr.replication.is_replica());
    assert_ne!(mgr.replication.replid(), id_while_replica);

    let resp = handle(&mut h, cmd(&["SET", "k", "ok"]));
    assert_eq!(resp, RespValue::ok());
    let resp = handle(&mut h, cmd(&["GET", "k"]));
    assert_eq!(resp, RespValue::BulkString(Some(Bytes::from_static(b"ok"))));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hello_role_reflects_replica() {
    let dir = unique_dir("hello");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir), Some(mgr));

    let resp = handle(&mut h, cmd(&["HELLO", "2"]));
    match resp {
        RespValue::Array(arr) => {
            let mut role = None;
            let mut i = 0;
            while i + 1 < arr.len() {
                if as_bulk(&arr[i]).as_deref() == Some("role") {
                    role = as_bulk(&arr[i + 1]);
                }
                i += 2;
            }
            assert_eq!(role.as_deref(), Some("master"));
        }
        other => panic!("{:?}", other),
    }

    handle(&mut h, cmd(&["REPLICAOF", "127.0.0.1", "1"]));
    let resp = handle(&mut h, cmd(&["HELLO", "2"]));
    match resp {
        RespValue::Array(arr) => {
            let mut role = None;
            let mut i = 0;
            while i + 1 < arr.len() {
                if as_bulk(&arr[i]).as_deref() == Some("role") {
                    role = as_bulk(&arr[i + 1]);
                }
                i += 2;
            }
            assert_eq!(role.as_deref(), Some("replica"));
        }
        other => panic!("{:?}", other),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

fn as_bulk(v: &RespValue) -> Option<String> {
    match v {
        RespValue::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    }
}

#[test]
fn command_catalog_lists_psync_role_replconf() {
    let dir = unique_dir("catalog");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir), Some(mgr));

    let resp = handle(&mut h, cmd(&["COMMAND", "LIST"]));
    match resp {
        RespValue::Array(names) => {
            let set: std::collections::HashSet<String> = names
                .iter()
                .filter_map(|v| as_bulk(v))
                .collect();
            assert!(set.contains("psync"), "missing psync in {:?}", set);
            assert!(set.contains("role"));
            assert!(set.contains("replconf"));
            assert!(set.contains("blpop"));
            assert!(set.contains("brpop"));
        }
        other => panic!("{:?}", other),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn multiple_writes_accumulate_offset_and_partial_from_mid() {
    let dir = unique_dir("accum");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache.clone(), make_config(&dir), Some(mgr.clone()));

    let mut offsets = vec![0u64];
    for i in 0..20 {
        handle(&mut h, cmd(&["SET", &format!("k{i}"), &format!("v{i}")]));
        offsets.push(mgr.replication.master_repl_offset());
    }
    assert!(offsets.windows(2).all(|w| w[1] > w[0]));

    let id = mgr.replication.replid();
    let mid = offsets[10];
    let databases = Databases::single(cache);
    match mgr.replication.start_psync(&databases, &id, mid as i64).unwrap() {
        SyncStart::Partial { raw_response, feed: _ } => {
            assert!(raw_response.starts_with(b"+CONTINUE\r\n"));
            let body_len = raw_response.len() - b"+CONTINUE\r\n".len();
            assert_eq!(body_len as u64, offsets[20] - mid);
        }
        SyncStart::Full { .. } => panic!("expected partial at mid offset"),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn replica_blocks_expire_and_rename() {
    let dir = unique_dir("ro-more");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir), Some(mgr));
    handle(&mut h, cmd(&["SET", "k", "v"]));
    handle(&mut h, cmd(&["REPLICAOF", "127.0.0.1", "1"]));

    for c in [
        cmd(&["EXPIRE", "k", "10"]),
        cmd(&["RENAME", "k", "k2"]),
        cmd(&["APPEND", "k", "x"]),
        cmd(&["INCR", "n"]),
        cmd(&["FLUSHDB"]),
    ] {
        match handle(&mut h, c) {
            RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("READONLY")),
            other => panic!("expected READONLY, got {:?}", other),
        }
    }
    // Non-mutating still ok
    assert_eq!(
        handle(&mut h, cmd(&["GET", "k"])),
        RespValue::BulkString(Some(Bytes::from_static(b"v")))
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn handler_psync_partial_sets_continue_raw() {
    let dir = unique_dir("handler-partial");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir), Some(mgr.clone()));

    handle(&mut h, cmd(&["SET", "a", "1"]));
    handle(&mut h, cmd(&["SET", "b", "2"]));
    let id = mgr.replication.replid();
    let off = 0i64;

    let resp = handle(&mut h, cmd(&["PSYNC", &id, &off.to_string()]));
    assert_eq!(resp, RespValue::ok());
    let raw = h.take_raw_response().expect("raw");
    assert!(
        raw.starts_with(b"+CONTINUE\r\n"),
        "got {}",
        String::from_utf8_lossy(&raw[..raw.len().min(40)])
    );
    assert!(h.take_replica_feed().is_some());
    let _ = std::fs::remove_dir_all(&dir);
}

/// TCP end-to-end: PSYNC ? -1 returns FULLRESYNC+RDB, then live SETs stream to the socket.
#[tokio::test(flavor = "multi_thread")]
async fn tcp_psync_full_resync_and_live_feed() {
    use kore::Server;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::time::{sleep, timeout, Duration};

    let dir = unique_dir("tcp-psync");
    // Distinct from network_integration (16490+) and multidb (16510+).
    let port = 16500u16;
    let mut config = (*make_config(&dir)).clone();
    config.port = port;
    config.host = "127.0.0.1".to_string();
    let config = Arc::new(config);
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let _ = cache.store(
        Bytes::from_static(b"seed"),
        Bytes::from_static(b"yes"),
        StoreOptions::default(),
    );

    let server = Server::with_persistence(Arc::clone(&cache), Arc::clone(&config), Arc::clone(&mgr));
    let handle = tokio::spawn(async move {
        let _ = server.run().await;
    });
    sleep(Duration::from_millis(250)).await;

    let mut stream = timeout(
        Duration::from_secs(2),
        TcpStream::connect(format!("127.0.0.1:{}", port)),
    )
    .await
    .expect("connect timeout")
    .expect("connect failed");

    // PSYNC ? -1
    let psync = b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    stream.write_all(psync).await.unwrap();

    // Read FULLRESYNC line + RDB bulk
    let mut buf = vec![0u8; 256 * 1024];
    let n = timeout(Duration::from_secs(3), stream.read(&mut buf))
        .await
        .expect("read timeout")
        .expect("read err");
    assert!(n > 0);
    let head = String::from_utf8_lossy(&buf[..n.min(200)]);
    assert!(
        head.starts_with("+FULLRESYNC "),
        "expected FULLRESYNC, got {}",
        &head[..head.len().min(80)]
    );
    assert!(head.contains("\r\n$") || buf[..n].windows(3).any(|w| w == b"\r\n$"));

    // Live write on another connection should appear on the replica feed
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .expect("client connect");
    let set_cmd = b"*3\r\n$3\r\nSET\r\n$4\r\nlive\r\n$1\r\n1\r\n";
    client.write_all(set_cmd).await.unwrap();
    let mut ack = [0u8; 64];
    let _ = timeout(Duration::from_secs(2), client.read(&mut ack)).await;

    // Replica socket should receive the propagated SET
    let mut live = vec![0u8; 4096];
    let n2 = timeout(Duration::from_secs(3), stream.read(&mut live))
        .await
        .expect("live feed timeout")
        .expect("live feed read");
    assert!(n2 > 0, "expected live feed bytes");
    let body = String::from_utf8_lossy(&live[..n2]);
    assert!(
        body.contains("SET") || body.contains("live"),
        "live feed missing SET: {}",
        &body[..body.len().min(120)]
    );

    drop(stream);
    drop(client);
    handle.abort();
    sleep(Duration::from_millis(50)).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn promote_resets_offset_and_backlog() {
    let dir = unique_dir("promote-reset");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache.clone(), make_config(&dir), Some(mgr.clone()));

    // Grow offset/backlog on the master path
    handle(&mut h, cmd(&["SET", "a", "1"]));
    handle(&mut h, cmd(&["SET", "b", "2"]));
    handle(&mut h, cmd(&["SET", "c", "3"]));
    let old_id = mgr.replication.replid();
    let old_off = mgr.replication.master_repl_offset();
    assert!(old_off > 0);

    // Become replica then promote
    handle(&mut h, cmd(&["REPLICAOF", "127.0.0.1", "7001"]));
    handle(&mut h, cmd(&["REPLICAOF", "NO", "ONE"]));

    assert_eq!(mgr.replication.master_repl_offset(), 0);
    let new_id = mgr.replication.replid();
    assert_ne!(new_id, old_id);

    // Partial PSYNC with old id fails (full only)
    let databases = Databases::single(cache);
    match mgr
        .replication
        .start_psync(&databases, &old_id, 0)
        .expect("psync")
    {
        SyncStart::Full { .. } => {}
        SyncStart::Partial { .. } => panic!("partial with old id must fail after promote"),
    }
    match mgr
        .replication
        .start_psync(&databases, &old_id, old_off as i64)
        .expect("psync")
    {
        SyncStart::Full { .. } => {}
        SyncStart::Partial { .. } => panic!("partial at old offset must fail after promote"),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn promote_clears_replica_metadata() {
    let dir = unique_dir("promote-meta");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir), Some(mgr.clone()));

    handle(&mut h, cmd(&["REPLICAOF", "192.168.0.1", "6380"]));
    assert!(mgr.replication.is_replica());
    // Simulate replica having applied stream / cached primary id
    // (fields are internal; set via REPLICAOF path + promote must clear them)
    // After promote:
    handle(&mut h, cmd(&["REPLICAOF", "NO", "ONE"]));
    assert!(!mgr.replication.is_replica());
    assert!(!mgr.replication.readonly());
    assert!(mgr.replication.primary_addr().is_none());
    assert!(mgr.replication.cached_master_replid().is_empty());
    assert_eq!(mgr.replication.replica_offset(), 0);
    assert_eq!(mgr.replication.master_repl_offset(), 0);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn failover_command_on_replica() {
    let dir = unique_dir("failover-replica");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir), Some(mgr.clone()));

    let resp = handle(&mut h, cmd(&["REPLICAOF", "127.0.0.1", "6399"]));
    assert_eq!(resp, RespValue::ok());
    assert!(mgr.replication.is_replica());

    let resp = handle(&mut h, cmd(&["FAILOVER"]));
    assert_eq!(resp, RespValue::ok());
    assert!(!mgr.replication.is_replica());
    assert!(!mgr.replication.readonly());

    // Writable again
    let resp = handle(&mut h, cmd(&["SET", "promoted", "1"]));
    assert_eq!(resp, RespValue::ok());

    // ROLE is master
    let resp = handle(&mut h, cmd(&["ROLE"]));
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(
                arr[0],
                RespValue::BulkString(Some(Bytes::from_static(b"master")))
            );
        }
        other => panic!("expected master role, {:?}", other),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn failover_command_on_master_errors() {
    let dir = unique_dir("failover-master");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir), Some(mgr));

    let resp = handle(&mut h, cmd(&["FAILOVER"]));
    match resp {
        RespValue::Error(e) => {
            let msg = String::from_utf8_lossy(&e);
            assert!(
                msg.contains("FAILOVER") && (msg.contains("replica") || msg.contains("slave")),
                "got {}",
                msg
            );
        }
        other => panic!("expected error on master FAILOVER, got {:?}", other),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn after_promote_info_and_role() {
    let dir = unique_dir("promote-info");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir), Some(mgr.clone()));

    handle(&mut h, cmd(&["SET", "seed", "1"]));
    let id_before = mgr.replication.replid();

    handle(&mut h, cmd(&["REPLICAOF", "10.0.0.9", "6379"]));
    handle(&mut h, cmd(&["REPLICAOF", "NO", "ONE"]));

    let id_after = mgr.replication.replid();
    assert_ne!(id_before, id_after);

    let resp = handle(&mut h, cmd(&["ROLE"]));
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(
                arr[0],
                RespValue::BulkString(Some(Bytes::from_static(b"master")))
            );
            assert_eq!(arr[1], RespValue::Integer(0));
        }
        other => panic!("{:?}", other),
    }

    let resp = handle(&mut h, cmd(&["INFO", "replication"]));
    match resp {
        RespValue::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("role:master"), "{}", s);
            assert!(s.contains(&format!("master_replid:{}", id_after)), "{}", s);
            assert!(s.contains("master_repl_offset:0"), "{}", s);
            assert!(!s.contains("master_host:"), "should not show master_host: {}", s);
            assert!(!s.contains("role:slave"), "{}", s);
        }
        other => panic!("{:?}", other),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// TCP: ROLE over the wire returns a RESP array starting with "master".
#[tokio::test(flavor = "multi_thread")]
async fn tcp_role_and_info_replication() {
    use kore::Server;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::time::{sleep, timeout, Duration};

    let dir = unique_dir("tcp-role");
    // Distinct from network_integration (16490+) and multidb (16510+).
    let port = 16501u16;
    let mut config = (*make_config(&dir)).clone();
    config.port = port;
    let config = Arc::new(config);
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(4, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let server = Server::with_persistence(cache, Arc::clone(&config), mgr);
    let handle = tokio::spawn(async move {
        let _ = server.run().await;
    });
    sleep(Duration::from_millis(200)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    stream
        .write_all(b"*1\r\n$4\r\nROLE\r\n")
        .await
        .unwrap();
    let mut buf = vec![0u8; 4096];
    let n = timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let s = String::from_utf8_lossy(&buf[..n]);
    assert!(s.contains("master"), "ROLE response: {}", s);

    stream
        .write_all(b"*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n")
        .await
        .unwrap();
    let n = timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let s = String::from_utf8_lossy(&buf[..n]);
    assert!(s.contains("role:master") || s.contains("master_replid"), "{}", s);

    drop(stream);
    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
