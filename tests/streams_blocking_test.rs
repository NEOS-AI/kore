//! Blocking XREAD / XREADGROUP (BLOCK ms)

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::protocol::RespValue;
use kore::Cache;
use std::sync::Arc;
use std::time::Duration;

fn make_cache() -> Arc<Cache> {
    Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false)
}

fn make_handler(cache: Arc<Cache>) -> CommandHandler {
    let config = Config {
        host: "127.0.0.1".to_string(),
        port: 6379,
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
        dir: "./data".to_string(),
        dbfilename: "dump.rdb".to_string(),
        appendonly: false,
        appendfilename: "appendonly.aof".to_string(),
        replicaof: String::new(),
        save: "900,1 300,10 60,10000".to_string(),
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
        tls_port: 0,
        tls_ca: String::new(),
        tls_auth_clients: false,
        tls_replication: false,
        aclfile: String::new(),
        cluster_enabled: false,
            cluster_replica_priority: 100,
            cluster_require_full_coverage: true,
            cluster_allow_reads_when_down: false,
            cluster_announce_ip: String::new(),
            cluster_announce_port: 0,
    unixsocket: String::new(),
            log_format: "text".to_string(),
    };
    CommandHandler::new(cache, Arc::new(config))
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

fn as_bulk_str(v: &RespValue) -> Option<String> {
    match v {
        RespValue::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    }
}

fn is_null_reply(v: &RespValue) -> bool {
    matches!(v, RespValue::BulkString(None) | RespValue::NullArray)
}

#[test]
fn xread_block_timeout_returns_null() {
    let cache = make_cache();
    let mut h = make_handler(cache);
    let start = std::time::Instant::now();
    let resp = handle(
        &mut h,
        cmd(&["XREAD", "BLOCK", "500", "STREAMS", "empty", "$"]),
    );
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(450),
        "expected ~timeout wait, got {:?}",
        elapsed
    );
    assert!(
        elapsed < Duration::from_millis(2000),
        "blocked too long: {:?}",
        elapsed
    );
    assert!(is_null_reply(&resp), "expected null on timeout, got {:?}", resp);
}

#[test]
fn xread_block_wakes_on_xadd() {
    let cache = make_cache();
    let cache2 = Arc::clone(&cache);
    let mut h_blocker = make_handler(Arc::clone(&cache));

    let blocker = std::thread::spawn(move || {
        handle(
            &mut h_blocker,
            cmd(&["XREAD", "BLOCK", "5000", "STREAMS", "s", "$"]),
        )
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut h_pusher = make_handler(cache2);
    let id = handle(
        &mut h_pusher,
        cmd(&["XADD", "s", "*", "f", "payload"]),
    );
    let id_s = as_bulk_str(&id).expect("xadd id");

    let resp = blocker.join().unwrap();
    match resp {
        RespValue::Array(streams) => {
            assert_eq!(streams.len(), 1);
            match &streams[0] {
                RespValue::Array(parts) => {
                    assert_eq!(as_bulk_str(&parts[0]).as_deref(), Some("s"));
                    match &parts[1] {
                        RespValue::Array(msgs) => {
                            assert_eq!(msgs.len(), 1);
                            match &msgs[0] {
                                RespValue::Array(entry) => {
                                    assert_eq!(as_bulk_str(&entry[0]).as_deref(), Some(id_s.as_str()));
                                }
                                other => panic!("entry: {:?}", other),
                            }
                        }
                        other => panic!("msgs: {:?}", other),
                    }
                }
                other => panic!("stream: {:?}", other),
            }
        }
        other => panic!("expected array after wake, got {:?}", other),
    }
}

#[test]
fn xread_block_dollar_fixed_at_start() {
    // Entries present before BLOCK starts must not be returned when id is `$`.
    // A concurrent XADD during the block should wake and return the new entry only.
    let cache = make_cache();
    let mut h = make_handler(Arc::clone(&cache));
    let old_id = handle(&mut h, cmd(&["XADD", "s", "*", "f", "old"]));
    let old_s = as_bulk_str(&old_id).expect("old id");

    let cache2 = Arc::clone(&cache);
    let mut h_blocker = make_handler(Arc::clone(&cache));
    let blocker = std::thread::spawn(move || {
        handle(
            &mut h_blocker,
            cmd(&["XREAD", "BLOCK", "5000", "STREAMS", "s", "$"]),
        )
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut h_pusher = make_handler(cache2);
    let new_id = handle(
        &mut h_pusher,
        cmd(&["XADD", "s", "*", "f", "new"]),
    );
    let new_s = as_bulk_str(&new_id).expect("new id");
    assert_ne!(old_s, new_s);

    let resp = blocker.join().unwrap();
    match resp {
        RespValue::Array(streams) => {
            assert_eq!(streams.len(), 1);
            match &streams[0] {
                RespValue::Array(parts) => match &parts[1] {
                    RespValue::Array(msgs) => {
                        assert_eq!(msgs.len(), 1, "only the post-block entry should be returned");
                        match &msgs[0] {
                            RespValue::Array(entry) => {
                                assert_eq!(as_bulk_str(&entry[0]).as_deref(), Some(new_s.as_str()));
                            }
                            other => panic!("{:?}", other),
                        }
                    }
                    other => panic!("{:?}", other),
                },
                other => panic!("{:?}", other),
            }
        }
        other => panic!("expected wake with new entry, got {:?}", other),
    }
}

#[test]
fn xread_block_zero_waits_until_data() {
    let cache = make_cache();
    let cache2 = Arc::clone(&cache);
    let mut h_blocker = make_handler(Arc::clone(&cache));

    let blocker = std::thread::spawn(move || {
        handle(
            &mut h_blocker,
            cmd(&["XREAD", "BLOCK", "0", "STREAMS", "forever", "$"]),
        )
    });

    std::thread::sleep(Duration::from_millis(100));
    let mut h_pusher = make_handler(cache2);
    handle(
        &mut h_pusher,
        cmd(&["XADD", "forever", "*", "f", "item"]),
    );

    let resp = blocker.join().unwrap();
    match resp {
        RespValue::Array(streams) => {
            assert_eq!(streams.len(), 1);
        }
        other => panic!("expected array, got {:?}", other),
    }
}

#[test]
fn xreadgroup_block_wakes_on_xadd() {
    let cache = make_cache();
    let mut h = make_handler(Arc::clone(&cache));
    // Create empty stream + group at $
    handle(
        &mut h,
        cmd(&["XGROUP", "CREATE", "jobs", "g", "$", "MKSTREAM"]),
    );

    let cache2 = Arc::clone(&cache);
    let mut h_blocker = make_handler(Arc::clone(&cache));
    let blocker = std::thread::spawn(move || {
        handle(
            &mut h_blocker,
            cmd(&[
                "XREADGROUP",
                "GROUP",
                "g",
                "c1",
                "BLOCK",
                "5000",
                "STREAMS",
                "jobs",
                ">",
            ]),
        )
    });

    std::thread::sleep(Duration::from_millis(100));
    let mut h_pusher = make_handler(cache2);
    let id = handle(
        &mut h_pusher,
        cmd(&["XADD", "jobs", "*", "task", "work"]),
    );
    let id_s = as_bulk_str(&id).expect("id");

    let resp = blocker.join().unwrap();
    match resp {
        RespValue::Array(streams) => {
            assert_eq!(streams.len(), 1);
            match &streams[0] {
                RespValue::Array(parts) => {
                    assert_eq!(as_bulk_str(&parts[0]).as_deref(), Some("jobs"));
                    match &parts[1] {
                        RespValue::Array(msgs) => {
                            assert_eq!(msgs.len(), 1);
                            match &msgs[0] {
                                RespValue::Array(entry) => {
                                    assert_eq!(as_bulk_str(&entry[0]).as_deref(), Some(id_s.as_str()));
                                }
                                other => panic!("{:?}", other),
                            }
                        }
                        other => panic!("{:?}", other),
                    }
                }
                other => panic!("{:?}", other),
            }
        }
        other => panic!("expected array after wake, got {:?}", other),
    }
}

#[test]
fn xread_without_block_still_immediate_null() {
    let cache = make_cache();
    let mut h = make_handler(cache);
    let start = std::time::Instant::now();
    let resp = handle(&mut h, cmd(&["XREAD", "STREAMS", "missing", "$"]));
    assert!(
        start.elapsed() < Duration::from_millis(200),
        "non-blocking XREAD must return immediately"
    );
    assert!(is_null_reply(&resp), "expected null, got {:?}", resp);
}
