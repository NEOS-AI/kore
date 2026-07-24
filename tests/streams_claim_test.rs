//! Batch AT: XCLAIM, XAUTOCLAIM, XPENDING range, XGROUP SETID, XSETID.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::protocol::RespValue;
use kore::Cache;
use std::sync::Arc;
use std::thread;
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

fn handle(handler: &mut CommandHandler, value: RespValue) -> RespValue {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async { handler.handle(value).await.unwrap() })
}

fn as_bulk_str(v: &RespValue) -> Option<String> {
    match v {
        RespValue::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    }
}

fn seed_group(h: &mut CommandHandler) -> Vec<String> {
    handle(h, cmd(&["XADD", "jobs", "1-0", "t", "a"]));
    handle(h, cmd(&["XADD", "jobs", "1-1", "t", "b"]));
    handle(h, cmd(&["XADD", "jobs", "1-2", "t", "c"]));
    assert_eq!(
        handle(h, cmd(&["XGROUP", "CREATE", "jobs", "g", "0"])),
        RespValue::ok()
    );
    let read = handle(
        h,
        cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c1",
            "COUNT",
            "10",
            "STREAMS",
            "jobs",
            ">",
        ]),
    );
    match read {
        RespValue::Array(streams) => match &streams[0] {
            RespValue::Array(parts) => match &parts[1] {
                RespValue::Array(msgs) => msgs
                    .iter()
                    .map(|m| match m {
                        RespValue::Array(e) => as_bulk_str(&e[0]).unwrap(),
                        _ => panic!(),
                    })
                    .collect(),
                _ => panic!(),
            },
            _ => panic!(),
        },
        other => panic!("{other:?}"),
    }
}

#[test]
fn test_xclaim_transfers_ownership() {
    let mut h = make_handler(make_cache());
    let ids = seed_group(&mut h);
    assert_eq!(ids.len(), 3);

    // min-idle huge → nothing claimed
    let resp = handle(
        &mut h,
        cmd(&["XCLAIM", "jobs", "g", "c2", "999999999", &ids[0]]),
    );
    assert_eq!(resp, RespValue::Array(vec![]));

    // min-idle 0 → claim
    let resp = handle(
        &mut h,
        cmd(&["XCLAIM", "jobs", "g", "c2", "0", &ids[0], &ids[1]]),
    );
    match resp {
        RespValue::Array(entries) => {
            assert_eq!(entries.len(), 2);
            assert_eq!(as_bulk_str(&match &entries[0] {
                RespValue::Array(e) => e[0].clone(),
                _ => panic!(),
            }), Some(ids[0].clone()));
        }
        other => panic!("{other:?}"),
    }

    // XPENDING range filtered by consumer c2
    let resp = handle(
        &mut h,
        cmd(&["XPENDING", "jobs", "g", "-", "+", "10", "c2"]),
    );
    match resp {
        RespValue::Array(rows) => {
            assert_eq!(rows.len(), 2);
            for row in rows {
                match row {
                    RespValue::Array(cols) => {
                        assert_eq!(as_bulk_str(&cols[1]).as_deref(), Some("c2"));
                        assert!(matches!(cols[2], RespValue::Integer(_)));
                        assert!(matches!(cols[3], RespValue::Integer(n) if n >= 1));
                    }
                    other => panic!("{other:?}"),
                }
            }
        }
        other => panic!("{other:?}"),
    }

    // JUSTID
    let resp = handle(
        &mut h,
        cmd(&["XCLAIM", "jobs", "g", "c3", "0", &ids[2], "JUSTID"]),
    );
    assert_eq!(
        resp,
        RespValue::Array(vec![bulk(&ids[2])])
    );
}

#[test]
fn test_xclaim_force() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["XADD", "s", "5-0", "f", "v"]));
    handle(&mut h, cmd(&["XGROUP", "CREATE", "s", "g", "$"]));
    // not in PEL without FORCE
    let resp = handle(&mut h, cmd(&["XCLAIM", "s", "g", "c", "0", "5-0"]));
    assert_eq!(resp, RespValue::Array(vec![]));
    // FORCE
    let resp = handle(
        &mut h,
        cmd(&["XCLAIM", "s", "g", "c", "0", "5-0", "FORCE"]),
    );
    match resp {
        RespValue::Array(entries) => assert_eq!(entries.len(), 1),
        other => panic!("{other:?}"),
    }
    match handle(&mut h, cmd(&["XPENDING", "s", "g"])) {
        RespValue::Array(summary) => assert_eq!(summary[0], RespValue::Integer(1)),
        other => panic!("{other:?}"),
    }
}

#[test]
fn test_xautoclaim() {
    let mut h = make_handler(make_cache());
    let ids = seed_group(&mut h);

    // Claim all idle (min-idle 0) to c2
    let resp = handle(
        &mut h,
        cmd(&[
            "XAUTOCLAIM",
            "jobs",
            "g",
            "c2",
            "0",
            "0-0",
            "COUNT",
            "2",
        ]),
    );
    match resp {
        RespValue::Array(parts) => {
            assert_eq!(parts.len(), 3);
            // next cursor should not be empty bulk
            assert!(as_bulk_str(&parts[0]).is_some());
            match &parts[1] {
                RespValue::Array(msgs) => assert_eq!(msgs.len(), 2),
                other => panic!("{other:?}"),
            }
            match &parts[2] {
                RespValue::Array(del) => assert!(del.is_empty()),
                other => panic!("{other:?}"),
            }
        }
        other => panic!("{other:?}"),
    }

    // JUSTID remainder
    let resp = handle(
        &mut h,
        cmd(&[
            "XAUTOCLAIM",
            "jobs",
            "g",
            "c2",
            "0",
            "0-0",
            "COUNT",
            "10",
            "JUSTID",
        ]),
    );
    match resp {
        RespValue::Array(parts) => match &parts[1] {
            RespValue::Array(ids_out) => {
                // may claim remaining or re-claim idle ones
                assert!(!ids_out.is_empty() || ids.len() == 2);
            }
            other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    }
}

#[test]
fn test_xgroup_setid_and_xsetid() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["XADD", "s", "1-0", "a", "1"]));
    handle(&mut h, cmd(&["XADD", "s", "2-0", "a", "2"]));
    handle(&mut h, cmd(&["XGROUP", "CREATE", "s", "g", "0"]));

    // SETID to last entry → no new messages on >
    assert_eq!(
        handle(&mut h, cmd(&["XGROUP", "SETID", "s", "g", "$"])),
        RespValue::ok()
    );
    assert_eq!(
        handle(
            &mut h,
            cmd(&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"])
        ),
        RespValue::null()
    );

    // SETID back to 0 → can read again
    assert_eq!(
        handle(&mut h, cmd(&["XGROUP", "SETID", "s", "g", "0-0"])),
        RespValue::ok()
    );
    match handle(
        &mut h,
        cmd(&["XREADGROUP", "GROUP", "g", "c", "COUNT", "1", "STREAMS", "s", ">"]),
    ) {
        RespValue::Array(streams) => assert_eq!(streams.len(), 1),
        other => panic!("{other:?}"),
    }

    // XSETID must be >= top id
    match handle(&mut h, cmd(&["XSETID", "s", "1-0"])) {
        RespValue::Error(e) => {
            assert!(String::from_utf8_lossy(&e).contains("XSETID") || String::from_utf8_lossy(&e).contains("smaller"));
        }
        other => panic!("expected error, got {other:?}"),
    }
    assert_eq!(
        handle(&mut h, cmd(&["XSETID", "s", "2-0"])),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["XSETID", "s", "99-0"])),
        RespValue::ok()
    );
}

#[test]
fn test_xpending_idle_filter() {
    let mut h = make_handler(make_cache());
    let ids = seed_group(&mut h);
    // Fresh pending: IDLE 10000 should filter all out
    let resp = handle(
        &mut h,
        cmd(&["XPENDING", "jobs", "g", "IDLE", "10000", "-", "+", "10"]),
    );
    assert_eq!(resp, RespValue::Array(vec![]));

    thread::sleep(Duration::from_millis(50));
    // IDLE 0 returns all
    let resp = handle(
        &mut h,
        cmd(&["XPENDING", "jobs", "g", "IDLE", "0", "-", "+", "10"]),
    );
    match resp {
        RespValue::Array(rows) => assert_eq!(rows.len(), ids.len()),
        other => panic!("{other:?}"),
    }
}
