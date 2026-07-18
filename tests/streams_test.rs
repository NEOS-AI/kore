//! Phase C P1: Redis Streams + basic consumer groups

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::protocol::RespValue;
use kore::Cache;
use std::sync::Arc;

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
        tls: false,
        tls_cert: String::new(),
        tls_key: String::new(),
        aclfile: String::new(),
        cluster_enabled: false,
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

#[test]
fn xadd_xlen_xrange() {
    let mut h = make_handler(make_cache());
    let id1 = handle(&mut h, cmd(&["XADD", "mystream", "*", "name", "Sara", "surname", "OConnor"]));
    let id1s = as_bulk_str(&id1).expect("id");
    assert!(id1s.contains('-'), "{id1s}");

    let id2 = handle(&mut h, cmd(&["XADD", "mystream", "*", "name", "John"]));
    assert!(as_bulk_str(&id2).is_some());

    assert_eq!(handle(&mut h, cmd(&["XLEN", "mystream"])), RespValue::Integer(2));
    assert_eq!(
        handle(&mut h, cmd(&["TYPE", "mystream"])),
        RespValue::SimpleString(Bytes::from_static(b"stream"))
    );

    let range = handle(&mut h, cmd(&["XRANGE", "mystream", "-", "+"]));
    match range {
        RespValue::Array(entries) => {
            assert_eq!(entries.len(), 2);
            // each entry: [id, [field, value, ...]]
            match &entries[0] {
                RespValue::Array(parts) => {
                    assert_eq!(parts.len(), 2);
                    assert!(as_bulk_str(&parts[0]).is_some());
                }
                other => panic!("expected entry array, got {other:?}"),
            }
        }
        other => panic!("expected xrange array, got {other:?}"),
    }

    // COUNT
    match handle(&mut h, cmd(&["XRANGE", "mystream", "-", "+", "COUNT", "1"])) {
        RespValue::Array(e) => assert_eq!(e.len(), 1),
        other => panic!("{other:?}"),
    }
}

#[test]
fn xadd_explicit_id_and_wrongtype() {
    let mut h = make_handler(make_cache());
    assert!(as_bulk_str(&handle(
        &mut h,
        cmd(&["XADD", "s", "1-0", "f", "v"])
    ))
    .as_deref()
        == Some("1-0"));
    assert!(as_bulk_str(&handle(
        &mut h,
        cmd(&["XADD", "s", "1-1", "f", "v2"])
    ))
    .as_deref()
        == Some("1-1"));
    // Equal or smaller ID rejected
    match handle(&mut h, cmd(&["XADD", "s", "1-0", "f", "x"])) {
        RespValue::Error(e) => {
            let s = String::from_utf8_lossy(&e);
            assert!(s.contains("equal or smaller") || s.contains("ERR"), "{s}");
        }
        other => panic!("{other:?}"),
    }

    handle(&mut h, cmd(&["SET", "str", "1"]));
    match handle(&mut h, cmd(&["XADD", "str", "*", "a", "b"])) {
        RespValue::Error(e) => {
            assert!(String::from_utf8_lossy(&e).contains("WRONGTYPE"));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn xdel_xtrim_xrevrange() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["XADD", "s", "1-0", "a", "1"]));
    handle(&mut h, cmd(&["XADD", "s", "1-1", "a", "2"]));
    handle(&mut h, cmd(&["XADD", "s", "1-2", "a", "3"]));

    match handle(&mut h, cmd(&["XREVRANGE", "s", "+", "-", "COUNT", "1"])) {
        RespValue::Array(e) => {
            assert_eq!(e.len(), 1);
            if let RespValue::Array(parts) = &e[0] {
                assert_eq!(as_bulk_str(&parts[0]).as_deref(), Some("1-2"));
            }
        }
        other => panic!("{other:?}"),
    }

    assert_eq!(
        handle(&mut h, cmd(&["XDEL", "s", "1-1"])),
        RespValue::Integer(1)
    );
    assert_eq!(handle(&mut h, cmd(&["XLEN", "s"])), RespValue::Integer(2));

    // Keep newest 1
    assert_eq!(
        handle(&mut h, cmd(&["XTRIM", "s", "MAXLEN", "1"])),
        RespValue::Integer(1)
    );
    assert_eq!(handle(&mut h, cmd(&["XLEN", "s"])), RespValue::Integer(1));
}

#[test]
fn xread_from_zero_and_dollar() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["XADD", "s", "5-0", "f", "v"]));

    // From beginning
    match handle(&mut h, cmd(&["XREAD", "STREAMS", "s", "0-0"])) {
        RespValue::Array(streams) => {
            assert_eq!(streams.len(), 1);
        }
        other => panic!("expected xread result, got {other:?}"),
    }

    // $ means only new — nothing yet
    assert_eq!(
        handle(&mut h, cmd(&["XREAD", "STREAMS", "s", "$"])),
        RespValue::null()
    );

    // After last
    assert_eq!(
        handle(&mut h, cmd(&["XREAD", "COUNT", "10", "STREAMS", "s", "5-0"])),
        RespValue::null()
    );
}

#[test]
fn consumer_group_xreadgroup_xack_xpending() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["XADD", "jobs", "1-0", "task", "a"]));
    handle(&mut h, cmd(&["XADD", "jobs", "1-1", "task", "b"]));

    assert_eq!(
        handle(
            &mut h,
            cmd(&["XGROUP", "CREATE", "jobs", "workers", "0"])
        ),
        RespValue::ok()
    );
    // Duplicate group
    match handle(
        &mut h,
        cmd(&["XGROUP", "CREATE", "jobs", "workers", "0"]),
    ) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("BUSYGROUP")),
        other => panic!("{other:?}"),
    }

    let read = handle(
        &mut h,
        cmd(&[
            "XREADGROUP",
            "GROUP",
            "workers",
            "c1",
            "COUNT",
            "10",
            "STREAMS",
            "jobs",
            ">",
        ]),
    );
    let ids: Vec<String> = match read {
        RespValue::Array(streams) => {
            assert_eq!(streams.len(), 1);
            match &streams[0] {
                RespValue::Array(parts) => {
                    assert_eq!(as_bulk_str(&parts[0]).as_deref(), Some("jobs"));
                    match &parts[1] {
                        RespValue::Array(msgs) => {
                            assert_eq!(msgs.len(), 2);
                            msgs.iter()
                                .map(|m| match m {
                                    RespValue::Array(e) => as_bulk_str(&e[0]).unwrap(),
                                    _ => panic!(),
                                })
                                .collect()
                        }
                        _ => panic!(),
                    }
                }
                _ => panic!(),
            }
        }
        other => panic!("{other:?}"),
    };

    // Pending summary
    match handle(&mut h, cmd(&["XPENDING", "jobs", "workers"])) {
        RespValue::Array(summary) => {
            assert_eq!(summary[0], RespValue::Integer(2));
        }
        other => panic!("{other:?}"),
    }

    assert_eq!(
        handle(
            &mut h,
            cmd(&["XACK", "jobs", "workers", &ids[0], &ids[1]])
        ),
        RespValue::Integer(2)
    );
    match handle(&mut h, cmd(&["XPENDING", "jobs", "workers"])) {
        RespValue::Array(summary) => {
            assert_eq!(summary[0], RespValue::Integer(0));
        }
        other => panic!("{other:?}"),
    }

    // No more new messages
    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "XREADGROUP",
                "GROUP",
                "workers",
                "c1",
                "STREAMS",
                "jobs",
                ">",
            ])
        ),
        RespValue::null()
    );
}

#[test]
fn xgroup_create_mkstream() {
    let mut h = make_handler(make_cache());
    assert_eq!(
        handle(
            &mut h,
            cmd(&["XGROUP", "CREATE", "empty", "g", "$", "MKSTREAM"])
        ),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["TYPE", "empty"])),
        RespValue::SimpleString(Bytes::from_static(b"stream"))
    );
    assert_eq!(handle(&mut h, cmd(&["XLEN", "empty"])), RespValue::Integer(0));
}

#[test]
fn del_and_keys_include_streams() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["XADD", "st", "*", "f", "1"]));
    assert_eq!(handle(&mut h, cmd(&["EXISTS", "st"])), RespValue::Integer(1));
    assert_eq!(handle(&mut h, cmd(&["DEL", "st"])), RespValue::Integer(1));
    assert_eq!(handle(&mut h, cmd(&["EXISTS", "st"])), RespValue::Integer(0));
}

#[test]
fn xadd_maxlen() {
    let mut h = make_handler(make_cache());
    for i in 0..5 {
        let id = format!("1-{i}");
        handle(
            &mut h,
            cmd(&["XADD", "s", "MAXLEN", "2", &id, "n", &i.to_string()]),
        );
    }
    assert_eq!(handle(&mut h, cmd(&["XLEN", "s"])), RespValue::Integer(2));
}
