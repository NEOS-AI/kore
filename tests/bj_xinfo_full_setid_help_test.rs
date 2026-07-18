//! Batch BJ: XINFO STREAM FULL, XGROUP SETID ENTRIESREAD, COMMAND/CLIENT HELP.

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

/// Walk flat map [k,v,k,v...] and return value for key.
fn map_get<'a>(arr: &'a [RespValue], key: &str) -> Option<&'a RespValue> {
    let mut i = 0;
    while i + 1 < arr.len() {
        if as_bulk_str(&arr[i]).as_deref() == Some(key) {
            return Some(&arr[i + 1]);
        }
        i += 2;
    }
    None
}

#[test]
fn bj_xinfo_stream_full_and_count() {
    let mut h = make_handler(make_cache());
    handle(
        &mut h,
        cmd(&["XGROUP", "CREATE", "s", "g", "0", "MKSTREAM"]),
    );
    handle(&mut h, cmd(&["XADD", "s", "1-0", "f", "a"]));
    handle(&mut h, cmd(&["XADD", "s", "2-0", "f", "b"]));
    handle(&mut h, cmd(&["XADD", "s", "3-0", "f", "c"]));
    // Create PEL entry
    handle(
        &mut h,
        cmd(&["XREADGROUP", "GROUP", "g", "c1", "COUNT", "1", "STREAMS", "s", ">"]),
    );

    // Summary form still has first-entry / last-entry
    match handle(&mut h, cmd(&["XINFO", "STREAM", "s"])) {
        RespValue::Array(arr) => {
            assert!(map_get(&arr, "first-entry").is_some());
            assert!(map_get(&arr, "last-entry").is_some());
            assert!(map_get(&arr, "entries").is_none());
            match map_get(&arr, "groups") {
                Some(RespValue::Integer(1)) => {}
                other => panic!("summary groups count: {:?}", other),
            }
        }
        other => panic!("{:?}", other),
    }

    // FULL with COUNT 2 limits entries
    match handle(&mut h, cmd(&["XINFO", "STREAM", "s", "FULL", "COUNT", "2"])) {
        RespValue::Array(arr) => {
            assert!(map_get(&arr, "first-entry").is_none());
            let entries = match map_get(&arr, "entries") {
                Some(RespValue::Array(e)) => e,
                other => panic!("entries: {:?}", other),
            };
            assert_eq!(entries.len(), 2, "COUNT 2 should limit entries");
            // first id 1-0
            match &entries[0] {
                RespValue::Array(e) => {
                    assert_eq!(as_bulk_str(&e[0]).as_deref(), Some("1-0"));
                }
                other => panic!("{:?}", other),
            }
            let groups = match map_get(&arr, "groups") {
                Some(RespValue::Array(g)) => g,
                other => panic!("full groups: {:?}", other),
            };
            assert_eq!(groups.len(), 1);
            match &groups[0] {
                RespValue::Array(g) => {
                    assert_eq!(
                        as_bulk_str(map_get(g, "name").unwrap()).as_deref(),
                        Some("g")
                    );
                    match map_get(g, "pel-count") {
                        Some(RespValue::Integer(1)) => {}
                        other => panic!("pel-count: {:?}", other),
                    }
                    match map_get(g, "pending") {
                        Some(RespValue::Array(p)) => {
                            assert_eq!(p.len(), 1);
                            match &p[0] {
                                RespValue::Array(pe) => {
                                    assert_eq!(as_bulk_str(&pe[0]).as_deref(), Some("1-0"));
                                    assert_eq!(as_bulk_str(&pe[1]).as_deref(), Some("c1"));
                                }
                                other => panic!("{:?}", other),
                            }
                        }
                        other => panic!("pending: {:?}", other),
                    }
                    match map_get(g, "consumers") {
                        Some(RespValue::Array(cs)) => {
                            assert_eq!(cs.len(), 1);
                            match &cs[0] {
                                RespValue::Array(c) => {
                                    assert_eq!(
                                        as_bulk_str(map_get(c, "name").unwrap()).as_deref(),
                                        Some("c1")
                                    );
                                }
                                other => panic!("{:?}", other),
                            }
                        }
                        other => panic!("consumers: {:?}", other),
                    }
                }
                other => panic!("{:?}", other),
            }
        }
        other => panic!("{:?}", other),
    }

    // FULL default COUNT 10 includes all 3 entries
    match handle(&mut h, cmd(&["XINFO", "STREAM", "s", "FULL"])) {
        RespValue::Array(arr) => {
            let entries = match map_get(&arr, "entries") {
                Some(RespValue::Array(e)) => e,
                other => panic!("{:?}", other),
            };
            assert_eq!(entries.len(), 3);
        }
        other => panic!("{:?}", other),
    }

    // COUNT without FULL → syntax error
    match handle(&mut h, cmd(&["XINFO", "STREAM", "s", "COUNT", "1"])) {
        RespValue::Error(e) => {
            let s = String::from_utf8_lossy(&e);
            assert!(s.contains("syntax"), "{}", s);
        }
        other => panic!("expected syntax error: {:?}", other),
    }
}

#[test]
fn bj_xgroup_setid_entriesread() {
    let mut h = make_handler(make_cache());
    handle(
        &mut h,
        cmd(&["XGROUP", "CREATE", "s", "g", "0", "MKSTREAM"]),
    );
    handle(&mut h, cmd(&["XADD", "s", "1-0", "f", "a"]));
    handle(&mut h, cmd(&["XADD", "s", "2-0", "f", "b"]));

    // SETID advances last-delivered-id and sets entries-read
    let r = handle(
        &mut h,
        cmd(&["XGROUP", "SETID", "s", "g", "1-0", "ENTRIESREAD", "5"]),
    );
    assert_eq!(r, RespValue::ok());

    match handle(&mut h, cmd(&["XINFO", "GROUPS", "s"])) {
        RespValue::Array(groups) => {
            let row = match &groups[0] {
                RespValue::Array(r) => r,
                other => panic!("{:?}", other),
            };
            assert_eq!(
                as_bulk_str(map_get(row, "last-delivered-id").unwrap()).as_deref(),
                Some("1-0")
            );
            assert_eq!(
                map_get(row, "entries-read"),
                Some(&RespValue::Integer(5))
            );
        }
        other => panic!("{:?}", other),
    }

    // SETID without ENTRIESREAD leaves counter intact
    handle(&mut h, cmd(&["XGROUP", "SETID", "s", "g", "2-0"]));
    match handle(&mut h, cmd(&["XINFO", "GROUPS", "s"])) {
        RespValue::Array(groups) => {
            let row = match &groups[0] {
                RespValue::Array(r) => r,
                other => panic!("{:?}", other),
            };
            assert_eq!(
                as_bulk_str(map_get(row, "last-delivered-id").unwrap()).as_deref(),
                Some("2-0")
            );
            assert_eq!(
                map_get(row, "entries-read"),
                Some(&RespValue::Integer(5))
            );
        }
        other => panic!("{:?}", other),
    }
}

#[test]
fn bj_command_and_client_help() {
    let mut h = make_handler(make_cache());

    match handle(&mut h, cmd(&["COMMAND", "HELP"])) {
        RespValue::Array(lines) => {
            assert!(!lines.is_empty());
            let joined: String = lines
                .iter()
                .filter_map(|l| as_bulk_str(l))
                .collect::<Vec<_>>()
                .join("\n");
            assert!(joined.contains("GETKEYSANDFLAGS"), "{}", joined);
            assert!(joined.contains("DOCS"), "{}", joined);
            assert!(joined.contains("HELP"), "{}", joined);
        }
        other => panic!("{:?}", other),
    }

    match handle(&mut h, cmd(&["CLIENT", "HELP"])) {
        RespValue::Array(lines) => {
            assert!(!lines.is_empty());
            let joined: String = lines
                .iter()
                .filter_map(|l| as_bulk_str(l))
                .collect::<Vec<_>>()
                .join("\n");
            assert!(joined.contains("NO-EVICT"), "{}", joined);
            assert!(joined.contains("TRACKINGINFO"), "{}", joined);
            assert!(joined.contains("GETREDIR"), "{}", joined);
        }
        other => panic!("{:?}", other),
    }
}
