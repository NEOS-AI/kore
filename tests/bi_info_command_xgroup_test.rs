//! Batch BI: INFO section filter, COMMAND GETKEYSANDFLAGS/DOCS, XGROUP CREATE ENTRIESREAD.

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
        tls: false,
        tls_cert: String::new(),
        tls_key: String::new(),
        aclfile: String::new(),
        cluster_enabled: false,
        unixsocket: String::new(),
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

#[test]
fn bi_info_section_filter() {
    let mut h = make_handler(make_cache());

    let full = as_bulk_str(&handle(&mut h, cmd(&["INFO"]))).expect("INFO bulk");
    assert!(full.contains("# Server"), "{}", full);
    assert!(full.contains("# Stats"), "{}", full);
    assert!(full.contains("# Memory"), "{}", full);
    assert!(full.contains("# Replication"), "{}", full);

    let server = as_bulk_str(&handle(&mut h, cmd(&["INFO", "server"]))).expect("server");
    assert!(server.contains("# Server"), "{}", server);
    assert!(server.contains("kore_version:"), "{}", server);
    assert!(
        !server.contains("# Stats"),
        "should not include Stats: {}",
        server
    );
    assert!(
        !server.contains("# Memory"),
        "should not include Memory: {}",
        server
    );

    let multi = as_bulk_str(&handle(&mut h, cmd(&["INFO", "memory", "stats"]))).expect("multi");
    assert!(multi.contains("# Memory"), "{}", multi);
    assert!(multi.contains("# Stats"), "{}", multi);
    assert!(
        !multi.contains("# Server"),
        "should not include Server: {}",
        multi
    );
    // Request order preserved.
    let mem_pos = multi.find("# Memory").unwrap();
    let stats_pos = multi.find("# Stats").unwrap();
    assert!(mem_pos < stats_pos, "{}", multi);

    let all = as_bulk_str(&handle(&mut h, cmd(&["INFO", "all"]))).expect("all");
    assert!(all.contains("# Server") && all.contains("# Keyspace"), "{}", all);

    let unknown = as_bulk_str(&handle(&mut h, cmd(&["INFO", "nosuch"]))).expect("unknown");
    assert!(
        unknown.is_empty() || !unknown.contains("# Server"),
        "unknown section should be empty/omit: {:?}",
        unknown
    );
}

#[test]
fn bi_command_getkeysandflags() {
    let mut h = make_handler(make_cache());

    // GET → single key with RO flags
    match handle(
        &mut h,
        cmd(&["COMMAND", "GETKEYSANDFLAGS", "GET", "mykey"]),
    ) {
        RespValue::Array(rows) => {
            assert_eq!(rows.len(), 1);
            match &rows[0] {
                RespValue::Array(pair) => {
                    assert_eq!(pair.len(), 2);
                    assert_eq!(as_bulk_str(&pair[0]).as_deref(), Some("mykey"));
                    match &pair[1] {
                        RespValue::Array(flags) => {
                            let flag_strs: Vec<String> = flags
                                .iter()
                                .filter_map(|f| as_bulk_str(f))
                                .collect();
                            assert!(flag_strs.iter().any(|f| f == "RO"), "{:?}", flag_strs);
                            assert!(flag_strs.iter().any(|f| f == "access"), "{:?}", flag_strs);
                        }
                        other => panic!("expected flags array: {:?}", other),
                    }
                }
                other => panic!("expected [key, flags]: {:?}", other),
            }
        }
        other => panic!("{:?}", other),
    }

    // SET → RW + update
    match handle(
        &mut h,
        cmd(&["COMMAND", "GETKEYSANDFLAGS", "SET", "k", "v"]),
    ) {
        RespValue::Array(rows) => {
            assert_eq!(rows.len(), 1);
            match &rows[0] {
                RespValue::Array(pair) => match &pair[1] {
                    RespValue::Array(flags) => {
                        let flag_strs: Vec<String> =
                            flags.iter().filter_map(|f| as_bulk_str(f)).collect();
                        assert!(flag_strs.iter().any(|f| f == "RW"), "{:?}", flag_strs);
                        assert!(flag_strs.iter().any(|f| f == "update"), "{:?}", flag_strs);
                    }
                    other => panic!("{:?}", other),
                },
                other => panic!("{:?}", other),
            }
        }
        other => panic!("{:?}", other),
    }

    // MSET multi-key
    match handle(
        &mut h,
        cmd(&[
            "COMMAND",
            "GETKEYSANDFLAGS",
            "MSET",
            "a",
            "1",
            "b",
            "2",
        ]),
    ) {
        RespValue::Array(rows) => {
            assert_eq!(rows.len(), 2);
            assert_eq!(
                as_bulk_str(match &rows[0] {
                    RespValue::Array(p) => &p[0],
                    _ => panic!(),
                })
                .as_deref(),
                Some("a")
            );
            assert_eq!(
                as_bulk_str(match &rows[1] {
                    RespValue::Array(p) => &p[0],
                    _ => panic!(),
                })
                .as_deref(),
                Some("b")
            );
        }
        other => panic!("{:?}", other),
    }

    // PING → empty
    match handle(&mut h, cmd(&["COMMAND", "GETKEYSANDFLAGS", "PING"])) {
        RespValue::Array(rows) => assert!(rows.is_empty()),
        other => panic!("{:?}", other),
    }
}

#[test]
fn bi_command_docs() {
    let mut h = make_handler(make_cache());

    // Named command
    match handle(&mut h, cmd(&["COMMAND", "DOCS", "get"])) {
        RespValue::Array(flat) => {
            assert!(flat.len() >= 2, "{:?}", flat);
            assert_eq!(as_bulk_str(&flat[0]).as_deref(), Some("get"));
            match &flat[1] {
                RespValue::Array(doc) => {
                    // Flat map: summary, ..., group, ..., arity, ...
                    let mut found_summary = false;
                    let mut found_arity = false;
                    let mut found_group = false;
                    let mut i = 0;
                    while i + 1 < doc.len() {
                        let k = as_bulk_str(&doc[i]).unwrap_or_default();
                        match k.as_str() {
                            "summary" => {
                                found_summary = true;
                                let s = as_bulk_str(&doc[i + 1]).unwrap_or_default();
                                assert!(s.contains("get"), "{}", s);
                            }
                            "arity" => {
                                found_arity = true;
                                assert_eq!(doc[i + 1], RespValue::Integer(2));
                            }
                            "group" => {
                                found_group = true;
                                assert_eq!(as_bulk_str(&doc[i + 1]).as_deref(), Some("string"));
                            }
                            _ => {}
                        }
                        i += 2;
                    }
                    assert!(found_summary && found_arity && found_group, "{:?}", doc);
                }
                other => panic!("expected docs map: {:?}", other),
            }
        }
        other => panic!("{:?}", other),
    }

    // Bare DOCS returns catalog (non-empty flat map)
    match handle(&mut h, cmd(&["COMMAND", "DOCS"])) {
        RespValue::Array(flat) => {
            assert!(flat.len() >= 2);
            assert!(flat.len() % 2 == 0, "RESP2 map must be even length");
        }
        other => panic!("{:?}", other),
    }

    // Unknown command name → empty
    match handle(&mut h, cmd(&["COMMAND", "DOCS", "nosuchcmd"])) {
        RespValue::Array(flat) => assert!(flat.is_empty()),
        other => panic!("{:?}", other),
    }
}

#[test]
fn bi_xgroup_create_entriesread() {
    let mut h = make_handler(make_cache());

    // Without ENTRIESREAD → null entries-read
    handle(
        &mut h,
        cmd(&["XGROUP", "CREATE", "s", "g0", "0", "MKSTREAM"]),
    );
    match handle(&mut h, cmd(&["XINFO", "GROUPS", "s"])) {
        RespValue::Array(groups) => {
            assert_eq!(groups.len(), 1);
            let row = match &groups[0] {
                RespValue::Array(r) => r,
                other => panic!("{:?}", other),
            };
            // Find entries-read field
            let mut i = 0;
            let mut found_null = false;
            while i + 1 < row.len() {
                if as_bulk_str(&row[i]).as_deref() == Some("entries-read") {
                    assert!(
                        matches!(row[i + 1], RespValue::Null | RespValue::BulkString(None)),
                        "expected null entries-read: {:?}",
                        row[i + 1]
                    );
                    found_null = true;
                    break;
                }
                i += 2;
            }
            assert!(found_null, "entries-read missing: {:?}", row);
        }
        other => panic!("{:?}", other),
    }

    // With ENTRIESREAD 42
    handle(
        &mut h,
        cmd(&[
            "XGROUP",
            "CREATE",
            "s",
            "g1",
            "0",
            "ENTRIESREAD",
            "42",
        ]),
    );
    match handle(&mut h, cmd(&["XINFO", "GROUPS", "s"])) {
        RespValue::Array(groups) => {
            assert_eq!(groups.len(), 2);
            // Find g1
            let mut saw = false;
            for g in &groups {
                let row = match g {
                    RespValue::Array(r) => r,
                    _ => continue,
                };
                let mut name = None;
                let mut er = None;
                let mut i = 0;
                while i + 1 < row.len() {
                    match as_bulk_str(&row[i]).as_deref() {
                        Some("name") => name = as_bulk_str(&row[i + 1]),
                        Some("entries-read") => er = Some(&row[i + 1]),
                        _ => {}
                    }
                    i += 2;
                }
                if name.as_deref() == Some("g1") {
                    assert_eq!(er, Some(&RespValue::Integer(42)), "{:?}", row);
                    saw = true;
                }
            }
            assert!(saw, "g1 not found: {:?}", groups);
        }
        other => panic!("{:?}", other),
    }

    // MKSTREAM + ENTRIESREAD either order
    handle(
        &mut h,
        cmd(&[
            "XGROUP",
            "CREATE",
            "s2",
            "g",
            "0",
            "ENTRIESREAD",
            "7",
            "MKSTREAM",
        ]),
    );
    match handle(&mut h, cmd(&["XINFO", "GROUPS", "s2"])) {
        RespValue::Array(groups) => {
            assert_eq!(groups.len(), 1);
            let row = match &groups[0] {
                RespValue::Array(r) => r,
                other => panic!("{:?}", other),
            };
            let mut i = 0;
            let mut er = None;
            while i + 1 < row.len() {
                if as_bulk_str(&row[i]).as_deref() == Some("entries-read") {
                    er = Some(&row[i + 1]);
                    break;
                }
                i += 2;
            }
            assert_eq!(er, Some(&RespValue::Integer(7)), "{:?}", row);
        }
        other => panic!("{:?}", other),
    }
}
