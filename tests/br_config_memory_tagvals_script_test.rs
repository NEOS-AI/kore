//! Batch BR: CONFIG GET ops params, MEMORY MALLOC-STATS, FT.TAGVALS, Lua COPY/MOVE.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::protocol::RespValue;
use kore::Databases;
use std::sync::Arc;

fn test_config() -> Config {
    Config {
        host: "127.0.0.1".to_string(),
        port: 6381,
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
        dir: "/tmp/kore-br-data".to_string(),
        dbfilename: "br.rdb".to_string(),
        appendonly: true,
        appendfilename: "br.aof".to_string(),
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
        unixsocket: "/tmp/kore-br.sock".to_string(),
            log_format: "text".to_string(),
    }
}

fn make_handler() -> CommandHandler {
    // Multi-DB so MOVE / SELECT work like a real server.
    let databases = Databases::create(16, 16, 1024 * 1024 * 100, 500 * 1024 * 1024, false, 0.75);
    CommandHandler::with_databases(databases, Arc::new(test_config()), None)
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

fn config_pairs(v: RespValue) -> Vec<(String, String)> {
    match v {
        RespValue::Array(a) => {
            assert!(a.len() % 2 == 0, "odd CONFIG GET array: {:?}", a);
            let mut out = Vec::with_capacity(a.len() / 2);
            let mut i = 0;
            while i + 1 < a.len() {
                out.push((
                    as_bulk_str(&a[i]).expect("key"),
                    as_bulk_str(&a[i + 1]).expect("val"),
                ));
                i += 2;
            }
            out
        }
        other => panic!("expected CONFIG array, got {:?}", other),
    }
}

fn config_map(v: RespValue) -> std::collections::HashMap<String, String> {
    config_pairs(v).into_iter().collect()
}

#[test]
fn br_config_get_ops_params() {
    let mut h = make_handler();

    let m = config_map(handle(&mut h, cmd(&["CONFIG", "GET", "port"])));
    assert_eq!(m.get("port").map(String::as_str), Some("6381"));

    let m = config_map(handle(&mut h, cmd(&["CONFIG", "GET", "bind"])));
    assert_eq!(m.get("bind").map(String::as_str), Some("127.0.0.1"));

    // alias host → bind
    let m = config_map(handle(&mut h, cmd(&["CONFIG", "GET", "host"])));
    assert_eq!(m.get("bind").map(String::as_str), Some("127.0.0.1"));

    let m = config_map(handle(&mut h, cmd(&["CONFIG", "GET", "dir"])));
    assert_eq!(m.get("dir").map(String::as_str), Some("/tmp/kore-br-data"));

    let m = config_map(handle(&mut h, cmd(&["CONFIG", "GET", "dbfilename"])));
    assert_eq!(m.get("dbfilename").map(String::as_str), Some("br.rdb"));

    let m = config_map(handle(&mut h, cmd(&["CONFIG", "GET", "appendonly"])));
    assert_eq!(m.get("appendonly").map(String::as_str), Some("yes"));

    let m = config_map(handle(&mut h, cmd(&["CONFIG", "GET", "appendfilename"])));
    assert_eq!(m.get("appendfilename").map(String::as_str), Some("br.aof"));

    let m = config_map(handle(&mut h, cmd(&["CONFIG", "GET", "unixsocket"])));
    assert_eq!(
        m.get("unixsocket").map(String::as_str),
        Some("/tmp/kore-br.sock")
    );

    let m = config_map(handle(&mut h, cmd(&["CONFIG", "GET", "cluster-enabled"])));
    assert_eq!(m.get("cluster-enabled").map(String::as_str), Some("no"));

    // multi-pattern
    let m = config_map(handle(
        &mut h,
        cmd(&["CONFIG", "GET", "port", "dir", "appendonly"]),
    ));
    assert_eq!(m.len(), 3);
    assert_eq!(m.get("port").map(String::as_str), Some("6381"));
    assert_eq!(m.get("dir").map(String::as_str), Some("/tmp/kore-br-data"));
    assert_eq!(m.get("appendonly").map(String::as_str), Some("yes"));

    // glob still includes new params
    let pairs = config_pairs(handle(&mut h, cmd(&["CONFIG", "GET", "*"])));
    let keys: std::collections::HashSet<_> = pairs.iter().map(|(k, _)| k.as_str()).collect();
    for need in [
        "port",
        "bind",
        "dir",
        "dbfilename",
        "appendonly",
        "appendfilename",
        "unixsocket",
        "cluster-enabled",
        "maxmemory",
    ] {
        assert!(keys.contains(need), "CONFIG GET * missing {}", need);
    }
}

#[test]
fn br_memory_malloc_stats() {
    let mut h = make_handler();
    let _ = handle(&mut h, cmd(&["SET", "m", "hello"]));

    match handle(&mut h, cmd(&["MEMORY", "MALLOC-STATS"])) {
        RespValue::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(
                s.contains("Allocated:") || s.contains("malloc"),
                "unexpected malloc-stats body: {}",
                s
            );
            assert!(s.contains("Keys:"), "missing Keys in {}", s);
        }
        other => panic!("expected bulk string, got {:?}", other),
    }

    // HELP mentions MALLOC-STATS
    match handle(&mut h, cmd(&["MEMORY", "HELP"])) {
        RespValue::Array(lines) => {
            let joined: String = lines
                .iter()
                .filter_map(as_bulk_str)
                .collect::<Vec<_>>()
                .join("\n")
                .to_ascii_uppercase();
            assert!(
                joined.contains("MALLOC-STATS"),
                "MEMORY HELP missing MALLOC-STATS: {}",
                joined
            );
        }
        other => panic!("expected HELP array, got {:?}", other),
    }
}

#[test]
fn br_ft_tagvals() {
    let mut h = make_handler();

    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "FT.CREATE",
                "tagidx",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "item:",
                "SCHEMA",
                "title",
                "TEXT",
                "tags",
                "TAG",
            ])
        ),
        RespValue::ok()
    );

    // empty index → empty array
    match handle(&mut h, cmd(&["FT.TAGVALS", "tagidx", "tags"])) {
        RespValue::Array(a) => assert!(a.is_empty()),
        other => panic!("expected empty array, got {:?}", other),
    }

    assert_eq!(
        handle(
            &mut h,
            cmd(&["HSET", "item:1", "title", "a", "tags", "rust,systems"])
        ),
        RespValue::Integer(2)
    );
    assert_eq!(
        handle(
            &mut h,
            cmd(&["HSET", "item:2", "title", "b", "tags", "python,systems"])
        ),
        RespValue::Integer(2)
    );

    match handle(&mut h, cmd(&["FT.TAGVALS", "tagidx", "tags"])) {
        RespValue::Array(a) => {
            let mut vals: Vec<String> = a.iter().filter_map(as_bulk_str).collect();
            vals.sort();
            assert_eq!(
                vals,
                vec![
                    "python".to_string(),
                    "rust".to_string(),
                    "systems".to_string()
                ]
            );
        }
        other => panic!("expected tagvals array, got {:?}", other),
    }

    // missing index
    match handle(&mut h, cmd(&["FT.TAGVALS", "nope", "tags"])) {
        RespValue::Error(e) => {
            let s = String::from_utf8_lossy(&e);
            assert!(s.contains("Unknown") || s.contains("not found") || s.contains("nope"), "{}", s);
        }
        other => panic!("expected error, got {:?}", other),
    }

    // non-TAG field
    match handle(&mut h, cmd(&["FT.TAGVALS", "tagidx", "title"])) {
        RespValue::Error(e) => {
            let s = String::from_utf8_lossy(&e);
            assert!(s.to_ascii_lowercase().contains("tag"), "{}", s);
        }
        other => panic!("expected non-TAG error, got {:?}", other),
    }

    // COMMAND catalog includes ft.tagvals
    match handle(&mut h, cmd(&["COMMAND", "INFO", "FT.TAGVALS"])) {
        RespValue::Array(a) => {
            assert!(!a.is_empty());
            match &a[0] {
                RespValue::Array(spec) if !spec.is_empty() => {
                    assert_eq!(as_bulk_str(&spec[0]).as_deref(), Some("ft.tagvals"));
                }
                RespValue::Null | RespValue::BulkString(None) => {
                    panic!("FT.TAGVALS not in COMMAND catalog");
                }
                other => panic!("unexpected COMMAND INFO entry: {:?}", other),
            }
        }
        other => panic!("expected COMMAND INFO array, got {:?}", other),
    }
}

#[test]
fn br_script_copy_move() {
    let mut h = make_handler();

    // COPY via redis.call
    let resp = handle(
        &mut h,
        cmd(&[
            "EVAL",
            "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('COPY', KEYS[1], KEYS[2])",
            "2",
            "src",
            "dst",
            "hello",
        ]),
    );
    assert_eq!(resp, RespValue::Integer(1));
    assert_eq!(
        handle(&mut h, cmd(&["GET", "dst"])),
        RespValue::BulkString(Some(Bytes::from_static(b"hello")))
    );
    assert_eq!(
        handle(&mut h, cmd(&["GET", "src"])),
        RespValue::BulkString(Some(Bytes::from_static(b"hello")))
    );

    // MOVE via redis.call (to DB 1)
    let resp = handle(
        &mut h,
        cmd(&[
            "EVAL",
            "return redis.call('MOVE', KEYS[1], ARGV[1])",
            "1",
            "src",
            "1",
        ]),
    );
    assert_eq!(resp, RespValue::Integer(1));
    assert_eq!(
        handle(&mut h, cmd(&["GET", "src"])),
        RespValue::BulkString(None)
    );
    assert_eq!(handle(&mut h, cmd(&["SELECT", "1"])), RespValue::ok());
    assert_eq!(
        handle(&mut h, cmd(&["GET", "src"])),
        RespValue::BulkString(Some(Bytes::from_static(b"hello")))
    );
}
