//! Batch BM: CONFIG GET glob/multi, COMMAND LIST FILTERBY, PUBSUB/XGROUP HELP.

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

/// Flatten RESP2 CONFIG GET array into (key, value) pairs.
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

#[test]
fn bm_config_get_glob_all() {
    let mut h = make_handler(make_cache());
    let pairs = config_pairs(handle(&mut h, cmd(&["CONFIG", "GET", "*"])));
    assert!(pairs.len() >= 8, "expected many params, got {:?}", pairs);
    let keys: Vec<&str> = pairs.iter().map(|(k, _)| k.as_str()).collect();
    assert!(keys.contains(&"maxmemory"), "{:?}", keys);
    assert!(keys.contains(&"maxentrysize"), "{:?}", keys);
    assert!(keys.contains(&"maxmemory-policy"), "{:?}", keys);
    assert!(keys.contains(&"databases"), "{:?}", keys);
}

#[test]
fn bm_config_get_pattern_and_multi() {
    let mut h = make_handler(make_cache());

    let pairs = config_pairs(handle(&mut h, cmd(&["CONFIG", "GET", "maxmemory*"])));
    let keys: Vec<&str> = pairs.iter().map(|(k, _)| k.as_str()).collect();
    assert!(keys.contains(&"maxmemory"), "{:?}", keys);
    assert!(keys.contains(&"maxmemory-policy"), "{:?}", keys);
    assert!(!keys.contains(&"maxentrysize"), "{:?}", keys);

    // Multi-pattern: union, no duplicates
    let pairs = config_pairs(handle(
        &mut h,
        cmd(&["CONFIG", "GET", "maxmemory", "databases", "maxmemory"]),
    ));
    assert_eq!(pairs.len(), 2, "{:?}", pairs);
    let keys: Vec<&str> = pairs.iter().map(|(k, _)| k.as_str()).collect();
    assert!(keys.contains(&"maxmemory"), "{:?}", keys);
    assert!(keys.contains(&"databases"), "{:?}", keys);

    // Alias still resolves to canonical name
    let pairs = config_pairs(handle(
        &mut h,
        cmd(&["CONFIG", "GET", "max-entry-size"]),
    ));
    assert_eq!(pairs.len(), 1);
    assert_eq!(pairs[0].0, "maxentrysize");

    // No match → empty
    let pairs = config_pairs(handle(&mut h, cmd(&["CONFIG", "GET", "nosuchparam"])));
    assert!(pairs.is_empty(), "{:?}", pairs);
}

#[test]
fn bm_command_list_filterby_pattern() {
    let mut h = make_handler(make_cache());

    // Bare LIST still works
    match handle(&mut h, cmd(&["COMMAND", "LIST"])) {
        RespValue::Array(names) => {
            assert!(names.len() > 50, "catalog too small: {}", names.len());
        }
        other => panic!("{:?}", other),
    }

    match handle(
        &mut h,
        cmd(&["COMMAND", "LIST", "FILTERBY", "PATTERN", "get*"]),
    ) {
        RespValue::Array(names) => {
            let strs: Vec<String> = names.iter().filter_map(as_bulk_str).collect();
            assert!(strs.iter().any(|n| n == "get"), "{:?}", strs);
            assert!(strs.iter().any(|n| n.starts_with("get")), "{:?}", strs);
            assert!(
                strs.iter().all(|n| n.starts_with("get")),
                "non-get*: {:?}",
                strs
            );
        }
        other => panic!("{:?}", other),
    }

    // MODULE filter → empty (no loadable modules)
    match handle(
        &mut h,
        cmd(&["COMMAND", "LIST", "FILTERBY", "MODULE", "search"]),
    ) {
        RespValue::Array(names) => assert!(names.is_empty()),
        other => panic!("{:?}", other),
    }
}

#[test]
fn bm_command_list_filterby_aclcat() {
    let mut h = make_handler(make_cache());

    match handle(
        &mut h,
        cmd(&["COMMAND", "LIST", "FILTERBY", "ACLCAT", "write"]),
    ) {
        RespValue::Array(names) => {
            let strs: Vec<String> = names.iter().filter_map(as_bulk_str).collect();
            assert!(strs.iter().any(|n| n == "set"), "{:?}", strs);
            assert!(strs.iter().any(|n| n == "del"), "{:?}", strs);
            // GET is readonly
            assert!(!strs.iter().any(|n| n == "get"), "{:?}", strs);
        }
        other => panic!("{:?}", other),
    }

    match handle(
        &mut h,
        cmd(&["COMMAND", "LIST", "FILTERBY", "ACLCAT", "pubsub"]),
    ) {
        RespValue::Array(names) => {
            let strs: Vec<String> = names.iter().filter_map(as_bulk_str).collect();
            assert!(
                strs.iter().any(|n| n == "publish" || n == "subscribe"),
                "{:?}",
                strs
            );
        }
        other => panic!("{:?}", other),
    }

    // Bad syntax
    match handle(&mut h, cmd(&["COMMAND", "LIST", "FILTERBY"])) {
        RespValue::Error(e) => {
            assert!(String::from_utf8_lossy(&e).contains("syntax"), "{:?}", e);
        }
        other => panic!("{:?}", other),
    }
}

#[test]
fn bm_pubsub_and_xgroup_help() {
    let mut h = make_handler(make_cache());

    match handle(&mut h, cmd(&["PUBSUB", "HELP"])) {
        RespValue::Array(lines) => {
            let joined: String = lines
                .iter()
                .filter_map(as_bulk_str)
                .collect::<Vec<_>>()
                .join("\n");
            assert!(joined.contains("CHANNELS"), "{}", joined);
            assert!(joined.contains("NUMSUB"), "{}", joined);
            assert!(joined.contains("NUMPAT"), "{}", joined);
            assert!(joined.contains("SHARDCHANNELS"), "{}", joined);
        }
        other => panic!("{:?}", other),
    }

    match handle(&mut h, cmd(&["XGROUP", "HELP"])) {
        RespValue::Array(lines) => {
            let joined: String = lines
                .iter()
                .filter_map(as_bulk_str)
                .collect::<Vec<_>>()
                .join("\n");
            assert!(joined.contains("CREATE"), "{}", joined);
            assert!(joined.contains("SETID"), "{}", joined);
            assert!(joined.contains("DESTROY"), "{}", joined);
            assert!(joined.contains("ENTRIESREAD"), "{}", joined);
        }
        other => panic!("{:?}", other),
    }
}
