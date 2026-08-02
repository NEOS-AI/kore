//! Batch BO: FUNCTION/FCALL stubs, GETKEYS zset algebra, CONFIG REWRITE, FT catalog.

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

fn keys_of(v: RespValue) -> Vec<String> {
    match v {
        RespValue::Array(a) => a.iter().filter_map(as_bulk_str).collect(),
        other => panic!("expected key array, got {:?}", other),
    }
}

#[test]
fn bo_function_list_help_fcall() {
    let mut h = make_handler(make_cache());

    match handle(&mut h, cmd(&["FUNCTION", "LIST"])) {
        RespValue::Array(a) => assert!(a.is_empty()),
        other => panic!("{:?}", other),
    }

    match handle(&mut h, cmd(&["FUNCTION", "HELP"])) {
        RespValue::Array(lines) => {
            let joined: String = lines
                .iter()
                .filter_map(as_bulk_str)
                .collect::<Vec<_>>()
                .join("\n");
            assert!(joined.contains("LIST"), "{}", joined);
            assert!(joined.contains("STATS"), "{}", joined);
        }
        other => panic!("{:?}", other),
    }

    match handle(&mut h, cmd(&["FUNCTION", "LOAD", "#!lua name=lib\n"])) {
        RespValue::Error(e) => {
            assert!(
                String::from_utf8_lossy(&e).contains("not supported"),
                "{:?}",
                e
            );
        }
        other => panic!("{:?}", other),
    }

    match handle(&mut h, cmd(&["FCALL", "myfn", "1", "k", "arg"])) {
        RespValue::Error(e) => {
            let msg = String::from_utf8_lossy(&e);
            assert!(msg.contains("not found") && msg.contains("myfn"), "{}", msg);
        }
        other => panic!("{:?}", other),
    }

    match handle(&mut h, cmd(&["FCALL_RO", "rofn", "0"])) {
        RespValue::Error(e) => {
            assert!(
                String::from_utf8_lossy(&e).contains("not found"),
                "{:?}",
                e
            );
        }
        other => panic!("{:?}", other),
    }

    // Catalog lists function / fcall
    match handle(&mut h, cmd(&["COMMAND", "INFO", "function", "fcall", "ft.search"])) {
        RespValue::Array(entries) => {
            assert_eq!(entries.len(), 3);
            for e in &entries {
                assert!(
                    matches!(e, RespValue::Array(_)),
                    "expected catalog entry, got {:?}",
                    e
                );
            }
        }
        other => panic!("{:?}", other),
    }
}

#[test]
fn bo_getkeys_zset_algebra() {
    let mut h = make_handler(make_cache());

    assert_eq!(
        keys_of(handle(
            &mut h,
            cmd(&["COMMAND", "GETKEYS", "ZUNION", "2", "a", "b", "WITHSCORES"])
        )),
        vec!["a", "b"]
    );
    assert_eq!(
        keys_of(handle(
            &mut h,
            cmd(&[
                "COMMAND",
                "GETKEYS",
                "ZINTER",
                "2",
                "x",
                "y",
                "WEIGHTS",
                "1",
                "2"
            ])
        )),
        vec!["x", "y"]
    );
    assert_eq!(
        keys_of(handle(
            &mut h,
            cmd(&["COMMAND", "GETKEYS", "ZDIFF", "2", "p", "q"])
        )),
        vec!["p", "q"]
    );
    assert_eq!(
        keys_of(handle(
            &mut h,
            cmd(&["COMMAND", "GETKEYS", "ZINTERCARD", "2", "i1", "i2", "LIMIT", "5"])
        )),
        vec!["i1", "i2"]
    );
    assert_eq!(
        keys_of(handle(
            &mut h,
            cmd(&[
                "COMMAND",
                "GETKEYS",
                "ZUNIONSTORE",
                "out",
                "2",
                "s1",
                "s2",
                "AGGREGATE",
                "SUM"
            ])
        )),
        vec!["out", "s1", "s2"]
    );
    assert_eq!(
        keys_of(handle(
            &mut h,
            cmd(&["COMMAND", "GETKEYS", "ZDIFFSTORE", "dest", "1", "only"])
        )),
        vec!["dest", "only"]
    );
    assert_eq!(
        keys_of(handle(
            &mut h,
            cmd(&["COMMAND", "GETKEYS", "FCALL", "fn", "2", "k1", "k2", "arg"])
        )),
        vec!["k1", "k2"]
    );
}

#[test]
fn bo_config_rewrite_and_help() {
    let mut h = make_handler(make_cache());

    match handle(&mut h, cmd(&["CONFIG", "REWRITE"])) {
        RespValue::Error(e) => {
            let msg = String::from_utf8_lossy(&e);
            assert!(
                msg.contains("config file") || msg.contains("without a config"),
                "{}",
                msg
            );
        }
        other => panic!("{:?}", other),
    }

    match handle(&mut h, cmd(&["CONFIG", "HELP"])) {
        RespValue::Array(lines) => {
            let joined: String = lines
                .iter()
                .filter_map(as_bulk_str)
                .collect::<Vec<_>>()
                .join("\n");
            assert!(joined.contains("REWRITE"), "{}", joined);
            assert!(joined.contains("GET"), "{}", joined);
        }
        other => panic!("{:?}", other),
    }
}
