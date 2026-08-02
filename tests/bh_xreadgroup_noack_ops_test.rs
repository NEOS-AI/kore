//! Batch BH: XREADGROUP NOACK, CONFIG RESETSTAT, LATENCY, MODULE LIST.

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
            admin_bind: "127.0.0.1".to_string(),
            admin_http_token: String::new(),
            admin_http_user: String::new(),
            admin_http_password: String::new(),
            admin_tls: false,
            admin_tls_cert: String::new(),
            admin_tls_key: String::new(),
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

#[test]
fn xreadgroup_noack_skips_pel() {
    let mut h = make_handler(make_cache());
    handle(
        &mut h,
        cmd(&["XGROUP", "CREATE", "s", "g", "0", "MKSTREAM"]),
    );
    handle(&mut h, cmd(&["XADD", "s", "1-0", "f", "a"]));
    handle(&mut h, cmd(&["XADD", "s", "2-0", "f", "b"]));

    // NOACK delivery
    let r = handle(
        &mut h,
        cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c1",
            "NOACK",
            "STREAMS",
            "s",
            ">",
        ]),
    );
    match &r {
        RespValue::Array(streams) => assert_eq!(streams.len(), 1),
        other => panic!("expected messages, got {other:?}"),
    }

    // PEL should be empty
    let pending = handle(&mut h, cmd(&["XPENDING", "s", "g"]));
    match pending {
        RespValue::Array(summary) => {
            // Redis XPENDING summary: [total, min, max, consumers]
            assert_eq!(summary[0], RespValue::Integer(0));
        }
        other => panic!("expected xpending array, got {other:?}"),
    }

    // History re-read with > should not redeliver (cursor advanced)
    let r2 = handle(
        &mut h,
        cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c1",
            "STREAMS",
            "s",
            ">",
        ]),
    );
    assert_eq!(r2, RespValue::null());
}

#[test]
fn xreadgroup_without_noack_fills_pel() {
    let mut h = make_handler(make_cache());
    handle(
        &mut h,
        cmd(&["XGROUP", "CREATE", "s", "g", "0", "MKSTREAM"]),
    );
    handle(&mut h, cmd(&["XADD", "s", "1-0", "f", "a"]));
    handle(
        &mut h,
        cmd(&["XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">"]),
    );
    let pending = handle(&mut h, cmd(&["XPENDING", "s", "g"]));
    match pending {
        RespValue::Array(summary) => {
            assert_eq!(summary[0], RespValue::Integer(1));
        }
        other => panic!("expected xpending array, got {other:?}"),
    }
}

#[test]
fn config_resetstat_clears_hits() {
    let cache = make_cache();
    let mut h = make_handler(Arc::clone(&cache));
    handle(&mut h, cmd(&["SET", "k", "v"]));
    handle(&mut h, cmd(&["GET", "k"]));
    handle(&mut h, cmd(&["GET", "missing"]));
    assert!(cache.stats.hits.load(std::sync::atomic::Ordering::Relaxed) >= 1
        || cache.stats.misses.load(std::sync::atomic::Ordering::Relaxed) >= 1
        || cache.stats.cmd_get.load(std::sync::atomic::Ordering::Relaxed) >= 1
        || cache.stats.cmd_set.load(std::sync::atomic::Ordering::Relaxed) >= 1);

    // Bump counters directly so the test is robust if GET path doesn't touch hits.
    cache.stats.hits.store(5, std::sync::atomic::Ordering::Relaxed);
    cache.stats.misses.store(3, std::sync::atomic::Ordering::Relaxed);
    cache.stats.cmd_get.store(10, std::sync::atomic::Ordering::Relaxed);

    assert_eq!(
        handle(&mut h, cmd(&["CONFIG", "RESETSTAT"])),
        RespValue::ok()
    );
    assert_eq!(
        cache.stats.hits.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        cache.stats.misses.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        cache.stats.cmd_get.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[test]
fn latency_empty_and_help() {
    let mut h = make_handler(make_cache());
    assert_eq!(
        handle(&mut h, cmd(&["LATENCY", "LATEST"])),
        RespValue::Array(vec![])
    );
    assert_eq!(
        handle(&mut h, cmd(&["LATENCY", "HISTORY", "command"])),
        RespValue::Array(vec![])
    );
    assert_eq!(
        handle(&mut h, cmd(&["LATENCY", "RESET"])),
        RespValue::Integer(0)
    );
    match handle(&mut h, cmd(&["LATENCY", "DOCTOR"])) {
        RespValue::BulkString(Some(b)) => {
            assert!(!b.is_empty());
        }
        other => panic!("expected doctor bulk, got {other:?}"),
    }
    match handle(&mut h, cmd(&["LATENCY", "HELP"])) {
        RespValue::Array(a) => assert!(!a.is_empty()),
        other => panic!("expected help array, got {other:?}"),
    }
}

#[test]
fn module_list_empty() {
    let mut h = make_handler(make_cache());
    assert_eq!(
        handle(&mut h, cmd(&["MODULE", "LIST"])),
        RespValue::Array(vec![])
    );
    match handle(&mut h, cmd(&["MODULE", "HELP"])) {
        RespValue::Array(a) => assert!(!a.is_empty()),
        other => panic!("expected help, got {other:?}"),
    }
    match handle(&mut h, cmd(&["MODULE", "LOAD", "foo.so"])) {
        RespValue::Error(e) => {
            assert!(String::from_utf8_lossy(&e).contains("not supported"));
        }
        other => panic!("expected error, got {other:?}"),
    }
}

#[test]
fn command_catalog_lists_latency_module() {
    let mut h = make_handler(make_cache());
    for name in ["latency", "module"] {
        match handle(&mut h, cmd(&["COMMAND", "INFO", name])) {
            RespValue::Array(a) => match &a[0] {
                RespValue::Array(spec) => {
                    assert_eq!(as_bulk_str(&spec[0]).as_deref(), Some(name));
                }
                other => panic!("expected spec for {name}, got {other:?}"),
            },
            other => panic!("expected array for {name}, got {other:?}"),
        }
    }
}
