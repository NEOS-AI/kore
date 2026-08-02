//! Batch BG: EVAL_RO / EVALSHA_RO, CLIENT GETREDIR / TRACKINGINFO.

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

fn err_contains(v: &RespValue, needle: &str) -> bool {
    match v {
        RespValue::Error(e) => String::from_utf8_lossy(e).contains(needle),
        _ => false,
    }
}

#[test]
fn eval_ro_allows_reads() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["SET", "k", "v"]));
    let r = handle(
        &mut h,
        cmd(&[
            "EVAL_RO",
            "return redis.call('GET', KEYS[1])",
            "1",
            "k",
        ]),
    );
    assert_eq!(as_bulk_str(&r).as_deref(), Some("v"));
}

#[test]
fn eval_ro_rejects_writes() {
    let mut h = make_handler(make_cache());
    let r = handle(
        &mut h,
        cmd(&[
            "EVAL_RO",
            "return redis.call('SET', KEYS[1], 'x')",
            "1",
            "k",
        ]),
    );
    assert!(
        err_contains(&r, "Write commands are not allowed")
            || err_contains(&r, "read-only"),
        "got {r:?}"
    );
    // Key must remain unset
    assert_eq!(handle(&mut h, cmd(&["GET", "k"])), RespValue::null());
}

#[test]
fn eval_ro_pcall_write_returns_err_table() {
    let mut h = make_handler(make_cache());
    let r = handle(
        &mut h,
        cmd(&[
            "EVAL_RO",
            "local t = redis.pcall('SET', KEYS[1], 'x'); if type(t) == 'table' and t.err then return t.err else return 'ok' end",
            "1",
            "k",
        ]),
    );
    match r {
        RespValue::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(
                s.contains("Write commands") || s.contains("read-only"),
                "{s}"
            );
        }
        other => panic!("expected err bulk, got {other:?}"),
    }
}

#[test]
fn evalsha_ro_after_script_load() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["SET", "a", "1"]));
    let sha = match handle(
        &mut h,
        cmd(&["SCRIPT", "LOAD", "return redis.call('GET', KEYS[1])"]),
    ) {
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(&b).into_owned(),
        other => panic!("expected sha, got {other:?}"),
    };
    let r = handle(
        &mut h,
        cmd(&["EVALSHA_RO", &sha, "1", "a"]),
    );
    assert_eq!(as_bulk_str(&r).as_deref(), Some("1"));

    // Write via EVALSHA_RO must fail
    let sha_w = match handle(
        &mut h,
        cmd(&["SCRIPT", "LOAD", "return redis.call('DEL', KEYS[1])"]),
    ) {
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(&b).into_owned(),
        other => panic!("expected sha, got {other:?}"),
    };
    let r = handle(&mut h, cmd(&["EVALSHA_RO", &sha_w, "1", "a"]));
    assert!(err_contains(&r, "Write commands") || err_contains(&r, "read-only"), "{r:?}");
    assert_eq!(as_bulk_str(&handle(&mut h, cmd(&["GET", "a"]))).as_deref(), Some("1"));
}

#[test]
fn eval_ro_still_allows_regular_eval_writes() {
    let mut h = make_handler(make_cache());
    let r = handle(
        &mut h,
        cmd(&["EVAL", "return redis.call('SET', KEYS[1], 'z')", "1", "z"]),
    );
    assert_eq!(r, RespValue::ok());
    assert_eq!(as_bulk_str(&handle(&mut h, cmd(&["GET", "z"]))).as_deref(), Some("z"));
}

#[test]
fn command_getkeys_eval_ro() {
    let mut h = make_handler(make_cache());
    let r = handle(
        &mut h,
        cmd(&[
            "COMMAND",
            "GETKEYS",
            "EVAL_RO",
            "return 1",
            "2",
            "k1",
            "k2",
            "arg",
        ]),
    );
    match r {
        RespValue::Array(items) => {
            let keys: Vec<_> = items.iter().filter_map(as_bulk_str).collect();
            assert_eq!(keys, vec!["k1".to_string(), "k2".to_string()]);
        }
        other => panic!("expected array, got {other:?}"),
    }
}

#[test]
fn client_getredir_is_minus_one() {
    let mut h = make_handler(make_cache());
    assert_eq!(
        handle(&mut h, cmd(&["CLIENT", "GETREDIR"])),
        RespValue::Integer(-1)
    );
}

#[test]
fn client_trackinginfo_off() {
    let mut h = make_handler(make_cache());
    let r = handle(&mut h, cmd(&["CLIENT", "TRACKINGINFO"]));
    match r {
        RespValue::Array(items) => {
            assert!(items.len() >= 6);
            assert_eq!(as_bulk_str(&items[0]).as_deref(), Some("flags"));
            assert_eq!(as_bulk_str(&items[2]).as_deref(), Some("redirect"));
            assert_eq!(items[3], RespValue::Integer(-1));
            assert_eq!(as_bulk_str(&items[4]).as_deref(), Some("prefixes"));
        }
        other => panic!("expected array, got {other:?}"),
    }
}
