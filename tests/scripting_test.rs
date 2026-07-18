//! Batch Z: EVAL / EVALSHA / SCRIPT + redis.call

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::protocol::RespValue;
use kore::scripting::script_sha1;
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

fn err_str(v: &RespValue) -> String {
    match v {
        RespValue::Error(e) => String::from_utf8_lossy(e).into_owned(),
        other => panic!("expected error, got {:?}", other),
    }
}

#[test]
fn eval_return_integer_and_string() {
    let mut h = make_handler(make_cache());
    assert_eq!(
        handle(&mut h, cmd(&["EVAL", "return 42", "0"])),
        RespValue::Integer(42)
    );
    assert_eq!(
        as_bulk_str(&handle(
            &mut h,
            cmd(&["EVAL", "return 'hello'", "0"])
        ))
        .as_deref(),
        Some("hello")
    );
    assert_eq!(
        handle(&mut h, cmd(&["EVAL", "return false", "0"])),
        RespValue::null()
    );
    assert_eq!(
        handle(&mut h, cmd(&["EVAL", "return true", "0"])),
        RespValue::Integer(1)
    );
}

#[test]
fn eval_keys_and_argv() {
    let mut h = make_handler(make_cache());
    let r = handle(
        &mut h,
        cmd(&[
            "EVAL",
            "return {KEYS[1], KEYS[2], ARGV[1]}",
            "2",
            "ka",
            "kb",
            "va",
        ]),
    );
    match r {
        RespValue::Array(a) => {
            assert_eq!(a.len(), 3);
            assert_eq!(as_bulk_str(&a[0]).as_deref(), Some("ka"));
            assert_eq!(as_bulk_str(&a[1]).as_deref(), Some("kb"));
            assert_eq!(as_bulk_str(&a[2]).as_deref(), Some("va"));
        }
        other => panic!("expected array, got {:?}", other),
    }
}

#[test]
fn eval_redis_call_get_set() {
    let mut h = make_handler(make_cache());
    let r = handle(
        &mut h,
        cmd(&[
            "EVAL",
            "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])",
            "1",
            "user:1",
            "alice",
        ]),
    );
    assert_eq!(as_bulk_str(&r).as_deref(), Some("alice"));
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GET", "user:1"]))).as_deref(),
        Some("alice")
    );
}

#[test]
fn eval_redis_call_incr_hset() {
    let mut h = make_handler(make_cache());
    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "EVAL",
                "return redis.call('INCR', KEYS[1])",
                "1",
                "counter",
            ])
        ),
        RespValue::Integer(1)
    );
    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "EVAL",
                "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]); return redis.call('HGET', KEYS[1], ARGV[1])",
                "1",
                "hash1",
                "f",
                "v",
            ])
        )
        .pipe(|r| as_bulk_str(&r)),
        Some("v".to_string())
    );
}

trait Pipe: Sized {
    fn pipe<F, R>(self, f: F) -> R
    where
        F: FnOnce(Self) -> R,
    {
        f(self)
    }
}
impl<T> Pipe for T {}

#[test]
fn eval_redis_call_missing_key_is_false() {
    let mut h = make_handler(make_cache());
    // redis.call GET miss → false in Lua → null bulk when returned
    assert_eq!(
        handle(
            &mut h,
            cmd(&["EVAL", "return redis.call('GET', KEYS[1])", "1", "nope"])
        ),
        RespValue::null()
    );
}

#[test]
fn eval_redis_call_error_propagates() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["HSET", "h", "f", "1"]));
    let e = err_str(&handle(
        &mut h,
        cmd(&["EVAL", "return redis.call('GET', KEYS[1])", "1", "h"]),
    ));
    assert!(
        e.contains("WRONGTYPE") || e.contains("wrong kind"),
        "got {}",
        e
    );
}

#[test]
fn eval_redis_pcall_returns_err_table() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["HSET", "h", "f", "1"]));
    // pcall should not abort the script
    let r = handle(
        &mut h,
        cmd(&[
            "EVAL",
            "local r = redis.pcall('GET', KEYS[1]); if r['err'] then return 'caught' else return r end",
            "1",
            "h",
        ]),
    );
    assert_eq!(as_bulk_str(&r).as_deref(), Some("caught"));
}

#[test]
fn eval_disallows_nested_eval() {
    let mut h = make_handler(make_cache());
    let e = err_str(&handle(
        &mut h,
        cmd(&["EVAL", "return redis.call('EVAL', 'return 1', 0)", "0"]),
    ));
    assert!(
        e.contains("not allowed") || e.contains("ERR"),
        "got {}",
        e
    );
}

#[test]
fn script_load_exists_evalsha_flush() {
    let mut h = make_handler(make_cache());
    let body = "return redis.call('GET', KEYS[1])";
    let expected_sha = script_sha1(body);

    let loaded = handle(&mut h, cmd(&["SCRIPT", "LOAD", body]));
    assert_eq!(as_bulk_str(&loaded).as_deref(), Some(expected_sha.as_str()));

    match handle(&mut h, cmd(&["SCRIPT", "EXISTS", &expected_sha, "deadbeef"])) {
        RespValue::Array(a) => {
            assert_eq!(a, vec![RespValue::Integer(1), RespValue::Integer(0)]);
        }
        other => panic!("expected array, got {:?}", other),
    }

    handle(&mut h, cmd(&["SET", "k", "v"]));
    assert_eq!(
        as_bulk_str(&handle(
            &mut h,
            cmd(&["EVALSHA", &expected_sha, "1", "k"])
        ))
        .as_deref(),
        Some("v")
    );

    assert_eq!(handle(&mut h, cmd(&["SCRIPT", "FLUSH"])), RespValue::ok());
    let e = err_str(&handle(
        &mut h,
        cmd(&["EVALSHA", &expected_sha, "1", "k"]),
    ));
    assert!(e.starts_with("NOSCRIPT"), "got {}", e);
}

#[test]
fn eval_auto_caches_for_evalsha() {
    let mut h = make_handler(make_cache());
    let body = "return 7";
    let sha = script_sha1(body);
    assert_eq!(
        handle(&mut h, cmd(&["EVAL", body, "0"])),
        RespValue::Integer(7)
    );
    assert_eq!(
        handle(&mut h, cmd(&["EVALSHA", &sha, "0"])),
        RespValue::Integer(7)
    );
}

#[test]
fn script_kill_notbusy() {
    let mut h = make_handler(make_cache());
    let e = err_str(&handle(&mut h, cmd(&["SCRIPT", "KILL"])));
    assert!(e.starts_with("NOTBUSY"), "got {}", e);
}

#[test]
fn eval_wrong_numkeys() {
    let mut h = make_handler(make_cache());
    let e = err_str(&handle(&mut h, cmd(&["EVAL", "return 1", "2", "onlyone"])));
    assert!(e.contains("Number of keys") || e.contains("ERR"), "got {}", e);
}

#[test]
fn eval_list_and_set_ops() {
    let mut h = make_handler(make_cache());
    let r = handle(
        &mut h,
        cmd(&[
            "EVAL",
            "redis.call('LPUSH', KEYS[1], 'a', 'b'); redis.call('SADD', KEYS[2], 'x'); return redis.call('LLEN', KEYS[1])",
            "2",
            "list1",
            "set1",
        ]),
    );
    assert_eq!(r, RespValue::Integer(2));
    assert_eq!(handle(&mut h, cmd(&["SCARD", "set1"])), RespValue::Integer(1));
}

#[test]
fn command_catalog_lists_eval() {
    let mut h = make_handler(make_cache());
    match handle(&mut h, cmd(&["COMMAND", "INFO", "eval"])) {
        RespValue::Array(a) => {
            assert!(!a.is_empty());
            // First entry is the eval command array (or null if unknown)
            match &a[0] {
                RespValue::Array(spec) => {
                    assert_eq!(as_bulk_str(&spec[0]).as_deref(), Some("eval"));
                }
                other => panic!("expected command spec, got {:?}", other),
            }
        }
        other => panic!("expected array, got {:?}", other),
    }
}
