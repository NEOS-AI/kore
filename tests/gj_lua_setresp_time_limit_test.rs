//! Batch GJ: redis.setresp, lua-time-limit, SCRIPT KILL.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::protocol::RespValue;
use kore::scripting::ScriptRuntime;
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

fn err_str(v: &RespValue) -> String {
    match v {
        RespValue::Error(e) => String::from_utf8_lossy(e).into_owned(),
        other => panic!("expected error, got {:?}", other),
    }
}

fn config_get_value(h: &mut CommandHandler, key: &str) -> String {
    match handle(h, cmd(&["CONFIG", "GET", key])) {
        RespValue::Array(a) => {
            // flat k,v pairs
            for i in (0..a.len()).step_by(2) {
                if as_bulk_str(&a[i]).as_deref() == Some(key) {
                    return as_bulk_str(&a[i + 1]).unwrap_or_default();
                }
            }
            panic!("key {} not found in {:?}", key, a);
        }
        RespValue::Map(pairs) => {
            for (k, v) in pairs {
                if as_bulk_str(&k).as_deref() == Some(key) {
                    return as_bulk_str(&v).unwrap_or_default();
                }
            }
            panic!("key {} not found in map", key);
        }
        other => panic!("{:?}", other),
    }
}

#[test]
fn gj_setresp_boolean_mapping() {
    let mut h = make_handler(make_cache());

    // Default RESP2: true → integer 1, false → null bulk
    assert_eq!(
        handle(&mut h, cmd(&["EVAL", "return true", "0"])),
        RespValue::Integer(1)
    );
    assert_eq!(
        handle(&mut h, cmd(&["EVAL", "return false", "0"])),
        RespValue::null()
    );

    // setresp(3): booleans stay RESP3 Bool
    assert_eq!(
        handle(
            &mut h,
            cmd(&["EVAL", "redis.setresp(3); return true", "0"])
        ),
        RespValue::Bool(true)
    );
    assert_eq!(
        handle(
            &mut h,
            cmd(&["EVAL", "redis.setresp(3); return false", "0"])
        ),
        RespValue::Bool(false)
    );
}

#[test]
fn gj_setresp_map_from_hgetall() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["HSET", "mh", "f1", "v1", "f2", "v2"]));

    // RESP3 path: HGETALL returns Map → {map=…} in Lua → Map reply
    let r = handle(
        &mut h,
        cmd(&[
            "EVAL",
            "redis.setresp(3); return redis.call('HGETALL', KEYS[1])",
            "1",
            "mh",
        ]),
    );
    match r {
        RespValue::Map(pairs) => {
            assert_eq!(pairs.len(), 2);
            let mut keys: Vec<String> = pairs
                .iter()
                .filter_map(|(k, _)| as_bulk_str(k))
                .collect();
            keys.sort();
            assert_eq!(keys, vec!["f1".to_string(), "f2".to_string()]);
        }
        other => panic!("expected RESP3 map, got {:?}", other),
    }

    // RESP2 path: flat array of field/value
    let r2 = handle(
        &mut h,
        cmd(&[
            "EVAL",
            "return redis.call('HGETALL', KEYS[1])",
            "1",
            "mh",
        ]),
    );
    match r2 {
        RespValue::Array(a) => assert_eq!(a.len(), 4),
        other => panic!("expected array, got {:?}", other),
    }
}

#[test]
fn gj_setresp_invalid_version() {
    let mut h = make_handler(make_cache());
    let e = err_str(&handle(
        &mut h,
        cmd(&["EVAL", "redis.setresp(4)", "0"]),
    ));
    assert!(e.contains("RESP version") || e.contains("2 or 3"), "{}", e);
}

#[test]
fn gj_lua_time_limit_config_and_timeout() {
    let mut h = make_handler(make_cache());

    assert_eq!(config_get_value(&mut h, "lua-time-limit"), "5000");
    assert_eq!(
        handle(&mut h, cmd(&["CONFIG", "SET", "lua-time-limit", "5"])),
        RespValue::ok()
    );
    assert_eq!(config_get_value(&mut h, "lua-time-limit"), "5");

    // Busy loop should hit the hard time limit.
    let e = err_str(&handle(
        &mut h,
        cmd(&["EVAL", "local i=0; while true do i=i+1 end", "0"]),
    ));
    assert!(
        e.contains("lua-time-limit") || e.contains("time"),
        "{}",
        e
    );

    // 0 = unlimited (quick script still works)
    assert_eq!(
        handle(&mut h, cmd(&["CONFIG", "SET", "lua-time-limit", "0"])),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["EVAL", "return 1", "0"])),
        RespValue::Integer(1)
    );

    // restore default for other tests on same process (not shared across handlers)
    handle(&mut h, cmd(&["CONFIG", "SET", "lua-time-limit", "5000"]));
}

#[test]
fn gj_script_kill_busy_loop() {
    let runtime = ScriptRuntime::shared();
    // Very high limit so only KILL aborts (not timeout).
    runtime.set_time_limit_ms(60_000);

    let cache = make_cache();
    let mut killer = make_handler(cache.clone()).with_script_runtime(runtime.clone());
    let mut runner = make_handler(cache).with_script_runtime(runtime.clone());

    let handle_runner = thread::spawn(move || {
        handle(
            &mut runner,
            cmd(&["EVAL", "local i=0; while true do i=i+1 end", "0"]),
        )
    });

    // Wait until script is registered as active.
    for _ in 0..200 {
        if runtime.active_count() > 0 {
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    assert!(
        runtime.active_count() > 0,
        "script never became active"
    );

    assert_eq!(
        handle(&mut killer, cmd(&["SCRIPT", "KILL"])),
        RespValue::ok()
    );

    let result = handle_runner.join().expect("runner panicked");
    let msg = err_str(&result);
    assert!(
        msg.contains("SCRIPT KILL") || msg.contains("killed"),
        "{}",
        msg
    );

    // Idle again
    let e = err_str(&handle(&mut killer, cmd(&["SCRIPT", "KILL"])));
    assert!(e.contains("NOTBUSY"), "{}", e);
}

#[test]
fn gj_script_kill_unkillable_after_write() {
    let runtime = ScriptRuntime::shared();
    runtime.set_time_limit_ms(60_000);

    let cache = make_cache();
    let mut killer = make_handler(cache.clone()).with_script_runtime(runtime.clone());
    let mut runner = make_handler(cache).with_script_runtime(runtime.clone());

    let handle_runner = thread::spawn(move || {
        // Write then spin — KILL must be UNKILLABLE.
        handle(
            &mut runner,
            cmd(&[
                "EVAL",
                "redis.call('SET', KEYS[1], '1'); local i=0; while true do i=i+1 end",
                "1",
                "gj_w",
            ]),
        )
    });

    for _ in 0..200 {
        if runtime.active_count() > 0 {
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    // Give the write a moment to complete before kill.
    thread::sleep(Duration::from_millis(20));

    let kill_reply = handle(&mut killer, cmd(&["SCRIPT", "KILL"]));
    match kill_reply {
        RespValue::Error(e) => {
            let msg = String::from_utf8_lossy(&e);
            assert!(msg.contains("UNKILLABLE"), "{}", msg);
        }
        other => panic!("expected UNKILLABLE, got {:?}", other),
    }

    // Force timeout path to finish the runner: lower limit and wait.
    runtime.set_time_limit_ms(5);
    let result = handle_runner.join().expect("runner panicked");
    // Either still running until timeout error, or killed somehow.
    match result {
        RespValue::Error(e) => {
            let msg = String::from_utf8_lossy(&e);
            assert!(
                msg.contains("lua-time-limit")
                    || msg.contains("time")
                    || msg.contains("killed"),
                "{}",
                msg
            );
        }
        other => panic!("expected error from timed-out write script, got {:?}", other),
    }
}
