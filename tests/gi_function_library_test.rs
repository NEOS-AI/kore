//! Batch GI: Redis Functions library — FUNCTION LOAD/LIST/DELETE/FLUSH/DUMP/RESTORE + FCALL.

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

fn err_str(v: &RespValue) -> String {
    match v {
        RespValue::Error(e) => String::from_utf8_lossy(e).into_owned(),
        other => panic!("expected error, got {:?}", other),
    }
}

const ECHO_LIB: &str = r#"#!lua name=mylib
redis.register_function('echo', function(keys, args)
  return args[1]
end)
"#;

const GETSET_LIB: &str = r#"#!lua name=kvlib
redis.register_function('myget', function(keys, args)
  return redis.call('GET', keys[1])
end)
redis.register_function('myset', function(keys, args)
  return redis.call('SET', keys[1], args[1])
end)
"#;

const RO_LIB: &str = r#"#!lua name=rolib
redis.register_function{
  function_name='roget',
  callback=function(keys, args)
    return redis.call('GET', keys[1])
  end,
  flags={'no-writes'}
}
"#;

#[test]
fn gi_load_list_fcall_delete() {
    let mut h = make_handler(make_cache());

    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["FUNCTION", "LOAD", ECHO_LIB]))).as_deref(),
        Some("mylib")
    );

    // Duplicate without REPLACE fails.
    match handle(&mut h, cmd(&["FUNCTION", "LOAD", ECHO_LIB])) {
        RespValue::Error(e) => {
            assert!(
                String::from_utf8_lossy(&e).contains("already exists"),
                "{:?}",
                e
            );
        }
        other => panic!("{:?}", other),
    }

    // REPLACE succeeds.
    assert_eq!(
        as_bulk_str(&handle(
            &mut h,
            cmd(&["FUNCTION", "LOAD", "REPLACE", ECHO_LIB])
        ))
        .as_deref(),
        Some("mylib")
    );

    match handle(&mut h, cmd(&["FUNCTION", "LIST"])) {
        RespValue::Array(libs) => {
            assert_eq!(libs.len(), 1);
            // library entry is a flat field/value array
            match &libs[0] {
                RespValue::Array(fields) => {
                    let joined: String = fields
                        .iter()
                        .filter_map(as_bulk_str)
                        .collect::<Vec<_>>()
                        .join("|");
                    assert!(joined.contains("mylib"), "{}", joined);
                    assert!(joined.contains("LUA"), "{}", joined);
                }
                other => panic!("{:?}", other),
            }
        }
        other => panic!("{:?}", other),
    }

    assert_eq!(
        as_bulk_str(&handle(
            &mut h,
            cmd(&["FCALL", "echo", "0", "hello-fn"])
        ))
        .as_deref(),
        Some("hello-fn")
    );

    assert_eq!(
        handle(&mut h, cmd(&["FUNCTION", "DELETE", "mylib"])),
        RespValue::ok()
    );
    assert!(err_str(&handle(&mut h, cmd(&["FCALL", "echo", "0", "x"])))
        .to_ascii_lowercase()
        .contains("not found"));
}

#[test]
fn gi_fcall_redis_call_get_set() {
    let mut h = make_handler(make_cache());
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["FUNCTION", "LOAD", GETSET_LIB]))).as_deref(),
        Some("kvlib")
    );

    assert_eq!(
        handle(&mut h, cmd(&["FCALL", "myset", "1", "k1", "v1"])),
        RespValue::ok()
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GET", "k1"]))).as_deref(),
        Some("v1")
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["FCALL", "myget", "1", "k1"]))).as_deref(),
        Some("v1")
    );
}

#[test]
fn gi_fcall_ro_requires_no_writes_flag() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["FUNCTION", "LOAD", GETSET_LIB]));
    handle(&mut h, cmd(&["SET", "rk", "rv"]));

    // myget has no no-writes flag → FCALL_RO rejected.
    let e = err_str(&handle(&mut h, cmd(&["FCALL_RO", "myget", "1", "rk"])));
    assert!(
        e.contains("fcall_ro") || e.contains("flags") || e.contains("Can not"),
        "{}",
        e
    );

    handle(&mut h, cmd(&["FUNCTION", "LOAD", RO_LIB]));
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["FCALL_RO", "roget", "1", "rk"]))).as_deref(),
        Some("rv")
    );

    // Write via redis.call inside no-writes + FCALL_RO is denied at runtime.
    let write_ro = r#"#!lua name=badro
redis.register_function{
  function_name='badset',
  callback=function(keys, args)
    return redis.call('SET', keys[1], args[1])
  end,
  flags={'no-writes'}
}
"#;
    handle(&mut h, cmd(&["FUNCTION", "LOAD", write_ro]));
    let e2 = err_str(&handle(
        &mut h,
        cmd(&["FCALL_RO", "badset", "1", "wk", "wv"]),
    ));
    assert!(
        e2.contains("read-only") || e2.contains("Write") || e2.contains("not allowed"),
        "{}",
        e2
    );
}

#[test]
fn gi_flush_dump_restore() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["FUNCTION", "LOAD", ECHO_LIB]));

    let dump = match handle(&mut h, cmd(&["FUNCTION", "DUMP"])) {
        RespValue::BulkString(Some(b)) => b,
        other => panic!("expected dump bulk, got {:?}", other),
    };
    assert!(dump.starts_with(b"KORF1"), "magic missing");

    assert_eq!(
        handle(&mut h, cmd(&["FUNCTION", "FLUSH"])),
        RespValue::ok()
    );
    match handle(&mut h, cmd(&["FUNCTION", "LIST"])) {
        RespValue::Array(a) => assert!(a.is_empty()),
        other => panic!("{:?}", other),
    }

    // RESTORE FLUSH with payload.
    let restore_cmd = RespValue::Array(vec![
        bulk("FUNCTION"),
        bulk("RESTORE"),
        RespValue::BulkString(Some(dump.clone())),
        bulk("FLUSH"),
    ]);
    assert_eq!(handle(&mut h, restore_cmd), RespValue::ok());
    assert_eq!(
        as_bulk_str(&handle(
            &mut h,
            cmd(&["FCALL", "echo", "0", "restored"])
        ))
        .as_deref(),
        Some("restored")
    );

    // APPEND conflict.
    let restore_append = RespValue::Array(vec![
        bulk("FUNCTION"),
        bulk("RESTORE"),
        RespValue::BulkString(Some(dump)),
        bulk("APPEND"),
    ]);
    match handle(&mut h, restore_append) {
        RespValue::Error(e) => {
            assert!(
                String::from_utf8_lossy(&e).contains("already exists"),
                "{:?}",
                e
            );
        }
        other => panic!("{:?}", other),
    }
}

#[test]
fn gi_function_help_stats() {
    let mut h = make_handler(make_cache());
    match handle(&mut h, cmd(&["FUNCTION", "HELP"])) {
        RespValue::Array(lines) => {
            let joined: String = lines
                .iter()
                .filter_map(as_bulk_str)
                .collect::<Vec<_>>()
                .join("\n");
            assert!(joined.contains("LOAD"), "{}", joined);
            assert!(joined.contains("DUMP"), "{}", joined);
        }
        other => panic!("{:?}", other),
    }
    match handle(&mut h, cmd(&["FUNCTION", "STATS"])) {
        RespValue::Array(_) => {}
        other => panic!("{:?}", other),
    }
}

#[test]
fn gi_missing_shebang_rejected() {
    let mut h = make_handler(make_cache());
    let e = err_str(&handle(
        &mut h,
        cmd(&[
            "FUNCTION",
            "LOAD",
            "redis.register_function('x', function() return 1 end)",
        ]),
    ));
    assert!(e.contains("shebang") || e.contains("#!"), "{}", e);
}
