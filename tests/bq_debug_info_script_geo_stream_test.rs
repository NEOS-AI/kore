//! Batch BQ: DEBUG HELP/SLEEP/OBJECT, INFO Clients/CPU/Persistence, Lua GEO/stream.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::protocol::RespValue;
use kore::Cache;
use std::sync::Arc;
use std::time::Instant;

fn test_config() -> Config {
    Config {
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
            log_format: "text".to_string(),
    }
}

fn make_handler() -> CommandHandler {
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false);
    CommandHandler::new(cache, Arc::new(test_config()))
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
fn bq_debug_help_object_sleep() {
    let mut h = make_handler();

    match handle(&mut h, cmd(&["DEBUG", "HELP"])) {
        RespValue::Array(lines) => {
            let joined: String = lines
                .iter()
                .filter_map(as_bulk_str)
                .collect::<Vec<_>>()
                .join("\n");
            assert!(joined.contains("OBJECT"), "{}", joined);
            assert!(joined.contains("SLEEP"), "{}", joined);
        }
        other => panic!("{:?}", other),
    }

    assert_eq!(handle(&mut h, cmd(&["SET", "k", "v"])), RespValue::ok());
    match handle(&mut h, cmd(&["DEBUG", "OBJECT", "k"])) {
        RespValue::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("encoding:"), "{}", s);
            assert!(s.contains("raw") || s.contains("embstr"), "{}", s);
        }
        other => panic!("{:?}", other),
    }
    match handle(&mut h, cmd(&["DEBUG", "OBJECT", "missing"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("no such key")),
        other => panic!("{:?}", other),
    }

    let t0 = Instant::now();
    assert_eq!(
        handle(&mut h, cmd(&["DEBUG", "SLEEP", "0"])),
        RespValue::ok()
    );
    // fractional sleep
    assert_eq!(
        handle(&mut h, cmd(&["DEBUG", "SLEEP", "0.05"])),
        RespValue::ok()
    );
    assert!(t0.elapsed().as_millis() >= 40);

    // Catalog
    match handle(&mut h, cmd(&["COMMAND", "INFO", "debug"])) {
        RespValue::Array(a) => assert!(!a.is_empty()),
        other => panic!("{:?}", other),
    }
}

#[test]
fn bq_info_clients_cpu_persistence() {
    let mut h = make_handler();

    for section in ["clients", "cpu", "persistence"] {
        match handle(&mut h, cmd(&["INFO", section])) {
            RespValue::BulkString(Some(b)) => {
                let s = String::from_utf8_lossy(&b);
                match section {
                    "clients" => {
                        assert!(s.contains("connected_clients:"), "{}", s);
                        assert!(s.contains("blocked_clients:"), "{}", s);
                    }
                    "cpu" => {
                        assert!(s.contains("used_cpu_sys:"), "{}", s);
                        assert!(s.contains("used_cpu_user:"), "{}", s);
                    }
                    "persistence" => {
                        assert!(s.contains("rdb_changes_since_last_save:"), "{}", s);
                        assert!(s.contains("aof_enabled:"), "{}", s);
                        assert!(s.contains("loading:"), "{}", s);
                    }
                    _ => unreachable!(),
                }
            }
            other => panic!("{} => {:?}", section, other),
        }
    }

    // Full INFO includes new section headers
    match handle(&mut h, cmd(&["INFO"])) {
        RespValue::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("# Clients"), "{}", s);
            assert!(s.contains("# CPU"), "{}", s);
            assert!(s.contains("# Persistence"), "{}", s);
        }
        other => panic!("{:?}", other),
    }
}

#[test]
fn bq_script_geo_and_stream() {
    let mut h = make_handler();

    // GEOADD via redis.call
    let r = handle(
        &mut h,
        cmd(&[
            "EVAL",
            "return redis.call('GEOADD', KEYS[1], ARGV[1], ARGV[2], ARGV[3])",
            "1",
            "cities",
            "13.361389",
            "38.115556",
            "Palermo",
        ]),
    );
    assert_eq!(r, RespValue::Integer(1), "{:?}", r);

    let r = handle(
        &mut h,
        cmd(&[
            "EVAL",
            "return redis.call('GEOPOS', KEYS[1], ARGV[1])",
            "1",
            "cities",
            "Palermo",
        ]),
    );
    match r {
        RespValue::Array(a) => assert_eq!(a.len(), 1),
        other => panic!("{:?}", other),
    }

    // Stream XADD + XLEN via script
    let r = handle(
        &mut h,
        cmd(&[
            "EVAL",
            "return redis.call('XADD', KEYS[1], '*', 'f', 'v')",
            "1",
            "s",
        ]),
    );
    match r {
        RespValue::BulkString(Some(_)) => {}
        other => panic!("{:?}", other),
    }
    assert_eq!(
        handle(
            &mut h,
            cmd(&["EVAL", "return redis.call('XLEN', KEYS[1])", "1", "s"])
        ),
        RespValue::Integer(1)
    );

    // TOUCH / SCAN allowed
    assert_eq!(
        handle(
            &mut h,
            cmd(&["EVAL", "return redis.call('TOUCH', KEYS[1])", "1", "s"])
        ),
        RespValue::Integer(1)
    );
    match handle(
        &mut h,
        cmd(&["EVAL", "return redis.call('SCAN', '0')", "0"]),
    ) {
        RespValue::Array(a) => assert_eq!(a.len(), 2),
        other => panic!("{:?}", other),
    }
}

#[test]
fn bq_bitop_getkeys() {
    let mut h = make_handler();
    match handle(
        &mut h,
        cmd(&["COMMAND", "GETKEYS", "BITOP", "AND", "dest", "a", "b", "c"]),
    ) {
        RespValue::Array(keys) => {
            let ks: Vec<String> = keys.iter().filter_map(as_bulk_str).collect();
            assert_eq!(ks, vec!["dest", "a", "b", "c"]);
        }
        other => panic!("{:?}", other),
    }
}
