//! Batch BB: SLOWLOG, MEMORY STATS/DOCTOR/PURGE, CLIENT REPLY, slowlog CONFIG.

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

#[test]
fn slowlog_records_and_config() {
    let cache = make_cache();
    let mut h = make_handler(cache.clone());

    // Force every command into the slow log.
    assert_eq!(
        handle(
            &mut h,
            cmd(&["CONFIG", "SET", "slowlog-log-slower-than", "0"])
        ),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["CONFIG", "SET", "slowlog-max-len", "10"])),
        RespValue::ok()
    );

    match handle(
        &mut h,
        cmd(&["CONFIG", "GET", "slowlog-log-slower-than"]),
    ) {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 2);
            assert_eq!(as_bulk_str(&items[1]).as_deref(), Some("0"));
        }
        other => panic!("{:?}", other),
    }

    handle(&mut h, cmd(&["SET", "a", "1"]));
    handle(&mut h, cmd(&["GET", "a"]));
    handle(&mut h, cmd(&["SET", "b", "2"]));

    match handle(&mut h, cmd(&["SLOWLOG", "LEN"])) {
        RespValue::Integer(n) => assert!(n >= 2, "len={n}"),
        other => panic!("{:?}", other),
    }

    match handle(&mut h, cmd(&["SLOWLOG", "GET", "2"])) {
        RespValue::Array(entries) => {
            assert_eq!(entries.len(), 2);
            // Each entry: [id, ts, duration_us, argv]
            for e in &entries {
                match e {
                    RespValue::Array(fields) => {
                        assert_eq!(fields.len(), 4);
                        assert!(matches!(fields[0], RespValue::Integer(_)));
                        assert!(matches!(fields[1], RespValue::Integer(_)));
                        assert!(matches!(fields[2], RespValue::Integer(d) if d >= 0));
                        assert!(matches!(fields[3], RespValue::Array(_)));
                    }
                    other => panic!("{:?}", other),
                }
            }
        }
        other => panic!("{:?}", other),
    }

    assert_eq!(
        handle(&mut h, cmd(&["SLOWLOG", "RESET"])),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["SLOWLOG", "LEN"])),
        RespValue::Integer(0)
    );

    // Disable with negative threshold
    assert_eq!(
        handle(
            &mut h,
            cmd(&["CONFIG", "SET", "slowlog-log-slower-than", "-1"])
        ),
        RespValue::ok()
    );
    handle(&mut h, cmd(&["SET", "c", "3"]));
    assert_eq!(
        handle(&mut h, cmd(&["SLOWLOG", "LEN"])),
        RespValue::Integer(0)
    );
}

#[test]
fn memory_stats_doctor_purge() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["SET", "s", "hello"]));
    handle(&mut h, cmd(&["HSET", "h", "f", "v"]));

    match handle(&mut h, cmd(&["MEMORY", "STATS"])) {
        RespValue::Array(items) => {
            assert!(items.len() >= 4);
            assert!(items.len() % 2 == 0);
            // Find maxmemory and total.allocated
            let mut found_total = false;
            let mut found_max = false;
            let mut i = 0;
            while i + 1 < items.len() {
                if let Some(k) = as_bulk_str(&items[i]) {
                    if k == "total.allocated" {
                        found_total = true;
                        assert!(matches!(items[i + 1], RespValue::Integer(n) if n >= 0));
                    }
                    if k == "maxmemory" {
                        found_max = true;
                    }
                }
                i += 2;
            }
            assert!(found_total && found_max);
        }
        other => panic!("{:?}", other),
    }

    match handle(&mut h, cmd(&["MEMORY", "DOCTOR"])) {
        RespValue::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("memory") || s.contains("Memory") || s.contains("Tracked"));
        }
        other => panic!("{:?}", other),
    }

    assert_eq!(
        handle(&mut h, cmd(&["MEMORY", "PURGE"])),
        RespValue::ok()
    );
}

#[test]
fn client_reply_off_skip_on() {
    let mut h = make_handler(make_cache());

    // SKIP: next command suppressed
    assert_eq!(
        handle(&mut h, cmd(&["CLIENT", "REPLY", "SKIP"])),
        RespValue::ok()
    );
    assert!(!h.take_suppress_reply()); // REPLY itself not suppressed

    let _ = handle(&mut h, cmd(&["SET", "k", "v"]));
    assert!(h.take_suppress_reply());

    // Next is normal
    assert_eq!(handle(&mut h, cmd(&["GET", "k"])), {
        // get value
        RespValue::BulkString(Some(Bytes::from_static(b"v")))
    });
    assert!(!h.take_suppress_reply());

    // OFF suppresses subsequent
    assert_eq!(
        handle(&mut h, cmd(&["CLIENT", "REPLY", "OFF"])),
        RespValue::ok()
    );
    assert!(!h.take_suppress_reply());

    let _ = handle(&mut h, cmd(&["SET", "k2", "x"]));
    assert!(h.take_suppress_reply());
    let _ = handle(&mut h, cmd(&["GET", "k2"]));
    assert!(h.take_suppress_reply());

    // ON restores
    assert_eq!(
        handle(&mut h, cmd(&["CLIENT", "REPLY", "ON"])),
        RespValue::ok()
    );
    assert!(!h.take_suppress_reply());
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GET", "k2"]))).as_deref(),
        Some("x")
    );
    assert!(!h.take_suppress_reply());
}
