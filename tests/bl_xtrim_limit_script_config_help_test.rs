//! Batch BL: XADD/XTRIM LIMIT, SCRIPT HELP, CONFIG HELP.

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
fn bl_xtrim_limit_caps_deletions() {
    let mut h = make_handler(make_cache());
    for i in 1..=10 {
        let id = format!("{}-0", i);
        handle(&mut h, cmd(&["XADD", "s", &id, "f", "v"]));
    }
    assert_eq!(
        handle(&mut h, cmd(&["XLEN", "s"])),
        RespValue::Integer(10)
    );

    // MAXLEN 3 would delete 7, LIMIT 2 deletes only 2
    assert_eq!(
        handle(&mut h, cmd(&["XTRIM", "s", "MAXLEN", "3", "LIMIT", "2"])),
        RespValue::Integer(2)
    );
    assert_eq!(
        handle(&mut h, cmd(&["XLEN", "s"])),
        RespValue::Integer(8)
    );

    // Without LIMIT, trim down to 3
    assert_eq!(
        handle(&mut h, cmd(&["XTRIM", "s", "MAXLEN", "3"])),
        RespValue::Integer(5)
    );
    assert_eq!(
        handle(&mut h, cmd(&["XLEN", "s"])),
        RespValue::Integer(3)
    );
}

#[test]
fn bl_xtrim_minid_limit() {
    let mut h = make_handler(make_cache());
    for i in 1..=5 {
        let id = format!("{}-0", i);
        handle(&mut h, cmd(&["XADD", "s", &id, "f", "v"]));
    }
    // MINID 4-0 would remove 1-0,2-0,3-0 (3 entries); LIMIT 1 removes only 1
    assert_eq!(
        handle(
            &mut h,
            cmd(&["XTRIM", "s", "MINID", "4-0", "LIMIT", "1"])
        ),
        RespValue::Integer(1)
    );
    assert_eq!(
        handle(&mut h, cmd(&["XLEN", "s"])),
        RespValue::Integer(4)
    );
}

#[test]
fn bl_xadd_maxlen_limit() {
    let mut h = make_handler(make_cache());
    for i in 1..=5 {
        let id = format!("{}-0", i);
        handle(&mut h, cmd(&["XADD", "s", &id, "f", "v"]));
    }
    // XADD with MAXLEN 2 LIMIT 1: after add would need to delete 4 to reach 2,
    // but LIMIT 1 deletes only 1 → length 5
    handle(
        &mut h,
        cmd(&["XADD", "s", "MAXLEN", "2", "LIMIT", "1", "6-0", "f", "v"]),
    );
    assert_eq!(
        handle(&mut h, cmd(&["XLEN", "s"])),
        RespValue::Integer(5)
    );

    // Full MAXLEN 2 without LIMIT
    handle(
        &mut h,
        cmd(&["XADD", "s", "MAXLEN", "2", "7-0", "f", "v"]),
    );
    assert_eq!(
        handle(&mut h, cmd(&["XLEN", "s"])),
        RespValue::Integer(2)
    );

    // LIMIT without MAXLEN/MINID → syntax error
    match handle(&mut h, cmd(&["XADD", "s", "LIMIT", "1", "*", "f", "v"])) {
        RespValue::Error(e) => {
            assert!(String::from_utf8_lossy(&e).contains("syntax"), "{:?}", e);
        }
        other => panic!("{:?}", other),
    }
}

#[test]
fn bl_script_and_config_help() {
    let mut h = make_handler(make_cache());

    match handle(&mut h, cmd(&["SCRIPT", "HELP"])) {
        RespValue::Array(lines) => {
            let joined: String = lines
                .iter()
                .filter_map(|l| as_bulk_str(l))
                .collect::<Vec<_>>()
                .join("\n");
            assert!(joined.contains("LOAD"), "{}", joined);
            assert!(joined.contains("EXISTS"), "{}", joined);
            assert!(joined.contains("FLUSH"), "{}", joined);
            assert!(joined.contains("KILL"), "{}", joined);
        }
        other => panic!("{:?}", other),
    }

    match handle(&mut h, cmd(&["CONFIG", "HELP"])) {
        RespValue::Array(lines) => {
            let joined: String = lines
                .iter()
                .filter_map(|l| as_bulk_str(l))
                .collect::<Vec<_>>()
                .join("\n");
            assert!(joined.contains("GET"), "{}", joined);
            assert!(joined.contains("SET"), "{}", joined);
            assert!(joined.contains("RESETSTAT"), "{}", joined);
        }
        other => panic!("{:?}", other),
    }
}
