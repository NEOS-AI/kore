//! Batch BP: ACL LOG, GEORADIUS GETKEYS STORE, SHUTDOWN/QUIT close, CLIENT KILL ID.

use bytes::Bytes;
use kore::acl::AclStore;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::databases::Databases;
use kore::protocol::RespValue;
use kore::Cache;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::watch;

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
        deadlock_ui_port: 0,
        tls: false,
        tls_cert: String::new(),
        tls_key: String::new(),
        aclfile: String::new(),
        cluster_enabled: false,
        unixsocket: String::new(),
            log_format: "text".to_string(),
    }
}

fn make_cache() -> Arc<Cache> {
    Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false)
}

fn make_handler(cache: Arc<Cache>) -> CommandHandler {
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

fn keys_of(v: RespValue) -> Vec<String> {
    match v {
        RespValue::Array(a) => a.iter().filter_map(as_bulk_str).collect(),
        other => panic!("expected key array, got {:?}", other),
    }
}

#[test]
fn bp_georadius_getkeys_store() {
    let mut h = make_handler(make_cache());

    let keys = keys_of(handle(
        &mut h,
        cmd(&[
            "COMMAND",
            "GETKEYS",
            "GEORADIUS",
            "cities",
            "13.3",
            "52.5",
            "100",
            "km",
        ]),
    ));
    assert_eq!(keys, vec!["cities"]);

    let keys = keys_of(handle(
        &mut h,
        cmd(&[
            "COMMAND",
            "GETKEYS",
            "GEORADIUS",
            "cities",
            "13.3",
            "52.5",
            "100",
            "km",
            "STORE",
            "outz",
        ]),
    ));
    assert_eq!(keys, vec!["cities", "outz"]);

    let keys = keys_of(handle(
        &mut h,
        cmd(&[
            "COMMAND",
            "GETKEYS",
            "GEORADIUSBYMEMBER",
            "cities",
            "berlin",
            "50",
            "km",
            "COUNT",
            "10",
            "ANY",
            "STOREDIST",
            "dists",
        ]),
    ));
    assert_eq!(keys, vec!["cities", "dists"]);
}

#[test]
fn bp_acl_log_records_denials() {
    let config = Arc::new(test_config());
    let dbs = Databases::single(make_cache());
    let acl = AclStore::from_auth_arc("");
    let mut admin =
        CommandHandler::with_databases_and_acl(dbs.clone(), config.clone(), None, acl.clone());
    let mut limited =
        CommandHandler::with_databases_and_acl(dbs, config, None, acl);

    match handle(&mut admin, cmd(&["ACL", "LOG"])) {
        RespValue::Array(a) => assert!(a.is_empty()),
        other => panic!("{:?}", other),
    }
    match handle(&mut admin, cmd(&["ACL", "LOG", "LEN"])) {
        RespValue::Integer(0) => {}
        other => panic!("{:?}", other),
    }

    // limited: only PING (+auth so they can switch identity)
    assert_eq!(
        handle(
            &mut admin,
            cmd(&[
                "ACL", "SETUSER", "limited", "on", "nopass", "-@all", "+ping", "+auth"
            ])
        ),
        RespValue::ok()
    );

    // Switch limited connection to the restricted user
    assert_eq!(
        handle(&mut limited, cmd(&["AUTH", "limited", ""])),
        RespValue::ok()
    );

    match handle(&mut limited, cmd(&["GET", "k"])) {
        RespValue::Error(e) => {
            let msg = String::from_utf8_lossy(&e);
            assert!(msg.contains("NOPERM"), "{}", msg);
        }
        other => panic!("{:?}", other),
    }

    match handle(&mut admin, cmd(&["ACL", "LOG", "1"])) {
        RespValue::Array(entries) => {
            assert_eq!(entries.len(), 1);
            match &entries[0] {
                RespValue::Array(fields) => {
                    let flat: Vec<String> = fields.iter().filter_map(as_bulk_str).collect();
                    let joined = flat.join(" ");
                    assert!(joined.contains("command"), "{}", joined);
                    assert!(joined.contains("get"), "{}", joined);
                    assert!(joined.contains("limited"), "{}", joined);
                }
                other => panic!("{:?}", other),
            }
        }
        other => panic!("{:?}", other),
    }

    match handle(&mut admin, cmd(&["ACL", "LOG", "LEN"])) {
        RespValue::Integer(n) => assert!(n >= 1),
        other => panic!("{:?}", other),
    }

    assert_eq!(
        handle(&mut admin, cmd(&["ACL", "LOG", "RESET"])),
        RespValue::ok()
    );
    match handle(&mut admin, cmd(&["ACL", "LOG"])) {
        RespValue::Array(a) => assert!(a.is_empty()),
        other => panic!("{:?}", other),
    }

    match handle(&mut admin, cmd(&["ACL", "HELP"])) {
        RespValue::Array(lines) => {
            let joined: String = lines
                .iter()
                .filter_map(as_bulk_str)
                .collect::<Vec<_>>()
                .join("\n");
            assert!(joined.contains("LOG"), "{}", joined);
        }
        other => panic!("{:?}", other),
    }

    assert_eq!(
        handle(&mut admin, cmd(&["CONFIG", "SET", "acllog-max-len", "5"])),
        RespValue::ok()
    );
    match handle(&mut admin, cmd(&["CONFIG", "GET", "acllog-max-len"])) {
        RespValue::Array(a) => {
            let vals: Vec<String> = a.iter().filter_map(as_bulk_str).collect();
            assert!(vals.iter().any(|v| v == "5"), "{:?}", vals);
        }
        other => panic!("{:?}", other),
    }
}

#[test]
fn bp_shutdown_signals_and_closes() {
    let cache = make_cache();
    let (tx, mut rx) = watch::channel(false);
    let nosave = Arc::new(AtomicBool::new(false));
    let mut h = make_handler(cache).with_shutdown(tx, Arc::clone(&nosave));

    assert_eq!(
        handle(&mut h, cmd(&["SHUTDOWN", "NOSAVE"])),
        RespValue::ok()
    );
    assert!(h.take_close_after_reply());
    assert!(nosave.load(Ordering::SeqCst));
    assert!(*rx.borrow_and_update());

    match handle(&mut h, cmd(&["COMMAND", "INFO", "shutdown"])) {
        RespValue::Array(a) => assert!(!a.is_empty()),
        other => panic!("{:?}", other),
    }
}

#[test]
fn bp_quit_and_client_kill_close() {
    let mut h = make_handler(make_cache());
    h.set_client_id(42);

    assert_eq!(handle(&mut h, cmd(&["QUIT"])), RespValue::ok());
    assert!(h.take_close_after_reply());

    // SKIPME yes (default): do not kill self
    assert_eq!(
        handle(&mut h, cmd(&["CLIENT", "KILL", "ID", "42"])),
        RespValue::Integer(0)
    );
    assert!(!h.take_close_after_reply());

    assert_eq!(
        handle(
            &mut h,
            cmd(&["CLIENT", "KILL", "ID", "42", "SKIPME", "no"])
        ),
        RespValue::Integer(1)
    );
    assert!(h.take_close_after_reply());

    assert_eq!(
        handle(
            &mut h,
            cmd(&["CLIENT", "KILL", "ID", "999", "SKIPME", "no"])
        ),
        RespValue::Integer(0)
    );
}

#[test]
fn bp_client_help_lists_kill() {
    let mut h = make_handler(make_cache());
    match handle(&mut h, cmd(&["CLIENT", "HELP"])) {
        RespValue::Array(lines) => {
            let joined: String = lines
                .iter()
                .filter_map(as_bulk_str)
                .collect::<Vec<_>>()
                .join("\n");
            assert!(joined.contains("KILL"), "{}", joined);
        }
        other => panic!("{:?}", other),
    }
}
