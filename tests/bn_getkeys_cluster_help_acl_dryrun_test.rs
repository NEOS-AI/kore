//! Batch BN: COMMAND GETKEYS movablekeys, CLUSTER HELP, ACL DRYRUN.

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
fn bn_command_getkeys_movablekeys() {
    let mut h = make_handler(make_cache());

    assert_eq!(
        keys_of(handle(
            &mut h,
            cmd(&["COMMAND", "GETKEYS", "LMPOP", "2", "a", "b", "LEFT"])
        )),
        vec!["a", "b"]
    );
    assert_eq!(
        keys_of(handle(
            &mut h,
            cmd(&["COMMAND", "GETKEYS", "ZMPOP", "1", "z", "MIN"])
        )),
        vec!["z"]
    );
    assert_eq!(
        keys_of(handle(
            &mut h,
            cmd(&[
                "COMMAND", "GETKEYS", "BLMPOP", "0", "2", "l1", "l2", "RIGHT", "COUNT", "3"
            ])
        )),
        vec!["l1", "l2"]
    );
    assert_eq!(
        keys_of(handle(
            &mut h,
            cmd(&["COMMAND", "GETKEYS", "SINTERCARD", "2", "s1", "s2", "LIMIT", "10"])
        )),
        vec!["s1", "s2"]
    );
    assert_eq!(
        keys_of(handle(
            &mut h,
            cmd(&[
                "COMMAND", "GETKEYS", "XREAD", "COUNT", "5", "STREAMS", "st1", "st2", "0-0",
                "0-0"
            ])
        )),
        vec!["st1", "st2"]
    );
    assert_eq!(
        keys_of(handle(
            &mut h,
            cmd(&[
                "COMMAND",
                "GETKEYS",
                "XREADGROUP",
                "GROUP",
                "g",
                "c",
                "STREAMS",
                "mystream",
                ">"
            ])
        )),
        vec!["mystream"]
    );
    assert_eq!(
        keys_of(handle(
            &mut h,
            cmd(&["COMMAND", "GETKEYS", "MEMORY", "USAGE", "mykey"])
        )),
        vec!["mykey"]
    );
}

#[test]
fn bn_cluster_help() {
    let mut h = make_handler(make_cache());
    match handle(&mut h, cmd(&["CLUSTER", "HELP"])) {
        RespValue::Array(lines) => {
            let joined: String = lines
                .iter()
                .filter_map(as_bulk_str)
                .collect::<Vec<_>>()
                .join("\n");
            assert!(joined.contains("KEYSLOT"), "{}", joined);
            assert!(joined.contains("SETSLOT"), "{}", joined);
            assert!(joined.contains("MIGRATEKEYS"), "{}", joined);
            assert!(joined.contains("MEET"), "{}", joined);
        }
        other => panic!("{:?}", other),
    }
}

#[test]
fn bn_acl_dryrun() {
    let mut h = make_handler(make_cache());

    // default user can GET
    assert_eq!(
        handle(&mut h, cmd(&["ACL", "DRYRUN", "default", "GET", "k"])),
        RespValue::ok()
    );

    // Restrict alice to +get ~allowed*
    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "ACL", "SETUSER", "alice", "on", "nopass", "+get", "~allowed*"
            ])
        ),
        RespValue::ok()
    );

    assert_eq!(
        handle(&mut h, cmd(&["ACL", "DRYRUN", "alice", "GET", "allowed1"])),
        RespValue::ok()
    );

    match handle(&mut h, cmd(&["ACL", "DRYRUN", "alice", "SET", "allowed1", "v"])) {
        RespValue::Error(e) => {
            let msg = String::from_utf8_lossy(&e);
            assert!(msg.contains("NOPERM") && msg.contains("set"), "{}", msg);
        }
        other => panic!("{:?}", other),
    }

    match handle(&mut h, cmd(&["ACL", "DRYRUN", "alice", "GET", "denied"])) {
        RespValue::Error(e) => {
            let msg = String::from_utf8_lossy(&e);
            assert!(msg.contains("NOPERM") && msg.contains("keys"), "{}", msg);
        }
        other => panic!("{:?}", other),
    }

    match handle(&mut h, cmd(&["ACL", "DRYRUN", "nobody", "GET", "k"])) {
        RespValue::Error(e) => {
            assert!(
                String::from_utf8_lossy(&e).contains("not found"),
                "{:?}",
                e
            );
        }
        other => panic!("{:?}", other),
    }

    // HELP mentions DRYRUN
    match handle(&mut h, cmd(&["ACL", "HELP"])) {
        RespValue::Array(lines) => {
            let joined: String = lines
                .iter()
                .filter_map(as_bulk_str)
                .collect::<Vec<_>>()
                .join("\n");
            assert!(joined.contains("DRYRUN"), "{}", joined);
        }
        other => panic!("{:?}", other),
    }
}
