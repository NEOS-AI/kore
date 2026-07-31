//! Batch BA: DUMP/RESTORE, EXPIRE NX|XX|GT|LT, COMMAND GETKEYS, ACL GENPASS.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::protocol::RespValue;
use kore::Cache;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

fn bulk_bytes(b: Bytes) -> RespValue {
    RespValue::BulkString(Some(b))
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

fn as_bulk(v: &RespValue) -> Option<Bytes> {
    match v {
        RespValue::BulkString(Some(b)) => Some(b.clone()),
        _ => None,
    }
}

fn as_bulk_str(v: &RespValue) -> Option<String> {
    as_bulk(v).map(|b| String::from_utf8_lossy(&b).into_owned())
}

fn is_null(v: &RespValue) -> bool {
    matches!(v, RespValue::BulkString(None) | RespValue::Null)
}

#[test]
fn dump_restore_string_hash_replace_ttl() {
    let mut h = make_handler(make_cache());

    assert!(is_null(&handle(&mut h, cmd(&["DUMP", "missing"]))));

    handle(&mut h, cmd(&["SET", "s", "hello"]));
    let dump = as_bulk(&handle(&mut h, cmd(&["DUMP", "s"]))).expect("dump blob");
    // Batch FY: core types emit Redis RDB wire (type 0 string), not KDF1.
    assert_eq!(dump[0], 0, "redis string type opcode");
    assert!(!dump.starts_with(b"KDF1"));

    // Restore under new key with TTL.
    let resp = handle(
        &mut h,
        RespValue::Array(vec![
            bulk("RESTORE"),
            bulk("s2"),
            bulk("5000"),
            bulk_bytes(dump.clone()),
        ]),
    );
    assert_eq!(resp, RespValue::ok());
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GET", "s2"]))).as_deref(),
        Some("hello")
    );
    match handle(&mut h, cmd(&["PTTL", "s2"])) {
        RespValue::Integer(t) => assert!(t > 0 && t <= 5000, "pttl={t}"),
        other => panic!("{:?}", other),
    }

    // BUSYKEY without REPLACE
    let busy = handle(
        &mut h,
        RespValue::Array(vec![
            bulk("RESTORE"),
            bulk("s2"),
            bulk("0"),
            bulk_bytes(dump.clone()),
        ]),
    );
    assert!(matches!(busy, RespValue::Error(ref e) if e.starts_with(b"BUSYKEY")));

    // REPLACE overwrites
    handle(&mut h, cmd(&["SET", "s", "world"]));
    let dump2 = as_bulk(&handle(&mut h, cmd(&["DUMP", "s"]))).unwrap();
    assert_eq!(
        handle(
            &mut h,
            RespValue::Array(vec![
                bulk("RESTORE"),
                bulk("s2"),
                bulk("0"),
                bulk_bytes(dump2),
                bulk("REPLACE"),
            ]),
        ),
        RespValue::ok()
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GET", "s2"]))).as_deref(),
        Some("world")
    );
    // ttl 0 → no expire
    assert_eq!(
        handle(&mut h, cmd(&["TTL", "s2"])),
        RespValue::Integer(-1)
    );

    // Hash round-trip
    handle(&mut h, cmd(&["HSET", "h", "f", "v", "g", "w"]));
    let hd = as_bulk(&handle(&mut h, cmd(&["DUMP", "h"]))).unwrap();
    assert_eq!(
        handle(
            &mut h,
            RespValue::Array(vec![
                bulk("RESTORE"),
                bulk("h2"),
                bulk("0"),
                bulk_bytes(hd),
            ]),
        ),
        RespValue::ok()
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["HGET", "h2", "f"]))).as_deref(),
        Some("v")
    );
    assert_eq!(
        handle(&mut h, cmd(&["HLEN", "h2"])),
        RespValue::Integer(2)
    );

    // List / set / zset
    handle(&mut h, cmd(&["LPUSH", "l", "a", "b"]));
    let ld = as_bulk(&handle(&mut h, cmd(&["DUMP", "l"]))).unwrap();
    handle(
        &mut h,
        RespValue::Array(vec![
            bulk("RESTORE"),
            bulk("l2"),
            bulk("0"),
            bulk_bytes(ld),
        ]),
    );
    assert_eq!(
        handle(&mut h, cmd(&["LLEN", "l2"])),
        RespValue::Integer(2)
    );

    handle(&mut h, cmd(&["SADD", "set", "x", "y"]));
    let sd = as_bulk(&handle(&mut h, cmd(&["DUMP", "set"]))).unwrap();
    handle(
        &mut h,
        RespValue::Array(vec![
            bulk("RESTORE"),
            bulk("set2"),
            bulk("0"),
            bulk_bytes(sd),
        ]),
    );
    assert_eq!(
        handle(&mut h, cmd(&["SCARD", "set2"])),
        RespValue::Integer(2)
    );

    handle(&mut h, cmd(&["ZADD", "z", "1", "m"]));
    let zd = as_bulk(&handle(&mut h, cmd(&["DUMP", "z"]))).unwrap();
    handle(
        &mut h,
        RespValue::Array(vec![
            bulk("RESTORE"),
            bulk("z2"),
            bulk("0"),
            bulk_bytes(zd),
        ]),
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["ZSCORE", "z2", "m"]))).as_deref(),
        Some("1")
    );

    // Bad payload
    assert!(matches!(
        handle(
            &mut h,
            RespValue::Array(vec![
                bulk("RESTORE"),
                bulk("bad"),
                bulk("0"),
                bulk("not-a-dump"),
            ]),
        ),
        RespValue::Error(_)
    ));
}

#[test]
fn expire_nx_xx_gt_lt() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["SET", "k", "v"]));

    // NX succeeds when no expire
    assert_eq!(
        handle(&mut h, cmd(&["EXPIRE", "k", "100", "NX"])),
        RespValue::Integer(1)
    );
    // NX fails when expire exists
    assert_eq!(
        handle(&mut h, cmd(&["EXPIRE", "k", "200", "NX"])),
        RespValue::Integer(0)
    );

    // XX succeeds when expire exists
    assert_eq!(
        handle(&mut h, cmd(&["EXPIRE", "k", "50", "XX"])),
        RespValue::Integer(1)
    );
    match handle(&mut h, cmd(&["TTL", "k"])) {
        RespValue::Integer(t) => assert!(t > 0 && t <= 50, "ttl={t}"),
        other => panic!("{:?}", other),
    }

    // GT: only if new > current
    assert_eq!(
        handle(&mut h, cmd(&["EXPIRE", "k", "10", "GT"])),
        RespValue::Integer(0)
    );
    assert_eq!(
        handle(&mut h, cmd(&["EXPIRE", "k", "200", "GT"])),
        RespValue::Integer(1)
    );

    // LT: only if new < current
    assert_eq!(
        handle(&mut h, cmd(&["EXPIRE", "k", "500", "LT"])),
        RespValue::Integer(0)
    );
    assert_eq!(
        handle(&mut h, cmd(&["EXPIRE", "k", "30", "LT"])),
        RespValue::Integer(1)
    );

    // Key with no expire: XX/GT/LT fail
    handle(&mut h, cmd(&["SET", "n", "v"]));
    assert_eq!(
        handle(&mut h, cmd(&["EXPIRE", "n", "10", "XX"])),
        RespValue::Integer(0)
    );
    assert_eq!(
        handle(&mut h, cmd(&["EXPIRE", "n", "10", "GT"])),
        RespValue::Integer(0)
    );
    assert_eq!(
        handle(&mut h, cmd(&["PEXPIRE", "n", "5000", "NX"])),
        RespValue::Integer(1)
    );

    // EXPIREAT with NX
    handle(&mut h, cmd(&["SET", "a", "v"]));
    let future = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        + 120;
    assert_eq!(
        handle(
            &mut h,
            cmd(&["EXPIREAT", "a", &future.to_string(), "NX"])
        ),
        RespValue::Integer(1)
    );
    assert_eq!(
        handle(
            &mut h,
            cmd(&["EXPIREAT", "a", &(future + 60).to_string(), "NX"])
        ),
        RespValue::Integer(0)
    );

    // Missing key
    assert_eq!(
        handle(&mut h, cmd(&["EXPIRE", "nope", "1", "NX"])),
        RespValue::Integer(0)
    );
}

#[test]
fn command_getkeys_and_acl_genpass() {
    let mut h = make_handler(make_cache());

    // GETKEYS MSET a b c d → a, c
    match handle(
        &mut h,
        cmd(&["COMMAND", "GETKEYS", "MSET", "a", "b", "c", "d"]),
    ) {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 2);
            assert_eq!(as_bulk_str(&items[0]).as_deref(), Some("a"));
            assert_eq!(as_bulk_str(&items[1]).as_deref(), Some("c"));
        }
        other => panic!("{:?}", other),
    }

    match handle(&mut h, cmd(&["COMMAND", "GETKEYS", "GET", "mykey"])) {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 1);
            assert_eq!(as_bulk_str(&items[0]).as_deref(), Some("mykey"));
        }
        other => panic!("{:?}", other),
    }

    // Commands with no keys
    match handle(&mut h, cmd(&["COMMAND", "GETKEYS", "PING"])) {
        RespValue::Array(items) => assert!(items.is_empty()),
        other => panic!("{:?}", other),
    }

    // EVAL numkeys
    match handle(
        &mut h,
        cmd(&[
            "COMMAND",
            "GETKEYS",
            "EVAL",
            "return 1",
            "2",
            "k1",
            "k2",
            "arg",
        ]),
    ) {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 2);
            assert_eq!(as_bulk_str(&items[0]).as_deref(), Some("k1"));
            assert_eq!(as_bulk_str(&items[1]).as_deref(), Some("k2"));
        }
        other => panic!("{:?}", other),
    }

    // Unknown command
    assert!(matches!(
        handle(&mut h, cmd(&["COMMAND", "GETKEYS", "NOPE"])),
        RespValue::Error(_)
    ));

    // ACL GENPASS default 256 bits → 64 hex chars
    match handle(&mut h, cmd(&["ACL", "GENPASS"])) {
        RespValue::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert_eq!(s.len(), 64);
            assert!(s.chars().all(|c| c.is_ascii_hexdigit()));
        }
        other => panic!("{:?}", other),
    }
    match handle(&mut h, cmd(&["ACL", "GENPASS", "64"])) {
        RespValue::BulkString(Some(b)) => {
            assert_eq!(b.len(), 16); // 64 bits → 16 hex
        }
        other => panic!("{:?}", other),
    }
    // Two genpasses should almost always differ
    let p1 = as_bulk_str(&handle(&mut h, cmd(&["ACL", "GENPASS"]))).unwrap();
    let p2 = as_bulk_str(&handle(&mut h, cmd(&["ACL", "GENPASS"]))).unwrap();
    assert_ne!(p1, p2);

    // Catalog has dump/restore
    match handle(&mut h, cmd(&["COMMAND", "INFO", "dump", "restore"])) {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 2);
            assert!(!matches!(items[0], RespValue::BulkString(None) | RespValue::Null));
            assert!(!matches!(items[1], RespValue::BulkString(None) | RespValue::Null));
        }
        other => panic!("{:?}", other),
    }

    let _ = thread::sleep(Duration::from_millis(1));
}
