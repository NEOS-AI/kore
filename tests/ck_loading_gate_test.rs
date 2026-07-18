//! Batch CK: LOADING gate during multi-DB keyspace replace; typed TTL round-trip.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::databases::Databases;
use kore::protocol::RespValue;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn make_config() -> Arc<Config> {
    Arc::new(Config {
        host: "127.0.0.1".to_string(),
        port: 6379,
        threads: 1,
        shards: 8,
        maxmemory: 1024 * 1024 * 50,
        evict: true,
        autosweep: false,
        loadfactor: 0.75,
        maxconns: 10,
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
        save: "".to_string(),
        maxmemory_policy: "allkeys-lru".to_string(),
        databases: 16,
        metrics_port: 0,
        tls: false,
        tls_cert: String::new(),
        tls_key: String::new(),
        aclfile: String::new(),
        cluster_enabled: false,
        unixsocket: String::new(),
    })
}

fn make_handler(dbs: Arc<Databases>) -> CommandHandler {
    CommandHandler::with_databases(dbs, make_config(), None)
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

fn is_loading(resp: &RespValue) -> bool {
    match resp {
        RespValue::Error(e) => String::from_utf8_lossy(e).contains("LOADING"),
        _ => false,
    }
}

#[test]
fn set_denied_while_load_in_progress_ping_and_info_allowed() {
    let dbs = Databases::create(4, 8, 1024 * 1024 * 50, 500 * 1024 * 1024, false, 0.75);
    let mut h = make_handler(dbs.clone());

    dbs.with_load_in_progress_flag(|| {
        let set = handle(&mut h, cmd(&["SET", "k", "v"]));
        assert!(is_loading(&set), "SET should LOADING, got {:?}", set);

        let get = handle(&mut h, cmd(&["GET", "k"]));
        assert!(is_loading(&get), "GET should LOADING, got {:?}", get);

        let ping = handle(&mut h, cmd(&["PING"]));
        assert!(
            matches!(ping, RespValue::SimpleString(ref s) if s.as_ref() == b"PONG"),
            "PING allowed during load, got {:?}",
            ping
        );

        let info = handle(&mut h, cmd(&["INFO", "persistence"]));
        match info {
            RespValue::BulkString(Some(b)) => {
                let s = String::from_utf8_lossy(&b);
                assert!(
                    s.contains("loading:1"),
                    "INFO should report loading:1, got:\n{s}"
                );
            }
            other => panic!("expected bulk INFO, got {:?}", other),
        }
    });

    assert!(matches!(
        handle(&mut h, cmd(&["SET", "k", "v"])),
        RespValue::SimpleString(_)
    ));
    let info = handle(&mut h, cmd(&["INFO", "persistence"]));
    if let RespValue::BulkString(Some(b)) = info {
        assert!(
            String::from_utf8_lossy(&b).contains("loading:0"),
            "loading should be 0 after load"
        );
    }
}

#[test]
fn sync_and_psync_denied_while_load_in_progress() {
    // Batch CR: fullresync must not snapshot mid-install torn keyspace maps.
    let dbs = Databases::create(4, 8, 1024 * 1024 * 50, 500 * 1024 * 1024, false, 0.75);
    let mut h = make_handler(dbs.clone());

    dbs.with_load_in_progress_flag(|| {
        let sync = handle(&mut h, cmd(&["SYNC"]));
        assert!(
            is_loading(&sync),
            "SYNC should LOADING during multi-DB replace, got {:?}",
            sync
        );

        let psync = handle(&mut h, cmd(&["PSYNC", "?", "-1"]));
        assert!(
            is_loading(&psync),
            "PSYNC should LOADING during multi-DB replace, got {:?}",
            psync
        );

        // Repl handshake / discovery still allowed (no keyspace snapshot).
        let ping = handle(&mut h, cmd(&["PING"]));
        assert!(
            matches!(ping, RespValue::SimpleString(ref s) if s.as_ref() == b"PONG"),
            "PING allowed during load, got {:?}",
            ping
        );

        let info = handle(&mut h, cmd(&["INFO", "replication"]));
        assert!(
            matches!(info, RespValue::BulkString(Some(_))),
            "INFO allowed during load, got {:?}",
            info
        );

        let role = handle(&mut h, cmd(&["ROLE"]));
        assert!(
            matches!(role, RespValue::Array(_)),
            "ROLE allowed during load, got {:?}",
            role
        );

        let replconf = handle(&mut h, cmd(&["REPLCONF", "listening-port", "6380"]));
        assert!(
            matches!(replconf, RespValue::SimpleString(ref s) if s.as_ref() == b"OK"),
            "REPLCONF allowed during load, got {:?}",
            replconf
        );

        // Data plane still gated (regression).
        let set = handle(&mut h, cmd(&["SET", "k", "v"]));
        assert!(is_loading(&set), "SET should LOADING, got {:?}", set);
    });
}

#[test]
fn watch_and_exec_denied_while_load_in_progress() {
    let dbs = Databases::create(4, 8, 1024 * 1024 * 50, 500 * 1024 * 1024, false, 0.75);
    let mut h = make_handler(dbs.clone());
    assert!(matches!(
        handle(&mut h, cmd(&["SET", "w", "1"])),
        RespValue::SimpleString(_)
    ));

    dbs.with_load_in_progress_flag(|| {
        for (name, parts) in [
            ("WATCH", &["WATCH", "w"][..]),
            ("MULTI", &["MULTI"][..]),
            ("EXEC", &["EXEC"][..]),
            ("SET", &["SET", "w", "2"][..]),
        ] {
            let resp = handle(&mut h, cmd(parts));
            assert!(
                is_loading(&resp),
                "{name} should LOADING during multi-DB replace, got {:?}",
                resp
            );
        }
    });

    // After load flag clears, WATCH/EXEC work
    assert!(matches!(
        handle(&mut h, cmd(&["WATCH", "w"])),
        RespValue::SimpleString(_)
    ));
}

#[test]
fn typed_ttl_survives_rdb_snapshot_replace() {
    let dbs = Databases::create(2, 4, 1024 * 1024 * 50, 500 * 1024 * 1024, false, 0.75);
    let mut h = make_handler(dbs.clone());
    assert!(matches!(
        handle(&mut h, cmd(&["HSET", "th", "f", "1"])),
        RespValue::Integer(_)
    ));
    assert!(matches!(
        handle(&mut h, cmd(&["PEXPIRE", "th", "60000"])),
        RespValue::Integer(1)
    ));

    let dir = {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "kore-ck-ttl-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    };
    let path = dir.join("d.rdb");
    kore::persistence::rdb::save_databases(&dbs, &path).unwrap();
    let loaded = Databases::create(2, 4, 1024 * 1024 * 50, 500 * 1024 * 1024, false, 0.75);
    kore::persistence::rdb::load_databases(&loaded, &path, true).unwrap();
    let mut h2 = make_handler(loaded);
    let ttl_after = handle(&mut h2, cmd(&["PTTL", "th"]));
    let pt_after = match ttl_after {
        RespValue::Integer(n) => n,
        other => panic!("PTTL after {:?}", other),
    };
    assert!(
        pt_after > 0,
        "typed TTL must survive RDB replace path, got {pt_after}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
