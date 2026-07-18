//! Batch BK: CLIENT TRACKING/CACHING, XSETID ENTRIESADDED/MAXDELETEDID, stream counters.

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

fn map_get<'a>(arr: &'a [RespValue], key: &str) -> Option<&'a RespValue> {
    let mut i = 0;
    while i + 1 < arr.len() {
        if as_bulk_str(&arr[i]).as_deref() == Some(key) {
            return Some(&arr[i + 1]);
        }
        i += 2;
    }
    None
}

#[test]
fn bk_client_tracking_and_caching() {
    let mut h = make_handler(make_cache());

    // Default off
    assert_eq!(
        handle(&mut h, cmd(&["CLIENT", "GETREDIR"])),
        RespValue::Integer(-1)
    );
    match handle(&mut h, cmd(&["CLIENT", "TRACKINGINFO"])) {
        RespValue::Array(m) => {
            match map_get(&m, "flags") {
                Some(RespValue::Array(f)) => {
                    assert_eq!(as_bulk_str(&f[0]).as_deref(), Some("off"));
                }
                other => panic!("{:?}", other),
            }
        }
        other => panic!("{:?}", other),
    }

    // TRACKING ON with REDIRECT and PREFIX
    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "CLIENT",
                "TRACKING",
                "ON",
                "REDIRECT",
                "42",
                "PREFIX",
                "user:",
                "PREFIX",
                "session:",
                "NOLOOP",
            ])
        ),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["CLIENT", "GETREDIR"])),
        RespValue::Integer(42)
    );
    match handle(&mut h, cmd(&["CLIENT", "TRACKINGINFO"])) {
        RespValue::Array(m) => {
            match map_get(&m, "flags") {
                Some(RespValue::Array(f)) => {
                    let flags: Vec<String> = f.iter().filter_map(|x| as_bulk_str(x)).collect();
                    assert!(flags.iter().any(|x| x == "on"), "{:?}", flags);
                    assert!(flags.iter().any(|x| x == "noloop"), "{:?}", flags);
                }
                other => panic!("{:?}", other),
            }
            assert_eq!(map_get(&m, "redirect"), Some(&RespValue::Integer(42)));
            match map_get(&m, "prefixes") {
                Some(RespValue::Array(p)) => {
                    assert_eq!(p.len(), 2);
                    assert_eq!(as_bulk_str(&p[0]).as_deref(), Some("user:"));
                    assert_eq!(as_bulk_str(&p[1]).as_deref(), Some("session:"));
                }
                other => panic!("{:?}", other),
            }
        }
        other => panic!("{:?}", other),
    }

    // CACHING without OPTIN → error
    match handle(&mut h, cmd(&["CLIENT", "CACHING", "YES"])) {
        RespValue::Error(e) => {
            let s = String::from_utf8_lossy(&e);
            assert!(s.contains("OPTIN") || s.contains("OPTOUT"), "{}", s);
        }
        other => panic!("{:?}", other),
    }

    // OPTIN + CACHING YES
    assert_eq!(
        handle(
            &mut h,
            cmd(&["CLIENT", "TRACKING", "ON", "OPTIN", "REDIRECT", "7"])
        ),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["CLIENT", "CACHING", "YES"])),
        RespValue::ok()
    );
    match handle(&mut h, cmd(&["CLIENT", "TRACKINGINFO"])) {
        RespValue::Array(m) => match map_get(&m, "flags") {
            Some(RespValue::Array(f)) => {
                let flags: Vec<String> = f.iter().filter_map(|x| as_bulk_str(x)).collect();
                assert!(flags.iter().any(|x| x == "optin"), "{:?}", flags);
                assert!(flags.iter().any(|x| x == "caching-yes"), "{:?}", flags);
            }
            other => panic!("{:?}", other),
        },
        other => panic!("{:?}", other),
    }

    // OFF clears
    assert_eq!(
        handle(&mut h, cmd(&["CLIENT", "TRACKING", "OFF"])),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["CLIENT", "GETREDIR"])),
        RespValue::Integer(-1)
    );

    // OPTIN+OPTOUT exclusive
    match handle(
        &mut h,
        cmd(&["CLIENT", "TRACKING", "ON", "OPTIN", "OPTOUT"]),
    ) {
        RespValue::Error(e) => {
            assert!(String::from_utf8_lossy(&e).contains("mutually exclusive"));
        }
        other => panic!("{:?}", other),
    }
}

#[test]
fn bk_xsetid_entriesadded_maxdeletedid() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["XADD", "s", "1-0", "f", "a"]));
    handle(&mut h, cmd(&["XADD", "s", "2-0", "f", "b"]));
    handle(&mut h, cmd(&["XADD", "s", "3-0", "f", "c"]));

    // entries-added tracks XADD count
    match handle(&mut h, cmd(&["XINFO", "STREAM", "s"])) {
        RespValue::Array(arr) => {
            assert_eq!(
                map_get(&arr, "entries-added"),
                Some(&RespValue::Integer(3))
            );
            assert_eq!(
                as_bulk_str(map_get(&arr, "max-deleted-entry-id").unwrap()).as_deref(),
                Some("0-0")
            );
        }
        other => panic!("{:?}", other),
    }

    // XDEL updates max-deleted and length but not entries-added
    assert_eq!(
        handle(&mut h, cmd(&["XDEL", "s", "2-0"])),
        RespValue::Integer(1)
    );
    match handle(&mut h, cmd(&["XINFO", "STREAM", "s"])) {
        RespValue::Array(arr) => {
            assert_eq!(map_get(&arr, "length"), Some(&RespValue::Integer(2)));
            assert_eq!(
                map_get(&arr, "entries-added"),
                Some(&RespValue::Integer(3))
            );
            assert_eq!(
                as_bulk_str(map_get(&arr, "max-deleted-entry-id").unwrap()).as_deref(),
                Some("2-0")
            );
        }
        other => panic!("{:?}", other),
    }

    // XSETID with ENTRIESADDED + MAXDELETEDID
    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "XSETID",
                "s",
                "99-0",
                "ENTRIESADDED",
                "100",
                "MAXDELETEDID",
                "50-1",
            ])
        ),
        RespValue::ok()
    );
    match handle(&mut h, cmd(&["XINFO", "STREAM", "s"])) {
        RespValue::Array(arr) => {
            assert_eq!(
                as_bulk_str(map_get(&arr, "last-generated-id").unwrap()).as_deref(),
                Some("99-0")
            );
            assert_eq!(
                map_get(&arr, "entries-added"),
                Some(&RespValue::Integer(100))
            );
            assert_eq!(
                as_bulk_str(map_get(&arr, "max-deleted-entry-id").unwrap()).as_deref(),
                Some("50-1")
            );
        }
        other => panic!("{:?}", other),
    }
}
