//! Phase C P1: HELLO / CLIENT / COMMAND

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::protocol::RespValue;
use kore::Cache;
use std::sync::Arc;

fn make_cache() -> Arc<Cache> {
    Cache::new_with_sweep(16, 1024 * 1024 * 50, 500 * 1024 * 1024, false)
}

fn make_handler_with_auth(cache: Arc<Cache>, auth: &str) -> CommandHandler {
    let config = Config {
        host: "127.0.0.1".to_string(),
        port: 6379,
        threads: 1,
        shards: 16,
        maxmemory: 1024 * 1024 * 50,
        evict: true,
        autosweep: false,
        loadfactor: 0.75,
        maxconns: 100,
        auth: auth.to_string(),
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
            log_format: "text".to_string(),
    };
    let mut h = CommandHandler::new(cache, Arc::new(config));
    h.set_client_id(42);
    h
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

fn array_bulk_map(v: &RespValue) -> std::collections::HashMap<String, RespValue> {
    let arr = match v {
        RespValue::Array(a) => a,
        other => panic!("expected array, got {:?}", other),
    };
    let mut map = std::collections::HashMap::new();
    let mut i = 0;
    while i + 1 < arr.len() {
        if let RespValue::BulkString(Some(k)) = &arr[i] {
            map.insert(String::from_utf8_lossy(k).into_owned(), arr[i + 1].clone());
        }
        i += 2;
    }
    map
}

#[test]
fn hello_basic_resp2() {
    let mut h = make_handler_with_auth(make_cache(), "");
    let resp = handle(&mut h, cmd(&["HELLO"]));
    let map = array_bulk_map(&resp);
    assert_eq!(
        map.get("server"),
        Some(&bulk("kore"))
    );
    assert_eq!(map.get("proto"), Some(&RespValue::Integer(2)));
    assert_eq!(map.get("id"), Some(&RespValue::Integer(42)));
    assert_eq!(map.get("mode"), Some(&bulk("standalone")));
    assert_eq!(map.get("role"), Some(&bulk("master")));
}

#[test]
fn hello_basic_resp3() {
    let mut h = make_handler_with_auth(make_cache(), "");
    let resp = handle(&mut h, cmd(&["HELLO", "3"]));
    match &resp {
        RespValue::Map(pairs) => {
            let mut map = std::collections::HashMap::new();
            for (k, v) in pairs {
                if let Some(kb) = k.as_bulk_string() {
                    map.insert(String::from_utf8_lossy(kb).into_owned(), v.clone());
                }
            }
            assert_eq!(map.get("server"), Some(&bulk("kore")));
            assert_eq!(map.get("proto"), Some(&RespValue::Integer(3)));
            assert_eq!(map.get("mode"), Some(&bulk("standalone")));
            assert_eq!(map.get("role"), Some(&bulk("master")));
        }
        other => panic!("expected RESP3 map, got {:?}", other),
    }
    assert_eq!(h.protocol_version(), 3);
    // Wire starts with %
    let wire = resp.serialize();
    assert!(wire.starts_with(b"%"), "expected map wire, got {:?}", wire);

    // CLIENT INFO reflects proto 3
    let info = handle(&mut h, cmd(&["CLIENT", "INFO"]));
    match info {
        RespValue::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("resp=3"), "{}", s);
        }
        other => panic!("{:?}", other),
    }

    // HELLO 2 switches back
    let r2 = handle(&mut h, cmd(&["HELLO", "2"]));
    assert!(matches!(r2, RespValue::Array(_)));
    assert_eq!(h.protocol_version(), 2);

    // Unsupported version still NOPROTO
    match handle(&mut h, cmd(&["HELLO", "4"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("NOPROTO")),
        other => panic!("expected NOPROTO, got {:?}", other),
    }
}

#[test]
fn hello_with_auth_and_setname() {
    let mut h = make_handler_with_auth(make_cache(), "s3cret");
    // Unauthenticated SET should fail
    match handle(&mut h, cmd(&["SET", "a", "1"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("NOAUTH")),
        other => panic!("expected NOAUTH, got {:?}", other),
    }

    let resp = handle(
        &mut h,
        cmd(&["HELLO", "2", "AUTH", "default", "s3cret", "SETNAME", "myapp"]),
    );
    let map = array_bulk_map(&resp);
    assert_eq!(map.get("server"), Some(&bulk("kore")));

    // Now authenticated
    assert_eq!(handle(&mut h, cmd(&["SET", "a", "1"])), RespValue::ok());
    assert_eq!(
        handle(&mut h, cmd(&["CLIENT", "GETNAME"])),
        bulk("myapp")
    );
}

#[test]
fn hello_wrong_password() {
    let mut h = make_handler_with_auth(make_cache(), "s3cret");
    match handle(&mut h, cmd(&["HELLO", "2", "AUTH", "wrong"])) {
        RespValue::Error(e) => assert!(String::from_utf8_lossy(&e).contains("WRONGPASS")),
        other => panic!("expected WRONGPASS, got {:?}", other),
    }
}

#[test]
fn client_id_setname_getname_setinfo() {
    let mut h = make_handler_with_auth(make_cache(), "");
    assert_eq!(handle(&mut h, cmd(&["CLIENT", "ID"])), RespValue::Integer(42));
    assert_eq!(handle(&mut h, cmd(&["CLIENT", "GETNAME"])), RespValue::null());
    assert_eq!(
        handle(&mut h, cmd(&["CLIENT", "SETNAME", "worker-1"])),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["CLIENT", "GETNAME"])),
        bulk("worker-1")
    );
    assert_eq!(
        handle(&mut h, cmd(&["CLIENT", "SETINFO", "lib-name", "redis-rs"])),
        RespValue::ok()
    );

    match handle(&mut h, cmd(&["CLIENT", "LIST"])) {
        RespValue::BulkString(Some(s)) => {
            let text = String::from_utf8_lossy(&s);
            assert!(text.contains("id=42"));
            assert!(text.contains("name=worker-1"));
        }
        other => panic!("expected bulk string, got {:?}", other),
    }
}

#[test]
fn command_count_list_info() {
    let mut h = make_handler_with_auth(make_cache(), "");

    match handle(&mut h, cmd(&["COMMAND", "COUNT"])) {
        RespValue::Integer(n) => assert!(n > 50, "expected substantial catalog, got {}", n),
        other => panic!("expected integer, got {:?}", other),
    }

    match handle(&mut h, cmd(&["COMMAND", "LIST"])) {
        RespValue::Array(arr) => {
            assert!(arr.len() > 50);
            let names: Vec<String> = arr
                .iter()
                .filter_map(|v| match v {
                    RespValue::BulkString(Some(b)) => {
                        Some(String::from_utf8_lossy(b).into_owned())
                    }
                    _ => None,
                })
                .collect();
            assert!(names.iter().any(|n| n == "get"));
            assert!(names.iter().any(|n| n == "hello"));
        }
        other => panic!("expected array, got {:?}", other),
    }

    match handle(&mut h, cmd(&["COMMAND", "INFO", "get", "nosuch"])) {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            match &arr[0] {
                RespValue::Array(info) => {
                    assert_eq!(info[0], bulk("get"));
                    assert_eq!(info[1], RespValue::Integer(2));
                }
                other => panic!("expected command info array, got {:?}", other),
            }
            assert_eq!(arr[1], RespValue::null());
        }
        other => panic!("expected array, got {:?}", other),
    }

    match handle(&mut h, cmd(&["COMMAND"])) {
        RespValue::Array(arr) => assert!(arr.len() > 50),
        other => panic!("expected full catalog array, got {:?}", other),
    }
}

#[test]
fn reset_clears_client_name() {
    let mut h = make_handler_with_auth(make_cache(), "");
    handle(&mut h, cmd(&["CLIENT", "SETNAME", "tmp"]));
    assert_eq!(handle(&mut h, cmd(&["CLIENT", "GETNAME"])), bulk("tmp"));
    assert_eq!(
        handle(&mut h, cmd(&["RESET"])),
        RespValue::SimpleString(Bytes::from_static(b"RESET"))
    );
    assert_eq!(handle(&mut h, cmd(&["CLIENT", "GETNAME"])), RespValue::null());
}

#[test]
fn resp3_hgetall_returns_map() {
    let mut h = make_handler_with_auth(make_cache(), "");
    assert!(matches!(
        handle(&mut h, cmd(&["HELLO", "3"])),
        RespValue::Map(_)
    ));
    assert_eq!(
        handle(&mut h, cmd(&["HSET", "u", "a", "1", "b", "2"])),
        RespValue::Integer(2)
    );
    match handle(&mut h, cmd(&["HGETALL", "u"])) {
        RespValue::Map(pairs) => {
            assert_eq!(pairs.len(), 2);
            let mut map = std::collections::HashMap::new();
            for (k, v) in pairs {
                let kb = k.as_bulk_string().map(|b| String::from_utf8_lossy(b).into_owned()).unwrap();
                let vb = v.as_bulk_string().map(|b| String::from_utf8_lossy(b).into_owned()).unwrap();
                map.insert(kb, vb);
            }
            assert_eq!(map.get("a").map(|s| s.as_str()), Some("1"));
            assert_eq!(map.get("b").map(|s| s.as_str()), Some("2"));
        }
        other => panic!("expected map, got {:?}", other),
    }
    // Empty hash key → empty map
    match handle(&mut h, cmd(&["HGETALL", "missing"])) {
        RespValue::Map(p) => assert!(p.is_empty()),
        other => panic!("{:?}", other),
    }
    // RESP2 still flat array
    assert!(matches!(handle(&mut h, cmd(&["HELLO", "2"])), RespValue::Array(_)));
    match handle(&mut h, cmd(&["HGETALL", "u"])) {
        RespValue::Array(a) => assert_eq!(a.len(), 4),
        other => panic!("{:?}", other),
    }
}

#[test]
fn resp3_config_get_returns_map() {
    let mut h = make_handler_with_auth(make_cache(), "");
    handle(&mut h, cmd(&["HELLO", "3"]));
    match handle(&mut h, cmd(&["CONFIG", "GET", "maxmemory"])) {
        RespValue::Map(pairs) => {
            assert_eq!(pairs.len(), 1);
            assert_eq!(
                pairs[0].0.as_bulk_string().map(|b| b.as_ref()),
                Some(&b"maxmemory"[..])
            );
        }
        other => panic!("expected map, got {:?}", other),
    }
    handle(&mut h, cmd(&["HELLO", "2"]));
    match handle(&mut h, cmd(&["CONFIG", "GET", "maxmemory"])) {
        RespValue::Array(a) => assert_eq!(a.len(), 2),
        other => panic!("{:?}", other),
    }
}

#[test]
fn resp3_subscribe_confirmation_is_push() {
    let cache = make_cache();
    let mut h = make_handler_with_auth(Arc::clone(&cache), "");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (cid, _rx) = rt.block_on(cache.pubsub.register_client());
    h.set_client_id(cid);
    handle(&mut h, cmd(&["HELLO", "3"]));
    let conf = handle(&mut h, cmd(&["SUBSCRIBE", "ch"]));
    let wire = conf.serialize();
    assert!(wire.starts_with(b">"), "push wire {:?}", wire);
    match conf {
        RespValue::Push(arr) => {
            assert_eq!(
                arr[0].as_bulk_string().map(|b| b.as_ref()),
                Some(&b"subscribe"[..])
            );
            assert_eq!(arr[2], RespValue::Integer(1));
        }
        other => panic!("expected Push confirmation, got {:?}", other),
    }
    // RESP2 still array
    handle(&mut h, cmd(&["HELLO", "2"]));
    match handle(&mut h, cmd(&["SUBSCRIBE", "ch2"])) {
        RespValue::Array(arr) => {
            assert_eq!(
                arr[0].as_bulk_string().map(|b| b.as_ref()),
                Some(&b"subscribe"[..])
            );
        }
        other => panic!("expected Array confirmation, got {:?}", other),
    }
}

#[test]
fn mixed_case_command_dispatch_no_heap_path() {
    let mut h = make_handler_with_auth(make_cache(), "");
    // Lower/mixed case must resolve via stack uppercase path
    assert_eq!(
        handle(&mut h, cmd(&["ping"])),
        RespValue::SimpleString(Bytes::from_static(b"PONG"))
    );
    assert_eq!(
        handle(&mut h, cmd(&["PiNg"])),
        RespValue::SimpleString(Bytes::from_static(b"PONG"))
    );
    assert!(matches!(
        handle(&mut h, cmd(&["sEt", "k", "v"])),
        RespValue::SimpleString(_)
    ));
    assert_eq!(handle(&mut h, cmd(&["gEt", "k"])), bulk("v"));
}
