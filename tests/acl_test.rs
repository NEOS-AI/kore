//! Phase D P1: ACL MVP — users, AUTH, command/key permissions.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::protocol::RespValue;
use kore::Cache;
use std::sync::Arc;

fn make_cache() -> Arc<Cache> {
    Cache::new_with_sweep(16, 1024 * 1024 * 50, 500 * 1024 * 1024, false)
}

fn make_handler(auth: &str) -> CommandHandler {
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
    CommandHandler::new(make_cache(), Arc::new(config))
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

fn err_contains(resp: &RespValue, needle: &str) -> bool {
    match resp {
        RespValue::Error(e) => String::from_utf8_lossy(e).contains(needle),
        _ => false,
    }
}

fn is_ok(resp: &RespValue) -> bool {
    matches!(resp, RespValue::SimpleString(s) if s.as_ref() == b"OK")
}

fn array_as_strings(resp: &RespValue) -> Vec<String> {
    match resp {
        RespValue::Array(arr) => arr
            .iter()
            .filter_map(|v| match v {
                RespValue::BulkString(Some(b)) | RespValue::SimpleString(b) => {
                    Some(String::from_utf8_lossy(b).into_owned())
                }
                _ => None,
            })
            .collect(),
        _ => panic!("expected array, got {:?}", resp),
    }
}

fn getuser_map(resp: &RespValue) -> std::collections::HashMap<String, RespValue> {
    let arr = match resp {
        RespValue::Array(a) => a,
        other => panic!("expected GETUSER array, got {:?}", other),
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
fn acl_default_open_when_no_auth() {
    let mut h = make_handler("");
    // No --auth: connections are open (auto-authenticated as default).
    assert_eq!(
        handle(&mut h, cmd(&["PING"])),
        RespValue::SimpleString(Bytes::from_static(b"PONG"))
    );
    assert!(is_ok(&handle(&mut h, cmd(&["SET", "k", "v"]))));
    assert_eq!(handle(&mut h, cmd(&["GET", "k"])), bulk("v"));
}

#[test]
fn acl_requirepass_auth_password_only() {
    let mut h = make_handler("s3cret");

    assert!(err_contains(
        &handle(&mut h, cmd(&["PING"])),
        "NOAUTH"
    ));

    let wrong = handle(&mut h, cmd(&["AUTH", "wrong"]));
    assert!(
        err_contains(&wrong, "WRONGPASS") || err_contains(&wrong, "invalid"),
        "expected auth failure, got {:?}",
        wrong
    );

    assert!(is_ok(&handle(&mut h, cmd(&["AUTH", "s3cret"]))));
    assert_eq!(
        handle(&mut h, cmd(&["PING"])),
        RespValue::SimpleString(Bytes::from_static(b"PONG"))
    );
    assert!(is_ok(&handle(&mut h, cmd(&["SET", "a", "1"]))));
}

#[test]
fn acl_auth_username_password() {
    let mut h = make_handler("adminpass");
    assert!(is_ok(&handle(&mut h, cmd(&["AUTH", "adminpass"]))));

    // Create a named user and authenticate as them.
    assert!(is_ok(&handle(
        &mut h,
        cmd(&["ACL", "SETUSER", "alice", "on", ">alicepass", "+@all", "~*"])
    )));

    // Switch identity via AUTH username password.
    assert!(is_ok(&handle(
        &mut h,
        cmd(&["AUTH", "alice", "alicepass"])
    )));
    assert_eq!(handle(&mut h, cmd(&["ACL", "WHOAMI"])), bulk("alice"));
    assert!(is_ok(&handle(&mut h, cmd(&["SET", "x", "1"]))));
}

#[test]
fn acl_setuser_and_list() {
    let mut h = make_handler("");
    assert!(is_ok(&handle(
        &mut h,
        cmd(&["ACL", "SETUSER", "bob", "on", ">bobpass", "+@read", "~*"])
    )));

    let list = handle(&mut h, cmd(&["ACL", "LIST"]));
    let entries = array_as_strings(&list);
    assert!(
        entries.iter().any(|e| e.contains("user bob") && e.contains("on")),
        "LIST should include bob: {:?}",
        entries
    );
    assert!(
        entries.iter().any(|e| e.contains("user default")),
        "LIST should include default: {:?}",
        entries
    );
}

#[test]
fn acl_getuser_shape() {
    let mut h = make_handler("");
    assert!(is_ok(&handle(
        &mut h,
        cmd(&[
            "ACL", "SETUSER", "carol", "on", ">cpass", "+@all", "~cached:*"
        ])
    )));

    let resp = handle(&mut h, cmd(&["ACL", "GETUSER", "carol"]));
    let map = getuser_map(&resp);

    assert!(map.contains_key("flags"), "missing flags: {:?}", map.keys());
    assert!(
        map.contains_key("passwords") || map.contains_key("pass"),
        "missing passwords: {:?}",
        map.keys()
    );
    assert!(
        map.contains_key("commands"),
        "missing commands: {:?}",
        map.keys()
    );
    assert!(map.contains_key("keys"), "missing keys: {:?}", map.keys());

    // flags should mention on
    match map.get("flags") {
        Some(RespValue::Array(flags)) => {
            let flag_strs: Vec<String> = flags
                .iter()
                .filter_map(|v| v.as_bulk_string().map(|b| String::from_utf8_lossy(b).into_owned()))
                .collect();
            assert!(
                flag_strs.iter().any(|f| f == "on"),
                "expected on in flags {:?}",
                flag_strs
            );
        }
        other => panic!("flags should be array, got {:?}", other),
    }
}

#[test]
fn acl_command_deny() {
    let mut h = make_handler("adminpass");
    assert!(is_ok(&handle(&mut h, cmd(&["AUTH", "adminpass"]))));

    // Reader: only GET (and connection basics via +@connection so they can stay usable).
    assert!(is_ok(&handle(
        &mut h,
        cmd(&[
            "ACL",
            "SETUSER",
            "reader",
            "on",
            ">rpass",
            "-@all",
            "+@connection",
            "+get",
            "~*"
        ])
    )));

    assert!(is_ok(&handle(
        &mut h,
        cmd(&["AUTH", "reader", "rpass"])
    )));

    // GET allowed
    assert_eq!(handle(&mut h, cmd(&["GET", "missing"])), RespValue::null());

    // SET denied
    let deny = handle(&mut h, cmd(&["SET", "k", "v"]));
    assert!(
        err_contains(&deny, "NOPERM") && err_contains(&deny, "set"),
        "expected NOPERM for set, got {:?}",
        deny
    );
}

#[test]
fn acl_key_pattern_deny() {
    let mut h = make_handler("adminpass");
    assert!(is_ok(&handle(&mut h, cmd(&["AUTH", "adminpass"]))));

    // Seed a key as admin
    assert!(is_ok(&handle(&mut h, cmd(&["SET", "allowed:1", "yes"]))));
    assert!(is_ok(&handle(&mut h, cmd(&["SET", "secret", "no"]))));

    assert!(is_ok(&handle(
        &mut h,
        cmd(&[
            "ACL",
            "SETUSER",
            "limited",
            "on",
            ">lpass",
            "+@all",
            "~allowed:*"
        ])
    )));

    assert!(is_ok(&handle(
        &mut h,
        cmd(&["AUTH", "limited", "lpass"])
    )));

    // Key matching pattern works
    assert_eq!(handle(&mut h, cmd(&["GET", "allowed:1"])), bulk("yes"));

    // Key outside pattern denied
    let deny = handle(&mut h, cmd(&["GET", "secret"]));
    assert!(
        err_contains(&deny, "NOPERM") && err_contains(&deny, "key"),
        "expected NOPERM key, got {:?}",
        deny
    );
}

#[test]
fn acl_cat_lists_categories() {
    let mut h = make_handler("");
    let resp = handle(&mut h, cmd(&["ACL", "CAT"]));
    let cats = array_as_strings(&resp);
    for expected in ["all", "read", "write", "admin", "dangerous", "connection", "pubsub"] {
        assert!(
            cats.iter().any(|c| c == expected || c.as_str() == format!("@{}", expected)),
            "missing category {}: {:?}",
            expected,
            cats
        );
    }

    // ACL CAT read should list some read commands
    let read_cmds = array_as_strings(&handle(&mut h, cmd(&["ACL", "CAT", "read"])));
    assert!(
        read_cmds.iter().any(|c| c.eq_ignore_ascii_case("get")),
        "CAT read should include get: {:?}",
        read_cmds
    );
}

#[test]
fn acl_whoami_after_auth() {
    let mut h = make_handler("s3cret");
    assert!(is_ok(&handle(&mut h, cmd(&["AUTH", "s3cret"]))));
    assert_eq!(handle(&mut h, cmd(&["ACL", "WHOAMI"])), bulk("default"));

    assert!(is_ok(&handle(
        &mut h,
        cmd(&["ACL", "SETUSER", "dave", "on", ">dpass", "+@all", "~*"])
    )));
    assert!(is_ok(&handle(&mut h, cmd(&["AUTH", "dave", "dpass"]))));
    assert_eq!(handle(&mut h, cmd(&["ACL", "WHOAMI"])), bulk("dave"));
}

#[test]
fn acl_disabled_user_cannot_auth() {
    let mut h = make_handler("adminpass");
    assert!(is_ok(&handle(&mut h, cmd(&["AUTH", "adminpass"]))));

    assert!(is_ok(&handle(
        &mut h,
        cmd(&[
            "ACL",
            "SETUSER",
            "ghost",
            "on",
            ">gpass",
            "+@all",
            "~*"
        ])
    )));
    // Disable
    assert!(is_ok(&handle(
        &mut h,
        cmd(&["ACL", "SETUSER", "ghost", "off"])
    )));

    let resp = handle(&mut h, cmd(&["AUTH", "ghost", "gpass"]));
    assert!(
        err_contains(&resp, "WRONGPASS")
            || err_contains(&resp, "disabled")
            || err_contains(&resp, "NOPERM")
            || err_contains(&resp, "invalid"),
        "disabled user must not auth: {:?}",
        resp
    );
}

#[test]
fn acl_hello_auth_user_password() {
    let mut h = make_handler("adminpass");
    assert!(is_ok(&handle(&mut h, cmd(&["AUTH", "adminpass"]))));

    assert!(is_ok(&handle(
        &mut h,
        cmd(&[
            "ACL",
            "SETUSER",
            "hello_user",
            "on",
            ">hpass",
            "+@all",
            "~*"
        ])
    )));

    // Wrong password for the named user must fail even if it matches default's password.
    let mut h2 = make_handler("adminpass");
    // Share is per-handler in unit tests — set up user on h2 via default auth first.
    assert!(is_ok(&handle(&mut h2, cmd(&["AUTH", "adminpass"]))));
    assert!(is_ok(&handle(
        &mut h2,
        cmd(&[
            "ACL",
            "SETUSER",
            "hello_user",
            "on",
            ">hpass",
            "+@all",
            "~*"
        ])
    )));

    // Re-create unauthenticated connection simulation: new handler with same ACL needed.
    // Use SETUSER then unauth by creating handler that shares ACL — for unit test,
    // AUTH as hello_user with wrong pass via HELLO.
    let bad = handle(
        &mut h2,
        cmd(&["HELLO", "2", "AUTH", "hello_user", "adminpass"]),
    );
    assert!(
        err_contains(&bad, "WRONGPASS") || err_contains(&bad, "invalid"),
        "HELLO AUTH must validate named user password, got {:?}",
        bad
    );

    let good = handle(
        &mut h2,
        cmd(&["HELLO", "2", "AUTH", "hello_user", "hpass"]),
    );
    match good {
        RespValue::Array(_) => {}
        RespValue::Error(e) => panic!("expected HELLO ok, got error {}", String::from_utf8_lossy(&e)),
        other => panic!("expected HELLO array, got {:?}", other),
    }
    assert_eq!(handle(&mut h2, cmd(&["ACL", "WHOAMI"])), bulk("hello_user"));
}

#[test]
fn acl_auto_auth_respects_live_default_nopass() {
    use kore::acl::AclStore;
    use kore::databases::Databases;
    use kore::commands::CommandHandler;

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
        auth: String::new(), // open at start
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
    let config = Arc::new(config);
    let dbs = Databases::create(16, 16, 1024 * 1024 * 50, 500 * 1024 * 1024, false, 0.75);
    let acl = AclStore::from_auth_arc("");
    let mut admin = CommandHandler::with_databases_and_acl(dbs.clone(), config.clone(), None, acl.clone());
    // Open mode works
    assert_eq!(handle(&mut admin, cmd(&["PING"])), RespValue::SimpleString(Bytes::from_static(b"PONG")));
    // Require password on default
    assert_eq!(
        handle(&mut admin, cmd(&["ACL", "SETUSER", "default", "resetpass", ">secret"])),
        RespValue::ok()
    );
    // New connection must NOT auto-auth
    let mut client = CommandHandler::with_databases_and_acl(dbs, config, None, acl);
    let r = handle(&mut client, cmd(&["PING"]));
    assert!(err_contains(&r, "NOAUTH"), "expected NOAUTH after default lost nopass, got {:?}", r);
    // AUTH with password works
    assert_eq!(handle(&mut client, cmd(&["AUTH", "secret"])), RespValue::ok());
    assert_eq!(handle(&mut client, cmd(&["PING"])), RespValue::SimpleString(Bytes::from_static(b"PONG")));
}

#[test]
fn acl_deluser_removes_user() {
    let mut h = make_handler("");
    assert!(is_ok(&handle(
        &mut h,
        cmd(&["ACL", "SETUSER", "temp", "on", ">tpass", "+@all", "~*", "&*"])
    )));
    let users = array_as_strings(&handle(&mut h, cmd(&["ACL", "USERS"])));
    assert!(users.iter().any(|u| u == "temp"), "{:?}", users);

    let n = handle(&mut h, cmd(&["ACL", "DELUSER", "temp"]));
    assert_eq!(n, RespValue::Integer(1));
    let users = array_as_strings(&handle(&mut h, cmd(&["ACL", "USERS"])));
    assert!(!users.iter().any(|u| u == "temp"), "{:?}", users);

    // default cannot be deleted
    let bad = handle(&mut h, cmd(&["ACL", "DELUSER", "default"]));
    assert!(err_contains(&bad, "default"), "{:?}", bad);

    // missing user → 0
    assert_eq!(
        handle(&mut h, cmd(&["ACL", "DELUSER", "nope"])),
        RespValue::Integer(0)
    );
}

#[test]
fn acl_channel_pattern_deny() {
    let mut h = make_handler("adminpass");
    assert!(is_ok(&handle(&mut h, cmd(&["AUTH", "adminpass"]))));

    assert!(is_ok(&handle(
        &mut h,
        cmd(&[
            "ACL",
            "SETUSER",
            "chuser",
            "on",
            ">cpass",
            "+@all",
            "~*",
            "resetchannels",
            "&news:*"
        ])
    )));

    assert!(is_ok(&handle(
        &mut h,
        cmd(&["AUTH", "chuser", "cpass"])
    )));

    // Allowed channel
    let ok_pub = handle(&mut h, cmd(&["PUBLISH", "news:1", "hi"]));
    assert!(
        matches!(ok_pub, RespValue::Integer(_)),
        "expected integer publish, got {:?}",
        ok_pub
    );

    // Denied channel
    let deny = handle(&mut h, cmd(&["PUBLISH", "secret", "no"]));
    assert!(
        err_contains(&deny, "NOPERM") && err_contains(&deny, "channel"),
        "expected NOPERM channel, got {:?}",
        deny
    );

    let deny_sub = handle(&mut h, cmd(&["SUBSCRIBE", "secret"]));
    assert!(
        err_contains(&deny_sub, "NOPERM") && err_contains(&deny_sub, "channel"),
        "expected NOPERM channel subscribe, got {:?}",
        deny_sub
    );
}

#[test]
fn acl_load_save_roundtrip() {
    use kore::acl::AclStore;
    use kore::databases::Databases;
    use std::path::PathBuf;

    let dir = std::env::temp_dir().join(format!(
        "kore-acl-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&dir);
    let acl_path: PathBuf = dir.join("users.acl");

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
        dir: dir.to_string_lossy().to_string(),
        dbfilename: "dump.rdb".to_string(),
        appendonly: false,
        appendfilename: "appendonly.aof".to_string(),
        replicaof: String::new(),
        save: "".to_string(),
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
        aclfile: acl_path.to_string_lossy().to_string(),
        cluster_enabled: false,
            cluster_replica_priority: 100,
            cluster_require_full_coverage: true,
            cluster_allow_reads_when_down: false,
            cluster_announce_ip: String::new(),
            cluster_announce_port: 0,
    unixsocket: String::new(),
            log_format: "text".to_string(),
    };
    let config = Arc::new(config);
    let dbs = Databases::create(16, 16, 1024 * 1024 * 50, 500 * 1024 * 1024, false, 0.75);
    let acl = AclStore::from_auth_arc("");
    acl.set_aclfile(&acl_path);

    let mut h = CommandHandler::with_databases_and_acl(dbs.clone(), config.clone(), None, acl.clone());
    assert!(is_ok(&handle(
        &mut h,
        cmd(&[
            "ACL",
            "SETUSER",
            "fileuser",
            "on",
            ">fpass",
            "+@read",
            "+@connection",
            "+acl",
            "~cached:*",
            "&news:*"
        ])
    )));
    assert!(is_ok(&handle(&mut h, cmd(&["ACL", "SAVE"]))));
    assert!(acl_path.exists(), "aclfile should exist after SAVE");

    // Wipe user and reload
    assert_eq!(
        handle(&mut h, cmd(&["ACL", "DELUSER", "fileuser"])),
        RespValue::Integer(1)
    );
    assert!(!array_as_strings(&handle(&mut h, cmd(&["ACL", "USERS"])))
        .iter()
        .any(|u| u == "fileuser"));

    assert!(is_ok(&handle(&mut h, cmd(&["ACL", "LOAD"]))));
    let users = array_as_strings(&handle(&mut h, cmd(&["ACL", "USERS"])));
    assert!(
        users.iter().any(|u| u == "fileuser"),
        "LOAD should restore fileuser: {:?}",
        users
    );

    // Auth as restored user
    assert!(is_ok(&handle(
        &mut h,
        cmd(&["AUTH", "fileuser", "fpass"])
    )));
    assert_eq!(handle(&mut h, cmd(&["ACL", "WHOAMI"])), bulk("fileuser"));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn acl_save_errors_without_aclfile() {
    let mut h = make_handler("");
    let resp = handle(&mut h, cmd(&["ACL", "SAVE"]));
    assert!(
        err_contains(&resp, "ACL file") || err_contains(&resp, "aclfile"),
        "{:?}",
        resp
    );
}
