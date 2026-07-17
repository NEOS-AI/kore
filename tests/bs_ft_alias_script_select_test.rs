//! Batch BS: FT.ALIASADD/DEL/UPDATE, alias resolution, Lua SELECT/FLUSHDB.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::protocol::RespValue;
use kore::Databases;
use std::sync::Arc;

fn test_config() -> Config {
    Config {
        host: "127.0.0.1".to_string(),
        port: 6382,
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
        dir: "/tmp/kore-bs-data".to_string(),
        dbfilename: "bs.rdb".to_string(),
        appendonly: true,
        appendfilename: "bs.aof".to_string(),
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
        unixsocket: "/tmp/kore-bs.sock".to_string(),
    }
}

fn make_handler() -> CommandHandler {
    let databases = Databases::create(16, 16, 1024 * 1024 * 100, 500 * 1024 * 1024, false, 0.75);
    CommandHandler::with_databases(databases, Arc::new(test_config()), None)
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

fn search_total(resp: RespValue) -> i64 {
    match resp {
        RespValue::Array(a) => match a.first() {
            Some(RespValue::Integer(n)) => *n,
            _ => panic!("FT.SEARCH reply missing total integer: {:?}", a),
        },
        RespValue::Error(e) => panic!("FT.SEARCH error: {}", String::from_utf8_lossy(&e)),
        other => panic!("expected FT.SEARCH array, got {:?}", other),
    }
}

fn create_blog_index(h: &mut CommandHandler, name: &str) {
    assert_eq!(
        handle(
            h,
            cmd(&[
                "FT.CREATE",
                name,
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "doc:",
                "SCHEMA",
                "title",
                "TEXT",
                "tags",
                "TAG",
            ])
        ),
        RespValue::ok()
    );
}

#[test]
fn bs_ft_alias_add_search_info_tagvals() {
    let mut h = make_handler();
    create_blog_index(&mut h, "articles");

    assert_eq!(
        handle(&mut h, cmd(&["HSET", "doc:1", "title", "hello rust", "tags", "rust,db"])),
        RespValue::Integer(2)
    );

    // ALIASADD alias index
    assert_eq!(
        handle(&mut h, cmd(&["FT.ALIASADD", "blog", "articles"])),
        RespValue::ok()
    );

    // search via alias
    assert!(search_total(handle(&mut h, cmd(&["FT.SEARCH", "blog", "rust"]))) >= 1);
    assert!(search_total(handle(&mut h, cmd(&["FT.SEARCH", "articles", "rust"]))) >= 1);

    // INFO via alias
    match handle(&mut h, cmd(&["FT.INFO", "blog"])) {
        RespValue::Array(a) => {
            // index_name field should resolve to real name
            let mut found = false;
            let mut i = 0;
            while i + 1 < a.len() {
                if as_bulk_str(&a[i]).as_deref() == Some("index_name") {
                    assert_eq!(as_bulk_str(&a[i + 1]).as_deref(), Some("articles"));
                    found = true;
                    break;
                }
                i += 2;
            }
            assert!(found, "index_name missing in FT.INFO: {:?}", a);
        }
        other => panic!("expected FT.INFO array, got {:?}", other),
    }

    // TAGVALS via alias
    match handle(&mut h, cmd(&["FT.TAGVALS", "blog", "tags"])) {
        RespValue::Array(a) => {
            let mut vals: Vec<String> = a.iter().filter_map(as_bulk_str).collect();
            vals.sort();
            assert_eq!(vals, vec!["db".to_string(), "rust".to_string()]);
        }
        other => panic!("expected TAGVALS array, got {:?}", other),
    }

    // duplicate alias fails
    match handle(&mut h, cmd(&["FT.ALIASADD", "blog", "articles"])) {
        RespValue::Error(e) => {
            let s = String::from_utf8_lossy(&e);
            assert!(s.to_ascii_lowercase().contains("already"), "{}", s);
        }
        other => panic!("expected duplicate alias error, got {:?}", other),
    }

    // unknown index fails
    match handle(&mut h, cmd(&["FT.ALIASADD", "x", "nope"])) {
        RespValue::Error(e) => {
            let s = String::from_utf8_lossy(&e);
            assert!(s.to_ascii_lowercase().contains("unknown") || s.contains("nope"), "{}", s);
        }
        other => panic!("expected unknown index error, got {:?}", other),
    }
}

#[test]
fn bs_ft_alias_update_del_dropindex() {
    let mut h = make_handler();
    create_blog_index(&mut h, "idx_a");
    create_blog_index(&mut h, "idx_b");

    assert_eq!(
        handle(&mut h, cmd(&["HSET", "doc:a", "title", "alpha", "tags", "a"])),
        RespValue::Integer(2)
    );
    // idx_b uses same PREFIX so both get the doc on auto-index — use distinct prefixes
    // via a second index with different prefix for clearer retarget semantics.
    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "FT.CREATE",
                "idx_c",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "post:",
                "SCHEMA",
                "title",
                "TEXT",
                "tags",
                "TAG",
            ])
        ),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["HSET", "post:1", "title", "gamma", "tags", "c"])),
        RespValue::Integer(2)
    );

    assert_eq!(
        handle(&mut h, cmd(&["FT.ALIASADD", "live", "idx_a"])),
        RespValue::ok()
    );
    assert!(search_total(handle(&mut h, cmd(&["FT.SEARCH", "live", "alpha"]))) >= 1);

    // ALIASUPDATE retargets to idx_c
    assert_eq!(
        handle(&mut h, cmd(&["FT.ALIASUPDATE", "live", "idx_c"])),
        RespValue::ok()
    );
    assert_eq!(
        search_total(handle(&mut h, cmd(&["FT.SEARCH", "live", "alpha"]))),
        0
    );
    assert!(search_total(handle(&mut h, cmd(&["FT.SEARCH", "live", "gamma"]))) >= 1);

    // ALIASUPDATE can create a brand-new alias
    assert_eq!(
        handle(&mut h, cmd(&["FT.ALIASUPDATE", "fresh", "idx_a"])),
        RespValue::ok()
    );
    assert!(search_total(handle(&mut h, cmd(&["FT.SEARCH", "fresh", "alpha"]))) >= 1);

    // ALIASDEL
    assert_eq!(
        handle(&mut h, cmd(&["FT.ALIASDEL", "fresh"])),
        RespValue::ok()
    );
    match handle(&mut h, cmd(&["FT.SEARCH", "fresh", "alpha"])) {
        RespValue::Error(e) => {
            let s = String::from_utf8_lossy(&e);
            assert!(s.contains("not found") || s.contains("fresh"), "{}", s);
        }
        other => panic!("expected missing alias error, got {:?}", other),
    }
    match handle(&mut h, cmd(&["FT.ALIASDEL", "fresh"])) {
        RespValue::Error(e) => {
            let s = String::from_utf8_lossy(&e);
            assert!(s.to_ascii_lowercase().contains("not") || s.contains("fresh"), "{}", s);
        }
        other => panic!("expected second del error, got {:?}", other),
    }

    // DROPINDEX by real name cleans aliases
    assert_eq!(
        handle(&mut h, cmd(&["FT.DROPINDEX", "idx_c"])),
        RespValue::ok()
    );
    match handle(&mut h, cmd(&["FT.SEARCH", "live", "gamma"])) {
        RespValue::Error(e) => {
            let s = String::from_utf8_lossy(&e);
            assert!(s.contains("not found") || s.contains("live") || s.contains("idx_c"), "{}", s);
        }
        other => panic!("expected error after drop cleaned alias, got {:?}", other),
    }

    // re-add alias and DROPINDEX via alias name
    assert_eq!(
        handle(&mut h, cmd(&["FT.ALIASADD", "dropme", "idx_a"])),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["FT.DROPINDEX", "dropme"])),
        RespValue::ok()
    );
    match handle(&mut h, cmd(&["FT.SEARCH", "idx_a", "alpha"])) {
        RespValue::Error(_) => {}
        other => panic!("expected index gone after drop via alias, got {:?}", other),
    }
    match handle(&mut h, cmd(&["FT.ALIASDEL", "dropme"])) {
        RespValue::Error(_) => {}
        other => panic!("expected alias cleaned on drop, got {:?}", other),
    }
}

#[test]
fn bs_ft_alias_command_catalog() {
    let mut h = make_handler();
    for name in ["FT.ALIASADD", "FT.ALIASDEL", "FT.ALIASUPDATE"] {
        match handle(&mut h, cmd(&["COMMAND", "INFO", name])) {
            RespValue::Array(a) => {
                assert!(!a.is_empty());
                match &a[0] {
                    RespValue::Array(spec) if !spec.is_empty() => {
                        let expected = name.to_ascii_lowercase();
                        assert_eq!(as_bulk_str(&spec[0]).as_deref(), Some(expected.as_str()));
                    }
                    RespValue::Null | RespValue::BulkString(None) => {
                        panic!("{} not in COMMAND catalog", name);
                    }
                    other => panic!("unexpected COMMAND INFO entry for {}: {:?}", name, other),
                }
            }
            other => panic!("expected COMMAND INFO array for {}, got {:?}", name, other),
        }
    }
}

#[test]
fn bs_script_select_and_flushdb() {
    let mut h = make_handler();

    // SELECT via redis.call switches DB for subsequent ops in the same script
    let resp = handle(
        &mut h,
        cmd(&[
            "EVAL",
            r#"
                redis.call('SET', 'k', 'db0')
                redis.call('SELECT', 1)
                redis.call('SET', 'k', 'db1')
                local v1 = redis.call('GET', 'k')
                redis.call('SELECT', 0)
                local v0 = redis.call('GET', 'k')
                return {v0, v1}
            "#,
            "0",
        ]),
    );
    match resp {
        RespValue::Array(a) => {
            assert_eq!(a.len(), 2);
            assert_eq!(as_bulk_str(&a[0]).as_deref(), Some("db0"));
            assert_eq!(as_bulk_str(&a[1]).as_deref(), Some("db1"));
        }
        other => panic!("expected array from SELECT script, got {:?}", other),
    }

    // Connection should still be on DB 0 after script (Redis keeps SELECT from script)
    // Actually Redis DOES keep the SELECT from inside the script on the connection.
    // Verify key on current DB:
    assert_eq!(
        handle(&mut h, cmd(&["GET", "k"])),
        RespValue::BulkString(Some(Bytes::from_static(b"db0")))
    );

    // Switch to DB 1 and confirm
    assert_eq!(handle(&mut h, cmd(&["SELECT", "1"])), RespValue::ok());
    assert_eq!(
        handle(&mut h, cmd(&["GET", "k"])),
        RespValue::BulkString(Some(Bytes::from_static(b"db1")))
    );

    // FLUSHDB via redis.call on DB 1
    let resp = handle(
        &mut h,
        cmd(&[
            "EVAL",
            "redis.call('FLUSHDB'); return redis.call('GET', 'k')",
            "0",
        ]),
    );
    assert_eq!(resp, RespValue::BulkString(None));
    assert_eq!(
        handle(&mut h, cmd(&["GET", "k"])),
        RespValue::BulkString(None)
    );

    // DB 0 still has its key
    assert_eq!(handle(&mut h, cmd(&["SELECT", "0"])), RespValue::ok());
    assert_eq!(
        handle(&mut h, cmd(&["GET", "k"])),
        RespValue::BulkString(Some(Bytes::from_static(b"db0")))
    );

    // SELECT out of range from script
    match handle(
        &mut h,
        cmd(&["EVAL", "return redis.call('SELECT', 99)", "0"]),
    ) {
        RespValue::Error(e) => {
            let s = String::from_utf8_lossy(&e);
            assert!(s.contains("out of range") || s.contains("ERR"), "{}", s);
        }
        other => panic!("expected out-of-range error, got {:?}", other),
    }
}
