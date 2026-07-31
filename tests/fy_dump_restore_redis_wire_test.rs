//! Batch FY: DUMP/RESTORE Redis wire (core types) + KDF1 dual-detect + fixtures.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::protocol::RespValue;
use kore::rdb_object::{decode_redis_dump, encode_string_dump, redis_crc64, RdbObject};
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

fn hex_to_bytes(s: &str) -> Bytes {
    let v: Vec<u8> = (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect();
    Bytes::from(v)
}

#[test]
fn dump_emits_redis_wire_for_core_types() {
    let mut h = make_handler(make_cache());

    handle(&mut h, cmd(&["SET", "s", "hello"]));
    let d = as_bulk(&handle(&mut h, cmd(&["DUMP", "s"]))).unwrap();
    assert_eq!(d[0], 0); // RDB string
    assert_eq!(d[d.len() - 10..d.len() - 8], [9, 0]); // RDB version 9 LE
    match decode_redis_dump(&d).unwrap() {
        RdbObject::String(s) => assert_eq!(&s[..], b"hello"),
        _ => panic!(),
    }

    handle(&mut h, cmd(&["RPUSH", "l", "a", "b"]));
    let d = as_bulk(&handle(&mut h, cmd(&["DUMP", "l"]))).unwrap();
    assert_eq!(d[0], 1); // classic list

    handle(&mut h, cmd(&["SADD", "set", "x"]));
    let d = as_bulk(&handle(&mut h, cmd(&["DUMP", "set"]))).unwrap();
    assert_eq!(d[0], 2);

    handle(&mut h, cmd(&["HSET", "h", "f", "v"]));
    let d = as_bulk(&handle(&mut h, cmd(&["DUMP", "h"]))).unwrap();
    assert_eq!(d[0], 4);

    handle(&mut h, cmd(&["ZADD", "z", "1.5", "m"]));
    let d = as_bulk(&handle(&mut h, cmd(&["DUMP", "z"]))).unwrap();
    assert_eq!(d[0], 5); // ZSET_2
}

#[test]
fn roundtrip_string_list_set_hash_zset() {
    let mut h = make_handler(make_cache());

    // String
    handle(&mut h, cmd(&["SET", "s", "hello"]));
    let dump = as_bulk(&handle(&mut h, cmd(&["DUMP", "s"]))).unwrap();
    assert_eq!(
        handle(
            &mut h,
            RespValue::Array(vec![
                bulk("RESTORE"),
                bulk("s2"),
                bulk("0"),
                bulk_bytes(dump),
            ]),
        ),
        RespValue::ok()
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GET", "s2"]))).as_deref(),
        Some("hello")
    );

    // List (RPUSH order)
    handle(&mut h, cmd(&["RPUSH", "l", "a", "b", "c"]));
    let dump = as_bulk(&handle(&mut h, cmd(&["DUMP", "l"]))).unwrap();
    assert_eq!(
        handle(
            &mut h,
            RespValue::Array(vec![
                bulk("RESTORE"),
                bulk("l2"),
                bulk("0"),
                bulk_bytes(dump),
            ]),
        ),
        RespValue::ok()
    );
    match handle(&mut h, cmd(&["LRANGE", "l2", "0", "-1"])) {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(as_bulk_str(&items[0]).as_deref(), Some("a"));
            assert_eq!(as_bulk_str(&items[2]).as_deref(), Some("c"));
        }
        other => panic!("{:?}", other),
    }

    // Set
    handle(&mut h, cmd(&["SADD", "set", "x", "y"]));
    let dump = as_bulk(&handle(&mut h, cmd(&["DUMP", "set"]))).unwrap();
    handle(
        &mut h,
        RespValue::Array(vec![
            bulk("RESTORE"),
            bulk("set2"),
            bulk("0"),
            bulk_bytes(dump),
        ]),
    );
    assert_eq!(
        handle(&mut h, cmd(&["SCARD", "set2"])),
        RespValue::Integer(2)
    );
    assert_eq!(
        handle(&mut h, cmd(&["SISMEMBER", "set2", "x"])),
        RespValue::Integer(1)
    );

    // Hash
    handle(&mut h, cmd(&["HSET", "h", "f", "v", "g", "w"]));
    let dump = as_bulk(&handle(&mut h, cmd(&["DUMP", "h"]))).unwrap();
    handle(
        &mut h,
        RespValue::Array(vec![
            bulk("RESTORE"),
            bulk("h2"),
            bulk("0"),
            bulk_bytes(dump),
        ]),
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["HGET", "h2", "f"]))).as_deref(),
        Some("v")
    );
    assert_eq!(
        handle(&mut h, cmd(&["HLEN", "h2"])),
        RespValue::Integer(2)
    );

    // ZSet
    handle(&mut h, cmd(&["ZADD", "z", "1.5", "m", "2", "n"]));
    let dump = as_bulk(&handle(&mut h, cmd(&["DUMP", "z"]))).unwrap();
    handle(
        &mut h,
        RespValue::Array(vec![
            bulk("RESTORE"),
            bulk("z2"),
            bulk("0"),
            bulk_bytes(dump),
        ]),
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["ZSCORE", "z2", "m"]))).as_deref(),
        Some("1.5")
    );
    assert_eq!(
        handle(&mut h, cmd(&["ZCARD", "z2"])),
        RespValue::Integer(2)
    );
}

#[test]
fn restore_real_redis_string_fixture() {
    // Real Valkey/Redis DUMP of SET key hello (RDB version 80).
    let fixture = hex_to_bytes("000568656c6c6f5000ac5816e7fb6647fe");
    let mut h = make_handler(make_cache());
    assert_eq!(
        handle(
            &mut h,
            RespValue::Array(vec![
                bulk("RESTORE"),
                bulk("from_redis"),
                bulk("0"),
                bulk_bytes(fixture),
            ]),
        ),
        RespValue::ok()
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GET", "from_redis"]))).as_deref(),
        Some("hello")
    );
}

#[test]
fn restore_real_redis_listpack_fixtures() {
    let mut h = make_handler(make_cache());

    // LIST (quicklist2) a,b,c
    let list = hex_to_bytes(
        "12010210100000000300816102816202816302ff50000732709d0b61356a",
    );
    assert_eq!(
        handle(
            &mut h,
            RespValue::Array(vec![
                bulk("RESTORE"),
                bulk("rl"),
                bulk("0"),
                bulk_bytes(list),
            ]),
        ),
        RespValue::ok()
    );
    match handle(&mut h, cmd(&["LRANGE", "rl", "0", "-1"])) {
        RespValue::Array(items) => {
            assert_eq!(as_bulk_str(&items[0]).as_deref(), Some("a"));
            assert_eq!(as_bulk_str(&items[1]).as_deref(), Some("b"));
            assert_eq!(as_bulk_str(&items[2]).as_deref(), Some("c"));
        }
        other => panic!("{:?}", other),
    }

    // SET listpack x,y
    let set = hex_to_bytes("140d0d0000000200817802817902ff5000ba652f89b43519f7");
    assert_eq!(
        handle(
            &mut h,
            RespValue::Array(vec![
                bulk("RESTORE"),
                bulk("rs"),
                bulk("0"),
                bulk_bytes(set),
            ]),
        ),
        RespValue::ok()
    );
    assert_eq!(
        handle(&mut h, cmd(&["SCARD", "rs"])),
        RespValue::Integer(2)
    );

    // HASH listpack
    let hash =
        hex_to_bytes("1013130000000400816602817602816702817702ff500016f40c58885dd5c1");
    assert_eq!(
        handle(
            &mut h,
            RespValue::Array(vec![
                bulk("RESTORE"),
                bulk("rh"),
                bulk("0"),
                bulk_bytes(hash),
            ]),
        ),
        RespValue::ok()
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["HGET", "rh", "f"]))).as_deref(),
        Some("v")
    );

    // ZSET listpack m@1.5
    let zset =
        hex_to_bytes("110f0f0000000200816d0283312e3504ff500086f34ef1e677297e");
    assert_eq!(
        handle(
            &mut h,
            RespValue::Array(vec![
                bulk("RESTORE"),
                bulk("rz"),
                bulk("0"),
                bulk_bytes(zset),
            ]),
        ),
        RespValue::ok()
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["ZSCORE", "rz", "m"]))).as_deref(),
        Some("1.5")
    );

    // INT16-encoded string "12345"
    let num = hex_to_bytes("00c13930500052be23b60dae6f4d");
    assert_eq!(
        handle(
            &mut h,
            RespValue::Array(vec![
                bulk("RESTORE"),
                bulk("rn"),
                bulk("0"),
                bulk_bytes(num),
            ]),
        ),
        RespValue::ok()
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GET", "rn"]))).as_deref(),
        Some("12345")
    );
}

#[test]
fn restore_still_accepts_kdf1() {
    let mut h = make_handler(make_cache());

    // Geo DUMP stays KDF1 (Redis geo is a zset encoding residual).
    handle(
        &mut h,
        cmd(&["GEOADD", "g", "13.361389", "38.115556", "Palermo"]),
    );
    let dump = as_bulk(&handle(&mut h, cmd(&["DUMP", "g"]))).unwrap();
    assert!(
        dump.starts_with(b"KDF1"),
        "geo still uses KDF1, got {:?}",
        &dump[..dump.len().min(8)]
    );
    assert_eq!(
        handle(
            &mut h,
            RespValue::Array(vec![
                bulk("RESTORE"),
                bulk("g2"),
                bulk("0"),
                bulk_bytes(dump),
            ]),
        ),
        RespValue::ok()
    );
    match handle(&mut h, cmd(&["GEOPOS", "g2", "Palermo"])) {
        RespValue::Array(items) => assert_eq!(items.len(), 1),
        other => panic!("geopos {:?}", other),
    }

    // Explicit KDF1 string still restorable (dual-detect).
    let kdf1_string = {
        // magic KDF1 | type1 | flags u32 LE 0 | len u32 LE 5 | hello
        let mut v = b"KDF1".to_vec();
        v.push(1); // KDF_STRING
        v.extend_from_slice(&0u32.to_le_bytes());
        v.extend_from_slice(&5u32.to_le_bytes());
        v.extend_from_slice(b"hello");
        Bytes::from(v)
    };
    assert_eq!(
        handle(
            &mut h,
            RespValue::Array(vec![
                bulk("RESTORE"),
                bulk("kdf_s"),
                bulk("0"),
                bulk_bytes(kdf1_string),
            ]),
        ),
        RespValue::ok()
    );
    assert_eq!(
        as_bulk_str(&handle(&mut h, cmd(&["GET", "kdf_s"]))).as_deref(),
        Some("hello")
    );
}

#[test]
fn restore_ttl_replace_busykey_bad_payload() {
    let mut h = make_handler(make_cache());
    handle(&mut h, cmd(&["SET", "s", "v"]));
    let dump = as_bulk(&handle(&mut h, cmd(&["DUMP", "s"]))).unwrap();

    assert_eq!(
        handle(
            &mut h,
            RespValue::Array(vec![
                bulk("RESTORE"),
                bulk("t"),
                bulk("3000"),
                bulk_bytes(dump.clone()),
            ]),
        ),
        RespValue::ok()
    );
    match handle(&mut h, cmd(&["PTTL", "t"])) {
        RespValue::Integer(t) => assert!(t > 0 && t <= 3000, "pttl={t}"),
        other => panic!("{:?}", other),
    }

    let busy = handle(
        &mut h,
        RespValue::Array(vec![
            bulk("RESTORE"),
            bulk("t"),
            bulk("0"),
            bulk_bytes(dump.clone()),
        ]),
    );
    assert!(matches!(busy, RespValue::Error(ref e) if e.starts_with(b"BUSYKEY")));

    assert_eq!(
        handle(
            &mut h,
            RespValue::Array(vec![
                bulk("RESTORE"),
                bulk("t"),
                bulk("0"),
                bulk_bytes(dump),
                bulk("REPLACE"),
            ]),
        ),
        RespValue::ok()
    );

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

    // Corrupt CRC
    let mut good = encode_string_dump(b"x");
    let last = good.len() - 1;
    good[last] ^= 0xff;
    assert!(matches!(
        handle(
            &mut h,
            RespValue::Array(vec![
                bulk("RESTORE"),
                bulk("badcrc"),
                bulk("0"),
                bulk_bytes(Bytes::from(good)),
            ]),
        ),
        RespValue::Error(_)
    ));
}

#[test]
fn crc64_and_our_string_dump_self_consistent() {
    assert_eq!(redis_crc64(b"123456789"), 0xe9c6d914c4b8d9ca);
    let d = encode_string_dump(b"hello");
    match decode_redis_dump(&d).unwrap() {
        RdbObject::String(s) => assert_eq!(&s[..], b"hello"),
        _ => panic!(),
    }
}
