//! Lane D: Redis Cluster MVP — slots, CLUSTER cmds, MOVED/CROSSSLOT/ASK, ASKING.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::protocol::RespValue;
use kore::{key_hash_slot, Cache, ClusterState};
use std::sync::Arc;

fn make_config(cluster_enabled: bool) -> Arc<Config> {
    Arc::new(Config {
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
        cluster_enabled,
        unixsocket: String::new(),
            log_format: "text".to_string(),
    })
}

fn make_cache() -> Arc<Cache> {
    Cache::new_with_sweep(16, 1024 * 1024 * 50, 500 * 1024 * 1024, false)
}

fn make_handler(cluster_enabled: bool) -> (CommandHandler, Option<Arc<ClusterState>>) {
    let config = make_config(cluster_enabled);
    let cluster = if cluster_enabled {
        Some(ClusterState::single_node(config.host.clone(), config.port))
    } else {
        None
    };
    let h = CommandHandler::new(make_cache(), config).with_cluster(cluster.clone());
    (h, cluster)
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

fn err_str(resp: &RespValue) -> String {
    match resp {
        RespValue::Error(e) => String::from_utf8_lossy(e).into_owned(),
        other => panic!("expected error, got {:?}", other),
    }
}

fn is_ok(resp: &RespValue) -> bool {
    matches!(resp, RespValue::SimpleString(s) if s.as_ref() == b"OK")
}

fn as_bulk(resp: &RespValue) -> String {
    match resp {
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        other => panic!("expected bulk, got {:?}", other),
    }
}

fn as_int(resp: &RespValue) -> i64 {
    match resp {
        RespValue::Integer(i) => *i,
        other => panic!("expected integer, got {:?}", other),
    }
}

/// Find two keys that hash to different slots.
fn different_slot_keys() -> (&'static str, &'static str) {
    // foo=12182, bar=5061
    ("foo", "bar")
}

#[test]
fn single_node_owns_all_slots() {
    let cs = ClusterState::single_node("127.0.0.1", 6379);
    assert!(cs.owns_slot(0));
    assert!(cs.owns_slot(12182));
    assert!(cs.owns_slot(16383));
    let ranges = cs.slots_ranges();
    assert_eq!(ranges.len(), 1);
    assert_eq!((ranges[0].0, ranges[0].1), (0, 16383));
}

#[test]
fn cluster_nodes_slots_info_myid() {
    let (mut h, cluster) = make_handler(true);
    let cs = cluster.unwrap();
    let myid = cs.my_id();

    let id = as_bulk(&handle(&mut h, cmd(&["CLUSTER", "MYID"])));
    assert_eq!(id, myid);

    let nodes = as_bulk(&handle(&mut h, cmd(&["CLUSTER", "NODES"])));
    assert!(nodes.contains(&myid));
    assert!(nodes.contains("myself,master") || nodes.contains("myself"));
    assert!(nodes.contains("0-16383"));
    assert!(nodes.contains("connected"));

    let slots = handle(&mut h, cmd(&["CLUSTER", "SLOTS"]));
    match slots {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 1);
            match &arr[0] {
                RespValue::Array(entry) => {
                    assert_eq!(entry[0], RespValue::Integer(0));
                    assert_eq!(entry[1], RespValue::Integer(16383));
                    match &entry[2] {
                        RespValue::Array(node) => {
                            assert_eq!(as_bulk(&node[0]), "127.0.0.1");
                            assert_eq!(as_int(&node[1]), 6379);
                            assert_eq!(as_bulk(&node[2]), myid);
                        }
                        other => panic!("bad node entry {:?}", other),
                    }
                }
                other => panic!("bad slots entry {:?}", other),
            }
        }
        other => panic!("expected array {:?}", other),
    }

    let info = as_bulk(&handle(&mut h, cmd(&["CLUSTER", "INFO"])));
    assert!(info.contains("cluster_state:ok"));
    assert!(info.contains("cluster_slots_assigned:16384"));
    assert!(info.contains("cluster_known_nodes:1"));
}

#[test]
fn cluster_keyslot_matches_hash() {
    let (mut h, _) = make_handler(true);
    let slot = as_int(&handle(&mut h, cmd(&["CLUSTER", "KEYSLOT", "foo"])));
    assert_eq!(slot, key_hash_slot(b"foo") as i64);
    assert_eq!(slot, 12182);

    let tagged = as_int(&handle(
        &mut h,
        cmd(&["CLUSTER", "KEYSLOT", "{user1000}.following"]),
    ));
    assert_eq!(tagged, 3443);
}

#[test]
fn set_ok_when_owning_all_slots() {
    let (mut h, _) = make_handler(true);
    assert!(is_ok(&handle(&mut h, cmd(&["SET", "foo", "v"]))));
    assert_eq!(
        as_bulk(&handle(&mut h, cmd(&["GET", "foo"]))),
        "v".to_string()
    );
}

#[test]
fn synthetic_split_returns_moved() {
    let (mut h, cluster) = make_handler(true);
    let cs = cluster.unwrap();
    let other_id = "b".repeat(40);
    cs.add_node(&other_id, "10.0.0.2", 7001);

    let slot = key_hash_slot(b"foo");
    cs.reassign_slot(slot, &other_id).unwrap();
    assert!(!cs.owns_slot(slot));

    let err = err_str(&handle(&mut h, cmd(&["GET", "foo"])));
    assert!(
        err.starts_with(&format!("MOVED {} 10.0.0.2:7001", slot)),
        "got {}",
        err
    );

    // SET also MOVED
    let err = err_str(&handle(&mut h, cmd(&["SET", "foo", "x"])));
    assert!(err.starts_with("MOVED "), "got {}", err);
}

#[test]
fn crossslot_on_multi_key_different_slots() {
    let (mut h, _) = make_handler(true);
    let (a, b) = different_slot_keys();
    assert_ne!(key_hash_slot(a.as_bytes()), key_hash_slot(b.as_bytes()));

    let err = err_str(&handle(&mut h, cmd(&["MGET", a, b])));
    assert!(
        err.contains("CROSSSLOT"),
        "expected CROSSSLOT, got {}",
        err
    );

    let err = err_str(&handle(&mut h, cmd(&["DEL", a, b])));
    assert!(err.contains("CROSSSLOT"), "got {}", err);
}

#[test]
fn multi_key_same_hash_tag_ok() {
    let (mut h, _) = make_handler(true);
    assert!(is_ok(&handle(
        &mut h,
        cmd(&["MSET", "{user}.a", "1", "{user}.b", "2"])
    )));
    match handle(&mut h, cmd(&["MGET", "{user}.a", "{user}.b"])) {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(as_bulk(&arr[0]), "1");
            assert_eq!(as_bulk(&arr[1]), "2");
        }
        other => panic!("{:?}", other),
    }
}

#[test]
fn select_rejected_in_cluster_mode() {
    let (mut h, _) = make_handler(true);
    let err = err_str(&handle(&mut h, cmd(&["SELECT", "1"])));
    assert!(
        err.contains("SELECT is not allowed in cluster mode"),
        "got {}",
        err
    );
}

#[test]
fn asking_and_setslot_importing_stub() {
    let (mut h, cluster) = make_handler(true);
    let cs = cluster.unwrap();
    let other_id = "c".repeat(40);
    cs.add_node(&other_id, "10.0.0.3", 7002);

    let slot = key_hash_slot(b"foo");
    // Simulate slot owned by peer, we are importing it
    cs.reassign_slot(slot, &other_id).unwrap();
    cs.set_importing(slot, &other_id).unwrap();

    // Without ASKING → MOVED
    let err = err_str(&handle(&mut h, cmd(&["GET", "foo"])));
    assert!(err.starts_with("MOVED "), "got {}", err);

    // ASKING then GET is allowed (one-shot)
    assert!(is_ok(&handle(&mut h, cmd(&["ASKING"]))));
    // Key missing → null, but not MOVED
    match handle(&mut h, cmd(&["GET", "foo"])) {
        RespValue::BulkString(None) => {}
        RespValue::Error(e) => panic!("unexpected error after ASKING: {}", String::from_utf8_lossy(&e)),
        other => panic!("expected null bulk, got {:?}", other),
    }

    // Flag consumed — next GET without ASKING is MOVED again
    let err = err_str(&handle(&mut h, cmd(&["GET", "foo"])));
    assert!(err.starts_with("MOVED "), "got {}", err);

    // SETSLOT STABLE clears importing
    assert!(is_ok(&handle(
        &mut h,
        cmd(&["CLUSTER", "SETSLOT", &slot.to_string(), "STABLE"])
    )));
}

#[test]
fn setslot_migrating_ask_same_slot() {
    let (mut h, cluster) = make_handler(true);
    let cs = cluster.unwrap();
    let other_id = "e".repeat(40);
    cs.add_node(&other_id, "10.0.0.5", 7004);

    // Keys with same tag share slot
    let slot = key_hash_slot(b"{m}.a");
    assert_eq!(slot, key_hash_slot(b"{m}.b"));

    // Create key while we still stably own the slot, then mark MIGRATING
    assert!(is_ok(&handle(&mut h, cmd(&["SET", "{m}.a", "1"]))));

    assert!(is_ok(&handle(
        &mut h,
        cmd(&[
            "CLUSTER",
            "SETSLOT",
            &slot.to_string(),
            "MIGRATING",
            &other_id
        ])
    )));

    // present → served locally even while migrating
    assert_eq!(as_bulk(&handle(&mut h, cmd(&["GET", "{m}.a"]))), "1");

    // missing same slot → ASK
    let err = err_str(&handle(&mut h, cmd(&["GET", "{m}.b"])));
    assert!(
        err.starts_with(&format!("ASK {} 10.0.0.5:7004", slot)),
        "got {}",
        err
    );

    // SETSLOT NODE reassigns ownership
    assert!(is_ok(&handle(
        &mut h,
        cmd(&[
            "CLUSTER",
            "SETSLOT",
            &slot.to_string(),
            "NODE",
            &other_id
        ])
    )));
    let err = err_str(&handle(&mut h, cmd(&["GET", "{m}.a"])));
    assert!(err.starts_with("MOVED "), "got {}", err);
}

#[test]
fn cluster_off_no_moved_no_cluster_cmds() {
    let (mut h, _) = make_handler(false);
    assert!(is_ok(&handle(&mut h, cmd(&["SET", "foo", "v"]))));
    assert_eq!(as_bulk(&handle(&mut h, cmd(&["GET", "foo"]))), "v");

    // Multi-key different slots fine when cluster off
    assert!(matches!(
        handle(&mut h, cmd(&["MGET", "foo", "bar"])),
        RespValue::Array(_)
    ));

    // SELECT works
    assert!(is_ok(&handle(&mut h, cmd(&["SELECT", "0"]))));

    // CLUSTER/ASKING report disabled
    let err = err_str(&handle(&mut h, cmd(&["CLUSTER", "INFO"])));
    assert!(
        err.contains("cluster support disabled"),
        "got {}",
        err
    );
    let err = err_str(&handle(&mut h, cmd(&["ASKING"])));
    assert!(
        err.contains("cluster support disabled"),
        "got {}",
        err
    );
}

#[test]
fn setslot_node_and_stable() {
    let (mut h, cluster) = make_handler(true);
    let cs = cluster.unwrap();
    let other = "f".repeat(40);
    cs.add_node(&other, "10.0.0.6", 7005);
    let slot = 42u16;

    assert!(is_ok(&handle(
        &mut h,
        cmd(&["CLUSTER", "SETSLOT", "42", "MIGRATING", &other])
    )));
    assert!(cs.is_migrating(slot));

    assert!(is_ok(&handle(
        &mut h,
        cmd(&["CLUSTER", "SETSLOT", "42", "STABLE"])
    )));
    assert!(!cs.is_migrating(slot));

    assert!(is_ok(&handle(
        &mut h,
        cmd(&["CLUSTER", "SETSLOT", "42", "NODE", &other])
    )));
    assert!(!cs.owns_slot(slot));
    assert_eq!(cs.owner_of(slot).unwrap().id, other);
}
