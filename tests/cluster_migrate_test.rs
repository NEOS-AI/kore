//! Lane D item 3: thin slot resharding / MIGRATEKEYS (string keys only).

use bytes::Bytes;
use kore::entry::StoreOptions;
use kore::protocol::{RespParser, RespValue};
use kore::{key_hash_slot, keys_in_slot, Cache, ClusterState, Server};
use kore::config::Config;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::{sleep, timeout, Duration};

fn make_config(port: u16, cluster: bool) -> Arc<Config> {
    Arc::new(Config {
        host: "127.0.0.1".to_string(),
        port,
        threads: 1,
        shards: 8,
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
        deadlock_ui_port: 0,
        enable_deadlock_detection: false,
        deadlock_max_wait_ms: 30_000,
        deadlock_auto_resolve: false,
        deadlock_victim_strategy: "youngest".to_string(),
        tls: false,
        tls_cert: String::new(),
        tls_key: String::new(),
        aclfile: String::new(),
        cluster_enabled: cluster,
    unixsocket: String::new(),
            log_format: "text".to_string(),
    })
}

fn encode_cmd(parts: &[&str]) -> Vec<u8> {
    let args: Vec<RespValue> = parts
        .iter()
        .map(|p| RespValue::BulkString(Some(Bytes::from(p.to_string()))))
        .collect();
    RespValue::Array(args).serialize().to_vec()
}

async fn read_one(stream: &mut TcpStream) -> RespValue {
    let mut parser = RespParser::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        if let Some(v) = parser.parse().expect("parse") {
            return v;
        }
        let n = timeout(Duration::from_secs(5), stream.read(&mut buf))
            .await
            .expect("read timeout")
            .expect("read err");
        assert!(n > 0, "connection closed while waiting for response");
        parser.feed(&buf[..n]);
    }
}

async fn send_cmd(stream: &mut TcpStream, parts: &[&str]) -> RespValue {
    stream.write_all(&encode_cmd(parts)).await.unwrap();
    read_one(stream).await
}

fn as_bulk(resp: &RespValue) -> String {
    match resp {
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        other => panic!("expected bulk, got {:?}", other),
    }
}

fn as_err(resp: &RespValue) -> String {
    match resp {
        RespValue::Error(e) => String::from_utf8_lossy(e).into_owned(),
        other => panic!("expected error, got {:?}", other),
    }
}

fn is_ok(resp: &RespValue) -> bool {
    matches!(resp, RespValue::SimpleString(s) if s.as_ref() == b"OK")
}

async fn wait_listen(port: u16) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("server on {} did not start", port);
        }
        sleep(Duration::from_millis(20)).await;
    }
}

/// Unit: keys_in_slot returns only keys whose CRC16 slot matches.
#[test]
fn keys_in_slot_iterator() {
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let opts = StoreOptions::default();

    // "foo" → 12182, "bar" → 5061
    cache
        .store(
            Bytes::from_static(b"foo"),
            Bytes::from_static(b"vfoo"),
            opts.clone(),
        )
        .unwrap();
    cache
        .store(
            Bytes::from_static(b"bar"),
            Bytes::from_static(b"vbar"),
            opts.clone(),
        )
        .unwrap();
    // Same-slot tagged keys
    cache
        .store(
            Bytes::from_static(b"{m}.a"),
            Bytes::from_static(b"1"),
            opts.clone(),
        )
        .unwrap();
    cache
        .store(
            Bytes::from_static(b"{m}.b"),
            Bytes::from_static(b"2"),
            opts,
        )
        .unwrap();

    let slot_foo = key_hash_slot(b"foo");
    let in_foo = keys_in_slot(&cache, slot_foo);
    assert_eq!(in_foo.len(), 1);
    assert_eq!(in_foo[0].as_ref(), b"foo");

    let slot_m = key_hash_slot(b"{m}.a");
    assert_eq!(slot_m, key_hash_slot(b"{m}.b"));
    let mut in_m = keys_in_slot(&cache, slot_m);
    in_m.sort();
    let mut expected = vec![Bytes::from_static(b"{m}.a"), Bytes::from_static(b"{m}.b")];
    expected.sort();
    assert_eq!(in_m, expected);

    // Empty slot
    let empty_slot = (slot_foo + 1) % 16384;
    // Avoid collision with bar / m
    let empty_slot = if empty_slot == key_hash_slot(b"bar") || empty_slot == slot_m {
        (empty_slot + 7) % 16384
    } else {
        empty_slot
    };
    assert!(keys_in_slot(&cache, empty_slot).is_empty());
}

/// Two-node e2e: migrate one slot's string keys; present on dest, gone on source.
#[tokio::test(flavor = "multi_thread")]
async fn migrate_string_keys_one_slot_e2e() {
    let port_a = 16720u16; // source
    let port_b = 16721u16; // dest

    let cs_a = ClusterState::single_node("127.0.0.1", port_a);
    let cs_b = ClusterState::single_node("127.0.0.1", port_b);
    let id_a = cs_a.my_id();
    let id_b = cs_b.my_id();

    // Dest starts owning nothing for the slot we'll move — reassign after MEET.
    // Both start as single-node full owners; after MEET we set up migration state.

    let cache_a = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let cache_b = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);

    let srv_a =
        Server::new(cache_a, make_config(port_a, true)).with_cluster(Some(Arc::clone(&cs_a)));
    let srv_b =
        Server::new(cache_b, make_config(port_b, true)).with_cluster(Some(Arc::clone(&cs_b)));

    let (shut_a_tx, shut_a_rx) = watch::channel(false);
    let (shut_b_tx, shut_b_rx) = watch::channel(false);
    let ha = tokio::spawn(async move {
        let _ = srv_a.run_with_shutdown(shut_a_rx).await;
    });
    let hb = tokio::spawn(async move {
        let _ = srv_b.run_with_shutdown(shut_b_rx).await;
    });

    wait_listen(port_a).await;
    wait_listen(port_b).await;

    let mut sa = TcpStream::connect(("127.0.0.1", port_a)).await.unwrap();
    let mut sb = TcpStream::connect(("127.0.0.1", port_b)).await.unwrap();

    // MEET both ways (or one way + peer handshake)
    assert!(is_ok(
        &send_cmd(&mut sa, &["CLUSTER", "MEET", "127.0.0.1", &port_b.to_string()]).await
    ));
    // Ensure B knows A (MEETPEER already ran); also MEET A from B for symmetry
    assert!(is_ok(
        &send_cmd(&mut sb, &["CLUSTER", "MEET", "127.0.0.1", &port_a.to_string()]).await
    ));

    // Use key "foo" → slot 12182; ensure source A owns it, dest B does not stably.
    let key = "foo";
    let slot = key_hash_slot(key.as_bytes());
    assert_eq!(slot, 12182);

    // B currently also claims all slots (single_node). Reassign slot on B away from B
    // so B is IMPORTING (not owning). A keeps ownership for MIGRATING.
    cs_b.reassign_slot(slot, &id_a).unwrap();

    // SET key on source while it owns the slot
    assert!(is_ok(&send_cmd(&mut sa, &["SET", key, "hello-migrate"]).await));
    assert_eq!(as_bulk(&send_cmd(&mut sa, &["GET", key]).await), "hello-migrate");

    // Operator flow
    assert!(is_ok(
        &send_cmd(
            &mut sb,
            &["CLUSTER", "SETSLOT", &slot.to_string(), "IMPORTING", &id_a]
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut sa,
            &["CLUSTER", "SETSLOT", &slot.to_string(), "MIGRATING", &id_b]
        )
        .await
    ));

    // Migrate string keys
    let resp = send_cmd(
        &mut sa,
        &[
            "CLUSTER",
            "MIGRATEKEYS",
            &slot.to_string(),
            "127.0.0.1",
            &port_b.to_string(),
        ],
    )
    .await;
    match resp {
        RespValue::Integer(n) => assert_eq!(n, 1, "expected 1 key migrated"),
        other => panic!("MIGRATEKEYS failed: {:?}", other),
    }

    // Gone on source (exists check / GET while migrating → ASK for miss)
    let err = as_err(&send_cmd(&mut sa, &["GET", key]).await);
    assert!(
        err.starts_with(&format!("ASK {} 127.0.0.1:{}", slot, port_b)),
        "source miss should ASK dest, got {}",
        err
    );

    // Present on dest via ASKING (still IMPORTING)
    assert!(is_ok(&send_cmd(&mut sb, &["ASKING"]).await));
    assert_eq!(
        as_bulk(&send_cmd(&mut sb, &["GET", key]).await),
        "hello-migrate"
    );

    // Finalize ownership
    assert!(is_ok(
        &send_cmd(
            &mut sa,
            &["CLUSTER", "SETSLOT", &slot.to_string(), "NODE", &id_b]
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut sb,
            &["CLUSTER", "SETSLOT", &slot.to_string(), "NODE", &id_b]
        )
        .await
    ));

    // Dest serves without ASKING; source MOVEs
    assert_eq!(
        as_bulk(&send_cmd(&mut sb, &["GET", key]).await),
        "hello-migrate"
    );
    let err = as_err(&send_cmd(&mut sa, &["GET", key]).await);
    assert!(
        err.starts_with(&format!("MOVED {} 127.0.0.1:{}", slot, port_b)),
        "got {}",
        err
    );

    let _ = shut_a_tx.send(true);
    let _ = shut_b_tx.send(true);
    let _ = ha.await;
    let _ = hb.await;
}

/// Missing key on MIGRATING source returns ASK (not MOVED).
#[tokio::test(flavor = "multi_thread")]
async fn migrating_miss_returns_ask() {
    let port_a = 16722u16;
    let port_b = 16723u16;

    let cs_a = ClusterState::single_node("127.0.0.1", port_a);
    let cs_b = ClusterState::single_node("127.0.0.1", port_b);
    let id_b = cs_b.my_id();

    let cache_a = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let cache_b = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);

    let srv_a =
        Server::new(cache_a, make_config(port_a, true)).with_cluster(Some(Arc::clone(&cs_a)));
    let srv_b =
        Server::new(cache_b, make_config(port_b, true)).with_cluster(Some(Arc::clone(&cs_b)));

    let (shut_a_tx, shut_a_rx) = watch::channel(false);
    let (shut_b_tx, shut_b_rx) = watch::channel(false);
    let ha = tokio::spawn(async move {
        let _ = srv_a.run_with_shutdown(shut_a_rx).await;
    });
    let hb = tokio::spawn(async move {
        let _ = srv_b.run_with_shutdown(shut_b_rx).await;
    });

    wait_listen(port_a).await;
    wait_listen(port_b).await;

    let mut sa = TcpStream::connect(("127.0.0.1", port_a)).await.unwrap();
    assert!(is_ok(
        &send_cmd(&mut sa, &["CLUSTER", "MEET", "127.0.0.1", &port_b.to_string()]).await
    ));

    // Ensure A knows B's address for ASK target (MEET added B)
    let _ = id_b;
    // Use B's real id from MEET
    let id_b = cs_a
        .peer_snapshots()
        .into_iter()
        .find(|n| n.port == port_b)
        .map(|n| n.id)
        .expect("peer B known after MEET");

    let key_present = "{mig}.present";
    let key_miss = "{mig}.missing";
    let slot = key_hash_slot(key_present.as_bytes());
    assert_eq!(slot, key_hash_slot(key_miss.as_bytes()));

    assert!(is_ok(
        &send_cmd(&mut sa, &["SET", key_present, "here"]).await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut sa,
            &["CLUSTER", "SETSLOT", &slot.to_string(), "MIGRATING", &id_b]
        )
        .await
    ));

    // Present key still served
    assert_eq!(
        as_bulk(&send_cmd(&mut sa, &["GET", key_present]).await),
        "here"
    );

    // Missing → ASK
    let err = as_err(&send_cmd(&mut sa, &["GET", key_miss]).await);
    assert!(
        err.starts_with(&format!("ASK {} 127.0.0.1:{}", slot, port_b)),
        "got {}",
        err
    );

    let _ = shut_a_tx.send(true);
    let _ = shut_b_tx.send(true);
    let _ = ha.await;
    let _ = hb.await;
}

/// After SETSLOT NODE to dest, source returns MOVED to dest.
#[tokio::test(flavor = "multi_thread")]
async fn after_node_assignment_moved_to_dest() {
    let port_a = 16724u16;
    let port_b = 16725u16;

    let cs_a = ClusterState::single_node("127.0.0.1", port_a);
    let cs_b = ClusterState::single_node("127.0.0.1", port_b);

    let cache_a = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let cache_b = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);

    let srv_a =
        Server::new(cache_a, make_config(port_a, true)).with_cluster(Some(Arc::clone(&cs_a)));
    let srv_b =
        Server::new(cache_b, make_config(port_b, true)).with_cluster(Some(Arc::clone(&cs_b)));

    let (shut_a_tx, shut_a_rx) = watch::channel(false);
    let (shut_b_tx, shut_b_rx) = watch::channel(false);
    let ha = tokio::spawn(async move {
        let _ = srv_a.run_with_shutdown(shut_a_rx).await;
    });
    let hb = tokio::spawn(async move {
        let _ = srv_b.run_with_shutdown(shut_b_rx).await;
    });

    wait_listen(port_a).await;
    wait_listen(port_b).await;

    let mut sa = TcpStream::connect(("127.0.0.1", port_a)).await.unwrap();
    let mut sb = TcpStream::connect(("127.0.0.1", port_b)).await.unwrap();

    assert!(is_ok(
        &send_cmd(&mut sa, &["CLUSTER", "MEET", "127.0.0.1", &port_b.to_string()]).await
    ));
    assert!(is_ok(
        &send_cmd(&mut sb, &["CLUSTER", "MEET", "127.0.0.1", &port_a.to_string()]).await
    ));

    let id_b = cs_a
        .peer_snapshots()
        .into_iter()
        .find(|n| n.port == port_b)
        .map(|n| n.id)
        .expect("peer B");

    let key = "bar";
    let slot = key_hash_slot(key.as_bytes());

    // Both assign slot to B
    assert!(is_ok(
        &send_cmd(
            &mut sa,
            &["CLUSTER", "SETSLOT", &slot.to_string(), "NODE", &id_b]
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut sb,
            &["CLUSTER", "SETSLOT", &slot.to_string(), "NODE", &id_b]
        )
        .await
    ));

    // Write on dest (owner)
    assert!(is_ok(&send_cmd(&mut sb, &["SET", key, "on-dest"]).await));
    assert_eq!(as_bulk(&send_cmd(&mut sb, &["GET", key]).await), "on-dest");

    // Source redirects with MOVED
    let err = as_err(&send_cmd(&mut sa, &["GET", key]).await);
    assert!(
        err.starts_with(&format!("MOVED {} 127.0.0.1:{}", slot, port_b)),
        "got {}",
        err
    );

    let _ = shut_a_tx.send(true);
    let _ = shut_b_tx.send(true);
    let _ = ha.await;
    let _ = hb.await;
}

/// Multi-type MIGRATEKEYS: hash + list + set + zset + string on one hash-tagged slot.
#[tokio::test(flavor = "multi_thread")]
async fn migrate_multi_type_keys_one_slot_e2e() {
    let port_a = 16730u16; // source
    let port_b = 16731u16; // dest

    let cs_a = ClusterState::single_node("127.0.0.1", port_a);
    let cs_b = ClusterState::single_node("127.0.0.1", port_b);
    let id_a = cs_a.my_id();
    let id_b = cs_b.my_id();

    let cache_a = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let cache_b = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);

    let srv_a =
        Server::new(cache_a, make_config(port_a, true)).with_cluster(Some(Arc::clone(&cs_a)));
    let srv_b =
        Server::new(cache_b, make_config(port_b, true)).with_cluster(Some(Arc::clone(&cs_b)));

    let (shut_a_tx, shut_a_rx) = watch::channel(false);
    let (shut_b_tx, shut_b_rx) = watch::channel(false);
    let ha = tokio::spawn(async move {
        let _ = srv_a.run_with_shutdown(shut_a_rx).await;
    });
    let hb = tokio::spawn(async move {
        let _ = srv_b.run_with_shutdown(shut_b_rx).await;
    });

    wait_listen(port_a).await;
    wait_listen(port_b).await;

    let mut sa = TcpStream::connect(("127.0.0.1", port_a)).await.unwrap();
    let mut sb = TcpStream::connect(("127.0.0.1", port_b)).await.unwrap();

    assert!(is_ok(
        &send_cmd(&mut sa, &["CLUSTER", "MEET", "127.0.0.1", &port_b.to_string()]).await
    ));
    assert!(is_ok(
        &send_cmd(&mut sb, &["CLUSTER", "MEET", "127.0.0.1", &port_a.to_string()]).await
    ));

    // All keys share the same hash tag → same slot
    let k_str = "{mt}.s";
    let k_hash = "{mt}.h";
    let k_list = "{mt}.l";
    let k_set = "{mt}.t";
    let k_zset = "{mt}.z";
    let slot = key_hash_slot(k_str.as_bytes());
    assert_eq!(slot, key_hash_slot(k_hash.as_bytes()));
    assert_eq!(slot, key_hash_slot(k_list.as_bytes()));
    assert_eq!(slot, key_hash_slot(k_set.as_bytes()));
    assert_eq!(slot, key_hash_slot(k_zset.as_bytes()));

    cs_b.reassign_slot(slot, &id_a).unwrap();

    assert!(is_ok(&send_cmd(&mut sa, &["SET", k_str, "sv"]).await));
    assert!(matches!(
        send_cmd(&mut sa, &["HSET", k_hash, "f1", "v1", "f2", "v2"]).await,
        RespValue::Integer(2)
    ));
    assert!(matches!(
        send_cmd(&mut sa, &["RPUSH", k_list, "a", "b", "c"]).await,
        RespValue::Integer(3)
    ));
    assert!(matches!(
        send_cmd(&mut sa, &["SADD", k_set, "m1", "m2"]).await,
        RespValue::Integer(2)
    ));
    assert!(matches!(
        send_cmd(&mut sa, &["ZADD", k_zset, "1.5", "zm"]).await,
        RespValue::Integer(1)
    ));

    assert!(is_ok(
        &send_cmd(
            &mut sb,
            &["CLUSTER", "SETSLOT", &slot.to_string(), "IMPORTING", &id_a]
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut sa,
            &["CLUSTER", "SETSLOT", &slot.to_string(), "MIGRATING", &id_b]
        )
        .await
    ));

    let resp = send_cmd(
        &mut sa,
        &[
            "CLUSTER",
            "MIGRATEKEYS",
            &slot.to_string(),
            "127.0.0.1",
            &port_b.to_string(),
        ],
    )
    .await;
    match resp {
        RespValue::Integer(n) => assert_eq!(n, 5, "expected 5 keys migrated, got {}", n),
        other => panic!("MIGRATEKEYS failed: {:?}", other),
    }

    // Source miss → ASK
    let err = as_err(&send_cmd(&mut sa, &["GET", k_str]).await);
    assert!(
        err.starts_with(&format!("ASK {} 127.0.0.1:{}", slot, port_b)),
        "got {}",
        err
    );

    // Dest has all types via ASKING
    assert!(is_ok(&send_cmd(&mut sb, &["ASKING"]).await));
    assert_eq!(as_bulk(&send_cmd(&mut sb, &["GET", k_str]).await), "sv");

    assert!(is_ok(&send_cmd(&mut sb, &["ASKING"]).await));
    assert_eq!(as_bulk(&send_cmd(&mut sb, &["HGET", k_hash, "f1"]).await), "v1");

    assert!(is_ok(&send_cmd(&mut sb, &["ASKING"]).await));
    match send_cmd(&mut sb, &["LRANGE", k_list, "0", "-1"]).await {
        RespValue::Array(a) => assert_eq!(a.len(), 3),
        other => panic!("LRANGE {:?}", other),
    }

    assert!(is_ok(&send_cmd(&mut sb, &["ASKING"]).await));
    match send_cmd(&mut sb, &["SMEMBERS", k_set]).await {
        RespValue::Array(a) => assert_eq!(a.len(), 2),
        other => panic!("SMEMBERS {:?}", other),
    }

    assert!(is_ok(&send_cmd(&mut sb, &["ASKING"]).await));
    match send_cmd(&mut sb, &["ZSCORE", k_zset, "zm"]).await {
        RespValue::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.starts_with("1.5") || s == "1.5", "score {}", s);
        }
        other => panic!("ZSCORE {:?}", other),
    }

    let _ = shut_a_tx.send(true);
    let _ = shut_b_tx.send(true);
    let _ = ha.await;
    let _ = hb.await;
}

/// Parse CLUSTER RESHARD reply: outer array of flat field-pair arrays.
fn parse_reshard_slot(resp: &RespValue) -> (u16, i64, String, String, String) {
    let outer = match resp {
        RespValue::Array(a) => a,
        other => panic!("RESHARD expected array, got {:?}", other),
    };
    assert_eq!(outer.len(), 1, "expected one slot result");
    let fields = match &outer[0] {
        RespValue::Array(a) => a,
        other => panic!("slot result expected array, got {:?}", other),
    };
    // flat pairs: slot, n, migrated, n, skipped, n, source_node, s, dest_node, s, status, s
    assert!(fields.len() >= 12);
    let slot = match &fields[1] {
        RespValue::Integer(n) => *n as u16,
        other => panic!("slot {:?}", other),
    };
    let migrated = match &fields[3] {
        RespValue::Integer(n) => *n,
        other => panic!("migrated {:?}", other),
    };
    let source_node = as_bulk(&fields[7]);
    let dest_node = as_bulk(&fields[9]);
    let status = as_bulk(&fields[11]);
    (slot, migrated, source_node, dest_node, status)
}

/// Batch DM: CLUSTER RESHARD orchestrates IMPORTING → MIGRATING → keys → dual NODE.
#[tokio::test(flavor = "multi_thread")]
async fn reshard_one_slot_end_to_end() {
    let port_a = 16740u16; // source
    let port_b = 16741u16; // dest

    let cs_a = ClusterState::single_node("127.0.0.1", port_a);
    let cs_b = ClusterState::single_node("127.0.0.1", port_b);

    let cache_a = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let cache_b = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);

    let srv_a =
        Server::new(cache_a, make_config(port_a, true)).with_cluster(Some(Arc::clone(&cs_a)));
    let srv_b =
        Server::new(cache_b, make_config(port_b, true)).with_cluster(Some(Arc::clone(&cs_b)));

    let (shut_a_tx, shut_a_rx) = watch::channel(false);
    let (shut_b_tx, shut_b_rx) = watch::channel(false);
    let ha = tokio::spawn(async move {
        let _ = srv_a.run_with_shutdown(shut_a_rx).await;
    });
    let hb = tokio::spawn(async move {
        let _ = srv_b.run_with_shutdown(shut_b_rx).await;
    });

    wait_listen(port_a).await;
    wait_listen(port_b).await;

    let mut sa = TcpStream::connect(("127.0.0.1", port_a)).await.unwrap();
    let mut sb = TcpStream::connect(("127.0.0.1", port_b)).await.unwrap();

    assert!(is_ok(
        &send_cmd(&mut sa, &["CLUSTER", "MEET", "127.0.0.1", &port_b.to_string()]).await
    ));
    assert!(is_ok(
        &send_cmd(&mut sb, &["CLUSTER", "MEET", "127.0.0.1", &port_a.to_string()]).await
    ));

    let id_b = cs_a
        .peer_snapshots()
        .into_iter()
        .find(|n| n.port == port_b)
        .map(|n| n.id)
        .expect("peer B after MEET");

    let key = "{rs}.k";
    let slot = key_hash_slot(key.as_bytes());
    assert!(is_ok(
        &send_cmd(&mut sa, &["SET", key, "reshard-val"]).await
    ));

    let resp = send_cmd(
        &mut sa,
        &["CLUSTER", "RESHARD", &slot.to_string(), &id_b],
    )
    .await;
    let (got_slot, migrated, source_node, dest_node, status) = parse_reshard_slot(&resp);
    assert_eq!(got_slot, slot);
    assert_eq!(migrated, 1);
    assert_eq!(source_node, "ok");
    assert_eq!(dest_node, "ok");
    assert_eq!(status, "complete");

    // Dest serves without ASKING; source MOVEs
    assert_eq!(
        as_bulk(&send_cmd(&mut sb, &["GET", key]).await),
        "reshard-val"
    );
    let err = as_err(&send_cmd(&mut sa, &["GET", key]).await);
    assert!(
        err.starts_with(&format!("MOVED {} 127.0.0.1:{}", slot, port_b)),
        "got {}",
        err
    );
    assert!(!cs_a.owns_slot(slot));
    assert!(cs_b.owns_slot(slot));

    let _ = shut_a_tx.send(true);
    let _ = shut_b_tx.send(true);
    let _ = ha.await;
    let _ = hb.await;
}

/// Empty slot RESHARD still dual-end NODE (keys=0) and transfers ownership.
#[tokio::test(flavor = "multi_thread")]
async fn reshard_empty_slot_transfers_ownership() {
    let port_a = 16742u16;
    let port_b = 16743u16;

    let cs_a = ClusterState::single_node("127.0.0.1", port_a);
    let cs_b = ClusterState::single_node("127.0.0.1", port_b);

    let cache_a = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let cache_b = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);

    let srv_a =
        Server::new(cache_a, make_config(port_a, true)).with_cluster(Some(Arc::clone(&cs_a)));
    let srv_b =
        Server::new(cache_b, make_config(port_b, true)).with_cluster(Some(Arc::clone(&cs_b)));

    let (shut_a_tx, shut_a_rx) = watch::channel(false);
    let (shut_b_tx, shut_b_rx) = watch::channel(false);
    let ha = tokio::spawn(async move {
        let _ = srv_a.run_with_shutdown(shut_a_rx).await;
    });
    let hb = tokio::spawn(async move {
        let _ = srv_b.run_with_shutdown(shut_b_rx).await;
    });

    wait_listen(port_a).await;
    wait_listen(port_b).await;

    let mut sa = TcpStream::connect(("127.0.0.1", port_a)).await.unwrap();
    let mut sb = TcpStream::connect(("127.0.0.1", port_b)).await.unwrap();

    assert!(is_ok(
        &send_cmd(&mut sa, &["CLUSTER", "MEET", "127.0.0.1", &port_b.to_string()]).await
    ));
    assert!(is_ok(
        &send_cmd(&mut sb, &["CLUSTER", "MEET", "127.0.0.1", &port_a.to_string()]).await
    ));

    let id_b = cs_a
        .peer_snapshots()
        .into_iter()
        .find(|n| n.port == port_b)
        .map(|n| n.id)
        .expect("peer B");

    // Pick a free-looking slot and ensure no keys; use slot of an unused tag.
    let slot = key_hash_slot(b"{emptyrs}.x");
    let resp = send_cmd(
        &mut sa,
        &["CLUSTER", "RESHARD", &slot.to_string(), &id_b],
    )
    .await;
    let (got_slot, migrated, source_node, dest_node, status) = parse_reshard_slot(&resp);
    assert_eq!(got_slot, slot);
    assert_eq!(migrated, 0);
    assert_eq!(source_node, "ok");
    assert_eq!(dest_node, "ok");
    assert_eq!(status, "complete");
    assert!(!cs_a.owns_slot(slot));
    assert!(cs_b.owns_slot(slot));

    // Source redirects writes for that slot
    let err = as_err(&send_cmd(&mut sa, &["SET", "{emptyrs}.x", "v"]).await);
    assert!(
        err.starts_with(&format!("MOVED {} 127.0.0.1:{}", slot, port_b)),
        "got {}",
        err
    );

    let _ = shut_a_tx.send(true);
    let _ = shut_b_tx.send(true);
    let _ = ha.await;
    let _ = hb.await;
}

/// Multi-slot range RESHARD: two empty slots move ownership in one command.
#[tokio::test(flavor = "multi_thread")]
async fn reshard_slot_range_two_slots() {
    let port_a = 16744u16;
    let port_b = 16745u16;

    let cs_a = ClusterState::single_node("127.0.0.1", port_a);
    let cs_b = ClusterState::single_node("127.0.0.1", port_b);

    let cache_a = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let cache_b = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);

    let srv_a =
        Server::new(cache_a, make_config(port_a, true)).with_cluster(Some(Arc::clone(&cs_a)));
    let srv_b =
        Server::new(cache_b, make_config(port_b, true)).with_cluster(Some(Arc::clone(&cs_b)));

    let (shut_a_tx, shut_a_rx) = watch::channel(false);
    let (shut_b_tx, shut_b_rx) = watch::channel(false);
    let ha = tokio::spawn(async move {
        let _ = srv_a.run_with_shutdown(shut_a_rx).await;
    });
    let hb = tokio::spawn(async move {
        let _ = srv_b.run_with_shutdown(shut_b_rx).await;
    });

    wait_listen(port_a).await;
    wait_listen(port_b).await;

    let mut sa = TcpStream::connect(("127.0.0.1", port_a)).await.unwrap();
    let mut sb = TcpStream::connect(("127.0.0.1", port_b)).await.unwrap();

    assert!(is_ok(
        &send_cmd(&mut sa, &["CLUSTER", "MEET", "127.0.0.1", &port_b.to_string()]).await
    ));
    assert!(is_ok(
        &send_cmd(&mut sb, &["CLUSTER", "MEET", "127.0.0.1", &port_a.to_string()]).await
    ));

    let id_b = cs_a
        .peer_snapshots()
        .into_iter()
        .find(|n| n.port == port_b)
        .map(|n| n.id)
        .expect("peer B");

    let start = 100u16;
    let end = 101u16;
    let resp = send_cmd(
        &mut sa,
        &[
            "CLUSTER",
            "RESHARD",
            &start.to_string(),
            &end.to_string(),
            &id_b,
        ],
    )
    .await;
    match resp {
        RespValue::Array(slots) => {
            assert_eq!(slots.len(), 2);
            for (i, item) in slots.iter().enumerate() {
                let fields = match item {
                    RespValue::Array(a) => a,
                    other => panic!("{:?}", other),
                };
                let slot = match &fields[1] {
                    RespValue::Integer(n) => *n as u16,
                    _ => panic!("slot field"),
                };
                assert_eq!(slot, start + i as u16);
                assert_eq!(as_bulk(&fields[11]), "complete");
            }
        }
        other => panic!("RESHARD range failed: {:?}", other),
    }
    assert!(!cs_a.owns_slot(start) && !cs_a.owns_slot(end));
    assert!(cs_b.owns_slot(start) && cs_b.owns_slot(end));

    let _ = shut_a_tx.send(true);
    let _ = shut_b_tx.send(true);
    let _ = ha.await;
    let _ = hb.await;
}

/// Unknown dest node id → hard error (no silent partial).
#[tokio::test(flavor = "multi_thread")]
async fn reshard_unknown_dest_errors() {
    let port_a = 16746u16;
    let cs_a = ClusterState::single_node("127.0.0.1", port_a);
    let cache_a = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_a =
        Server::new(cache_a, make_config(port_a, true)).with_cluster(Some(Arc::clone(&cs_a)));

    let (shut_a_tx, shut_a_rx) = watch::channel(false);
    let ha = tokio::spawn(async move {
        let _ = srv_a.run_with_shutdown(shut_a_rx).await;
    });
    wait_listen(port_a).await;

    let mut sa = TcpStream::connect(("127.0.0.1", port_a)).await.unwrap();
    let err = as_err(
        &send_cmd(
            &mut sa,
            &[
                "CLUSTER",
                "RESHARD",
                "0",
                "ffffffffffffffffffffffffffffffffffffffff",
            ],
        )
        .await,
    );
    assert!(
        err.contains("don't know about node") || err.contains("I don't know"),
        "got {}",
        err
    );

    // HELP lists RESHARD
    match send_cmd(&mut sa, &["CLUSTER", "HELP"]).await {
        RespValue::Array(lines) => {
            let joined: String = lines
                .iter()
                .filter_map(|v| match v {
                    RespValue::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n");
            assert!(joined.contains("RESHARD"), "HELP missing RESHARD: {}", joined);
        }
        other => panic!("HELP {:?}", other),
    }

    let _ = shut_a_tx.send(true);
    let _ = ha.await;
}

/// Dest unreachable → honest failed_connect status (not a bare panic / hang).
#[tokio::test(flavor = "multi_thread")]
async fn reshard_dest_down_reports_failed_connect() {
    let port_a = 16747u16;
    // No server on port_b
    let port_b = 16748u16;

    let cs_a = ClusterState::single_node("127.0.0.1", port_a);
    // Register a fake peer that is not listening
    let fake_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    cs_a.add_node(fake_id, "127.0.0.1", port_b);

    let cache_a = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_a =
        Server::new(cache_a, make_config(port_a, true)).with_cluster(Some(Arc::clone(&cs_a)));

    let (shut_a_tx, shut_a_rx) = watch::channel(false);
    let ha = tokio::spawn(async move {
        let _ = srv_a.run_with_shutdown(shut_a_rx).await;
    });
    wait_listen(port_a).await;

    let mut sa = TcpStream::connect(("127.0.0.1", port_a)).await.unwrap();
    let resp = send_cmd(&mut sa, &["CLUSTER", "RESHARD", "0", fake_id]).await;
    let (_slot, migrated, source_node, dest_node, status) = parse_reshard_slot(&resp);
    assert_eq!(migrated, 0);
    assert_eq!(source_node, "n/a");
    assert_eq!(dest_node, "n/a");
    assert!(
        status.starts_with("failed_connect"),
        "expected failed_connect, got {}",
        status
    );
    // Ownership unchanged
    assert!(cs_a.owns_slot(0));

    let _ = shut_a_tx.send(true);
    let _ = ha.await;
}
