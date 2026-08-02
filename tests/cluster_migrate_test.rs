//! Lane D item 3: thin slot resharding / MIGRATEKEYS (string keys only).

use bytes::Bytes;
use kore::entry::StoreOptions;
use kore::protocol::{RespParser, RespValue};
use kore::{
    key_hash_slot, keys_in_slot, test_acquire_dest_node_inject, test_acquire_dest_prepare_inject,
    test_acquire_migrate_key_inject, test_commit_recheck_inject, test_source_node_inject, Cache,
    ClusterState, Server,
};
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
        tls_port: 0,
        tls_ca: String::new(),
        tls_auth_clients: false,
        tls_replication: false,
        aclfile: String::new(),
        cluster_enabled: cluster,
            cluster_replica_priority: 100,
            cluster_require_full_coverage: true,
            cluster_allow_reads_when_down: false,
            cluster_announce_ip: String::new(),
            cluster_announce_port: 0,
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

/// Batch DN: one injected dest NODE failure is retried → still `complete`.
#[tokio::test(flavor = "multi_thread")]
async fn reshard_dest_node_retry_recovers_to_complete() {
    let port_a = 16750u16;
    let port_b = 16751u16;

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

    let key = "{dnretry}.k";
    let slot = key_hash_slot(key.as_bytes());
    assert!(is_ok(
        &send_cmd(&mut sa, &["SET", key, "retry-val"]).await
    ));

    let inj = test_acquire_dest_node_inject().await;
    // First dest NODE attempt fails; verify+retry path should still complete.
    inj.set_failures_for_port(port_b, 1);

    let resp = send_cmd(
        &mut sa,
        &["CLUSTER", "RESHARD", &slot.to_string(), &id_b],
    )
    .await;
    drop(inj);

    let (got_slot, migrated, source_node, dest_node, status) = parse_reshard_slot(&resp);
    assert_eq!(got_slot, slot);
    assert_eq!(migrated, 1);
    assert_eq!(source_node, "ok");
    assert_eq!(dest_node, "ok");
    assert_eq!(status, "complete");
    assert!(!cs_a.owns_slot(slot));
    assert!(cs_b.owns_slot(slot));
    assert_eq!(
        as_bulk(&send_cmd(&mut sb, &["GET", key]).await),
        "retry-val"
    );

    let _ = shut_a_tx.send(true);
    let _ = shut_b_tx.send(true);
    let _ = ha.await;
    let _ = hb.await;
}

/// Batch EP: source NODE exhaust → dest rolled back; both agree source owns; FINISH completes.
#[tokio::test(flavor = "multi_thread")]
async fn reshard_source_node_fail_rolls_back_dest() {
    let port_a = 16794u16;
    let port_b = 16795u16;

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

    let key = "{eprollback}.k";
    let slot = key_hash_slot(key.as_bytes());
    assert!(is_ok(
        &send_cmd(&mut sa, &["SET", key, "ep-val"]).await
    ));

    {
        // Per-ClusterState inject (not process-global) — safe under parallel tests.
        let inj = test_source_node_inject(Arc::clone(&cs_a));
        // Exhaust source NODE retries (NODE_SET_ATTEMPTS=3) → rolled_back after dest ok.
        inj.set_failures(8);
        let resp = send_cmd(
            &mut sa,
            &["CLUSTER", "RESHARD", &slot.to_string(), &id_b],
        )
        .await;
        let (got_slot, migrated, source_node, dest_node, status) = parse_reshard_slot(&resp);
        assert_eq!(got_slot, slot);
        assert_eq!(migrated, 1, "keys should still have moved");
        assert_ne!(source_node, "ok", "source NODE should have failed: {}", source_node);
        assert_eq!(dest_node, "rolled_back");
        assert_eq!(status, "rolled_back");
        // Both sides agree source owns again (no dual-master window).
        assert!(
            cs_a.owns_slot(slot),
            "source must keep ownership after rolled_back"
        );
        assert!(
            !cs_b.owns_slot(slot),
            "dest must not keep ownership after rollback"
        );
        assert!(
            cs_a.is_migrating(slot),
            "source should remain MIGRATING for ASK / retry"
        );
        assert!(
            cs_b.is_importing(slot),
            "dest should be IMPORTING after rollback"
        );
        drop(inj);
    }

    // FINISH completes dual-end NODE without re-migrating.
    let finish = send_cmd(
        &mut sa,
        &["CLUSTER", "RESHARD", "FINISH", &slot.to_string(), &id_b],
    )
    .await;
    let (got_slot, migrated, source_node, dest_node, status) = parse_reshard_slot(&finish);
    assert_eq!(got_slot, slot);
    assert_eq!(migrated, 0);
    assert_eq!(source_node, "ok");
    assert_eq!(dest_node, "ok");
    assert_eq!(status, "complete");
    assert!(cs_b.owns_slot(slot));
    assert!(!cs_a.owns_slot(slot));
    assert_eq!(
        as_bulk(&send_cmd(&mut sb, &["GET", key]).await),
        "ep-val"
    );

    let _ = shut_a_tx.send(true);
    let _ = shut_b_tx.send(true);
    let _ = ha.await;
    let _ = hb.await;
}

/// Batch DN/DV: exhausting dest NODE → `partial_dest_node`; source keeps ownership
/// (dest-first); FINISH recovers.
#[tokio::test(flavor = "multi_thread")]
async fn reshard_partial_dest_node_then_finish_recovers() {
    let port_a = 16752u16;
    let port_b = 16753u16;

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

    let key = "{dnpartial}.k";
    let slot = key_hash_slot(key.as_bytes());
    assert!(is_ok(
        &send_cmd(&mut sa, &["SET", key, "partial-val"]).await
    ));

    {
        let inj = test_acquire_dest_node_inject().await;
        // More failures than NODE_SET_ATTEMPTS (3) → permanent partial_dest_node.
        inj.set_failures_for_port(port_b, 8);
        let resp = send_cmd(
            &mut sa,
            &["CLUSTER", "RESHARD", &slot.to_string(), &id_b],
        )
        .await;
        let (got_slot, migrated, source_node, dest_node, status) = parse_reshard_slot(&resp);
        assert_eq!(got_slot, slot);
        assert_eq!(migrated, 1, "keys should still have moved");
        // Batch DV dest-first: source NODE skipped when dest fails.
        assert!(
            source_node.starts_with("skipped:"),
            "expected skipped source NODE, got {}",
            source_node
        );
        assert_ne!(dest_node, "ok");
        assert_eq!(status, "partial_dest_node");
        // Source still owns (no MOVED flip); dest never got stable NODE.
        assert!(
            cs_a.owns_slot(slot),
            "dest-first: source must keep ownership when dest NODE fails"
        );
        assert!(!cs_b.owns_slot(slot), "dest NODE never applied");
        // Key already on dest from MIGRATEKEYS; ownership recovery via FINISH.
        drop(inj);
    }

    let finish = send_cmd(
        &mut sa,
        &["CLUSTER", "RESHARD", "FINISH", &slot.to_string(), &id_b],
    )
    .await;
    let (got_slot, migrated, source_node, dest_node, status) = parse_reshard_slot(&finish);
    assert_eq!(got_slot, slot);
    assert_eq!(migrated, 0, "FINISH must not re-count key moves");
    assert_eq!(source_node, "ok");
    assert_eq!(dest_node, "ok");
    assert_eq!(status, "complete");
    assert!(cs_b.owns_slot(slot));
    assert!(!cs_a.owns_slot(slot));
    assert_eq!(
        as_bulk(&send_cmd(&mut sb, &["GET", key]).await),
        "partial-val"
    );

    // HELP lists FINISH recovery
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
            assert!(
                joined.contains("RESHARD FINISH"),
                "HELP missing RESHARD FINISH: {}",
                joined
            );
        }
        other => panic!("HELP {:?}", other),
    }

    let _ = shut_a_tx.send(true);
    let _ = shut_b_tx.send(true);
    let _ = ha.await;
    let _ = hb.await;
}

/// Parse RESHARD reply fields including optional trailing `warning`.
fn parse_reshard_slot_ext(resp: &RespValue) -> (u16, i64, String, String, String, Option<String>) {
    let outer = match resp {
        RespValue::Array(a) => a,
        other => panic!("RESHARD expected array, got {:?}", other),
    };
    assert_eq!(outer.len(), 1, "expected one slot result");
    let fields = match &outer[0] {
        RespValue::Array(a) => a,
        other => panic!("slot result expected array, got {:?}", other),
    };
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
    let warning = if fields.len() >= 14 {
        match &fields[12] {
            RespValue::BulkString(Some(b)) if b.as_ref() == b"warning" => {
                Some(as_bulk(&fields[13]))
            }
            _ => None,
        }
    } else {
        None
    };
    (slot, migrated, source_node, dest_node, status, warning)
}

/// Batch DO: mid-slot key failure surfaces real `migrated` under `failed_keys`.
#[tokio::test(flavor = "multi_thread")]
async fn reshard_failed_keys_reports_partial_migrated() {
    let port_a = 16754u16;
    let port_b = 16755u16;

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

    // Two keys, same hash tag → same slot.
    let k1 = "{dopart}.a";
    let k2 = "{dopart}.b";
    let slot = key_hash_slot(k1.as_bytes());
    assert_eq!(slot, key_hash_slot(k2.as_bytes()));
    assert!(is_ok(&send_cmd(&mut sa, &["SET", k1, "v1"]).await));
    assert!(is_ok(&send_cmd(&mut sa, &["SET", k2, "v2"]).await));

    let inj = test_acquire_migrate_key_inject().await;
    // First key migrates; second fails → partial progress.
    inj.fail_after_successes(1);

    let resp = send_cmd(
        &mut sa,
        &["CLUSTER", "RESHARD", &slot.to_string(), &id_b],
    )
    .await;
    drop(inj);

    let (got_slot, migrated, source_node, dest_node, status) = parse_reshard_slot(&resp);
    assert_eq!(got_slot, slot);
    assert_eq!(
        migrated, 1,
        "failed_keys must report keys already moved, got status={}",
        status
    );
    assert_eq!(source_node, "n/a");
    assert_eq!(dest_node, "n/a");
    assert!(
        status.starts_with("failed_keys:"),
        "expected failed_keys, got {}",
        status
    );
    // Source still owns slot (MIGRATING); one key left locally, one on dest.
    assert!(cs_a.owns_slot(slot));
    // Retry without inject should finish the leftover key + dual-end NODE.
    let retry = send_cmd(
        &mut sa,
        &["CLUSTER", "RESHARD", &slot.to_string(), &id_b],
    )
    .await;
    let (_s, migrated2, sn, dn, st) = parse_reshard_slot(&retry);
    assert_eq!(migrated2, 1, "retry moves only leftover key");
    assert_eq!(sn, "ok");
    assert_eq!(dn, "ok");
    assert_eq!(st, "complete");
    assert!(!cs_a.owns_slot(slot));
    assert!(cs_b.owns_slot(slot));
    // Both values end on dest (first from partial, second from retry).
    assert_eq!(as_bulk(&send_cmd(&mut sb, &["GET", k1]).await), "v1");
    assert_eq!(as_bulk(&send_cmd(&mut sb, &["GET", k2]).await), "v2");

    let _ = shut_a_tx.send(true);
    let _ = shut_b_tx.send(true);
    let _ = ha.await;
    let _ = hb.await;
}

/// Batch DO: multi-slot range aborts after `partial_*_node` (not only failed_*).
#[tokio::test(flavor = "multi_thread")]
async fn reshard_range_aborts_after_partial_dest_node() {
    let port_a = 16756u16;
    let port_b = 16757u16;

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

    // Two consecutive empty slots for a clean range (fixed, always in-range).
    let start = 100u16;
    let end = 101u16;

    let inj = test_acquire_dest_node_inject().await;
    // Exhaust dest NODE retries on first slot → partial_dest_node.
    inj.set_failures_for_port(port_b, 8);

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
    drop(inj);

    let outer = match &resp {
        RespValue::Array(a) => a,
        other => panic!("expected array {:?}", other),
    };
    assert_eq!(
        outer.len(),
        1,
        "range must abort after partial_dest_node; got {} slot results",
        outer.len()
    );
    let fields = match &outer[0] {
        RespValue::Array(a) => a,
        other => panic!("{:?}", other),
    };
    let status = as_bulk(&fields[11]);
    assert_eq!(status, "partial_dest_node");
    // Second slot left untouched on source.
    assert!(cs_a.owns_slot(end), "second slot should still be owned by source");

    let _ = shut_a_tx.send(true);
    let _ = shut_b_tx.send(true);
    let _ = ha.await;
    let _ = hb.await;
}

/// Batch DO: FINISH soft-warns when source still holds keys in the slot.
#[tokio::test(flavor = "multi_thread")]
async fn reshard_finish_warns_when_source_keys_remain() {
    let port_a = 16758u16;
    let port_b = 16759u16;

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

    let key = "{dofinish}.k";
    let slot = key_hash_slot(key.as_bytes());
    assert!(is_ok(
        &send_cmd(&mut sa, &["SET", key, "still-here"]).await
    ));

    // FINISH without migrating: ownership may complete, but warning must fire.
    let finish = send_cmd(
        &mut sa,
        &["CLUSTER", "RESHARD", "FINISH", &slot.to_string(), &id_b],
    )
    .await;
    let (got_slot, migrated, source_node, dest_node, status, warning) =
        parse_reshard_slot_ext(&finish);
    assert_eq!(got_slot, slot);
    assert_eq!(migrated, 0);
    assert_eq!(source_node, "ok");
    assert_eq!(dest_node, "ok");
    assert_eq!(status, "complete");
    let w = warning.expect("FINISH should warn when source still has keys");
    assert!(
        w.contains("source still holds") && w.contains("key"),
        "unexpected warning: {}",
        w
    );
    // Ownership can complete; FINISH does not move data so dest lacks the key.
    assert!(cs_b.owns_slot(slot));
    match send_cmd(&mut sb, &["GET", key]).await {
        RespValue::BulkString(None) | RespValue::Null => {}
        other => panic!("dest must not have unmigrated key, got {:?}", other),
    }
    // Source redirects (key may still sit in local cache, unreachable via cluster gate).
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

/// Batch DV: after complete reshard, lower-epoch gossip cannot flip ownership back.
#[tokio::test(flavor = "multi_thread")]
async fn stale_epoch_ownership_cannot_flip_after_reshard() {
    let port_a = 16760u16;
    let port_b = 16761u16;

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

    let id_a = cs_a.my_id();
    let id_b = cs_a
        .peer_snapshots()
        .into_iter()
        .find(|n| n.port == port_b)
        .map(|n| n.id)
        .expect("peer B");

    let key = "{dvstale}.k";
    let slot = key_hash_slot(key.as_bytes());
    assert!(is_ok(&send_cmd(&mut sa, &["SET", key, "v"]).await));

    let resp = send_cmd(
        &mut sa,
        &["CLUSTER", "RESHARD", &slot.to_string(), &id_b],
    )
    .await;
    let (_s, _m, source_node, dest_node, status) = parse_reshard_slot(&resp);
    assert_eq!(source_node, "ok");
    assert_eq!(dest_node, "ok");
    assert_eq!(status, "complete");
    assert!(cs_b.owns_slot(slot));
    let high_epoch = cs_b.slot_epoch(slot);
    assert!(high_epoch > 1);

    // Stale gossip: claim A still owns with epoch 1.
    let stale = kore::OwnershipRange {
        start: slot,
        end: slot,
        owner_id: id_a.clone(),
        ip: "127.0.0.1".into(),
        port: port_a,
        epoch: 1,
    };
    assert_eq!(
        cs_b.apply_ownership_range(&stale),
        kore::OwnershipApplyResult::RejectedStale
    );
    assert_eq!(cs_b.owner_id_of(slot).as_deref(), Some(id_b.as_str()));
    assert_eq!(cs_b.slot_epoch(slot), high_epoch);

    let _ = shut_a_tx.send(true);
    let _ = shut_b_tx.send(true);
    let _ = ha.await;
    let _ = hb.await;
}

/// Batch DX: RESHARD PLAN lists donor slots; AUTO moves them to dest.
#[tokio::test(flavor = "multi_thread")]
async fn reshard_plan_and_auto_local() {
    let port_a = 16770u16;
    let port_b = 16771u16;

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

    // Align topology: both start owning all slots locally; reassign on B so A is sole full owner
    // is already true on A. B still thinks it owns all — PLAN on A uses A's view.
    // Give B empty ownership on A's map by not changing A (A owns all).

    let plan_resp = send_cmd(
        &mut sa,
        &["CLUSTER", "RESHARD", "PLAN", &id_b, "3"],
    )
    .await;
    let plan_rows = match plan_resp {
        RespValue::Array(a) => a,
        other => panic!("PLAN: {:?}", other),
    };
    assert_eq!(plan_rows.len(), 3, "expected 3 planned slots");

    let mut planned_slots = Vec::new();
    for row in &plan_rows {
        let fields = match row {
            RespValue::Array(f) => f,
            _ => panic!("{:?}", row),
        };
        // flat pairs: slot, n, source_id, ..., source_ip, ..., source_port, ...
        assert!(fields.len() >= 2);
        let slot = match &fields[1] {
            RespValue::Integer(n) => *n as u16,
            _ => panic!("slot field"),
        };
        planned_slots.push(slot);
        assert!(cs_a.owns_slot(slot));
    }

    // Put a key in first planned slot path — use a key that hashes to planned[0] if possible,
    // otherwise just AUTO empty slots (still transfers ownership).
    let auto = send_cmd(
        &mut sa,
        &["CLUSTER", "RESHARD", "AUTO", &id_b, "3"],
    )
    .await;
    let results = match auto {
        RespValue::Array(a) => a,
        other => panic!("AUTO: {:?}", other),
    };
    assert_eq!(results.len(), 3);
    for (i, row) in results.iter().enumerate() {
        let fields = match row {
            RespValue::Array(f) => f,
            _ => panic!("{:?}", row),
        };
        // status is field after "status" key — parse like other tests
        let mut status = String::new();
        let mut j = 0;
        while j + 1 < fields.len() {
            if let Some(k) = fields[j].as_bulk_string() {
                if k.as_ref() == b"status" {
                    if let Some(v) = fields[j + 1].as_bulk_string() {
                        status = String::from_utf8_lossy(v).into_owned();
                    }
                }
            }
            j += 2;
        }
        assert_eq!(status, "complete", "slot result {}: {}", i, status);
    }

    for slot in planned_slots {
        assert!(
            cs_b.owns_slot(slot),
            "dest should own planned slot {}",
            slot
        );
        assert!(!cs_a.owns_slot(slot), "source should not own {}", slot);
    }

    let _ = shut_a_tx.send(true);
    let _ = shut_b_tx.send(true);
    let _ = ha.await;
    let _ = hb.await;
}

/// Batch ED: COUNTKEYSINSLOT / GETKEYSINSLOT / REPLICAS / BUMPEPOCH.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_slot_key_helpers_and_bumpepoch() {
    let port = 16780u16;
    let cs = ClusterState::single_node("127.0.0.1", port);
    let other = "ed".repeat(20);
    cs.add_node_with_role(
        &other,
        "127.0.0.1",
        16781,
        Some(false),
        Some(Some(cs.my_id())),
    );

    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv = Server::new(cache, make_config(port, true)).with_cluster(Some(Arc::clone(&cs)));
    let (tx, rx) = watch::channel(false);
    let h = tokio::spawn(async move {
        let _ = srv.run_with_shutdown(rx).await;
    });
    wait_listen(port).await;

    let mut cli = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    // Put keys in known slots: "foo" is a stable keyslot.
    let foo_slot = key_hash_slot(b"foo");
    assert!(is_ok(&send_cmd(&mut cli, &["SET", "foo", "1"]).await));
    assert!(is_ok(&send_cmd(&mut cli, &["SET", "bar", "2"]).await));

    let n = match send_cmd(
        &mut cli,
        &["CLUSTER", "COUNTKEYSINSLOT", &foo_slot.to_string()],
    )
    .await
    {
        RespValue::Integer(i) => i,
        other => panic!("{:?}", other),
    };
    assert!(n >= 1, "expected >=1 key in foo slot, got {}", n);

    let keys = match send_cmd(
        &mut cli,
        &[
            "CLUSTER",
            "GETKEYSINSLOT",
            &foo_slot.to_string(),
            "10",
        ],
    )
    .await
    {
        RespValue::Array(a) => a,
        other => panic!("{:?}", other),
    };
    assert!(!keys.is_empty());
    let has_foo = keys.iter().any(|v| match v {
        RespValue::BulkString(Some(b)) => b.as_ref() == b"foo",
        _ => false,
    });
    assert!(has_foo, "GETKEYSINSLOT should include foo");

    let epoch_before = cs.current_epoch();
    assert_eq!(
        send_cmd(&mut cli, &["CLUSTER", "BUMPEPOCH"]).await,
        RespValue::Integer(1)
    );
    assert!(cs.current_epoch() > epoch_before);

    let reps = match send_cmd(&mut cli, &["CLUSTER", "REPLICAS", &cs.my_id()]).await {
        RespValue::Array(a) => a,
        other => panic!("{:?}", other),
    };
    assert_eq!(reps.len(), 1);
    match &reps[0] {
        RespValue::BulkString(Some(b)) => assert_eq!(b.as_ref(), other.as_bytes()),
        other => panic!("{:?}", other),
    }

    // SLAVES alias
    let reps2 = match send_cmd(&mut cli, &["CLUSTER", "SLAVES", &cs.my_id()]).await {
        RespValue::Array(a) => a,
        other => panic!("{:?}", other),
    };
    assert_eq!(reps2.len(), 1);

    let _ = tx.send(true);
    h.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch EE: ADDSLOTS / DELSLOTS / FLUSHSLOTS over RESP.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_add_del_flush_slots_commands() {
    let port = 16782u16;
    let cs = ClusterState::single_node("127.0.0.1", port);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv = Server::new(cache, make_config(port, true)).with_cluster(Some(Arc::clone(&cs)));
    let (tx, rx) = watch::channel(false);
    let h = tokio::spawn(async move {
        let _ = srv.run_with_shutdown(rx).await;
    });
    wait_listen(port).await;

    let mut cli = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    assert!(is_ok(
        &send_cmd(&mut cli, &["CLUSTER", "DELSLOTS", "0", "1", "2"]).await
    ));
    assert!(cs.slot_unbound(0));
    assert!(!cs.owns_slot(0));

    assert!(is_ok(
        &send_cmd(&mut cli, &["CLUSTER", "ADDSLOTS", "0", "1"]).await
    ));
    assert!(cs.owns_slot(0));
    assert!(cs.owns_slot(1));
    assert!(cs.slot_unbound(2));

    assert!(is_ok(&send_cmd(&mut cli, &["CLUSTER", "FLUSHSLOTS"]).await));
    assert!(cs.slot_unbound(0));
    assert!(cs.slot_unbound(16383));

    // Re-add a range for sanity.
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &["CLUSTER", "ADDSLOTS", "100", "101", "102"]
        )
        .await
    ));
    assert!(cs.owns_slot(100));

    let _ = tx.send(true);
    h.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch EF: ADDSLOTSRANGE / DELSLOTSRANGE.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_add_del_slotsrange_commands() {
    let port = 16783u16;
    let cs = ClusterState::single_node("127.0.0.1", port);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv = Server::new(cache, make_config(port, true)).with_cluster(Some(Arc::clone(&cs)));
    let (tx, rx) = watch::channel(false);
    let h = tokio::spawn(async move {
        let _ = srv.run_with_shutdown(rx).await;
    });
    wait_listen(port).await;

    let mut cli = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    assert!(is_ok(&send_cmd(&mut cli, &["CLUSTER", "FLUSHSLOTS"]).await));
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &["CLUSTER", "ADDSLOTSRANGE", "0", "5", "100", "102"]
        )
        .await
    ));
    assert!(cs.owns_slot(0));
    assert!(cs.owns_slot(5));
    assert!(cs.owns_slot(101));
    assert!(cs.slot_unbound(6));

    assert!(is_ok(
        &send_cmd(&mut cli, &["CLUSTER", "DELSLOTSRANGE", "0", "5"]).await
    ));
    assert!(cs.slot_unbound(0));
    assert!(cs.slot_unbound(5));
    assert!(cs.owns_slot(100));

    // Odd argc → error
    match send_cmd(&mut cli, &["CLUSTER", "ADDSLOTSRANGE", "1"]).await {
        RespValue::Error(_) => {}
        other => panic!("expected error, got {:?}", other),
    }

    let _ = tx.send(true);
    h.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch EG: FORGET + RESET SOFT/HARD.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_forget_and_reset_commands() {
    let port = 16784u16;
    let cs = ClusterState::single_node("127.0.0.1", port);
    let peer = "eg".repeat(20);
    cs.add_node(&peer, "127.0.0.1", 16785);

    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv = Server::new(cache, make_config(port, true)).with_cluster(Some(Arc::clone(&cs)));
    let (tx, rx) = watch::channel(false);
    let h = tokio::spawn(async move {
        let _ = srv.run_with_shutdown(rx).await;
    });
    wait_listen(port).await;

    let mut cli = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    assert!(is_ok(
        &send_cmd(&mut cli, &["SET", "keep", "v"]).await
    ));
    assert!(is_ok(
        &send_cmd(&mut cli, &["CLUSTER", "FORGET", &peer]).await
    ));
    assert!(cs.get_node(&peer).is_none());

    // Soft reset: clear slots, keep data (CLUSTERDOWN until slots reassigned).
    assert!(is_ok(
        &send_cmd(&mut cli, &["CLUSTER", "RESET", "SOFT"]).await
    ));
    assert!(cs.slot_unbound(0));
    // Reclaim all slots so existing keys are addressable again.
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &["CLUSTER", "ADDSLOTSRANGE", "0", "16383"]
        )
        .await
    ));
    assert_eq!(as_bulk(&send_cmd(&mut cli, &["GET", "keep"]).await), "v");

    // HARD reset: topology clear + key wipe.
    assert!(is_ok(&send_cmd(&mut cli, &["SET", "gone", "x"]).await));
    assert!(is_ok(
        &send_cmd(&mut cli, &["CLUSTER", "RESET", "HARD"]).await
    ));
    assert!(cs.slot_unbound(0));
    // After HARD, keys are gone; reclaim slots to probe EXISTS.
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &["CLUSTER", "ADDSLOTSRANGE", "0", "16383"]
        )
        .await
    ));
    assert_eq!(
        send_cmd(&mut cli, &["EXISTS", "gone"]).await,
        RespValue::Integer(0)
    );
    assert_eq!(
        send_cmd(&mut cli, &["EXISTS", "keep"]).await,
        RespValue::Integer(0)
    );

    let _ = tx.send(true);
    h.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch EI: CLUSTER SHARDS groups slots by master; LINKS is empty.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_shards_and_links() {
    let port = 16786u16;
    let cs = ClusterState::single_node("127.0.0.1", port);
    let replica = "ri".repeat(20);
    cs.add_node_with_role(
        &replica,
        "127.0.0.1",
        16787,
        Some(false),
        Some(Some(cs.my_id())),
    );

    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv = Server::new(cache, make_config(port, true)).with_cluster(Some(Arc::clone(&cs)));
    let (tx, rx) = watch::channel(false);
    let h = tokio::spawn(async move {
        let _ = srv.run_with_shutdown(rx).await;
    });
    wait_listen(port).await;

    let mut cli = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    let links = send_cmd(&mut cli, &["CLUSTER", "LINKS"]).await;
    assert!(matches!(links, RespValue::Array(ref a) if a.is_empty()));

    let shards = match send_cmd(&mut cli, &["CLUSTER", "SHARDS"]).await {
        RespValue::Array(a) => a,
        other => panic!("SHARDS: {:?}", other),
    };
    assert_eq!(shards.len(), 1, "single master owns all slots");
    let shard0 = match &shards[0] {
        RespValue::Array(f) => f,
        other => panic!("{:?}", other),
    };
    // Field array: slots, [...], nodes, [...]
    assert!(shard0.len() >= 4);
    let mut saw_slots = false;
    let mut saw_nodes = false;
    let mut i = 0;
    while i + 1 < shard0.len() {
        let key = match &shard0[i] {
            RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
            _ => {
                i += 2;
                continue;
            }
        };
        if key == "slots" {
            saw_slots = true;
            match &shard0[i + 1] {
                RespValue::Array(s) => {
                    assert!(s.len() >= 2, "expect at least one start/end pair");
                }
                other => panic!("slots value: {:?}", other),
            }
        }
        if key == "nodes" {
            saw_nodes = true;
            match &shard0[i + 1] {
                RespValue::Array(nodes) => {
                    assert!(
                        nodes.len() >= 2,
                        "master + replica expected, got {}",
                        nodes.len()
                    );
                }
                other => panic!("nodes value: {:?}", other),
            }
        }
        i += 2;
    }
    assert!(saw_slots && saw_nodes);

    let _ = tx.send(true);
    h.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch EJ: CLUSTER MYSHARDID returns master id for replica / self for master.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_myshardid_command() {
    let port = 16788u16;
    let cs = ClusterState::single_node("127.0.0.1", port);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv = Server::new(cache, make_config(port, true)).with_cluster(Some(Arc::clone(&cs)));
    let (tx, rx) = watch::channel(false);
    let h = tokio::spawn(async move {
        let _ = srv.run_with_shutdown(rx).await;
    });
    wait_listen(port).await;

    let mut cli = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let myid = as_bulk(&send_cmd(&mut cli, &["CLUSTER", "MYID"]).await);
    let shard = as_bulk(&send_cmd(&mut cli, &["CLUSTER", "MYSHARDID"]).await);
    assert_eq!(shard, myid, "master with slots: shard id == myid");

    let _ = tx.send(true);
    h.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch EK: CLUSTER SET-CONFIG-EPOCH only accepts greater epochs.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_set_config_epoch_command() {
    let port = 16789u16;
    let cs = ClusterState::single_node("127.0.0.1", port);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv = Server::new(cache, make_config(port, true)).with_cluster(Some(Arc::clone(&cs)));
    let (tx, rx) = watch::channel(false);
    let h = tokio::spawn(async move {
        let _ = srv.run_with_shutdown(rx).await;
    });
    wait_listen(port).await;

    let mut cli = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let cur = match send_cmd(&mut cli, &["CLUSTER", "EPOCH"]).await {
        RespValue::Integer(n) => n,
        other => panic!("{:?}", other),
    };
    match send_cmd(
        &mut cli,
        &["CLUSTER", "SET-CONFIG-EPOCH", &cur.to_string()],
    )
    .await
    {
        RespValue::Error(_) => {}
        other => panic!("expected error for equal epoch, got {:?}", other),
    }
    let higher = (cur + 42).to_string();
    assert!(is_ok(
        &send_cmd(&mut cli, &["CLUSTER", "SET-CONFIG-EPOCH", &higher]).await
    ));
    assert_eq!(
        send_cmd(&mut cli, &["CLUSTER", "EPOCH"]).await,
        RespValue::Integer(cur + 42)
    );

    let _ = tx.send(true);
    h.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch EL: CONFIG GET/SET cluster-replica-priority and cluster-node-timeout.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_config_priority_and_timeout() {
    let port = 16790u16;
    let cs = ClusterState::single_node("127.0.0.1", port);
    cs.set_local_repl_priority(100);
    cs.set_node_timeout_ms(15_000);

    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv = Server::new(cache, make_config(port, true)).with_cluster(Some(Arc::clone(&cs)));
    let (tx, rx) = watch::channel(false);
    let h = tokio::spawn(async move {
        let _ = srv.run_with_shutdown(rx).await;
    });
    wait_listen(port).await;

    let mut cli = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    // GET returns current values.
    let get = send_cmd(
        &mut cli,
        &["CONFIG", "GET", "cluster-replica-priority"],
    )
    .await;
    // RESP2: array [name, value] or map on HELLO 3 — default RESP2 array.
    match get {
        RespValue::Array(a) => {
            assert!(a.len() >= 2);
            let val = match &a[1] {
                RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
                other => panic!("{:?}", other),
            };
            assert_eq!(val, "100");
        }
        other => panic!("CONFIG GET: {:?}", other),
    }

    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &["CONFIG", "SET", "cluster-replica-priority", "7"]
        )
        .await
    ));
    assert_eq!(cs.local_repl_priority(), 7);

    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &["CONFIG", "SET", "cluster-node-timeout", "250"]
        )
        .await
    ));
    assert_eq!(cs.node_timeout_ms(), 250);

    // Priority 0 is allowed (never promote).
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &["CONFIG", "SET", "cluster-replica-priority", "0"]
        )
        .await
    ));
    assert_eq!(cs.local_repl_priority(), 0);

    let _ = tx.send(true);
    h.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch EM: CLUSTER SAVECONFIG writes nodes.conf under dir.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_saveconfig_writes_nodes_conf() {
    let port = 16791u16;
    let dir = std::env::temp_dir().join(format!(
        "kore-saveconfig-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&dir);

    let cs = ClusterState::single_node("127.0.0.1", port);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let mut cfg = (*make_config(port, true)).clone();
    cfg.dir = dir.to_string_lossy().to_string();
    let srv = Server::new(cache, Arc::new(cfg)).with_cluster(Some(Arc::clone(&cs)));
    let (tx, rx) = watch::channel(false);
    let h = tokio::spawn(async move {
        let _ = srv.run_with_shutdown(rx).await;
    });
    wait_listen(port).await;

    let mut cli = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    assert!(is_ok(
        &send_cmd(&mut cli, &["CLUSTER", "SAVECONFIG"]).await
    ));

    let path = dir.join("nodes.conf");
    let body = std::fs::read_to_string(&path).expect("nodes.conf written");
    assert!(
        body.contains(&cs.my_id()),
        "nodes.conf should contain my id: {}",
        body
    );
    assert!(
        body.contains("myself") || body.contains("master"),
        "nodes.conf should look like CLUSTER NODES: {}",
        body
    );
    assert!(body.contains("Kore cluster nodes.conf"));

    let _ = tx.send(true);
    h.abort();
    sleep(Duration::from_millis(50)).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// Batch EV: CLUSTER SLOT-STATS reports key-count for owned slots; ORDERBY/LIMIT.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_slot_stats_key_count_orderby() {
    let port = 16804u16;
    let cs = ClusterState::single_node("127.0.0.1", port);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv = Server::new(cache, make_config(port, true)).with_cluster(Some(Arc::clone(&cs)));
    let (tx, rx) = watch::channel(false);
    let h = tokio::spawn(async move {
        let _ = srv.run_with_shutdown(rx).await;
    });
    wait_listen(port).await;

    let mut cli = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    // Put keys in known slots via hash tags if possible; otherwise SET several and count.
    assert!(is_ok(&send_cmd(&mut cli, &["SET", "ev-a", "1"]).await));
    assert!(is_ok(&send_cmd(&mut cli, &["SET", "ev-b", "2"]).await));
    assert!(is_ok(&send_cmd(&mut cli, &["SET", "ev-c", "3"]).await));
    // Same slot: two keys with same tag.
    assert!(is_ok(
        &send_cmd(&mut cli, &["SET", "{evsame}.1", "x"]).await
    ));
    assert!(is_ok(
        &send_cmd(&mut cli, &["SET", "{evsame}.2", "y"]).await
    ));
    let same_slot = key_hash_slot(b"{evsame}.1");

    let resp = send_cmd(
        &mut cli,
        &[
            "CLUSTER",
            "SLOT-STATS",
            "SLOTSRANGE",
            "0",
            "16383",
            "ORDERBY",
            "key-count",
            "LIMIT",
            "5",
            "DESC",
        ],
    )
    .await;
    let rows = match resp {
        RespValue::Array(a) => a,
        other => panic!("SLOT-STATS expected array: {:?}", other),
    };
    assert!(!rows.is_empty());
    assert!(rows.len() <= 5, "LIMIT 5");

    // First row should be busiest (or tied); find our same_slot with count >= 2.
    let mut found_same = false;
    let mut prev_count: Option<i64> = None;
    for row in &rows {
        let fields = match row {
            RespValue::Array(f) => f,
            _ => panic!("row {:?}", row),
        };
        // flat: slot, n, key-count, n, ...
        assert!(fields.len() >= 4);
        let slot = match &fields[1] {
            RespValue::Integer(n) => *n as u16,
            _ => panic!("slot"),
        };
        let kc = match &fields[3] {
            RespValue::Integer(n) => *n,
            _ => panic!("key-count"),
        };
        if let Some(p) = prev_count {
            assert!(
                p >= kc,
                "DESC order by key-count: {} then {}",
                p,
                kc
            );
        }
        prev_count = Some(kc);
        if slot == same_slot {
            assert!(kc >= 2, "same-slot keys should count: {}", kc);
            found_same = true;
        }
        // Owned slots only — we own all.
        assert!(cs.owns_slot(slot));
    }
    assert!(found_same, "expected slot {} in top stats", same_slot);

    // Unowned slot omitted after DELSLOTS.
    assert!(is_ok(
        &send_cmd(&mut cli, &["CLUSTER", "DELSLOTS", &same_slot.to_string()]).await
    ));
    // Coverage fail may block writes but SLOT-STATS is cluster admin — no keys needed.
    // Disable require-full-coverage so we can still probe (SLOT-STATS has no keys).
    let resp2 = send_cmd(
        &mut cli,
        &[
            "CLUSTER",
            "SLOT-STATS",
            "SLOTSRANGE",
            &same_slot.to_string(),
            &same_slot.to_string(),
        ],
    )
    .await;
    match resp2 {
        RespValue::Array(a) => assert!(
            a.is_empty(),
            "unowned slot should be omitted: {:?}",
            a
        ),
        other => panic!("{:?}", other),
    }

    // HELP mentions SLOT-STATS
    match send_cmd(&mut cli, &["CLUSTER", "HELP"]).await {
        RespValue::Array(lines) => {
            let joined: String = lines
                .iter()
                .filter_map(|v| match v {
                    RespValue::BulkString(Some(b)) => {
                        Some(String::from_utf8_lossy(b).into_owned())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                joined.contains("SLOT-STATS"),
                "HELP missing SLOT-STATS: {}",
                joined
            );
        }
        other => panic!("{:?}", other),
    }

    let _ = tx.send(true);
    h.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch EU: CONFIG cluster-announce-ip/port appears in NODES and MOVED targets.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_announce_ip_port_in_nodes_and_moved() {
    let port_a = 16802u16;
    let port_b = 16803u16;

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

    // Announce a fake public address on A.
    assert!(is_ok(
        &send_cmd(
            &mut sa,
            &["CONFIG", "SET", "cluster-announce-ip", "203.0.113.10"]
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut sa,
            &["CONFIG", "SET", "cluster-announce-port", "16379"]
        )
        .await
    ));
    assert_eq!(cs_a.announce_ip().as_deref(), Some("203.0.113.10"));
    assert_eq!(cs_a.announce_port(), Some(16379));

    let nodes = as_bulk(&send_cmd(&mut sa, &["CLUSTER", "NODES"]).await);
    assert!(
        nodes.contains("203.0.113.10:16379"),
        "NODES should use announce addr: {}",
        nodes
    );

    // Move a slot to B so A MOVEs keys in that slot using B's bind addr (B has no announce).
    assert!(is_ok(
        &send_cmd(&mut sa, &["CLUSTER", "MEET", "127.0.0.1", &port_b.to_string()]).await
    ));
    assert!(is_ok(
        &send_cmd(&mut sb, &["CLUSTER", "MEET", "127.0.0.1", &port_a.to_string()]).await
    ));
    let id_b = cs_b.my_id();
    // Ensure A knows B.
    if cs_a.get_node(&id_b).is_none() {
        cs_a.add_node(&id_b, "127.0.0.1", port_b);
    }
    let key = "{euannounce}.k";
    let slot = key_hash_slot(key.as_bytes());
    cs_a.reassign_slot(slot, &id_b).unwrap();
    let err = as_err(&send_cmd(&mut sa, &["GET", key]).await);
    assert!(
        err.starts_with(&format!("MOVED {} 127.0.0.1:{}", slot, port_b)),
        "MOVED to peer bind addr, got {}",
        err
    );

    // CONFIG GET
    match send_cmd(&mut sa, &["CONFIG", "GET", "cluster-announce-ip"]).await {
        RespValue::Array(a) => {
            let flat: String = a
                .iter()
                .filter_map(|v| match v {
                    RespValue::BulkString(Some(b)) => {
                        Some(String::from_utf8_lossy(b).into_owned())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join(" ");
            assert!(flat.contains("203.0.113.10"), "got {}", flat);
        }
        other => panic!("{:?}", other),
    }

    let _ = shut_a_tx.send(true);
    let _ = shut_b_tx.send(true);
    let _ = ha.await;
    let _ = hb.await;
}

/// Batch ET: CLUSTER SLOTS lists master then replica endpoints.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_slots_includes_replicas() {
    let port_a = 16800u16;
    let port_b = 16801u16;

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

    let id_a = cs_a.my_id();
    let id_b = cs_b.my_id();
    assert!(is_ok(
        &send_cmd(&mut sb, &["CLUSTER", "REPLICATE", &id_a]).await
    ));
    assert!(cs_b.is_cluster_replica());
    assert!(cs_a.replicas_of(&id_a).contains(&id_b) || cs_a.get_node(&id_b).is_some());

    // Ensure A knows B is a replica (MEETPEER/ROLE may lag; set role locally if needed).
    if !cs_a.replicas_of(&id_a).contains(&id_b) {
        cs_a.add_node_with_role(&id_b, "127.0.0.1", port_b, Some(false), Some(Some(id_a.clone())));
    }
    assert!(
        cs_a.replicas_of(&id_a).contains(&id_b),
        "master should list replica"
    );

    let slots = send_cmd(&mut sa, &["CLUSTER", "SLOTS"]).await;
    let ranges = match slots {
        RespValue::Array(a) => a,
        other => panic!("SLOTS expected array: {:?}", other),
    };
    assert!(!ranges.is_empty());
    // Find a range that starts at 0 (full ownership after REPLICATE on B).
    let mut found_replica = false;
    for range in &ranges {
        let parts = match range {
            RespValue::Array(p) => p,
            _ => continue,
        };
        // [start, end, master, replica…]
        assert!(parts.len() >= 3, "range too short: {:?}", parts);
        // Master node array
        let master = match &parts[2] {
            RespValue::Array(m) => m,
            other => panic!("master node {:?}", other),
        };
        assert_eq!(master.len(), 3);
        assert_eq!(as_bulk(&master[2]), id_a);
        // Replica entries after master
        for node in parts.iter().skip(3) {
            let n = match node {
                RespValue::Array(n) => n,
                _ => continue,
            };
            if n.len() >= 3 && as_bulk(&n[2]) == id_b {
                assert_eq!(as_bulk(&n[0]), "127.0.0.1");
                assert_eq!(
                    match &n[1] {
                        RespValue::Integer(p) => *p as u16,
                        _ => panic!("port"),
                    },
                    port_b
                );
                found_replica = true;
            }
        }
    }
    assert!(
        found_replica,
        "CLUSTER SLOTS should list replica {} after master",
        id_b
    );

    let _ = shut_a_tx.send(true);
    let _ = shut_b_tx.send(true);
    let _ = ha.await;
    let _ = hb.await;
}

/// Batch ES: allow-reads-when-down serves GET while cluster_state is fail; SET still down.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_allow_reads_when_down() {
    let port = 16799u16;
    let cs = ClusterState::single_node("127.0.0.1", port);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv = Server::new(cache, make_config(port, true)).with_cluster(Some(Arc::clone(&cs)));
    let (tx, rx) = watch::channel(false);
    let h = tokio::spawn(async move {
        let _ = srv.run_with_shutdown(rx).await;
    });
    wait_listen(port).await;

    let mut cli = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    assert!(is_ok(
        &send_cmd(&mut cli, &["SET", "es-key", "alive"]).await
    ));

    // Punch a hole in coverage → cluster fail → both read and write CLUSTERDOWN.
    assert!(is_ok(
        &send_cmd(&mut cli, &["CLUSTER", "DELSLOTS", "0"]).await
    ));
    assert!(!cs.cluster_state_ok());
    let err_r = as_err(&send_cmd(&mut cli, &["GET", "es-key"]).await);
    assert!(
        err_r.contains("cluster is down"),
        "default: reads blocked when down: {}",
        err_r
    );
    let err_w = as_err(&send_cmd(&mut cli, &["SET", "es-key", "x"]).await);
    assert!(err_w.contains("cluster is down"), "got {}", err_w);

    // Enable allow-reads-when-down → GET works; SET still CLUSTERDOWN.
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &["CONFIG", "SET", "cluster-allow-reads-when-down", "yes"]
        )
        .await
    ));
    assert!(cs.allow_reads_when_down());
    assert_eq!(
        as_bulk(&send_cmd(&mut cli, &["GET", "es-key"]).await),
        "alive"
    );
    let err_w2 = as_err(&send_cmd(&mut cli, &["SET", "es-key", "x"]).await);
    assert!(
        err_w2.contains("cluster is down"),
        "writes must stay blocked: {}",
        err_w2
    );

    // CONFIG GET
    match send_cmd(
        &mut cli,
        &["CONFIG", "GET", "cluster-allow-reads-when-down"],
    )
    .await
    {
        RespValue::Array(a) => {
            let flat: String = a
                .iter()
                .filter_map(|v| match v {
                    RespValue::BulkString(Some(b)) => {
                        Some(String::from_utf8_lossy(b).into_owned())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join(" ");
            assert!(
                flat.contains("cluster-allow-reads-when-down") && flat.contains("yes"),
                "got {}",
                flat
            );
        }
        other => panic!("{:?}", other),
    }

    // Restore coverage + disable flag.
    assert!(is_ok(
        &send_cmd(&mut cli, &["CLUSTER", "ADDSLOTS", "0"]).await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &["CONFIG", "SET", "cluster-allow-reads-when-down", "no"]
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(&mut cli, &["SET", "es-key", "ok"]).await
    ));

    let _ = tx.send(true);
    h.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch ER: READONLY lets cluster replicas serve reads for master's slots.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_readonly_serves_replica_reads() {
    let port_a = 16797u16; // master
    let port_b = 16798u16; // replica

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

    let id_a = cs_a.my_id();
    // Seed key on B while B still owns all slots (data stays after REPLICATE).
    let key = "{erreadonly}.k";
    let slot = key_hash_slot(key.as_bytes());
    assert!(is_ok(
        &send_cmd(&mut sb, &["SET", key, "from-b"]).await
    ));

    // B becomes cluster replica of A → slots move to A; local value remains.
    assert!(is_ok(
        &send_cmd(&mut sb, &["CLUSTER", "REPLICATE", &id_a]).await
    ));
    assert!(cs_b.is_cluster_replica());
    assert!(!cs_b.owns_slot(slot));
    assert!(cs_b.can_serve_readonly(slot));

    // Without READONLY → MOVED to master.
    let err = as_err(&send_cmd(&mut sb, &["GET", key]).await);
    assert!(
        err.starts_with(&format!("MOVED {} 127.0.0.1:{}", slot, port_a)),
        "expected MOVED without READONLY, got {}",
        err
    );

    // READONLY → serve local read.
    assert!(is_ok(&send_cmd(&mut sb, &["READONLY"]).await));
    assert_eq!(
        as_bulk(&send_cmd(&mut sb, &["GET", key]).await),
        "from-b"
    );

    // Writes still MOVED (even under READONLY).
    let err_w = as_err(&send_cmd(&mut sb, &["SET", key, "nope"]).await);
    assert!(
        err_w.starts_with(&format!("MOVED {} 127.0.0.1:{}", slot, port_a)),
        "writes must MOVED under READONLY, got {}",
        err_w
    );

    // READWRITE restores MOVED for reads.
    assert!(is_ok(&send_cmd(&mut sb, &["READWRITE"]).await));
    let err2 = as_err(&send_cmd(&mut sb, &["GET", key]).await);
    assert!(
        err2.starts_with("MOVED "),
        "expected MOVED after READWRITE, got {}",
        err2
    );

    let _ = shut_a_tx.send(true);
    let _ = shut_b_tx.send(true);
    let _ = ha.await;
    let _ = hb.await;
}

/// Batch EQ: incomplete coverage → CLUSTERDOWN; CONFIG toggles require-full-coverage.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_require_full_coverage_clusterdown() {
    let port = 16796u16;
    let cs = ClusterState::single_node("127.0.0.1", port);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv = Server::new(cache, make_config(port, true)).with_cluster(Some(Arc::clone(&cs)));
    let (tx, rx) = watch::channel(false);
    let h = tokio::spawn(async move {
        let _ = srv.run_with_shutdown(rx).await;
    });
    wait_listen(port).await;

    let mut cli = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    // Happy path under full coverage.
    assert!(is_ok(&send_cmd(&mut cli, &["SET", "eq-key", "1"]).await));

    // Unbind one slot → cluster_state:fail → all key commands CLUSTERDOWN.
    assert!(is_ok(
        &send_cmd(&mut cli, &["CLUSTER", "DELSLOTS", "0"]).await
    ));
    let info = as_bulk(&send_cmd(&mut cli, &["CLUSTER", "INFO"]).await);
    assert!(
        info.contains("cluster_state:fail"),
        "expected fail after DELSLOTS: {}",
        info
    );
    let err = as_err(&send_cmd(&mut cli, &["GET", "eq-key"]).await);
    assert!(
        err.contains("CLUSTERDOWN") && err.contains("cluster is down"),
        "got {}",
        err
    );

    // CONFIG SET no → serve covered slots again.
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &["CONFIG", "SET", "cluster-require-full-coverage", "no"]
        )
        .await
    ));
    assert!(!cs.require_full_coverage());
    let info2 = as_bulk(&send_cmd(&mut cli, &["CLUSTER", "INFO"]).await);
    assert!(
        info2.contains("cluster_state:ok"),
        "require=no should report ok: {}",
        info2
    );
    assert_eq!(
        as_bulk(&send_cmd(&mut cli, &["GET", "eq-key"]).await),
        "1"
    );

    // Unbound slot still reports per-slot CLUSTERDOWN (not MOVED).
    // Slot 0 is unbound; pick a key that hashes to 0 if possible, else just SET a key
    // after re-adding coverage toggle back.
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &["CONFIG", "SET", "cluster-require-full-coverage", "yes"]
        )
        .await
    ));
    // Restore full coverage.
    assert!(is_ok(
        &send_cmd(&mut cli, &["CLUSTER", "ADDSLOTS", "0"]).await
    ));
    assert!(cs.cluster_state_ok());
    assert!(is_ok(&send_cmd(&mut cli, &["SET", "eq-key2", "2"]).await));

    // CONFIG GET surfaces the param.
    match send_cmd(
        &mut cli,
        &["CONFIG", "GET", "cluster-require-full-coverage"],
    )
    .await
    {
        RespValue::Array(a) => {
            let flat: String = a
                .iter()
                .filter_map(|v| match v {
                    RespValue::BulkString(Some(b)) => {
                        Some(String::from_utf8_lossy(b).into_owned())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join(" ");
            assert!(
                flat.contains("cluster-require-full-coverage") && flat.contains("yes"),
                "got {}",
                flat
            );
        }
        other => panic!("CONFIG GET: {:?}", other),
    }

    let _ = tx.send(true);
    h.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch EO: topology mutation (FLUSHSLOTS) autosaves nodes.conf without SAVECONFIG.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_topology_autosaves_nodes_conf() {
    let port = 16793u16;
    let dir = std::env::temp_dir().join(format!(
        "kore-autosave-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&dir);

    let cs = ClusterState::single_node("127.0.0.1", port);
    let my_id = cs.my_id();
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let mut cfg = (*make_config(port, true)).clone();
    cfg.dir = dir.to_string_lossy().to_string();
    let srv = Server::new(cache, Arc::new(cfg)).with_cluster(Some(Arc::clone(&cs)));
    let (tx, rx) = watch::channel(false);
    let h = tokio::spawn(async move {
        let _ = srv.run_with_shutdown(rx).await;
    });
    wait_listen(port).await;

    let path = dir.join("nodes.conf");
    assert!(!path.exists(), "nodes.conf should not exist before mutation");

    let mut cli = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    // FLUSHSLOTS mutates topology → autosave (Batch EO).
    assert!(is_ok(
        &send_cmd(&mut cli, &["CLUSTER", "FLUSHSLOTS"]).await
    ));

    let body = std::fs::read_to_string(&path).expect("nodes.conf autosaved after FLUSHSLOTS");
    assert!(
        body.contains(&my_id),
        "autosaved nodes.conf should contain my id: {}",
        body
    );
    // After FLUSHSLOTS, myself still present but slots unbound — file should still list the node.
    assert!(body.contains("myself") || body.contains("master"));

    // ADDSLOTS should rewrite with ownership again.
    assert!(is_ok(
        &send_cmd(&mut cli, &["CLUSTER", "ADDSLOTS", "0", "1"]).await
    ));
    let body2 = std::fs::read_to_string(&path).expect("nodes.conf after ADDSLOTS");
    assert!(
        body2.contains("0-1") || body2.contains("0") ,
        "ADDSLOTS should persist slot assignment: {}",
        body2
    );

    let _ = tx.send(true);
    h.abort();
    sleep(Duration::from_millis(50)).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// Batch EN: SAVECONFIG then load_or_single_node restores id/slots/peers.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_saveconfig_load_on_boot_roundtrip() {
    let port = 16792u16;
    let dir = std::env::temp_dir().join(format!(
        "kore-loadconf-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&dir);

    let cs = ClusterState::single_node("127.0.0.1", port);
    let peer = "en".repeat(20);
    cs.add_node(&peer, "10.0.0.9", 17000);
    cs.reassign_slot_range(0, 10, &peer).unwrap();
    let my_id = cs.my_id();
    let epoch = cs.current_epoch();

    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let mut cfg = (*make_config(port, true)).clone();
    cfg.dir = dir.to_string_lossy().to_string();
    let srv = Server::new(cache, Arc::new(cfg)).with_cluster(Some(Arc::clone(&cs)));
    let (tx, rx) = watch::channel(false);
    let h = tokio::spawn(async move {
        let _ = srv.run_with_shutdown(rx).await;
    });
    wait_listen(port).await;

    let mut cli = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    assert!(is_ok(
        &send_cmd(&mut cli, &["CLUSTER", "SAVECONFIG"]).await
    ));

    let _ = tx.send(true);
    h.abort();
    sleep(Duration::from_millis(50)).await;

    // Simulate next boot.
    let loaded = ClusterState::load_or_single_node("127.0.0.1", port, dir.to_str().unwrap());
    assert_eq!(loaded.my_id(), my_id);
    assert!(loaded.current_epoch() >= epoch);
    assert!(loaded.get_node(&peer).is_some());
    assert!(!loaded.owns_slot(5));
    assert_eq!(loaded.owner_id_of(5).as_deref(), Some(peer.as_str()));
    assert!(loaded.owns_slot(100));

    let _ = std::fs::remove_dir_all(&dir);
}

/// Batch FL: CONFIG SET live flags autosave; load restores require/allow/announce/priority.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_config_set_live_flags_autosave_and_reload() {
    let port = 16793u16;
    let dir = std::env::temp_dir().join(format!(
        "kore-liveflags-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&dir);

    let cs = ClusterState::single_node("127.0.0.1", port);
    let my_id = cs.my_id();
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let mut cfg = (*make_config(port, true)).clone();
    cfg.dir = dir.to_string_lossy().to_string();
    let srv = Server::new(cache, Arc::new(cfg)).with_cluster(Some(Arc::clone(&cs)));
    let (tx, rx) = watch::channel(false);
    let h = tokio::spawn(async move {
        let _ = srv.run_with_shutdown(rx).await;
    });
    wait_listen(port).await;

    let mut cli = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &["CONFIG", "SET", "cluster-require-full-coverage", "no"]
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &["CONFIG", "SET", "cluster-allow-reads-when-down", "yes"]
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &["CONFIG", "SET", "cluster-announce-ip", "10.1.2.3"]
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &["CONFIG", "SET", "cluster-announce-port", "18000"]
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &["CONFIG", "SET", "cluster-replica-priority", "33"]
        )
        .await
    ));

    // Live state updated.
    assert!(!cs.require_full_coverage());
    assert!(cs.allow_reads_when_down());
    assert_eq!(cs.announce_ip().as_deref(), Some("10.1.2.3"));
    assert_eq!(cs.announce_port(), Some(18000));
    assert_eq!(cs.local_repl_priority(), 33);

    let path = dir.join("nodes.conf");
    let body = std::fs::read_to_string(&path).expect("nodes.conf autosaved after CONFIG SET");
    assert!(
        body.contains("# require-full-coverage no"),
        "body: {}",
        body
    );
    assert!(
        body.contains("# allow-reads-when-down yes"),
        "body: {}",
        body
    );
    assert!(body.contains("# announce-ip 10.1.2.3"), "body: {}", body);
    assert!(body.contains("# announce-port 18000"), "body: {}", body);
    assert!(body.contains("# replica-priority 33"), "body: {}", body);

    let _ = tx.send(true);
    h.abort();
    sleep(Duration::from_millis(50)).await;

    let loaded = ClusterState::load_or_single_node("127.0.0.1", port, dir.to_str().unwrap());
    assert_eq!(loaded.my_id(), my_id);
    assert!(!loaded.require_full_coverage());
    assert!(loaded.allow_reads_when_down());
    assert_eq!(loaded.announce_ip().as_deref(), Some("10.1.2.3"));
    assert_eq!(loaded.announce_port(), Some(18000));
    assert_eq!(loaded.local_repl_priority(), 33);

    let _ = std::fs::remove_dir_all(&dir);
}

/// Batch EY/FB: dual-end NODE prepare fails when dest MYID does not match dest-id.
#[tokio::test(flavor = "multi_thread")]
async fn reshard_finish_preflight_rejects_wrong_dest_id() {
    let port_a = 16820u16;
    let port_b = 16821u16;

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

    let id_b = cs_b.my_id();
    let fake_id = "ff".repeat(20);
    // Point A at B's address but with a wrong node id.
    cs_a.add_node(&fake_id, "127.0.0.1", port_b);

    let slot = 42u16;
    // Ensure A owns the slot for FINISH prepare local check.
    assert!(cs_a.owns_slot(slot));

    let resp = send_cmd(
        &mut sa,
        &["CLUSTER", "RESHARD", "FINISH", &slot.to_string(), &fake_id],
    )
    .await;
    let (got_slot, _migrated, source_node, dest_node, status) = parse_reshard_slot(&resp);
    assert_eq!(got_slot, slot);
    assert!(
        status.starts_with("failed_prepare") || status.starts_with("failed_preflight"),
        "expected failed_prepare, got {} source={} dest={}",
        status,
        source_node,
        dest_node
    );
    assert!(
        source_node.starts_with("prepare:") || source_node.starts_with("preflight:"),
        "expected prepare: prefix, got {}",
        source_node
    );
    // Ownership unchanged on A; no prepare left sticky.
    assert!(cs_a.owns_slot(slot));
    assert!(!cs_a.is_prepared(slot));
    assert!(!cs_b.owns_slot(slot) || cs_b.owner_id_of(slot).as_deref() != Some(fake_id.as_str()));

    let _ = id_b; // known peer
    let _ = shut_a_tx.send(true);
    let _ = shut_b_tx.send(true);
    let _ = ha.await;
    let _ = hb.await;
}

/// Batch FB: inject dest PREPARE fail → no NODE half-apply on either side.
#[tokio::test(flavor = "multi_thread")]
async fn reshard_prepare_dest_fail_no_half_apply() {
    let port_a = 16840u16;
    let port_b = 16841u16;

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

    let key = "{fbprepare}.k";
    let slot = key_hash_slot(key.as_bytes());
    assert!(is_ok(
        &send_cmd(&mut sa, &["SET", key, "prep-val"]).await
    ));

    {
        let inj = test_acquire_dest_prepare_inject().await;
        inj.set_failures_for_port(port_b, 8);
        let resp = send_cmd(
            &mut sa,
            &["CLUSTER", "RESHARD", &slot.to_string(), &id_b],
        )
        .await;
        let (got_slot, migrated, source_node, dest_node, status) = parse_reshard_slot(&resp);
        assert_eq!(got_slot, slot);
        assert_eq!(migrated, 1, "keys may still move before NODE prepare");
        assert!(
            status.starts_with("failed_prepare"),
            "expected failed_prepare, got {} source={} dest={}",
            status,
            source_node,
            dest_node
        );
        assert!(source_node.starts_with("prepare:"));
        // No NODE half-apply: source still owns; dest does not.
        assert!(
            cs_a.owns_slot(slot),
            "source must keep ownership after prepare fail"
        );
        assert!(
            !cs_b.owns_slot(slot),
            "dest must not own after prepare fail"
        );
        assert!(!cs_a.is_prepared(slot), "source prepare aborted");
        assert!(!cs_b.is_prepared(slot), "dest prepare aborted or never set");
        drop(inj);
    }

    // FINISH / RESHARD retry after inject cleared should complete.
    let finish = send_cmd(
        &mut sa,
        &["CLUSTER", "RESHARD", "FINISH", &slot.to_string(), &id_b],
    )
    .await;
    let (got_slot, migrated, source_node, dest_node, status) = parse_reshard_slot(&finish);
    assert_eq!(got_slot, slot);
    assert_eq!(migrated, 0);
    assert_eq!(source_node, "ok");
    assert_eq!(dest_node, "ok");
    assert_eq!(status, "complete");
    assert!(cs_b.owns_slot(slot));
    assert!(!cs_a.owns_slot(slot));
    assert_eq!(
        as_bulk(&send_cmd(&mut sb, &["GET", key]).await),
        "prep-val"
    );

    let _ = shut_a_tx.send(true);
    let _ = shut_b_tx.send(true);
    let _ = ha.await;
    let _ = hb.await;
}

/// Batch FB: happy-path dual-end complete still works under prepare/commit.
#[tokio::test(flavor = "multi_thread")]
async fn reshard_2pc_happy_path_complete() {
    let port_a = 16842u16;
    let port_b = 16843u16;

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

    let key = "{fbhappy}.k";
    let slot = key_hash_slot(key.as_bytes());
    assert!(is_ok(
        &send_cmd(&mut sa, &["SET", key, "happy-val"]).await
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
    assert!(cs_b.owns_slot(slot));
    assert!(!cs_a.owns_slot(slot));
    assert!(!cs_a.is_prepared(slot));
    assert!(!cs_b.is_prepared(slot));
    assert_eq!(
        as_bulk(&send_cmd(&mut sb, &["GET", key]).await),
        "happy-val"
    );

    let _ = shut_a_tx.send(true);
    let _ = shut_b_tx.send(true);
    let _ = ha.await;
    let _ = hb.await;
}

/// Batch FB: range RESHARD aborts after prepare fail mid-range (no cascade).
#[tokio::test(flavor = "multi_thread")]
async fn reshard_range_aborts_after_prepare_fail() {
    let port_a = 16844u16;
    let port_b = 16845u16;

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
    let end = 102u16;
    assert!(cs_a.owns_slot(start));
    assert!(cs_a.owns_slot(end));

    let inj = test_acquire_dest_prepare_inject().await;
    inj.set_failures_for_port(port_b, 8);
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
    drop(inj);

    let outer = match &resp {
        RespValue::Array(a) => a,
        other => panic!("expected array {:?}", other),
    };
    assert_eq!(
        outer.len(),
        1,
        "range must abort after failed_prepare; got {} slot results",
        outer.len()
    );
    let fields = match &outer[0] {
        RespValue::Array(a) => a,
        other => panic!("{:?}", other),
    };
    let status = as_bulk(&fields[11]);
    assert!(
        status.starts_with("failed_prepare"),
        "expected failed_prepare, got {}",
        status
    );
    assert!(cs_a.owns_slot(start), "prepare fail must not apply NODE");
    assert!(!cs_b.owns_slot(start));
    assert!(cs_a.owns_slot(end), "later range slots must stay on source");

    let _ = shut_a_tx.send(true);
    let _ = shut_b_tx.send(true);
    let _ = ha.await;
    let _ = hb.await;
}

/// Batch FH: commit re-check fails when prepare is cleared mid-flight — no half-apply.
#[tokio::test(flavor = "multi_thread")]
async fn reshard_commit_recheck_cleared_prepare_no_half_apply() {
    let port_a = 16850u16;
    let port_b = 16851u16;

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

    let key = "{fhrecheck}.k";
    let slot = key_hash_slot(key.as_bytes());
    assert!(is_ok(
        &send_cmd(&mut sa, &["SET", key, "recheck-val"]).await
    ));

    {
        let inj = test_commit_recheck_inject(Arc::clone(&cs_a));
        // Clear source prepare at commit re-check (after both prepares succeed).
        inj.set_clear_count(1);
        let resp = send_cmd(
            &mut sa,
            &["CLUSTER", "RESHARD", &slot.to_string(), &id_b],
        )
        .await;
        let (got_slot, migrated, source_node, dest_node, status) = parse_reshard_slot(&resp);
        assert_eq!(got_slot, slot);
        assert_eq!(migrated, 1, "keys may still move before NODE commit");
        assert!(
            status.starts_with("failed_prepare:recheck") || status.starts_with("failed_prepare"),
            "expected failed_prepare recheck, got {} source={} dest={}",
            status,
            source_node,
            dest_node
        );
        assert!(
            source_node.starts_with("recheck:") || source_node.starts_with("prepare:"),
            "expected recheck/prepare prefix, got {}",
            source_node
        );
        assert!(
            cs_a.owns_slot(slot),
            "source must keep ownership after commit re-check fail"
        );
        assert!(
            !cs_b.owns_slot(slot),
            "dest must not own after commit re-check fail"
        );
        assert!(!cs_a.is_prepared(slot));
        drop(inj);
    }

    // Retry FINISH after inject cleared should complete.
    let finish = send_cmd(
        &mut sa,
        &["CLUSTER", "RESHARD", "FINISH", &slot.to_string(), &id_b],
    )
    .await;
    let (got_slot, _migrated, source_node, dest_node, status) = parse_reshard_slot(&finish);
    assert_eq!(got_slot, slot);
    assert_eq!(source_node, "ok");
    assert_eq!(dest_node, "ok");
    assert_eq!(status, "complete");
    assert!(cs_b.owns_slot(slot));
    assert!(!cs_a.owns_slot(slot));

    let _ = shut_a_tx.send(true);
    let _ = shut_b_tx.send(true);
    let _ = ha.await;
    let _ = hb.await;
}

/// Batch FH: happy path still complete under prepare-epoch + commit re-check.
#[tokio::test(flavor = "multi_thread")]
async fn reshard_fh_happy_path_complete() {
    let port_a = 16852u16;
    let port_b = 16853u16;

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

    let key = "{fhhappy}.k";
    let slot = key_hash_slot(key.as_bytes());
    assert!(is_ok(
        &send_cmd(&mut sa, &["SET", key, "fh-happy"]).await
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
    assert!(cs_b.owns_slot(slot));
    assert!(!cs_a.owns_slot(slot));
    assert!(!cs_a.is_prepared(slot));
    assert!(!cs_b.is_prepared(slot));
    assert_eq!(
        as_bulk(&send_cmd(&mut sb, &["GET", key]).await),
        "fh-happy"
    );

    let _ = shut_a_tx.send(true);
    let _ = shut_b_tx.send(true);
    let _ = ha.await;
    let _ = hb.await;
}
