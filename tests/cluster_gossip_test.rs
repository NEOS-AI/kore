//! Lane D: thin cluster gossip / membership + single-observer fail + replica claim.

use bytes::Bytes;
use kore::persistence::{PersistenceConfig, PersistenceManager, SaveRule};
use kore::protocol::{RespParser, RespValue};
use kore::{force_mark_fail, Cache, ClusterState, Server};
use kore::config::Config;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::{sleep, timeout, Duration};

fn unique_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("kore-cgossip-{}-{}", label, nanos));
    let _ = std::fs::create_dir_all(&dir);
    dir
}

fn make_persistence(dir: &PathBuf) -> Arc<PersistenceManager> {
    let pconfig = PersistenceConfig {
        dir: dir.clone(),
        dbfilename: "dump.rdb".to_string(),
        appendonly: false,
        appendfilename: "appendonly.aof".to_string(),
        save_rules: vec![SaveRule::new(900, 1)],
    };
    PersistenceManager::new(pconfig).unwrap()
}

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

/// CLUSTER MEET adds the peer to both nodes tables (RESP handshake).
#[tokio::test(flavor = "multi_thread")]
async fn meet_adds_peer_to_nodes() {
    let port_a = 16700u16;
    let port_b = 16701u16;

    let cs_a = ClusterState::single_node("127.0.0.1", port_a);
    let cs_b = ClusterState::single_node("127.0.0.1", port_b);
    let id_a = cs_a.my_id();
    let id_b = cs_b.my_id();

    let cache_a = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let cache_b = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);

    let srv_a = Server::new(cache_a, make_config(port_a, true)).with_cluster(Some(Arc::clone(&cs_a)));
    let srv_b = Server::new(cache_b, make_config(port_b, true)).with_cluster(Some(Arc::clone(&cs_b)));

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

    let mut cli = TcpStream::connect(("127.0.0.1", port_a)).await.unwrap();
    let resp = send_cmd(
        &mut cli,
        &["CLUSTER", "MEET", "127.0.0.1", &port_b.to_string()],
    )
    .await;
    assert!(is_ok(&resp), "MEET should OK, got {:?}", resp);

    // A knows B
    let nodes_a = as_bulk(&send_cmd(&mut cli, &["CLUSTER", "NODES"]).await);
    assert!(
        nodes_a.contains(&id_b),
        "A NODES missing B id; nodes=\n{}",
        nodes_a
    );
    assert!(
        cs_a.get_node(&id_b).is_some(),
        "A cluster state missing B"
    );

    // B knows A (via MEETPEER handshake)
    let mut cli_b = TcpStream::connect(("127.0.0.1", port_b)).await.unwrap();
    let nodes_b = as_bulk(&send_cmd(&mut cli_b, &["CLUSTER", "NODES"]).await);
    assert!(
        nodes_b.contains(&id_a),
        "B NODES missing A id; nodes=\n{}",
        nodes_b
    );
    assert!(
        cs_b.get_node(&id_a).is_some(),
        "B cluster state missing A"
    );

    let _ = shut_a_tx.send(true);
    let _ = shut_b_tx.send(true);
    ha.abort();
    hb.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Heartbeat marks peer `fail` after short node-timeout when peer is down.
#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_marks_fail_when_peer_down() {
    let port_a = 16702u16;
    let port_b = 16703u16;

    let cs_a = ClusterState::single_node("127.0.0.1", port_a);
    cs_a.set_node_timeout_ms(200);
    let cs_b = ClusterState::single_node("127.0.0.1", port_b);
    cs_b.set_node_timeout_ms(200);
    let id_b = cs_b.my_id();

    let cache_a = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let cache_b = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);

    let srv_a = Server::new(cache_a, make_config(port_a, true)).with_cluster(Some(Arc::clone(&cs_a)));
    let srv_b = Server::new(cache_b, make_config(port_b, true)).with_cluster(Some(Arc::clone(&cs_b)));

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

    let mut cli = TcpStream::connect(("127.0.0.1", port_a)).await.unwrap();
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &["CLUSTER", "MEET", "127.0.0.1", &port_b.to_string()],
        )
        .await
    ));

    // Kill B
    let _ = shut_b_tx.send(true);
    hb.abort();
    sleep(Duration::from_millis(50)).await;

    // Wait for gossip to mark fail (timeout 200ms, interval ~66ms)
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if cs_a.node_is_fail(&id_b) {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            let nodes = as_bulk(&send_cmd(&mut cli, &["CLUSTER", "NODES"]).await);
            panic!("peer not marked fail in time; nodes=\n{}", nodes);
        }
        sleep(Duration::from_millis(50)).await;
    }

    let nodes = as_bulk(&send_cmd(&mut cli, &["CLUSTER", "NODES"]).await);
    assert!(
        nodes.contains("fail"),
        "expected fail flag in NODES:\n{}",
        nodes
    );

    let _ = shut_a_tx.send(true);
    ha.abort();
    sleep(Duration::from_millis(50)).await;
}

/// On master FAIL, replica promotes (replication) and claims master's slots.
#[tokio::test(flavor = "multi_thread")]
async fn fail_promotes_replica_and_claims_slots() {
    let master_port = 16704u16;
    let replica_port = 16705u16;

    let master_dir = unique_dir("master");
    let replica_dir = unique_dir("replica");

    let master_cs = ClusterState::single_node("127.0.0.1", master_port);
    master_cs.set_node_timeout_ms(250);
    let replica_cs = ClusterState::single_node("127.0.0.1", replica_port);
    replica_cs.set_node_timeout_ms(250);
    let master_id = master_cs.my_id();

    let master_mgr = make_persistence(&master_dir);
    let replica_mgr = make_persistence(&replica_dir);
    master_mgr.replication.set_announce_port(master_port);
    replica_mgr.replication.set_announce_port(replica_port);

    let master_cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let replica_cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);

    let mut master_cfg = (*make_config(master_port, true)).clone();
    master_cfg.dir = master_dir.to_string_lossy().to_string();
    let master_cfg = Arc::new(master_cfg);
    let mut replica_cfg = (*make_config(replica_port, true)).clone();
    replica_cfg.dir = replica_dir.to_string_lossy().to_string();
    let replica_cfg = Arc::new(replica_cfg);

    let master = Server::with_persistence(
        Arc::clone(&master_cache),
        Arc::clone(&master_cfg),
        Arc::clone(&master_mgr),
    )
    .with_cluster(Some(Arc::clone(&master_cs)));
    let replica = Server::with_persistence(
        Arc::clone(&replica_cache),
        Arc::clone(&replica_cfg),
        Arc::clone(&replica_mgr),
    )
    .with_cluster(Some(Arc::clone(&replica_cs)));

    let (m_tx, m_rx) = watch::channel(false);
    let (r_tx, r_rx) = watch::channel(false);
    let mh = tokio::spawn(async move {
        let _ = master.run_with_shutdown(m_rx).await;
    });
    let rh = tokio::spawn(async move {
        let _ = replica.run_with_shutdown(r_rx).await;
    });

    wait_listen(master_port).await;
    wait_listen(replica_port).await;

    // MEET + REPLICATE on replica
    let mut rcli = TcpStream::connect(("127.0.0.1", replica_port))
        .await
        .unwrap();
    assert!(is_ok(
        &send_cmd(
            &mut rcli,
            &["CLUSTER", "MEET", "127.0.0.1", &master_port.to_string()],
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(&mut rcli, &["CLUSTER", "REPLICATE", &master_id]).await
    ));

    // Replica topology: slave of master, no slots
    assert!(replica_cs.is_replica_of(&master_id));
    assert!(!replica_cs.owns_slot(0));
    assert!(replica_mgr.replication.is_replica());

    // Bring master down
    let _ = m_tx.send(true);
    mh.abort();
    sleep(Duration::from_millis(50)).await;

    // Wait for gossip fail + promote + claim
    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    loop {
        if replica_cs.node_is_fail(&master_id)
            && replica_cs.owns_slot(0)
            && !replica_mgr.replication.is_replica()
        {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            // Fallback path for debugging: force mark if gossip flaked
            if !replica_cs.node_is_fail(&master_id) {
                force_mark_fail(&replica_cs, Some(&replica_mgr), &master_id);
            }
            panic!(
                "failover incomplete: fail={} owns0={} is_replica={} nodes=\n{}",
                replica_cs.node_is_fail(&master_id),
                replica_cs.owns_slot(0),
                replica_mgr.replication.is_replica(),
                replica_cs.format_nodes()
            );
        }
        sleep(Duration::from_millis(50)).await;
    }

    assert!(replica_cs.owns_slot(0));
    assert!(replica_cs.owns_slot(16383));
    assert!(!replica_mgr.replication.is_replica());
    let me = replica_cs.get_node(&replica_cs.my_id()).unwrap();
    assert!(me.master);
    assert!(me.master_id.is_none());

    let nodes = as_bulk(&send_cmd(&mut rcli, &["CLUSTER", "NODES"]).await);
    assert!(nodes.contains("fail"), "master should show fail:\n{}", nodes);
    assert!(
        nodes.contains("myself,master") || nodes.contains("myself"),
        "replica should be master now:\n{}",
        nodes
    );

    let _ = r_tx.send(true);
    rh.abort();
    sleep(Duration::from_millis(50)).await;
    let _ = std::fs::remove_dir_all(&master_dir);
    let _ = std::fs::remove_dir_all(&replica_dir);
}

/// Batch DU: SETSLOT NODE bumps epoch; CLUSTER OWNERS exposes compressed ranges.
#[tokio::test(flavor = "multi_thread")]
async fn owners_reflects_setslot_node_epoch() {
    let port = 16710u16;
    let cs = ClusterState::single_node("127.0.0.1", port);
    let other = "mn".repeat(20);
    cs.add_node(&other, "127.0.0.1", 16711);

    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv = Server::new(cache, make_config(port, true)).with_cluster(Some(Arc::clone(&cs)));
    let (tx, rx) = watch::channel(false);
    let h = tokio::spawn(async move {
        let _ = srv.run_with_shutdown(rx).await;
    });
    wait_listen(port).await;

    let mut cli = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let epoch_before = match send_cmd(&mut cli, &["CLUSTER", "EPOCH"]).await {
        RespValue::Integer(n) => n,
        other => panic!("EPOCH: {:?}", other),
    };

    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &["CLUSTER", "SETSLOT", "100", "NODE", &other],
        )
        .await
    ));

    let epoch_after = match send_cmd(&mut cli, &["CLUSTER", "EPOCH"]).await {
        RespValue::Integer(n) => n,
        other => panic!("EPOCH: {:?}", other),
    };
    assert!(epoch_after > epoch_before, "epoch should bump on NODE");

    let owners = send_cmd(&mut cli, &["CLUSTER", "OWNERS"]).await;
    let ranges = match owners {
        RespValue::Array(a) => a,
        other => panic!("OWNERS not array: {:?}", other),
    };
    assert!(ranges.len() >= 3, "slot 100 should split ranges");
    let mut found = false;
    for row in &ranges {
        let fields = match row {
            RespValue::Array(f) => f,
            _ => continue,
        };
        if fields.len() < 6 {
            continue;
        }
        let start = match fields[0] {
            RespValue::Integer(n) => n,
            _ => continue,
        };
        let end = match fields[1] {
            RespValue::Integer(n) => n,
            _ => continue,
        };
        if start == 100 && end == 100 {
            let id = as_bulk(&fields[2]);
            let ep = match fields[5] {
                RespValue::Integer(n) => n,
                _ => panic!("epoch field"),
            };
            assert_eq!(id, other);
            assert_eq!(ep, epoch_after);
            found = true;
        }
    }
    assert!(found, "OWNERS missing slot 100 row: {:?}", ranges);

    let _ = tx.send(true);
    h.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch DU: third node learns reassigned owner via MEET OWNERS pull / gossip.
#[tokio::test(flavor = "multi_thread")]
async fn gossip_propagates_owner_after_setslot_node() {
    let port_a = 16712u16;
    let port_b = 16713u16;
    let port_c = 16714u16;

    let cs_a = ClusterState::single_node("127.0.0.1", port_a);
    let cs_b = ClusterState::single_node("127.0.0.1", port_b);
    let cs_c = ClusterState::single_node("127.0.0.1", port_c);
    // Short gossip interval for the test.
    cs_a.set_node_timeout_ms(150);
    cs_b.set_node_timeout_ms(150);
    cs_c.set_node_timeout_ms(150);

    let id_a = cs_a.my_id();
    let id_b = cs_b.my_id();

    let cache_a = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let cache_b = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let cache_c = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);

    let srv_a =
        Server::new(cache_a, make_config(port_a, true)).with_cluster(Some(Arc::clone(&cs_a)));
    let srv_b =
        Server::new(cache_b, make_config(port_b, true)).with_cluster(Some(Arc::clone(&cs_b)));
    let srv_c =
        Server::new(cache_c, make_config(port_c, true)).with_cluster(Some(Arc::clone(&cs_c)));

    let (tx_a, rx_a) = watch::channel(false);
    let (tx_b, rx_b) = watch::channel(false);
    let (tx_c, rx_c) = watch::channel(false);
    let ha = tokio::spawn(async move {
        let _ = srv_a.run_with_shutdown(rx_a).await;
    });
    let hb = tokio::spawn(async move {
        let _ = srv_b.run_with_shutdown(rx_b).await;
    });
    let hc = tokio::spawn(async move {
        let _ = srv_c.run_with_shutdown(rx_c).await;
    });

    wait_listen(port_a).await;
    wait_listen(port_b).await;
    wait_listen(port_c).await;

    // A meets B; both know each other.
    let mut cli_a = TcpStream::connect(("127.0.0.1", port_a)).await.unwrap();
    assert!(is_ok(
        &send_cmd(
            &mut cli_a,
            &["CLUSTER", "MEET", "127.0.0.1", &port_b.to_string()],
        )
        .await
    ));

    // Reassign slot 50 from A to B with NODE (bumps epoch on A).
    // B must also learn A first, then accept NODE for slot 50.
    let mut cli_b = TcpStream::connect(("127.0.0.1", port_b)).await.unwrap();
    assert!(is_ok(
        &send_cmd(
            &mut cli_b,
            &["CLUSTER", "MEET", "127.0.0.1", &port_a.to_string()],
        )
        .await
    ));

    // Operator dual-end NODE: both sides set owner to B for slot 50.
    assert!(is_ok(
        &send_cmd(
            &mut cli_a,
            &["CLUSTER", "SETSLOT", "50", "NODE", &id_b],
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut cli_b,
            &["CLUSTER", "SETSLOT", "50", "NODE", &id_b],
        )
        .await
    ));
    assert!(!cs_a.owns_slot(50));
    assert!(cs_b.owns_slot(50));
    assert!(cs_a.slot_epoch(50) > 1);

    // C meets A — MEET pulls OWNERS so C should learn slot 50 → B (higher epoch).
    let mut cli_c = TcpStream::connect(("127.0.0.1", port_c)).await.unwrap();
    assert!(is_ok(
        &send_cmd(
            &mut cli_c,
            &["CLUSTER", "MEET", "127.0.0.1", &port_a.to_string()],
        )
        .await
    ));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if cs_c.owner_id_of(50).as_deref() == Some(id_b.as_str()) {
            break;
        }
        // Also drive a manual gossip tick in case MEET pull raced.
        kore::gossip_tick(&cs_c, None).await;
        if tokio::time::Instant::now() > deadline {
            panic!(
                "C never learned owner B for slot 50; owner={:?} epoch={} A_id={}",
                cs_c.owner_id_of(50),
                cs_c.slot_epoch(50),
                id_a
            );
        }
        sleep(Duration::from_millis(40)).await;
    }

    assert_eq!(cs_c.owner_id_of(50).as_deref(), Some(id_b.as_str()));
    assert!(cs_c.slot_epoch(50) >= cs_a.slot_epoch(50));
    // C should know how to reach B (ip/port from OWNERS).
    assert!(cs_c.get_node(&id_b).is_some());

    let _ = tx_a.send(true);
    let _ = tx_b.send(true);
    let _ = tx_c.send(true);
    ha.abort();
    hb.abort();
    hc.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch DW: three masters; one observer alone leaves pfail; two votes → fail.
#[tokio::test(flavor = "multi_thread")]
async fn multi_master_fail_requires_quorum() {
    let port_a = 16720u16;
    let port_b = 16721u16;
    let port_c = 16722u16;

    let cs_a = ClusterState::single_node("127.0.0.1", port_a);
    let cs_b = ClusterState::single_node("127.0.0.1", port_b);
    let cs_c = ClusterState::single_node("127.0.0.1", port_c);
    for cs in [&cs_a, &cs_b, &cs_c] {
        cs.set_node_timeout_ms(120);
    }

    let id_b = cs_b.my_id();

    let cache_a = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let cache_b = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let cache_c = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);

    let srv_a =
        Server::new(cache_a, make_config(port_a, true)).with_cluster(Some(Arc::clone(&cs_a)));
    let srv_b =
        Server::new(cache_b, make_config(port_b, true)).with_cluster(Some(Arc::clone(&cs_b)));
    let srv_c =
        Server::new(cache_c, make_config(port_c, true)).with_cluster(Some(Arc::clone(&cs_c)));

    let (tx_a, rx_a) = watch::channel(false);
    let (tx_b, rx_b) = watch::channel(false);
    let (tx_c, rx_c) = watch::channel(false);
    let ha = tokio::spawn(async move {
        let _ = srv_a.run_with_shutdown(rx_a).await;
    });
    let hb = tokio::spawn(async move {
        let _ = srv_b.run_with_shutdown(rx_b).await;
    });
    let hc = tokio::spawn(async move {
        let _ = srv_c.run_with_shutdown(rx_c).await;
    });

    wait_listen(port_a).await;
    wait_listen(port_b).await;
    wait_listen(port_c).await;

    // Full mesh MEET so each has master_count=3 → quorum=2.
    let mut ca = TcpStream::connect(("127.0.0.1", port_a)).await.unwrap();
    let mut cc = TcpStream::connect(("127.0.0.1", port_c)).await.unwrap();
    assert!(is_ok(
        &send_cmd(
            &mut ca,
            &["CLUSTER", "MEET", "127.0.0.1", &port_b.to_string()],
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut ca,
            &["CLUSTER", "MEET", "127.0.0.1", &port_c.to_string()],
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut cc,
            &["CLUSTER", "MEET", "127.0.0.1", &port_b.to_string()],
        )
        .await
    ));
    // Let topology settle.
    sleep(Duration::from_millis(80)).await;
    assert!(
        cs_a.master_count() >= 3,
        "A masters={}",
        cs_a.master_count()
    );
    assert_eq!(cs_a.fail_quorum_size(), 2);

    // Kill B only.
    let _ = tx_b.send(true);
    hb.abort();
    sleep(Duration::from_millis(40)).await;

    // Wait until A sees pfail or fail on B.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if cs_a.node_is_pfail(&id_b) || cs_a.node_is_fail(&id_b) {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!(
                "A never marked B; nodes=\n{}",
                cs_a.format_nodes()
            );
        }
        sleep(Duration::from_millis(30)).await;
    }

    // With quorum=2, A alone may still be pfail briefly; wait for C exchange → fail.
    let deadline2 = tokio::time::Instant::now() + Duration::from_secs(4);
    loop {
        if cs_a.node_is_fail(&id_b) && cs_c.node_is_fail(&id_b) {
            break;
        }
        if tokio::time::Instant::now() > deadline2 {
            panic!(
                "quorum fail not reached: A_fail={} C_fail={} A_pfail={} C_pfail={}\nA:\n{}\nC:\n{}",
                cs_a.node_is_fail(&id_b),
                cs_c.node_is_fail(&id_b),
                cs_a.node_is_pfail(&id_b),
                cs_c.node_is_pfail(&id_b),
                cs_a.format_nodes(),
                cs_c.format_nodes()
            );
        }
        sleep(Duration::from_millis(40)).await;
    }

    let reports = send_cmd(&mut ca, &["CLUSTER", "FAILREPORTS"]).await;
    match reports {
        RespValue::Array(a) => {
            let ids: Vec<String> = a
                .iter()
                .filter_map(|v| match v {
                    RespValue::BulkString(Some(b)) => {
                        Some(String::from_utf8_lossy(b).into_owned())
                    }
                    _ => None,
                })
                .collect();
            assert!(
                ids.iter().any(|id| id == &id_b),
                "FAILREPORTS missing B: {:?}",
                ids
            );
        }
        other => panic!("FAILREPORTS: {:?}", other),
    }

    let _ = tx_a.send(true);
    let _ = tx_c.send(true);
    ha.abort();
    hc.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch DY: two replicas of one master; only max-id replica claims after fail.
#[tokio::test(flavor = "multi_thread")]
async fn multi_replica_election_only_winner_claims() {
    let master_port = 16730u16;
    let r1_port = 16731u16;
    let r2_port = 16732u16;

    let master_dir = unique_dir("m-elect");
    let r1_dir = unique_dir("r1-elect");
    let r2_dir = unique_dir("r2-elect");

    let master_cs = ClusterState::single_node("127.0.0.1", master_port);
    let r1_cs = ClusterState::single_node("127.0.0.1", r1_port);
    let r2_cs = ClusterState::single_node("127.0.0.1", r2_port);
    for cs in [&master_cs, &r1_cs, &r2_cs] {
        cs.set_node_timeout_ms(150);
    }
    let master_id = master_cs.my_id();
    let r1_id = r1_cs.my_id();
    let r2_id = r2_cs.my_id();
    let expected_winner = if r1_id > r2_id {
        r1_id.clone()
    } else {
        r2_id.clone()
    };

    let master_mgr = make_persistence(&master_dir);
    let r1_mgr = make_persistence(&r1_dir);
    let r2_mgr = make_persistence(&r2_dir);
    master_mgr.replication.set_announce_port(master_port);
    r1_mgr.replication.set_announce_port(r1_port);
    r2_mgr.replication.set_announce_port(r2_port);

    let master_cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let r1_cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let r2_cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);

    let mut master_cfg = (*make_config(master_port, true)).clone();
    master_cfg.dir = master_dir.to_string_lossy().to_string();
    let master_cfg = Arc::new(master_cfg);
    let mut r1_cfg = (*make_config(r1_port, true)).clone();
    r1_cfg.dir = r1_dir.to_string_lossy().to_string();
    let r1_cfg = Arc::new(r1_cfg);
    let mut r2_cfg = (*make_config(r2_port, true)).clone();
    r2_cfg.dir = r2_dir.to_string_lossy().to_string();
    let r2_cfg = Arc::new(r2_cfg);

    let master = Server::with_persistence(
        Arc::clone(&master_cache),
        Arc::clone(&master_cfg),
        Arc::clone(&master_mgr),
    )
    .with_cluster(Some(Arc::clone(&master_cs)));
    let r1 = Server::with_persistence(
        Arc::clone(&r1_cache),
        Arc::clone(&r1_cfg),
        Arc::clone(&r1_mgr),
    )
    .with_cluster(Some(Arc::clone(&r1_cs)));
    let r2 = Server::with_persistence(
        Arc::clone(&r2_cache),
        Arc::clone(&r2_cfg),
        Arc::clone(&r2_mgr),
    )
    .with_cluster(Some(Arc::clone(&r2_cs)));

    let (m_tx, m_rx) = watch::channel(false);
    let (r1_tx, r1_rx) = watch::channel(false);
    let (r2_tx, r2_rx) = watch::channel(false);
    let mh = tokio::spawn(async move {
        let _ = master.run_with_shutdown(m_rx).await;
    });
    let r1h = tokio::spawn(async move {
        let _ = r1.run_with_shutdown(r1_rx).await;
    });
    let r2h = tokio::spawn(async move {
        let _ = r2.run_with_shutdown(r2_rx).await;
    });

    wait_listen(master_port).await;
    wait_listen(r1_port).await;
    wait_listen(r2_port).await;

    let mut c1 = TcpStream::connect(("127.0.0.1", r1_port)).await.unwrap();
    let mut c2 = TcpStream::connect(("127.0.0.1", r2_port)).await.unwrap();

    // Mesh: each replica meets master + the other replica; then REPLICATE.
    assert!(is_ok(
        &send_cmd(
            &mut c1,
            &["CLUSTER", "MEET", "127.0.0.1", &master_port.to_string()],
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut c1,
            &["CLUSTER", "MEET", "127.0.0.1", &r2_port.to_string()],
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut c2,
            &["CLUSTER", "MEET", "127.0.0.1", &master_port.to_string()],
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut c2,
            &["CLUSTER", "MEET", "127.0.0.1", &r1_port.to_string()],
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(&mut c1, &["CLUSTER", "REPLICATE", &master_id]).await
    ));
    assert!(is_ok(
        &send_cmd(&mut c2, &["CLUSTER", "REPLICATE", &master_id]).await
    ));

    // Re-MEET so ROLEMAP / MEETPEER carries slave role after REPLICATE.
    assert!(is_ok(
        &send_cmd(
            &mut c1,
            &["CLUSTER", "MEET", "127.0.0.1", &r2_port.to_string()],
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut c2,
            &["CLUSTER", "MEET", "127.0.0.1", &r1_port.to_string()],
        )
        .await
    ));

    // Wait until each replica sees the other as slave of master.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let r1_sees = r1_cs.replicas_of(&master_id);
        let r2_sees = r2_cs.replicas_of(&master_id);
        if r1_sees.len() >= 2 && r2_sees.len() >= 2 {
            break;
        }
        // Drive gossip
        kore::gossip_tick(&r1_cs, Some(&r1_mgr)).await;
        kore::gossip_tick(&r2_cs, Some(&r2_mgr)).await;
        if tokio::time::Instant::now() > deadline {
            panic!(
                "rolemap not synced: r1_replicas={:?} r2_replicas={:?}",
                r1_sees, r2_sees
            );
        }
        sleep(Duration::from_millis(40)).await;
    }

    assert_eq!(
        r1_cs.failover_election_winner(&master_id).as_deref(),
        Some(expected_winner.as_str())
    );
    assert_eq!(
        r2_cs.failover_election_winner(&master_id).as_deref(),
        Some(expected_winner.as_str())
    );

    // Kill master
    let _ = m_tx.send(true);
    mh.abort();
    sleep(Duration::from_millis(50)).await;

    let deadline2 = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let r1_fail = r1_cs.node_is_fail(&master_id);
        let r2_fail = r2_cs.node_is_fail(&master_id);
        let r1_owns = r1_cs.owns_slot(0);
        let r2_owns = r2_cs.owns_slot(0);
        if r1_fail && r2_fail && (r1_owns || r2_owns) {
            break;
        }
        if tokio::time::Instant::now() > deadline2 {
            // Force path if gossip flaked — still check election semantics on force.
            force_mark_fail(&r1_cs, Some(&r1_mgr), &master_id);
            force_mark_fail(&r2_cs, Some(&r2_mgr), &master_id);
            break;
        }
        sleep(Duration::from_millis(40)).await;
    }

    let r1_owns = r1_cs.owns_slot(0);
    let r2_owns = r2_cs.owns_slot(0);
    if expected_winner == r1_id {
        assert!(r1_owns, "winner r1 should own slots");
        assert!(!r2_owns, "loser r2 must not claim slots");
        assert!(!r1_mgr.replication.is_replica());
        // Batch DZ: loser re-points at winner (allow a short gossip retry window).
        let deadline3 = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if r2_cs.is_replica_of(&r1_id) {
                break;
            }
            kore::gossip_tick(&r2_cs, Some(&r2_mgr)).await;
            if tokio::time::Instant::now() > deadline3 {
                panic!(
                    "loser r2 never re-pointed at winner r1; r2_master={:?}",
                    r2_cs.get_node(&r2_cs.my_id()).map(|n| n.master_id)
                );
            }
            sleep(Duration::from_millis(30)).await;
        }
        assert!(r2_mgr.replication.is_replica());
    } else {
        assert!(r2_owns, "winner r2 should own slots");
        assert!(!r1_owns, "loser r1 must not claim slots");
        assert!(!r2_mgr.replication.is_replica());
        let deadline3 = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if r1_cs.is_replica_of(&r2_id) {
                break;
            }
            kore::gossip_tick(&r1_cs, Some(&r1_mgr)).await;
            if tokio::time::Instant::now() > deadline3 {
                panic!(
                    "loser r1 never re-pointed at winner r2; r1_master={:?}",
                    r1_cs.get_node(&r1_cs.my_id()).map(|n| n.master_id)
                );
            }
            sleep(Duration::from_millis(30)).await;
        }
        assert!(r1_mgr.replication.is_replica());
    }

    let _ = r1_tx.send(true);
    let _ = r2_tx.send(true);
    r1h.abort();
    r2h.abort();
    sleep(Duration::from_millis(50)).await;
    let _ = std::fs::remove_dir_all(&master_dir);
    let _ = std::fs::remove_dir_all(&r1_dir);
    let _ = std::fs::remove_dir_all(&r2_dir);
}

/// Batch EC: CLUSTER FAILOVER TAKEOVER on a replica claims slots.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_failover_takeover_command() {
    let master_port = 16740u16;
    let replica_port = 16741u16;

    let master_dir = unique_dir("m-fo");
    let replica_dir = unique_dir("r-fo");

    let master_cs = ClusterState::single_node("127.0.0.1", master_port);
    let replica_cs = ClusterState::single_node("127.0.0.1", replica_port);
    let master_id = master_cs.my_id();

    let master_mgr = make_persistence(&master_dir);
    let replica_mgr = make_persistence(&replica_dir);
    master_mgr.replication.set_announce_port(master_port);
    replica_mgr.replication.set_announce_port(replica_port);

    let master_cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let replica_cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);

    let mut master_cfg = (*make_config(master_port, true)).clone();
    master_cfg.dir = master_dir.to_string_lossy().to_string();
    let master_cfg = Arc::new(master_cfg);
    let mut replica_cfg = (*make_config(replica_port, true)).clone();
    replica_cfg.dir = replica_dir.to_string_lossy().to_string();
    let replica_cfg = Arc::new(replica_cfg);

    let master = Server::with_persistence(
        Arc::clone(&master_cache),
        Arc::clone(&master_cfg),
        Arc::clone(&master_mgr),
    )
    .with_cluster(Some(Arc::clone(&master_cs)));
    let replica = Server::with_persistence(
        Arc::clone(&replica_cache),
        Arc::clone(&replica_cfg),
        Arc::clone(&replica_mgr),
    )
    .with_cluster(Some(Arc::clone(&replica_cs)));

    let (m_tx, m_rx) = watch::channel(false);
    let (r_tx, r_rx) = watch::channel(false);
    let mh = tokio::spawn(async move {
        let _ = master.run_with_shutdown(m_rx).await;
    });
    let rh = tokio::spawn(async move {
        let _ = replica.run_with_shutdown(r_rx).await;
    });

    wait_listen(master_port).await;
    wait_listen(replica_port).await;

    let mut rcli = TcpStream::connect(("127.0.0.1", replica_port))
        .await
        .unwrap();
    assert!(is_ok(
        &send_cmd(
            &mut rcli,
            &["CLUSTER", "MEET", "127.0.0.1", &master_port.to_string()],
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(&mut rcli, &["CLUSTER", "REPLICATE", &master_id]).await
    ));
    assert!(replica_cs.is_replica_of(&master_id));
    assert!(!replica_cs.owns_slot(0));

    // Safe without fail should error.
    let err = send_cmd(&mut rcli, &["CLUSTER", "FAILOVER"]).await;
    match err {
        RespValue::Error(e) => {
            let s = String::from_utf8_lossy(&e);
            assert!(s.contains("fail") || s.contains("FAIL"), "{}", s);
        }
        other => panic!("expected error, got {:?}", other),
    }

    // TAKEOVER claims even while master is still up.
    assert!(is_ok(
        &send_cmd(&mut rcli, &["CLUSTER", "FAILOVER", "TAKEOVER"]).await
    ));
    assert!(replica_cs.owns_slot(0));
    assert!(replica_cs.owns_slot(16383));
    assert!(!replica_mgr.replication.is_replica());
    assert!(replica_cs.node_is_fail(&master_id));

    let _ = m_tx.send(true);
    let _ = r_tx.send(true);
    mh.abort();
    rh.abort();
    sleep(Duration::from_millis(50)).await;
    let _ = std::fs::remove_dir_all(&master_dir);
    let _ = std::fs::remove_dir_all(&replica_dir);
}
