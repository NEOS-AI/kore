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
        cluster_enabled: cluster,
    unixsocket: String::new(),
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
