//! Coordinated FAILOVER TO tests (master-initiated promote of a replica).

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::persistence::replication::run_replica_loop;
use kore::persistence::{PersistenceConfig, PersistenceManager, SaveRule};
use kore::protocol::{RespParser, RespValue};
use kore::Cache;
use kore::Server;
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
    let dir = std::env::temp_dir().join(format!("kore-failover-{}-{}", label, nanos));
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

fn make_config(dir: &PathBuf, port: u16) -> Arc<Config> {
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
        dir: dir.to_string_lossy().to_string(),
        dbfilename: "dump.rdb".to_string(),
        appendonly: false,
        appendfilename: "appendonly.aof".to_string(),
        replicaof: String::new(),
        save: "900,1".to_string(),
        maxmemory_policy: "allkeys-lru".to_string(),
        databases: 16,
        metrics_port: 0,
        tls: false,
        tls_cert: String::new(),
        tls_key: String::new(),
        cluster_enabled: false,
    })
}

fn bulk(s: &str) -> RespValue {
    RespValue::BulkString(Some(Bytes::from(s.to_string())))
}

fn cmd(parts: &[&str]) -> RespValue {
    RespValue::Array(parts.iter().map(|p| bulk(p)).collect())
}

fn handle(h: &mut CommandHandler, value: RespValue) -> RespValue {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { h.handle(value).await.unwrap() })
}

fn encode_cmd(parts: &[&str]) -> Vec<u8> {
    cmd(parts).serialize().to_vec()
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

fn assert_err_contains(resp: RespValue, needles: &[&str]) {
    match resp {
        RespValue::Error(e) => {
            let msg = String::from_utf8_lossy(&e);
            for n in needles {
                assert!(
                    msg.to_ascii_lowercase().contains(&n.to_ascii_lowercase()),
                    "expected error containing {:?}, got {}",
                    n,
                    msg
                );
            }
        }
        other => panic!("expected error, got {:?}", other),
    }
}

#[test]
fn failover_to_on_replica_errors() {
    let dir = unique_dir("to-on-replica");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir, 6379), Some(mgr.clone()));

    handle(&mut h, cmd(&["REPLICAOF", "127.0.0.1", "6399"]));
    assert!(mgr.replication.is_replica());

    let resp = handle(&mut h, cmd(&["FAILOVER", "TO", "127.0.0.1", "16612"]));
    assert_err_contains(resp, &["FAILOVER", "master"]);

    // Still a replica (did not promote via FAILOVER TO)
    assert!(mgr.replication.is_replica());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn failover_to_syntax() {
    let dir = unique_dir("to-syntax");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir, 6379), Some(mgr));

    // Missing port
    assert_err_contains(
        handle(&mut h, cmd(&["FAILOVER", "TO", "127.0.0.1"])),
        &["wrong number", "failover"],
    );
    // Invalid port
    assert_err_contains(
        handle(&mut h, cmd(&["FAILOVER", "TO", "127.0.0.1", "notaport"])),
        &["port"],
    );
    // TIMEOUT without value
    assert_err_contains(
        handle(
            &mut h,
            cmd(&["FAILOVER", "TO", "127.0.0.1", "16613", "TIMEOUT"]),
        ),
        &["timeout"],
    );
    // TIMEOUT non-integer
    assert_err_contains(
        handle(
            &mut h,
            cmd(&["FAILOVER", "TO", "127.0.0.1", "16613", "TIMEOUT", "xyz"]),
        ),
        &["timeout"],
    );
    // Unknown option after TO host port
    assert_err_contains(
        handle(
            &mut h,
            cmd(&["FAILOVER", "TO", "127.0.0.1", "16613", "FORCE"]),
        ),
        &["syntax"],
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn failover_bare_still_promotes_replica() {
    let dir = unique_dir("bare-promote");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir, 6379), Some(mgr.clone()));

    handle(&mut h, cmd(&["REPLICAOF", "127.0.0.1", "6399"]));
    let resp = handle(&mut h, cmd(&["FAILOVER"]));
    assert_eq!(resp, RespValue::ok());
    assert!(!mgr.replication.is_replica());
    assert!(!mgr.replication.readonly());

    let resp = handle(&mut h, cmd(&["ROLE"]));
    match resp {
        RespValue::Array(arr) => {
            assert_eq!(
                arr[0],
                RespValue::BulkString(Some(Bytes::from_static(b"master")))
            );
        }
        other => panic!("expected master role, {:?}", other),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn failover_to_no_matching_replica() {
    // Master with no connected/tracked replica for the target: connect fails → clear ERR.
    // Uses a closed high port in the reserved test range.
    let dir = unique_dir("no-match");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir, 6379), Some(mgr.clone()));

    // Short timeout so unit test is fast; target not listening.
    let resp = handle(
        &mut h,
        cmd(&["FAILOVER", "TO", "127.0.0.1", "16614", "TIMEOUT", "200"]),
    );
    assert_err_contains(resp, &["FAILOVER"]);
    assert!(!mgr.replication.is_replica(), "master must remain master");

    let _ = std::fs::remove_dir_all(&dir);
}

/// Coordinated FAILOVER TO: master demotes, replica promotes, data preserved.
#[tokio::test(flavor = "multi_thread")]
async fn coordinated_failover_to_promotes_replica() {
    let master_port = 16610u16;
    let replica_port = 16611u16;

    let master_dir = unique_dir("coord-master");
    let replica_dir = unique_dir("coord-replica");

    let master_cfg = make_config(&master_dir, master_port);
    let mut replica_cfg = (*make_config(&replica_dir, replica_port)).clone();
    replica_cfg.replicaof = format!("127.0.0.1:{}", master_port);
    let replica_cfg = Arc::new(replica_cfg);

    let master_mgr = make_persistence(&master_dir);
    master_mgr.replication.set_announce_port(master_port);
    let replica_mgr = make_persistence(&replica_dir);
    replica_mgr.replication.set_announce_port(replica_port);
    replica_mgr
        .replication
        .set_replicaof(Some(format!("127.0.0.1:{}", master_port)));

    let master_cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let replica_cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);

    let master = Server::with_persistence(
        Arc::clone(&master_cache),
        Arc::clone(&master_cfg),
        Arc::clone(&master_mgr),
    );
    let replica = Server::with_persistence(
        Arc::clone(&replica_cache),
        Arc::clone(&replica_cfg),
        Arc::clone(&replica_mgr),
    );

    let (m_shutdown_tx, m_shutdown_rx) = watch::channel(false);
    let (r_shutdown_tx, r_shutdown_rx) = watch::channel(false);

    let master_handle = tokio::spawn(async move {
        let _ = master.run_with_shutdown(m_shutdown_rx).await;
    });
    let replica_handle = tokio::spawn(async move {
        let _ = replica.run_with_shutdown(r_shutdown_rx).await;
    });

    // Replica apply loop (Server itself does not start it)
    let repl_dbs = kore::Databases::single(Arc::clone(&replica_cache));
    let repl_mgr_loop = Arc::clone(&replica_mgr);
    let repl_shutdown = r_shutdown_tx.subscribe();
    let replica_loop = tokio::spawn(async move {
        run_replica_loop(repl_dbs, repl_mgr_loop.replication.clone(), repl_shutdown).await;
    });

    sleep(Duration::from_millis(300)).await;

    // Seed data on master
    let mut master_cli = timeout(
        Duration::from_secs(2),
        TcpStream::connect(format!("127.0.0.1:{}", master_port)),
    )
    .await
    .expect("master connect timeout")
    .expect("master connect");
    let set_resp = send_cmd(&mut master_cli, &["SET", "failover_key", "hello"]).await;
    assert_eq!(set_resp, RespValue::ok());

    // Wait for replica link + data
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if replica_mgr.replication.master_link_up() {
            let mut rcli = TcpStream::connect(format!("127.0.0.1:{}", replica_port))
                .await
                .expect("replica connect");
            let got = send_cmd(&mut rcli, &["GET", "failover_key"]).await;
            if got
                == RespValue::BulkString(Some(Bytes::from_static(b"hello")))
            {
                break;
            }
        }
        if tokio::time::Instant::now() > deadline {
            panic!("replica did not sync failover_key in time");
        }
        sleep(Duration::from_millis(50)).await;
    }

    // Coordinated failover from master
    let fo = send_cmd(
        &mut master_cli,
        &[
            "FAILOVER",
            "TO",
            "127.0.0.1",
            &replica_port.to_string(),
            "TIMEOUT",
            "3000",
        ],
    )
    .await;
    assert_eq!(fo, RespValue::ok(), "FAILOVER TO should succeed");

    // New master (former replica) ROLE
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let mut rcli = TcpStream::connect(format!("127.0.0.1:{}", replica_port))
            .await
            .expect("replica connect");
        let role = send_cmd(&mut rcli, &["ROLE"]).await;
        match &role {
            RespValue::Array(arr)
                if arr.first()
                    == Some(&RespValue::BulkString(Some(Bytes::from_static(b"master")))) =>
            {
                let get = send_cmd(&mut rcli, &["GET", "failover_key"]).await;
                assert_eq!(
                    get,
                    RespValue::BulkString(Some(Bytes::from_static(b"hello"))),
                    "data must remain readable on new master"
                );
                break;
            }
            _ => {}
        }
        if tokio::time::Instant::now() > deadline {
            panic!("former replica did not become master; last ROLE={:?}", role);
        }
        sleep(Duration::from_millis(50)).await;
    }

    // Old master demoted
    assert!(
        master_mgr.replication.is_replica(),
        "old master should be demoted to replica"
    );

    let _ = m_shutdown_tx.send(true);
    let _ = r_shutdown_tx.send(true);
    master_handle.abort();
    replica_handle.abort();
    replica_loop.abort();
    sleep(Duration::from_millis(50)).await;
    let _ = std::fs::remove_dir_all(&master_dir);
    let _ = std::fs::remove_dir_all(&replica_dir);
}

/// Unreachable target: timeout, master remains master.
#[tokio::test(flavor = "multi_thread")]
async fn coordinated_failover_timeout() {
    let master_port = 16615u16;
    let dir = unique_dir("coord-timeout");
    let cfg = make_config(&dir, master_port);
    let mgr = make_persistence(&dir);
    mgr.replication.set_announce_port(master_port);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);

    let server = Server::with_persistence(Arc::clone(&cache), Arc::clone(&cfg), Arc::clone(&mgr));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle = tokio::spawn(async move {
        let _ = server.run_with_shutdown(shutdown_rx).await;
    });
    sleep(Duration::from_millis(250)).await;

    let mut cli = timeout(
        Duration::from_secs(2),
        TcpStream::connect(format!("127.0.0.1:{}", master_port)),
    )
    .await
    .expect("connect timeout")
    .expect("connect");

    // 16616 is unused in this test — nothing listening
    let start = std::time::Instant::now();
    let resp = send_cmd(
        &mut cli,
        &[
            "FAILOVER",
            "TO",
            "127.0.0.1",
            "16616",
            "TIMEOUT",
            "300",
        ],
    )
    .await;
    let elapsed = start.elapsed();
    assert_err_contains(resp, &["FAILOVER"]);
    assert!(
        elapsed < Duration::from_secs(3),
        "should fail near timeout, took {:?}",
        elapsed
    );
    assert!(!mgr.replication.is_replica(), "must remain master");

    let role = send_cmd(&mut cli, &["ROLE"]).await;
    match role {
        RespValue::Array(arr) => {
            assert_eq!(
                arr[0],
                RespValue::BulkString(Some(Bytes::from_static(b"master")))
            );
        }
        other => panic!("expected master ROLE, {:?}", other),
    }

    let _ = shutdown_tx.send(true);
    handle.abort();
    sleep(Duration::from_millis(50)).await;
    let _ = std::fs::remove_dir_all(&dir);
}
