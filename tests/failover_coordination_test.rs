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
        enable_fair_queue: false,
        fair_queue_max_size: 1024,
        fair_queue_cleanup_ms: 500,
        dir: dir.to_string_lossy().to_string(),
        dbfilename: "dump.rdb".to_string(),
        appendonly: false,
        appendfilename: "appendonly.aof".to_string(),
        replicaof: String::new(),
        save: "900,1".to_string(),
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
    unixsocket: String::new(),
            log_format: "text".to_string(),
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

async fn wait_listen(port: u16) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match TcpStream::connect(format!("127.0.0.1:{}", port)).await {
            Ok(_) => return,
            Err(_) if tokio::time::Instant::now() < deadline => {
                sleep(Duration::from_millis(20)).await;
            }
            Err(e) => panic!("server on port {} never became ready: {}", port, e),
        }
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
    // FORCE is a valid option (skips catch-up); without a live target it fails on connect.
    assert_err_contains(
        handle(
            &mut h,
            cmd(&[
                "FAILOVER",
                "TO",
                "127.0.0.1",
                "16613",
                "TIMEOUT",
                "100",
                "FORCE",
            ]),
        ),
        &["FAILOVER"],
    );
    // Unknown option after TO host port
    assert_err_contains(
        handle(
            &mut h,
            cmd(&["FAILOVER", "TO", "127.0.0.1", "16613", "NOPE"]),
        ),
        &["syntax"],
    );
    // Bare FORCE without TO
    assert_err_contains(handle(&mut h, cmd(&["FAILOVER", "FORCE"])), &["FORCE"]);

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

/// Target that never reaches master offset: catch-up wait times out; master stays master.
#[tokio::test(flavor = "multi_thread")]
async fn failover_to_catchup_timeout_when_target_lags() {
    // Master has writes (offset > 0). Target is a standalone server with ack 0 forever.
    let master_port = 16617u16;
    let lag_port = 16618u16;

    let master_dir = unique_dir("catchup-master");
    let lag_dir = unique_dir("catchup-lag");

    let master_cfg = make_config(&master_dir, master_port);
    let lag_cfg = make_config(&lag_dir, lag_port);

    let master_mgr = make_persistence(&master_dir);
    master_mgr.replication.set_announce_port(master_port);
    let lag_mgr = make_persistence(&lag_dir);
    lag_mgr.replication.set_announce_port(lag_port);

    let master_cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let lag_cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);

    let master = Server::with_persistence(
        Arc::clone(&master_cache),
        Arc::clone(&master_cfg),
        Arc::clone(&master_mgr),
    );
    let lag = Server::with_persistence(
        Arc::clone(&lag_cache),
        Arc::clone(&lag_cfg),
        Arc::clone(&lag_mgr),
    );

    let (m_tx, m_rx) = watch::channel(false);
    let (l_tx, l_rx) = watch::channel(false);
    let m_h = tokio::spawn(async move {
        let _ = master.run_with_shutdown(m_rx).await;
    });
    let l_h = tokio::spawn(async move {
        let _ = lag.run_with_shutdown(l_rx).await;
    });
    sleep(Duration::from_millis(250)).await;

    let mut cli = timeout(
        Duration::from_secs(2),
        TcpStream::connect(format!("127.0.0.1:{}", master_port)),
    )
    .await
    .expect("connect timeout")
    .expect("connect");

    // Bump master_repl_offset above 0 (no replica applying)
    assert_eq!(
        send_cmd(&mut cli, &["SET", "k", "v"]).await,
        RespValue::ok()
    );
    assert!(
        master_mgr.replication.master_repl_offset() > 0,
        "master offset must be > 0 after write"
    );

    // Soft identity: no replica announced, so connect to lag_port is still attempted.
    // Lag server is not a replica of this master → GETACK stays 0 → catch-up timeout.
    let start = std::time::Instant::now();
    let resp = send_cmd(
        &mut cli,
        &[
            "FAILOVER",
            "TO",
            "127.0.0.1",
            &lag_port.to_string(),
            "TIMEOUT",
            "400",
        ],
    )
    .await;
    let elapsed = start.elapsed();
    assert_err_contains(resp, &["catch-up"]);
    assert!(
        elapsed >= Duration::from_millis(300),
        "should spend budget waiting for catch-up, took {:?}",
        elapsed
    );
    assert!(
        !master_mgr.replication.is_replica(),
        "master must remain master after catch-up timeout"
    );
    // Target must NOT have been promoted (still accepts writes as master)
    let mut lag_cli = TcpStream::connect(format!("127.0.0.1:{}", lag_port))
        .await
        .expect("lag connect");
    let role = send_cmd(&mut lag_cli, &["ROLE"]).await;
    match role {
        RespValue::Array(arr) => {
            assert_eq!(
                arr[0],
                RespValue::BulkString(Some(Bytes::from_static(b"master"))),
                "lag target must not receive FAILOVER when catch-up fails"
            );
        }
        other => panic!("expected ROLE array, {:?}", other),
    }

    let _ = m_tx.send(true);
    let _ = l_tx.send(true);
    m_h.abort();
    l_h.abort();
    sleep(Duration::from_millis(50)).await;
    let _ = std::fs::remove_dir_all(&master_dir);
    let _ = std::fs::remove_dir_all(&lag_dir);
}

/// After many writes, FAILOVER TO waits for catch-up so all keys are on the new master.
#[tokio::test(flavor = "multi_thread")]
async fn failover_to_catchup_preserves_all_writes() {
    let master_port = 16619u16;
    let replica_port = 16620u16;

    let master_dir = unique_dir("catchup-all-master");
    let replica_dir = unique_dir("catchup-all-replica");

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

    let repl_dbs = kore::Databases::single(Arc::clone(&replica_cache));
    let repl_mgr_loop = Arc::clone(&replica_mgr);
    let repl_shutdown = r_shutdown_tx.subscribe();
    let replica_loop = tokio::spawn(async move {
        run_replica_loop(repl_dbs, repl_mgr_loop.replication.clone(), repl_shutdown).await;
    });

    sleep(Duration::from_millis(300)).await;

    let mut master_cli = timeout(
        Duration::from_secs(2),
        TcpStream::connect(format!("127.0.0.1:{}", master_port)),
    )
    .await
    .expect("master connect timeout")
    .expect("master connect");

    // Wait for link, then burst writes
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if replica_mgr.replication.master_link_up() {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("replica link never came up");
        }
        sleep(Duration::from_millis(50)).await;
    }

    const N: usize = 40;
    for i in 0..N {
        let key = format!("ck{}", i);
        let val = format!("v{}", i);
        let r = send_cmd(&mut master_cli, &["SET", &key, &val]).await;
        assert_eq!(r, RespValue::ok());
    }

    let fo = send_cmd(
        &mut master_cli,
        &[
            "FAILOVER",
            "TO",
            "127.0.0.1",
            &replica_port.to_string(),
            "TIMEOUT",
            "5000",
        ],
    )
    .await;
    assert_eq!(fo, RespValue::ok(), "FAILOVER TO should succeed after catch-up");

    // All keys readable on new master
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let mut rcli = TcpStream::connect(format!("127.0.0.1:{}", replica_port))
            .await
            .expect("replica connect");
        let role = send_cmd(&mut rcli, &["ROLE"]).await;
        let is_master = matches!(
            &role,
            RespValue::Array(arr)
                if arr.first()
                    == Some(&RespValue::BulkString(Some(Bytes::from_static(b"master"))))
        );
        if is_master {
            let mut ok = true;
            for i in 0..N {
                let key = format!("ck{}", i);
                let val = format!("v{}", i);
                let got = send_cmd(&mut rcli, &["GET", &key]).await;
                if got != RespValue::BulkString(Some(Bytes::from(val.clone()))) {
                    ok = false;
                    break;
                }
            }
            if ok {
                break;
            }
        }
        if tokio::time::Instant::now() > deadline {
            panic!("not all keys present on new master after catch-up failover");
        }
        sleep(Duration::from_millis(50)).await;
    }

    assert!(master_mgr.replication.is_replica());

    let _ = m_shutdown_tx.send(true);
    let _ = r_shutdown_tx.send(true);
    master_handle.abort();
    replica_handle.abort();
    replica_loop.abort();
    sleep(Duration::from_millis(50)).await;
    let _ = std::fs::remove_dir_all(&master_dir);
    let _ = std::fs::remove_dir_all(&replica_dir);
}

/// FORCE skips catch-up: lagging standalone target is promoted even when offset is 0.
#[tokio::test(flavor = "multi_thread")]
async fn failover_to_force_skips_catchup_and_promotes() {
    let master_port = 16621u16;
    let lag_port = 16622u16;

    let master_dir = unique_dir("force-master");
    let lag_dir = unique_dir("force-lag");

    let master_cfg = make_config(&master_dir, master_port);
    let lag_cfg = make_config(&lag_dir, lag_port);

    let master_mgr = make_persistence(&master_dir);
    master_mgr.replication.set_announce_port(master_port);
    let lag_mgr = make_persistence(&lag_dir);
    lag_mgr.replication.set_announce_port(lag_port);
    // Target is a replica (can accept bare FAILOVER) but not linked — offset 0.
    lag_mgr
        .replication
        .set_replicaof(Some(format!("127.0.0.1:{}", master_port)));

    let master_cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let lag_cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);

    let master = Server::with_persistence(
        Arc::clone(&master_cache),
        Arc::clone(&master_cfg),
        Arc::clone(&master_mgr),
    );
    let lag = Server::with_persistence(
        Arc::clone(&lag_cache),
        Arc::clone(&lag_cfg),
        Arc::clone(&lag_mgr),
    );

    let (m_tx, m_rx) = watch::channel(false);
    let (l_tx, l_rx) = watch::channel(false);
    let m_h = tokio::spawn(async move {
        let _ = master.run_with_shutdown(m_rx).await;
    });
    let l_h = tokio::spawn(async move {
        let _ = lag.run_with_shutdown(l_rx).await;
    });
    sleep(Duration::from_millis(250)).await;

    let mut cli = timeout(
        Duration::from_secs(2),
        TcpStream::connect(format!("127.0.0.1:{}", master_port)),
    )
    .await
    .expect("connect timeout")
    .expect("connect");

    assert_eq!(
        send_cmd(&mut cli, &["SET", "only_on_master", "x"]).await,
        RespValue::ok()
    );
    assert!(master_mgr.replication.master_repl_offset() > 0);

    // Without FORCE this would catch-up-timeout; with FORCE it promotes the lagging target.
    let resp = send_cmd(
        &mut cli,
        &[
            "FAILOVER",
            "TO",
            "127.0.0.1",
            &lag_port.to_string(),
            "TIMEOUT",
            "2000",
            "FORCE",
        ],
    )
    .await;
    assert_eq!(resp, RespValue::ok(), "FORCE FAILOVER TO should succeed");
    assert!(
        master_mgr.replication.is_replica(),
        "old master demoted after FORCE"
    );
    assert!(
        !lag_mgr.replication.is_replica(),
        "lag target promoted despite missing catch-up"
    );

    let mut lag_cli = TcpStream::connect(format!("127.0.0.1:{}", lag_port))
        .await
        .expect("lag connect");
    let role = send_cmd(&mut lag_cli, &["ROLE"]).await;
    match role {
        RespValue::Array(arr) => {
            assert_eq!(
                arr[0],
                RespValue::BulkString(Some(Bytes::from_static(b"master")))
            );
        }
        other => panic!("expected master ROLE on promoted lag, {:?}", other),
    }

    let _ = m_tx.send(true);
    let _ = l_tx.send(true);
    m_h.abort();
    l_h.abort();
    sleep(Duration::from_millis(50)).await;
    let _ = std::fs::remove_dir_all(&master_dir);
    let _ = std::fs::remove_dir_all(&lag_dir);
}

/// Live-link ACK via REPLCONF ACK on a registered feed satisfies catch-up without client GETACK.
#[tokio::test(flavor = "multi_thread")]
async fn wait_catchup_uses_note_replica_ack_from_client_path() {
    // Unit-style integration through the manager: register feed + note ack + wait.
    use kore::persistence::replication::ReplicationManager;
    use tokio::time::Instant;

    let repl = ReplicationManager::new();
    let _feed = repl.register_replica_announced(Some("127.0.0.1".into()), Some(16623));
    repl.note_replica_ack(Some("127.0.0.1"), Some(16623), 1000);
    let deadline = Instant::now() + Duration::from_millis(300);
    repl.wait_replica_offset_catchup("127.0.0.1", 16623, 1000, deadline)
        .await
        .expect("tracked ACK must satisfy catch-up");
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

/// Three-node FAILOVER TO: sibling replica is redirected to the new master.
#[tokio::test(flavor = "multi_thread")]
async fn failover_to_redirects_sibling_replica() {
    // Distinct from other tests in this file (16610–16622) to avoid parallel races.
    let master_port = 16630u16;
    let r1_port = 16631u16; // promote target
    let r2_port = 16632u16; // sibling that must re-follow

    let master_dir = unique_dir("refollow-m");
    let r1_dir = unique_dir("refollow-r1");
    let r2_dir = unique_dir("refollow-r2");

    let master_cfg = make_config(&master_dir, master_port);
    let mut r1_cfg = (*make_config(&r1_dir, r1_port)).clone();
    r1_cfg.replicaof = format!("127.0.0.1:{}", master_port);
    let r1_cfg = Arc::new(r1_cfg);
    let mut r2_cfg = (*make_config(&r2_dir, r2_port)).clone();
    r2_cfg.replicaof = format!("127.0.0.1:{}", master_port);
    let r2_cfg = Arc::new(r2_cfg);

    let master_mgr = make_persistence(&master_dir);
    master_mgr.replication.set_announce_port(master_port);
    let r1_mgr = make_persistence(&r1_dir);
    r1_mgr.replication.set_announce_port(r1_port);
    r1_mgr
        .replication
        .set_replicaof(Some(format!("127.0.0.1:{}", master_port)));
    let r2_mgr = make_persistence(&r2_dir);
    r2_mgr.replication.set_announce_port(r2_port);
    r2_mgr
        .replication
        .set_replicaof(Some(format!("127.0.0.1:{}", master_port)));

    let master_cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let r1_cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let r2_cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);

    let master = Server::with_persistence(
        Arc::clone(&master_cache),
        Arc::clone(&master_cfg),
        Arc::clone(&master_mgr),
    );
    let r1 = Server::with_persistence(
        Arc::clone(&r1_cache),
        Arc::clone(&r1_cfg),
        Arc::clone(&r1_mgr),
    );
    let r2 = Server::with_persistence(
        Arc::clone(&r2_cache),
        Arc::clone(&r2_cfg),
        Arc::clone(&r2_mgr),
    );

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

    let r1_dbs = kore::Databases::single(Arc::clone(&r1_cache));
    let r1_loop = {
        let mgr = Arc::clone(&r1_mgr);
        let sh = r1_tx.subscribe();
        tokio::spawn(async move {
            run_replica_loop(r1_dbs, mgr.replication.clone(), sh).await;
        })
    };
    let r2_dbs = kore::Databases::single(Arc::clone(&r2_cache));
    let r2_loop = {
        let mgr = Arc::clone(&r2_mgr);
        let sh = r2_tx.subscribe();
        tokio::spawn(async move {
            run_replica_loop(r2_dbs, mgr.replication.clone(), sh).await;
        })
    };

    wait_listen(master_port).await;
    wait_listen(r1_port).await;
    wait_listen(r2_port).await;

    // Wait until both replica links are up before writing (avoids racing PSYNC).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if r1_mgr.replication.master_link_up() && r2_mgr.replication.master_link_up() {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!(
                "replicas did not establish master link (r1={} r2={})",
                r1_mgr.replication.master_link_up(),
                r2_mgr.replication.master_link_up()
            );
        }
        sleep(Duration::from_millis(50)).await;
    }

    let mut master_cli = timeout(
        Duration::from_secs(2),
        TcpStream::connect(format!("127.0.0.1:{}", master_port)),
    )
    .await
    .expect("master connect timeout")
    .expect("master connect");
    assert_eq!(
        send_cmd(&mut master_cli, &["SET", "rf_key", "v1"]).await,
        RespValue::ok()
    );
    // Durability: wait for both replicas to ACK the write when possible.
    let _ = send_cmd(&mut master_cli, &["WAIT", "2", "10000"]).await;

    // Wait until both replicas have the key
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let mut ok = 0;
        for port in [r1_port, r2_port] {
            if let Ok(mut c) = TcpStream::connect(format!("127.0.0.1:{}", port)).await {
                let got = send_cmd(&mut c, &["GET", "rf_key"]).await;
                if got == RespValue::BulkString(Some(Bytes::from_static(b"v1"))) {
                    ok += 1;
                }
            }
        }
        if ok == 2 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!(
                "replicas did not sync rf_key (ok={} r1_link={} r2_link={} connected={})",
                ok,
                r1_mgr.replication.master_link_up(),
                r2_mgr.replication.master_link_up(),
                master_mgr.replication.connected_replicas()
            );
        }
        sleep(Duration::from_millis(50)).await;
    }

    let fo = send_cmd(
        &mut master_cli,
        &[
            "FAILOVER",
            "TO",
            "127.0.0.1",
            &r1_port.to_string(),
            "TIMEOUT",
            "5000",
        ],
    )
    .await;
    assert_eq!(fo, RespValue::ok(), "FAILOVER TO should succeed");

    // r1 becomes master
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let mut c = TcpStream::connect(format!("127.0.0.1:{}", r1_port))
            .await
            .expect("r1 connect");
        let role = send_cmd(&mut c, &["ROLE"]).await;
        match &role {
            RespValue::Array(arr)
                if arr.first()
                    == Some(&RespValue::BulkString(Some(Bytes::from_static(b"master")))) =>
            {
                break;
            }
            _ => {}
        }
        if tokio::time::Instant::now() > deadline {
            panic!("r1 did not become master; ROLE={:?}", role);
        }
        sleep(Duration::from_millis(50)).await;
    }

    // r2 should re-follow r1 (primary_addr points at r1)
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    let expected = format!("127.0.0.1:{}", r1_port);
    loop {
        if let Some(addr) = r2_mgr.replication.primary_addr() {
            if addr == expected || addr.ends_with(&format!(":{}", r1_port)) {
                break;
            }
        }
        // Also accept via ROLE slave listing
        if let Ok(mut c) = TcpStream::connect(format!("127.0.0.1:{}", r2_port)).await {
            let role = send_cmd(&mut c, &["ROLE"]).await;
            if let RespValue::Array(arr) = &role {
                // ROLE slave: ["slave", host, port_int, state, offset]
                if arr.first()
                    == Some(&RespValue::BulkString(Some(Bytes::from_static(b"slave"))))
                {
                    let port_ok = matches!(arr.get(2), Some(RespValue::Integer(p)) if *p == r1_port as i64);
                    if port_ok {
                        break;
                    }
                }
            }
        }
        if tokio::time::Instant::now() > deadline {
            panic!(
                "r2 did not re-follow r1; primary_addr={:?} is_replica={}",
                r2_mgr.replication.primary_addr(),
                r2_mgr.replication.is_replica()
            );
        }
        sleep(Duration::from_millis(50)).await;
    }

    // Write on new master and observe on r2
    let mut r1_cli = TcpStream::connect(format!("127.0.0.1:{}", r1_port))
        .await
        .expect("r1 connect");
    assert_eq!(
        send_cmd(&mut r1_cli, &["SET", "rf_key2", "after"]).await,
        RespValue::ok()
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        if let Ok(mut c) = TcpStream::connect(format!("127.0.0.1:{}", r2_port)).await {
            let got = send_cmd(&mut c, &["GET", "rf_key2"]).await;
            if got == RespValue::BulkString(Some(Bytes::from_static(b"after"))) {
                break;
            }
        }
        if tokio::time::Instant::now() > deadline {
            panic!("r2 did not receive write from new master after re-follow");
        }
        sleep(Duration::from_millis(50)).await;
    }

    let _ = m_tx.send(true);
    let _ = r1_tx.send(true);
    let _ = r2_tx.send(true);
    mh.abort();
    r1h.abort();
    r2h.abort();
    r1_loop.abort();
    r2_loop.abort();
    sleep(Duration::from_millis(50)).await;
    let _ = std::fs::remove_dir_all(&master_dir);
    let _ = std::fs::remove_dir_all(&r1_dir);
    let _ = std::fs::remove_dir_all(&r2_dir);
}
