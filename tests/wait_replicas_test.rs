//! WAIT + min-replicas-to-write durability tests.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::persistence::{PersistenceConfig, PersistenceManager, SaveRule};
use kore::protocol::RespValue;
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
    let dir = std::env::temp_dir().join(format!("kore-wait-{}-{}", label, nanos));
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

fn assert_err_contains(resp: RespValue, needles: &[&str]) {
    match resp {
        RespValue::Error(e) => {
            let msg = String::from_utf8_lossy(&e).to_ascii_lowercase();
            for n in needles {
                assert!(
                    msg.contains(&n.to_ascii_lowercase()),
                    "error {:?} missing {:?}",
                    msg,
                    n
                );
            }
        }
        other => panic!("expected error, got {:?}", other),
    }
}

async fn send_cmd(stream: &mut TcpStream, parts: &[&str]) -> RespValue {
    let data = cmd(parts).serialize();
    stream.write_all(&data).await.expect("write");
    let mut parser = kore::protocol::RespParser::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        if let Some(v) = parser.parse().expect("parse") {
            return v;
        }
        let n = stream.read(&mut buf).await.expect("read");
        assert!(n > 0, "eof");
        parser.feed(&buf[..n]);
    }
}

#[test]
fn wait_arity_errors() {
    let dir = unique_dir("arity");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir, 6379), Some(mgr));

    assert_err_contains(handle(&mut h, cmd(&["WAIT"])), &["wrong number"]);
    assert_err_contains(handle(&mut h, cmd(&["WAIT", "1"])), &["wrong number"]);
    assert_err_contains(
        handle(&mut h, cmd(&["WAIT", "x", "100"])),
        &["integer"],
    );
    assert_err_contains(
        handle(&mut h, cmd(&["WAIT", "1", "-1"])),
        &["negative"],
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn wait_no_replicas_returns_zero_quickly() {
    let dir = unique_dir("none");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir, 6379), Some(mgr.clone()));

    handle(&mut h, cmd(&["SET", "a", "1"]));
    let start = std::time::Instant::now();
    let resp = handle(&mut h, cmd(&["WAIT", "1", "100"]));
    assert!(start.elapsed() < Duration::from_millis(500));
    assert_eq!(resp, RespValue::Integer(0));

    // WAIT 0 returns current acked count (0)
    assert_eq!(
        handle(&mut h, cmd(&["WAIT", "0", "1000"])),
        RespValue::Integer(0)
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn wait_succeeds_when_feed_acked() {
    let dir = unique_dir("acked");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir, 6379), Some(mgr.clone()));

    handle(&mut h, cmd(&["SET", "k", "v"]));
    let off = mgr.replication.master_repl_offset();
    let _feed = mgr
        .replication
        .register_replica_announced(Some("127.0.0.1".into()), Some(7000));
    mgr.replication
        .note_replica_ack(Some("127.0.0.1"), Some(7000), off);

    let resp = handle(&mut h, cmd(&["WAIT", "1", "500"]));
    assert_eq!(resp, RespValue::Integer(1));

    // Two requested but only one acked → timeout with 1
    let start = std::time::Instant::now();
    let resp = handle(&mut h, cmd(&["WAIT", "2", "80"]));
    assert_eq!(resp, RespValue::Integer(1));
    assert!(start.elapsed() >= Duration::from_millis(50));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn wait_on_replica_returns_zero() {
    let dir = unique_dir("replica-wait");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir, 6379), Some(mgr.clone()));

    handle(&mut h, cmd(&["REPLICAOF", "127.0.0.1", "1"]));
    assert_eq!(
        handle(&mut h, cmd(&["WAIT", "1", "50"])),
        RespValue::Integer(0)
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn min_replicas_to_write_blocks_and_allows() {
    let dir = unique_dir("min-write");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir, 6379), Some(mgr.clone()));

    // Enable durability gate
    assert_eq!(
        handle(
            &mut h,
            cmd(&["CONFIG", "SET", "min-replicas-to-write", "1"])
        ),
        RespValue::ok()
    );
    let got = handle(
        &mut h,
        cmd(&["CONFIG", "GET", "min-replicas-to-write"]),
    );
    match got {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(
                arr[1],
                RespValue::BulkString(Some(Bytes::from_static(b"1")))
            );
        }
        other => panic!("{:?}", other),
    }

    // No good replicas → NOREPLICAS
    assert_err_contains(
        handle(&mut h, cmd(&["SET", "x", "1"])),
        &["NOREPLICAS"],
    );

    // Register a fresh replica → writes allowed
    let _feed = mgr.replication.register_replica();
    assert_eq!(
        handle(&mut h, cmd(&["SET", "x", "1"])),
        RespValue::ok()
    );

    // Disable gate
    assert_eq!(
        handle(
            &mut h,
            cmd(&["CONFIG", "SET", "min-replicas-to-write", "0"])
        ),
        RespValue::ok()
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn min_replicas_max_lag_config() {
    let dir = unique_dir("max-lag");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir, 6379), Some(mgr.clone()));

    assert_eq!(
        handle(
            &mut h,
            cmd(&["CONFIG", "SET", "min-replicas-max-lag", "3"])
        ),
        RespValue::ok()
    );
    assert_eq!(mgr.replication.min_replicas_max_lag(), 3);

    let info = handle(&mut h, cmd(&["INFO", "replication"]));
    match info {
        RespValue::BulkString(Some(b)) => {
            let s = String::from_utf8_lossy(&b);
            assert!(s.contains("min_slaves_max_lag:3"), "{}", s);
            assert!(s.contains("min_slaves_to_write:"), "{}", s);
        }
        other => panic!("{:?}", other),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn command_catalog_lists_wait() {
    let dir = unique_dir("catalog-wait");
    let mgr = make_persistence(&dir);
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let mut h = CommandHandler::with_persistence(cache, make_config(&dir, 6379), Some(mgr));

    let resp = handle(&mut h, cmd(&["COMMAND", "INFO", "wait"]));
    match resp {
        RespValue::Array(arr) => {
            assert!(!arr.is_empty());
            // COMMAND INFO returns array of entries; first entry is wait spec
            match &arr[0] {
                RespValue::Array(spec) => {
                    assert_eq!(
                        spec[0],
                        RespValue::BulkString(Some(Bytes::from_static(b"wait")))
                    );
                    assert_eq!(spec[1], RespValue::Integer(3));
                }
                other => panic!("expected wait spec, {:?}", other),
            }
        }
        other => panic!("{:?}", other),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// End-to-end: master + replica, SET then WAIT until replica has the write.
#[tokio::test(flavor = "multi_thread")]
async fn wait_tcp_after_write_with_linked_replica() {
    use kore::persistence::replication::run_replica_loop;

    let master_port = 16710u16;
    let replica_port = 16711u16;
    let master_dir = unique_dir("tcp-m");
    let replica_dir = unique_dir("tcp-r");

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

    let (m_tx, m_rx) = watch::channel(false);
    let (r_tx, r_rx) = watch::channel(false);
    let m_h = tokio::spawn(async move {
        let _ = master.run_with_shutdown(m_rx).await;
    });
    let r_h = tokio::spawn(async move {
        let _ = replica.run_with_shutdown(r_rx).await;
    });

    let repl_dbs = kore::Databases::single(Arc::clone(&replica_cache));
    let repl_mgr_loop = Arc::clone(&replica_mgr);
    let repl_shutdown = r_tx.subscribe();
    let loop_h = tokio::spawn(async move {
        run_replica_loop(repl_dbs, repl_mgr_loop.replication.clone(), repl_shutdown).await;
    });

    sleep(Duration::from_millis(350)).await;

    let mut cli = timeout(
        Duration::from_secs(2),
        TcpStream::connect(format!("127.0.0.1:{}", master_port)),
    )
    .await
    .expect("timeout")
    .expect("connect");

    // Wait for link
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if replica_mgr.replication.master_link_up() {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("replica never linked");
        }
        sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(
        send_cmd(&mut cli, &["SET", "wait_key", "1"]).await,
        RespValue::ok()
    );

    // WAIT should eventually see at least 1 replica ACK (via feed GETACK or client path)
    let w = send_cmd(&mut cli, &["WAIT", "1", "5000"]).await;
    match w {
        RespValue::Integer(n) => assert!(n >= 1, "WAIT returned {}", n),
        other => panic!("expected integer, {:?}", other),
    }

    // Data on replica
    let mut rcli = TcpStream::connect(format!("127.0.0.1:{}", replica_port))
        .await
        .expect("r connect");
    let got = send_cmd(&mut rcli, &["GET", "wait_key"]).await;
    assert_eq!(
        got,
        RespValue::BulkString(Some(Bytes::from_static(b"1")))
    );

    let _ = m_tx.send(true);
    let _ = r_tx.send(true);
    m_h.abort();
    r_h.abort();
    loop_h.abort();
    sleep(Duration::from_millis(50)).await;
    let _ = std::fs::remove_dir_all(&master_dir);
    let _ = std::fs::remove_dir_all(&replica_dir);
}
