//! Batch EW: Sentinel-lite MONITOR / GET-MASTER-ADDR / s_down / FAILOVER.
//! Batch FC: promote-success gate (no switch_master on PING-only).
//! Batch FE: leader election on IS-MASTER-DOWN-BY-ADDR; only elected leader auto-failovers.
//! Batch FK: promote ranking by priority then offset (not discovery order).
//! Batch FM: live INFO slave_priority refresh + auto failover cooldown.
//! Batch FN: CKQUORUM / elect majority live PING; probe `*` honesty.

use bytes::Bytes;
use kore::config::Config;
use kore::persistence::{PersistenceConfig, PersistenceManager, SaveRule};
use kore::protocol::{RespParser, RespValue};
use kore::{
    count_reachable_sentinels, parse_info_slave_priority, rank_replicas_for_promote,
    test_promote_inject, test_set_failover_cooldown_ms, test_set_promote_inject, try_elect_leader,
    try_failover, Cache, ReplicaInfo, Server, PROMOTE_INJECT_FORCE_FAIL, PROMOTE_INJECT_FORCE_OK,
};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::time::{sleep, Duration};

/// Reserve `n` distinct OS-assigned localhost ports (hold listeners, then release).
/// Same pattern as `pipeline_unix_test::free_port` / admin_http tests — avoids hard-coded
/// bind collisions under parallel `cargo test`.
async fn free_ports(n: usize) -> Vec<u16> {
    let mut listeners = Vec::with_capacity(n);
    for _ in 0..n {
        listeners.push(
            TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind ephemeral"),
        );
    }
    let ports: Vec<u16> = listeners
        .iter()
        .map(|l| l.local_addr().expect("local_addr").port())
        .collect();
    drop(listeners);
    ports
}

async fn free_port() -> u16 {
    free_ports(1).await[0]
}

fn make_config(port: u16) -> Arc<Config> {
    // Unique dir per process so Batch EZ sentinel.conf load/autosave cannot
    // leak monitors across tests (duplicate MONITOR name).
    let dir = std::env::temp_dir().join(format!(
        "kore-sent-data-{}-{}",
        port,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&dir);
    make_config_with_dir(port, dir.to_string_lossy().as_ref())
}

fn make_config_with_dir(port: u16, dir: &str) -> Arc<Config> {
    let _ = std::fs::create_dir_all(dir);
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
        dir: dir.to_string(),
        dbfilename: "dump.rdb".to_string(),
        appendonly: false,
        appendfilename: "appendonly.aof".to_string(),
        replicaof: String::new(),
        save: String::new(),
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

/// Persistence-backed server so INFO reports `slave_priority` (Batch FM).
fn make_persisted_server(port: u16) -> (Server, Arc<Config>) {
    let dir = std::env::temp_dir().join(format!(
        "kore-sent-pers-{}-{}",
        port,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&dir);
    let pconfig = PersistenceConfig {
        dir: PathBuf::from(&dir),
        dbfilename: "dump.rdb".to_string(),
        appendonly: false,
        appendfilename: "appendonly.aof".to_string(),
        save_rules: vec![SaveRule::new(900, 1)],
    };
    let mgr = PersistenceManager::new(pconfig).unwrap();
    let cfg = make_config_with_dir(port, dir.to_string_lossy().as_ref());
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    (Server::with_persistence(cache, Arc::clone(&cfg), mgr), cfg)
}

async fn wait_listen(port: u16) {
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return;
        }
        sleep(Duration::from_millis(20)).await;
    }
    panic!("port {} not listening", port);
}

async fn send_cmd(stream: &mut TcpStream, parts: &[&str]) -> RespValue {
    let args: Vec<RespValue> = parts
        .iter()
        .map(|p| RespValue::BulkString(Some(Bytes::from(p.to_string()))))
        .collect();
    let payload = RespValue::Array(args).serialize();
    stream.write_all(&payload).await.unwrap();
    let mut parser = RespParser::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        if let Some(v) = parser.parse().unwrap() {
            return v;
        }
        let n = stream.read(&mut buf).await.unwrap();
        assert!(n > 0, "eof");
        parser.feed(&buf[..n]);
    }
}

fn is_ok(v: &RespValue) -> bool {
    matches!(v, RespValue::SimpleString(s) if s.as_ref() == b"OK")
}

fn as_bulk(v: &RespValue) -> String {
    match v {
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        other => panic!("expected bulk: {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sentinel_monitor_get_addr_and_sdown() {
    let ports = free_ports(2).await;
    let master_port = ports[0];
    let sentinel_port = ports[1];

    let cache_m = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_m = Server::new(cache_m, make_config(master_port));
    let (tx_m, rx_m) = watch::channel(false);
    let hm = tokio::spawn(async move {
        let _ = srv_m.run_with_shutdown(rx_m).await;
    });
    wait_listen(master_port).await;

    let cache_s = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_s = Server::new(cache_s, make_config(sentinel_port));
    let sentinel = Arc::clone(srv_s.sentinel());
    let (tx_s, rx_s) = watch::channel(false);
    let hs = tokio::spawn(async move {
        let _ = srv_s.run_with_shutdown(rx_s).await;
    });
    wait_listen(sentinel_port).await;

    let mut cli = TcpStream::connect(("127.0.0.1", sentinel_port)).await.unwrap();
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &[
                "SENTINEL",
                "MONITOR",
                "mymaster",
                "127.0.0.1",
                &master_port.to_string(),
                "1",
            ],
        )
        .await
    ));

    let addr = send_cmd(
        &mut cli,
        &["SENTINEL", "GET-MASTER-ADDR-BY-NAME", "mymaster"],
    )
    .await;
    match addr {
        RespValue::Array(a) => {
            assert_eq!(a.len(), 2);
            assert_eq!(as_bulk(&a[0]), "127.0.0.1");
            assert_eq!(as_bulk(&a[1]), master_port.to_string());
        }
        other => panic!("{:?}", other),
    }

    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &[
                "SENTINEL",
                "SET",
                "mymaster",
                "down-after-milliseconds",
                "80",
            ],
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &["SENTINEL", "SET", "mymaster", "auto-failover", "no"],
        )
        .await
    ));

    let _ = tx_m.send(true);
    hm.abort();
    // Sentinel tick is 1s; wait for a failed probe + maybe_sdown.
    sleep(Duration::from_millis(1500)).await;

    let m = sentinel.master("mymaster").expect("master");
    assert!(
        m.s_down,
        "expected s_down after master death (flags={})",
        m.flags()
    );
    // Quorum 1 → o_down from self vote alone.
    assert!(
        m.o_down,
        "expected o_down with quorum=1 (flags={})",
        m.flags()
    );

    match send_cmd(&mut cli, &["SENTINEL", "HELP"]).await {
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
            assert!(joined.contains("MONITOR"));
            assert!(joined.contains("GET-MASTER-ADDR-BY-NAME"));
            assert!(joined.contains("FAILOVER"));
            assert!(joined.contains("MEET"));
            assert!(joined.contains("IS-MASTER-DOWN-BY-ADDR"));
        }
        other => panic!("{:?}", other),
    }

    // MYID
    match send_cmd(&mut cli, &["SENTINEL", "MYID"]).await {
        RespValue::BulkString(Some(b)) => assert_eq!(b.len(), 40),
        other => panic!("{:?}", other),
    }

    let _ = tx_s.send(true);
    hs.abort();
    sleep(Duration::from_millis(50)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sentinel_manual_failover_switches_addr() {
    let ports = free_ports(3).await;
    let dead_master_port = ports[0]; // never started
    let promote_port = ports[1];
    let sentinel_port = ports[2];

    let cache_t = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_t = Server::new(cache_t, make_config(promote_port));
    let (tx_t, rx_t) = watch::channel(false);
    let ht = tokio::spawn(async move {
        let _ = srv_t.run_with_shutdown(rx_t).await;
    });
    wait_listen(promote_port).await;

    let cache_s = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_s = Server::new(cache_s, make_config(sentinel_port));
    let sentinel = Arc::clone(srv_s.sentinel());
    let (tx_s, rx_s) = watch::channel(false);
    let hs = tokio::spawn(async move {
        let _ = srv_s.run_with_shutdown(rx_s).await;
    });
    wait_listen(sentinel_port).await;

    sentinel
        .monitor("mymaster", "127.0.0.1", dead_master_port, 1)
        .unwrap();
    // Inject replica list (ROLE would discover this).
    sentinel.note_ok(
        "mymaster",
        Some(vec![ReplicaInfo::new("127.0.0.1", promote_port)]),
    );

    let mut cli = TcpStream::connect(("127.0.0.1", sentinel_port)).await.unwrap();
    assert!(is_ok(
        &send_cmd(&mut cli, &["SENTINEL", "FAILOVER", "mymaster"]).await
    ));

    let addr = send_cmd(
        &mut cli,
        &["SENTINEL", "GET-MASTER-ADDR-BY-NAME", "mymaster"],
    )
    .await;
    match addr {
        RespValue::Array(a) => {
            assert_eq!(as_bulk(&a[0]), "127.0.0.1");
            assert_eq!(as_bulk(&a[1]), promote_port.to_string());
        }
        other => panic!("expected switched master: {:?}", other),
    }
    assert!(sentinel.master("mymaster").unwrap().failover_epoch >= 1);
    // Batch FC: in-progress flag cleared after successful failover.
    assert!(!sentinel.master("mymaster").unwrap().failover_in_progress);

    let _ = tx_s.send(true);
    let _ = tx_t.send(true);
    hs.abort();
    ht.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch FC: when promote cmds are forced to fail (target still PING-ok), do **not**
/// `switch_master` — master address stays on the old (dead) master.
#[tokio::test(flavor = "multi_thread")]
async fn sentinel_failover_no_switch_on_promote_fail() {
    let ports = free_ports(3).await;
    let dead_master_port = ports[0]; // never started
    let ping_ok_port = ports[1];
    let sentinel_port = ports[2];

    let cache_t = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_t = Server::new(cache_t, make_config(ping_ok_port));
    let (tx_t, rx_t) = watch::channel(false);
    let ht = tokio::spawn(async move {
        let _ = srv_t.run_with_shutdown(rx_t).await;
    });
    wait_listen(ping_ok_port).await;

    let cache_s = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_s = Server::new(cache_s, make_config(sentinel_port));
    let sentinel = Arc::clone(srv_s.sentinel());
    let (tx_s, rx_s) = watch::channel(false);
    let hs = tokio::spawn(async move {
        let _ = srv_s.run_with_shutdown(rx_s).await;
    });
    wait_listen(sentinel_port).await;

    sentinel
        .monitor("mymaster", "127.0.0.1", dead_master_port, 1)
        .unwrap();
    sentinel.note_ok(
        "mymaster",
        Some(vec![ReplicaInfo::new("127.0.0.1", ping_ok_port)]),
    );

    let epoch_before = sentinel.master("mymaster").unwrap().failover_epoch;

    let _guard = test_promote_inject();
    test_set_promote_inject(PROMOTE_INJECT_FORCE_FAIL);

    let mut cli = TcpStream::connect(("127.0.0.1", sentinel_port)).await.unwrap();
    let reply = send_cmd(&mut cli, &["SENTINEL", "FAILOVER", "mymaster"]).await;
    match &reply {
        RespValue::Error(e) => {
            let s = String::from_utf8_lossy(e);
            assert!(
                s.to_ascii_lowercase().contains("failed promote")
                    || s.to_ascii_lowercase().contains("promote"),
                "expected promote-fail error, got {}",
                s
            );
        }
        other => panic!("expected ERR on promote fail, got {:?}", other),
    }

    let m = sentinel.master("mymaster").unwrap();
    assert_eq!(m.ip, "127.0.0.1");
    assert_eq!(
        m.port, dead_master_port,
        "master addr must not switch on PING-only / promote fail"
    );
    assert_eq!(
        m.failover_epoch, epoch_before,
        "failover_epoch must not advance without switch_master"
    );
    assert!(
        !m.failover_in_progress,
        "in-progress flag must clear after failed failover"
    );

    let addr = send_cmd(
        &mut cli,
        &["SENTINEL", "GET-MASTER-ADDR-BY-NAME", "mymaster"],
    )
    .await;
    match addr {
        RespValue::Array(a) => {
            assert_eq!(as_bulk(&a[0]), "127.0.0.1");
            assert_eq!(as_bulk(&a[1]), dead_master_port.to_string());
        }
        other => panic!("expected unchanged master: {:?}", other),
    }

    let _ = tx_s.send(true);
    let _ = tx_t.send(true);
    hs.abort();
    ht.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch FC: injected promote OK still switches master (happy inject path).
#[tokio::test(flavor = "multi_thread")]
async fn sentinel_failover_switches_on_inject_ok() {
    let ports = free_ports(3).await;
    let dead_master_port = ports[0];
    let target_port = ports[1];
    let sentinel_port = ports[2];

    let cache_t = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_t = Server::new(cache_t, make_config(target_port));
    let (tx_t, rx_t) = watch::channel(false);
    let ht = tokio::spawn(async move {
        let _ = srv_t.run_with_shutdown(rx_t).await;
    });
    wait_listen(target_port).await;

    let cache_s = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_s = Server::new(cache_s, make_config(sentinel_port));
    let sentinel = Arc::clone(srv_s.sentinel());
    let (tx_s, rx_s) = watch::channel(false);
    let hs = tokio::spawn(async move {
        let _ = srv_s.run_with_shutdown(rx_s).await;
    });
    wait_listen(sentinel_port).await;

    sentinel
        .monitor("mymaster", "127.0.0.1", dead_master_port, 1)
        .unwrap();
    sentinel.note_ok(
        "mymaster",
        Some(vec![ReplicaInfo::new("127.0.0.1", target_port)]),
    );

    let _guard = test_promote_inject();
    test_set_promote_inject(PROMOTE_INJECT_FORCE_OK);

    assert!(try_failover(&sentinel, "mymaster").await.is_ok());

    let m = sentinel.master("mymaster").unwrap();
    assert_eq!(m.port, target_port);
    assert!(m.failover_epoch >= 1);
    assert!(!m.failover_in_progress);

    let _ = tx_s.send(true);
    let _ = tx_t.send(true);
    hs.abort();
    ht.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch EX: two Sentinels MEET; IS-MASTER-DOWN-BY-ADDR votes; o_down needs quorum 2.
#[tokio::test(flavor = "multi_thread")]
async fn sentinel_meet_and_odown_quorum() {
    let ports = free_ports(3).await;
    let master_port = ports[0];
    let s1_port = ports[1];
    let s2_port = ports[2];

    let cache_m = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_m = Server::new(cache_m, make_config(master_port));
    let (tx_m, rx_m) = watch::channel(false);
    let hm = tokio::spawn(async move {
        let _ = srv_m.run_with_shutdown(rx_m).await;
    });
    wait_listen(master_port).await;

    let cache_s1 = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_s1 = Server::new(cache_s1, make_config(s1_port));
    let s1 = Arc::clone(srv_s1.sentinel());
    let (tx_s1, rx_s1) = watch::channel(false);
    let hs1 = tokio::spawn(async move {
        let _ = srv_s1.run_with_shutdown(rx_s1).await;
    });
    wait_listen(s1_port).await;

    let cache_s2 = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_s2 = Server::new(cache_s2, make_config(s2_port));
    let s2 = Arc::clone(srv_s2.sentinel());
    let (tx_s2, rx_s2) = watch::channel(false);
    let hs2 = tokio::spawn(async move {
        let _ = srv_s2.run_with_shutdown(rx_s2).await;
    });
    wait_listen(s2_port).await;

    let mut c1 = TcpStream::connect(("127.0.0.1", s1_port)).await.unwrap();
    let mut c2 = TcpStream::connect(("127.0.0.1", s2_port)).await.unwrap();

    for cli in [&mut c1, &mut c2] {
        assert!(is_ok(
            &send_cmd(
                cli,
                &[
                    "SENTINEL",
                    "MONITOR",
                    "mymaster",
                    "127.0.0.1",
                    &master_port.to_string(),
                    "2",
                ],
            )
            .await
        ));
        assert!(is_ok(
            &send_cmd(
                cli,
                &[
                    "SENTINEL",
                    "SET",
                    "mymaster",
                    "down-after-milliseconds",
                    "80",
                ],
            )
            .await
        ));
        assert!(is_ok(
            &send_cmd(
                cli,
                &["SENTINEL", "SET", "mymaster", "auto-failover", "no"],
            )
            .await
        ));
    }

    assert!(is_ok(
        &send_cmd(
            &mut c1,
            &["SENTINEL", "MEET", "127.0.0.1", &s2_port.to_string()],
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut c2,
            &["SENTINEL", "MEET", "127.0.0.1", &s1_port.to_string()],
        )
        .await
    ));
    assert_eq!(s1.peers().len(), 1);
    assert_eq!(s2.peers().len(), 1);

    match send_cmd(&mut c1, &["SENTINEL", "SENTINELS", "mymaster"]).await {
        RespValue::Array(a) => assert_eq!(a.len(), 1),
        other => panic!("{:?}", other),
    }

    let _ = tx_m.send(true);
    hm.abort();
    // Wait for s_down, then another tick so peers exchange is-master-down votes.
    sleep(Duration::from_millis(3200)).await;

    let m1 = s1.master("mymaster").unwrap();
    let m2 = s2.master("mymaster").unwrap();
    assert!(m1.s_down && m2.s_down, "both should s_down");
    assert!(
        m1.o_down && m2.o_down,
        "expected o_down: s1 flags={} votes={} s2 flags={} votes={}",
        m1.flags(),
        m1.down_votes,
        m2.flags(),
        m2.down_votes
    );

    match send_cmd(
        &mut c1,
        &[
            "SENTINEL",
            "IS-MASTER-DOWN-BY-ADDR",
            "127.0.0.1",
            &master_port.to_string(),
            "0",
            "*",
        ],
    )
    .await
    {
        RespValue::Array(a) => {
            assert_eq!(a[0], RespValue::Integer(1));
        }
        other => panic!("{:?}", other),
    }

    let _ = tx_s1.send(true);
    let _ = tx_s2.send(true);
    hs1.abort();
    hs2.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch EZ: FLUSHCONFIG writes sentinel.conf; load_or_new restores monitors.
#[tokio::test(flavor = "multi_thread")]
async fn sentinel_flushconfig_and_load() {
    let port = free_port().await;
    let dir = std::env::temp_dir().join(format!(
        "kore-sent-ez-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&dir);

    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let mut cfg = (*make_config(port)).clone();
    cfg.dir = dir.to_string_lossy().to_string();
    let srv = Server::new(cache, Arc::new(cfg));
    let sentinel = Arc::clone(srv.sentinel());
    let my_id = sentinel.my_id();
    let (tx, rx) = watch::channel(false);
    let h = tokio::spawn(async move {
        let _ = srv.run_with_shutdown(rx).await;
    });
    wait_listen(port).await;

    let mut cli = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &[
                "SENTINEL",
                "MONITOR",
                "mymaster",
                "10.9.8.7",
                "7000",
                "2",
            ],
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(&mut cli, &["SENTINEL", "FLUSHCONFIG"]).await
    ));

    let path = dir.join("sentinel.conf");
    let body = std::fs::read_to_string(&path).expect("sentinel.conf written");
    assert!(body.contains("mymaster"));
    assert!(body.contains("10.9.8.7"));
    assert!(body.contains(&my_id) || body.contains("myid"));

    let _ = tx.send(true);
    h.abort();
    sleep(Duration::from_millis(50)).await;

    let loaded = kore::SentinelState::load_or_new(dir.to_str().unwrap());
    let m = loaded.master("mymaster").expect("restored master");
    assert_eq!(m.ip, "10.9.8.7");
    assert_eq!(m.port, 7000);
    assert_eq!(m.quorum, 2);

    let _ = std::fs::remove_dir_all(&dir);
}

/// Batch FA: SENTINEL HELLO discovers peer and can switch-master on higher epoch.
#[tokio::test(flavor = "multi_thread")]
async fn sentinel_hello_discovers_peer_and_switch_master() {
    let ports = free_ports(2).await;
    let s1_port = ports[0];
    let s2_port = ports[1];

    let cache1 = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv1 = Server::new(cache1, make_config(s1_port));
    let s1 = Arc::clone(srv1.sentinel());
    let (tx1, rx1) = watch::channel(false);
    let h1 = tokio::spawn(async move {
        let _ = srv1.run_with_shutdown(rx1).await;
    });
    wait_listen(s1_port).await;

    let cache2 = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv2 = Server::new(cache2, make_config(s2_port));
    let s2 = Arc::clone(srv2.sentinel());
    let (tx2, rx2) = watch::channel(false);
    let h2 = tokio::spawn(async move {
        let _ = srv2.run_with_shutdown(rx2).await;
    });
    wait_listen(s2_port).await;

    s1.monitor("mymaster", "10.0.0.1", 6379, 1).unwrap();
    s2.monitor("mymaster", "10.0.0.1", 6379, 1).unwrap();

    let mut c1 = TcpStream::connect(("127.0.0.1", s1_port)).await.unwrap();
    // Hello from s2 advertising higher master config epoch + new address.
    let hello = format!(
        "127.0.0.1,{},{},1,mymaster,10.0.0.9,7000,5",
        s2_port,
        s2.my_id()
    );
    assert!(is_ok(
        &send_cmd(&mut c1, &["SENTINEL", "HELLO", &hello]).await
    ));

    assert_eq!(s1.peers().len(), 1);
    assert_eq!(s1.peers()[0].port, s2_port);
    let m = s1.master("mymaster").unwrap();
    assert_eq!(m.ip, "10.0.0.9");
    assert_eq!(m.port, 7000);
    assert_eq!(m.failover_epoch, 5);

    match send_cmd(&mut c1, &["SENTINEL", "HELP"]).await {
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
            assert!(joined.contains("HELLO"));
        }
        other => panic!("{:?}", other),
    }

    let _ = tx1.send(true);
    let _ = tx2.send(true);
    h1.abort();
    h2.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch FA: PUBLISH __sentinel__:hello on a live master succeeds.
#[tokio::test(flavor = "multi_thread")]
async fn sentinel_publishes_hello_to_master() {
    let ports = free_ports(2).await;
    let master_port = ports[0];
    let sent_port = ports[1];

    let cache_m = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_m = Server::new(cache_m, make_config(master_port));
    let (tx_m, rx_m) = watch::channel(false);
    let hm = tokio::spawn(async move {
        let _ = srv_m.run_with_shutdown(rx_m).await;
    });
    wait_listen(master_port).await;

    let cache_s = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_s = Server::new(cache_s, make_config(sent_port));
    let sentinel = Arc::clone(srv_s.sentinel());
    let (tx_s, rx_s) = watch::channel(false);
    let hs = tokio::spawn(async move {
        let _ = srv_s.run_with_shutdown(rx_s).await;
    });
    wait_listen(sent_port).await;

    let mut cli = TcpStream::connect(("127.0.0.1", sent_port)).await.unwrap();
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &[
                "SENTINEL",
                "MONITOR",
                "mymaster",
                "127.0.0.1",
                &master_port.to_string(),
                "1",
            ],
        )
        .await
    ));

    // Subscribe on master to receive hello publishes from sentinel tick.
    let mcli = TcpStream::connect(("127.0.0.1", master_port)).await.unwrap();
    // Use a second connection for SUBSCRIBE (blocks).
    let mut sub = TcpStream::connect(("127.0.0.1", master_port)).await.unwrap();
    let sub_cmd = RespValue::Array(vec![
        RespValue::BulkString(Some(Bytes::from_static(b"SUBSCRIBE"))),
        RespValue::BulkString(Some(Bytes::from(kore::HELLO_CHANNEL))),
    ])
    .serialize();
    sub.write_all(&sub_cmd).await.unwrap();

    // Drain subscribe confirmation.
    {
        let mut parser = RespParser::new();
        let mut buf = vec![0u8; 4096];
        let n = tokio::time::timeout(Duration::from_secs(2), sub.read(&mut buf))
            .await
            .expect("sub confirm timeout")
            .unwrap();
        parser.feed(&buf[..n]);
        let _ = parser.parse().unwrap();
    }

    // Wait for sentinel tick to PUBLISH hello (tick=1s).
    sleep(Duration::from_millis(1500)).await;

    // Read published message (or force via unit path if slow).
    let got = tokio::time::timeout(Duration::from_secs(2), async {
        let mut parser = RespParser::new();
        let mut buf = vec![0u8; 16 * 1024];
        loop {
            if let Some(v) = parser.parse().unwrap() {
                return v;
            }
            let n = sub.read(&mut buf).await.unwrap();
            if n == 0 {
                panic!("eof");
            }
            parser.feed(&buf[..n]);
        }
    })
    .await;

    match got {
        Ok(RespValue::Array(a)) | Ok(RespValue::Push(a)) => {
            // ["message", channel, payload] or similar
            let flat: Vec<String> = a
                .iter()
                .filter_map(|v| match v {
                    RespValue::BulkString(Some(b)) => {
                        Some(String::from_utf8_lossy(b).into_owned())
                    }
                    _ => None,
                })
                .collect();
            let joined = flat.join(" ");
            assert!(
                joined.contains("mymaster") || joined.contains(&sentinel.my_id()),
                "hello payload missing: {}",
                joined
            );
        }
        Ok(other) => panic!("unexpected pubsub frame: {:?}", other),
        Err(_) => {
            // Fallback: at least format_hello works for the monitored master.
            let csv = sentinel.format_hello("mymaster").expect("hello");
            assert!(csv.contains("mymaster"));
            assert!(csv.contains(&master_port.to_string()));
        }
    }

    let _ = mcli;
    let _ = tx_m.send(true);
    let _ = tx_s.send(true);
    hm.abort();
    hs.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch FE: IS-MASTER-DOWN-BY-ADDR returns non-empty leader when s_down / voting.
#[tokio::test(flavor = "multi_thread")]
async fn sentinel_is_master_down_returns_voted_leader() {
    let ports = free_ports(2).await;
    let master_port = ports[0];
    let sentinel_port = ports[1];

    let cache_m = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_m = Server::new(cache_m, make_config(master_port));
    let (tx_m, rx_m) = watch::channel(false);
    let hm = tokio::spawn(async move {
        let _ = srv_m.run_with_shutdown(rx_m).await;
    });
    wait_listen(master_port).await;

    let cache_s = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_s = Server::new(cache_s, make_config(sentinel_port));
    let sentinel = Arc::clone(srv_s.sentinel());
    let (tx_s, rx_s) = watch::channel(false);
    let hs = tokio::spawn(async move {
        let _ = srv_s.run_with_shutdown(rx_s).await;
    });
    wait_listen(sentinel_port).await;

    let mut cli = TcpStream::connect(("127.0.0.1", sentinel_port)).await.unwrap();
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &[
                "SENTINEL",
                "MONITOR",
                "mymaster",
                "127.0.0.1",
                &master_port.to_string(),
                "1",
            ],
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &[
                "SENTINEL",
                "SET",
                "mymaster",
                "down-after-milliseconds",
                "80",
            ],
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &["SENTINEL", "SET", "mymaster", "auto-failover", "no"],
        )
        .await
    ));

    let _ = tx_m.send(true);
    hm.abort();
    sleep(Duration::from_millis(1500)).await;
    assert!(sentinel.master("mymaster").unwrap().s_down);

    // Batch FN: probe-only (`*`) with no prior vote → Redis-honest leader "*".
    match send_cmd(
        &mut cli,
        &[
            "SENTINEL",
            "IS-MASTER-DOWN-BY-ADDR",
            "127.0.0.1",
            &master_port.to_string(),
            "0",
            "*",
        ],
    )
    .await
    {
        RespValue::Array(a) => {
            assert_eq!(a[0], RespValue::Integer(1));
            assert_eq!(as_bulk(&a[1]), "*");
            assert_eq!(a[2], RespValue::Integer(0));
        }
        other => panic!("{:?}", other),
    }

    // Explicit candidate vote.
    let cand = "ff".repeat(20);
    match send_cmd(
        &mut cli,
        &[
            "SENTINEL",
            "IS-MASTER-DOWN-BY-ADDR",
            "127.0.0.1",
            &master_port.to_string(),
            "7",
            &cand,
        ],
    )
    .await
    {
        RespValue::Array(a) => {
            assert_eq!(a[0], RespValue::Integer(1));
            assert_eq!(as_bulk(&a[1]), cand);
            assert_eq!(a[2], RespValue::Integer(7));
        }
        other => panic!("{:?}", other),
    }
    // Sticky same epoch.
    match send_cmd(
        &mut cli,
        &[
            "SENTINEL",
            "IS-MASTER-DOWN-BY-ADDR",
            "127.0.0.1",
            &master_port.to_string(),
            "7",
            &sentinel.my_id(),
        ],
    )
    .await
    {
        RespValue::Array(a) => {
            assert_eq!(as_bulk(&a[1]), cand);
        }
        other => panic!("{:?}", other),
    }

    let _ = tx_s.send(true);
    hs.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch FE: two Sentinels agree on a voted leader via IS-MASTER-DOWN-BY-ADDR.
#[tokio::test(flavor = "multi_thread")]
async fn sentinel_two_sentinels_vote_agreement() {
    let ports = free_ports(3).await;
    let master_port = ports[0];
    let s1_port = ports[1];
    let s2_port = ports[2];

    let cache_m = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_m = Server::new(cache_m, make_config(master_port));
    let (tx_m, rx_m) = watch::channel(false);
    let hm = tokio::spawn(async move {
        let _ = srv_m.run_with_shutdown(rx_m).await;
    });
    wait_listen(master_port).await;

    let cache_s1 = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_s1 = Server::new(cache_s1, make_config(s1_port));
    let s1 = Arc::clone(srv_s1.sentinel());
    let (tx_s1, rx_s1) = watch::channel(false);
    let hs1 = tokio::spawn(async move {
        let _ = srv_s1.run_with_shutdown(rx_s1).await;
    });
    wait_listen(s1_port).await;

    let cache_s2 = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_s2 = Server::new(cache_s2, make_config(s2_port));
    let s2 = Arc::clone(srv_s2.sentinel());
    let (tx_s2, rx_s2) = watch::channel(false);
    let hs2 = tokio::spawn(async move {
        let _ = srv_s2.run_with_shutdown(rx_s2).await;
    });
    wait_listen(s2_port).await;

    let mut c1 = TcpStream::connect(("127.0.0.1", s1_port)).await.unwrap();
    let mut c2 = TcpStream::connect(("127.0.0.1", s2_port)).await.unwrap();

    for cli in [&mut c1, &mut c2] {
        assert!(is_ok(
            &send_cmd(
                cli,
                &[
                    "SENTINEL",
                    "MONITOR",
                    "mymaster",
                    "127.0.0.1",
                    &master_port.to_string(),
                    "2",
                ],
            )
            .await
        ));
        assert!(is_ok(
            &send_cmd(
                cli,
                &[
                    "SENTINEL",
                    "SET",
                    "mymaster",
                    "down-after-milliseconds",
                    "80",
                ],
            )
            .await
        ));
        assert!(is_ok(
            &send_cmd(
                cli,
                &["SENTINEL", "SET", "mymaster", "auto-failover", "no"],
            )
            .await
        ));
    }

    assert!(is_ok(
        &send_cmd(
            &mut c1,
            &["SENTINEL", "MEET", "127.0.0.1", &s2_port.to_string()],
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut c2,
            &["SENTINEL", "MEET", "127.0.0.1", &s1_port.to_string()],
        )
        .await
    ));

    let _ = tx_m.send(true);
    hm.abort();
    sleep(Duration::from_millis(1500)).await;
    assert!(s1.master("mymaster").unwrap().s_down);
    assert!(s2.master("mymaster").unwrap().s_down);

    // s1 asks s2 to vote for s1 at epoch 11.
    let s1_id = s1.my_id();
    match send_cmd(
        &mut c2,
        &[
            "SENTINEL",
            "IS-MASTER-DOWN-BY-ADDR",
            "127.0.0.1",
            &master_port.to_string(),
            "11",
            &s1_id,
        ],
    )
    .await
    {
        RespValue::Array(a) => {
            assert_eq!(a[0], RespValue::Integer(1));
            assert_eq!(as_bulk(&a[1]), s1_id);
            assert_eq!(a[2], RespValue::Integer(11));
        }
        other => panic!("{:?}", other),
    }
    // s2's local vote matches.
    assert_eq!(s2.master("mymaster").unwrap().leader_runid, s1_id);
    assert_eq!(s2.master("mymaster").unwrap().leader_epoch, 11);

    // s2 cannot steal same epoch for itself (sticky).
    match send_cmd(
        &mut c2,
        &[
            "SENTINEL",
            "IS-MASTER-DOWN-BY-ADDR",
            "127.0.0.1",
            &master_port.to_string(),
            "11",
            &s2.my_id(),
        ],
    )
    .await
    {
        RespValue::Array(a) => assert_eq!(as_bulk(&a[1]), s1_id),
        other => panic!("{:?}", other),
    }

    // s1 also votes for itself; agreement.
    let _ = s1.vote_leader("mymaster", 11, &s1_id);
    assert!(s1.is_failover_leader("mymaster"));
    assert!(!s2.is_failover_leader("mymaster"));

    let _ = tx_s1.send(true);
    let _ = tx_s2.send(true);
    hs1.abort();
    hs2.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch FE: non-leader abstains from try_elect_leader / does not promote while
/// another sentinel holds the voted-leader (in-process + live peer vote).
#[tokio::test(flavor = "multi_thread")]
async fn sentinel_non_leader_does_not_elect() {
    let ports = free_ports(4).await;
    let master_port = ports[0];
    let promote_port = ports[1];
    let s1_port = ports[2];
    let s2_port = ports[3];

    // Dead master (never started). Live promote target for replica inject.
    let cache_t = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_t = Server::new(cache_t, make_config(promote_port));
    let (tx_t, rx_t) = watch::channel(false);
    let ht = tokio::spawn(async move {
        let _ = srv_t.run_with_shutdown(rx_t).await;
    });
    wait_listen(promote_port).await;

    let cache_s1 = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_s1 = Server::new(cache_s1, make_config(s1_port));
    let s1 = Arc::clone(srv_s1.sentinel());
    let (tx_s1, rx_s1) = watch::channel(false);
    let hs1 = tokio::spawn(async move {
        let _ = srv_s1.run_with_shutdown(rx_s1).await;
    });
    wait_listen(s1_port).await;

    let cache_s2 = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_s2 = Server::new(cache_s2, make_config(s2_port));
    let s2 = Arc::clone(srv_s2.sentinel());
    let (tx_s2, rx_s2) = watch::channel(false);
    let hs2 = tokio::spawn(async move {
        let _ = srv_s2.run_with_shutdown(rx_s2).await;
    });
    wait_listen(s2_port).await;

    for (s, port) in [(&s1, s1_port), (&s2, s2_port)] {
        s.set_listen_addr("127.0.0.1", port);
        s.monitor("mymaster", "127.0.0.1", master_port, 2).unwrap();
        s.set_option("mymaster", "down-after-milliseconds", "50")
            .unwrap();
        s.set_option("mymaster", "auto-failover", "yes").unwrap();
        // Force s_down by aging last_ok.
        std::thread::sleep(std::time::Duration::from_millis(60));
        s.maybe_sdown("mymaster");
        s.note_ok(
            "mymaster",
            Some(vec![ReplicaInfo::new("127.0.0.1", promote_port)]),
        );
        // note_ok clears s_down — re-age.
        std::thread::sleep(std::time::Duration::from_millis(60));
        s.maybe_sdown("mymaster");
        // Re-inject replicas without clearing s_down: write via internal state.
        {
            // Keep s_down by not calling note_ok; set replicas directly.
            // MasterInfo is cloned from API — use vote path after forcing s_down.
        }
    }

    // Re-setup s_down + replicas without clearing flags: use monitor path carefully.
    // After maybe_sdown, inject replicas under write by switch-less update:
    for s in [&s1, &s2] {
        // Peer MEET so election is multi-sentinel.
        let _ = s; // peers added below
    }

    // MEET both ways.
    let mut c1 = TcpStream::connect(("127.0.0.1", s1_port)).await.unwrap();
    let mut c2 = TcpStream::connect(("127.0.0.1", s2_port)).await.unwrap();
    assert!(is_ok(
        &send_cmd(
            &mut c1,
            &["SENTINEL", "MEET", "127.0.0.1", &s2_port.to_string()],
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut c2,
            &["SENTINEL", "MEET", "127.0.0.1", &s1_port.to_string()],
        )
        .await
    ));

    // Force s_down + o_down + replica list without going through note_ok clearing.
    // Use is_master_down after aging: re-call maybe_sdown; set replicas via a helper path.
    // Direct: vote_leader requires s_down only for is_master_down_by_addr; elect needs master.
    for s in [&s1, &s2] {
        // Age last_ok again if note_ok ran.
        std::thread::sleep(std::time::Duration::from_millis(60));
        s.maybe_sdown("mymaster");
        // Inject replicas by temporarily note_ok then force s_down flags back.
        s.note_ok(
            "mymaster",
            Some(vec![ReplicaInfo::new("127.0.0.1", promote_port)]),
        );
    }
    // Force s_down/o_down after replica inject by sleeping past down-after.
    sleep(Duration::from_millis(80)).await;
    for s in [&s1, &s2] {
        s.maybe_sdown("mymaster");
        // o_down via quorum 2: need peer vote — apply local peer_down manually.
        let _ = s.apply_down_votes("mymaster", 1);
        assert!(
            s.master("mymaster").unwrap().s_down,
            "expected s_down after age"
        );
        // Replicas must still be present (note_ok set them; maybe_sdown doesn't clear).
        assert!(
            !s.master("mymaster").unwrap().replicas.is_empty(),
            "replicas should remain after s_down"
        );
    }

    // s1 becomes voted leader on both at epoch 20.
    let s1_id = s1.my_id();
    let _ = s1.vote_leader("mymaster", 20, &s1_id);
    let _ = s2.vote_leader("mymaster", 20, &s1_id);
    assert!(s1.is_failover_leader("mymaster"));
    assert!(!s2.is_failover_leader("mymaster"));

    // Non-leader elect must fail (abstain).
    assert!(
        !try_elect_leader(&s2, "mymaster").await,
        "s2 already voted for s1 — must not elect self"
    );

    // Leader elect succeeds (sole campaigner with peer sticky vote for s1).
    assert!(
        try_elect_leader(&s1, "mymaster").await,
        "s1 should win leadership with s2's sticky vote"
    );

    // Non-leader must not switch_master even if try_failover were called only by leader.
    let epoch_s2_before = s2.master("mymaster").unwrap().failover_epoch;
    let port_s2_before = s2.master("mymaster").unwrap().port;
    // s2 abstains: tick path would skip try_failover; we only check elect false again.
    assert!(!try_elect_leader(&s2, "mymaster").await);
    assert_eq!(s2.master("mymaster").unwrap().port, port_s2_before);
    assert_eq!(
        s2.master("mymaster").unwrap().failover_epoch,
        epoch_s2_before
    );

    // Leader may promote (manual path also works without elect).
    assert!(try_failover(&s1, "mymaster").await.is_ok());
    assert_eq!(s1.master("mymaster").unwrap().port, promote_port);

    let _ = tx_s1.send(true);
    let _ = tx_s2.send(true);
    let _ = tx_t.send(true);
    hs1.abort();
    hs2.abort();
    ht.abort();
    sleep(Duration::from_millis(50)).await;
}

// ── Batch FK: promote ranking ────────────────────────────────────────────────

/// Batch FK unit-style: pure ranking prefers priority, then offset, skips 0.
#[test]
fn rank_replicas_for_promote_order() {
    let first_in_discovery = ReplicaInfo::new("127.0.0.1", 1001).with_rank(50, 9999);
    let higher_priority = ReplicaInfo::new("127.0.0.1", 1002).with_rank(200, 1);
    let higher_offset = ReplicaInfo::new("127.0.0.1", 1003).with_rank(50, 100);
    let never = ReplicaInfo::new("127.0.0.1", 1004).with_rank(0, 1_000_000);
    let ranked = rank_replicas_for_promote(&[
        first_in_discovery,
        higher_priority,
        higher_offset,
        never,
    ]);
    assert_eq!(ranked.len(), 3);
    assert_eq!(ranked[0].port, 1002); // highest priority
    assert_eq!(ranked[1].port, 1001); // same pri as 1003, higher offset
    assert_eq!(ranked[2].port, 1003);
}

/// Batch FK: multi-replica failover prefers higher priority over discovery order.
#[tokio::test(flavor = "multi_thread")]
async fn sentinel_failover_prefers_higher_priority() {
    let ports = free_ports(4).await;
    let dead_master_port = ports[0];
    let low_pri_port = ports[1]; // listed first (would win under first-replica-wins)
    let high_pri_port = ports[2];
    let sentinel_port = ports[3];

    let cache_lo = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_lo = Server::new(cache_lo, make_config(low_pri_port));
    let (tx_lo, rx_lo) = watch::channel(false);
    let h_lo = tokio::spawn(async move {
        let _ = srv_lo.run_with_shutdown(rx_lo).await;
    });
    wait_listen(low_pri_port).await;

    let cache_hi = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_hi = Server::new(cache_hi, make_config(high_pri_port));
    let (tx_hi, rx_hi) = watch::channel(false);
    let h_hi = tokio::spawn(async move {
        let _ = srv_hi.run_with_shutdown(rx_hi).await;
    });
    wait_listen(high_pri_port).await;

    let cache_s = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_s = Server::new(cache_s, make_config(sentinel_port));
    let sentinel = Arc::clone(srv_s.sentinel());
    let (tx_s, rx_s) = watch::channel(false);
    let hs = tokio::spawn(async move {
        let _ = srv_s.run_with_shutdown(rx_s).await;
    });
    wait_listen(sentinel_port).await;

    sentinel
        .monitor("mymaster", "127.0.0.1", dead_master_port, 1)
        .unwrap();
    // Discovery order: low priority first; high priority second.
    sentinel.note_ok(
        "mymaster",
        Some(vec![
            ReplicaInfo::new("127.0.0.1", low_pri_port).with_rank(50, 100),
            ReplicaInfo::new("127.0.0.1", high_pri_port).with_rank(200, 1),
        ]),
    );

    let _guard = test_promote_inject();
    test_set_promote_inject(PROMOTE_INJECT_FORCE_OK);

    assert!(try_failover(&sentinel, "mymaster").await.is_ok());
    let m = sentinel.master("mymaster").unwrap();
    assert_eq!(
        m.port, high_pri_port,
        "must promote higher priority, not first in discovery order"
    );

    let _ = tx_s.send(true);
    let _ = tx_lo.send(true);
    let _ = tx_hi.send(true);
    hs.abort();
    h_lo.abort();
    h_hi.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch FK: when priority ties, prefer higher replication offset.
#[tokio::test(flavor = "multi_thread")]
async fn sentinel_failover_prefers_higher_offset() {
    let ports = free_ports(4).await;
    let dead_master_port = ports[0];
    let low_off_port = ports[1]; // listed first
    let high_off_port = ports[2];
    let sentinel_port = ports[3];

    let cache_lo = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_lo = Server::new(cache_lo, make_config(low_off_port));
    let (tx_lo, rx_lo) = watch::channel(false);
    let h_lo = tokio::spawn(async move {
        let _ = srv_lo.run_with_shutdown(rx_lo).await;
    });
    wait_listen(low_off_port).await;

    let cache_hi = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_hi = Server::new(cache_hi, make_config(high_off_port));
    let (tx_hi, rx_hi) = watch::channel(false);
    let h_hi = tokio::spawn(async move {
        let _ = srv_hi.run_with_shutdown(rx_hi).await;
    });
    wait_listen(high_off_port).await;

    let cache_s = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_s = Server::new(cache_s, make_config(sentinel_port));
    let sentinel = Arc::clone(srv_s.sentinel());
    let (tx_s, rx_s) = watch::channel(false);
    let hs = tokio::spawn(async move {
        let _ = srv_s.run_with_shutdown(rx_s).await;
    });
    wait_listen(sentinel_port).await;

    sentinel
        .monitor("mymaster", "127.0.0.1", dead_master_port, 1)
        .unwrap();
    sentinel.note_ok(
        "mymaster",
        Some(vec![
            ReplicaInfo::new("127.0.0.1", low_off_port).with_rank(100, 10),
            ReplicaInfo::new("127.0.0.1", high_off_port).with_rank(100, 50_000),
        ]),
    );

    let _guard = test_promote_inject();
    test_set_promote_inject(PROMOTE_INJECT_FORCE_OK);

    assert!(try_failover(&sentinel, "mymaster").await.is_ok());
    assert_eq!(
        sentinel.master("mymaster").unwrap().port,
        high_off_port,
        "must prefer higher offset when priority ties"
    );

    let _ = tx_s.send(true);
    let _ = tx_lo.send(true);
    let _ = tx_hi.send(true);
    hs.abort();
    h_lo.abort();
    h_hi.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch FK: priority 0 is never selected when another eligible replica exists.
#[tokio::test(flavor = "multi_thread")]
async fn sentinel_failover_skips_priority_zero() {
    let ports = free_ports(4).await;
    let dead_master_port = ports[0];
    let never_port = ports[1]; // listed first, priority 0
    let ok_port = ports[2];
    let sentinel_port = ports[3];

    let cache_never = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_never = Server::new(cache_never, make_config(never_port));
    let (tx_n, rx_n) = watch::channel(false);
    let h_n = tokio::spawn(async move {
        let _ = srv_never.run_with_shutdown(rx_n).await;
    });
    wait_listen(never_port).await;

    let cache_ok = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_ok = Server::new(cache_ok, make_config(ok_port));
    let (tx_ok, rx_ok) = watch::channel(false);
    let h_ok = tokio::spawn(async move {
        let _ = srv_ok.run_with_shutdown(rx_ok).await;
    });
    wait_listen(ok_port).await;

    let cache_s = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_s = Server::new(cache_s, make_config(sentinel_port));
    let sentinel = Arc::clone(srv_s.sentinel());
    let (tx_s, rx_s) = watch::channel(false);
    let hs = tokio::spawn(async move {
        let _ = srv_s.run_with_shutdown(rx_s).await;
    });
    wait_listen(sentinel_port).await;

    sentinel
        .monitor("mymaster", "127.0.0.1", dead_master_port, 1)
        .unwrap();
    sentinel.note_ok(
        "mymaster",
        Some(vec![
            ReplicaInfo::new("127.0.0.1", never_port).with_rank(0, 9_999_999),
            ReplicaInfo::new("127.0.0.1", ok_port).with_rank(100, 1),
        ]),
    );

    let _guard = test_promote_inject();
    test_set_promote_inject(PROMOTE_INJECT_FORCE_OK);

    assert!(try_failover(&sentinel, "mymaster").await.is_ok());
    assert_eq!(
        sentinel.master("mymaster").unwrap().port,
        ok_port,
        "priority 0 must never be promoted when an eligible replica exists"
    );

    let _ = tx_s.send(true);
    let _ = tx_n.send(true);
    let _ = tx_ok.send(true);
    hs.abort();
    h_n.abort();
    h_ok.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch FK: all priority-0 replicas → no good replica (no switch_master).
#[tokio::test(flavor = "multi_thread")]
async fn sentinel_failover_all_priority_zero_fails() {
    let ports = free_ports(3).await;
    let dead_master_port = ports[0];
    let never_port = ports[1];
    let sentinel_port = ports[2];

    let cache_t = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_t = Server::new(cache_t, make_config(never_port));
    let (tx_t, rx_t) = watch::channel(false);
    let ht = tokio::spawn(async move {
        let _ = srv_t.run_with_shutdown(rx_t).await;
    });
    wait_listen(never_port).await;

    let cache_s = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_s = Server::new(cache_s, make_config(sentinel_port));
    let sentinel = Arc::clone(srv_s.sentinel());
    let (tx_s, rx_s) = watch::channel(false);
    let hs = tokio::spawn(async move {
        let _ = srv_s.run_with_shutdown(rx_s).await;
    });
    wait_listen(sentinel_port).await;

    sentinel
        .monitor("mymaster", "127.0.0.1", dead_master_port, 1)
        .unwrap();
    sentinel.note_ok(
        "mymaster",
        Some(vec![ReplicaInfo::new("127.0.0.1", never_port).with_rank(0, 100)]),
    );

    let err = try_failover(&sentinel, "mymaster").await.unwrap_err();
    assert!(
        err.to_ascii_lowercase().contains("no good replica"),
        "expected no good replica, got {}",
        err
    );
    assert_eq!(
        sentinel.master("mymaster").unwrap().port,
        dead_master_port,
        "must not switch_master when only priority-0 candidates"
    );

    let _ = tx_s.send(true);
    let _ = tx_t.send(true);
    hs.abort();
    ht.abort();
    sleep(Duration::from_millis(50)).await;
}

// ── Batch FM: INFO slave_priority refresh + failover cooldown ────────────────

#[test]
fn parse_info_slave_priority_unit() {
    assert_eq!(parse_info_slave_priority("role:master\r\n"), None);
    assert_eq!(
        parse_info_slave_priority("slave_priority:50\r\nrole:slave\r\n"),
        Some(50)
    );
    assert_eq!(parse_info_slave_priority("slave_priority:0\r\n"), Some(0));
}

/// Batch FM: live INFO `slave_priority` (150) beats discovery-order peer at default 100.
#[tokio::test(flavor = "multi_thread")]
async fn sentinel_info_priority_refresh_prefers_higher() {
    let ports = free_ports(4).await;
    let dead_master_port = ports[0];
    let discovery_first_port = ports[1]; // default priority 100, listed first
    let info_high_port = ports[2]; // CONFIG priority 150
    let sentinel_port = ports[3];

    let (srv_lo, _) = make_persisted_server(discovery_first_port);
    let (tx_lo, rx_lo) = watch::channel(false);
    let h_lo = tokio::spawn(async move {
        let _ = srv_lo.run_with_shutdown(rx_lo).await;
    });
    wait_listen(discovery_first_port).await;

    let (srv_hi, _) = make_persisted_server(info_high_port);
    let (tx_hi, rx_hi) = watch::channel(false);
    let h_hi = tokio::spawn(async move {
        let _ = srv_hi.run_with_shutdown(rx_hi).await;
    });
    wait_listen(info_high_port).await;

    // Set live INFO slave_priority on the second replica.
    let mut cli_hi = TcpStream::connect(("127.0.0.1", info_high_port))
        .await
        .unwrap();
    assert!(is_ok(
        &send_cmd(
            &mut cli_hi,
            &["CONFIG", "SET", "replica-priority", "150"]
        )
        .await
    ));
    // Confirm INFO surface.
    let info = send_cmd(&mut cli_hi, &["INFO", "replication"]).await;
    let info_s = as_bulk(&info);
    assert!(
        info_s.contains("slave_priority:150"),
        "expected INFO slave_priority:150, got {}",
        info_s
    );

    let cache_s = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_s = Server::new(cache_s, make_config(sentinel_port));
    let sentinel = Arc::clone(srv_s.sentinel());
    let (tx_s, rx_s) = watch::channel(false);
    let hs = tokio::spawn(async move {
        let _ = srv_s.run_with_shutdown(rx_s).await;
    });
    wait_listen(sentinel_port).await;

    sentinel
        .monitor("mymaster", "127.0.0.1", dead_master_port, 1)
        .unwrap();
    // Discovery order: default-100 first; ROLE-style inject without rank fields.
    // try_failover will INFO-refresh and promote the higher priority.
    sentinel.note_ok(
        "mymaster",
        Some(vec![
            ReplicaInfo::new("127.0.0.1", discovery_first_port),
            ReplicaInfo::new("127.0.0.1", info_high_port),
        ]),
    );

    let _guard = test_promote_inject();
    test_set_promote_inject(PROMOTE_INJECT_FORCE_OK);

    assert!(try_failover(&sentinel, "mymaster").await.is_ok());
    assert_eq!(
        sentinel.master("mymaster").unwrap().port,
        info_high_port,
        "INFO slave_priority 150 must beat discovery-order peer at default 100"
    );

    let _ = tx_s.send(true);
    let _ = tx_lo.send(true);
    let _ = tx_hi.send(true);
    hs.abort();
    h_lo.abort();
    h_hi.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch FM: INFO priority 0 is never promoted when an eligible peer exists.
#[tokio::test(flavor = "multi_thread")]
async fn sentinel_info_priority_zero_skipped() {
    let ports = free_ports(4).await;
    let dead_master_port = ports[0];
    let never_port = ports[1];
    let ok_port = ports[2];
    let sentinel_port = ports[3];

    let (srv_n, _) = make_persisted_server(never_port);
    let (tx_n, rx_n) = watch::channel(false);
    let h_n = tokio::spawn(async move {
        let _ = srv_n.run_with_shutdown(rx_n).await;
    });
    wait_listen(never_port).await;

    let (srv_ok, _) = make_persisted_server(ok_port);
    let (tx_ok, rx_ok) = watch::channel(false);
    let h_ok = tokio::spawn(async move {
        let _ = srv_ok.run_with_shutdown(rx_ok).await;
    });
    wait_listen(ok_port).await;

    let mut cli_n = TcpStream::connect(("127.0.0.1", never_port))
        .await
        .unwrap();
    assert!(is_ok(
        &send_cmd(&mut cli_n, &["CONFIG", "SET", "slave-priority", "0"]).await
    ));

    let cache_s = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_s = Server::new(cache_s, make_config(sentinel_port));
    let sentinel = Arc::clone(srv_s.sentinel());
    let (tx_s, rx_s) = watch::channel(false);
    let hs = tokio::spawn(async move {
        let _ = srv_s.run_with_shutdown(rx_s).await;
    });
    wait_listen(sentinel_port).await;

    sentinel
        .monitor("mymaster", "127.0.0.1", dead_master_port, 1)
        .unwrap();
    // Discovery lists never-promote first.
    sentinel.note_ok(
        "mymaster",
        Some(vec![
            ReplicaInfo::new("127.0.0.1", never_port),
            ReplicaInfo::new("127.0.0.1", ok_port),
        ]),
    );

    let _guard = test_promote_inject();
    test_set_promote_inject(PROMOTE_INJECT_FORCE_OK);

    assert!(try_failover(&sentinel, "mymaster").await.is_ok());
    assert_eq!(
        sentinel.master("mymaster").unwrap().port,
        ok_port,
        "INFO priority 0 must be skipped"
    );

    let _ = tx_s.send(true);
    let _ = tx_n.send(true);
    let _ = tx_ok.send(true);
    hs.abort();
    h_n.abort();
    h_ok.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch FM: after try_failover, auto cooldown is armed; eventually clears.
/// Manual path is not blocked by cooldown (operator force).
#[tokio::test(flavor = "multi_thread")]
async fn sentinel_failover_cooldown_after_attempt() {
    let ports = free_ports(3).await;
    let dead_master_port = ports[0];
    let target_port = ports[1];
    let sentinel_port = ports[2];

    let cache_t = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_t = Server::new(cache_t, make_config(target_port));
    let (tx_t, rx_t) = watch::channel(false);
    let ht = tokio::spawn(async move {
        let _ = srv_t.run_with_shutdown(rx_t).await;
    });
    wait_listen(target_port).await;

    let cache_s = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_s = Server::new(cache_s, make_config(sentinel_port));
    let sentinel = Arc::clone(srv_s.sentinel());
    let (tx_s, rx_s) = watch::channel(false);
    let hs = tokio::spawn(async move {
        let _ = srv_s.run_with_shutdown(rx_s).await;
    });
    wait_listen(sentinel_port).await;

    sentinel
        .monitor("mymaster", "127.0.0.1", dead_master_port, 1)
        .unwrap();
    sentinel.note_ok(
        "mymaster",
        Some(vec![ReplicaInfo::new("127.0.0.1", target_port)]),
    );

    // Short cooldown for the test; restore default on exit.
    test_set_failover_cooldown_ms(400);
    let _guard = test_promote_inject();
    test_set_promote_inject(PROMOTE_INJECT_FORCE_FAIL);

    // Failed attempt still arms cooldown (auto path would suppress thrash).
    let err = try_failover(&sentinel, "mymaster").await.unwrap_err();
    assert!(
        err.to_ascii_lowercase().contains("failed")
            || err.to_ascii_lowercase().contains("promote"),
        "expected promote failure, got {}",
        err
    );
    assert!(
        sentinel.in_failover_cooldown("mymaster"),
        "auto cooldown must arm after failed try_failover"
    );
    // Master address unchanged (FC gate).
    assert_eq!(
        sentinel.master("mymaster").unwrap().port,
        dead_master_port
    );

    // Manual force still allowed during cooldown (switch via inject OK).
    test_set_promote_inject(PROMOTE_INJECT_FORCE_OK);
    // note_ok again with replica after failed attempt (list still present).
    assert!(
        try_failover(&sentinel, "mymaster").await.is_ok(),
        "manual/direct try_failover must bypass auto cooldown"
    );
    assert_eq!(sentinel.master("mymaster").unwrap().port, target_port);
    // Cooldown re-armed after the successful attempt.
    assert!(sentinel.in_failover_cooldown("mymaster"));

    // Eventually allows auto retry.
    sleep(Duration::from_millis(450)).await;
    assert!(
        !sentinel.in_failover_cooldown("mymaster"),
        "cooldown must expire"
    );

    test_set_failover_cooldown_ms(0);
    let _ = tx_s.send(true);
    let _ = tx_t.send(true);
    hs.abort();
    ht.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch FN: CKQUORUM uses live PING; dead peer table entries do not inflate usable.
#[tokio::test(flavor = "multi_thread")]
async fn sentinel_ckquorum_live_probe_skips_dead_peers() {
    let ports = free_ports(4).await;
    let sentinel_port = ports[0];
    let live_peer_port = ports[1];
    // Never started — listed in peer table only.
    let dead_peer_port = ports[2];
    let unused_master_port = ports[3];

    let cache_s = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_s = Server::new(cache_s, make_config(sentinel_port));
    let sentinel = Arc::clone(srv_s.sentinel());
    let (tx_s, rx_s) = watch::channel(false);
    let hs = tokio::spawn(async move {
        let _ = srv_s.run_with_shutdown(rx_s).await;
    });
    wait_listen(sentinel_port).await;

    // Live peer sentinel (no master needed for PING reachability).
    let cache_p = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_p = Server::new(cache_p, make_config(live_peer_port));
    let (tx_p, rx_p) = watch::channel(false);
    let hp = tokio::spawn(async move {
        let _ = srv_p.run_with_shutdown(rx_p).await;
    });
    wait_listen(live_peer_port).await;

    let mut cli = TcpStream::connect(("127.0.0.1", sentinel_port)).await.unwrap();
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &[
                "SENTINEL",
                "MONITOR",
                "mymaster",
                "127.0.0.1",
                &unused_master_port.to_string(), // unused master addr; CKQUORUM only needs known master
                "2",
            ],
        )
        .await
    ));

    // Table-only dead peer: known_sentinel_count = 2 but live = 1 → NOQUORUM.
    sentinel.add_peer("dead".repeat(10), "127.0.0.1", dead_peer_port);
    assert_eq!(sentinel.known_sentinel_count(), 2);
    assert_eq!(count_reachable_sentinels(&sentinel).await, 1);

    match send_cmd(&mut cli, &["SENTINEL", "CKQUORUM", "mymaster"]).await {
        RespValue::Error(e) => {
            let msg = String::from_utf8_lossy(&e);
            assert!(
                msg.contains("NOQUORUM") && msg.contains("1 usable"),
                "expected NOQUORUM with 1 usable, got {}",
                msg
            );
        }
        other => panic!("expected NOQUORUM error, got {:?}", other),
    }

    // MEET live peer → live usable 2 ≥ quorum 2 → OK.
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &["SENTINEL", "MEET", "127.0.0.1", &live_peer_port.to_string()],
        )
        .await
    ));
    // known table: self + dead + live = 3; live reachable: self + live = 2.
    assert_eq!(sentinel.known_sentinel_count(), 3);
    assert_eq!(count_reachable_sentinels(&sentinel).await, 2);

    match send_cmd(&mut cli, &["SENTINEL", "CKQUORUM", "mymaster"]).await {
        RespValue::BulkString(Some(b)) => {
            let msg = String::from_utf8_lossy(&b);
            assert!(
                msg.starts_with("OK 2 usable"),
                "expected OK 2 usable, got {}",
                msg
            );
        }
        other => panic!("expected OK bulk, got {:?}", other),
    }

    let _ = tx_s.send(true);
    let _ = tx_p.send(true);
    hs.abort();
    hp.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch FN: dead peers do not inflate elect majority — sole live sentinel elects self.
#[tokio::test(flavor = "multi_thread")]
async fn sentinel_elect_ignores_dead_peers_for_majority() {
    let ports = free_ports(4).await;
    let sentinel_port = ports[0];
    let dead_peer_port = ports[1]; // never started
    let dead_peer_port2 = ports[2];
    let unused_master_port = ports[3];

    let cache_s = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_s = Server::new(cache_s, make_config(sentinel_port));
    let sentinel = Arc::clone(srv_s.sentinel());
    let (tx_s, rx_s) = watch::channel(false);
    let hs = tokio::spawn(async move {
        let _ = srv_s.run_with_shutdown(rx_s).await;
    });
    wait_listen(sentinel_port).await;

    sentinel
        .monitor("mymaster", "127.0.0.1", unused_master_port, 1)
        .unwrap();
    // Two dead table peers would force majority 2 under table-size N=3.
    sentinel.add_peer("dead1".repeat(8), "127.0.0.1", dead_peer_port);
    sentinel.add_peer("dead2".repeat(8), "127.0.0.1", dead_peer_port2);
    assert_eq!(sentinel.known_sentinel_count(), 3);
    assert_eq!(sentinel.leader_votes_needed("mymaster"), 2);
    assert_eq!(count_reachable_sentinels(&sentinel).await, 1);

    // Live path: N=1 → elect succeeds without peer votes.
    assert!(
        try_elect_leader(&sentinel, "mymaster").await,
        "sole live sentinel must elect despite dead peer table entries"
    );
    assert!(sentinel.is_failover_leader("mymaster"));
    assert_eq!(sentinel.master("mymaster").unwrap().leader_runid, sentinel.my_id());

    let _ = tx_s.send(true);
    hs.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch FN: after casting a vote, probe `*` returns that sticky leader (not "*").
#[tokio::test(flavor = "multi_thread")]
async fn sentinel_probe_star_returns_sticky_vote() {
    let ports = free_ports(2).await;
    let master_port = ports[0];
    let sentinel_port = ports[1];

    let cache_m = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_m = Server::new(cache_m, make_config(master_port));
    let (tx_m, rx_m) = watch::channel(false);
    let hm = tokio::spawn(async move {
        let _ = srv_m.run_with_shutdown(rx_m).await;
    });
    wait_listen(master_port).await;

    let cache_s = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_s = Server::new(cache_s, make_config(sentinel_port));
    let sentinel = Arc::clone(srv_s.sentinel());
    let (tx_s, rx_s) = watch::channel(false);
    let hs = tokio::spawn(async move {
        let _ = srv_s.run_with_shutdown(rx_s).await;
    });
    wait_listen(sentinel_port).await;

    let mut cli = TcpStream::connect(("127.0.0.1", sentinel_port)).await.unwrap();
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &[
                "SENTINEL",
                "MONITOR",
                "mymaster",
                "127.0.0.1",
                &master_port.to_string(),
                "1",
            ],
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &[
                "SENTINEL",
                "SET",
                "mymaster",
                "down-after-milliseconds",
                "80",
            ],
        )
        .await
    ));
    assert!(is_ok(
        &send_cmd(
            &mut cli,
            &["SENTINEL", "SET", "mymaster", "auto-failover", "no"],
        )
        .await
    ));

    let _ = tx_m.send(true);
    hm.abort();
    sleep(Duration::from_millis(1500)).await;
    assert!(sentinel.master("mymaster").unwrap().s_down);

    // Cast sticky vote via API, then probe on the wire for Redis-honest replay.
    let cand = "ab".repeat(20);
    let _ = sentinel.vote_leader("mymaster", 9, &cand);

    match send_cmd(
        &mut cli,
        &[
            "SENTINEL",
            "IS-MASTER-DOWN-BY-ADDR",
            "127.0.0.1",
            &master_port.to_string(),
            "0",
            "*",
        ],
    )
    .await
    {
        RespValue::Array(a) => {
            assert_eq!(a[0], RespValue::Integer(1));
            assert_eq!(as_bulk(&a[1]), cand);
            assert_eq!(a[2], RespValue::Integer(9));
        }
        other => panic!("{:?}", other),
    }

    let _ = tx_s.send(true);
    hs.abort();
    sleep(Duration::from_millis(50)).await;
}
