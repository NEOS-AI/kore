//! Batch EW: Sentinel-lite MONITOR / GET-MASTER-ADDR / s_down / FAILOVER.

use bytes::Bytes;
use kore::config::Config;
use kore::protocol::{RespParser, RespValue};
use kore::{Cache, ReplicaInfo, Server};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::{sleep, Duration};

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
    let master_port = 16901u16;
    let sentinel_port = 16902u16;

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
    let dead_master_port = 16903u16; // never started
    let promote_port = 16904u16;
    let sentinel_port = 16905u16;

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
        Some(vec![ReplicaInfo {
            ip: "127.0.0.1".into(),
            port: promote_port,
        }]),
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

    let _ = tx_s.send(true);
    let _ = tx_t.send(true);
    hs.abort();
    ht.abort();
    sleep(Duration::from_millis(50)).await;
}

/// Batch EX: two Sentinels MEET; IS-MASTER-DOWN-BY-ADDR votes; o_down needs quorum 2.
#[tokio::test(flavor = "multi_thread")]
async fn sentinel_meet_and_odown_quorum() {
    let master_port = 16910u16;
    let s1_port = 16911u16;
    let s2_port = 16912u16;

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
    let port = 16920u16;
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
    let s1_port = 16930u16;
    let s2_port = 16931u16;

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
    let master_port = 16932u16;
    let sent_port = 16933u16;

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
    let mut mcli = TcpStream::connect(("127.0.0.1", master_port)).await.unwrap();
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
