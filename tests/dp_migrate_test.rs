//! Batch DP/DQ/DT/GG: Redis key-level MIGRATE (DUMP→RESTORE for core types;
//! recreate for geo/stream; absolute expire).

use bytes::Bytes;
use kore::config::Config;
use kore::protocol::{RespParser, RespValue};
use kore::{test_acquire_migrate_key_inject, Cache, ClusterState, Server};
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

fn is_nokey(resp: &RespValue) -> bool {
    matches!(resp, RespValue::SimpleString(s) if s.as_ref() == b"NOKEY")
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

struct Pair {
    port_a: u16,
    port_b: u16,
    shut_a: watch::Sender<bool>,
    shut_b: watch::Sender<bool>,
    ha: tokio::task::JoinHandle<()>,
    hb: tokio::task::JoinHandle<()>,
}

async fn spawn_standalone_pair(port_a: u16, port_b: u16) -> Pair {
    let cache_a = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let cache_b = Cache::new_with_sweep(8, 1024 * 1024 * 20, 500 * 1024 * 1024, false);
    let srv_a = Server::new(cache_a, make_config(port_a, false));
    let srv_b = Server::new(cache_b, make_config(port_b, false));
    let (shut_a, ra) = watch::channel(false);
    let (shut_b, rb) = watch::channel(false);
    let ha = tokio::spawn(async move {
        let _ = srv_a.run_with_shutdown(ra).await;
    });
    let hb = tokio::spawn(async move {
        let _ = srv_b.run_with_shutdown(rb).await;
    });
    wait_listen(port_a).await;
    wait_listen(port_b).await;
    Pair {
        port_a,
        port_b,
        shut_a,
        shut_b,
        ha,
        hb,
    }
}

async fn shutdown(pair: Pair) {
    let _ = pair.shut_a.send(true);
    let _ = pair.shut_b.send(true);
    let _ = pair.ha.await;
    let _ = pair.hb.await;
}

/// Standalone string MIGRATE: present on dest, gone on source.
#[tokio::test(flavor = "multi_thread")]
async fn migrate_string_standalone_e2e() {
    let pair = spawn_standalone_pair(16900, 16901).await;
    let mut sa = TcpStream::connect(("127.0.0.1", pair.port_a)).await.unwrap();
    let mut sb = TcpStream::connect(("127.0.0.1", pair.port_b)).await.unwrap();

    assert!(is_ok(&send_cmd(&mut sa, &["SET", "mk", "hello-dp"]).await));
    let port_b = pair.port_b.to_string();
    let resp = send_cmd(
        &mut sa,
        &["MIGRATE", "127.0.0.1", &port_b, "mk", "0", "2000"],
    )
    .await;
    assert!(is_ok(&resp), "MIGRATE failed: {:?}", resp);

    assert!(matches!(
        send_cmd(&mut sa, &["GET", "mk"]).await,
        RespValue::BulkString(None)
    ));
    assert_eq!(as_bulk(&send_cmd(&mut sb, &["GET", "mk"]).await), "hello-dp");

    shutdown(pair).await;
}

/// Hash type MIGRATE.
#[tokio::test(flavor = "multi_thread")]
async fn migrate_hash_standalone_e2e() {
    let pair = spawn_standalone_pair(16902, 16903).await;
    let mut sa = TcpStream::connect(("127.0.0.1", pair.port_a)).await.unwrap();
    let mut sb = TcpStream::connect(("127.0.0.1", pair.port_b)).await.unwrap();

    match send_cmd(&mut sa, &["HSET", "mh", "f1", "v1", "f2", "v2"]).await {
        RespValue::Integer(n) => assert_eq!(n, 2),
        other => panic!("HSET failed: {:?}", other),
    }
    let port_b = pair.port_b.to_string();
    assert!(is_ok(
        &send_cmd(
            &mut sa,
            &["MIGRATE", "127.0.0.1", &port_b, "mh", "0", "2000"]
        )
        .await
    ));

    assert_eq!(
        send_cmd(&mut sa, &["EXISTS", "mh"]).await,
        RespValue::Integer(0)
    );
    assert_eq!(as_bulk(&send_cmd(&mut sb, &["HGET", "mh", "f1"]).await), "v1");
    assert_eq!(as_bulk(&send_cmd(&mut sb, &["HGET", "mh", "f2"]).await), "v2");

    shutdown(pair).await;
}

/// COPY leaves source key.
#[tokio::test(flavor = "multi_thread")]
async fn migrate_copy_leaves_source() {
    let pair = spawn_standalone_pair(16904, 16905).await;
    let mut sa = TcpStream::connect(("127.0.0.1", pair.port_a)).await.unwrap();
    let mut sb = TcpStream::connect(("127.0.0.1", pair.port_b)).await.unwrap();

    assert!(is_ok(&send_cmd(&mut sa, &["SET", "ck", "copy-me"]).await));
    let port_b = pair.port_b.to_string();
    assert!(is_ok(
        &send_cmd(
            &mut sa,
            &["MIGRATE", "127.0.0.1", &port_b, "ck", "0", "2000", "COPY"]
        )
        .await
    ));

    assert_eq!(as_bulk(&send_cmd(&mut sa, &["GET", "ck"]).await), "copy-me");
    assert_eq!(as_bulk(&send_cmd(&mut sb, &["GET", "ck"]).await), "copy-me");

    shutdown(pair).await;
}

/// Without REPLACE, existing dest key → BUSYKEY.
#[tokio::test(flavor = "multi_thread")]
async fn migrate_busykey_without_replace() {
    let pair = spawn_standalone_pair(16906, 16907).await;
    let mut sa = TcpStream::connect(("127.0.0.1", pair.port_a)).await.unwrap();
    let mut sb = TcpStream::connect(("127.0.0.1", pair.port_b)).await.unwrap();

    assert!(is_ok(&send_cmd(&mut sa, &["SET", "bk", "src"]).await));
    assert!(is_ok(&send_cmd(&mut sb, &["SET", "bk", "dst"]).await));
    let port_b = pair.port_b.to_string();
    let err = as_err(
        &send_cmd(
            &mut sa,
            &["MIGRATE", "127.0.0.1", &port_b, "bk", "0", "2000"],
        )
        .await,
    );
    assert!(
        err.starts_with("BUSYKEY"),
        "expected BUSYKEY, got {}",
        err
    );
    // Source unchanged
    assert_eq!(as_bulk(&send_cmd(&mut sa, &["GET", "bk"]).await), "src");
    assert_eq!(as_bulk(&send_cmd(&mut sb, &["GET", "bk"]).await), "dst");

    shutdown(pair).await;
}

/// REPLACE overwrites dest.
#[tokio::test(flavor = "multi_thread")]
async fn migrate_replace_overwrites_dest() {
    let pair = spawn_standalone_pair(16908, 16909).await;
    let mut sa = TcpStream::connect(("127.0.0.1", pair.port_a)).await.unwrap();
    let mut sb = TcpStream::connect(("127.0.0.1", pair.port_b)).await.unwrap();

    assert!(is_ok(&send_cmd(&mut sa, &["SET", "rk", "from-src"]).await));
    assert!(is_ok(&send_cmd(&mut sb, &["SET", "rk", "old-dst"]).await));
    let port_b = pair.port_b.to_string();
    assert!(is_ok(
        &send_cmd(
            &mut sa,
            &["MIGRATE", "127.0.0.1", &port_b, "rk", "0", "2000", "REPLACE"]
        )
        .await
    ));
    assert_eq!(
        send_cmd(&mut sa, &["EXISTS", "rk"]).await,
        RespValue::Integer(0)
    );
    assert_eq!(as_bulk(&send_cmd(&mut sb, &["GET", "rk"]).await), "from-src");

    shutdown(pair).await;
}

/// Missing key → NOKEY (not an error).
#[tokio::test(flavor = "multi_thread")]
async fn migrate_missing_returns_nokey() {
    let pair = spawn_standalone_pair(16910, 16911).await;
    let mut sa = TcpStream::connect(("127.0.0.1", pair.port_a)).await.unwrap();
    let port_b = pair.port_b.to_string();
    let resp = send_cmd(
        &mut sa,
        &["MIGRATE", "127.0.0.1", &port_b, "no-such", "0", "1000"],
    )
    .await;
    assert!(is_nokey(&resp), "expected NOKEY, got {:?}", resp);
    shutdown(pair).await;
}

/// Connection failure / bad port surfaces an error.
#[tokio::test(flavor = "multi_thread")]
async fn migrate_connect_failure() {
    let cache = Cache::new_with_sweep(8, 1024 * 1024 * 10, 500 * 1024 * 1024, false);
    let port = 16912u16;
    let srv = Server::new(cache, make_config(port, false));
    let (shut, r) = watch::channel(false);
    let h = tokio::spawn(async move {
        let _ = srv.run_with_shutdown(r).await;
    });
    wait_listen(port).await;
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    assert!(is_ok(&send_cmd(&mut s, &["SET", "x", "1"]).await));
    // Port with nothing listening
    let err = as_err(
        &send_cmd(
            &mut s,
            &["MIGRATE", "127.0.0.1", "1", "x", "0", "200"],
        )
        .await,
    );
    assert!(
        err.contains("connect") || err.contains("MIGRATE") || err.contains("timed out"),
        "unexpected err: {}",
        err
    );
    // Source key still present on failure before any transfer
    assert_eq!(as_bulk(&send_cmd(&mut s, &["GET", "x"]).await), "1");
    let _ = shut.send(true);
    let _ = h.await;
}

/// Multi-key KEYS form.
#[tokio::test(flavor = "multi_thread")]
async fn migrate_keys_option_multi() {
    // Hold inject lock so parallel mid-batch inject tests cannot trip this path.
    let _no_inject = test_acquire_migrate_key_inject().await;

    let pair = spawn_standalone_pair(16914, 16915).await;
    let mut sa = TcpStream::connect(("127.0.0.1", pair.port_a)).await.unwrap();
    let mut sb = TcpStream::connect(("127.0.0.1", pair.port_b)).await.unwrap();

    assert!(is_ok(&send_cmd(&mut sa, &["SET", "k1", "a"]).await));
    assert!(is_ok(&send_cmd(&mut sa, &["SET", "k2", "b"]).await));
    let port_b = pair.port_b.to_string();
    assert!(is_ok(
        &send_cmd(
            &mut sa,
            &[
                "MIGRATE",
                "127.0.0.1",
                &port_b,
                "",
                "0",
                "2000",
                "KEYS",
                "k1",
                "k2",
            ]
        )
        .await
    ));
    assert_eq!(
        send_cmd(&mut sa, &["EXISTS", "k1", "k2"]).await,
        RespValue::Integer(0)
    );
    assert_eq!(as_bulk(&send_cmd(&mut sb, &["GET", "k1"]).await), "a");
    assert_eq!(as_bulk(&send_cmd(&mut sb, &["GET", "k2"]).await), "b");

    shutdown(pair).await;
}

/// Batch DQ: multi-key mid-batch inject → IOERR reports migrated/skipped counts.
#[tokio::test(flavor = "multi_thread")]
async fn migrate_multi_key_partial_failure_reports_counts() {
    let pair = spawn_standalone_pair(16918, 16919).await;
    let mut sa = TcpStream::connect(("127.0.0.1", pair.port_a)).await.unwrap();
    let mut sb = TcpStream::connect(("127.0.0.1", pair.port_b)).await.unwrap();

    assert!(is_ok(&send_cmd(&mut sa, &["SET", "p1", "a"]).await));
    assert!(is_ok(&send_cmd(&mut sa, &["SET", "p2", "b"]).await));
    assert!(is_ok(&send_cmd(&mut sa, &["SET", "p3", "c"]).await));

    let _inj = test_acquire_migrate_key_inject().await;
    _inj.fail_after_successes(1);

    let port_b = pair.port_b.to_string();
    let err = as_err(
        &send_cmd(
            &mut sa,
            &[
                "MIGRATE",
                "127.0.0.1",
                &port_b,
                "",
                "0",
                "2000",
                "KEYS",
                "p1",
                "p2",
                "p3",
            ],
        )
        .await,
    );
    assert!(err.starts_with("IOERR"), "expected IOERR, got {}", err);
    assert!(
        err.contains("migrated=1"),
        "expected migrated=1 in partial reply: {}",
        err
    );
    assert!(
        err.contains("skipped="),
        "expected skipped= count in partial reply: {}",
        err
    );
    assert!(
        err.contains("Partial keys may have moved"),
        "expected partial wording: {}",
        err
    );

    // First key moved; later keys remain on source.
    assert_eq!(
        send_cmd(&mut sa, &["EXISTS", "p1"]).await,
        RespValue::Integer(0)
    );
    assert_eq!(as_bulk(&send_cmd(&mut sb, &["GET", "p1"]).await), "a");
    assert_eq!(as_bulk(&send_cmd(&mut sa, &["GET", "p2"]).await), "b");
    assert_eq!(as_bulk(&send_cmd(&mut sa, &["GET", "p3"]).await), "c");

    // Drop inject (guard end of scope) — retry leftovers completes.
    drop(_inj);
    assert!(is_ok(
        &send_cmd(
            &mut sa,
            &[
                "MIGRATE",
                "127.0.0.1",
                &port_b,
                "",
                "0",
                "2000",
                "KEYS",
                "p2",
                "p3",
            ]
        )
        .await
    ));
    assert_eq!(
        send_cmd(&mut sa, &["EXISTS", "p2", "p3"]).await,
        RespValue::Integer(0)
    );
    assert_eq!(as_bulk(&send_cmd(&mut sb, &["GET", "p2"]).await), "b");
    assert_eq!(as_bulk(&send_cmd(&mut sb, &["GET", "p3"]).await), "c");

    shutdown(pair).await;
}

/// Batch DQ/DT: string TTL via absolute SET PXAT (preserves end time).
#[tokio::test(flavor = "multi_thread")]
async fn migrate_string_with_ttl() {
    let pair = spawn_standalone_pair(16920, 16921).await;
    let mut sa = TcpStream::connect(("127.0.0.1", pair.port_a)).await.unwrap();
    let mut sb = TcpStream::connect(("127.0.0.1", pair.port_b)).await.unwrap();

    assert!(is_ok(
        &send_cmd(&mut sa, &["SET", "sttl", "v", "PX", "60000"]).await
    ));
    let source_abs = match send_cmd(&mut sa, &["PEXPIRETIME", "sttl"]).await {
        RespValue::Integer(n) => n,
        other => panic!("expected PEXPIRETIME integer, got {:?}", other),
    };
    assert!(source_abs > 0, "source absolute expire missing");

    let port_b = pair.port_b.to_string();
    assert!(is_ok(
        &send_cmd(
            &mut sa,
            &["MIGRATE", "127.0.0.1", &port_b, "sttl", "0", "2000"]
        )
        .await
    ));

    let dest_abs = match send_cmd(&mut sb, &["PEXPIRETIME", "sttl"]).await {
        RespValue::Integer(n) => n,
        other => panic!("expected integer PEXPIRETIME, got {:?}", other),
    };
    // Absolute end time preserved within a small clock/skew budget (not remaining-ms shrink).
    assert!(
        (dest_abs - source_abs).abs() <= 2_000,
        "dest absolute expire drifted: source={} dest={}",
        source_abs,
        dest_abs
    );
    let pttl = match send_cmd(&mut sb, &["PTTL", "sttl"]).await {
        RespValue::Integer(n) => n,
        other => panic!("expected integer PTTL, got {:?}", other),
    };
    assert!(
        pttl > 50_000 && pttl <= 60_000,
        "dest PTTL out of range: {}",
        pttl
    );
    assert_eq!(as_bulk(&send_cmd(&mut sb, &["GET", "sttl"]).await), "v");

    shutdown(pair).await;
}

/// Batch DQ/DT: hash typed TTL via trailing PEXPIREAT.
#[tokio::test(flavor = "multi_thread")]
async fn migrate_hash_with_ttl() {
    let pair = spawn_standalone_pair(16922, 16923).await;
    let mut sa = TcpStream::connect(("127.0.0.1", pair.port_a)).await.unwrap();
    let mut sb = TcpStream::connect(("127.0.0.1", pair.port_b)).await.unwrap();

    match send_cmd(&mut sa, &["HSET", "httl", "f", "1"]).await {
        RespValue::Integer(n) => assert_eq!(n, 1),
        other => panic!("HSET failed: {:?}", other),
    }
    assert_eq!(
        send_cmd(&mut sa, &["PEXPIRE", "httl", "45000"]).await,
        RespValue::Integer(1)
    );
    let source_abs = match send_cmd(&mut sa, &["PEXPIRETIME", "httl"]).await {
        RespValue::Integer(n) => n,
        other => panic!("expected PEXPIRETIME integer, got {:?}", other),
    };

    let port_b = pair.port_b.to_string();
    assert!(is_ok(
        &send_cmd(
            &mut sa,
            &["MIGRATE", "127.0.0.1", &port_b, "httl", "0", "2000"]
        )
        .await
    ));

    assert_eq!(
        send_cmd(&mut sa, &["EXISTS", "httl"]).await,
        RespValue::Integer(0)
    );
    assert_eq!(as_bulk(&send_cmd(&mut sb, &["HGET", "httl", "f"]).await), "1");
    let dest_abs = match send_cmd(&mut sb, &["PEXPIRETIME", "httl"]).await {
        RespValue::Integer(n) => n,
        other => panic!("expected integer PEXPIRETIME, got {:?}", other),
    };
    assert!(
        (dest_abs - source_abs).abs() <= 2_000,
        "hash absolute expire drifted: source={} dest={}",
        source_abs,
        dest_abs
    );
    let pttl = match send_cmd(&mut sb, &["PTTL", "httl"]).await {
        RespValue::Integer(n) => n,
        other => panic!("expected integer PTTL, got {:?}", other),
    };
    assert!(
        pttl > 35_000 && pttl <= 45_000,
        "dest hash PTTL out of range: {}",
        pttl
    );

    shutdown(pair).await;
}

/// Batch DQ/DT: list typed TTL via absolute PEXPIREAT.
#[tokio::test(flavor = "multi_thread")]
async fn migrate_list_with_ttl() {
    let pair = spawn_standalone_pair(16924, 16925).await;
    let mut sa = TcpStream::connect(("127.0.0.1", pair.port_a)).await.unwrap();
    let mut sb = TcpStream::connect(("127.0.0.1", pair.port_b)).await.unwrap();

    match send_cmd(&mut sa, &["RPUSH", "lttl", "a", "b"]).await {
        RespValue::Integer(n) => assert_eq!(n, 2),
        other => panic!("RPUSH failed: {:?}", other),
    }
    assert_eq!(
        send_cmd(&mut sa, &["PEXPIRE", "lttl", "40000"]).await,
        RespValue::Integer(1)
    );
    let source_abs = match send_cmd(&mut sa, &["PEXPIRETIME", "lttl"]).await {
        RespValue::Integer(n) => n,
        other => panic!("expected PEXPIRETIME integer, got {:?}", other),
    };

    let port_b = pair.port_b.to_string();
    assert!(is_ok(
        &send_cmd(
            &mut sa,
            &["MIGRATE", "127.0.0.1", &port_b, "lttl", "0", "2000"]
        )
        .await
    ));

    let dest_abs = match send_cmd(&mut sb, &["PEXPIRETIME", "lttl"]).await {
        RespValue::Integer(n) => n,
        other => panic!("expected integer PEXPIRETIME, got {:?}", other),
    };
    assert!(
        (dest_abs - source_abs).abs() <= 2_000,
        "list absolute expire drifted: source={} dest={}",
        source_abs,
        dest_abs
    );
    let pttl = match send_cmd(&mut sb, &["PTTL", "lttl"]).await {
        RespValue::Integer(n) => n,
        other => panic!("expected integer PTTL, got {:?}", other),
    };
    assert!(
        pttl > 30_000 && pttl <= 40_000,
        "dest list PTTL out of range: {}",
        pttl
    );

    shutdown(pair).await;
}

/// Batch DT: known absolute PEXPIREAT end time survives MIGRATE (not remaining-ms).
#[tokio::test(flavor = "multi_thread")]
async fn migrate_preserves_absolute_pexpireat() {
    let pair = spawn_standalone_pair(16926, 16927).await;
    let mut sa = TcpStream::connect(("127.0.0.1", pair.port_a)).await.unwrap();
    let mut sb = TcpStream::connect(("127.0.0.1", pair.port_b)).await.unwrap();

    // Wall-clock absolute ~90s from now (stable across small delays).
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let abs = now_ms + 90_000;
    assert!(is_ok(
        &send_cmd(&mut sa, &["SET", "abskey", "payload"]).await
    ));
    assert_eq!(
        send_cmd(
            &mut sa,
            &["PEXPIREAT", "abskey", &abs.to_string()]
        )
        .await,
        RespValue::Integer(1)
    );

    // Brief pause simulates processing delay before MIGRATE; remaining-ms would shrink.
    sleep(Duration::from_millis(200)).await;

    let port_b = pair.port_b.to_string();
    assert!(is_ok(
        &send_cmd(
            &mut sa,
            &["MIGRATE", "127.0.0.1", &port_b, "abskey", "0", "2000"]
        )
        .await
    ));

    let dest_abs = match send_cmd(&mut sb, &["PEXPIRETIME", "abskey"]).await {
        RespValue::Integer(n) => n,
        other => panic!("expected PEXPIRETIME, got {:?}", other),
    };
    assert!(
        (dest_abs - abs).abs() <= 2_000,
        "absolute end not preserved: want≈{} got={}",
        abs,
        dest_abs
    );
    assert_eq!(
        as_bulk(&send_cmd(&mut sb, &["GET", "abskey"]).await),
        "payload"
    );

    shutdown(pair).await;
}

/// Cluster: MIGRATE into IMPORTING slot via ASKING.
#[tokio::test(flavor = "multi_thread")]
async fn migrate_cluster_importing_via_asking() {
    let port_a = 16916u16;
    let port_b = 16917u16;
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
    let (shut_a, ra) = watch::channel(false);
    let (shut_b, rb) = watch::channel(false);
    let ha = tokio::spawn(async move {
        let _ = srv_a.run_with_shutdown(ra).await;
    });
    let hb = tokio::spawn(async move {
        let _ = srv_b.run_with_shutdown(rb).await;
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

    let key = "foo";
    let slot = kore::key_hash_slot(key.as_bytes());
    cs_b.reassign_slot(slot, &id_a).unwrap();
    assert!(is_ok(&send_cmd(&mut sa, &["SET", key, "cluster-mig"]).await));

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
            "MIGRATE",
            "127.0.0.1",
            &port_b.to_string(),
            key,
            "0",
            "2000",
        ],
    )
    .await;
    assert!(is_ok(&resp), "cluster MIGRATE failed: {:?}", resp);

    assert!(is_ok(&send_cmd(&mut sb, &["ASKING"]).await));
    assert_eq!(
        as_bulk(&send_cmd(&mut sb, &["GET", key]).await),
        "cluster-mig"
    );

    let _ = shut_a.send(true);
    let _ = shut_b.send(true);
    let _ = ha.await;
    let _ = hb.await;
}

/// Batch GG: zset + TTL migrate via DUMP→RESTORE (ABSTTL on dest).
#[tokio::test(flavor = "multi_thread")]
async fn migrate_zset_with_ttl_dump_restore() {
    let pair = spawn_standalone_pair(16920, 16921).await;
    let mut sa = TcpStream::connect(("127.0.0.1", pair.port_a)).await.unwrap();
    let mut sb = TcpStream::connect(("127.0.0.1", pair.port_b)).await.unwrap();

    assert_eq!(
        send_cmd(&mut sa, &["ZADD", "zk", "1.5", "m1", "2.5", "m2"]).await,
        RespValue::Integer(2)
    );
    assert_eq!(
        send_cmd(&mut sa, &["PEXPIRE", "zk", "60000"]).await,
        RespValue::Integer(1)
    );
    let abs_before = match send_cmd(&mut sa, &["PEXPIRETIME", "zk"]).await {
        RespValue::Integer(n) => n,
        other => panic!("PEXPIRETIME: {:?}", other),
    };
    assert!(abs_before > 0);

    let port_b = pair.port_b.to_string();
    assert!(is_ok(
        &send_cmd(
            &mut sa,
            &["MIGRATE", "127.0.0.1", &port_b, "zk", "0", "3000"]
        )
        .await
    ));

    assert_eq!(
        send_cmd(&mut sa, &["EXISTS", "zk"]).await,
        RespValue::Integer(0)
    );
    assert_eq!(
        send_cmd(&mut sb, &["ZCARD", "zk"]).await,
        RespValue::Integer(2)
    );
    let abs_after = match send_cmd(&mut sb, &["PEXPIRETIME", "zk"]).await {
        RespValue::Integer(n) => n,
        other => panic!("dest PEXPIRETIME: {:?}", other),
    };
    // Wall-clock end preserved within a small RTT/processing window.
    assert!(
        (abs_after - abs_before).abs() <= 2000,
        "ABSTTL drift too large: before={abs_before} after={abs_after}"
    );

    shutdown(pair).await;
}

/// Batch GG: geo still migrates via recreate path (not Redis DUMP wire).
#[tokio::test(flavor = "multi_thread")]
async fn migrate_geo_recreate_path() {
    let pair = spawn_standalone_pair(16922, 16923).await;
    let mut sa = TcpStream::connect(("127.0.0.1", pair.port_a)).await.unwrap();
    let mut sb = TcpStream::connect(("127.0.0.1", pair.port_b)).await.unwrap();

    assert_eq!(
        send_cmd(
            &mut sa,
            &["GEOADD", "cities", "13.361389", "38.115556", "Palermo"]
        )
        .await,
        RespValue::Integer(1)
    );
    let port_b = pair.port_b.to_string();
    assert!(is_ok(
        &send_cmd(
            &mut sa,
            &["MIGRATE", "127.0.0.1", &port_b, "cities", "0", "3000"]
        )
        .await
    ));
    assert_eq!(
        send_cmd(&mut sa, &["EXISTS", "cities"]).await,
        RespValue::Integer(0)
    );
    // TYPE geo reports zset; membership via GEOPOS
    match send_cmd(&mut sb, &["GEOPOS", "cities", "Palermo"]).await {
        RespValue::Array(a) => assert_eq!(a.len(), 1),
        other => panic!("GEOPOS: {:?}", other),
    }

    shutdown(pair).await;
}
