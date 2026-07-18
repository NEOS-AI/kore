//! Batch Y: Redlock remote RESP backends against live Kore servers.

use bytes::Bytes;
use kore::config::Config;
use kore::{Cache, LockBackend, Redlock, RespBackend, Server};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::{sleep, timeout, Duration};

fn make_config(port: u16) -> Arc<Config> {
    Arc::new(Config {
        host: "127.0.0.1".to_string(),
        port,
        threads: 1,
        shards: 8,
        maxmemory: 1024 * 1024 * 32,
        evict: true,
        autosweep: false,
        loadfactor: 0.75,
        maxconns: 50,
        auth: String::new(),
        maxentrysize: 500 * 1024 * 1024,
        verbosity: 0,
        enable_redlock: false,
        redlock_instances: String::new(),
        redlock_retry_count: 3,
        redlock_retry_delay_ms: 50,
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
        tls: false,
        tls_cert: String::new(),
        tls_key: String::new(),
        aclfile: String::new(),
        cluster_enabled: false,
        unixsocket: String::new(),
            log_format: "text".to_string(),
    })
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

#[tokio::test(flavor = "multi_thread")]
async fn remote_resp_backends_mutual_exclusion() {
    let ports = [16801u16, 16802, 16803];
    let mut shutdowns = Vec::new();
    let mut handles = Vec::new();

    for &port in &ports {
        let cache = Cache::new_with_sweep(8, 32 * 1024 * 1024, 1024 * 1024, false);
        let srv = Server::new(cache, make_config(port));
        let (tx, rx) = watch::channel(false);
        shutdowns.push(tx);
        handles.push(tokio::spawn(async move {
            let _ = srv.run_with_shutdown(rx).await;
        }));
        wait_listen(port).await;
    }

    let addrs: Vec<String> = ports.iter().map(|p| format!("127.0.0.1:{}", p)).collect();
    let backends = RespBackend::from_addrs(&addrs);
    assert_eq!(backends.len(), 3);

    let redlock = Redlock::with_backend_config(backends, 3, 50, 0.01).unwrap();

    // Run lock acquisition on a blocking thread (Redlock uses std::thread::sleep).
    let redlock2 = redlock.clone();
    let lock = timeout(Duration::from_secs(5), tokio::task::spawn_blocking(move || {
        redlock2
            .lock("remote-res", Bytes::from("client-a"), 10_000)
            .expect("acquire")
    }))
    .await
    .expect("timeout")
    .expect("join");

    let redlock4 = redlock.clone();
    let contested = timeout(Duration::from_secs(5), tokio::task::spawn_blocking(move || {
        // Short TTL + few retries → fail while client-a still holds the lock
        redlock4.lock("remote-res", Bytes::from("client-b"), 200)
    }))
    .await
    .expect("timeout")
    .expect("join");
    assert!(contested.is_err(), "second client must not take held lock");

    drop(lock);

    let redlock5 = redlock.clone();
    let after = timeout(Duration::from_secs(5), tokio::task::spawn_blocking(move || {
        redlock5
            .lock("remote-res", Bytes::from("client-c"), 5_000)
            .expect("acquire after release")
    }))
    .await
    .expect("timeout")
    .expect("join");
    drop(after);

    // Direct backend sanity: SET NX over RESP
    let b = RespBackend::new(format!("127.0.0.1:{}", ports[0]));
    let key = Bytes::from_static(b"lock:direct");
    assert!(b.try_acquire(&key, &Bytes::from_static(b"v1"), 3000));
    assert!(!b.try_acquire(&key, &Bytes::from_static(b"v2"), 3000));
    b.release_if_equal(&key, &Bytes::from_static(b"v1"));
    assert!(b.try_acquire(&key, &Bytes::from_static(b"v2"), 3000));

    for tx in shutdowns {
        let _ = tx.send(true);
    }
    for h in handles {
        let _ = h.await;
    }
}

#[test]
fn from_config_builds_remote_backends_when_none_injected() {
    let mut c = Config::default();
    c.enable_redlock = true;
    c.redlock_instances = "127.0.0.1:16811,127.0.0.1:16812,127.0.0.1:16813".into();
    c.redlock_retry_count = 2;
    c.redlock_retry_delay_ms = 30;
    c.validate().unwrap();

    let redlock = Redlock::from_config(&c, None)
        .unwrap()
        .expect("remote backends");
    assert_eq!(redlock.instance_count(), 3);
    assert_eq!(redlock.quorum, 2);
    // Unreachable instances → soft fail, no panic
    let r = redlock.lock("x", Bytes::from("v"), 100);
    assert!(r.is_err());
}
