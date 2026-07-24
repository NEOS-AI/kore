//! Metrics + HEALTH readiness (observability MVP).

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::metrics::{
    collect_snapshot, render_prometheus, run_metrics_server_on_listener, MetricsSnapshot,
};
use kore::persistence::{PersistenceConfig, PersistenceManager};
use kore::protocol::RespValue;
use kore::Cache;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::{timeout, Duration};

fn make_cache() -> Arc<Cache> {
    Cache::new_with_sweep(16, 1024 * 1024 * 50, 500 * 1024 * 1024, false)
}

fn make_config() -> Config {
    Config {
        host: "127.0.0.1".to_string(),
        port: 6379,
        threads: 1,
        shards: 16,
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
        cluster_enabled: false,
            cluster_replica_priority: 100,
            cluster_require_full_coverage: true,
            cluster_allow_reads_when_down: false,
            cluster_announce_ip: String::new(),
            cluster_announce_port: 0,
    unixsocket: String::new(),
            log_format: "text".to_string(),
    }
}

fn unique_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("kore-metrics-{}-{}", label, nanos));
    let _ = std::fs::create_dir_all(&dir);
    dir
}

fn make_persistence(dir: &PathBuf) -> Arc<PersistenceManager> {
    let pconfig = PersistenceConfig {
        dir: dir.clone(),
        dbfilename: "dump.rdb".to_string(),
        appendonly: false,
        appendfilename: "appendonly.aof".to_string(),
        save_rules: vec![],
    };
    PersistenceManager::new(pconfig).unwrap()
}

fn bulk(s: &str) -> RespValue {
    RespValue::BulkString(Some(Bytes::from(s.to_string())))
}

fn cmd(parts: &[&str]) -> RespValue {
    RespValue::Array(parts.iter().map(|p| bulk(p)).collect())
}

fn handle(handler: &mut CommandHandler, value: RespValue) -> RespValue {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async { handler.handle(value).await.unwrap() })
}

fn bulk_str(v: &RespValue) -> String {
    match v {
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        other => panic!("expected bulk string, got {:?}", other),
    }
}

fn simple_str(v: &RespValue) -> String {
    match v {
        RespValue::SimpleString(b) => String::from_utf8_lossy(b).into_owned(),
        other => panic!("expected simple string, got {:?}", other),
    }
}

#[test]
fn info_health_section_present() {
    let mut h = CommandHandler::new(make_cache(), Arc::new(make_config()));
    let resp = handle(&mut h, cmd(&["INFO"]));
    let body = bulk_str(&resp);
    assert!(
        body.contains("# Health"),
        "INFO should include Health section: {}",
        body
    );
    assert!(body.contains("ready:"));
    assert!(body.contains("role:master") || body.contains("role:slave"));
    assert!(body.contains("master_link:"));
    assert!(body.contains("rdb_last_save:"));
    assert!(body.contains("aof:"));
}

#[test]
fn health_full_on_master() {
    let dir = unique_dir("master");
    let p = make_persistence(&dir);
    let mut h = CommandHandler::with_persistence(make_cache(), Arc::new(make_config()), Some(p));

    // Bare HEALTH → OK
    assert_eq!(simple_str(&handle(&mut h, cmd(&["HEALTH"]))), "OK");
    // HEALTH PING → PONG
    assert_eq!(simple_str(&handle(&mut h, cmd(&["HEALTH", "PING"]))), "PONG");

    let full = bulk_str(&handle(&mut h, cmd(&["HEALTH", "FULL"])));
    assert!(full.contains("ready:1"), "master should be ready: {}", full);
    assert!(full.contains("role:master"), "expected master role: {}", full);
    assert!(
        full.contains("master_link:n/a"),
        "master has no master link: {}",
        full
    );
    assert!(full.contains("used_memory:"));
    assert!(full.contains("maxmemory:"));
    assert!(full.contains("rdb_last_save:"));
    assert!(full.contains("aof:0"));
}

#[test]
fn health_full_replica_link_down() {
    let dir = unique_dir("replica");
    let p = make_persistence(&dir);
    // Configure as replica without an active master connection → link down
    p.replication
        .set_replicaof(Some("127.0.0.1:6399".to_string()));

    let mut h = CommandHandler::with_persistence(make_cache(), Arc::new(make_config()), Some(p));
    let full = bulk_str(&handle(&mut h, cmd(&["HEALTH", "FULL"])));

    assert!(
        full.contains("role:slave") || full.contains("role:replica"),
        "expected replica role: {}",
        full
    );
    assert!(
        full.contains("master_link:down"),
        "unconnected replica link should be down: {}",
        full
    );
    assert!(
        full.contains("ready:0"),
        "replica with link down should not be ready: {}",
        full
    );
}

#[test]
fn prometheus_text_contains_core_series() {
    let snap = MetricsSnapshot {
        connected_clients: 3,
        total_connections: 7,
        commands_processed_total: 99,
        keyspace_hits_total: 40,
        keyspace_misses_total: 2,
        used_memory_bytes: 2048,
        maxmemory_bytes: 4096,
        connected_replicas: 0,
        master_repl_offset: 12,
        replica_link_up: -1,
        rdb_last_save_timestamp_seconds: 0,
    };
    let text = render_prometheus(&snap);
    for name in [
        "kore_connected_clients",
        "kore_total_connections",
        "kore_commands_processed_total",
        "kore_keyspace_hits_total",
        "kore_keyspace_misses_total",
        "kore_used_memory_bytes",
        "kore_maxmemory_bytes",
        "kore_connected_replicas",
        "kore_master_repl_offset",
        "kore_replica_link_up",
        "kore_rdb_last_save_timestamp_seconds",
    ] {
        assert!(text.contains(name), "missing series {}", name);
        assert!(
            text.contains(&format!("# TYPE {} ", name)),
            "missing TYPE for {}",
            name
        );
    }
    assert!(text.contains("kore_connected_clients 3\n"));
    assert!(text.contains("kore_commands_processed_total 99\n"));
    assert!(text.contains("kore_replica_link_up -1\n"));
}

async fn metrics_http_exchange(port: u16, request_line: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let req = format!(
        "{}\r\nHost: 127.0.0.1:{}\r\nConnection: close\r\n\r\n",
        request_line, port
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    timeout(Duration::from_secs(3), stream.read_to_end(&mut buf))
        .await
        .expect("timeout reading metrics")
        .expect("read metrics");
    String::from_utf8_lossy(&buf).into_owned()
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_http_endpoint_integration() {
    let cache = make_cache();
    // Seed some stats so the scrape is non-zero
    cache.stats.incr_connections();
    cache.stats.hits.store(5, std::sync::atomic::Ordering::Relaxed);
    cache.stats.cmd_get.store(3, std::sync::atomic::Ordering::Relaxed);

    let databases = kore::Databases::single(cache);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Bind once and hand the listener to the server (no probe-then-rebind TOCTOU).
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let dbs = databases.clone();
    let server = tokio::spawn(async move {
        run_metrics_server_on_listener(listener, dbs, None, shutdown_rx)
            .await
            .unwrap();
    });

    let resp = metrics_http_exchange(port, "GET /metrics HTTP/1.1").await;
    assert!(
        resp.starts_with("HTTP/1.1 200"),
        "expected 200: {}",
        &resp[..resp.len().min(200)]
    );
    assert!(resp.contains("kore_connected_clients"));
    assert!(resp.contains("kore_commands_processed_total"));
    assert!(resp.contains("kore_keyspace_hits_total"));
    assert!(resp.contains("kore_used_memory_bytes"));

    // Non-GET on known path → 405 + Allow: GET
    let resp = metrics_http_exchange(port, "POST /metrics HTTP/1.1").await;
    assert!(
        resp.starts_with("HTTP/1.1 405"),
        "expected 405: {}",
        &resp[..resp.len().min(200)]
    );
    assert!(
        resp.to_ascii_lowercase().contains("allow: get"),
        "missing Allow: GET: {}",
        resp
    );

    // Unknown path → 404
    let resp = metrics_http_exchange(port, "GET /nope HTTP/1.1").await;
    assert!(
        resp.starts_with("HTTP/1.1 404"),
        "expected 404: {}",
        &resp[..resp.len().min(200)]
    );

    // Also verify collect_snapshot sees seeded stats
    let snap = collect_snapshot(&databases.db0(), None);
    assert!(snap.total_connections >= 1);
    assert_eq!(snap.keyspace_hits_total, 5);
    assert_eq!(snap.commands_processed_total, 3);

    let _ = shutdown_tx.send(true);
    let _ = timeout(Duration::from_secs(2), server).await;
}

#[test]
fn health_requires_auth_when_password_set() {
    let mut cfg = make_config();
    cfg.auth = "s3cret".to_string();
    let mut h = CommandHandler::new(make_cache(), Arc::new(cfg));
    match handle(&mut h, cmd(&["HEALTH"])) {
        RespValue::Error(e) => {
            assert!(
                String::from_utf8_lossy(&e).contains("NOAUTH"),
                "expected NOAUTH, got {:?}",
                e
            );
        }
        other => panic!("expected NOAUTH error, got {:?}", other),
    }
    // After AUTH, HEALTH works
    assert_eq!(
        simple_str(&handle(&mut h, cmd(&["AUTH", "s3cret"]))),
        "OK"
    );
    assert_eq!(simple_str(&handle(&mut h, cmd(&["HEALTH"]))), "OK");
}
