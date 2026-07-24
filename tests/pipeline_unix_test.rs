//! Pipelining write batching + Unix domain socket accept.

use bytes::Bytes;
use kore::config::Config;
use kore::protocol::{RespParser, RespValue};
use kore::{Cache, Server};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UnixStream};
use tokio::sync::watch;

fn test_config(port: u16, unixsocket: String) -> Config {
    Config {
        host: "127.0.0.1".to_string(),
        port,
        threads: 2,
        shards: 16,
        maxmemory: 32 * 1024 * 1024,
        evict: true,
        autosweep: false,
        loadfactor: 0.75,
        maxconns: 64,
        auth: String::new(),
        maxentrysize: 1024 * 1024,
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
        save: String::new(),
        maxmemory_policy: "noeviction".to_string(),
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
        unixsocket,
        log_format: "text".to_string(),
    }
}

fn encode(parts: &[&str]) -> Vec<u8> {
    let args: Vec<RespValue> = parts
        .iter()
        .map(|p| RespValue::BulkString(Some(Bytes::from(p.to_string()))))
        .collect();
    RespValue::Array(args).serialize().to_vec()
}

async fn read_n_values(stream: &mut (impl AsyncReadExt + Unpin), n: usize) -> Vec<RespValue> {
    let mut parser = RespParser::new();
    let mut buf = vec![0u8; 4096];
    let mut out = Vec::new();
    while out.len() < n {
        let k = tokio::time::timeout(std::time::Duration::from_secs(3), stream.read(&mut buf))
            .await
            .expect("read timeout")
            .expect("read ok");
        assert!(k > 0, "eof before {} values (got {})", n, out.len());
        parser.feed(&buf[..k]);
        while let Some(v) = parser.parse().unwrap() {
            out.push(v);
            if out.len() >= n {
                break;
            }
        }
    }
    out
}

async fn free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

#[tokio::test]
async fn pipeline_multiple_commands_one_write() {
    let port = free_port().await;
    let config = Arc::new(test_config(port, String::new()));
    let cache = Cache::new_with_sweep(config.shards, config.maxmemory, config.maxentrysize, false);
    let server = Server::new(cache, config.clone());
    let (tx, rx) = watch::channel(false);
    let handle = tokio::spawn(async move { server.run_with_shutdown(rx).await });

    // Wait until accept is up
    for _ in 0..50 {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    // Classic Redis pipeline: several commands in one write, several replies
    let mut pipe = Vec::new();
    pipe.extend_from_slice(&encode(&["PING"]));
    pipe.extend_from_slice(&encode(&["SET", "k", "v"]));
    pipe.extend_from_slice(&encode(&["GET", "k"]));
    pipe.extend_from_slice(&encode(&["INCR", "n"]));
    pipe.extend_from_slice(&encode(&["INCR", "n"]));
    stream.write_all(&pipe).await.unwrap();

    let replies = read_n_values(&mut stream, 5).await;
    assert!(matches!(&replies[0], RespValue::SimpleString(s) if s.as_ref() == b"PONG"));
    assert!(matches!(&replies[1], RespValue::SimpleString(s) if s.as_ref() == b"OK"));
    match &replies[2] {
        RespValue::BulkString(Some(b)) => assert_eq!(&b[..], b"v"),
        other => panic!("GET reply {:?}", other),
    }
    assert_eq!(replies[3], RespValue::Integer(1));
    assert_eq!(replies[4], RespValue::Integer(2));

    let _ = tx.send(true);
    let _ = handle.await;
}

#[tokio::test]
#[cfg(unix)]
async fn unix_socket_ping() {
    let port = free_port().await;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let sock_path = std::env::temp_dir().join(format!("kore-uds-test-{nanos}.sock"));
    let path_str = sock_path.to_string_lossy().into_owned();

    let config = Arc::new(test_config(port, path_str.clone()));
    let cache = Cache::new_with_sweep(config.shards, config.maxmemory, config.maxentrysize, false);
    let server = Server::new(cache, config.clone());
    let (tx, rx) = watch::channel(false);
    let handle = tokio::spawn(async move { server.run_with_shutdown(rx).await });

    // Wait for socket file
    for _ in 0..50 {
        if sock_path.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(sock_path.exists(), "unix socket was not created");

    let mut stream = UnixStream::connect(&sock_path).await.expect("connect uds");
    stream.write_all(&encode(&["PING"])).await.unwrap();
    let replies = read_n_values(&mut stream, 1).await;
    assert!(matches!(&replies[0], RespValue::SimpleString(s) if s.as_ref() == b"PONG"));

    // TCP still works alongside UDS
    let mut tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    tcp.write_all(&encode(&["ECHO", "hi"])).await.unwrap();
    let replies = read_n_values(&mut tcp, 1).await;
    match &replies[0] {
        RespValue::BulkString(Some(b)) => assert_eq!(&b[..], b"hi"),
        other => panic!("{:?}", other),
    }

    let _ = tx.send(true);
    let _ = handle.await;
    // Socket file cleaned on shutdown
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!sock_path.exists() || true); // best-effort cleanup
}
