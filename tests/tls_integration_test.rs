//! Optional TLS for client connections (Phase D security MVP).

use kore::network::load_tls_acceptor;
use kore::{Cache, Config, Server};
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Duration};
use tokio_rustls::TlsConnector;

/// Generate a self-signed cert/key pair under `dir` for tests.
fn write_self_signed_certs(dir: &Path) -> (PathBuf, PathBuf, Vec<u8>) {
    let _ = std::fs::create_dir_all(dir);
    let certified = rcgen::generate_simple_self_signed(vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ])
    .expect("generate self-signed cert");

    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, certified.cert.pem()).expect("write cert");
    std::fs::write(&key_path, certified.key_pair.serialize_pem()).expect("write key");

    let cert_der = certified.cert.der().as_ref().to_vec();
    (cert_path, key_path, cert_der)
}

fn unique_cert_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("kore-tls-{}-{}", label, nanos))
}

fn base_config(port: u16) -> Config {
    Config {
        host: "127.0.0.1".to_string(),
        port,
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
        save: String::new(),
        maxmemory_policy: "allkeys-lru".to_string(),
        databases: 16,
        metrics_port: 0,
        tls: false,
        tls_cert: String::new(),
        tls_key: String::new(),
        aclfile: String::new(),
        cluster_enabled: false,
    unixsocket: String::new(),
            log_format: "text".to_string(),
    }
}

async fn send_command_plain(stream: &mut TcpStream, command: &str) -> Result<Vec<u8>, String> {
    timeout(Duration::from_secs(3), stream.write_all(command.as_bytes()))
        .await
        .map_err(|_| "Timeout writing command".to_string())?
        .map_err(|e| format!("Failed to write command: {}", e))?;

    let mut buffer = vec![0u8; 4096];
    let n = timeout(Duration::from_secs(3), stream.read(&mut buffer))
        .await
        .map_err(|_| "Timeout reading response".to_string())?
        .map_err(|e| format!("Failed to read response: {}", e))?;

    if n == 0 {
        return Err("Connection closed".to_string());
    }
    buffer.truncate(n);
    Ok(buffer)
}

async fn send_command_tls<S>(stream: &mut S, command: &str) -> Result<Vec<u8>, String>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    timeout(Duration::from_secs(3), stream.write_all(command.as_bytes()))
        .await
        .map_err(|_| "Timeout writing command".to_string())?
        .map_err(|e| format!("Failed to write command: {}", e))?;

    let mut buffer = vec![0u8; 4096];
    let n = timeout(Duration::from_secs(3), stream.read(&mut buffer))
        .await
        .map_err(|_| "Timeout reading response".to_string())?
        .map_err(|e| format!("Failed to read response: {}", e))?;

    if n == 0 {
        return Err("Connection closed".to_string());
    }
    buffer.truncate(n);
    Ok(buffer)
}

fn tls_connector_for_cert(cert_der: &[u8]) -> TlsConnector {
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from(cert_der.to_vec()))
        .expect("add test root cert");
    let client_config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    TlsConnector::from(Arc::new(client_config))
}

#[test]
fn config_tls_requires_cert_key() {
    let mut cfg = base_config(16600);
    cfg.tls = true;
    // Missing cert and key
    let err = cfg.validate().expect_err("tls without cert/key must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("tls-cert") || msg.contains("TLS"),
        "unexpected error: {}",
        msg
    );

    cfg.tls_cert = "/nonexistent/cert.pem".to_string();
    cfg.tls_key = "/nonexistent/key.pem".to_string();
    let err = cfg.validate().expect_err("missing files must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("not found") || msg.contains("certificate") || msg.contains("TLS"),
        "unexpected error: {}",
        msg
    );

    // Valid files pass config validation (PEM validity checked at Server start)
    let dir = unique_cert_dir("validate");
    let (cert, key, _) = write_self_signed_certs(&dir);
    cfg.tls_cert = cert.to_string_lossy().into_owned();
    cfg.tls_key = key.to_string_lossy().into_owned();
    cfg.validate()
        .expect("valid cert/key paths should pass validate");

    // load_tls_acceptor should succeed with valid PEM
    load_tls_acceptor(&cfg.tls_cert, &cfg.tls_key).expect("load valid cert/key");

    // Invalid PEM content fails at load time
    let bad_cert = dir.join("bad_cert.pem");
    let bad_key = dir.join("bad_key.pem");
    std::fs::write(&bad_cert, "not a cert").unwrap();
    std::fs::write(&bad_key, "not a key").unwrap();
    assert!(
        load_tls_acceptor(bad_cert.to_str().unwrap(), bad_key.to_str().unwrap()).is_err(),
        "invalid PEM must fail load_tls_acceptor"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn tls_ping_set_get() {
    let port = 16601;
    let dir = unique_cert_dir("ping");
    let (cert_path, key_path, cert_der) = write_self_signed_certs(&dir);

    let mut config = base_config(port);
    config.tls = true;
    config.tls_cert = cert_path.to_string_lossy().into_owned();
    config.tls_key = key_path.to_string_lossy().into_owned();
    config.validate().expect("config");

    let config = Arc::new(config);
    let cache = Cache::new(config.shards, config.maxmemory);
    let server = Server::new(cache, config.clone());

    let server_handle = tokio::spawn(async move {
        let _ = server.run().await;
    });

    sleep(Duration::from_millis(250)).await;

    let connector = tls_connector_for_cert(&cert_der);
    let tcp = timeout(
        Duration::from_secs(2),
        TcpStream::connect(format!("127.0.0.1:{}", port)),
    )
    .await
    .expect("connect timeout")
    .expect("connect");

    let server_name = ServerName::try_from("localhost").expect("server name");
    let mut stream = timeout(Duration::from_secs(3), connector.connect(server_name, tcp))
        .await
        .expect("tls handshake timeout")
        .expect("tls handshake");

    let response = send_command_tls(&mut stream, "*1\r\n$4\r\nPING\r\n")
        .await
        .expect("PING");
    assert!(
        response.starts_with(b"+PONG"),
        "expected PONG, got {:?}",
        String::from_utf8_lossy(&response)
    );

    let response = send_command_tls(
        &mut stream,
        "*3\r\n$3\r\nSET\r\n$7\r\ntls-key\r\n$9\r\ntls-value\r\n",
    )
    .await
    .expect("SET");
    assert!(
        response.starts_with(b"+OK"),
        "SET: {:?}",
        String::from_utf8_lossy(&response)
    );

    let response = send_command_tls(&mut stream, "*2\r\n$3\r\nGET\r\n$7\r\ntls-key\r\n")
        .await
        .expect("GET");
    assert!(
        String::from_utf8_lossy(&response).contains("tls-value"),
        "GET: {:?}",
        String::from_utf8_lossy(&response)
    );

    drop(stream);
    server_handle.abort();
    sleep(Duration::from_millis(100)).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn tls_reject_plaintext_when_tls_enabled() {
    let port = 16602;
    let dir = unique_cert_dir("reject");
    let (cert_path, key_path, _) = write_self_signed_certs(&dir);

    let mut config = base_config(port);
    config.tls = true;
    config.tls_cert = cert_path.to_string_lossy().into_owned();
    config.tls_key = key_path.to_string_lossy().into_owned();
    let config = Arc::new(config);
    let cache = Cache::new(config.shards, config.maxmemory);
    let server = Server::new(cache, config);

    let server_handle = tokio::spawn(async move {
        let _ = server.run().await;
    });

    sleep(Duration::from_millis(250)).await;

    let mut stream = timeout(
        Duration::from_secs(2),
        TcpStream::connect(format!("127.0.0.1:{}", port)),
    )
    .await
    .expect("connect timeout")
    .expect("connect");

    // Plaintext RESP PING over a TLS-only listener must not yield a valid PONG.
    let result = send_command_plain(&mut stream, "*1\r\n$4\r\nPING\r\n").await;
    match result {
        Ok(resp) => {
            assert!(
                !resp.starts_with(b"+PONG"),
                "plaintext must not receive PONG when TLS enabled; got {:?}",
                String::from_utf8_lossy(&resp)
            );
        }
        Err(_) => {
            // Connection closed / timeout / read error is also acceptable.
        }
    }

    drop(stream);
    server_handle.abort();
    sleep(Duration::from_millis(100)).await;
    let _ = std::fs::remove_dir_all(&dir);
}
