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
        deadlock_ui_port: 0,
            admin_bind: "127.0.0.1".to_string(),
            admin_http_token: String::new(),
            admin_http_user: String::new(),
            admin_http_password: String::new(),
            admin_tls: false,
            admin_tls_cert: String::new(),
            admin_tls_key: String::new(),
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

/// Batch GL: plain on --port and TLS on --tls-port simultaneously.
#[tokio::test(flavor = "multi_thread")]
async fn dual_listener_plain_and_tls() {
    let plain_port = 16610;
    let tls_port = 16611;
    let dir = unique_cert_dir("dual");
    let (cert_path, key_path, cert_der) = write_self_signed_certs(&dir);

    let mut config = base_config(plain_port);
    config.tls = true;
    config.tls_port = tls_port;
    config.tls_cert = cert_path.to_string_lossy().into_owned();
    config.tls_key = key_path.to_string_lossy().into_owned();
    config.validate().expect("dual config");
    let config = Arc::new(config);
    let cache = Cache::new(config.shards, config.maxmemory);
    let server = Server::new(cache, config);
    let server_handle = tokio::spawn(async move {
        let _ = server.run().await;
    });
    sleep(Duration::from_millis(300)).await;

    // Plain PING on --port
    let mut plain = timeout(
        Duration::from_secs(2),
        TcpStream::connect(format!("127.0.0.1:{}", plain_port)),
    )
    .await
    .expect("plain connect timeout")
    .expect("plain connect");
    let resp = send_command_plain(&mut plain, "*1\r\n$4\r\nPING\r\n")
        .await
        .expect("plain PING");
    assert!(
        resp.starts_with(b"+PONG"),
        "plain port should PONG, got {:?}",
        String::from_utf8_lossy(&resp)
    );

    // TLS PING on --tls-port
    let connector = tls_connector_for_cert(&cert_der);
    let tcp = timeout(
        Duration::from_secs(2),
        TcpStream::connect(format!("127.0.0.1:{}", tls_port)),
    )
    .await
    .expect("tls connect timeout")
    .expect("tls connect");
    let server_name = ServerName::try_from("localhost").expect("server name");
    let mut tls = timeout(Duration::from_secs(3), connector.connect(server_name, tcp))
        .await
        .expect("handshake timeout")
        .expect("handshake");
    let resp = send_command_tls(&mut tls, "*1\r\n$4\r\nPING\r\n")
        .await
        .expect("tls PING");
    assert!(
        resp.starts_with(b"+PONG"),
        "tls port should PONG, got {:?}",
        String::from_utf8_lossy(&resp)
    );

    drop(plain);
    drop(tls);
    server_handle.abort();
    sleep(Duration::from_millis(100)).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// Batch GL: mTLS rejects clients without a cert; accepts with client cert.
#[tokio::test(flavor = "multi_thread")]
async fn mtls_requires_client_certificate() {
    use rcgen::{
        BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose,
    };

    let dir = unique_cert_dir("mtls");
    let _ = std::fs::create_dir_all(&dir);

    // CA
    let mut ca_params = CertificateParams::new(vec!["kore-test-ca".into()]).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let ca_path = dir.join("ca.pem");
    std::fs::write(&ca_path, ca_cert.pem()).unwrap();

    // Server cert signed by CA
    let mut server_params =
        CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
    server_params
        .distinguished_name
        .push(DnType::CommonName, "kore-server");
    let server_key = KeyPair::generate().unwrap();
    let server_cert = server_params
        .signed_by(&server_key, &ca_cert, &ca_key)
        .unwrap();
    let server_cert_path = dir.join("server.pem");
    let server_key_path = dir.join("server-key.pem");
    std::fs::write(&server_cert_path, server_cert.pem()).unwrap();
    std::fs::write(&server_key_path, server_key.serialize_pem()).unwrap();

    // Client cert signed by CA
    let mut client_params = CertificateParams::new(vec!["client".into()]).unwrap();
    client_params
        .distinguished_name
        .push(DnType::CommonName, "kore-client");
    let client_key = KeyPair::generate().unwrap();
    let client_cert = client_params
        .signed_by(&client_key, &ca_cert, &ca_key)
        .unwrap();

    let port = 16612;
    let mut config = base_config(port);
    config.tls = true;
    config.tls_auth_clients = true;
    config.tls_cert = server_cert_path.to_string_lossy().into_owned();
    config.tls_key = server_key_path.to_string_lossy().into_owned();
    config.tls_ca = ca_path.to_string_lossy().into_owned();
    config.validate().expect("mtls config");
    let config = Arc::new(config);
    let cache = Cache::new(config.shards, config.maxmemory);
    let server = Server::new(cache, config);
    let server_handle = tokio::spawn(async move {
        let _ = server.run().await;
    });
    sleep(Duration::from_millis(300)).await;

    // Client without cert → must not get a working session (handshake fail or
    // connection drop before/during first command).
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from(ca_cert.der().as_ref().to_vec()))
        .unwrap();
    let no_client = ClientConfig::builder()
        .with_root_certificates(roots.clone())
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(no_client));
    let tcp = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .expect("tcp");
    let name = ServerName::try_from("localhost").unwrap();
    let no_cert = timeout(Duration::from_secs(3), connector.connect(name.clone(), tcp)).await;
    let no_cert_ok = match no_cert {
        Ok(Ok(mut s)) => {
            // Some stacks complete the record layer before rejecting; first app
            // data must not return a clean PONG.
            match send_command_tls(&mut s, "*1\r\n$4\r\nPING\r\n").await {
                Ok(resp) if resp.starts_with(b"+PONG") => false,
                _ => true,
            }
        }
        Ok(Err(_)) | Err(_) => true,
    };
    assert!(
        no_cert_ok,
        "mTLS must reject anonymous client (handshake or first PING)"
    );

    // Client with cert → PING works
    let client_cert_der = CertificateDer::from(client_cert.der().as_ref().to_vec());
    let client_key_der = rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(client_key.serialize_der()),
    );
    let with_client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(vec![client_cert_der], client_key_der)
        .expect("client auth cert");
    let connector = TlsConnector::from(Arc::new(with_client));
    let tcp = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .expect("tcp2");
    let mut stream = timeout(Duration::from_secs(3), connector.connect(name, tcp))
        .await
        .expect("hs timeout")
        .expect("hs with client cert");
    let resp = send_command_tls(&mut stream, "*1\r\n$4\r\nPING\r\n")
        .await
        .expect("PING");
    assert!(
        resp.starts_with(b"+PONG"),
        "got {:?}",
        String::from_utf8_lossy(&resp)
    );

    drop(stream);
    server_handle.abort();
    sleep(Duration::from_millis(100)).await;
    let _ = std::fs::remove_dir_all(&dir);
}
