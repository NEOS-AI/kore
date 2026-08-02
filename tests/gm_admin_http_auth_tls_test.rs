//! Batch GM: admin HTTP auth (Bearer / Basic) + TLS for metrics/deadlock UI.

use kore::admin_http::AdminHttpOptions;
use kore::metrics::run_metrics_server_on_listener;
use kore::network::load_tls_acceptor;
use kore::Databases;
use kore::{Cache, DeadlockDetector};
use kore::deadlock_ui::run_deadlock_ui_server_on_listener;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::timeout;
use tokio_rustls::rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
use tokio_rustls::TlsConnector;

fn make_cache() -> Arc<Cache> {
    Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false)
}

async fn http_exchange(port: u16, request_line: &str, extra_headers: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let req = format!(
        "{}\r\nHost: 127.0.0.1:{}\r\n{}Connection: close\r\n\r\n",
        request_line, port, extra_headers
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    timeout(Duration::from_secs(3), stream.read_to_end(&mut buf))
        .await
        .expect("timeout")
        .unwrap();
    String::from_utf8_lossy(&buf).into_owned()
}

#[tokio::test(flavor = "multi_thread")]
async fn gm_metrics_bearer_auth() {
    let databases = Databases::single(make_cache());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let mut opts = AdminHttpOptions::default();
    opts.token = "topsecret".into();

    let dbs = databases.clone();
    let server = tokio::spawn(async move {
        run_metrics_server_on_listener(listener, dbs, None, shutdown_rx, opts)
            .await
            .unwrap();
    });

    // No auth → 401
    let resp = http_exchange(port, "GET /metrics HTTP/1.1", "").await;
    assert!(resp.starts_with("HTTP/1.1 401"), "got {}", &resp[..80.min(resp.len())]);
    assert!(resp.to_ascii_lowercase().contains("www-authenticate"));

    // Wrong token → 401
    let resp = http_exchange(
        port,
        "GET /metrics HTTP/1.1",
        "Authorization: Bearer wrong\r\n",
    )
    .await;
    assert!(resp.starts_with("HTTP/1.1 401"), "{}", &resp[..80.min(resp.len())]);

    // Correct token → 200
    let resp = http_exchange(
        port,
        "GET /metrics HTTP/1.1",
        "Authorization: Bearer topsecret\r\n",
    )
    .await;
    assert!(resp.starts_with("HTTP/1.1 200"), "{}", &resp[..120.min(resp.len())]);
    assert!(resp.contains("kore_connected_clients"));

    let _ = shutdown_tx.send(true);
    let _ = timeout(Duration::from_secs(2), server).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn gm_deadlock_ui_basic_auth() {
    let det = Arc::new(DeadlockDetector::new(30_000, false));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let mut opts = AdminHttpOptions::default();
    opts.basic_user = "admin".into();
    opts.basic_password = "pw".into();

    let d = det.clone();
    let server = tokio::spawn(async move {
        run_deadlock_ui_server_on_listener(listener, Some(d), shutdown_rx, opts)
            .await
            .unwrap();
    });

    let resp = http_exchange(port, "GET /api/deadlock HTTP/1.1", "").await;
    assert!(resp.starts_with("HTTP/1.1 401"), "{}", &resp[..80.min(resp.len())]);

    // admin:pw → YWRtaW46cHc=
    let resp = http_exchange(
        port,
        "GET /api/deadlock HTTP/1.1",
        "Authorization: Basic YWRtaW46cHc=\r\n",
    )
    .await;
    assert!(resp.starts_with("HTTP/1.1 200"), "{}", &resp[..120.min(resp.len())]);
    assert!(resp.contains("\"status\""));

    let _ = shutdown_tx.send(true);
    let _ = timeout(Duration::from_secs(2), server).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn gm_metrics_tls_with_token() {
    // Generate ephemeral cert with rcgen (dev-dep).
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let dir = tempfile_dir();
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.key_pair.serialize_pem()).unwrap();

    let acceptor = load_tls_acceptor(
        cert_path.to_str().unwrap(),
        key_path.to_str().unwrap(),
    )
    .unwrap();

    let databases = Databases::single(make_cache());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let mut opts = AdminHttpOptions::default();
    opts.token = "tls-token".into();
    opts.tls = Some(acceptor);

    let dbs = databases.clone();
    let server = tokio::spawn(async move {
        run_metrics_server_on_listener(listener, dbs, None, shutdown_rx, opts)
            .await
            .unwrap();
    });

    // Build client trusting this self-signed cert.
    let mut roots = RootCertStore::empty();
    let cert_der = cert.cert.der().clone();
    roots.add(cert_der).unwrap();
    let client_cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(client_cfg));

    let tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let server_name = ServerName::try_from("localhost").unwrap();
    let mut tls = connector.connect(server_name, tcp).await.unwrap();

    let req = format!(
        "GET /metrics HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer tls-token\r\nConnection: close\r\n\r\n"
    );
    tls.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    timeout(Duration::from_secs(3), tls.read_to_end(&mut buf))
        .await
        .expect("timeout")
        .unwrap();
    let resp = String::from_utf8_lossy(&buf);
    assert!(
        resp.starts_with("HTTP/1.1 200"),
        "tls metrics: {}",
        &resp[..resp.len().min(200)]
    );
    assert!(resp.contains("kore_connected_clients"));

    let _ = shutdown_tx.send(true);
    let _ = timeout(Duration::from_secs(2), server).await;
    let _ = std::fs::remove_dir_all(&dir);
}

fn tempfile_dir() -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("kore-gm-tls-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

#[test]
fn gm_config_rejects_non_loopback_without_auth() {
    let mut c = kore::Config::default();
    c.metrics_port = 9121;
    c.admin_bind = "0.0.0.0".into();
    // Need enough other fields valid for validate()
    c.shards = 16;
    c.maxentrysize = 1024 * 1024;
    let err = c.validate().unwrap_err().to_string();
    assert!(
        err.contains("admin-bind") || err.contains("auth"),
        "{}",
        err
    );

    c.admin_http_token = "ok".into();
    // May still fail on other things (save etc) — token should clear the bind check.
    // Ensure bind+auth alone is not the failure reason.
    match c.validate() {
        Ok(()) => {}
        Err(e) => {
            let s = e.to_string();
            assert!(
                !s.contains("admin-bind") && !s.contains("non-loopback"),
                "unexpected bind error: {}",
                s
            );
        }
    }
}

#[test]
fn gm_config_basic_user_password_paired() {
    let mut c = kore::Config::default();
    c.shards = 16;
    c.maxentrysize = 1024 * 1024;
    c.admin_http_user = "u".into();
    // password empty
    let err = c.validate().unwrap_err().to_string();
    assert!(err.contains("admin-http-user") || err.contains("password"), "{}", err);
}
