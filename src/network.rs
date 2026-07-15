use crate::acl::AclStore;
use crate::cache::Cache;
use crate::cluster::ClusterState;
use crate::commands::CommandHandler;
use crate::config::Config;
use crate::databases::Databases;
use crate::persistence::PersistenceManager;
use crate::protocol::{RespParser, RespValue};
use crate::redlock::Redlock;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig as RustlsServerConfig;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch, Semaphore};
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use tracing::{error, info, warn};

pub struct Server {
    databases: Arc<Databases>,
    config: Arc<Config>,
    persistence: Option<Arc<PersistenceManager>>,
    /// Shared ACL store for all connections.
    acl: Arc<AclStore>,
    /// Optional Redlock (wired from `--enable-redlock` CLI flags).
    redlock: Option<Arc<Redlock>>,
    /// Cluster topology when `--cluster-enabled`.
    cluster: Option<Arc<ClusterState>>,
}

const BUFFER_SIZE: usize = 8192;
const RESPONSE_QUEUE_SIZE: usize = 100; // Maximum pending responses per client
const SLOW_CLIENT_THRESHOLD: usize = 80; // Trigger warning at 80% full
const WRITE_TIMEOUT_SECS: u64 = 5; // Timeout for write operations
const MAX_CLIENTS_ERR: &[u8] = b"-ERR max number of clients reached\r\n";

/// Load a rustls server acceptor from PEM cert/key paths.
/// Fails fast if files are missing or contain invalid PEM material.
pub fn load_tls_acceptor(cert_path: &str, key_path: &str) -> anyhow::Result<TlsAcceptor> {
    let cert_file = File::open(cert_path).map_err(|e| {
        anyhow::anyhow!("Failed to open TLS certificate '{}': {}", cert_path, e)
    })?;
    let mut cert_reader = BufReader::new(cert_file);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("Failed to parse TLS certificate '{}': {}", cert_path, e))?;
    if certs.is_empty() {
        anyhow::bail!("No certificates found in '{}'", cert_path);
    }

    let key_file = File::open(key_path)
        .map_err(|e| anyhow::anyhow!("Failed to open TLS private key '{}': {}", key_path, e))?;
    let mut key_reader = BufReader::new(key_file);
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| anyhow::anyhow!("Failed to parse TLS private key '{}': {}", key_path, e))?
        .ok_or_else(|| anyhow::anyhow!("No private key found in '{}'", key_path))?;

    let server_config = RustlsServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| anyhow::anyhow!("Invalid TLS cert/key pair: {}", e))?;

    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

impl Server {
    fn build(
        databases: Arc<Databases>,
        config: Arc<Config>,
        persistence: Option<Arc<PersistenceManager>>,
    ) -> Self {
        let acl = AclStore::from_auth_arc(&config.auth);
        let cluster = if config.cluster_enabled {
            Some(ClusterState::single_node(config.host.clone(), config.port))
        } else {
            None
        };
        Self {
            databases,
            config,
            persistence,
            acl,
            redlock: None,
            cluster,
        }
    }

    /// Single-keyspace server (tests / embeds). Prefer `with_databases` in production.
    pub fn new(cache: Arc<Cache>, config: Arc<Config>) -> Self {
        Self::build(Databases::single(cache), config, None)
    }

    pub fn with_databases(databases: Arc<Databases>, config: Arc<Config>) -> Self {
        Self::build(databases, config, None)
    }

    pub fn with_persistence(
        cache: Arc<Cache>,
        config: Arc<Config>,
        persistence: Arc<PersistenceManager>,
    ) -> Self {
        Self::build(Databases::single(cache), config, Some(persistence))
    }

    pub fn with_databases_and_persistence(
        databases: Arc<Databases>,
        config: Arc<Config>,
        persistence: Arc<PersistenceManager>,
    ) -> Self {
        Self::build(databases, config, Some(persistence))
    }

    /// Attach a Redlock instance constructed from CLI/config flags.
    pub fn with_redlock(mut self, redlock: Option<Arc<Redlock>>) -> Self {
        self.redlock = redlock;
        self
    }

    /// Redlock held by this server, if enabled.
    pub fn redlock(&self) -> Option<&Arc<Redlock>> {
        self.redlock.as_ref()
    }

    /// Override / attach cluster state (tests may inject synthetic topology).
    pub fn with_cluster(mut self, cluster: Option<Arc<ClusterState>>) -> Self {
        self.cluster = cluster;
        self
    }

    /// Cluster state held by this server, if enabled.
    pub fn cluster(&self) -> Option<&Arc<ClusterState>> {
        self.cluster.as_ref()
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let (_tx, rx) = watch::channel(false);
        self.run_with_shutdown(rx).await
    }

    /// Run the accept loop until `shutdown` becomes `true`.
    /// On shutdown: stop accepting, optionally SAVE if persistence is present, then return.
    pub async fn run_with_shutdown(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        let addr = self.config.socket_addr();
        let listener = TcpListener::bind(&addr).await?;

        // Load TLS material once at server start (fail fast on invalid cert/key).
        let tls_acceptor = if self.config.tls {
            let acceptor = load_tls_acceptor(&self.config.tls_cert, &self.config.tls_key)?;
            info!(
                "TLS enabled for client connections (cert={}, key={})",
                self.config.tls_cert, self.config.tls_key
            );
            Some(acceptor)
        } else {
            None
        };

        let cache = self.databases.db0();
        info!("Kore server listening on {}", addr);
        info!("Shards: {}", self.config.shards);
        info!("Databases: {}", self.databases.len());
        info!("Max memory: {} bytes", cache.max_memory());
        info!("Worker threads: {}", self.config.num_threads());
        info!("Max connections: {}", self.config.maxconns);
        if self.cluster.is_some() {
            info!("Cluster mode: enabled (gossip over client RESP; single-observer fail)");
        } else {
            info!("Cluster mode: disabled");
        }

        // Cluster heartbeat / fail detection (RESP PING over client port).
        if let Some(ref cluster) = self.cluster {
            let gossip_cluster = Arc::clone(cluster);
            let gossip_persistence = self.persistence.clone();
            let gossip_shutdown = shutdown.clone();
            tokio::spawn(async move {
                crate::cluster::run_cluster_gossip(
                    gossip_cluster,
                    gossip_persistence,
                    gossip_shutdown,
                )
                .await;
            });
        }

        // Limit concurrent connections with a semaphore (race-free vs post-accept stats check)
        let conn_limit = Arc::new(Semaphore::new(self.config.maxconns));

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("Shutdown signal received — stopping accept loop");
                        break;
                    }
                }
                accept = listener.accept() => {
                    match accept {
                        Ok((mut socket, peer_addr)) => {
                            // Try to reserve a connection slot before spawning the handler
                            let permit = match conn_limit.clone().try_acquire_owned() {
                                Ok(permit) => permit,
                                Err(_) => {
                                    warn!(
                                        "Max connections reached ({}), rejecting connection from {}",
                                        self.config.maxconns, peer_addr
                                    );
                                    // Redis-like error, then close without full handler
                                    let _ = socket.write_all(MAX_CLIENTS_ERR).await;
                                    let _ = socket.shutdown().await;
                                    continue;
                                }
                            };

                            // Disable Nagle before optional TLS wrap
                            if let Err(e) = socket.set_nodelay(true) {
                                warn!("Failed to set TCP_NODELAY for {}: {}", peer_addr, e);
                            }

                            info!("New connection from {}", peer_addr);

                            let databases = self.databases.clone();
                            let config = self.config.clone();
                            let persistence = self.persistence.clone();
                            let acl = self.acl.clone();
                            let cluster = self.cluster.clone();
                            let tls_acceptor = tls_acceptor.clone();

                            // Spawn a new task to handle the connection
                            // Tokio will schedule this on its thread pool (number of threads configured at runtime)
                            // Permit is held for the lifetime of the connection task
                            tokio::spawn(async move {
                                let _permit = permit;
                                let result = if let Some(acceptor) = tls_acceptor {
                                    match acceptor.accept(socket).await {
                                        Ok(tls_stream) => {
                                            handle_connection(
                                                tls_stream,
                                                databases,
                                                config,
                                                persistence,
                                                acl,
                                                cluster,
                                            )
                                            .await
                                        }
                                        Err(e) => Err(anyhow::anyhow!(
                                            "TLS handshake failed: {}",
                                            e
                                        )),
                                    }
                                } else {
                                    handle_connection(
                                        socket,
                                        databases,
                                        config,
                                        persistence,
                                        acl,
                                        cluster,
                                    )
                                    .await
                                };
                                if let Err(e) = result {
                                    warn!("Connection error from {}: {}", peer_addr, e);
                                }
                                info!("Connection closed from {}", peer_addr);
                            });
                        }
                        Err(e) => {
                            error!("Failed to accept connection: {}", e);
                        }
                    }
                }
            }
        }

        // Best-effort multi-DB persistence flush on shutdown
        if let Some(ref p) = self.persistence {
            info!("Saving database before shutdown...");
            match p.save(&self.databases) {
                Ok(()) => info!("Database saved successfully"),
                Err(e) => warn!("Failed to save database on shutdown: {}", e),
            }
        }

        info!("Kore server shut down cleanly");
        Ok(())
    }
}

async fn handle_connection<S>(
    socket: S,
    databases: Arc<Databases>,
    config: Arc<Config>,
    persistence: Option<Arc<PersistenceManager>>,
    acl: Arc<AclStore>,
    cluster: Option<Arc<ClusterState>>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // Track connection statistics / pubsub on DB 0 (shared across keyspaces)
    let cache = databases.db0();
    cache.stats.incr_connections();

    // Register client for pub/sub
    let (client_id, mut pubsub_rx) = cache.pubsub.register_client().await;

    // RESP (REdis Serialization Protocol) parser and command handler
    let mut parser = RespParser::new();
    let mut handler =
        CommandHandler::with_databases_and_acl(databases, config, persistence, acl)
            .with_cluster(cluster);
    handler.set_client_id(client_id);
    let mut buf = vec![0u8; BUFFER_SIZE]; // 8KB buffer

    // Create a channel for sending responses
    let (response_tx, mut response_rx) = mpsc::channel::<Vec<u8>>(RESPONSE_QUEUE_SIZE);

    // Split socket into reader and writer (works for TcpStream and TlsStream)
    let (mut reader, mut writer) = tokio::io::split(socket);

    // Spawn a task to handle outgoing messages (both responses and pub/sub messages)
    let cache_clone = cache.clone();
    let write_task = tokio::spawn(async move {
        let mut slow_client_warnings = 0;

        loop {
            tokio::select! {
                // Handle pub/sub messages
                msg = pubsub_rx.recv() => {
                    match msg {
                        Ok(value) => {
                            // Message left the broadcast buffer — free fan-out accounting.
                            cache_clone.note_pubsub_delivered(client_id).await;

                            let data = value.serialize();

                            // Write with timeout
                            match timeout(Duration::from_secs(WRITE_TIMEOUT_SECS), writer.write_all(&data)).await {
                                Ok(Ok(_)) => {
                                    cache_clone.stats.incr_bytes_sent(data.len());
                                }
                                Ok(Err(e)) => {
                                    warn!("Failed to write pub/sub message: {}", e);
                                    break;
                                }
                                Err(_) => {
                                    warn!("Pub/sub write timeout - slow client detected");
                                    slow_client_warnings += 1;
                                    if slow_client_warnings >= 3 {
                                        warn!("Client too slow, disconnecting");
                                        break;
                                    }
                                }
                            }
                        }
                        // Slow consumer: broadcast ring overwrote unread messages.
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!(
                                "Pub/sub client {} lagged by {} message(s) — disconnecting slow client",
                                client_id, n
                            );
                            let freed = cache_clone.pubsub.note_lagged(client_id, n).await;
                            cache_clone.release_pubsub_memory(freed);
                            break;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
                // Handle command responses
                Some(data) = response_rx.recv() => {
                    // Write with timeout
                    match timeout(Duration::from_secs(WRITE_TIMEOUT_SECS), writer.write_all(&data)).await {
                        Ok(Ok(_)) => {
                            cache_clone.stats.incr_bytes_sent(data.len());
                        }
                        Ok(Err(e)) => {
                            warn!("Failed to write response: {}", e);
                            break;
                        }
                        Err(_) => {
                            warn!("Response write timeout - slow client detected");
                            slow_client_warnings += 1;
                            if slow_client_warnings >= 3 {
                                warn!("Client too slow, disconnecting");
                                break;
                            }
                        }
                    }
                }
            }
        }
    });

    // Main loop for handling client commands
    let result = 'conn: loop {
        // Read data from socket
        let n = match reader.read(&mut buf).await {
            Ok(0) => break Ok(()), // Connection closed
            Ok(n) => n,
            Err(e) => break Err(e.into()),
        };

        // Feed data to parser
        parser.feed(&buf[..n]);

        // Track bytes received
        cache.stats.incr_bytes_received(n);

        // Parse and handle commands
        while let Some(value) = parser.parse()? {
            let response = match handler.handle(value).await {
                Ok(resp) => resp,
                Err(e) => {
                    error!("Command handler error: {}", e);
                    RespValue::error(e.to_resp_string())
                }
            };

            // SYNC / PSYNC: send pre-serialized handshake (+ RDB or CONTINUE+backlog),
            // then stream live write commands to the replica socket while reading
            // REPLCONF ACK replies for catch-up tracking.
            if let Some(raw) = handler.take_raw_response() {
                let feed_rx = handler.take_replica_feed();
                let _ = response_tx.send(raw.to_vec()).await;
                if let Some(mut feed_rx) = feed_rx {
                    info!("Connection entered replica feed mode");
                    let repl: Option<Arc<crate::persistence::replication::ReplicationManager>> =
                        handler
                            .persistence()
                            .map(|p| Arc::clone(&p.replication));
                    let announce_host = handler.replica_announce_ip().map(|s| s.to_string());
                    let announce_port = handler.replica_announce_port();
                    let mut ack_parser = RespParser::new();
                    let mut ack_buf = vec![0u8; 4096];
                    let mut getack_tick =
                        tokio::time::interval(Duration::from_secs(1));
                    getack_tick.set_missed_tick_behavior(
                        tokio::time::MissedTickBehavior::Delay,
                    );
                    // Skip the immediate first tick so we don't probe before the
                    // handshake bytes have left the write queue.
                    getack_tick.tick().await;

                    loop {
                        tokio::select! {
                            cmd = feed_rx.recv() => {
                                match cmd {
                                    Some(cmd_bytes) => {
                                        if response_tx.send(cmd_bytes.to_vec()).await.is_err() {
                                            break;
                                        }
                                    }
                                    None => break,
                                }
                            }
                            read_res = reader.read(&mut ack_buf) => {
                                match read_res {
                                    Ok(0) => break,
                                    Ok(n) => {
                                        ack_parser.feed(&ack_buf[..n]);
                                        loop {
                                            match ack_parser.parse() {
                                                Ok(Some(val)) => {
                                                    if let Some(off) =
                                                        crate::persistence::replication::parse_replconf_ack_offset(
                                                            &val,
                                                        )
                                                    {
                                                        if let Some(ref r) = repl {
                                                            r.note_replica_ack(
                                                                announce_host.as_deref(),
                                                                announce_port,
                                                                off,
                                                            );
                                                        }
                                                    }
                                                }
                                                Ok(None) => break,
                                                Err(e) => {
                                                    warn!("replica feed ACK parse error: {}", e);
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        warn!("replica feed read error: {}", e);
                                        break;
                                    }
                                }
                            }
                            _ = getack_tick.tick() => {
                                // Probe without backlog so master_repl_offset stays
                                // stable for FAILOVER TO catch-up.
                                if let Some(ref r) = repl {
                                    r.send_getack_probe_to_feeds(
                                        announce_host.as_deref(),
                                        announce_port,
                                    );
                                }
                            }
                        }
                    }
                    break 'conn Ok(());
                }
                continue;
            }

            // Send response through channel
            let data = response.serialize().to_vec();

            // Check if response queue is getting full (backpressure detection)
            let current_capacity = response_tx.capacity();
            if current_capacity < (RESPONSE_QUEUE_SIZE - SLOW_CLIENT_THRESHOLD) {
                warn!(
                    "Response queue filling up: {}/{} capacity remaining - slow client detected",
                    current_capacity, RESPONSE_QUEUE_SIZE
                );
            }

            // Try to send response with timeout to avoid blocking on slow clients
            match timeout(Duration::from_millis(100), response_tx.send(data)).await {
                Ok(Ok(_)) => {
                    // Successfully sent
                }
                Ok(Err(_)) => {
                    // Channel closed
                    warn!("Response channel closed");
                    break;
                }
                Err(_) => {
                    // Timeout - client is too slow to accept responses
                    warn!(
                        "Response queue send timeout - client not consuming responses fast enough"
                    );
                    break;
                }
            }
        }
    };

    // Cleanup: unregister client (frees any remaining pub/sub pending memory) and stats
    cache.unregister_pubsub_client(client_id).await;
    cache.stats.decr_active_connections();
    write_task.abort();

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup_test_server() -> (Arc<Cache>, Arc<Config>, String) {
        let config = Arc::new(Config {
            host: "127.0.0.1".to_string(),
            port: 0, // Let OS assign port
            threads: 1,
            shards: 16,
            maxmemory: 1024 * 1024 * 100, // 100MB
            evict: true,
            autosweep: false,
            loadfactor: 0.75,
            maxconns: 100,
            auth: String::new(),
            maxentrysize: 500 * 1024 * 1024, // 500MB
            verbosity: 0,
            enable_redlock: false,
            redlock_instances: String::new(),
            redlock_retry_count: 3,
            redlock_retry_delay_ms: 200,
            dir: "./data".to_string(),
            dbfilename: "dump.rdb".to_string(),
            appendonly: false,
            appendfilename: "appendonly.aof".to_string(),
            replicaof: String::new(),
            save: "900,1 300,10 60,10000".to_string(),
            maxmemory_policy: "allkeys-lru".to_string(),
            databases: 16,
            metrics_port: 0,
            tls: false,
            tls_cert: String::new(),
            tls_key: String::new(),
            cluster_enabled: false,
        });

        let cache = Cache::new(config.shards, config.maxmemory);

        // For testing, we'll use a fixed port
        let test_addr = "127.0.0.1:16379".to_string();

        (cache, config, test_addr)
    }

    #[tokio::test]
    async fn test_ping_pong() {
        let (_cache, _config, _addr) = setup_test_server().await;

        // Note: Full integration tests would require spawning the server
        // This is a placeholder for the test structure
    }
}
