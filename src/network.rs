use crate::acl::AclStore;
use crate::cache::Cache;
use crate::cluster::ClusterState;
use crate::commands::CommandHandler;
use crate::config::Config;
use crate::databases::Databases;
use crate::persistence::PersistenceManager;
use crate::protocol::{RespParser, RespValue};
use crate::redlock::Redlock;
use crate::scripting::ScriptCache;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig as RustlsServerConfig;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::sync::{mpsc, watch, Semaphore};
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error, info, warn};

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
    /// Shared SCRIPT LOAD / EVALSHA cache for all connections.
    script_cache: Arc<ScriptCache>,
}

const BUFFER_SIZE: usize = 8192;
const RESPONSE_QUEUE_SIZE: usize = 256; // Pending response buffers per client
const SLOW_CLIENT_THRESHOLD: usize = 200; // Warn when free capacity drops below this
const WRITE_TIMEOUT_SECS: u64 = 5; // Timeout for write operations
/// Cap coalesced write size so one slow flush cannot hold unbounded memory.
const WRITE_BATCH_MAX_BYTES: usize = 64 * 1024;
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
        if !config.aclfile.is_empty() {
            acl.set_aclfile(&config.aclfile);
            match acl.try_load_on_boot() {
                Ok(true) => info!("Loaded ACL rules from {}", config.aclfile),
                Ok(false) => {}
                Err(e) => warn!("Failed to load ACL file '{}': {}", config.aclfile, e),
            }
        }
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
            script_cache: ScriptCache::shared(),
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

        // Optional Unix domain socket (in addition to TCP).
        #[cfg(unix)]
        let unix_listener = {
            if self.config.unixsocket.is_empty() {
                None
            } else {
                let path = std::path::Path::new(&self.config.unixsocket);
                if path.exists() {
                    // Stale socket from a previous crash — remove so bind succeeds.
                    let _ = std::fs::remove_file(path);
                }
                if let Some(parent) = path.parent() {
                    if !parent.as_os_str().is_empty() {
                        std::fs::create_dir_all(parent).map_err(|e| {
                            anyhow::anyhow!(
                                "Failed to create unix socket directory '{}': {}",
                                parent.display(),
                                e
                            )
                        })?;
                    }
                }
                match UnixListener::bind(path) {
                    Ok(l) => {
                        info!("Kore server listening on unix socket {}", self.config.unixsocket);
                        Some(l)
                    }
                    Err(e) => {
                        return Err(anyhow::anyhow!(
                            "Failed to bind unix socket '{}': {}",
                            self.config.unixsocket,
                            e
                        ));
                    }
                }
            }
        };
        #[cfg(not(unix))]
        let _unix_listener: Option<()> = {
            if !self.config.unixsocket.is_empty() {
                warn!(
                    "--unixsocket is not supported on this platform; ignoring '{}'",
                    self.config.unixsocket
                );
            }
            None
        };

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

        // Track unix path for cleanup on shutdown.
        #[cfg(unix)]
        let unix_path = if self.config.unixsocket.is_empty() {
            None
        } else {
            Some(self.config.unixsocket.clone())
        };

        loop {
            #[cfg(unix)]
            {
                tokio::select! {
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            info!("Shutdown signal received — stopping accept loop");
                            break;
                        }
                    }
                    accept = listener.accept() => {
                        match accept {
                            Ok((socket, peer_addr)) => {
                                if let Err(e) = socket.set_nodelay(true) {
                                    warn!("Failed to set TCP_NODELAY for {}: {}", peer_addr, e);
                                }
                                self.spawn_client(
                                    socket,
                                    peer_addr.to_string(),
                                    conn_limit.clone(),
                                    tls_acceptor.clone(),
                                );
                            }
                            Err(e) => error!("Failed to accept connection: {}", e),
                        }
                    }
                    accept = accept_unix_optional(&unix_listener) => {
                        match accept {
                            Ok(socket) => {
                                let peer = format!("unix:{}", self.config.unixsocket);
                                self.spawn_client(
                                    socket,
                                    peer,
                                    conn_limit.clone(),
                                    None,
                                );
                            }
                            Err(e) => error!("Failed to accept unix connection: {}", e),
                        }
                    }
                }
            }
            #[cfg(not(unix))]
            {
                tokio::select! {
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            info!("Shutdown signal received — stopping accept loop");
                            break;
                        }
                    }
                    accept = listener.accept() => {
                        match accept {
                            Ok((socket, peer_addr)) => {
                                if let Err(e) = socket.set_nodelay(true) {
                                    warn!("Failed to set TCP_NODELAY for {}: {}", peer_addr, e);
                                }
                                self.spawn_client(
                                    socket,
                                    peer_addr.to_string(),
                                    conn_limit.clone(),
                                    tls_acceptor.clone(),
                                );
                            }
                            Err(e) => error!("Failed to accept connection: {}", e),
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

        // Remove unix socket file so the next bind is clean.
        #[cfg(unix)]
        if let Some(path) = unix_path {
            let _ = std::fs::remove_file(&path);
        }

        info!("Kore server shut down cleanly");
        Ok(())
    }

    /// Spawn a connection handler task (TCP or UDS).
    fn spawn_client<S>(
        &self,
        mut socket: S,
        peer_label: String,
        conn_limit: Arc<Semaphore>,
        tls_acceptor: Option<TlsAcceptor>,
    ) where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let permit = match conn_limit.try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                warn!(
                    "Max connections reached ({}), rejecting connection from {}",
                    self.config.maxconns, peer_label
                );
                tokio::spawn(async move {
                    let _ = socket.write_all(MAX_CLIENTS_ERR).await;
                    let _ = socket.shutdown().await;
                });
                return;
            }
        };

        debug!("New connection from {}", peer_label);

        let databases = self.databases.clone();
        let config = self.config.clone();
        let persistence = self.persistence.clone();
        let acl = self.acl.clone();
        let cluster = self.cluster.clone();
        let redlock = self.redlock.clone();
        let script_cache = self.script_cache.clone();

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
                            redlock,
                            script_cache,
                        )
                        .await
                    }
                    Err(e) => Err(anyhow::anyhow!("TLS handshake failed: {}", e)),
                }
            } else {
                handle_connection(
                    socket,
                    databases,
                    config,
                    persistence,
                    acl,
                    cluster,
                    redlock,
                    script_cache,
                )
                .await
            };
            if let Err(e) = result {
                // Disconnects and short-lived clients are normal; avoid WARN noise.
                debug!("Connection closed from {} with error: {}", peer_label, e);
            } else {
                debug!("Connection closed from {}", peer_label);
            }
        });
    }
}

/// Accept on an optional Unix listener; pending forever when `None`.
#[cfg(unix)]
async fn accept_unix_optional(
    listener: &Option<UnixListener>,
) -> std::io::Result<tokio::net::UnixStream> {
    match listener {
        Some(l) => l.accept().await.map(|(s, _)| s),
        None => std::future::pending().await,
    }
}



/// Enqueue a response buffer with backpressure / slow-client detection.
async fn send_response_buf(
    tx: &mpsc::Sender<Vec<u8>>,
    data: Vec<u8>,
) -> Result<(), ()> {
    let current_capacity = tx.capacity();
    if current_capacity < (RESPONSE_QUEUE_SIZE.saturating_sub(SLOW_CLIENT_THRESHOLD)) {
        warn!(
            "Response queue filling up: {}/{} capacity remaining - slow client detected",
            current_capacity, RESPONSE_QUEUE_SIZE
        );
    }
    match timeout(Duration::from_millis(100), tx.send(data)).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(_)) => {
            warn!("Response channel closed");
            Err(())
        }
        Err(_) => {
            warn!("Response queue send timeout - client not consuming responses fast enough");
            Err(())
        }
    }
}

async fn handle_connection<S>(
    socket: S,
    databases: Arc<Databases>,
    config: Arc<Config>,
    persistence: Option<Arc<PersistenceManager>>,
    acl: Arc<AclStore>,
    cluster: Option<Arc<ClusterState>>,
    redlock: Option<Arc<Redlock>>,
    script_cache: Arc<ScriptCache>,
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
            .with_cluster(cluster)
            .with_redlock(redlock)
            .with_script_cache(script_cache);
    handler.set_client_id(client_id);
    let mut buf = vec![0u8; BUFFER_SIZE]; // 8KB buffer

    // Create a channel for sending responses
    let (response_tx, mut response_rx) = mpsc::channel::<Vec<u8>>(RESPONSE_QUEUE_SIZE);

    // Split socket into reader and writer (works for TcpStream and TlsStream)
    let (mut reader, mut writer) = tokio::io::split(socket);

    // Spawn a task to handle outgoing messages (both responses and pub/sub messages).
    // Response path coalesces queued buffers into larger writes (pipeline-friendly).
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
                // Handle command responses — drain/coalesce for pipelined clients
                Some(data) = response_rx.recv() => {
                    let mut batch = data;
                    while batch.len() < WRITE_BATCH_MAX_BYTES {
                        match response_rx.try_recv() {
                            Ok(more) => {
                                batch.extend_from_slice(&more);
                            }
                            Err(_) => break,
                        }
                    }

                    match timeout(Duration::from_secs(WRITE_TIMEOUT_SECS), writer.write_all(&batch)).await {
                        Ok(Ok(_)) => {
                            cache_clone.stats.incr_bytes_sent(batch.len());
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

        // Parse and handle all complete commands available after this read
        // (Redis pipelining). Coalesce serialized replies into fewer channel sends.
        let mut pipeline_buf: Vec<u8> = Vec::new();
        let mut entered_replica_feed = false;

        while let Some(value) = parser.parse()? {
            let response = match handler.handle(value).await {
                Ok(resp) => resp,
                Err(e) => {
                    error!("Command handler error: {}", e);
                    RespValue::error(e.to_resp_string())
                }
            };

            // SYNC / PSYNC: flush any prior pipeline bytes, then handshake + feed.
            if let Some(raw) = handler.take_raw_response() {
                if !pipeline_buf.is_empty() {
                    if send_response_buf(&response_tx, pipeline_buf).await.is_err() {
                        break 'conn Ok(());
                    }
                    pipeline_buf = Vec::new();
                }
                let feed_rx = handler.take_replica_feed();
                let _ = response_tx.send(raw.to_vec()).await;
                if let Some(mut feed_rx) = feed_rx {
                    info!("Connection entered replica feed mode");
                    entered_replica_feed = true;
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

            let serialized = response.serialize();
            pipeline_buf.extend_from_slice(&serialized);
            // Bound memory if a huge pipeline arrives in one read.
            if pipeline_buf.len() >= WRITE_BATCH_MAX_BYTES {
                if send_response_buf(&response_tx, std::mem::take(&mut pipeline_buf))
                    .await
                    .is_err()
                {
                    break 'conn Ok(());
                }
            }
        }

        if entered_replica_feed {
            // already exited via break 'conn
        } else if !pipeline_buf.is_empty() {
            if send_response_buf(&response_tx, pipeline_buf).await.is_err() {
                break Ok(());
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
            enable_fair_queue: false,
            fair_queue_max_size: 1024,
            fair_queue_cleanup_ms: 500,
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
            aclfile: String::new(),
            cluster_enabled: false,
                unixsocket: String::new(),
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
