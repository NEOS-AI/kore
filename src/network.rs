use crate::acl::AclStore;
use crate::cache::Cache;
use crate::cluster::ClusterState;
use crate::commands::CommandHandler;
use crate::config::Config;
use crate::databases::Databases;
use crate::persistence::PersistenceManager;
use crate::sentinel::SentinelState;
use crate::protocol::{RespParser, RespValue};
use crate::redlock::Redlock;
use crate::scripting::{FunctionLibraryStore, ScriptCache, ScriptRuntime};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig as RustlsServerConfig;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::sync::{mpsc, watch, Semaphore};
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
    /// Sentinel-lite monitors (Batch EW). Always present.
    sentinel: Arc<SentinelState>,
    /// Shared SCRIPT LOAD / EVALSHA cache for all connections.
    script_cache: Arc<ScriptCache>,
    /// Shared Redis Functions library store for all connections.
    function_libs: Arc<FunctionLibraryStore>,
    /// Shared script runtime (`lua-time-limit`, SCRIPT KILL).
    script_runtime: Arc<ScriptRuntime>,
    /// When true, skip SAVE on process shutdown (SHUTDOWN NOSAVE).
    shutdown_nosave: Arc<std::sync::atomic::AtomicBool>,
}

const BUFFER_SIZE: usize = 8192;
/// Cap coalesced write size so one slow flush cannot hold unbounded memory.
const WRITE_BATCH_MAX_BYTES: usize = 64 * 1024;
const MAX_CLIENTS_ERR: &[u8] = b"-ERR max number of clients reached\r\n";
/// Static RESP for the common SET/write OK reply (Batch FI / GX).
const RESP_OK: &[u8] = b"+OK\r\n";

/// Load a rustls server acceptor from PEM cert/key paths (no client auth).
/// Fails fast if files are missing or contain invalid PEM material.
pub fn load_tls_acceptor(cert_path: &str, key_path: &str) -> anyhow::Result<TlsAcceptor> {
    load_tls_acceptor_ex(cert_path, key_path, None, false)
}

/// Load TLS acceptor with optional mTLS (Batch GL).
///
/// When `require_client_auth` is true, `ca_path` must point at a PEM CA; clients
/// must present a certificate chain trusted by that CA.
pub fn load_tls_acceptor_ex(
    cert_path: &str,
    key_path: &str,
    ca_path: Option<&str>,
    require_client_auth: bool,
) -> anyhow::Result<TlsAcceptor> {
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

    let server_config = if require_client_auth {
        let ca = ca_path.ok_or_else(|| {
            anyhow::anyhow!("mTLS requires a CA path (--tls-ca)")
        })?;
        let roots = load_root_store(ca)?;
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid TLS client verifier: {}", e))?;
        RustlsServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .map_err(|e| anyhow::anyhow!("Invalid TLS cert/key pair: {}", e))?
    } else {
        RustlsServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| anyhow::anyhow!("Invalid TLS cert/key pair: {}", e))?
    };

    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

/// Load PEM CA certs into a rustls root store (mTLS / replica trust).
pub fn load_root_store(ca_path: &str) -> anyhow::Result<rustls::RootCertStore> {
    let f = File::open(ca_path)
        .map_err(|e| anyhow::anyhow!("Failed to open TLS CA '{}': {}", ca_path, e))?;
    let mut reader = BufReader::new(f);
    let mut roots = rustls::RootCertStore::empty();
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("Failed to parse TLS CA '{}': {}", ca_path, e))?;
    if certs.is_empty() {
        anyhow::bail!("No certificates found in CA file '{}'", ca_path);
    }
    for c in certs {
        roots
            .add(c)
            .map_err(|e| anyhow::anyhow!("Failed to add CA cert from '{}': {}", ca_path, e))?;
    }
    Ok(roots)
}

/// Client TLS connector trusting `ca_path` PEMs (replica → primary, Batch GL).
pub fn load_tls_connector(ca_path: &str) -> anyhow::Result<tokio_rustls::TlsConnector> {
    let roots = load_root_store(ca_path)?;
    let client_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(tokio_rustls::TlsConnector::from(Arc::new(client_config)))
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
            // Batch EN/FL: prefer dir/nodes.conf (topology + live flags) when present.
            let cs = ClusterState::load_or_single_node(
                config.host.clone(),
                config.port,
                &config.dir,
            );
            // Batch FL: CLI/boot flags override only when non-default so a plain
            // restart keeps SAVECONFIG / CONFIG SET values from nodes.conf.
            // Explicit non-default CLI still wins (ops pin).
            if config.cluster_replica_priority != 100 {
                cs.set_local_repl_priority(config.cluster_replica_priority);
            }
            if !config.cluster_require_full_coverage {
                // Default is true; only apply explicit "no"/false.
                cs.set_require_full_coverage(false);
            }
            if config.cluster_allow_reads_when_down {
                // Default is false; only apply explicit true.
                cs.set_allow_reads_when_down(true);
            }
            // Batch EU: client-facing announce (empty/0 = leave loaded/default).
            if !config.cluster_announce_ip.is_empty() {
                cs.set_announce_ip(Some(config.cluster_announce_ip.clone()));
            }
            if config.cluster_announce_port > 0 {
                cs.set_announce_port(Some(config.cluster_announce_port));
            }
            // Batch EO: autosave nodes.conf after topology mutations / failover claim.
            cs.set_nodes_conf_dir(&config.dir);
            Some(cs)
        } else {
            None
        };
        // Batch EZ: load dir/sentinel.conf when present.
        let sentinel = SentinelState::load_or_new(&config.dir);
        Self {
            databases,
            config,
            persistence,
            acl,
            redlock: None,
            cluster,
            sentinel,
            script_cache: ScriptCache::shared(),
            function_libs: FunctionLibraryStore::shared(),
            script_runtime: ScriptRuntime::shared(),
            shutdown_nosave: Arc::new(std::sync::atomic::AtomicBool::new(false)),
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

    /// Replace Sentinel-lite state (tests).
    pub fn with_sentinel(mut self, sentinel: Arc<SentinelState>) -> Self {
        self.sentinel = sentinel;
        self
    }

    /// Shared Sentinel-lite state (Batch EW).
    pub fn sentinel(&self) -> &Arc<SentinelState> {
        &self.sentinel
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let (tx, _rx) = watch::channel(false);
        self.run_with_shutdown_tx(tx).await
    }

    /// Run the accept loop until `shutdown` becomes `true`.
    /// Prefer [`run_with_shutdown_tx`] so the SHUTDOWN command can signal exit.
    pub async fn run_with_shutdown(
        &self,
        shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        self.run_with_shutdown_inner(None, shutdown).await
    }

    /// Preferred entry: provide the shutdown Sender so SHUTDOWN and signals share one channel.
    pub async fn run_with_shutdown_tx(
        &self,
        shutdown_tx: watch::Sender<bool>,
    ) -> anyhow::Result<()> {
        let rx = shutdown_tx.subscribe();
        self.run_with_shutdown_inner(Some(shutdown_tx), rx).await
    }

    async fn run_with_shutdown_inner(
        &self,
        shutdown_tx: Option<watch::Sender<bool>>,
        mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        let shutdown_tx = shutdown_tx.map(Arc::new);
        let shutdown_nosave = Arc::clone(&self.shutdown_nosave);
        let addr = self.config.socket_addr();

        // Batch GL dual listener:
        // - !tls: plain on --port
        // - tls && tls_port==0: TLS-only on --port
        // - tls && tls_port>0: plain on --port, TLS on --tls-port
        let dual_tls = self.config.tls && self.config.tls_port > 0;
        let plain_listener = if !self.config.tls || dual_tls {
            let l = TcpListener::bind(&addr).await?;
            info!("Kore server listening (plain) on {}", addr);
            Some(l)
        } else {
            None
        };
        let tls_listener = if self.config.tls {
            let tls_addr = if dual_tls {
                format!("{}:{}", self.config.host, self.config.tls_port)
                    .parse()
                    .expect("Invalid TLS socket address")
            } else {
                addr
            };
            let l = TcpListener::bind(&tls_addr).await?;
            info!("Kore server listening (TLS) on {}", tls_addr);
            Some(l)
        } else {
            None
        };
        // At least one TCP listener must exist.
        if plain_listener.is_none() && tls_listener.is_none() {
            return Err(anyhow::anyhow!("no TCP listeners configured"));
        }

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
            let ca = if self.config.tls_auth_clients {
                Some(self.config.tls_ca.as_str())
            } else {
                None
            };
            let acceptor = load_tls_acceptor_ex(
                &self.config.tls_cert,
                &self.config.tls_key,
                ca,
                self.config.tls_auth_clients,
            )?;
            info!(
                "TLS enabled (cert={}, key={}, mTLS={}, dual_port={})",
                self.config.tls_cert,
                self.config.tls_key,
                self.config.tls_auth_clients,
                dual_tls
            );
            Some(acceptor)
        } else {
            None
        };

        let cache = self.databases.db0();
        info!("Shards: {}", self.config.shards);
        info!("Databases: {}", self.databases.len());
        info!("Max memory: {} bytes", cache.max_memory());
        info!("Worker threads: {}", self.config.num_threads());
        info!("Max connections: {}", self.config.maxconns);
        if self.cluster.is_some() {
            info!(
                "Cluster mode: enabled (gossip over client RESP; peer bus lite on cport; single-observer fail)"
            );
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
            // Batch GP: Kore peer bus lite for dual-end NODE 2PC (client_port+10000).
            // Soft-fail inside run_cluster_bus if bind fails → RESP-only prepare/commit.
            let bus_cluster = Arc::clone(cluster);
            let bus_shutdown = shutdown.clone();
            tokio::spawn(async move {
                crate::cluster::run_cluster_bus(bus_cluster, bus_shutdown).await;
            });
        }

        // Sentinel-lite health / ODOWN / auto-failover (Batch EW/EX).
        {
            // Advertise this process's client port for MEETPEER (Batch EX).
            self.sentinel
                .set_listen_addr(self.config.host.clone(), self.config.port);
            let sentinel = Arc::clone(&self.sentinel);
            let sentinel_shutdown = shutdown.clone();
            tokio::spawn(async move {
                crate::sentinel::run_sentinel_loop(sentinel, sentinel_shutdown).await;
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
                    accept = accept_tcp_optional(&plain_listener) => {
                        match accept {
                            Ok((socket, peer_addr)) => {
                                if let Err(e) = socket.set_nodelay(true) {
                                    warn!("Failed to set TCP_NODELAY for {}: {}", peer_addr, e);
                                }
                                self.spawn_client(
                                    socket,
                                    peer_addr.to_string(),
                                    conn_limit.clone(),
                                    None, // plain
                                    shutdown_tx.clone(),
                                    Arc::clone(&shutdown_nosave),
                                );
                            }
                            Err(e) => error!("Failed to accept connection: {}", e),
                        }
                    }
                    accept = accept_tcp_optional(&tls_listener) => {
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
                                    shutdown_tx.clone(),
                                    Arc::clone(&shutdown_nosave),
                                );
                            }
                            Err(e) => error!("Failed to accept TLS connection: {}", e),
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
                                    shutdown_tx.clone(),
                                    Arc::clone(&shutdown_nosave),
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
                    accept = accept_tcp_optional(&plain_listener) => {
                        match accept {
                            Ok((socket, peer_addr)) => {
                                if let Err(e) = socket.set_nodelay(true) {
                                    warn!("Failed to set TCP_NODELAY for {}: {}", peer_addr, e);
                                }
                                self.spawn_client(
                                    socket,
                                    peer_addr.to_string(),
                                    conn_limit.clone(),
                                    None,
                                    shutdown_tx.clone(),
                                    Arc::clone(&shutdown_nosave),
                                );
                            }
                            Err(e) => error!("Failed to accept connection: {}", e),
                        }
                    }
                    accept = accept_tcp_optional(&tls_listener) => {
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
                                    shutdown_tx.clone(),
                                    Arc::clone(&shutdown_nosave),
                                );
                            }
                            Err(e) => error!("Failed to accept TLS connection: {}", e),
                        }
                    }
                }
            }
        }

        // Best-effort multi-DB persistence flush on shutdown (unless SHUTDOWN NOSAVE).
        let skip_save = shutdown_nosave.load(std::sync::atomic::Ordering::SeqCst);
        if !skip_save {
            if let Some(ref p) = self.persistence {
                info!("Saving database before shutdown...");
                match p.save(&self.databases) {
                    Ok(()) => info!("Database saved successfully"),
                    Err(e) => warn!("Failed to save database on shutdown: {}", e),
                }
            }
        } else {
            info!("Skipping SAVE on shutdown (NOSAVE)");
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
        shutdown_tx: Option<Arc<watch::Sender<bool>>>,
        shutdown_nosave: Arc<std::sync::atomic::AtomicBool>,
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
        let sentinel = Arc::clone(&self.sentinel);
        let redlock = self.redlock.clone();
        let script_cache = self.script_cache.clone();
        let function_libs = self.function_libs.clone();
        let script_runtime = self.script_runtime.clone();

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
                            sentinel,
                            redlock,
                            script_cache,
                            function_libs,
                            script_runtime,
                            shutdown_tx,
                            shutdown_nosave,
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
                    sentinel,
                    redlock,
                    script_cache,
                    function_libs,
                    script_runtime,
                    shutdown_tx,
                    shutdown_nosave,
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

/// Accept on an optional TCP listener; pending forever when `None` (Batch GL dual).
async fn accept_tcp_optional(
    listener: &Option<TcpListener>,
) -> std::io::Result<(TcpStream, std::net::SocketAddr)> {
    match listener {
        Some(l) => l.accept().await,
        None => std::future::pending().await,
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



/// Write a response buffer on the connection task (Batch GX).
///
/// Hot path skips a per-write `timeout` timer (timerfd/mach port tax under
/// pipeline SET). TCP backpressure still stalls the connection task — same
/// practical slow-client behaviour as Redis/Valkey single-thread loops.
/// Pub/sub lag disconnect remains on the broadcast ring path.
async fn write_response_buf<W>(
    writer: &mut W,
    cache: &Cache,
    data: &[u8],
) -> Result<(), ()>
where
    W: AsyncWrite + Unpin,
{
    if data.is_empty() {
        return Ok(());
    }
    match writer.write_all(data).await {
        Ok(()) => {
            cache.stats.incr_bytes_sent(data.len());
            Ok(())
        }
        Err(e) => {
            warn!("Failed to write response: {}", e);
            Err(())
        }
    }
}

/// Append a RESP reply into the pipeline coalesce buffer (hot-path OK is static).
#[inline]
fn append_resp_reply(pipeline_buf: &mut Vec<u8>, response: &RespValue) {
    if let RespValue::SimpleString(s) = response {
        if s.as_ref() == b"OK" {
            pipeline_buf.extend_from_slice(RESP_OK);
            return;
        }
    }
    let serialized = response.serialize();
    pipeline_buf.extend_from_slice(&serialized);
}

async fn handle_connection<S>(
    socket: S,
    databases: Arc<Databases>,
    config: Arc<Config>,
    persistence: Option<Arc<PersistenceManager>>,
    acl: Arc<AclStore>,
    cluster: Option<Arc<ClusterState>>,
    sentinel: Arc<SentinelState>,
    redlock: Option<Arc<Redlock>>,
    script_cache: Arc<ScriptCache>,
    function_libs: Arc<FunctionLibraryStore>,
    script_runtime: Arc<ScriptRuntime>,
    shutdown_tx: Option<Arc<watch::Sender<bool>>>,
    shutdown_nosave: Arc<std::sync::atomic::AtomicBool>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // Track connection statistics / pubsub on DB 0 (shared across keyspaces)
    let cache = databases.db0();
    cache.stats.incr_connections();

    // Register client for pub/sub (receiver polled only while subscribed — Batch GX).
    let (client_id, mut pubsub_rx) = cache.pubsub.register_client().await;

    // RESP (REdis Serialization Protocol) parser and command handler
    let mut parser = RespParser::new();
    let mut handler =
        CommandHandler::with_databases_and_acl(databases, config, persistence, acl)
            .with_cluster(cluster)
            .with_sentinel(Some(sentinel))
            .with_redlock(redlock)
            .with_script_cache(script_cache)
            .with_function_libs(function_libs)
            .with_script_runtime(script_runtime);
    if let Some(tx) = shutdown_tx {
        // watch::Sender is Clone; Arc unwraps to a clone for the handler field.
        handler = handler.with_shutdown((*tx).clone(), shutdown_nosave);
    }
    handler.set_client_id(client_id);
    let mut buf = vec![0u8; BUFFER_SIZE]; // 8KB buffer

    // Batch GX: keep read+write on one task (no per-connection write-task / mpsc hop).
    // redis-benchmark / normal clients never subscribed → no select! vs pubsub.
    // Pub/sub clients multiplex reads and push deliveries in the same task.
    let (mut reader, mut writer) = tokio::io::split(socket);
    // Reused across pipeline batches to avoid per-read Vec alloc (Batch GX).
    let mut pipeline_buf: Vec<u8> = Vec::with_capacity(4096);

    // Outcome of processing one read's worth of complete RESP commands.
    enum PipelineOutcome {
        Continue,
        Close,
        /// Enter replica feed after writing `raw` handshake (already flushed pipeline).
        ReplicaFeed {
            raw: bytes::Bytes,
            feed_rx: mpsc::Receiver<bytes::Bytes>,
        },
    }

    /// Parse + handle every complete command currently in `parser`, coalesce
    /// replies into `pipeline_buf`, and flush when full / done / close.
    async fn process_available_commands<W>(
        parser: &mut RespParser,
        handler: &mut CommandHandler,
        writer: &mut W,
        cache: &Cache,
        pipeline_buf: &mut Vec<u8>,
    ) -> anyhow::Result<PipelineOutcome>
    where
        W: AsyncWrite + Unpin,
    {
        pipeline_buf.clear();
        while let Some(value) = parser.parse()? {
            let response = match handler.handle(value).await {
                Ok(resp) => resp,
                Err(e) => {
                    error!("Command handler error: {}", e);
                    RespValue::error(e.to_resp_string())
                }
            };

            // CLIENT REPLY OFF/SKIP: execute command but omit response on the wire.
            if handler.take_suppress_reply() {
                if handler.take_close_after_reply() {
                    return Ok(PipelineOutcome::Close);
                }
                continue;
            }

            let close_after = handler.take_close_after_reply();

            // SYNC / PSYNC: flush any prior pipeline bytes, then handshake + feed.
            if let Some(raw) = handler.take_raw_response() {
                if !pipeline_buf.is_empty() {
                    if write_response_buf(writer, cache, pipeline_buf).await.is_err() {
                        return Ok(PipelineOutcome::Close);
                    }
                    pipeline_buf.clear();
                }
                if let Some(feed_rx) = handler.take_replica_feed() {
                    return Ok(PipelineOutcome::ReplicaFeed { raw, feed_rx });
                }
                // Raw response without feed (e.g. partial handshake error path).
                if write_response_buf(writer, cache, raw.as_ref()).await.is_err() {
                    return Ok(PipelineOutcome::Close);
                }
                continue;
            }

            append_resp_reply(pipeline_buf, &response);
            // Bound memory if a huge pipeline arrives in one read.
            if pipeline_buf.len() >= WRITE_BATCH_MAX_BYTES {
                if write_response_buf(writer, cache, pipeline_buf).await.is_err() {
                    return Ok(PipelineOutcome::Close);
                }
                pipeline_buf.clear();
            }
            // QUIT / SHUTDOWN / CLIENT KILL: flush reply then close.
            if close_after {
                if !pipeline_buf.is_empty()
                    && write_response_buf(writer, cache, pipeline_buf).await.is_err()
                {
                    return Ok(PipelineOutcome::Close);
                }
                return Ok(PipelineOutcome::Close);
            }
        }

        if !pipeline_buf.is_empty()
            && write_response_buf(writer, cache, pipeline_buf).await.is_err()
        {
            return Ok(PipelineOutcome::Close);
        }
        Ok(PipelineOutcome::Continue)
    }

    // Main loop for handling client commands
    let result = 'conn: loop {
        // When subscribed, also drain pub/sub push messages between reads.
        // When not, a plain read avoids select! wakeups (Batch GX redis-benchmark path).
        let n = if handler.in_pubsub_mode() {
            loop {
                tokio::select! {
                    msg = pubsub_rx.recv() => {
                        match msg {
                            Ok(value) => {
                                cache.note_pubsub_delivered(client_id).await;
                                let data = value.serialize();
                                if write_response_buf(&mut writer, &cache, data.as_ref())
                                    .await
                                    .is_err()
                                {
                                    break 'conn Ok(());
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                warn!(
                                    "Pub/sub client {} lagged by {} message(s) — disconnecting slow client",
                                    client_id, n
                                );
                                let freed = cache.pubsub.note_lagged(client_id, n).await;
                                cache.release_pubsub_memory(freed);
                                break 'conn Ok(());
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                break 'conn Ok(());
                            }
                        }
                    }
                    read_res = reader.read(&mut buf) => {
                        break match read_res {
                            Ok(0) => break 'conn Ok(()),
                            Ok(n) => n,
                            Err(e) => break 'conn Err(e.into()),
                        };
                    }
                }
            }
        } else {
            match reader.read(&mut buf).await {
                Ok(0) => break Ok(()),
                Ok(n) => n,
                Err(e) => break Err(e.into()),
            }
        };

        parser.feed(&buf[..n]);
        cache.stats.incr_bytes_received(n);

        match process_available_commands(
            &mut parser,
            &mut handler,
            &mut writer,
            &cache,
            &mut pipeline_buf,
        )
        .await?
        {
            PipelineOutcome::Continue => {}
            PipelineOutcome::Close => break Ok(()),
            PipelineOutcome::ReplicaFeed { raw, mut feed_rx } => {
                if write_response_buf(&mut writer, &cache, raw.as_ref())
                    .await
                    .is_err()
                {
                    break Ok(());
                }
                info!("Connection entered replica feed mode");
                let repl: Option<Arc<crate::persistence::replication::ReplicationManager>> =
                    handler
                        .persistence()
                        .map(|p| Arc::clone(&p.replication));
                let announce_host = handler.replica_announce_ip().map(|s| s.to_string());
                let announce_port = handler.replica_announce_port();
                let mut ack_parser = RespParser::new();
                let mut ack_buf = vec![0u8; 4096];
                let mut getack_tick = tokio::time::interval(Duration::from_secs(1));
                getack_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                // Skip the immediate first tick so we don't probe before the
                // handshake bytes have left the socket.
                getack_tick.tick().await;

                loop {
                    tokio::select! {
                        cmd = feed_rx.recv() => {
                            match cmd {
                                Some(cmd_bytes) => {
                                    if write_response_buf(
                                        &mut writer,
                                        &cache,
                                        cmd_bytes.as_ref(),
                                    )
                                    .await
                                    .is_err()
                                    {
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
                            if let Some(ref r) = repl {
                                r.send_getack_probe_to_feeds(
                                    announce_host.as_deref(),
                                    announce_port,
                                );
                            }
                        }
                    }
                }
                break Ok(());
            }
        }
    };

    // Cleanup: unregister client (frees any remaining pub/sub pending memory) and stats
    cache.unregister_pubsub_client(client_id).await;
    cache.stats.decr_active_connections();

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
