use crate::cache::Cache;
use crate::commands::CommandHandler;
use crate::config::Config;
use crate::protocol::{RespParser, RespValue};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::{error, info, warn};

pub struct Server {
    cache: Arc<Cache>,
    config: Arc<Config>,
}

const BUFFER_SIZE: usize = 8192;
const RESPONSE_QUEUE_SIZE: usize = 100; // Maximum pending responses per client
const SLOW_CLIENT_THRESHOLD: usize = 80; // Trigger warning at 80% full
const WRITE_TIMEOUT_SECS: u64 = 5; // Timeout for write operations

impl Server {
    pub fn new(cache: Arc<Cache>, config: Arc<Config>) -> Self {
        Self { cache, config }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let addr = self.config.socket_addr();
        let listener = TcpListener::bind(&addr).await?;

        info!("Kore server listening on {}", addr);
        info!("Shards: {}", self.config.shards);
        info!("Max memory: {} bytes", self.cache.max_memory);
        info!("Worker threads: {}", self.config.num_threads());

        loop {
            match listener.accept().await {
                Ok((socket, peer_addr)) => {
                    info!("New connection from {}", peer_addr);

                    let cache = self.cache.clone();
                    let config = self.config.clone();

                    // Spawn a new task to handle the connection
                    // Tokio will schedule this on its thread pool (number of threads configured at runtime)
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(socket, cache, config).await {
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

async fn handle_connection(
    socket: TcpStream,
    cache: Arc<Cache>,
    config: Arc<Config>,
) -> anyhow::Result<()> {
    // Set TCP_NODELAY (disable Nagle's algorithm)
    socket.set_nodelay(true)?;
    
    // Track connection statistics
    cache.stats.incr_connections();

    // Register client for pub/sub
    let (client_id, mut pubsub_rx) = cache.pubsub.register_client().await;

    // RESP (REdis Serialization Protocol) parser and command handler
    let mut parser = RespParser::new();
    let mut handler = CommandHandler::new(cache.clone(), config);
    handler.set_client_id(client_id);
    let mut buf = vec![0u8; BUFFER_SIZE]; // 8KB buffer

    // Create a channel for sending responses
    let (response_tx, mut response_rx) = mpsc::channel::<Vec<u8>>(RESPONSE_QUEUE_SIZE);

    // Split socket into reader and writer (takes ownership)
    let (mut reader, mut writer) = socket.into_split();

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
                        Err(_) => break,
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
    let result = loop {
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
                    RespValue::error(format!("ERR {}", e))
                }
            };

            // Send response through channel
            let data = response.serialize().to_vec();
            
            // Check if response queue is getting full (backpressure detection)
            let current_capacity = response_tx.capacity();
            if current_capacity < (RESPONSE_QUEUE_SIZE - SLOW_CLIENT_THRESHOLD) {
                warn!("Response queue filling up: {}/{} capacity remaining - slow client detected", 
                      current_capacity, RESPONSE_QUEUE_SIZE);
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
                    warn!("Response queue send timeout - client not consuming responses fast enough");
                    break;
                }
            }
        }
    };

    // Cleanup: unregister client and update statistics
    cache.pubsub.unregister_client(client_id).await;
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
