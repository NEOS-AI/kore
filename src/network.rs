use crate::cache::Cache;
use crate::commands::CommandHandler;
use crate::config::Config;
use crate::protocol::{RespParser, RespValue};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

pub struct Server {
    cache: Arc<Cache>,
    config: Arc<Config>,
}

const BUFFER_SIZE: usize = 8192;

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
    mut socket: TcpStream,
    cache: Arc<Cache>,
    config: Arc<Config>,
) -> anyhow::Result<()> {
    // Set TCP_NODELAY (disable Nagle's algorithm)
    socket.set_nodelay(true)?;

    // RESP (REdis Serialization Protocol) parser and command handler
    let mut parser = RespParser::new();
    let mut handler = CommandHandler::new(cache, config);
    let mut buf = vec![0u8; BUFFER_SIZE]; // 8KB buffer

    loop {
        // Read data from socket
        let n = socket.read(&mut buf).await?;

        if n == 0 {
            // Connection closed
            return Ok(());
        }

        // Feed data to parser
        parser.feed(&buf[..n]);

        // Parse and handle commands
        while let Some(value) = parser.parse()? {
            let response = match handler.handle(value).await {
                Ok(resp) => resp,
                Err(e) => {
                    error!("Command handler error: {}", e);
                    RespValue::error(format!("ERR {}", e))
                }
            };

            // Serialize and send response
            let data = response.serialize();
            socket.write_all(&data).await?;
        }
    }
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
