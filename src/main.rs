use clap::Parser;
use kore::{Cache, Config};
use kore::network::Server;
use std::sync::Arc;
use tracing::{info, Level};
use tracing_subscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse command line arguments
    let config = Config::parse();

    // Initialize tracing/logging
    let level = match config.verbosity {
        0 => Level::ERROR,
        1 => Level::WARN,
        2 => Level::INFO,
        _ => Level::DEBUG,
    };

    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_target(false)
        .init();

    info!("Starting Kore database server v0.1.0");

    // Create cache
    let max_memory = config.max_memory();
    let cache = Cache::new(config.shards, max_memory);
    cache.set_evict(config.evict);
    cache.set_autosweep(config.autosweep);

    info!("Cache initialized with {} shards", config.shards);
    info!("Max memory: {} bytes (~{} MB)", max_memory, max_memory / (1024 * 1024));
    info!("Eviction: {}", if config.evict { "enabled" } else { "disabled" });
    info!("Auto-sweep: {}", if config.autosweep { "enabled" } else { "disabled" });

    // Create and run server
    let config = Arc::new(config);
    let server = Server::new(cache, config);

    server.run().await?;

    Ok(())
}
