use clap::Parser;
use kore::network::Server;
use kore::{Cache, Config};
use std::sync::Arc;
use tracing::{info, Level};
use tracing_subscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse command line arguments
    let config = Config::parse();

    // Initialize tracing/logging
    // verbosity level (higher is more verbose):
    // 0 = ERROR
    // 1 = WARN
    // 2 = INFO
    // 3+ = DEBUG
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

    // get version from Cargo.toml
    let version = env!("CARGO_PKG_VERSION");
    info!("Starting Kore database server v{}", version);

    // Create cache
    let max_memory = config.max_memory();
    let cache = Cache::new(config.shards, max_memory);
    cache.set_evict(config.evict);
    cache.set_autosweep(config.autosweep);

    info!("Cache initialized with {} shards", config.shards);
    info!(
        "Max memory: {} bytes (~{} MB)",
        max_memory,
        max_memory / (1024 * 1024)
    );
    info!(
        "Eviction: {}",
        if config.evict { "enabled" } else { "disabled" }
    );
    info!(
        "Auto-sweep: {}",
        if config.autosweep {
            "enabled"
        } else {
            "disabled"
        }
    );

    // Create and run server
    let config = Arc::new(config);
    let server = Server::new(cache, config);

    server.run().await?;

    Ok(())
}
