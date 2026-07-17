use clap::Parser;
use kore::network::Server;
use kore::persistence::replication::run_replica_loop;
use kore::{Config, Databases, PersistenceConfig, PersistenceManager, Redlock};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{info, warn, Level};
use tracing_subscriber;

fn main() -> anyhow::Result<()> {
    // Parse command line arguments
    let config = Config::parse();

    // Validate configuration
    if let Err(e) = config.validate() {
        eprintln!("Configuration error: {}", e);
        std::process::exit(1);
    }

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

    // Build Tokio runtime with the configured worker thread count
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.num_threads())
        .enable_all()
        .build()?;

    rt.block_on(async move {
        // get version from Cargo.toml
        let version = env!("CARGO_PKG_VERSION");
        info!("Starting Kore database server v{}", version);
        info!("Worker threads: {}", config.num_threads());

        // Create multi-DB keyspaces (loadfactor drives initial per-shard HashMap capacity)
        let max_memory = config.max_memory();
        let max_entry_size = config.maxentrysize;
        let databases = Databases::create(
            config.databases,
            config.shards,
            max_memory,
            max_entry_size,
            true,
            config.loadfactor,
        );
        let cache = databases.db0();
        for db in databases.iter() {
            db.set_autosweep(config.autosweep);
            // maxmemory-policy: --evict=false forces noeviction
            if !config.evict {
                db.set_eviction_policy(kore::cache::EvictionPolicy::NoEviction);
            } else {
                let policy = kore::cache::EvictionPolicy::parse(&config.maxmemory_policy)
                    .expect("validated at startup");
                db.set_eviction_policy(policy);
            }
        }

        info!(
            "Cache initialized with {} shards, {} databases",
            config.shards,
            databases.len()
        );
        info!(
            "Max memory: {} bytes (~{} MB)",
            max_memory,
            max_memory / (1024 * 1024)
        );
        info!(
            "Max entry size: {} bytes (~{} MB)",
            max_entry_size,
            max_entry_size / (1024 * 1024)
        );
        info!(
            "Eviction policy: {}",
            cache.eviction_policy().as_str()
        );
        info!(
            "Auto-sweep: {}",
            if config.autosweep {
                "enabled"
            } else {
                "disabled"
            }
        );
        info!("Max connections: {}", config.maxconns);
        if config.tls {
            info!(
                "TLS enabled (cert={}, key={})",
                config.tls_cert, config.tls_key
            );
        } else {
            info!("TLS disabled");
        }

        // Persistence
        let save_rules = kore::persistence::parse_save_rules(&config.save)
            .map_err(|e| anyhow::anyhow!("Invalid --save: {}", e))?;
        let pconfig = PersistenceConfig {
            dir: PathBuf::from(&config.dir),
            dbfilename: config.dbfilename.clone(),
            appendonly: config.appendonly,
            appendfilename: config.appendfilename.clone(),
            save_rules: save_rules.clone(),
        };
        let persistence = PersistenceManager::new(pconfig)?;
        persistence.ensure_dir()?;
        info!(
            "Persistence dir={} rdb={} aof={} save={}",
            persistence.config().dir.display(),
            config.dbfilename,
            if config.appendonly {
                config.appendfilename.as_str()
            } else {
                "disabled"
            },
            if save_rules.is_empty() {
                "disabled".to_string()
            } else {
                kore::persistence::format_save_rules(&save_rules)
            }
        );

        if let Err(e) = persistence.load_at_startup(&databases) {
            warn!("Failed to load data at startup: {}", e);
        }

        // Announce this instance's listen port for REPLCONF / FAILOVER TO matching.
        persistence.replication.set_announce_port(config.port);

        // Optional replica-of at startup
        if !config.replicaof.is_empty() {
            info!("Configured as replica of {}", config.replicaof);
            persistence
                .replication
                .set_replicaof(Some(config.replicaof.clone()));
        }

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        // Timed SAVE policies (BGSAVE when seconds+changes thresholds met)
        persistence.spawn_auto_save_scheduler(databases.clone(), shutdown_rx.clone());

        // Signal handler: SIGINT (Ctrl-C) and SIGTERM trigger graceful shutdown
        {
            let shutdown_tx = shutdown_tx.clone();
            tokio::spawn(async move {
                #[cfg(unix)]
                {
                    use tokio::signal::unix::{signal, SignalKind};
                    let mut sigterm = match signal(SignalKind::terminate()) {
                        Ok(s) => s,
                        Err(e) => {
                            warn!("Failed to install SIGTERM handler: {}", e);
                            let _ = tokio::signal::ctrl_c().await;
                            let _ = shutdown_tx.send(true);
                            return;
                        }
                    };
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => {
                            info!("Received SIGINT (Ctrl-C)");
                        }
                        _ = sigterm.recv() => {
                            info!("Received SIGTERM");
                        }
                    }
                }
                #[cfg(not(unix))]
                {
                    let _ = tokio::signal::ctrl_c().await;
                    info!("Received Ctrl-C");
                }
                let _ = shutdown_tx.send(true);
            });
        }

        {
            let databases_r = databases.clone();
            let repl = persistence.replication.clone();
            let shutdown_rx_replica = shutdown_rx.clone();
            tokio::spawn(async move {
                run_replica_loop(databases_r, repl, shutdown_rx_replica).await;
            });
        }

        // Optional Prometheus metrics HTTP endpoint (127.0.0.1 only)
        if config.metrics_port != 0 {
            let databases_m = databases.clone();
            let persistence_m = persistence.clone();
            let shutdown_rx_metrics = shutdown_rx.clone();
            let metrics_port = config.metrics_port;
            tokio::spawn(async move {
                if let Err(e) = kore::metrics::run_metrics_server(
                    metrics_port,
                    databases_m,
                    Some(persistence_m),
                    shutdown_rx_metrics,
                )
                .await
                {
                    warn!("Metrics server exited: {}", e);
                }
            });
            info!(
                "Metrics endpoint enabled on 127.0.0.1:{}",
                config.metrics_port
            );
        }

        // Wire Redlock from CLI flags (in-process multi-cache backends for MVP;
        // remote RESP backends deferred).
        let redlock = match Redlock::from_config(&config, None) {
            Ok(rl) => rl,
            Err(e) => {
                eprintln!("Redlock configuration error: {}", e);
                std::process::exit(1);
            }
        };
        if let Some(ref rl) = redlock {
            info!(
                "Redlock enabled: instances={} quorum={} retry_count={} retry_delay_ms={} \
                 (in-process backends; remote RESP deferred)",
                rl.instance_count(),
                rl.quorum,
                rl.retry_count(),
                rl.retry_delay_ms()
            );
        } else {
            info!("Redlock disabled");
        }

        // Create and run server until shutdown signal
        let config = Arc::new(config);
        let server = Server::with_databases_and_persistence(databases, config, persistence)
            .with_redlock(redlock);

        // Pass the Sender so SHUTDOWN can exit the accept loop (signals still use clones).
        let _shutdown_rx = shutdown_rx;
        server.run_with_shutdown_tx(shutdown_tx).await?;

        Ok(())
    })
}
