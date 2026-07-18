use clap::Parser;
use kore::network::Server;
use kore::persistence::replication::run_replica_loop;
use kore::{Config, Databases, PersistenceConfig, PersistenceManager, Redlock};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{info, warn, Level};
use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    // Parse command line arguments
    let config = Config::parse();

    // Validate configuration
    if let Err(e) = config.validate() {
        eprintln!("Configuration error: {}", e);
        std::process::exit(1);
    }

    // Initialize tracing/logging (boot-only — not live via CONFIG SET).
    // Verbosity 0–3 maps to ERROR / WARN / INFO / DEBUG (default 1 = WARN).
    // `RUST_LOG` / EnvFilter overrides the verbosity floor when set.
    let level = match config.verbosity {
        0 => Level::ERROR,
        1 => Level::WARN,
        2 => Level::INFO,
        _ => Level::DEBUG,
    };
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(level.to_string()));

    // `--log-format json` → structured JSON lines (targets on for aggregators);
    // default `text` stays human-readable (targets off for quieter console).
    match config.log_format.as_str() {
        "json" => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(filter)
                .with_target(true)
                .init();
        }
        _ => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_target(false)
                .init();
        }
    }

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

        // Create multi-DB keyspaces (loadfactor drives initial per-shard HashMap capacity).
        // start_sweep: false until after load_at_startup so expire cannot race
        // scratch-load replace (Batch CC). Sweep tasks start below.
        let max_memory = config.max_memory();
        let max_entry_size = config.maxentrysize;
        let databases = Databases::create(
            config.databases,
            config.shards,
            max_memory,
            max_entry_size,
            false,
            config.loadfactor,
        );
        let cache = databases.db0();
        for db in databases.iter() {
            // Keep autosweep off through startup load (task not running yet either).
            db.set_autosweep(false);
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

        // Enable background expire only after startup load commit.
        databases.set_autosweep_all(config.autosweep);
        databases.start_background_sweep_all();

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

#[cfg(test)]
mod log_format_tests {
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};
    use tracing::info;
    use tracing_subscriber::fmt::MakeWriter;

    /// Capture writer for a one-shot JSON log smoke (Batch CY ops polish).
    #[derive(Clone, Default)]
    struct Buf(Arc<Mutex<Vec<u8>>>);

    impl<'a> MakeWriter<'a> for Buf {
        type Writer = Guard;

        fn make_writer(&'a self) -> Self::Writer {
            Guard(self.0.clone())
        }
    }

    struct Guard(Arc<Mutex<Vec<u8>>>);

    impl Write for Guard {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().expect("buf lock").extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn json_log_line_is_parseable_object() {
        let buf = Buf::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_target(true)
            .with_writer(buf.clone())
            .with_max_level(tracing::Level::INFO)
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            info!(target: "kore::test", "cy_json_smoke");
        });

        let bytes = buf.0.lock().expect("buf lock").clone();
        let line = String::from_utf8(bytes).expect("utf8 log");
        let line = line.trim();
        assert!(
            line.starts_with('{') && line.ends_with('}'),
            "JSON log must be a single object line, got: {line}"
        );
        // tracing-subscriber JSON fields: level + message (+ target when enabled)
        assert!(
            line.contains("\"level\"") || line.contains("\"INFO\""),
            "expected level field in JSON log: {line}"
        );
        assert!(
            line.contains("cy_json_smoke"),
            "expected message body in JSON log: {line}"
        );
        assert!(
            line.contains("kore::test") || line.contains("\"target\""),
            "expected target in JSON log (with_target true): {line}"
        );
    }
}
