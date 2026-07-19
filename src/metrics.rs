//! Prometheus-style metrics and readiness helpers (hand-rolled, no extra crates).
//!
//! HTTP scrape endpoint shares request-line / response helpers with the deadlock UI
//! via [`crate::admin_http`].

use crate::admin_http::{
    parse_request_line, read_request_line, write_400, write_404, write_405_get_only, write_response,
};
use crate::cache::Cache;
use crate::databases::Databases;
use crate::persistence::PersistenceManager;
use crate::stats::Stats;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{info, warn};

/// Point-in-time metrics for Prometheus text exposition.
#[derive(Debug, Clone, Default)]
pub struct MetricsSnapshot {
    pub connected_clients: u64,
    pub total_connections: u64,
    pub commands_processed_total: u64,
    pub keyspace_hits_total: u64,
    pub keyspace_misses_total: u64,
    pub used_memory_bytes: u64,
    pub maxmemory_bytes: u64,
    pub connected_replicas: u64,
    pub master_repl_offset: u64,
    /// 1 if replica link is up, 0 if down, -1 if not a replica.
    pub replica_link_up: i64,
    pub rdb_last_save_timestamp_seconds: u64,
}

/// Structured readiness / health report (HEALTH FULL + INFO health).
#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub ready: bool,
    pub role: String,
    pub used_memory: u64,
    pub maxmemory: u64,
    /// "up" | "down" | "n/a" (masters have no master link).
    pub master_link: String,
    pub rdb_last_save: u64,
    pub aof: bool,
}

impl HealthStatus {
    /// INFO-style key:value body (without section header).
    pub fn to_info_lines(&self) -> String {
        format!(
            "ready:{}\r\n\
             role:{}\r\n\
             used_memory:{}\r\n\
             maxmemory:{}\r\n\
             master_link:{}\r\n\
             rdb_last_save:{}\r\n\
             aof:{}\r\n",
            if self.ready { 1 } else { 0 },
            self.role,
            self.used_memory,
            self.maxmemory,
            self.master_link,
            self.rdb_last_save,
            if self.aof { 1 } else { 0 },
        )
    }
}

/// Collect metrics from live server state.
pub fn collect_snapshot(
    cache: &Cache,
    persistence: Option<&PersistenceManager>,
) -> MetricsSnapshot {
    let stats = &cache.stats;
    let (connected_replicas, master_repl_offset, replica_link_up, rdb_last_save) =
        match persistence {
            Some(p) => {
                let repl = &p.replication;
                let link = if repl.is_replica() {
                    if repl.master_link_up() {
                        1
                    } else {
                        0
                    }
                } else {
                    -1
                };
                (
                    repl.connected_replicas() as u64,
                    repl.master_repl_offset(),
                    link,
                    p.last_save_unix(),
                )
            }
            None => (0, 0, -1, 0),
        };

    MetricsSnapshot {
        connected_clients: stats.active_connections.load(Ordering::Relaxed),
        total_connections: stats.total_connections.load(Ordering::Relaxed),
        commands_processed_total: stats.total_commands_processed(),
        keyspace_hits_total: stats.hits.load(Ordering::Relaxed),
        keyspace_misses_total: stats.misses.load(Ordering::Relaxed),
        used_memory_bytes: cache.memory_usage() as u64,
        maxmemory_bytes: cache.max_memory() as u64,
        connected_replicas,
        master_repl_offset,
        replica_link_up,
        rdb_last_save_timestamp_seconds: rdb_last_save,
    }
}

/// Collect HEALTH FULL status.
pub fn collect_health(
    cache: &Cache,
    persistence: Option<&PersistenceManager>,
) -> HealthStatus {
    let (role, master_link, rdb_last_save, aof) = match persistence {
        Some(p) => {
            let repl = &p.replication;
            let role = repl.role_name().to_string();
            let master_link = if repl.is_replica() {
                if repl.master_link_up() {
                    "up".to_string()
                } else {
                    "down".to_string()
                }
            } else {
                "n/a".to_string()
            };
            (role, master_link, p.last_save_unix(), p.appendonly())
        }
        None => ("master".to_string(), "n/a".to_string(), 0, false),
    };

    // Master is ready when serving; replica is ready only with an up master link.
    let ready = match master_link.as_str() {
        "down" => false,
        _ => true,
    };

    HealthStatus {
        ready,
        role,
        used_memory: cache.memory_usage() as u64,
        maxmemory: cache.max_memory() as u64,
        master_link,
        rdb_last_save,
        aof,
    }
}

/// Render Prometheus text format (0.0.4 exposition).
pub fn render_prometheus(snap: &MetricsSnapshot) -> String {
    let mut out = String::with_capacity(1024);
    metric(
        &mut out,
        "kore_connected_clients",
        "gauge",
        "Number of client connections currently open",
        snap.connected_clients as f64,
    );
    metric(
        &mut out,
        "kore_total_connections",
        "counter",
        "Total number of connections accepted since start",
        snap.total_connections as f64,
    );
    metric(
        &mut out,
        "kore_commands_processed_total",
        "counter",
        "Total number of commands processed",
        snap.commands_processed_total as f64,
    );
    metric(
        &mut out,
        "kore_keyspace_hits_total",
        "counter",
        "Number of successful key lookups",
        snap.keyspace_hits_total as f64,
    );
    metric(
        &mut out,
        "kore_keyspace_misses_total",
        "counter",
        "Number of failed key lookups",
        snap.keyspace_misses_total as f64,
    );
    metric(
        &mut out,
        "kore_used_memory_bytes",
        "gauge",
        "Current memory usage in bytes",
        snap.used_memory_bytes as f64,
    );
    metric(
        &mut out,
        "kore_maxmemory_bytes",
        "gauge",
        "Configured maxmemory limit in bytes",
        snap.maxmemory_bytes as f64,
    );
    metric(
        &mut out,
        "kore_connected_replicas",
        "gauge",
        "Number of connected replicas",
        snap.connected_replicas as f64,
    );
    metric(
        &mut out,
        "kore_master_repl_offset",
        "gauge",
        "Master replication offset",
        snap.master_repl_offset as f64,
    );
    metric(
        &mut out,
        "kore_replica_link_up",
        "gauge",
        "1 if replica master link is up, 0 if down, -1 if not a replica",
        snap.replica_link_up as f64,
    );
    metric(
        &mut out,
        "kore_rdb_last_save_timestamp_seconds",
        "gauge",
        "Unix timestamp of last successful RDB save (0 if never)",
        snap.rdb_last_save_timestamp_seconds as f64,
    );
    out
}

fn metric(out: &mut String, name: &str, ty: &str, help: &str, value: f64) {
    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    out.push_str(help);
    out.push('\n');
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push(' ');
    out.push_str(ty);
    out.push('\n');
    out.push_str(name);
    out.push(' ');
    // Integer-looking gauges/counters without scientific notation noise
    if value.fract() == 0.0 && value.abs() < (i64::MAX as f64) {
        out.push_str(&format!("{}\n", value as i64));
    } else {
        out.push_str(&format!("{}\n", value));
    }
}

/// Collect from shared stats alone (unit tests / minimal embeds).
pub fn snapshot_from_stats(stats: &Stats, used_memory: u64, maxmemory: u64) -> MetricsSnapshot {
    MetricsSnapshot {
        connected_clients: stats.active_connections.load(Ordering::Relaxed),
        total_connections: stats.total_connections.load(Ordering::Relaxed),
        commands_processed_total: stats.total_commands_processed(),
        keyspace_hits_total: stats.hits.load(Ordering::Relaxed),
        keyspace_misses_total: stats.misses.load(Ordering::Relaxed),
        used_memory_bytes: used_memory,
        maxmemory_bytes: maxmemory,
        connected_replicas: 0,
        master_repl_offset: 0,
        replica_link_up: -1,
        rdb_last_save_timestamp_seconds: 0,
    }
}

/// Spawn a minimal HTTP server on `127.0.0.1:port` serving GET /metrics.
/// Serves until `shutdown` is true. Non-GET on `/metrics` → 405; unknown path → 404.
/// For tests, prefer [`run_metrics_server_on_listener`] with a pre-bound `127.0.0.1:0` listener.
pub async fn run_metrics_server(
    port: u16,
    databases: Arc<Databases>,
    persistence: Option<Arc<PersistenceManager>>,
    shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    run_metrics_server_on_listener(listener, databases, persistence, shutdown).await
}

/// Same as [`run_metrics_server`] but uses an already-bound listener
/// (tests: bind `127.0.0.1:0`, read `local_addr().port()`, pass the listener).
pub async fn run_metrics_server_on_listener(
    listener: TcpListener,
    databases: Arc<Databases>,
    persistence: Option<Arc<PersistenceManager>>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let bound = listener.local_addr()?;
    info!("Prometheus metrics listening on http://{}/metrics", bound);

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("Metrics server shutting down");
                    break;
                }
            }
            accept = listener.accept() => {
                match accept {
                    Ok((mut stream, _)) => {
                        let databases = databases.clone();
                        let persistence = persistence.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_metrics_conn(
                                &mut stream,
                                &databases,
                                persistence.as_deref(),
                            )
                            .await
                            {
                                warn!("metrics connection error: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        warn!("metrics accept error: {}", e);
                    }
                }
            }
        }
    }
    Ok(())
}

async fn handle_metrics_conn(
    stream: &mut tokio::net::TcpStream,
    databases: &Databases,
    persistence: Option<&PersistenceManager>,
) -> anyhow::Result<()> {
    let Some(first_line) = read_request_line(stream).await? else {
        return Ok(());
    };
    let Some(req) = parse_request_line(&first_line) else {
        write_400(stream).await?;
        let _ = stream.shutdown().await;
        return Ok(());
    };

    if req.path == "/metrics" && !req.is_get() {
        write_405_get_only(stream).await?;
    } else if req.is_get() && req.path == "/metrics" {
        let cache = databases.db0();
        let snap = collect_snapshot(&cache, persistence);
        let body = render_prometheus(&snap);
        write_response(
            stream,
            200,
            "OK",
            "text/plain; version=0.0.4; charset=utf-8",
            &body,
            &[],
        )
        .await?;
    } else {
        write_404(stream).await?;
    }
    let _ = stream.shutdown().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_contains_core_series_names() {
        let snap = MetricsSnapshot {
            connected_clients: 2,
            total_connections: 10,
            commands_processed_total: 100,
            keyspace_hits_total: 50,
            keyspace_misses_total: 5,
            used_memory_bytes: 1024,
            maxmemory_bytes: 1_048_576,
            connected_replicas: 1,
            master_repl_offset: 42,
            replica_link_up: -1,
            rdb_last_save_timestamp_seconds: 1_700_000_000,
        };
        let text = render_prometheus(&snap);
        for name in [
            "kore_connected_clients",
            "kore_total_connections",
            "kore_commands_processed_total",
            "kore_keyspace_hits_total",
            "kore_keyspace_misses_total",
            "kore_used_memory_bytes",
            "kore_maxmemory_bytes",
            "kore_connected_replicas",
            "kore_master_repl_offset",
            "kore_replica_link_up",
            "kore_rdb_last_save_timestamp_seconds",
        ] {
            assert!(text.contains(name), "missing series {}", name);
        }
        assert!(text.contains("kore_connected_clients 2\n"));
        assert!(text.contains("kore_commands_processed_total 100\n"));
    }
}
