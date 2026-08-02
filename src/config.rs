use clap::Parser;
use crate::error::{Error, Result};
use std::net::SocketAddr;

#[derive(Parser, Debug, Clone)]
#[command(name = "kore")]
#[command(about = "A low-latency, high-performance caching database", long_about = None)]
// Keep clap's `-h`/`--help`; bind address is long-only (`--host`) so short names stay unique.
pub struct Config {
    /// Host address to bind to
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Port to bind to
    #[arg(short = 'p', long, default_value = "6379")]
    pub port: u16,

    /// Number of worker threads (0 = number of CPU cores)
    #[arg(long, default_value = "0")]
    pub threads: usize,

    /// Number of shards for the hashmap
    #[arg(long, default_value = "4096")]
    pub shards: usize,

    /// Maximum memory in bytes (0 = 80% of system memory)
    #[arg(long, default_value = "0")]
    pub maxmemory: usize,

    /// Enable eviction when memory is full (false forces maxmemory-policy=noeviction)
    #[arg(long, default_value = "true")]
    pub evict: bool,

    /// Maxmemory eviction policy (Redis-compatible).
    /// One of: noeviction, allkeys-lru, volatile-lru, allkeys-lfu, volatile-lfu,
    /// allkeys-random, volatile-random, volatile-ttl
    #[arg(long, default_value = "allkeys-lru")]
    pub maxmemory_policy: String,

    /// Enable automatic sweeping of expired entries
    #[arg(long, default_value = "true")]
    pub autosweep: bool,

    /// Load factor (0.55-0.95)
    #[arg(long, default_value = "0.75")]
    pub loadfactor: f64,

    /// Maximum number of connections
    #[arg(long, default_value = "1024")]
    pub maxconns: usize,

    /// Authentication password (empty = no auth)
    #[arg(long, default_value = "")]
    pub auth: String,

    /// Maximum entry size in bytes (default: 500MB)
    #[arg(long, default_value = "524288000")]
    pub maxentrysize: usize,

    /// Verbosity level 0–3: ERROR/WARN/INFO/DEBUG (default 1 = WARN). Boot-only; RUST_LOG overrides.
    #[arg(short = 'v', long, default_value = "1")]
    pub verbosity: u8,

    /// Log format: `text` (default) or `json` (structured lines, targets on). Boot-only (not CONFIG SET).
    #[arg(long, default_value = "text", value_parser = ["text", "json"])]
    pub log_format: String,

    /// Enable Redlock distributed locking (requires multiple instances)
    #[arg(long, default_value = "false")]
    pub enable_redlock: bool,

    /// Redlock instance addresses (comma-separated, e.g., "host1:port1,host2:port2")
    #[arg(long, default_value = "")]
    pub redlock_instances: String,

    /// Redlock retry count (default: 3)
    #[arg(long, default_value = "3")]
    pub redlock_retry_count: usize,

    /// Redlock retry delay in milliseconds (default: 200)
    #[arg(long, default_value = "200")]
    pub redlock_retry_delay_ms: u64,

    /// Enable fair lock queueing on Redlock (FIFO / priority waiters).
    #[arg(long, default_value = "false")]
    pub enable_fair_queue: bool,

    /// Max waiters per resource when fair queueing is enabled (default: 1024).
    #[arg(long, default_value = "1024")]
    pub fair_queue_max_size: usize,

    /// Background fair-queue expired-entry cleanup interval in ms (default: 500).
    #[arg(long, default_value = "500")]
    pub fair_queue_cleanup_ms: u64,

    /// Working directory for RDB/AOF files
    #[arg(long, default_value = "./data")]
    pub dir: String,

    /// RDB filename (relative to --dir)
    #[arg(long, default_value = "dump.rdb")]
    pub dbfilename: String,

    /// Enable AOF (append-only file) persistence
    #[arg(long, default_value = "false")]
    pub appendonly: bool,

    /// AOF filename (relative to --dir)
    #[arg(long, default_value = "appendonly.aof")]
    pub appendfilename: String,

    /// Replicate from this primary (host:port). Empty = act as primary.
    #[arg(long, default_value = "")]
    pub replicaof: String,

    /// RDB auto-save policies: `"900,1 300,10 60,10000"` (seconds,changes).
    /// Empty string disables timed SAVE. Redis pair form `"900 1 300 10"` also accepted.
    #[arg(long, default_value = "900,1 300,10 60,10000")]
    pub save: String,

    /// Number of logical databases (SELECT 0 .. databases-1). Redis default is 16.
    #[arg(long, default_value = "16")]
    pub databases: usize,

    /// Prometheus metrics HTTP port bound to 127.0.0.1 (0 = disabled).
    #[arg(long, default_value = "0")]
    pub metrics_port: u16,

    /// Deadlock monitoring Web UI HTTP port bound to 127.0.0.1 (0 = disabled).
    /// Serves HTML at `/` and JSON at `/api/deadlock`. Localhost-only; no auth.
    /// Binds HTTP only — does not by itself configure detector params (see
    /// `--enable-deadlock-detection` / `--deadlock-max-wait-ms` / etc.). When
    /// non-zero and Redlock is on, a detector is still auto-attached for a live
    /// graph (back-compat) using the deadlock-* param flags.
    #[arg(long, default_value = "0")]
    pub deadlock_ui_port: u16,

    /// Enable Redlock deadlock detection (wait-for graph) independent of the UI.
    /// Also auto-enabled when `--deadlock-ui-port` is non-zero (so the UI has a
    /// live detector). Requires `--enable-redlock`.
    #[arg(long, default_value = "false")]
    pub enable_deadlock_detection: bool,

    /// Max wait time in ms for deadlock wait-edge cleanup / long-wait checks
    /// (default: 30000). Applied when detection is enabled.
    #[arg(long, default_value = "30000")]
    pub deadlock_max_wait_ms: u64,

    /// Automatically resolve deadlocks by releasing a victim's locks (default: false).
    #[arg(long, default_value = "false")]
    pub deadlock_auto_resolve: bool,

    /// Victim selection strategy when auto-resolving: `youngest` (default),
    /// `oldest`, or `fewest-locks`.
    #[arg(long, default_value = "youngest", value_parser = ["youngest", "oldest", "fewest-locks"])]
    pub deadlock_victim_strategy: String,

    /// Enable TLS for client connections (default: false).
    #[arg(long, default_value = "false")]
    pub tls: bool,

    /// Path to TLS certificate PEM file (required when --tls is set).
    #[arg(long, default_value = "")]
    pub tls_cert: String,

    /// Path to TLS private key PEM file (required when --tls is set).
    #[arg(long, default_value = "")]
    pub tls_key: String,

    /// Dedicated TLS listen port (Batch GL dual listener).
    /// When `--tls` and this is **0**, TLS-only on `--port` (legacy).
    /// When `--tls` and this is **>0**, plain RESP on `--port` and TLS on `--tls-port`.
    #[arg(long, default_value = "0")]
    pub tls_port: u16,

    /// CA / client-trust PEM for mTLS and/or replica TLS (Batch GL).
    /// With `--tls-auth-clients`, clients must present a cert chain trusted by this CA.
    /// With `--tls-replication`, used as trust root when connecting to the primary
    /// (falls back to `--tls-cert` if empty).
    #[arg(long, default_value = "")]
    pub tls_ca: String,

    /// Require client certificates (mTLS). Requires `--tls` and `--tls-ca`.
    #[arg(long, default_value = "false")]
    pub tls_auth_clients: bool,

    /// Use TLS when this node is a replica connecting to its primary (Batch GL).
    #[arg(long, default_value = "false")]
    pub tls_replication: bool,

    /// Path to ACL rules file for ACL LOAD / ACL SAVE (empty = not configured).
    #[arg(long, default_value = "")]
    pub aclfile: String,

    /// Enable Redis Cluster mode (hash slots, MOVED/ASK redirects). Default: false.
    #[arg(long, default_value = "false")]
    pub cluster_enabled: bool,

    /// Cluster replica failover priority (higher wins; 0 = never promote). Default 100.
    #[arg(long, default_value = "100")]
    pub cluster_replica_priority: u32,

    /// When true (default), refuse key commands if any hash slot is unserved or
    /// owned by a fail-marked master (`cluster_state:fail`). Batch EQ.
    #[arg(long, default_value = "true", action = clap::ArgAction::Set)]
    pub cluster_require_full_coverage: bool,

    /// When true, allow **read** key commands even if `cluster_state` is fail
    /// (Batch ES / Redis `cluster-allow-reads-when-down`). Default false.
    #[arg(long, default_value = "false", action = clap::ArgAction::Set)]
    pub cluster_allow_reads_when_down: bool,

    /// Client-facing IP announced in CLUSTER NODES/SLOTS/MEET/MOVED (Batch EU).
    /// Empty = use bind `host`.
    #[arg(long, default_value = "")]
    pub cluster_announce_ip: String,

    /// Client-facing port announced in topology (0 = use bind `port`). Batch EU.
    #[arg(long, default_value = "0")]
    pub cluster_announce_port: u16,

    /// Unix domain socket path (empty = disabled). Listens in addition to TCP.
    #[arg(long, default_value = "")]
    pub unixsocket: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 6379,
            threads: 0,
            shards: 4096,
            maxmemory: 0,
            evict: true,
            maxmemory_policy: "allkeys-lru".to_string(),
            autosweep: true,
            loadfactor: 0.75,
            maxconns: 1024,
            auth: String::new(),
            maxentrysize: 524288000,
            verbosity: 1,
            log_format: "text".to_string(),
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
            databases: 16,
            metrics_port: 0,
            deadlock_ui_port: 0,
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
        }
    }
}

impl Config {
    pub fn socket_addr(&self) -> SocketAddr {
        format!("{}:{}", self.host, self.port)
            .parse()
            .expect("Invalid socket address")
    }

    pub fn num_threads(&self) -> usize {
        if self.threads == 0 {
            num_cpus::get()
        } else {
            self.threads
        }
    }

    pub fn max_memory(&self) -> usize {
        if self.maxmemory == 0 {
            // 80% of system memory
            let sys = sysinfo::System::new_all();
            (sys.total_memory() as f64 * 0.8) as usize
        } else {
            self.maxmemory
        }
    }

    /// Parse Redlock instance addresses
    pub fn redlock_instance_addrs(&self) -> Vec<String> {
        if self.redlock_instances.is_empty() {
            Vec::new()
        } else {
            self.redlock_instances
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        }
    }

    /// Validate configuration settings
    pub fn validate(&self) -> Result<()> {
        // Validate port
        if self.port == 0 {
            return Err(Error::ConfigError("Port cannot be 0".to_string()));
        }

        // Validate shards (must be power of 2)
        if self.shards == 0 || (self.shards & (self.shards - 1)) != 0 {
            return Err(Error::ConfigError(
                format!("Shards must be a power of 2, got {}", self.shards)
            ));
        }

        // Validate maxmemory (must be at least 1MB if set)
        if self.maxmemory > 0 && self.maxmemory < 1024 * 1024 {
            return Err(Error::ConfigError(
                format!("Max memory must be at least 1MB, got {} bytes", self.maxmemory)
            ));
        }

        // Validate load factor
        if self.loadfactor < 0.55 || self.loadfactor > 0.95 {
            return Err(Error::ConfigError(
                format!("Load factor must be between 0.55 and 0.95, got {}", self.loadfactor)
            ));
        }

        // Validate maxconns
        if self.maxconns == 0 {
            return Err(Error::ConfigError("Max connections cannot be 0".to_string()));
        }

        // Validate maxentrysize (must be at least 1KB)
        if self.maxentrysize < 1024 {
            return Err(Error::ConfigError(
                format!("Max entry size must be at least 1KB, got {} bytes", self.maxentrysize)
            ));
        }

        // Validate verbosity level
        if self.verbosity > 3 {
            return Err(Error::ConfigError(
                format!("Verbosity level must be 0-3, got {}", self.verbosity)
            ));
        }

        // Validate save policies
        if let Err(e) = crate::persistence::parse_save_rules(&self.save) {
            return Err(Error::ConfigError(format!("Invalid --save: {}", e)));
        }

        // Validate maxmemory-policy
        if let Err(e) = crate::cache::EvictionPolicy::parse(&self.maxmemory_policy) {
            return Err(Error::ConfigError(e));
        }

        // Validate databases (at least 1, cap to avoid runaway memory)
        if self.databases == 0 {
            return Err(Error::ConfigError(
                "databases must be at least 1".to_string(),
            ));
        }
        if self.databases > 1024 {
            return Err(Error::ConfigError(
                "databases cannot exceed 1024".to_string(),
            ));
        }

        // Validate Redlock configuration
        if self.enable_redlock {
            let instances = self.redlock_instance_addrs();
            if instances.len() < 3 {
                return Err(Error::ConfigError(
                    format!("Redlock requires at least 3 instances, got {}", instances.len())
                ));
            }

            // Validate that all addresses are parseable
            for addr in instances {
                if addr.parse::<SocketAddr>().is_err() {
                    return Err(Error::ConfigError(
                        format!("Invalid Redlock instance address: {}", addr)
                    ));
                }
            }

            if self.redlock_retry_count == 0 {
                return Err(Error::ConfigError("Redlock retry count cannot be 0".to_string()));
            }
        }

        if self.enable_fair_queue && !self.enable_redlock {
            return Err(Error::ConfigError(
                "enable_fair_queue requires enable_redlock".to_string(),
            ));
        }
        if self.enable_fair_queue {
            if self.fair_queue_max_size == 0 {
                return Err(Error::ConfigError(
                    "fair_queue_max_size cannot be 0".to_string(),
                ));
            }
            if self.fair_queue_cleanup_ms == 0 {
                return Err(Error::ConfigError(
                    "fair_queue_cleanup_ms cannot be 0".to_string(),
                ));
            }
        }

        // Deadlock detection needs Redlock (UI may still bind without it).
        let deadlock_requested =
            self.enable_deadlock_detection || self.deadlock_ui_port != 0;
        if deadlock_requested && !self.enable_redlock {
            // Soft: UI can start disabled; only fail when detection was
            // explicitly requested without Redlock.
            if self.enable_deadlock_detection {
                return Err(Error::ConfigError(
                    "enable_deadlock_detection requires enable_redlock".to_string(),
                ));
            }
        }
        if deadlock_requested && self.deadlock_max_wait_ms == 0 {
            return Err(Error::ConfigError(
                "deadlock_max_wait_ms cannot be 0".to_string(),
            ));
        }
        // clap value_parser already restricts strategy; re-check for Default/mutate paths.
        if !matches!(
            self.deadlock_victim_strategy.as_str(),
            "youngest" | "oldest" | "fewest-locks"
        ) {
            return Err(Error::ConfigError(format!(
                "deadlock_victim_strategy must be youngest|oldest|fewest-locks, got {}",
                self.deadlock_victim_strategy
            )));
        }

        // Validate TLS configuration
        if self.tls {
            if self.tls_cert.is_empty() {
                return Err(Error::ConfigError(
                    "TLS enabled but --tls-cert is missing".to_string(),
                ));
            }
            if self.tls_key.is_empty() {
                return Err(Error::ConfigError(
                    "TLS enabled but --tls-key is missing".to_string(),
                ));
            }
            if !std::path::Path::new(&self.tls_cert).is_file() {
                return Err(Error::ConfigError(format!(
                    "TLS certificate file not found: {}",
                    self.tls_cert
                )));
            }
            if !std::path::Path::new(&self.tls_key).is_file() {
                return Err(Error::ConfigError(format!(
                    "TLS private key file not found: {}",
                    self.tls_key
                )));
            }
            if self.tls_port != 0 && self.tls_port == self.port {
                return Err(Error::ConfigError(
                    "--tls-port must differ from --port (or be 0 for TLS-only)".to_string(),
                ));
            }
            if self.tls_auth_clients {
                if self.tls_ca.is_empty() {
                    return Err(Error::ConfigError(
                        "--tls-auth-clients requires --tls-ca".to_string(),
                    ));
                }
                if !std::path::Path::new(&self.tls_ca).is_file() {
                    return Err(Error::ConfigError(format!(
                        "TLS CA file not found: {}",
                        self.tls_ca
                    )));
                }
            }
        } else if self.tls_auth_clients || self.tls_port > 0 {
            return Err(Error::ConfigError(
                "--tls-auth-clients / --tls-port require --tls".to_string(),
            ));
        }

        if self.tls_replication {
            // Need a trust root: explicit CA or server cert as pin for self-signed.
            let trust = if !self.tls_ca.is_empty() {
                self.tls_ca.as_str()
            } else if !self.tls_cert.is_empty() {
                self.tls_cert.as_str()
            } else {
                return Err(Error::ConfigError(
                    "--tls-replication requires --tls-ca or --tls-cert as trust root".to_string(),
                ));
            };
            if !std::path::Path::new(trust).is_file() {
                return Err(Error::ConfigError(format!(
                    "TLS trust root file not found: {}",
                    trust
                )));
            }
        }

        Ok(())
    }

    /// Trust root path for replica TLS (CA preferred, else server cert).
    pub fn tls_replication_trust_path(&self) -> Option<&str> {
        if !self.tls_replication {
            return None;
        }
        if !self.tls_ca.is_empty() {
            Some(self.tls_ca.as_str())
        } else if !self.tls_cert.is_empty() {
            Some(self.tls_cert.as_str())
        } else {
            None
        }
    }

    /// Update a configuration value at runtime
    pub fn set(&mut self, key: &str, value: &str) -> Result<String> {
        match key {
            "maxmemory" => {
                let val: usize = value.parse()
                    .map_err(|_| Error::ConfigError(format!("Invalid maxmemory value: {}", value)))?;
                
                if val > 0 && val < 1024 * 1024 {
                    return Err(Error::ConfigError(
                        "Max memory must be at least 1MB".to_string()
                    ));
                }
                
                self.maxmemory = val;
                Ok(format!("Set maxmemory to {}", val))
            }
            "maxentrysize" => {
                let val: usize = value.parse()
                    .map_err(|_| Error::ConfigError(format!("Invalid maxentrysize value: {}", value)))?;
                
                if val < 1024 {
                    return Err(Error::ConfigError(
                        "Max entry size must be at least 1KB".to_string()
                    ));
                }
                
                self.maxentrysize = val;
                Ok(format!("Set maxentrysize to {}", val))
            }
            "maxconns" => {
                let val: usize = value.parse()
                    .map_err(|_| Error::ConfigError(format!("Invalid maxconns value: {}", value)))?;
                
                if val == 0 {
                    return Err(Error::ConfigError(
                        "Max connections cannot be 0".to_string()
                    ));
                }
                
                self.maxconns = val;
                Ok(format!("Set maxconns to {}", val))
            }
            "verbosity" => {
                let val: u8 = value.parse()
                    .map_err(|_| Error::ConfigError(format!("Invalid verbosity value: {}", value)))?;
                
                if val > 3 {
                    return Err(Error::ConfigError(
                        "Verbosity level must be 0-3".to_string()
                    ));
                }
                
                self.verbosity = val;
                Ok(format!("Set verbosity to {}", val))
            }
            _ => Err(Error::ConfigError(format!("Unknown config key: {}", key)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn log_format_defaults_to_text() {
        let c = Config::try_parse_from(["kore"]).expect("default parse");
        assert_eq!(c.log_format, "text");
    }

    #[test]
    fn log_format_json_flag() {
        let c = Config::try_parse_from(["kore", "--log-format", "json"]).expect("json parse");
        assert_eq!(c.log_format, "json");
    }

    #[test]
    fn log_format_text_explicit() {
        let c = Config::try_parse_from(["kore", "--log-format", "text"]).expect("text parse");
        assert_eq!(c.log_format, "text");
    }

    #[test]
    fn log_format_rejects_unknown() {
        let err = Config::try_parse_from(["kore", "--log-format", "xml"]);
        assert!(err.is_err());
    }

    #[test]
    fn deadlock_ui_port_defaults_to_zero() {
        let c = Config::try_parse_from(["kore"]).expect("default parse");
        assert_eq!(c.deadlock_ui_port, 0);
        assert!(!c.enable_deadlock_detection);
        assert_eq!(c.deadlock_max_wait_ms, 30_000);
        assert!(!c.deadlock_auto_resolve);
        assert_eq!(c.deadlock_victim_strategy, "youngest");
    }

    #[test]
    fn deadlock_ui_port_flag() {
        let c = Config::try_parse_from(["kore", "--deadlock-ui-port", "9101"])
            .expect("deadlock-ui-port parse");
        assert_eq!(c.deadlock_ui_port, 9101);
    }

    #[test]
    fn deadlock_detection_flags_parse() {
        let c = Config::try_parse_from([
            "kore",
            "--enable-deadlock-detection",
            "--deadlock-max-wait-ms",
            "15000",
            "--deadlock-auto-resolve",
            "--deadlock-victim-strategy",
            "fewest-locks",
        ])
        .expect("deadlock flags parse");
        assert!(c.enable_deadlock_detection);
        assert_eq!(c.deadlock_max_wait_ms, 15_000);
        assert!(c.deadlock_auto_resolve);
        assert_eq!(c.deadlock_victim_strategy, "fewest-locks");
    }

    #[test]
    fn deadlock_victim_strategy_rejects_unknown() {
        let err = Config::try_parse_from(["kore", "--deadlock-victim-strategy", "random"]);
        assert!(err.is_err());
    }

    #[test]
    fn enable_deadlock_detection_requires_redlock() {
        let mut c = Config::default();
        c.enable_deadlock_detection = true;
        let err = c.validate().unwrap_err();
        assert!(
            err.to_string().contains("enable_deadlock_detection"),
            "got: {}",
            err
        );
    }
}

