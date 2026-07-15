use clap::Parser;
use crate::error::{Error, Result};
use std::net::SocketAddr;

#[derive(Parser, Debug, Clone)]
#[command(name = "kore")]
#[command(about = "A low-latency, high-performance caching database", long_about = None)]
pub struct Config {
    /// Host address to bind to
    #[arg(short = 'h', long, default_value = "127.0.0.1")]
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

    /// Verbosity level (0-3)
    #[arg(short = 'v', long, default_value = "1")]
    pub verbosity: u8,

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

    /// Enable TLS for client connections (default: false).
    #[arg(long, default_value = "false")]
    pub tls: bool,

    /// Path to TLS certificate PEM file (required when --tls is set).
    #[arg(long, default_value = "")]
    pub tls_cert: String,

    /// Path to TLS private key PEM file (required when --tls is set).
    #[arg(long, default_value = "")]
    pub tls_key: String,

    /// Enable Redis Cluster mode (hash slots, MOVED/ASK redirects). Default: false.
    #[arg(long, default_value = "false")]
    pub cluster_enabled: bool,
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
            enable_redlock: false,
            redlock_instances: String::new(),
            redlock_retry_count: 3,
            redlock_retry_delay_ms: 200,
            dir: "./data".to_string(),
            dbfilename: "dump.rdb".to_string(),
            appendonly: false,
            appendfilename: "appendonly.aof".to_string(),
            replicaof: String::new(),
            save: "900,1 300,10 60,10000".to_string(),
            databases: 16,
            metrics_port: 0,
            tls: false,
            tls_cert: String::new(),
            tls_key: String::new(),
            cluster_enabled: false,
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
        }

        Ok(())
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

