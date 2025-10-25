use clap::Parser;
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

    /// Enable eviction when memory is full
    #[arg(long, default_value = "true")]
    pub evict: bool,

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

    /// Verbosity level (0-3)
    #[arg(short = 'v', long, default_value = "1")]
    pub verbosity: u8,
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
}
