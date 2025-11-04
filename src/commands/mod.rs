mod basic;
mod key_value;
mod counter;
mod expiration;
mod admin;
mod sorted_set;

use crate::cache::Cache;
use crate::config::Config;
use crate::error::{Error, Result};
use crate::protocol::RespValue;
use std::sync::Arc;

pub struct CommandHandler {
    cache: Arc<Cache>,
    config: Arc<Config>,
    authenticated: bool,
}

impl CommandHandler {
    pub fn new(cache: Arc<Cache>, config: Arc<Config>) -> Self {
        let authenticated = config.auth.is_empty(); // Auto-auth if no password set
        Self {
            cache,
            config,
            authenticated,
        }
    }

    pub async fn handle(&mut self, value: RespValue) -> Result<RespValue> {
        let args = match value.as_array() {
            Some(arr) => arr,
            None => return Ok(RespValue::error("ERR invalid command format")),
        };

        if args.is_empty() {
            return Ok(RespValue::error("ERR empty command"));
        }

        let cmd = match args[0].as_bulk_string() {
            Some(s) => s,
            None => return Ok(RespValue::error("ERR invalid command")),
        };

        let cmd_upper = String::from_utf8_lossy(cmd).to_uppercase();

        // AUTH command doesn't require authentication
        if cmd_upper == "AUTH" {
            return self.handle_auth(&args[1..]);
        }

        // Check authentication
        if !self.authenticated {
            return Ok(RespValue::error("NOAUTH Authentication required"));
        }

        match cmd_upper.as_str() {
            // Basic commands
            "PING" => self.handle_ping(&args[1..]),
            "ECHO" => self.handle_echo(&args[1..]),
            
            // Key-Value commands
            "SET" => self.handle_set(&args[1..]),
            "GET" => self.handle_get(&args[1..]),
            "DEL" => self.handle_del(&args[1..]),
            "EXISTS" => self.handle_exists(&args[1..]),
            "MGET" => self.handle_mget(&args[1..]),
            "MSET" => self.handle_mset(&args[1..]),
            
            // Counter commands
            "INCR" => self.handle_incr(&args[1..]),
            "DECR" => self.handle_decr(&args[1..]),
            "INCRBY" => self.handle_incrby(&args[1..]),
            "DECRBY" => self.handle_decrby(&args[1..]),
            
            // Expiration commands
            "EXPIRE" => self.handle_expire(&args[1..]),
            "PEXPIRE" => self.handle_pexpire(&args[1..]),
            "TTL" => self.handle_ttl(&args[1..]),
            "PTTL" => self.handle_pttl(&args[1..]),
            
            // Admin commands
            "DBSIZE" => self.handle_dbsize(&args[1..]),
            "KEYS" => self.handle_keys(&args[1..]),
            "FLUSHDB" | "FLUSHALL" => self.handle_flush(&args[1..]),
            "INFO" => self.handle_info(&args[1..]),
            "SWEEP" => self.handle_sweep(&args[1..]),
            "CONFIG" => self.handle_config(&args[1..]),
            
            // Sorted Set commands
            "ZADD" => self.handle_zadd(&args[1..]),
            "ZRANGE" => self.handle_zrange(&args[1..]),
            "ZREVRANGE" => self.handle_zrevrange(&args[1..]),
            "ZCARD" => self.handle_zcard(&args[1..]),
            "ZSCORE" => self.handle_zscore(&args[1..]),
            "ZREM" => self.handle_zrem(&args[1..]),
            "ZRANK" => self.handle_zrank(&args[1..]),
            "ZREVRANK" => self.handle_zrevrank(&args[1..]),
            
            _ => Ok(RespValue::error(format!("ERR unknown command '{}'", cmd_upper))),
        }
    }

    // Helper method for parsing integers
    pub(crate) fn parse_integer(&self, value: &RespValue) -> Result<i64> {
        if let Some(i) = value.as_integer() {
            return Ok(i);
        }

        if let Some(s) = value.as_bulk_string() {
            let s = std::str::from_utf8(s)
                .map_err(|_| Error::InvalidArgument("invalid UTF-8".into()))?;
            return s
                .parse::<i64>()
                .map_err(|_| Error::InvalidArgument("invalid integer".into()));
        }

        Err(Error::InvalidArgument("expected integer".into()))
    }
}
