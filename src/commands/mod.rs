mod basic;
mod key_value;
mod counter;
mod expiration;
mod admin;
mod sorted_set;
mod geospatial;
mod pubsub;
mod search;

use crate::cache::Cache;
use crate::config::Config;
use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::pubsub::ClientId;
use std::sync::Arc;

pub struct CommandHandler {
    cache: Arc<Cache>,
    config: Arc<Config>,
    authenticated: bool,
    client_id: Option<ClientId>,
}

impl CommandHandler {
    pub fn new(cache: Arc<Cache>, config: Arc<Config>) -> Self {
        let authenticated = config.auth.is_empty(); // Auto-auth if no password set
        Self {
            cache,
            config,
            authenticated,
            client_id: None,
        }
    }

    pub fn set_client_id(&mut self, client_id: ClientId) {
        self.client_id = Some(client_id);
    }

    pub fn client_id(&self) -> Option<ClientId> {
        self.client_id
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
            
            // Distributed lock commands
            "SETNX" => self.handle_setnx(&args[1..]),
            "GETDEL" => self.handle_getdel(&args[1..]),
            "GETEX" => self.handle_getex(&args[1..]),
            
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
            
            // Geospatial commands
            "GEOADD" => self.handle_geoadd(&args[1..]),
            "GEOSEARCH" => self.handle_geosearch(&args[1..]),
            
            // Pub/Sub commands
            "PUBLISH" => self.handle_publish(&args[1..]),
            "SUBSCRIBE" => self.handle_subscribe(&args[1..]),
            "UNSUBSCRIBE" => self.handle_unsubscribe(&args[1..]),
            "PSUBSCRIBE" => self.handle_psubscribe(&args[1..]),
            "PUNSUBSCRIBE" => self.handle_punsubscribe(&args[1..]),
            "PUBSUB" => self.handle_pubsub(&args[1..]),

            // Search commands
            "FT.CREATE" => self.handle_ft_create(&args[1..]),
            "FT.DROPINDEX" => self.handle_ft_dropindex(&args[1..]),
            "FT._LIST" => self.handle_ft_list(&args[1..]),
            "FT.INFO" => self.handle_ft_info(&args[1..]),
            "FT.SEARCH" => self.handle_ft_search(&args[1..]),

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

    // Pub/Sub command handlers
    fn handle_publish(&self, args: &[RespValue]) -> Result<RespValue> {
        Ok(tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.cache.cmd_publish(args).await
            })
        })?)
    }

    fn handle_subscribe(&mut self, args: &[RespValue]) -> Result<RespValue> {
        let client_id = self.client_id.ok_or_else(|| {
            Error::InvalidArgument("client not registered for pub/sub".into())
        })?;
        
        let responses = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.cache.cmd_subscribe(client_id, args).await
            })
        })?;
        
        // Return multiple responses
        if responses.len() == 1 {
            Ok(responses.into_iter().next().unwrap())
        } else {
            Ok(RespValue::Array(responses))
        }
    }

    fn handle_unsubscribe(&mut self, args: &[RespValue]) -> Result<RespValue> {
        let client_id = self.client_id.ok_or_else(|| {
            Error::InvalidArgument("client not registered for pub/sub".into())
        })?;
        
        let responses = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.cache.cmd_unsubscribe(client_id, args).await
            })
        })?;
        
        if responses.len() == 1 {
            Ok(responses.into_iter().next().unwrap())
        } else {
            Ok(RespValue::Array(responses))
        }
    }

    fn handle_psubscribe(&mut self, args: &[RespValue]) -> Result<RespValue> {
        let client_id = self.client_id.ok_or_else(|| {
            Error::InvalidArgument("client not registered for pub/sub".into())
        })?;
        
        let responses = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.cache.cmd_psubscribe(client_id, args).await
            })
        })?;
        
        if responses.len() == 1 {
            Ok(responses.into_iter().next().unwrap())
        } else {
            Ok(RespValue::Array(responses))
        }
    }

    fn handle_punsubscribe(&mut self, args: &[RespValue]) -> Result<RespValue> {
        let client_id = self.client_id.ok_or_else(|| {
            Error::InvalidArgument("client not registered for pub/sub".into())
        })?;
        
        let responses = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.cache.cmd_punsubscribe(client_id, args).await
            })
        })?;
        
        if responses.len() == 1 {
            Ok(responses.into_iter().next().unwrap())
        } else {
            Ok(RespValue::Array(responses))
        }
    }

    fn handle_pubsub(&self, args: &[RespValue]) -> Result<RespValue> {
        Ok(tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.cache.cmd_pubsub(args).await
            })
        })?)
    }
}
