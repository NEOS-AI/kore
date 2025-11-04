use crate::cache::Cache;
use crate::config::Config;
use crate::entry::{LoadOptions, StoreOptions};
use crate::error::{Error, Result};
use crate::protocol::RespValue;
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::Ordering;

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
            "PING" => self.handle_ping(&args[1..]),
            "ECHO" => self.handle_echo(&args[1..]),
            "SET" => self.handle_set(&args[1..]),
            "GET" => self.handle_get(&args[1..]),
            "DEL" => self.handle_del(&args[1..]),
            "EXISTS" => self.handle_exists(&args[1..]),
            "MGET" => self.handle_mget(&args[1..]),
            "MSET" => self.handle_mset(&args[1..]),
            "INCR" => self.handle_incr(&args[1..]),
            "DECR" => self.handle_decr(&args[1..]),
            "INCRBY" => self.handle_incrby(&args[1..]),
            "DECRBY" => self.handle_decrby(&args[1..]),
            "EXPIRE" => self.handle_expire(&args[1..]),
            "PEXPIRE" => self.handle_pexpire(&args[1..]),
            "TTL" => self.handle_ttl(&args[1..]),
            "PTTL" => self.handle_pttl(&args[1..]),
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

    fn handle_ping(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            Ok(RespValue::SimpleString(Bytes::from("PONG")))
        } else if let Some(msg) = args[0].as_bulk_string() {
            Ok(RespValue::BulkString(Some(msg.clone())))
        } else {
            Ok(RespValue::error("ERR wrong number of arguments for 'ping'"))
        }
    }

    fn handle_echo(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'echo'"));
        }

        match args[0].as_bulk_string() {
            Some(msg) => Ok(RespValue::BulkString(Some(msg.clone()))),
            None => Ok(RespValue::error("ERR invalid argument")),
        }
    }

    fn handle_auth(&mut self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'auth'"));
        }

        let password = match args[0].as_bulk_string() {
            Some(p) => p,
            None => return Ok(RespValue::error("ERR invalid argument")),
        };

        self.cache.stats.incr(&self.cache.stats.auth_cmds);

        if self.config.auth.is_empty() {
            return Ok(RespValue::error("ERR Client sent AUTH, but no password is set"));
        }

        if password.as_ref() == self.config.auth.as_bytes() {
            self.authenticated = true;
            Ok(RespValue::ok())
        } else {
            self.cache.stats.incr(&self.cache.stats.auth_errors);
            Ok(RespValue::error("ERR invalid password"))
        }
    }

    fn handle_set(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'set'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let value = match args[1].as_bulk_string() {
            Some(v) => v.clone(),
            None => return Ok(RespValue::error("ERR invalid value")),
        };

        let mut opts = StoreOptions::default();
        let mut i = 2;

        while i < args.len() {
            let opt = match args[i].as_bulk_string() {
                Some(o) => String::from_utf8_lossy(o).to_uppercase(),
                None => return Ok(RespValue::error("ERR invalid option")),
            };

            match opt.as_str() {
                "NX" => opts.nx = true,
                "XX" => opts.xx = true,
                "GET" => opts.get = true,
                "KEEPTTL" => opts.keepttl = true,
                "EX" => {
                    if i + 1 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    let seconds = self.parse_integer(&args[i + 1])?;
                    opts.ttl_ms = Some((seconds * 1000) as u64);
                    i += 1;
                }
                "PX" => {
                    if i + 1 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    let ms = self.parse_integer(&args[i + 1])?;
                    opts.ttl_ms = Some(ms as u64);
                    i += 1;
                }
                "EXAT" => {
                    if i + 1 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    let timestamp = self.parse_integer(&args[i + 1])?;
                    opts.exat_ms = Some((timestamp * 1000) as u64);
                    i += 1;
                }
                "PXAT" => {
                    if i + 1 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    let timestamp = self.parse_integer(&args[i + 1])?;
                    opts.exat_ms = Some(timestamp as u64);
                    i += 1;
                }
                _ => return Ok(RespValue::error(format!("ERR unknown option '{}'", opt))),
            }

            i += 1;
        }

        match self.cache.store(key, value, opts.clone()) {
            Ok(old_value) => {
                if opts.get {
                    if let Some(entry) = old_value {
                        Ok(RespValue::BulkString(Some(entry.value.clone())))
                    } else {
                        Ok(RespValue::null())
                    }
                } else if opts.nx && old_value.is_none() {
                    Ok(RespValue::null())
                } else {
                    Ok(RespValue::ok())
                }
            }
            Err(Error::OutOfMemory) => Ok(RespValue::error("OOM command not allowed when used memory > 'maxmemory'")),
            Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
        }
    }

    fn handle_get(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'get'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        match self.cache.load(key, LoadOptions::default())? {
            Some(entry) => Ok(RespValue::BulkString(Some(entry.value.clone()))),
            None => Ok(RespValue::null()),
        }
    }

    fn handle_del(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'del'"));
        }

        let keys: Result<Vec<Bytes>> = args
            .iter()
            .map(|arg| {
                arg.as_bulk_string()
                    .cloned()
                    .ok_or_else(|| Error::InvalidArgument("invalid key".into()))
            })
            .collect();

        let keys = keys?;
        let count = self.cache.delete_many(&keys)?;

        Ok(RespValue::Integer(count as i64))
    }

    fn handle_exists(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'exists'"));
        }

        let mut count = 0;
        for arg in args {
            if let Some(key) = arg.as_bulk_string() {
                if self.cache.exists(key) {
                    count += 1;
                }
            }
        }

        Ok(RespValue::Integer(count))
    }

    fn handle_mget(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'mget'"));
        }

        let mut results = Vec::with_capacity(args.len());

        for arg in args {
            if let Some(key) = arg.as_bulk_string() {
                match self.cache.load(key, LoadOptions::default())? {
                    Some(entry) => results.push(RespValue::BulkString(Some(entry.value.clone()))),
                    None => results.push(RespValue::null()),
                }
            } else {
                results.push(RespValue::null());
            }
        }

        Ok(RespValue::Array(results))
    }

    fn handle_mset(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() || args.len() % 2 != 0 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'mset'"));
        }

        for i in (0..args.len()).step_by(2) {
            let key = match args[i].as_bulk_string() {
                Some(k) => k.clone(),
                None => return Ok(RespValue::error("ERR invalid key")),
            };

            let value = match args[i + 1].as_bulk_string() {
                Some(v) => v.clone(),
                None => return Ok(RespValue::error("ERR invalid value")),
            };

            self.cache.store(key, value, StoreOptions::default())?;
        }

        Ok(RespValue::ok())
    }

    fn handle_incr(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'incr'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        match self.cache.incr(key, 1) {
            Ok(new_value) => Ok(RespValue::Integer(new_value)),
            Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
        }
    }

    fn handle_decr(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'decr'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        match self.cache.decr(key, 1) {
            Ok(new_value) => Ok(RespValue::Integer(new_value)),
            Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
        }
    }

    fn handle_incrby(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'incrby'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let delta = self.parse_integer(&args[1])?;

        match self.cache.incr(key, delta) {
            Ok(new_value) => Ok(RespValue::Integer(new_value)),
            Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
        }
    }

    fn handle_decrby(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'decrby'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let delta = self.parse_integer(&args[1])?;

        match self.cache.decr(key, delta) {
            Ok(new_value) => Ok(RespValue::Integer(new_value)),
            Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
        }
    }

    fn handle_expire(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'expire'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let seconds = self.parse_integer(&args[1])?;
        let result = self.cache.expire(key, (seconds * 1000) as u64)?;

        Ok(RespValue::Integer(if result { 1 } else { 0 }))
    }

    fn handle_pexpire(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'pexpire'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let ms = self.parse_integer(&args[1])?;
        let result = self.cache.expire(key, ms as u64)?;

        Ok(RespValue::Integer(if result { 1 } else { 0 }))
    }

    fn handle_ttl(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'ttl'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let ttl_ms = self.cache.ttl(key);
        let ttl_sec = if ttl_ms >= 0 {
            (ttl_ms / 1000) as i64
        } else {
            ttl_ms
        };

        Ok(RespValue::Integer(ttl_sec))
    }

    fn handle_pttl(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'pttl'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let ttl = self.cache.ttl(key);
        Ok(RespValue::Integer(ttl))
    }

    fn handle_dbsize(&self, _args: &[RespValue]) -> Result<RespValue> {
        let size = self.cache.dbsize();
        Ok(RespValue::Integer(size as i64))
    }

    fn handle_keys(&self, args: &[RespValue]) -> Result<RespValue> {
        let pattern = if args.is_empty() {
            None
        } else {
            args[0]
                .as_bulk_string()
                .and_then(|b| std::str::from_utf8(b).ok())
        };

        let keys = self.cache.keys(pattern);
        let resp_keys: Vec<RespValue> = keys
            .into_iter()
            .map(|k| RespValue::BulkString(Some(k)))
            .collect();

        Ok(RespValue::Array(resp_keys))
    }

    fn handle_flush(&self, _args: &[RespValue]) -> Result<RespValue> {
        self.cache.flush();
        Ok(RespValue::ok())
    }

    fn handle_info(&self, _args: &[RespValue]) -> Result<RespValue> {
        let stats = &self.cache.stats;
        let total_cmds = stats.cmd_get.load(Ordering::Relaxed)
            + stats.cmd_set.load(Ordering::Relaxed)
            + stats.cmd_del.load(Ordering::Relaxed)
            + stats.cmd_incr.load(Ordering::Relaxed)
            + stats.cmd_decr.load(Ordering::Relaxed)
            + stats.cmd_zadd.load(Ordering::Relaxed)
            + stats.cmd_zrange.load(Ordering::Relaxed)
            + stats.cmd_zrevrange.load(Ordering::Relaxed)
            + stats.cmd_zcard.load(Ordering::Relaxed)
            + stats.cmd_zscore.load(Ordering::Relaxed)
            + stats.cmd_zrem.load(Ordering::Relaxed)
            + stats.cmd_zrank.load(Ordering::Relaxed)
            + stats.cmd_zrevrank.load(Ordering::Relaxed);
        
        let info = format!(
            "# Server\r\n\
             kore_version:{}\r\n\
             \r\n\
             # Stats\r\n\
             total_commands_processed:{}\r\n\
             cmd_get:{}\r\n\
             cmd_set:{}\r\n\
             cmd_del:{}\r\n\
             cmd_incr:{}\r\n\
             cmd_decr:{}\r\n\
             cmd_zadd:{}\r\n\
             cmd_zrange:{}\r\n\
             cmd_zrevrange:{}\r\n\
             cmd_zcard:{}\r\n\
             cmd_zscore:{}\r\n\
             cmd_zrem:{}\r\n\
             cmd_zrank:{}\r\n\
             cmd_zrevrank:{}\r\n\
             keyspace_hits:{}\r\n\
             keyspace_misses:{}\r\n\
             hit_rate:{:.2}\r\n\
             evicted_expired:{}\r\n\
             evicted_lru:{}\r\n\
             \r\n\
             # Memory\r\n\
             used_memory:{}\r\n\
             maxmemory:{}\r\n\
             maxentrysize:{}\r\n\
             \r\n\
             # Keyspace\r\n\
             db0:keys={}\r\n",
            env!("CARGO_PKG_VERSION"),
            total_cmds,
            stats.cmd_get.load(Ordering::Relaxed),
            stats.cmd_set.load(Ordering::Relaxed),
            stats.cmd_del.load(Ordering::Relaxed),
            stats.cmd_incr.load(Ordering::Relaxed),
            stats.cmd_decr.load(Ordering::Relaxed),
            stats.cmd_zadd.load(Ordering::Relaxed),
            stats.cmd_zrange.load(Ordering::Relaxed),
            stats.cmd_zrevrange.load(Ordering::Relaxed),
            stats.cmd_zcard.load(Ordering::Relaxed),
            stats.cmd_zscore.load(Ordering::Relaxed),
            stats.cmd_zrem.load(Ordering::Relaxed),
            stats.cmd_zrank.load(Ordering::Relaxed),
            stats.cmd_zrevrank.load(Ordering::Relaxed),
            stats.hits.load(Ordering::Relaxed),
            stats.misses.load(Ordering::Relaxed),
            stats.get_hit_rate(),
            stats.evicted_expired.load(Ordering::Relaxed),
            stats.evicted_lru.load(Ordering::Relaxed),
            self.cache.memory_usage(),
            self.cache.max_memory,
            self.cache.get_max_entry_size(),
            self.cache.dbsize(),
        );

        Ok(RespValue::BulkString(Some(Bytes::from(info))))
    }

    fn handle_sweep(&self, _args: &[RespValue]) -> Result<RespValue> {
        let removed = self.cache.sweep();
        Ok(RespValue::Integer(removed as i64))
    }

    fn parse_integer(&self, value: &RespValue) -> Result<i64> {
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

    fn handle_config(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'config'"));
        }

        let subcmd = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_uppercase(),
            None => return Ok(RespValue::error("ERR invalid subcommand")),
        };

        match subcmd.as_str() {
            "GET" => {
                if args.len() != 2 {
                    return Ok(RespValue::error("ERR wrong number of arguments for 'config get'"));
                }

                let param = match args[1].as_bulk_string() {
                    Some(s) => String::from_utf8_lossy(s).to_lowercase(),
                    None => return Ok(RespValue::error("ERR invalid parameter")),
                };

                match param.as_str() {
                    "maxentrysize" | "max-entry-size" => {
                        let value = self.cache.get_max_entry_size();
                        Ok(RespValue::Array(vec![
                            RespValue::BulkString(Some(Bytes::from("maxentrysize"))),
                            RespValue::BulkString(Some(Bytes::from(value.to_string()))),
                        ]))
                    }
                    _ => {
                        // Return empty array for unknown parameters (Redis behavior)
                        Ok(RespValue::Array(vec![]))
                    }
                }
            }
            "SET" => {
                if args.len() != 3 {
                    return Ok(RespValue::error("ERR wrong number of arguments for 'config set'"));
                }

                let param = match args[1].as_bulk_string() {
                    Some(s) => String::from_utf8_lossy(s).to_lowercase(),
                    None => return Ok(RespValue::error("ERR invalid parameter")),
                };

                let value_str = match args[2].as_bulk_string() {
                    Some(s) => String::from_utf8_lossy(s),
                    None => return Ok(RespValue::error("ERR invalid value")),
                };

                match param.as_str() {
                    "maxentrysize" | "max-entry-size" => {
                        let size: usize = value_str.parse()
                            .map_err(|_| Error::InvalidArgument("invalid size".into()))?;
                        
                        match self.cache.set_max_entry_size(size) {
                            Ok(_) => Ok(RespValue::ok()),
                            Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
                        }
                    }
                    _ => Ok(RespValue::error("ERR Unsupported CONFIG parameter")),
                }
            }
            _ => Ok(RespValue::error("ERR Unknown subcommand or wrong number of arguments")),
        }
    }

    // ========== Sorted Set Commands ==========

    fn handle_zadd(&self, args: &[RespValue]) -> Result<RespValue> {
        self.cache.stats.incr(&self.cache.stats.cmd_zadd);
        
        if args.len() < 3 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zadd'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        // Parse score-member pairs
        let mut pairs = Vec::new();
        let mut i = 1;
        
        while i + 1 < args.len() {
            let score = match self.parse_float(&args[i]) {
                Ok(s) => s,
                Err(_) => return Ok(RespValue::error("ERR value is not a valid float")),
            };

            let member = match args[i + 1].as_bulk_string() {
                Some(m) => m.clone(),
                None => return Ok(RespValue::error("ERR invalid member")),
            };

            pairs.push((score, member));
            i += 2;
        }

        if pairs.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zadd'"));
        }

        let zset = self.cache.get_or_create_sorted_set(&key);
        let mut set = zset.write().unwrap();
        
        let mut added = 0;
        for (score, member) in pairs {
            if set.add(member, score) {
                added += 1;
            }
        }

        Ok(RespValue::Integer(added as i64))
    }

    fn handle_zrange(&self, args: &[RespValue]) -> Result<RespValue> {
        self.cache.stats.incr(&self.cache.stats.cmd_zrange);
        
        if args.len() < 3 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zrange'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let start = match self.parse_integer(&args[1]) {
            Ok(s) => s as isize,
            Err(_) => return Ok(RespValue::error("ERR value is not an integer or out of range")),
        };

        let stop = match self.parse_integer(&args[2]) {
            Ok(s) => s as isize,
            Err(_) => return Ok(RespValue::error("ERR value is not an integer or out of range")),
        };

        // Check for WITHSCORES option
        let with_scores = if args.len() > 3 {
            match args[3].as_bulk_string() {
                Some(opt) => {
                    let opt_str = String::from_utf8_lossy(opt).to_uppercase();
                    if opt_str == "WITHSCORES" {
                        true
                    } else {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                }
                None => return Ok(RespValue::error("ERR syntax error")),
            }
        } else {
            false
        };

        let zset = match self.cache.get_sorted_set(key) {
            Some(z) => z,
            None => return Ok(RespValue::Array(vec![])),
        };

        let set = zset.read().unwrap();
        let members = set.range(start, stop, false);

        let mut result = Vec::new();
        for scored_member in members {
            result.push(RespValue::BulkString(Some(scored_member.member)));
            if with_scores {
                result.push(RespValue::BulkString(Some(Bytes::from(scored_member.score.to_string()))));
            }
        }

        Ok(RespValue::Array(result))
    }

    fn handle_zrevrange(&self, args: &[RespValue]) -> Result<RespValue> {
        self.cache.stats.incr(&self.cache.stats.cmd_zrevrange);
        
        if args.len() < 3 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zrevrange'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let start = match self.parse_integer(&args[1]) {
            Ok(s) => s as isize,
            Err(_) => return Ok(RespValue::error("ERR value is not an integer or out of range")),
        };

        let stop = match self.parse_integer(&args[2]) {
            Ok(s) => s as isize,
            Err(_) => return Ok(RespValue::error("ERR value is not an integer or out of range")),
        };

        // Check for WITHSCORES option
        let with_scores = if args.len() > 3 {
            match args[3].as_bulk_string() {
                Some(opt) => {
                    let opt_str = String::from_utf8_lossy(opt).to_uppercase();
                    if opt_str == "WITHSCORES" {
                        true
                    } else {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                }
                None => return Ok(RespValue::error("ERR syntax error")),
            }
        } else {
            false
        };

        let zset = match self.cache.get_sorted_set(key) {
            Some(z) => z,
            None => return Ok(RespValue::Array(vec![])),
        };

        let set = zset.read().unwrap();
        let members = set.range(start, stop, true);

        let mut result = Vec::new();
        for scored_member in members {
            result.push(RespValue::BulkString(Some(scored_member.member)));
            if with_scores {
                result.push(RespValue::BulkString(Some(Bytes::from(scored_member.score.to_string()))));
            }
        }

        Ok(RespValue::Array(result))
    }

    fn handle_zcard(&self, args: &[RespValue]) -> Result<RespValue> {
        self.cache.stats.incr(&self.cache.stats.cmd_zcard);
        
        if args.len() != 1 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zcard'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let count = match self.cache.get_sorted_set(key) {
            Some(zset) => {
                let set = zset.read().unwrap();
                set.len()
            }
            None => 0,
        };

        Ok(RespValue::Integer(count as i64))
    }

    fn handle_zscore(&self, args: &[RespValue]) -> Result<RespValue> {
        self.cache.stats.incr(&self.cache.stats.cmd_zscore);
        
        if args.len() != 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zscore'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let member = match args[1].as_bulk_string() {
            Some(m) => m,
            None => return Ok(RespValue::error("ERR invalid member")),
        };

        let zset = match self.cache.get_sorted_set(key) {
            Some(z) => z,
            None => return Ok(RespValue::null()),
        };

        let set = zset.read().unwrap();
        match set.score(member) {
            Some(score) => Ok(RespValue::BulkString(Some(Bytes::from(score.to_string())))),
            None => Ok(RespValue::null()),
        }
    }

    fn handle_zrem(&self, args: &[RespValue]) -> Result<RespValue> {
        self.cache.stats.incr(&self.cache.stats.cmd_zrem);
        
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zrem'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let zset = match self.cache.get_sorted_set(key) {
            Some(z) => z,
            None => return Ok(RespValue::Integer(0)),
        };

        let mut set = zset.write().unwrap();
        let mut removed = 0;

        for i in 1..args.len() {
            let member = match args[i].as_bulk_string() {
                Some(m) => m,
                None => continue,
            };

            if set.remove(member) {
                removed += 1;
            }
        }

        Ok(RespValue::Integer(removed as i64))
    }

    fn handle_zrank(&self, args: &[RespValue]) -> Result<RespValue> {
        self.cache.stats.incr(&self.cache.stats.cmd_zrank);
        
        if args.len() != 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zrank'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let member = match args[1].as_bulk_string() {
            Some(m) => m,
            None => return Ok(RespValue::error("ERR invalid member")),
        };

        let zset = match self.cache.get_sorted_set(key) {
            Some(z) => z,
            None => return Ok(RespValue::null()),
        };

        let set = zset.read().unwrap();
        match set.rank(member) {
            Some(rank) => Ok(RespValue::Integer(rank as i64)),
            None => Ok(RespValue::null()),
        }
    }

    fn handle_zrevrank(&self, args: &[RespValue]) -> Result<RespValue> {
        self.cache.stats.incr(&self.cache.stats.cmd_zrevrank);
        
        if args.len() != 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zrevrank'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let member = match args[1].as_bulk_string() {
            Some(m) => m,
            None => return Ok(RespValue::error("ERR invalid member")),
        };

        let zset = match self.cache.get_sorted_set(key) {
            Some(z) => z,
            None => return Ok(RespValue::null()),
        };

        let set = zset.read().unwrap();
        match set.rev_rank(member) {
            Some(rank) => Ok(RespValue::Integer(rank as i64)),
            None => Ok(RespValue::null()),
        }
    }

    // Helper method to parse float from RespValue
    fn parse_float(&self, value: &RespValue) -> Result<f64> {
        match value.as_bulk_string() {
            Some(bytes) => {
                let s = String::from_utf8_lossy(bytes);
                s.parse::<f64>()
                    .map_err(|_| Error::InvalidArgument("not a valid float".into()))
            }
            None => Err(Error::InvalidArgument("not a bulk string".into())),
        }
    }
}
