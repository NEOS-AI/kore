use crate::error::{Error, Result};
use crate::protocol::RespValue;
use bytes::Bytes;
use std::sync::atomic::Ordering;
use super::CommandHandler;

impl CommandHandler {
    pub(super) fn handle_dbsize(&self, _args: &[RespValue]) -> Result<RespValue> {
        let size = self.cache.dbsize();
        Ok(RespValue::Integer(size as i64))
    }

    pub(super) fn handle_keys(&self, args: &[RespValue]) -> Result<RespValue> {
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

    pub(super) fn handle_flush(&self, _args: &[RespValue]) -> Result<RespValue> {
        self.cache.flush();
        Ok(RespValue::ok())
    }

    pub(super) fn handle_info(&self, _args: &[RespValue]) -> Result<RespValue> {
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

    pub(super) fn handle_sweep(&self, _args: &[RespValue]) -> Result<RespValue> {
        let removed = self.cache.sweep();
        Ok(RespValue::Integer(removed as i64))
    }

    pub(super) fn handle_config(&self, args: &[RespValue]) -> Result<RespValue> {
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
}
