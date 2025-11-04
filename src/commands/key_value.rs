use crate::entry::StoreOptions;
use crate::entry::LoadOptions;
use crate::error::{Error, Result};
use crate::protocol::RespValue;
use bytes::Bytes;
use super::CommandHandler;

impl CommandHandler {
    pub(super) fn handle_set(&self, args: &[RespValue]) -> Result<RespValue> {
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
                    // GET option: return old value
                    if let Some(entry) = old_value {
                        Ok(RespValue::BulkString(Some(entry.value.clone())))
                    } else {
                        Ok(RespValue::null())
                    }
                } else if opts.nx {
                    // NX option: return null if key existed (failed), OK if set successfully
                    if old_value.is_some() {
                        Ok(RespValue::null())
                    } else {
                        Ok(RespValue::ok())
                    }
                } else {
                    Ok(RespValue::ok())
                }
            }
            Err(Error::OutOfMemory) => Ok(RespValue::error("OOM command not allowed when used memory > 'maxmemory'")),
            Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
        }
    }

    pub(super) fn handle_get(&self, args: &[RespValue]) -> Result<RespValue> {
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

    pub(super) fn handle_del(&self, args: &[RespValue]) -> Result<RespValue> {
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

    pub(super) fn handle_exists(&self, args: &[RespValue]) -> Result<RespValue> {
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

    pub(super) fn handle_mget(&self, args: &[RespValue]) -> Result<RespValue> {
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

    pub(super) fn handle_mset(&self, args: &[RespValue]) -> Result<RespValue> {
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

    /// SETNX - SET if Not eXists (distributed lock primitive)
    /// Returns 1 if the key was set, 0 if the key was not set (already exists)
    pub(super) fn handle_setnx(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'setnx'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let value = match args[1].as_bulk_string() {
            Some(v) => v.clone(),
            None => return Ok(RespValue::error("ERR invalid value")),
        };

        let opts = StoreOptions {
            nx: true,
            ..Default::default()
        };

        match self.cache.store(key, value, opts) {
            Ok(old_value) => {
                // If old_value is None, the key didn't exist and was set successfully
                Ok(RespValue::Integer(if old_value.is_none() { 1 } else { 0 }))
            }
            Err(Error::OutOfMemory) => Ok(RespValue::error("OOM command not allowed when used memory > 'maxmemory'")),
            Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
        }
    }

    /// GETDEL - GET and DELete atomically (useful for distributed lock release)
    /// Returns the value and deletes the key atomically
    pub(super) fn handle_getdel(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'getdel'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        // Get the value first
        match self.cache.load(key, LoadOptions::default())? {
            Some(entry) => {
                let value = entry.value.clone();
                // Delete the key
                self.cache.delete(key)?;
                Ok(RespValue::BulkString(Some(value)))
            }
            None => Ok(RespValue::null()),
        }
    }

    /// GETEX - GET with EXpire options (useful for renewing distributed locks)
    /// Returns the value and optionally sets expiration
    pub(super) fn handle_getex(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'getex'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        // Parse expiration options
        let mut ttl_ms: Option<u64> = None;
        let mut exat_ms: Option<u64> = None;
        let mut persist = false;

        let mut i = 1;
        while i < args.len() {
            let opt = match args[i].as_bulk_string() {
                Some(o) => String::from_utf8_lossy(o).to_uppercase(),
                None => return Ok(RespValue::error("ERR invalid option")),
            };

            match opt.as_str() {
                "EX" => {
                    if i + 1 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    let seconds = self.parse_integer(&args[i + 1])?;
                    ttl_ms = Some((seconds * 1000) as u64);
                    i += 1;
                }
                "PX" => {
                    if i + 1 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    let ms = self.parse_integer(&args[i + 1])?;
                    ttl_ms = Some(ms as u64);
                    i += 1;
                }
                "EXAT" => {
                    if i + 1 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    let timestamp = self.parse_integer(&args[i + 1])?;
                    exat_ms = Some((timestamp * 1000) as u64);
                    i += 1;
                }
                "PXAT" => {
                    if i + 1 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    let timestamp = self.parse_integer(&args[i + 1])?;
                    exat_ms = Some(timestamp as u64);
                    i += 1;
                }
                "PERSIST" => {
                    persist = true;
                }
                _ => return Ok(RespValue::error(format!("ERR unknown option '{}'", opt))),
            }

            i += 1;
        }

        // Get the current value
        match self.cache.load(key, LoadOptions::default())? {
            Some(entry) => {
                let value = entry.value.clone();
                
                // Update expiration if requested
                if persist {
                    // Remove expiration by storing with no TTL
                    self.cache.store(key.clone(), value.clone(), StoreOptions::default())?;
                } else if let Some(ms) = ttl_ms {
                    self.cache.expire(key, ms)?;
                } else if let Some(timestamp) = exat_ms {
                    // Calculate TTL from absolute timestamp
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;
                    
                    if timestamp > now {
                        let ttl = timestamp - now;
                        self.cache.expire(key, ttl)?;
                    }
                }
                
                Ok(RespValue::BulkString(Some(value)))
            }
            None => Ok(RespValue::null()),
        }
    }
}
