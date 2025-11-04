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
}
