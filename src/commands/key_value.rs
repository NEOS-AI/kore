use crate::cache::KeyType;
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

        // Type safety: refuse to overwrite zset/geo keys as strings
        if let Err(Error::WrongType) = self.cache.ensure_string_or_absent(&key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

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
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
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

        // GET on non-string keys is WRONGTYPE (not null)
        match self.cache.key_type(key) {
            KeyType::None | KeyType::String => {}
            _ => {
                return Ok(RespValue::error(Error::WrongType.to_resp_string()));
            }
        }

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

            if let Err(Error::WrongType) = self.cache.ensure_string_or_absent(&key) {
                return Ok(RespValue::error(Error::WrongType.to_resp_string()));
            }

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

        if let Err(Error::WrongType) = self.cache.ensure_string_or_absent(&key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

        let opts = StoreOptions {
            nx: true,
            ..Default::default()
        };

        match self.cache.store(key, value, opts) {
            Ok(old_value) => {
                // If old_value is None, the key didn't exist and was set successfully
                Ok(RespValue::Integer(if old_value.is_none() { 1 } else { 0 }))
            }
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
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

        match self.cache.key_type(key) {
            KeyType::None | KeyType::String => {}
            _ => {
                return Ok(RespValue::error(Error::WrongType.to_resp_string()));
            }
        }

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

        match self.cache.key_type(key) {
            KeyType::None | KeyType::String => {}
            _ => {
                return Ok(RespValue::error(Error::WrongType.to_resp_string()));
            }
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

    /// TYPE - return the type of value stored at key
    pub(super) fn handle_type(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'type'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let type_str = self.cache.key_type(key).as_redis_str();
        Ok(RespValue::SimpleString(Bytes::from_static(match type_str {
            "string" => b"string",
            "zset" => b"zset",
            "hash" => b"hash",
            "list" => b"list",
            "set" => b"set",
            "stream" => b"stream",
            _ => b"none",
        })))
    }

    /// APPEND key value — append to string; create if missing. Returns new length.
    pub(super) fn handle_append(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'append' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let value = match args[1].as_bulk_string() {
            Some(v) => v,
            None => return Ok(RespValue::error("ERR invalid value")),
        };

        match self.cache.append(key, value) {
            Ok(len) => Ok(RespValue::Integer(len as i64)),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    /// GETRANGE key start end — substring (inclusive Redis indices). Missing → empty bulk.
    pub(super) fn handle_getrange(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'getrange' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        match self.cache.key_type(key) {
            KeyType::None | KeyType::String => {}
            _ => return Ok(RespValue::error(Error::WrongType.to_resp_string())),
        }
        let start = match self.parse_integer(&args[1]) {
            Ok(i) => i as isize,
            Err(_) => {
                return Ok(RespValue::error(
                    "ERR value is not an integer or out of range",
                ));
            }
        };
        let end = match self.parse_integer(&args[2]) {
            Ok(i) => i as isize,
            Err(_) => {
                return Ok(RespValue::error(
                    "ERR value is not an integer or out of range",
                ));
            }
        };

        let value = match self.cache.load(key, LoadOptions::default())? {
            Some(entry) => entry.value.clone(),
            None => return Ok(RespValue::BulkString(Some(Bytes::new()))),
        };
        Ok(RespValue::BulkString(Some(substr_range(&value, start, end))))
    }

    /// SETRANGE key offset value — overwrite/pad from offset; returns new length.
    pub(super) fn handle_setrange(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'setrange' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let offset = match self.parse_integer(&args[1]) {
            Ok(n) if n >= 0 => n as usize,
            Ok(_) => {
                return Ok(RespValue::error(
                    "ERR offset is out of range",
                ));
            }
            Err(_) => {
                return Ok(RespValue::error(
                    "ERR value is not an integer or out of range",
                ));
            }
        };
        let value = match args[2].as_bulk_string() {
            Some(v) => v,
            None => return Ok(RespValue::error("ERR invalid value")),
        };

        match self.cache.setrange(key, offset, value) {
            Ok(len) => Ok(RespValue::Integer(len as i64)),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    /// MSETNX key value [key value ...] — set all only if none of the keys exist.
    pub(super) fn handle_msetnx(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() || args.len() % 2 != 0 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'msetnx' command",
            ));
        }

        let mut pairs: Vec<(Bytes, Bytes)> = Vec::with_capacity(args.len() / 2);
        for i in (0..args.len()).step_by(2) {
            let key = match args[i].as_bulk_string() {
                Some(k) => k.clone(),
                None => return Ok(RespValue::error("ERR invalid key")),
            };
            let value = match args[i + 1].as_bulk_string() {
                Some(v) => v.clone(),
                None => return Ok(RespValue::error("ERR invalid value")),
            };
            pairs.push((key, value));
        }

        // Any existing key (any type) aborts the whole op.
        for (key, _) in &pairs {
            if self.cache.exists(key) {
                return Ok(RespValue::Integer(0));
            }
        }

        for (key, value) in pairs {
            // Re-check NX under store; best-effort atomicity across shards.
            let opts = StoreOptions {
                nx: true,
                ..Default::default()
            };
            match self.cache.store(key, value, opts) {
                Ok(old) if old.is_none() => {}
                Ok(_) => {
                    // Concurrent create raced; treat as failed NX for this key.
                    // Already-written keys from this command remain (shard-local).
                    return Ok(RespValue::Integer(0));
                }
                Err(e) => return Ok(RespValue::error(e.to_resp_string())),
            }
        }
        Ok(RespValue::Integer(1))
    }

    /// LCS key1 key2 [LEN]
    /// Longest common subsequence of two string keys (missing key = empty string).
    /// Without options: bulk LCS string. With `LEN`: integer length.
    pub(super) fn handle_lcs(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'lcs' command",
            ));
        }
        let key1 = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let key2 = match args[1].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let mut len_only = false;
        let mut i = 2;
        while i < args.len() {
            let opt = match args[i].as_bulk_string() {
                Some(s) => String::from_utf8_lossy(s).to_ascii_uppercase(),
                None => return Ok(RespValue::error("ERR syntax error")),
            };
            match opt.as_str() {
                "LEN" => {
                    len_only = true;
                    i += 1;
                }
                // IDX / MINMATCHLEN / WITHMATCHLEN not implemented in this batch.
                "IDX" | "MINMATCHLEN" | "WITHMATCHLEN" => {
                    return Ok(RespValue::error(
                        "ERR LCS IDX/MINMATCHLEN/WITHMATCHLEN not supported",
                    ));
                }
                _ => return Ok(RespValue::error("ERR syntax error")),
            }
        }

        for key in [key1, key2] {
            match self.cache.key_type(key) {
                KeyType::None | KeyType::String => {}
                _ => return Ok(RespValue::error(Error::WrongType.to_resp_string())),
            }
        }

        let a = match self.cache.load(key1, LoadOptions::default())? {
            Some(e) => e.value.clone(),
            None => Bytes::new(),
        };
        let b = match self.cache.load(key2, LoadOptions::default())? {
            Some(e) => e.value.clone(),
            None => Bytes::new(),
        };

        let lcs = compute_lcs(a.as_ref(), b.as_ref());
        if len_only {
            Ok(RespValue::Integer(lcs.len() as i64))
        } else {
            Ok(RespValue::BulkString(Some(Bytes::from(lcs))))
        }
    }

    /// STRLEN key — length of string value (0 if missing).
    pub(super) fn handle_strlen(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'strlen' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        match self.cache.key_type(key) {
            KeyType::None => return Ok(RespValue::Integer(0)),
            KeyType::String => {}
            _ => return Ok(RespValue::error(Error::WrongType.to_resp_string())),
        }

        match self.cache.load(key, LoadOptions::default())? {
            Some(entry) => Ok(RespValue::Integer(entry.value.len() as i64)),
            None => Ok(RespValue::Integer(0)),
        }
    }

    /// SETEX key seconds value — SET with EX seconds.
    pub(super) fn handle_setex(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'setex' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let seconds = match self.parse_integer(&args[1]) {
            Ok(s) if s > 0 => s,
            Ok(_) => {
                return Ok(RespValue::error(
                    "ERR invalid expire time in 'setex' command",
                ));
            }
            Err(_) => {
                return Ok(RespValue::error(
                    "ERR value is not an integer or out of range",
                ));
            }
        };
        let value = match args[2].as_bulk_string() {
            Some(v) => v.clone(),
            None => return Ok(RespValue::error("ERR invalid value")),
        };

        if let Err(Error::WrongType) = self.cache.ensure_string_or_absent(&key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

        let opts = StoreOptions {
            ttl_ms: Some((seconds as u64) * 1000),
            ..Default::default()
        };
        match self.cache.store(key, value, opts) {
            Ok(_) => Ok(RespValue::ok()),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    /// GETSET key value — set new value, return old (or null).
    pub(super) fn handle_getset(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'getset' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let value = match args[1].as_bulk_string() {
            Some(v) => v.clone(),
            None => return Ok(RespValue::error("ERR invalid value")),
        };

        if let Err(Error::WrongType) = self.cache.ensure_string_or_absent(&key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

        let opts = StoreOptions {
            get: true,
            ..Default::default()
        };
        match self.cache.store(key, value, opts) {
            Ok(Some(old)) => Ok(RespValue::BulkString(Some(old.value.clone()))),
            Ok(None) => Ok(RespValue::null()),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    /// UNLINK key [key ...] — same as DEL for now (sync delete).
    pub(super) fn handle_unlink(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'unlink' command",
            ));
        }
        self.handle_del(args)
    }

    /// RENAME key newkey
    pub(super) fn handle_rename(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'rename' command",
            ));
        }
        let src = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let dst = match args[1].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        match self.cache.rename(src, dst, false) {
            Ok(_) => Ok(RespValue::ok()),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    /// RENAMENX key newkey — rename only if newkey does not exist.
    pub(super) fn handle_renamenx(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'renamenx' command",
            ));
        }
        let src = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let dst = match args[1].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        match self.cache.rename(src, dst, true) {
            Ok(true) => Ok(RespValue::Integer(1)),
            Ok(false) => Ok(RespValue::Integer(0)),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }
}


impl CommandHandler {
    /// MOVE key db — transfer key to another logical database.
    pub(super) fn handle_move(&mut self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'move'"));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let db = match self.parse_integer(&args[1]) {
            Ok(n) if n >= 0 => n as usize,
            _ => return Ok(RespValue::error("ERR index out of range")),
        };
        if db == self.selected_db() {
            return Ok(RespValue::error(
                "ERR source and destination objects are the same",
            ));
        }
        let Some(dst) = self.databases().get(db) else {
            return Ok(RespValue::error("ERR index out of range"));
        };
        match self.cache.move_key_to(key, &dst) {
            Ok(moved) => Ok(RespValue::Integer(if moved { 1 } else { 0 })),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    /// COPY source destination [DB destination-db] [REPLACE]
    pub(super) fn handle_copy(&mut self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'copy'"));
        }
        let src = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid source key")),
        };
        let dst = match args[1].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid destination key")),
        };

        let mut dest_db: Option<usize> = None;
        let mut replace = false;
        let mut i = 2;
        while i < args.len() {
            let opt = match args[i].as_bulk_string() {
                Some(s) => String::from_utf8_lossy(s).to_uppercase(),
                None => return Ok(RespValue::error("ERR syntax error")),
            };
            match opt.as_str() {
                "REPLACE" => {
                    replace = true;
                    i += 1;
                }
                "DB" => {
                    i += 1;
                    if i >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    match self.parse_integer(&args[i]) {
                        Ok(n) if n >= 0 => dest_db = Some(n as usize),
                        _ => return Ok(RespValue::error("ERR index out of range")),
                    }
                    i += 1;
                }
                _ => return Ok(RespValue::error("ERR syntax error")),
            }
        }

        if dest_db.is_some() && self.cluster().is_some() {
            return Ok(RespValue::error(
                "ERR COPY to a different DB is not allowed in cluster mode",
            ));
        }

        let dst_cache = if let Some(db) = dest_db {
            match self.databases().get(db) {
                Some(c) => Some(c),
                None => return Ok(RespValue::error("ERR index out of range")),
            }
        } else {
            None
        };

        let result = if let Some(ref target) = dst_cache {
            self.cache.copy_key(src, dst, Some(target.as_ref()), replace)
        } else {
            self.cache.copy_key(src, dst, None, replace)
        };

        match result {
            Ok(copied) => Ok(RespValue::Integer(if copied { 1 } else { 0 })),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    /// RANDOMKEY — random key from the selected DB, or null.
    pub(super) fn handle_randomkey(&self, _args: &[RespValue]) -> Result<RespValue> {
        match self.cache.random_key() {
            Some(k) => Ok(RespValue::BulkString(Some(k))),
            None => Ok(RespValue::null()),
        }
    }

    /// TOUCH key [key ...] — refresh LRU/LFU; return count of existing keys.
    pub(super) fn handle_touch(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'touch'"));
        }
        let mut keys = Vec::with_capacity(args.len());
        for a in args {
            match a.as_bulk_string() {
                Some(k) => keys.push(k.clone()),
                None => return Ok(RespValue::error("ERR invalid key")),
            }
        }
        Ok(RespValue::Integer(self.cache.touch_keys(&keys) as i64))
    }
}

/// Inclusive Redis-style substring of `value` using `start`/`end` indices.
fn substr_range(value: &Bytes, start: isize, end: isize) -> Bytes {
    let len = value.len() as isize;
    if len == 0 {
        return Bytes::new();
    }
    let start_idx = if start < 0 {
        (len + start).max(0)
    } else {
        start
    };
    let end_idx = if end < 0 {
        (len + end).max(0)
    } else {
        end
    };
    if start_idx > end_idx || start_idx >= len {
        return Bytes::new();
    }
    let end_idx = end_idx.min(len - 1);
    value.slice(start_idx as usize..=end_idx as usize)
}

/// Classic DP longest common subsequence (bytes). O(mn) time/space.
fn compute_lcs(a: &[u8], b: &[u8]) -> Vec<u8> {
    let m = a.len();
    let n = b.len();
    if m == 0 || n == 0 {
        return Vec::new();
    }
    // dp[i][j] = LCS length of a[..i], b[..j]
    let mut dp = vec![vec![0u32; n + 1]; m + 1];
    for i in 1..=m {
        for j in 1..=n {
            if a[i - 1] == b[j - 1] {
                dp[i][j] = dp[i - 1][j - 1] + 1;
            } else {
                dp[i][j] = dp[i - 1][j].max(dp[i][j - 1]);
            }
        }
    }
    // Backtrack to reconstruct one LCS
    let mut out = Vec::with_capacity(dp[m][n] as usize);
    let mut i = m;
    let mut j = n;
    while i > 0 && j > 0 {
        if a[i - 1] == b[j - 1] {
            out.push(a[i - 1]);
            i -= 1;
            j -= 1;
        } else if dp[i - 1][j] >= dp[i][j - 1] {
            i -= 1;
        } else {
            j -= 1;
        }
    }
    out.reverse();
    out
}
