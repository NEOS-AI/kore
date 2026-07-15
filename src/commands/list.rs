use crate::cache::KeyType;
use crate::error::{Error, Result};
use crate::protocol::RespValue;
use bytes::Bytes;
use std::time::{Duration, Instant};
use super::CommandHandler;

impl CommandHandler {
    fn ensure_list_key(&self, key: &Bytes) -> Result<Option<()>> {
        match self.cache.key_type(key) {
            KeyType::None => Ok(None),
            KeyType::List => Ok(Some(())),
            _ => Err(Error::WrongType),
        }
    }

    fn parse_list_values(args: &[RespValue]) -> std::result::Result<Vec<Bytes>, String> {
        let mut vals = Vec::with_capacity(args.len());
        for a in args {
            match a.as_bulk_string() {
                Some(v) => vals.push(v.clone()),
                None => return Err("ERR invalid value".into()),
            }
        }
        Ok(vals)
    }

    /// LPUSH key element [element ...]
    pub(super) fn handle_lpush(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'lpush' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let values = match Self::parse_list_values(&args[1..]) {
            Ok(v) => v,
            Err(e) => return Ok(RespValue::error(e)),
        };
        let est: usize = values.iter().map(|v| v.len() + 16).sum();
        if let Err(e) = self.cache.ensure_non_string_capacity(est) {
            return Ok(RespValue::error(e.to_resp_string()));
        }
        let list = match self.cache.get_or_create_list(&key) {
            Ok(l) => l,
            Err(Error::WrongType) => {
                return Ok(RespValue::error(Error::WrongType.to_resp_string()));
            }
            Err(e) => return Ok(RespValue::error(e.to_resp_string())),
        };
        let mut l = list.write();
        let before = crate::memory::estimate_keyed_object(key.len(), l.memory_size());
        let len = l.lpush(values) as i64;
        let after = crate::memory::estimate_keyed_object(key.len(), l.memory_size());
        drop(l);
        self.cache.account_list_delta(before, after);
        // Wake BLPOP/BRPOP waiters on this key
        self.cache.list_blockers.notify_key(&key);
        Ok(RespValue::Integer(len))
    }

    /// RPUSH key element [element ...]
    pub(super) fn handle_rpush(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'rpush' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let values = match Self::parse_list_values(&args[1..]) {
            Ok(v) => v,
            Err(e) => return Ok(RespValue::error(e)),
        };
        let est: usize = values.iter().map(|v| v.len() + 16).sum();
        if let Err(e) = self.cache.ensure_non_string_capacity(est) {
            return Ok(RespValue::error(e.to_resp_string()));
        }
        let list = match self.cache.get_or_create_list(&key) {
            Ok(l) => l,
            Err(Error::WrongType) => {
                return Ok(RespValue::error(Error::WrongType.to_resp_string()));
            }
            Err(e) => return Ok(RespValue::error(e.to_resp_string())),
        };
        let mut l = list.write();
        let before = crate::memory::estimate_keyed_object(key.len(), l.memory_size());
        let len = l.rpush(values) as i64;
        let after = crate::memory::estimate_keyed_object(key.len(), l.memory_size());
        drop(l);
        self.cache.account_list_delta(before, after);
        self.cache.list_blockers.notify_key(&key);
        Ok(RespValue::Integer(len))
    }

    /// Try to pop one element from the first non-empty list among `keys`.
    /// Returns `Ok(Some((key, value)))` or `Ok(None)` if all empty/missing.
    /// Callers must have already rejected WrongType keys.
    fn try_blocking_pop(
        &self,
        keys: &[Bytes],
        from_left: bool,
    ) -> Option<(Bytes, Bytes)> {
        for key in keys {
            if !matches!(self.cache.key_type(key), KeyType::List) {
                continue;
            }
            let Some(list) = self.cache.get_list(key) else {
                continue;
            };
            let mut l = list.write();
            let before = crate::memory::estimate_keyed_object(key.len(), l.memory_size());
            let val = if from_left { l.lpop() } else { l.rpop() };
            if let Some(v) = val {
                let empty = l.is_empty();
                let after = crate::memory::estimate_keyed_object(key.len(), l.memory_size());
                drop(l);
                self.cache.account_list_delta(before, after);
                if empty {
                    self.cache.remove_list(key);
                }
                return Some((key.clone(), v));
            }
        }
        None
    }

    /// BLPOP key [key ...] timeout
    pub(super) async fn handle_blpop(&self, args: &[RespValue]) -> Result<RespValue> {
        self.handle_blocking_pop(args, true).await
    }

    /// BRPOP key [key ...] timeout
    pub(super) async fn handle_brpop(&self, args: &[RespValue]) -> Result<RespValue> {
        self.handle_blocking_pop(args, false).await
    }

    async fn handle_blocking_pop(
        &self,
        args: &[RespValue],
        from_left: bool,
    ) -> Result<RespValue> {
        let cmd = if from_left { "blpop" } else { "brpop" };
        if args.len() < 2 {
            return Ok(RespValue::error(format!(
                "ERR wrong number of arguments for '{}' command",
                cmd
            )));
        }

        // Last arg is timeout (seconds); remaining are keys
        let timeout_arg = &args[args.len() - 1];
        let timeout_secs = match Self::parse_timeout_seconds(timeout_arg) {
            Ok(t) => t,
            Err(e) => return Ok(RespValue::error(e)),
        };

        let mut keys = Vec::with_capacity(args.len() - 1);
        for a in &args[..args.len() - 1] {
            match a.as_bulk_string() {
                Some(k) => keys.push(k.clone()),
                None => return Ok(RespValue::error("ERR invalid key")),
            }
        }

        // WrongType on any key that exists with a non-list type
        for key in &keys {
            if let Err(Error::WrongType) = self.ensure_list_key(key) {
                return Ok(RespValue::error(Error::WrongType.to_resp_string()));
            }
        }

        // Immediate try
        if let Some((key, val)) = self.try_blocking_pop(&keys, from_left) {
            return Ok(RespValue::Array(vec![
                RespValue::BulkString(Some(key)),
                RespValue::BulkString(Some(val)),
            ]));
        }

        // Inside MULTI/EXEC: never block (Redis treats as non-blocking)
        if self.executing_multi {
            return Ok(RespValue::null_array());
        }

        // timeout 0 = block forever; >0 = seconds
        let block_forever = timeout_secs == 0.0;
        let deadline = if block_forever {
            None
        } else {
            Some(Instant::now() + Duration::from_secs_f64(timeout_secs))
        };

        let (waiter_id, notify) = self.cache.list_blockers.register(&keys);

        let result = loop {
            // Re-check before sleeping (push may have raced with register)
            if let Some((key, val)) = self.try_blocking_pop(&keys, from_left) {
                break Ok(RespValue::Array(vec![
                    RespValue::BulkString(Some(key)),
                    RespValue::BulkString(Some(val)),
                ]));
            }

            if let Some(dl) = deadline {
                let now = Instant::now();
                if now >= dl {
                    break Ok(RespValue::null_array());
                }
                let remaining = dl - now;
                match tokio::time::timeout(remaining, notify.notified()).await {
                    Ok(()) => continue, // woken — retry pop
                    Err(_) => break Ok(RespValue::null_array()),
                }
            } else {
                notify.notified().await;
            }
        };

        self.cache.list_blockers.unregister(waiter_id, &keys);
        result
    }

    /// Parse BLPOP/BRPOP timeout in seconds (integer or float string; 0 = forever).
    fn parse_timeout_seconds(value: &RespValue) -> std::result::Result<f64, String> {
        if let Some(i) = value.as_integer() {
            if i < 0 {
                return Err("ERR timeout is negative".into());
            }
            return Ok(i as f64);
        }
        if let Some(s) = value.as_bulk_string() {
            let s = std::str::from_utf8(s).map_err(|_| "ERR timeout is not a float or out of range")?;
            let t: f64 = s
                .parse()
                .map_err(|_| "ERR timeout is not a float or out of range")?;
            if t < 0.0 || t.is_nan() {
                return Err("ERR timeout is negative".into());
            }
            return Ok(t);
        }
        Err("ERR timeout is not a float or out of range".into())
    }

    /// LPOP key [count]
    pub(super) fn handle_lpop(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() || args.len() > 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'lpop' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_list_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let count = if args.len() == 2 {
            match self.parse_integer(&args[1]) {
                Ok(c) if c >= 0 => Some(c as usize),
                _ => {
                    return Ok(RespValue::error(
                        "ERR value is not an integer or out of range",
                    ))
                }
            }
        } else {
            None
        };

        let list = match self.cache.get_list(key) {
            Some(l) => l,
            None => return Ok(RespValue::null()),
        };
        let mut l = list.write();
        let before = crate::memory::estimate_keyed_object(key.len(), l.memory_size());
        let resp = match count {
            None => match l.lpop() {
                Some(v) => RespValue::BulkString(Some(v)),
                None => RespValue::null(),
            },
            Some(n) => {
                let items = l.lpop_count(n);
                RespValue::Array(
                    items
                        .into_iter()
                        .map(|v| RespValue::BulkString(Some(v)))
                        .collect(),
                )
            }
        };
        let empty = l.is_empty();
        let after = crate::memory::estimate_keyed_object(key.len(), l.memory_size());
        drop(l);
        self.cache.account_list_delta(before, after);
        if empty {
            self.cache.remove_list(key);
        }
        Ok(resp)
    }

    /// RPOP key [count]
    pub(super) fn handle_rpop(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() || args.len() > 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'rpop' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_list_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let count = if args.len() == 2 {
            match self.parse_integer(&args[1]) {
                Ok(c) if c >= 0 => Some(c as usize),
                _ => {
                    return Ok(RespValue::error(
                        "ERR value is not an integer or out of range",
                    ))
                }
            }
        } else {
            None
        };

        let list = match self.cache.get_list(key) {
            Some(l) => l,
            None => return Ok(RespValue::null()),
        };
        let mut l = list.write();
        let before = crate::memory::estimate_keyed_object(key.len(), l.memory_size());
        let resp = match count {
            None => match l.rpop() {
                Some(v) => RespValue::BulkString(Some(v)),
                None => RespValue::null(),
            },
            Some(n) => {
                let items = l.rpop_count(n);
                RespValue::Array(
                    items
                        .into_iter()
                        .map(|v| RespValue::BulkString(Some(v)))
                        .collect(),
                )
            }
        };
        let empty = l.is_empty();
        let after = crate::memory::estimate_keyed_object(key.len(), l.memory_size());
        drop(l);
        self.cache.account_list_delta(before, after);
        if empty {
            self.cache.remove_list(key);
        }
        Ok(resp)
    }

    /// LRANGE key start stop
    pub(super) fn handle_lrange(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'lrange' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_list_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let start = match self.parse_integer(&args[1]) {
            Ok(s) => s as isize,
            Err(_) => {
                return Ok(RespValue::error(
                    "ERR value is not an integer or out of range",
                ))
            }
        };
        let stop = match self.parse_integer(&args[2]) {
            Ok(s) => s as isize,
            Err(_) => {
                return Ok(RespValue::error(
                    "ERR value is not an integer or out of range",
                ))
            }
        };
        match self.cache.get_list(key) {
            Some(l) => {
                let list = l.read();
                Ok(RespValue::Array(
                    list.lrange(start, stop)
                        .into_iter()
                        .map(|v| RespValue::BulkString(Some(v)))
                        .collect(),
                ))
            }
            None => Ok(RespValue::Array(vec![])),
        }
    }

    /// LLEN key
    pub(super) fn handle_llen(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'llen' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_list_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let n = self
            .cache
            .get_list(key)
            .map(|l| l.read().llen())
            .unwrap_or(0);
        Ok(RespValue::Integer(n as i64))
    }

    /// LINDEX key index
    pub(super) fn handle_lindex(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'lindex' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_list_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let index = match self.parse_integer(&args[1]) {
            Ok(i) => i as isize,
            Err(_) => {
                return Ok(RespValue::error(
                    "ERR value is not an integer or out of range",
                ))
            }
        };
        match self.cache.get_list(key) {
            Some(l) => {
                let list = l.read();
                match list.lindex(index) {
                    Some(v) => Ok(RespValue::BulkString(Some(v))),
                    None => Ok(RespValue::null()),
                }
            }
            None => Ok(RespValue::null()),
        }
    }

    /// LSET key index element
    pub(super) fn handle_lset(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'lset' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_list_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let index = match self.parse_integer(&args[1]) {
            Ok(i) => i as isize,
            Err(_) => {
                return Ok(RespValue::error(
                    "ERR value is not an integer or out of range",
                ))
            }
        };
        let value = match args[2].as_bulk_string() {
            Some(v) => v.clone(),
            None => return Ok(RespValue::error("ERR invalid value")),
        };
        let list = match self.cache.get_list(key) {
            Some(l) => l,
            None => return Ok(RespValue::error("ERR no such key")),
        };
        let mut l = list.write();
        let before = crate::memory::estimate_keyed_object(key.len(), l.memory_size());
        match l.lset(index, value) {
            Ok(()) => {
                let after = crate::memory::estimate_keyed_object(key.len(), l.memory_size());
                drop(l);
                self.cache.account_list_delta(before, after);
                Ok(RespValue::ok())
            }
            Err(msg) => Ok(RespValue::error(format!("ERR {}", msg))),
        }
    }
}
