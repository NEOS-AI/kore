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

    /// LREM key count element
    pub(super) fn handle_lrem(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'lrem' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_list_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let count = match self.parse_integer(&args[1]) {
            Ok(c) => c,
            Err(_) => {
                return Ok(RespValue::error(
                    "ERR value is not an integer or out of range",
                ));
            }
        };
        let element = match args[2].as_bulk_string() {
            Some(e) => e,
            None => return Ok(RespValue::error("ERR invalid value")),
        };
        let list = match self.cache.get_list(key) {
            Some(l) => l,
            None => return Ok(RespValue::Integer(0)),
        };
        let mut l = list.write();
        let before = crate::memory::estimate_keyed_object(key.len(), l.memory_size());
        let removed = l.lrem(count, element);
        let empty = l.is_empty();
        let after = crate::memory::estimate_keyed_object(key.len(), l.memory_size());
        drop(l);
        self.cache.account_list_delta(before, after);
        if empty {
            self.cache.remove_list(key);
        }
        Ok(RespValue::Integer(removed as i64))
    }

    /// LTRIM key start stop
    pub(super) fn handle_ltrim(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'ltrim' command",
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
            Ok(i) => i as isize,
            Err(_) => {
                return Ok(RespValue::error(
                    "ERR value is not an integer or out of range",
                ));
            }
        };
        let stop = match self.parse_integer(&args[2]) {
            Ok(i) => i as isize,
            Err(_) => {
                return Ok(RespValue::error(
                    "ERR value is not an integer or out of range",
                ));
            }
        };
        let list = match self.cache.get_list(key) {
            Some(l) => l,
            None => return Ok(RespValue::ok()),
        };
        let mut l = list.write();
        let before = crate::memory::estimate_keyed_object(key.len(), l.memory_size());
        l.ltrim(start, stop);
        let empty = l.is_empty();
        let after = crate::memory::estimate_keyed_object(key.len(), l.memory_size());
        drop(l);
        self.cache.account_list_delta(before, after);
        if empty {
            self.cache.remove_list(key);
        }
        Ok(RespValue::ok())
    }

    /// LINSERT key BEFORE|AFTER pivot element
    pub(super) fn handle_linsert(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 4 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'linsert' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_list_key(&key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let where_ = match args[1].as_bulk_string() {
            Some(w) => String::from_utf8_lossy(w).to_uppercase(),
            None => return Ok(RespValue::error("ERR syntax error")),
        };
        let before = match where_.as_str() {
            "BEFORE" => true,
            "AFTER" => false,
            _ => return Ok(RespValue::error("ERR syntax error")),
        };
        let pivot = match args[2].as_bulk_string() {
            Some(p) => p,
            None => return Ok(RespValue::error("ERR invalid pivot")),
        };
        let element = match args[3].as_bulk_string() {
            Some(e) => e.clone(),
            None => return Ok(RespValue::error("ERR invalid value")),
        };

        let list = match self.cache.get_list(&key) {
            Some(l) => l,
            // Redis: missing key → 0 (not created).
            None => return Ok(RespValue::Integer(0)),
        };

        // Capacity check only if pivot exists (cheap pre-check).
        {
            let l = list.read();
            if l.iter_items().any(|v| &v == pivot) {
                let est = element.len() + 16;
                drop(l);
                if let Err(e) = self.cache.ensure_non_string_capacity(est) {
                    return Ok(RespValue::error(e.to_resp_string()));
                }
            }
        }

        let mut l = list.write();
        let before_mem = crate::memory::estimate_keyed_object(key.len(), l.memory_size());
        match l.linsert(before, pivot, element) {
            Some(len) => {
                let after = crate::memory::estimate_keyed_object(key.len(), l.memory_size());
                drop(l);
                self.cache.account_list_delta(before_mem, after);
                self.cache.list_blockers.notify_key(&key);
                Ok(RespValue::Integer(len as i64))
            }
            None => Ok(RespValue::Integer(-1)),
        }
    }

    /// LPOS key element [RANK rank] [COUNT num] [MAXLEN len]
    pub(super) fn handle_lpos(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'lpos' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_list_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let element = match args[1].as_bulk_string() {
            Some(e) => e,
            None => return Ok(RespValue::error("ERR invalid value")),
        };

        let mut rank: i64 = 1;
        let mut count: Option<usize> = None;
        let mut maxlen: usize = 0;
        let mut i = 2;
        while i < args.len() {
            let opt = match args[i].as_bulk_string() {
                Some(b) => String::from_utf8_lossy(b).to_ascii_uppercase(),
                None => return Ok(RespValue::error("ERR syntax error")),
            };
            i += 1;
            if i >= args.len() {
                return Ok(RespValue::error("ERR syntax error"));
            }
            match opt.as_str() {
                "RANK" => {
                    rank = match self.parse_integer(&args[i]) {
                        Ok(r) if r != 0 => r,
                        Ok(_) => {
                            return Ok(RespValue::error(
                                "ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list",
                            ))
                        }
                        Err(_) => {
                            return Ok(RespValue::error(
                                "ERR value is not an integer or out of range",
                            ))
                        }
                    };
                }
                "COUNT" => {
                    let c = match self.parse_integer(&args[i]) {
                        Ok(c) if c >= 0 => c as usize,
                        Ok(_) => {
                            return Ok(RespValue::error(
                                "ERR COUNT can't be negative",
                            ))
                        }
                        Err(_) => {
                            return Ok(RespValue::error(
                                "ERR value is not an integer or out of range",
                            ))
                        }
                    };
                    count = Some(c);
                }
                "MAXLEN" => {
                    maxlen = match self.parse_integer(&args[i]) {
                        Ok(m) if m >= 0 => m as usize,
                        Ok(_) => {
                            return Ok(RespValue::error(
                                "ERR MAXLEN can't be negative",
                            ))
                        }
                        Err(_) => {
                            return Ok(RespValue::error(
                                "ERR value is not an integer or out of range",
                            ))
                        }
                    };
                }
                _ => return Ok(RespValue::error("ERR syntax error")),
            }
            i += 1;
        }

        let indices = match self.cache.get_list(key) {
            Some(list) => {
                let l = list.read();
                // Without COUNT, request a single match (count=1).
                let want = count.unwrap_or(1);
                l.lpos(element, rank, want, maxlen)
            }
            None => Vec::new(),
        };

        match count {
            None => match indices.first() {
                Some(&idx) => Ok(RespValue::Integer(idx as i64)),
                None => Ok(RespValue::null()),
            },
            Some(_) => Ok(RespValue::Array(
                indices
                    .into_iter()
                    .map(|idx| RespValue::Integer(idx as i64))
                    .collect(),
            )),
        }
    }

    /// LMOVE source destination LEFT|RIGHT LEFT|RIGHT
    pub(super) fn handle_lmove(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 4 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'lmove' command",
            ));
        }
        let source = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid source key")),
        };
        let dest = match args[1].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid destination key")),
        };
        let from_left = match parse_list_side(&args[2]) {
            Ok(v) => v,
            Err(e) => return Ok(RespValue::error(e)),
        };
        let to_left = match parse_list_side(&args[3]) {
            Ok(v) => v,
            Err(e) => return Ok(RespValue::error(e)),
        };
        match self.do_lmove(&source, &dest, from_left, to_left) {
            Ok(v) => Ok(v),
            Err(e) => Ok(RespValue::error(e)),
        }
    }

    /// BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
    pub(super) async fn handle_blmove(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 5 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'blmove' command",
            ));
        }
        let source = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid source key")),
        };
        let dest = match args[1].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid destination key")),
        };
        let from_left = match parse_list_side(&args[2]) {
            Ok(v) => v,
            Err(e) => return Ok(RespValue::error(e)),
        };
        let to_left = match parse_list_side(&args[3]) {
            Ok(v) => v,
            Err(e) => return Ok(RespValue::error(e)),
        };
        let timeout_secs = match Self::parse_timeout_seconds(&args[4]) {
            Ok(t) => t,
            Err(e) => return Ok(RespValue::error(e)),
        };

        // Type-check before blocking.
        if let Err(Error::WrongType) = self.ensure_list_key(&source) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        match self.cache.key_type(&dest) {
            KeyType::None | KeyType::List => {}
            _ => return Ok(RespValue::error(Error::WrongType.to_resp_string())),
        }

        // Immediate try
        match self.do_lmove(&source, &dest, from_left, to_left) {
            Ok(RespValue::BulkString(None)) => {}
            Ok(v) => return Ok(v),
            Err(e) => return Ok(RespValue::error(e)),
        }

        if self.executing_multi {
            return Ok(RespValue::null());
        }

        let block_forever = timeout_secs == 0.0;
        let deadline = if block_forever {
            None
        } else {
            Some(Instant::now() + Duration::from_secs_f64(timeout_secs))
        };

        let keys = [source.clone()];
        let (waiter_id, notify) = self.cache.list_blockers.register(&keys);

        let result = loop {
            match self.do_lmove(&source, &dest, from_left, to_left) {
                Ok(RespValue::BulkString(None)) => {}
                Ok(v) => break Ok(v),
                Err(e) => break Ok(RespValue::error(e)),
            }

            if let Some(dl) = deadline {
                let now = Instant::now();
                if now >= dl {
                    break Ok(RespValue::null());
                }
                let remaining = dl - now;
                match tokio::time::timeout(remaining, notify.notified()).await {
                    Ok(()) => continue,
                    Err(_) => break Ok(RespValue::null()),
                }
            } else {
                notify.notified().await;
            }
        };

        self.cache.list_blockers.unregister(waiter_id, &keys);
        result
    }

    /// Core LMOVE: pop from `source` and push onto `dest`.
    /// Returns bulk element, null if source empty, or Err string for WRONGTYPE/OOM.
    fn do_lmove(
        &self,
        source: &Bytes,
        dest: &Bytes,
        from_left: bool,
        to_left: bool,
    ) -> std::result::Result<RespValue, String> {
        match self.cache.key_type(source) {
            KeyType::None => return Ok(RespValue::null()),
            KeyType::List => {}
            _ => return Err(Error::WrongType.to_resp_string()),
        }
        match self.cache.key_type(dest) {
            KeyType::None | KeyType::List => {}
            _ => return Err(Error::WrongType.to_resp_string()),
        }

        // Same key: rotate under one lock.
        if source == dest {
            let list = match self.cache.get_list(source) {
                Some(l) => l,
                None => return Ok(RespValue::null()),
            };
            let mut l = list.write();
            if l.is_empty() {
                return Ok(RespValue::null());
            }
            let before = crate::memory::estimate_keyed_object(source.len(), l.memory_size());
            let val = if from_left {
                l.lpop().unwrap()
            } else {
                l.rpop().unwrap()
            };
            if to_left {
                let _ = l.lpush([val.clone()]);
            } else {
                let _ = l.rpush([val.clone()]);
            }
            let after = crate::memory::estimate_keyed_object(source.len(), l.memory_size());
            drop(l);
            self.cache.account_list_delta(before, after);
            self.cache.list_blockers.notify_key(source);
            return Ok(RespValue::BulkString(Some(val)));
        }

        // Different keys: pop source, then push dest.
        let src_list = match self.cache.get_list(source) {
            Some(l) => l,
            None => return Ok(RespValue::null()),
        };

        let val = {
            let mut l = src_list.write();
            if l.is_empty() {
                return Ok(RespValue::null());
            }
            let before = crate::memory::estimate_keyed_object(source.len(), l.memory_size());
            let v = if from_left { l.lpop() } else { l.rpop() }.unwrap();
            let empty = l.is_empty();
            let after = crate::memory::estimate_keyed_object(source.len(), l.memory_size());
            drop(l);
            self.cache.account_list_delta(before, after);
            if empty {
                self.cache.remove_list(source);
            }
            v
        };

        let est = val.len() + 16;
        if let Err(e) = self.cache.ensure_non_string_capacity(est) {
            // Best-effort: element already left source (same as Redis single-thread
            // atomicity — we accept rare OOM after pop under multi-thread pressure).
            return Err(e.to_resp_string());
        }

        let dest_list = match self.cache.get_or_create_list(dest) {
            Ok(l) => l,
            Err(Error::WrongType) => return Err(Error::WrongType.to_resp_string()),
            Err(e) => return Err(e.to_resp_string()),
        };
        let mut d = dest_list.write();
        let before = crate::memory::estimate_keyed_object(dest.len(), d.memory_size());
        if to_left {
            let _ = d.lpush([val.clone()]);
        } else {
            let _ = d.rpush([val.clone()]);
        }
        let after = crate::memory::estimate_keyed_object(dest.len(), d.memory_size());
        drop(d);
        self.cache.account_list_delta(before, after);
        self.cache.list_blockers.notify_key(dest);
        Ok(RespValue::BulkString(Some(val)))
    }
}

fn parse_list_side(value: &RespValue) -> std::result::Result<bool, String> {
    match value.as_bulk_string() {
        Some(b) => match String::from_utf8_lossy(b).to_ascii_uppercase().as_str() {
            "LEFT" => Ok(true),
            "RIGHT" => Ok(false),
            _ => Err("ERR syntax error".into()),
        },
        None => Err("ERR syntax error".into()),
    }
}
