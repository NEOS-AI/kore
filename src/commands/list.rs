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

    /// LPUSHX key element [element ...] — push left only if key already holds a list.
    pub(super) fn handle_lpushx(&self, args: &[RespValue]) -> Result<RespValue> {
        self.pushx(args, true, "lpushx")
    }

    /// RPUSHX key element [element ...] — push right only if key already holds a list.
    pub(super) fn handle_rpushx(&self, args: &[RespValue]) -> Result<RespValue> {
        self.pushx(args, false, "rpushx")
    }

    fn pushx(&self, args: &[RespValue], left: bool, cmd: &str) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(format!(
                "ERR wrong number of arguments for '{}' command",
                cmd
            )));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let values = match Self::parse_list_values(&args[1..]) {
            Ok(v) => v,
            Err(e) => return Ok(RespValue::error(e)),
        };

        match self.cache.key_type(&key) {
            KeyType::None => return Ok(RespValue::Integer(0)),
            KeyType::List => {}
            _ => return Ok(RespValue::error(Error::WrongType.to_resp_string())),
        }

        let list = match self.cache.get_list(&key) {
            Some(l) => l,
            None => return Ok(RespValue::Integer(0)),
        };

        let est: usize = values.iter().map(|v| v.len() + 16).sum();
        if let Err(e) = self.cache.ensure_non_string_capacity(est) {
            return Ok(RespValue::error(e.to_resp_string()));
        }

        let mut l = list.write();
        let before = crate::memory::estimate_keyed_object(key.len(), l.memory_size());
        let len = if left {
            l.lpush(values) as i64
        } else {
            l.rpush(values) as i64
        };
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

    /// Parse BLPOP/BRPOP/BZPOP* timeout in seconds (integer or float string; 0 = forever).
    pub(crate) fn parse_timeout_seconds(value: &RespValue) -> std::result::Result<f64, String> {
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

    /// RPOPLPUSH source destination — legacy alias of LMOVE source dest RIGHT LEFT.
    pub(super) fn handle_rpoplpush(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'rpoplpush' command",
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
        match self.do_lmove(&source, &dest, false, true) {
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
        self.do_blmove(source, dest, from_left, to_left, timeout_secs)
            .await
    }

    /// BRPOPLPUSH source destination timeout — legacy alias of BLMOVE … RIGHT LEFT.
    pub(super) async fn handle_brpoplpush(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'brpoplpush' command",
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
        let timeout_secs = match Self::parse_timeout_seconds(&args[2]) {
            Ok(t) => t,
            Err(e) => return Ok(RespValue::error(e)),
        };
        self.do_blmove(source, dest, false, true, timeout_secs).await
    }

    async fn do_blmove(
        &self,
        source: Bytes,
        dest: Bytes,
        from_left: bool,
        to_left: bool,
        timeout_secs: f64,
    ) -> Result<RespValue> {
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

    /// LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
    pub(super) fn handle_lmpop(&self, args: &[RespValue]) -> Result<RespValue> {
        match parse_lmpop_args(self, args, "lmpop") {
            Ok((keys, from_left, count)) => {
                for key in &keys {
                    if let Err(Error::WrongType) = self.ensure_list_key(key) {
                        return Ok(RespValue::error(Error::WrongType.to_resp_string()));
                    }
                }
                match self.try_lmpop(&keys, from_left, count) {
                    Some((key, elems)) => Ok(lmpop_reply(key, elems)),
                    None => Ok(RespValue::null()),
                }
            }
            Err(e) => Ok(RespValue::error(e)),
        }
    }

    /// BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
    pub(super) async fn handle_blmpop(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 4 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'blmpop' command",
            ));
        }
        let timeout_secs = match Self::parse_timeout_seconds(&args[0]) {
            Ok(t) => t,
            Err(e) => return Ok(RespValue::error(e)),
        };
        let (keys, from_left, count) = match parse_lmpop_args(self, &args[1..], "blmpop") {
            Ok(v) => v,
            Err(e) => return Ok(RespValue::error(e)),
        };

        for key in &keys {
            if let Err(Error::WrongType) = self.ensure_list_key(key) {
                return Ok(RespValue::error(Error::WrongType.to_resp_string()));
            }
        }

        if let Some((key, elems)) = self.try_lmpop(&keys, from_left, count) {
            return Ok(lmpop_reply(key, elems));
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

        let (waiter_id, notify) = self.cache.list_blockers.register(&keys);

        let result = loop {
            if let Some((key, elems)) = self.try_lmpop(&keys, from_left, count) {
                break Ok(lmpop_reply(key, elems));
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

    /// Pop up to `count` elements from the first non-empty list (left-to-right keys).
    fn try_lmpop(
        &self,
        keys: &[Bytes],
        from_left: bool,
        count: usize,
    ) -> Option<(Bytes, Vec<Bytes>)> {
        if count == 0 {
            return None;
        }
        for key in keys {
            if !matches!(self.cache.key_type(key), KeyType::List) {
                continue;
            }
            let Some(list) = self.cache.get_list(key) else {
                continue;
            };
            let mut l = list.write();
            if l.is_empty() {
                continue;
            }
            let before = crate::memory::estimate_keyed_object(key.len(), l.memory_size());
            let elems = if from_left {
                l.lpop_count(count)
            } else {
                l.rpop_count(count)
            };
            if elems.is_empty() {
                continue;
            }
            let empty = l.is_empty();
            let after = crate::memory::estimate_keyed_object(key.len(), l.memory_size());
            drop(l);
            self.cache.account_list_delta(before, after);
            if empty {
                self.cache.remove_list(key);
            }
            return Some((key.clone(), elems));
        }
        None
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

    /// SORT key [BY nosort] [LIMIT offset count] [ASC|DESC] [ALPHA] [STORE destination]
    ///
    /// Sorts list/set/zset elements. Numeric by default (double parse); ALPHA for
    /// lexicographic. BY only accepts `nosort` (skip sort). GET patterns not supported.
    pub(super) fn handle_sort(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'sort' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let mut alpha = false;
        let mut desc = false;
        let mut nosort = false;
        let mut limit_offset: usize = 0;
        let mut limit_count: Option<i64> = None; // None = no LIMIT; negative count = all
        let mut store_dest: Option<Bytes> = None;

        let mut i = 1;
        while i < args.len() {
            let opt = match args[i].as_bulk_string() {
                Some(s) => String::from_utf8_lossy(s).to_ascii_uppercase(),
                None => return Ok(RespValue::error("ERR syntax error")),
            };
            match opt.as_str() {
                "ALPHA" => {
                    alpha = true;
                    i += 1;
                }
                "ASC" => {
                    desc = false;
                    i += 1;
                }
                "DESC" => {
                    desc = true;
                    i += 1;
                }
                "BY" => {
                    if i + 1 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    let pattern = match args[i + 1].as_bulk_string() {
                        Some(p) => String::from_utf8_lossy(p).to_ascii_lowercase(),
                        None => return Ok(RespValue::error("ERR syntax error")),
                    };
                    if pattern != "nosort" {
                        return Ok(RespValue::error(
                            "ERR BY pattern not supported (only BY nosort)",
                        ));
                    }
                    nosort = true;
                    i += 2;
                }
                "LIMIT" => {
                    if i + 2 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    let off = match self.parse_integer(&args[i + 1]) {
                        Ok(n) if n >= 0 => n as usize,
                        Ok(_) => {
                            return Ok(RespValue::error(
                                "ERR value is not an integer or out of range",
                            ));
                        }
                        Err(_) => {
                            return Ok(RespValue::error(
                                "ERR value is not an integer or out of range",
                            ));
                        }
                    };
                    let cnt = match self.parse_integer(&args[i + 2]) {
                        Ok(n) => n,
                        Err(_) => {
                            return Ok(RespValue::error(
                                "ERR value is not an integer or out of range",
                            ));
                        }
                    };
                    limit_offset = off;
                    limit_count = Some(cnt);
                    i += 3;
                }
                "STORE" => {
                    if i + 1 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    let dest = match args[i + 1].as_bulk_string() {
                        Some(d) => d.clone(),
                        None => return Ok(RespValue::error("ERR invalid destination key")),
                    };
                    store_dest = Some(dest);
                    i += 2;
                }
                "GET" => {
                    return Ok(RespValue::error(
                        "ERR GET option not supported",
                    ));
                }
                _ => return Ok(RespValue::error("ERR syntax error")),
            }
        }

        // Collect source elements by type.
        let mut elements: Vec<Bytes> = match self.cache.key_type(&key) {
            KeyType::None => Vec::new(),
            KeyType::List => match self.cache.get_list(&key) {
                Some(l) => l.read().lrange(0, -1),
                None => Vec::new(),
            },
            KeyType::Set => match self.cache.get_set(&key) {
                Some(s) => s.read().smembers(),
                None => Vec::new(),
            },
            KeyType::ZSet => match self.cache.get_sorted_set(&key) {
                Some(z) => z
                    .read()
                    .range(0, -1, false)
                    .into_iter()
                    .map(|m| m.member)
                    .collect(),
                None => Vec::new(),
            },
            _ => {
                return Ok(RespValue::error(Error::WrongType.to_resp_string()));
            }
        };

        if !nosort {
            if alpha {
                elements.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
            } else {
                // Numeric sort: parse each as double; fail if any cannot convert.
                let mut scored: Vec<(f64, Bytes)> = Vec::with_capacity(elements.len());
                for el in elements {
                    let s = String::from_utf8_lossy(&el);
                    let n = match s.trim().parse::<f64>() {
                        Ok(v) if v.is_finite() => v,
                        _ => {
                            return Ok(RespValue::error(
                                "ERR One or more scores can't be converted into double",
                            ));
                        }
                    };
                    scored.push((n, el));
                }
                scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                elements = scored.into_iter().map(|(_, b)| b).collect();
            }
            if desc {
                elements.reverse();
            }
        } else if desc {
            // BY nosort + DESC: reverse the source order (Redis behavior).
            elements.reverse();
        }

        // Apply LIMIT offset count.
        if let Some(cnt) = limit_count {
            if limit_offset >= elements.len() {
                elements.clear();
            } else {
                let start = limit_offset;
                let end = if cnt < 0 {
                    elements.len()
                } else {
                    (start + cnt as usize).min(elements.len())
                };
                elements = elements[start..end].to_vec();
            }
        }

        if let Some(dest) = store_dest {
            // Overwrite destination as a list (any prior type).
            let _ = self.cache.delete(&dest);
            let len = elements.len() as i64;
            if elements.is_empty() {
                return Ok(RespValue::Integer(0));
            }
            let est: usize = elements.iter().map(|e| e.len() + 16).sum();
            if let Err(e) = self.cache.ensure_non_string_capacity(est) {
                return Ok(RespValue::error(e.to_resp_string()));
            }
            let list = match self.cache.get_or_create_list(&dest) {
                Ok(l) => l,
                Err(e) => return Ok(RespValue::error(e.to_resp_string())),
            };
            let mut l = list.write();
            let before = crate::memory::estimate_keyed_object(dest.len(), l.memory_size());
            let _ = l.rpush(elements);
            let after = crate::memory::estimate_keyed_object(dest.len(), l.memory_size());
            drop(l);
            self.cache.account_list_delta(before, after);
            self.cache.list_blockers.notify_key(&dest);
            return Ok(RespValue::Integer(len));
        }

        Ok(RespValue::Array(
            elements
                .into_iter()
                .map(|v| RespValue::BulkString(Some(v)))
                .collect(),
        ))
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

/// LMPOP / BLMPOP reply: `[key, [elem, …]]`.
fn lmpop_reply(key: Bytes, elems: Vec<Bytes>) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(Some(key)),
        RespValue::Array(
            elems
                .into_iter()
                .map(|e| RespValue::BulkString(Some(e)))
                .collect(),
        ),
    ])
}

/// Parse `numkeys key [key ...] LEFT|RIGHT [COUNT count]` for LMPOP/BLMPOP.
fn parse_lmpop_args(
    handler: &CommandHandler,
    args: &[RespValue],
    name: &str,
) -> std::result::Result<(Vec<Bytes>, bool, usize), String> {
    if args.len() < 3 {
        return Err(format!(
            "ERR wrong number of arguments for '{}' command",
            name
        ));
    }
    let numkeys = match handler.parse_integer(&args[0]) {
        Ok(n) if n > 0 => n as usize,
        _ => {
            return Err(format!(
                "ERR numkeys should be greater than 0"
            ))
        }
    };
    if args.len() < 1 + numkeys + 1 {
        return Err(format!(
            "ERR wrong number of arguments for '{}' command",
            name
        ));
    }

    let mut keys = Vec::with_capacity(numkeys);
    for a in &args[1..1 + numkeys] {
        match a.as_bulk_string() {
            Some(k) => keys.push(k.clone()),
            None => return Err("ERR invalid key".into()),
        }
    }

    let from_left = parse_list_side(&args[1 + numkeys])?;
    let mut count = 1usize;
    let mut i = 2 + numkeys;
    while i < args.len() {
        let opt = match args[i].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_ascii_uppercase(),
            None => return Err("ERR syntax error".into()),
        };
        match opt.as_str() {
            "COUNT" => {
                if i + 1 >= args.len() {
                    return Err("ERR syntax error".into());
                }
                count = match handler.parse_integer(&args[i + 1]) {
                    Ok(n) if n > 0 => n as usize,
                    Ok(_) => {
                        return Err("ERR COUNT must be a positive integer".into());
                    }
                    Err(_) => {
                        return Err("ERR value is not an integer or out of range".into());
                    }
                };
                i += 2;
            }
            _ => return Err("ERR syntax error".into()),
        }
    }
    Ok((keys, from_left, count))
}
