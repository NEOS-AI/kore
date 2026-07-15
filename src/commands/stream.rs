//! Redis Streams commands: XADD, XRANGE, XREVRANGE, XLEN, XDEL, XTRIM,
//! XREAD, XGROUP, XREADGROUP, XACK, XPENDING.

use crate::cache::KeyType;
use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::stream_type::{StreamEntry, StreamId};
use bytes::Bytes;
use std::time::{Duration, Instant};
use super::CommandHandler;

impl CommandHandler {
    fn stream_entry_to_resp(entry: &StreamEntry) -> RespValue {
        let mut fields = Vec::with_capacity(entry.fields.len() * 2);
        for (k, v) in &entry.fields {
            fields.push(RespValue::BulkString(Some(k.clone())));
            fields.push(RespValue::BulkString(Some(v.clone())));
        }
        RespValue::Array(vec![
            RespValue::BulkString(Some(entry.id.to_bytes())),
            RespValue::Array(fields),
        ])
    }

    fn parse_stream_id_bound(s: &str) -> std::result::Result<StreamId, String> {
        if s == "-" {
            return Ok(StreamId::MIN);
        }
        if s == "+" {
            return Ok(StreamId::MAX);
        }
        StreamId::parse(s)
            .or_else(|| StreamId::parse_explicit(s))
            .ok_or_else(|| "ERR Invalid stream ID specified as stream command argument".into())
    }

    fn pair_fields(args: &[RespValue]) -> std::result::Result<Vec<(Bytes, Bytes)>, String> {
        if args.is_empty() || args.len() % 2 != 0 {
            return Err("ERR wrong number of arguments for 'xadd' command".into());
        }
        let mut out = Vec::with_capacity(args.len() / 2);
        let mut i = 0;
        while i < args.len() {
            let k = args[i]
                .as_bulk_string()
                .ok_or_else(|| "ERR invalid field".to_string())?
                .clone();
            let v = args[i + 1]
                .as_bulk_string()
                .ok_or_else(|| "ERR invalid value".to_string())?
                .clone();
            out.push((k, v));
            i += 2;
        }
        Ok(out)
    }

    /// XADD key [MAXLEN [~|=] count] [* | ID] field value [field value ...]
    pub(super) fn handle_xadd(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'xadd' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let mut i = 1;
        let mut maxlen: Option<usize> = None;

        // Optional MAXLEN
        if let Some(s) = args.get(i).and_then(|a| a.as_bulk_string()) {
            if s.eq_ignore_ascii_case(b"MAXLEN") {
                i += 1;
                // optional ~ or =
                if let Some(tok) = args.get(i).and_then(|a| a.as_bulk_string()) {
                    if tok.as_ref() == b"~" || tok.as_ref() == b"=" {
                        i += 1;
                    }
                }
                let count = match args.get(i).and_then(|a| a.as_bulk_string()) {
                    Some(b) => match std::str::from_utf8(b).ok().and_then(|s| s.parse::<i64>().ok()) {
                        Some(n) if n >= 0 => n as usize,
                        _ => {
                            return Ok(RespValue::error(
                                "ERR value is not an integer or out of range",
                            ));
                        }
                    },
                    None => {
                        return Ok(RespValue::error(
                            "ERR wrong number of arguments for 'xadd' command",
                        ));
                    }
                };
                maxlen = Some(count);
                i += 1;
            }
        }

        let id_spec = match args.get(i).and_then(|a| a.as_bulk_string()) {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => {
                return Ok(RespValue::error(
                    "ERR wrong number of arguments for 'xadd' command",
                ));
            }
        };
        i += 1;

        let fields = match Self::pair_fields(&args[i..]) {
            Ok(f) => f,
            Err(e) => return Ok(RespValue::error(e)),
        };

        let est: usize = fields.iter().map(|(k, v)| k.len() + v.len() + 64).sum();
        if let Err(e) = self.cache.ensure_non_string_capacity(est) {
            return Ok(RespValue::error(e.to_resp_string()));
        }

        let stream = match self.cache.get_or_create_stream(&key) {
            Ok(s) => s,
            Err(Error::WrongType) => {
                return Ok(RespValue::error(Error::WrongType.to_resp_string()));
            }
            Err(e) => return Ok(RespValue::error(e.to_resp_string())),
        };

        let mut s = stream.write().unwrap();
        let before = key.len() + s.memory_size();
        let id = match s.xadd_maxlen(&id_spec, fields, maxlen) {
            Ok(id) => id,
            Err(e) => return Ok(RespValue::error(e)),
        };
        let after = key.len() + s.memory_size();
        drop(s);
        self.cache.account_stream_delta(before, after);
        self.cache.touch_watch_key(&key);
        // Wake XREAD / XREADGROUP BLOCK waiters on this key
        self.cache.stream_blockers.notify_key(&key);
        Ok(RespValue::BulkString(Some(id.to_bytes())))
    }

    /// XLEN key
    pub(super) fn handle_xlen(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'xlen' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        match self.cache.key_type(&key) {
            KeyType::None => Ok(RespValue::Integer(0)),
            KeyType::Stream => {
                let len = self
                    .cache
                    .get_stream(&key)
                    .and_then(|s| s.read().ok().map(|g| g.len() as i64))
                    .unwrap_or(0);
                Ok(RespValue::Integer(len))
            }
            _ => Ok(RespValue::error(Error::WrongType.to_resp_string())),
        }
    }

    /// XRANGE key start end [COUNT count]
    pub(super) fn handle_xrange(&self, args: &[RespValue]) -> Result<RespValue> {
        self.xrange_common(args, false)
    }

    /// XREVRANGE key end start [COUNT count]
    pub(super) fn handle_xrevrange(&self, args: &[RespValue]) -> Result<RespValue> {
        self.xrange_common(args, true)
    }

    fn xrange_common(&self, args: &[RespValue], rev: bool) -> Result<RespValue> {
        let cmd = if rev { "xrevrange" } else { "xrange" };
        if args.len() < 3 {
            return Ok(RespValue::error(format!(
                "ERR wrong number of arguments for '{}' command",
                cmd
            )));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let a = match args[1].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).into_owned(),
            None => return Ok(RespValue::error("ERR invalid stream ID")),
        };
        let b = match args[2].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).into_owned(),
            None => return Ok(RespValue::error("ERR invalid stream ID")),
        };

        let mut count: Option<usize> = None;
        let mut i = 3;
        while i < args.len() {
            let opt = match args[i].as_bulk_string() {
                Some(s) => String::from_utf8_lossy(s).to_uppercase(),
                None => return Ok(RespValue::error("ERR syntax error")),
            };
            if opt == "COUNT" {
                i += 1;
                let c = match args.get(i).and_then(|v| v.as_bulk_string()) {
                    Some(b) => match std::str::from_utf8(b).ok().and_then(|s| s.parse::<i64>().ok())
                    {
                        Some(n) if n >= 0 => n as usize,
                        _ => {
                            return Ok(RespValue::error(
                                "ERR value is not an integer or out of range",
                            ));
                        }
                    },
                    None => return Ok(RespValue::error("ERR syntax error")),
                };
                count = Some(c);
                i += 1;
            } else {
                return Ok(RespValue::error("ERR syntax error"));
            }
        }

        match self.cache.key_type(&key) {
            KeyType::None => Ok(RespValue::Array(vec![])),
            KeyType::Stream => {
                let stream = match self.cache.get_stream(&key) {
                    Some(s) => s,
                    None => return Ok(RespValue::Array(vec![])),
                };
                let s = stream.read().unwrap();
                let entries = if rev {
                    // XREVRANGE key end start
                    let end = match Self::parse_stream_id_bound(&a) {
                        Ok(id) => id,
                        Err(e) => return Ok(RespValue::error(e)),
                    };
                    let start = match Self::parse_stream_id_bound(&b) {
                        Ok(id) => id,
                        Err(e) => return Ok(RespValue::error(e)),
                    };
                    s.xrevrange(end, start, count)
                } else {
                    let start = match Self::parse_stream_id_bound(&a) {
                        Ok(id) => id,
                        Err(e) => return Ok(RespValue::error(e)),
                    };
                    let end = match Self::parse_stream_id_bound(&b) {
                        Ok(id) => id,
                        Err(e) => return Ok(RespValue::error(e)),
                    };
                    s.xrange(start, end, count)
                };
                let resp: Vec<RespValue> = entries.iter().map(|e| Self::stream_entry_to_resp(e)).collect();
                Ok(RespValue::Array(resp))
            }
            _ => Ok(RespValue::error(Error::WrongType.to_resp_string())),
        }
    }

    /// XDEL key ID [ID ...]
    pub(super) fn handle_xdel(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'xdel' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let mut ids = Vec::new();
        for a in &args[1..] {
            let s = match a.as_bulk_string() {
                Some(b) => String::from_utf8_lossy(b).into_owned(),
                None => return Ok(RespValue::error("ERR invalid stream ID")),
            };
            match StreamId::parse_explicit(&s).or_else(|| StreamId::parse(&s)) {
                Some(id) => ids.push(id),
                None => {
                    return Ok(RespValue::error(
                        "ERR Invalid stream ID specified as stream command argument",
                    ));
                }
            }
        }
        match self.cache.key_type(&key) {
            KeyType::None => Ok(RespValue::Integer(0)),
            KeyType::Stream => {
                let stream = match self.cache.get_stream(&key) {
                    Some(s) => s,
                    None => return Ok(RespValue::Integer(0)),
                };
                let mut s = stream.write().unwrap();
                let before = key.len() + s.memory_size();
                let n = s.xdel(&ids) as i64;
                let after = key.len() + s.memory_size();
                drop(s);
                self.cache.account_stream_delta(before, after);
                if n > 0 {
                    self.cache.touch_watch_key(&key);
                }
                Ok(RespValue::Integer(n))
            }
            _ => Ok(RespValue::error(Error::WrongType.to_resp_string())),
        }
    }

    /// XTRIM key MAXLEN [~|=] count
    pub(super) fn handle_xtrim(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'xtrim' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let mut i = 1;
        let strategy = match args.get(i).and_then(|a| a.as_bulk_string()) {
            Some(s) => String::from_utf8_lossy(s).to_uppercase(),
            None => return Ok(RespValue::error("ERR syntax error")),
        };
        if strategy != "MAXLEN" {
            return Ok(RespValue::error("ERR syntax error"));
        }
        i += 1;
        if let Some(tok) = args.get(i).and_then(|a| a.as_bulk_string()) {
            if tok.as_ref() == b"~" || tok.as_ref() == b"=" {
                i += 1;
            }
        }
        let count = match args.get(i).and_then(|a| a.as_bulk_string()) {
            Some(b) => match std::str::from_utf8(b).ok().and_then(|s| s.parse::<i64>().ok()) {
                Some(n) if n >= 0 => n as usize,
                _ => {
                    return Ok(RespValue::error(
                        "ERR value is not an integer or out of range",
                    ));
                }
            },
            None => return Ok(RespValue::error("ERR syntax error")),
        };

        match self.cache.key_type(&key) {
            KeyType::None => Ok(RespValue::Integer(0)),
            KeyType::Stream => {
                let stream = match self.cache.get_stream(&key) {
                    Some(s) => s,
                    None => return Ok(RespValue::Integer(0)),
                };
                let mut s = stream.write().unwrap();
                let before = key.len() + s.memory_size();
                let n = s.trim_maxlen(count) as i64;
                let after = key.len() + s.memory_size();
                drop(s);
                self.cache.account_stream_delta(before, after);
                if n > 0 {
                    self.cache.touch_watch_key(&key);
                }
                Ok(RespValue::Integer(n))
            }
            _ => Ok(RespValue::error(Error::WrongType.to_resp_string())),
        }
    }

    /// XREAD [COUNT count] [BLOCK ms] STREAMS key [key ...] id [id ...]
    ///
    /// Without BLOCK: return immediately (null if no data).
    /// With BLOCK: wait up to `ms` milliseconds (0 = forever) for new entries.
    /// Stream IDs (especially `$`) are resolved once before waiting.
    pub(super) async fn handle_xread(&self, args: &[RespValue]) -> Result<RespValue> {
        let mut i = 0;
        let mut count: Option<usize> = None;
        let mut block_ms: Option<u64> = None;
        while i < args.len() {
            let opt = match args[i].as_bulk_string() {
                Some(s) => String::from_utf8_lossy(s).to_uppercase(),
                None => break,
            };
            match opt.as_str() {
                "COUNT" => {
                    i += 1;
                    let c = match args.get(i).and_then(|v| v.as_bulk_string()) {
                        Some(b) => {
                            match std::str::from_utf8(b).ok().and_then(|s| s.parse::<i64>().ok()) {
                                Some(n) if n >= 0 => n as usize,
                                _ => {
                                    return Ok(RespValue::error(
                                        "ERR value is not an integer or out of range",
                                    ));
                                }
                            }
                        }
                        None => return Ok(RespValue::error("ERR syntax error")),
                    };
                    count = Some(c);
                    i += 1;
                }
                "BLOCK" => {
                    i += 1;
                    let ms = match args.get(i).and_then(|v| v.as_bulk_string()) {
                        Some(b) => {
                            match std::str::from_utf8(b).ok().and_then(|s| s.parse::<i64>().ok()) {
                                Some(n) if n >= 0 => n as u64,
                                Some(_) => {
                                    return Ok(RespValue::error("ERR timeout is negative"));
                                }
                                None => {
                                    return Ok(RespValue::error(
                                        "ERR timeout is not an integer or out of range",
                                    ));
                                }
                            }
                        }
                        None => return Ok(RespValue::error("ERR syntax error")),
                    };
                    block_ms = Some(ms);
                    i += 1;
                }
                "STREAMS" => {
                    i += 1;
                    break;
                }
                _ => {
                    return Ok(RespValue::error("ERR syntax error"));
                }
            }
        }

        // Remaining: key1 key2 ... id1 id2 ...
        let rest = &args[i..];
        if rest.len() < 2 || rest.len() % 2 != 0 {
            return Ok(RespValue::error(
                "ERR Unbalanced XREAD list of streams: for each stream key one ID must be specified.",
            ));
        }
        let half = rest.len() / 2;
        let key_args = &rest[..half];
        let id_args = &rest[half..];

        // Resolve keys + IDs once (Redis: `$` is fixed at command start).
        let mut keys: Vec<Bytes> = Vec::with_capacity(half);
        let mut after_ids: Vec<StreamId> = Vec::with_capacity(half);
        for (k, idv) in key_args.iter().zip(id_args.iter()) {
            let key = match k.as_bulk_string() {
                Some(b) => b.clone(),
                None => return Ok(RespValue::error("ERR invalid key")),
            };
            let id_s = match idv.as_bulk_string() {
                Some(b) => String::from_utf8_lossy(b).into_owned(),
                None => return Ok(RespValue::error("ERR invalid stream ID")),
            };

            match self.cache.key_type(&key) {
                KeyType::None | KeyType::Stream => {}
                _ => return Ok(RespValue::error(Error::WrongType.to_resp_string())),
            }

            let after = if id_s == "$" {
                self.cache
                    .get_stream(&key)
                    .and_then(|s| s.read().ok().map(|g| g.last_id()))
                    .unwrap_or(StreamId::ZERO)
            } else if id_s == "0" || id_s == "0-0" {
                StreamId::ZERO
            } else {
                match StreamId::parse_explicit(&id_s).or_else(|| StreamId::parse(&id_s)) {
                    Some(id) => id,
                    None => {
                        return Ok(RespValue::error(
                            "ERR Invalid stream ID specified as stream command argument",
                        ));
                    }
                }
            };
            keys.push(key);
            after_ids.push(after);
        }

        // Immediate try with resolved IDs
        if let Some(resp) = self.xread_once(&keys, &after_ids, count)? {
            return Ok(resp);
        }

        // No BLOCK / inside MULTI: never sleep
        let Some(timeout_ms) = block_ms else {
            return Ok(RespValue::null());
        };
        if self.executing_multi {
            return Ok(RespValue::null());
        }

        let block_forever = timeout_ms == 0;
        let deadline = if block_forever {
            None
        } else {
            Some(Instant::now() + Duration::from_millis(timeout_ms))
        };

        let (waiter_id, notify) = self.cache.stream_blockers.register(&keys);

        let result = loop {
            if let Some(resp) = self.xread_once(&keys, &after_ids, count)? {
                break Ok(resp);
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

        self.cache.stream_blockers.unregister(waiter_id, &keys);
        result
    }

    /// Single non-blocking XREAD pass using pre-resolved IDs.
    /// Returns `Ok(None)` when there is no data (caller may block).
    fn xread_once(
        &self,
        keys: &[Bytes],
        after_ids: &[StreamId],
        count: Option<usize>,
    ) -> Result<Option<RespValue>> {
        let mut result: Vec<RespValue> = Vec::new();
        for (key, after) in keys.iter().zip(after_ids.iter()) {
            match self.cache.key_type(key) {
                KeyType::None => continue,
                KeyType::Stream => {}
                _ => return Ok(Some(RespValue::error(Error::WrongType.to_resp_string()))),
            }
            let stream = match self.cache.get_stream(key) {
                Some(s) => s,
                None => continue,
            };
            let s = stream.read().unwrap();
            let entries = s.xread_after(*after, count);
            if entries.is_empty() {
                continue;
            }
            let msgs: Vec<RespValue> = entries
                .iter()
                .map(|e| Self::stream_entry_to_resp(e))
                .collect();
            result.push(RespValue::Array(vec![
                RespValue::BulkString(Some(key.clone())),
                RespValue::Array(msgs),
            ]));
        }
        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RespValue::Array(result)))
        }
    }

    /// XGROUP CREATE key groupname id|$ [MKSTREAM]
    /// XGROUP DESTROY key groupname
    pub(super) fn handle_xgroup(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'xgroup' command",
            ));
        }
        let sub = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_uppercase(),
            None => return Ok(RespValue::error("ERR unknown subcommand")),
        };

        match sub.as_str() {
            "CREATE" => {
                if args.len() < 4 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'xgroup|create' command",
                    ));
                }
                let key = match args[1].as_bulk_string() {
                    Some(k) => k.clone(),
                    None => return Ok(RespValue::error("ERR invalid key")),
                };
                let group = match args[2].as_bulk_string() {
                    Some(g) => g.clone(),
                    None => return Ok(RespValue::error("ERR invalid group name")),
                };
                let id_s = match args[3].as_bulk_string() {
                    Some(b) => String::from_utf8_lossy(b).into_owned(),
                    None => return Ok(RespValue::error("ERR invalid stream ID")),
                };
                let mkstream = args.iter().skip(4).any(|a| {
                    a.as_bulk_string()
                        .map(|b| b.eq_ignore_ascii_case(b"MKSTREAM"))
                        .unwrap_or(false)
                });

                let exists = self.cache.stream_exists(&key);
                if !exists {
                    if mkstream {
                        if let Err(e) = self.cache.get_or_create_stream(&key) {
                            return Ok(RespValue::error(e.to_resp_string()));
                        }
                    } else {
                        return Ok(RespValue::error(
                            "ERR The XGROUP subcommand requires the key to exist. \
                             Note that for CREATE you may want to use the MKSTREAM option \
                             to create an empty stream automatically.",
                        ));
                    }
                } else if self.cache.key_type(&key) != KeyType::Stream {
                    return Ok(RespValue::error(Error::WrongType.to_resp_string()));
                }

                let id = if id_s == "$" {
                    self.cache
                        .get_stream(&key)
                        .and_then(|s| s.read().ok().map(|g| g.last_id()))
                        .unwrap_or(StreamId::ZERO)
                } else if id_s == "0" || id_s == "0-0" {
                    StreamId::ZERO
                } else {
                    match StreamId::parse_explicit(&id_s).or_else(|| StreamId::parse(&id_s)) {
                        Some(id) => id,
                        None => {
                            return Ok(RespValue::error(
                                "ERR Invalid stream ID specified as stream command argument",
                            ));
                        }
                    }
                };

                let stream = match self.cache.get_stream(&key) {
                    Some(s) => s,
                    None => return Ok(RespValue::error("ERR no such key")),
                };
                let mut s = stream.write().unwrap();
                match s.group_create(group, id, mkstream) {
                    Ok(()) => Ok(RespValue::ok()),
                    Err(e) => Ok(RespValue::error(e)),
                }
            }
            "DESTROY" => {
                if args.len() != 3 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'xgroup|destroy' command",
                    ));
                }
                let key = match args[1].as_bulk_string() {
                    Some(k) => k.clone(),
                    None => return Ok(RespValue::error("ERR invalid key")),
                };
                let group = match args[2].as_bulk_string() {
                    Some(g) => g.clone(),
                    None => return Ok(RespValue::error("ERR invalid group name")),
                };
                match self.cache.key_type(&key) {
                    KeyType::None => Ok(RespValue::Integer(0)),
                    KeyType::Stream => {
                        let stream = match self.cache.get_stream(&key) {
                            Some(s) => s,
                            None => return Ok(RespValue::Integer(0)),
                        };
                        let mut s = stream.write().unwrap();
                        Ok(RespValue::Integer(if s.group_destroy(&group) { 1 } else { 0 }))
                    }
                    _ => Ok(RespValue::error(Error::WrongType.to_resp_string())),
                }
            }
            _ => Ok(RespValue::error(format!(
                "ERR unknown subcommand '{}'",
                sub
            ))),
        }
    }

    /// XREADGROUP GROUP group consumer [COUNT count] [BLOCK ms] STREAMS key [key ...] id [id ...]
    ///
    /// BLOCK waits for new messages when reading with `>` and nothing is available.
    pub(super) async fn handle_xreadgroup(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 6 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'xreadgroup' command",
            ));
        }
        let mut i = 0;
        let gword = match args[i].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_uppercase(),
            None => return Ok(RespValue::error("ERR syntax error")),
        };
        if gword != "GROUP" {
            return Ok(RespValue::error("ERR syntax error"));
        }
        i += 1;
        let group = match args.get(i).and_then(|a| a.as_bulk_string()) {
            Some(g) => g.clone(),
            None => return Ok(RespValue::error("ERR syntax error")),
        };
        i += 1;
        let consumer = match args.get(i).and_then(|a| a.as_bulk_string()) {
            Some(c) => c.clone(),
            None => return Ok(RespValue::error("ERR syntax error")),
        };
        i += 1;

        let mut count: Option<usize> = None;
        let mut block_ms: Option<u64> = None;
        while i < args.len() {
            let opt = match args[i].as_bulk_string() {
                Some(s) => String::from_utf8_lossy(s).to_uppercase(),
                None => break,
            };
            match opt.as_str() {
                "COUNT" => {
                    i += 1;
                    let c = match args.get(i).and_then(|v| v.as_bulk_string()) {
                        Some(b) => {
                            match std::str::from_utf8(b).ok().and_then(|s| s.parse::<i64>().ok()) {
                                Some(n) if n >= 0 => n as usize,
                                _ => {
                                    return Ok(RespValue::error(
                                        "ERR value is not an integer or out of range",
                                    ));
                                }
                            }
                        }
                        None => return Ok(RespValue::error("ERR syntax error")),
                    };
                    count = Some(c);
                    i += 1;
                }
                "BLOCK" => {
                    i += 1;
                    let ms = match args.get(i).and_then(|v| v.as_bulk_string()) {
                        Some(b) => {
                            match std::str::from_utf8(b).ok().and_then(|s| s.parse::<i64>().ok()) {
                                Some(n) if n >= 0 => n as u64,
                                Some(_) => {
                                    return Ok(RespValue::error("ERR timeout is negative"));
                                }
                                None => {
                                    return Ok(RespValue::error(
                                        "ERR timeout is not an integer or out of range",
                                    ));
                                }
                            }
                        }
                        None => return Ok(RespValue::error("ERR syntax error")),
                    };
                    block_ms = Some(ms);
                    i += 1;
                }
                "STREAMS" => {
                    i += 1;
                    break;
                }
                _ => return Ok(RespValue::error("ERR syntax error")),
            }
        }

        let rest = &args[i..];
        if rest.len() < 2 || rest.len() % 2 != 0 {
            return Ok(RespValue::error(
                "ERR Unbalanced XREADGROUP list of streams: for each stream key one ID must be specified.",
            ));
        }
        let half = rest.len() / 2;
        let key_args = &rest[..half];
        let id_args = &rest[half..];

        let mut keys: Vec<Bytes> = Vec::with_capacity(half);
        let mut id_specs: Vec<String> = Vec::with_capacity(half);
        let mut wants_new = false;
        for (k, idv) in key_args.iter().zip(id_args.iter()) {
            let key = match k.as_bulk_string() {
                Some(b) => b.clone(),
                None => return Ok(RespValue::error("ERR invalid key")),
            };
            let id_s = match idv.as_bulk_string() {
                Some(b) => String::from_utf8_lossy(b).into_owned(),
                None => return Ok(RespValue::error("ERR invalid stream ID")),
            };
            if id_s == ">" {
                wants_new = true;
            }
            keys.push(key);
            id_specs.push(id_s);
        }

        // Immediate try
        match self.xreadgroup_once(&group, &consumer, &keys, &id_specs, count)? {
            XReadGroupOutcome::Data(resp) | XReadGroupOutcome::Error(resp) => return Ok(resp),
            XReadGroupOutcome::Empty => {}
        }

        // Only block for new-message reads (`>`). History ID empty → immediate null.
        let Some(timeout_ms) = block_ms.filter(|_| wants_new) else {
            return Ok(RespValue::null());
        };
        if self.executing_multi {
            return Ok(RespValue::null());
        }

        let block_forever = timeout_ms == 0;
        let deadline = if block_forever {
            None
        } else {
            Some(Instant::now() + Duration::from_millis(timeout_ms))
        };

        let (waiter_id, notify) = self.cache.stream_blockers.register(&keys);

        let result = loop {
            match self.xreadgroup_once(&group, &consumer, &keys, &id_specs, count)? {
                XReadGroupOutcome::Data(resp) | XReadGroupOutcome::Error(resp) => break Ok(resp),
                XReadGroupOutcome::Empty => {}
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

        self.cache.stream_blockers.unregister(waiter_id, &keys);
        result
    }

    /// Single non-blocking XREADGROUP pass.
    fn xreadgroup_once(
        &self,
        group: &Bytes,
        consumer: &Bytes,
        keys: &[Bytes],
        id_specs: &[String],
        count: Option<usize>,
    ) -> Result<XReadGroupOutcome> {
        let mut result: Vec<RespValue> = Vec::new();
        for (key, id_s) in keys.iter().zip(id_specs.iter()) {
            if self.cache.key_type(key) == KeyType::None {
                return Ok(XReadGroupOutcome::Error(RespValue::error(format!(
                    "NOGROUP No such key '{}' or consumer group",
                    String::from_utf8_lossy(key)
                ))));
            }
            if self.cache.key_type(key) != KeyType::Stream {
                return Ok(XReadGroupOutcome::Error(RespValue::error(
                    Error::WrongType.to_resp_string(),
                )));
            }

            let stream = match self.cache.get_stream(key) {
                Some(s) => s,
                None => {
                    return Ok(XReadGroupOutcome::Error(RespValue::error(format!(
                        "NOGROUP No such key '{}' or consumer group",
                        String::from_utf8_lossy(key)
                    ))));
                }
            };
            let mut s = stream.write().unwrap();
            let entries = match s.xreadgroup(group, consumer, id_s, count) {
                Ok(e) => e,
                Err(e) => {
                    if e.starts_with("NOGROUP") {
                        return Ok(XReadGroupOutcome::Error(RespValue::error(format!(
                            "NOGROUP No such key '{}' or consumer group",
                            String::from_utf8_lossy(key)
                        ))));
                    }
                    return Ok(XReadGroupOutcome::Error(RespValue::error(e)));
                }
            };
            if entries.is_empty() {
                continue;
            }
            let msgs: Vec<RespValue> = entries
                .iter()
                .map(|e| Self::stream_entry_to_resp(e))
                .collect();
            result.push(RespValue::Array(vec![
                RespValue::BulkString(Some(key.clone())),
                RespValue::Array(msgs),
            ]));
        }

        if result.is_empty() {
            Ok(XReadGroupOutcome::Empty)
        } else {
            Ok(XReadGroupOutcome::Data(RespValue::Array(result)))
        }
    }

    /// XACK key group ID [ID ...]
    pub(super) fn handle_xack(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'xack' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let group = match args[1].as_bulk_string() {
            Some(g) => g.clone(),
            None => return Ok(RespValue::error("ERR invalid group name")),
        };
        let mut ids = Vec::new();
        for a in &args[2..] {
            let s = match a.as_bulk_string() {
                Some(b) => String::from_utf8_lossy(b).into_owned(),
                None => return Ok(RespValue::error("ERR invalid stream ID")),
            };
            match StreamId::parse_explicit(&s).or_else(|| StreamId::parse(&s)) {
                Some(id) => ids.push(id),
                None => {
                    return Ok(RespValue::error(
                        "ERR Invalid stream ID specified as stream command argument",
                    ));
                }
            }
        }
        match self.cache.key_type(&key) {
            KeyType::None => Ok(RespValue::Integer(0)),
            KeyType::Stream => {
                let stream = match self.cache.get_stream(&key) {
                    Some(s) => s,
                    None => return Ok(RespValue::Integer(0)),
                };
                let mut s = stream.write().unwrap();
                match s.xack(&group, &ids) {
                    Ok(n) => Ok(RespValue::Integer(n as i64)),
                    Err(e) => Ok(RespValue::error(e)),
                }
            }
            _ => Ok(RespValue::error(Error::WrongType.to_resp_string())),
        }
    }

    /// XPENDING key group  — summary form only
    pub(super) fn handle_xpending(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'xpending' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let group = match args[1].as_bulk_string() {
            Some(g) => g.clone(),
            None => return Ok(RespValue::error("ERR invalid group name")),
        };
        // Extended form not implemented yet
        if args.len() > 2 {
            return Ok(RespValue::error(
                "ERR XPENDING extended form not implemented",
            ));
        }
        match self.cache.key_type(&key) {
            KeyType::None => Ok(RespValue::error(format!(
                "NOGROUP No such key '{}' or consumer group",
                String::from_utf8_lossy(&key)
            ))),
            KeyType::Stream => {
                let stream = match self.cache.get_stream(&key) {
                    Some(s) => s,
                    None => {
                        return Ok(RespValue::error(format!(
                            "NOGROUP No such key '{}' or consumer group",
                            String::from_utf8_lossy(&key)
                        )));
                    }
                };
                let s = stream.read().unwrap();
                match s.xpending_summary(&group) {
                    Ok((total, min_id, max_id, consumers)) => {
                        let min = min_id
                            .map(|id| RespValue::BulkString(Some(id.to_bytes())))
                            .unwrap_or_else(RespValue::null);
                        let max = max_id
                            .map(|id| RespValue::BulkString(Some(id.to_bytes())))
                            .unwrap_or_else(RespValue::null);
                        let cons: Vec<RespValue> = consumers
                            .into_iter()
                            .map(|(name, cnt)| {
                                RespValue::Array(vec![
                                    RespValue::BulkString(Some(name)),
                                    RespValue::BulkString(Some(Bytes::from(cnt.to_string()))),
                                ])
                            })
                            .collect();
                        Ok(RespValue::Array(vec![
                            RespValue::Integer(total as i64),
                            min,
                            max,
                            if cons.is_empty() {
                                RespValue::null()
                            } else {
                                RespValue::Array(cons)
                            },
                        ]))
                    }
                    Err(e) => Ok(RespValue::error(e)),
                }
            }
            _ => Ok(RespValue::error(Error::WrongType.to_resp_string())),
        }
    }
}

/// Outcome of one non-blocking XREADGROUP attempt.
enum XReadGroupOutcome {
    Data(RespValue),
    Empty,
    Error(RespValue),
}
