//! Redis Streams commands: XADD, XRANGE, XREVRANGE, XLEN, XDEL, XTRIM,
//! XREAD, XGROUP, XREADGROUP, XACK, XPENDING, XCLAIM, XAUTOCLAIM, XSETID, XINFO.

use crate::cache::KeyType;
use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::stream_type::{
    PendingEntry, StreamEntry, StreamId, XClaimOpts, XInfoStreamFullGroup,
};
use bytes::Bytes;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
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

    fn pending_entry_to_resp(pe: &PendingEntry) -> RespValue {
        RespValue::Array(vec![
            RespValue::BulkString(Some(pe.id.to_bytes())),
            RespValue::BulkString(Some(pe.consumer.clone())),
            RespValue::Integer(pe.delivery_time_ms as i64),
            RespValue::Integer(pe.delivery_count as i64),
        ])
    }

    fn xinfo_full_group_to_resp(g: XInfoStreamFullGroup) -> RespValue {
        let pending: Vec<RespValue> = g
            .pending
            .iter()
            .map(|pe| Self::pending_entry_to_resp(pe))
            .collect();
        let consumers: Vec<RespValue> = g
            .consumers
            .into_iter()
            .map(|c| {
                let c_pending: Vec<RespValue> = c
                    .pending
                    .iter()
                    .map(|pe| Self::pending_entry_to_resp(pe))
                    .collect();
                RespValue::Array(vec![
                    bulk_static(b"name"),
                    RespValue::BulkString(Some(c.name)),
                    bulk_static(b"seen-time"),
                    RespValue::Integer(c.seen_time_ms as i64),
                    bulk_static(b"pel-count"),
                    RespValue::Integer(c.pel_count as i64),
                    bulk_static(b"pending"),
                    RespValue::Array(c_pending),
                ])
            })
            .collect();
        RespValue::Array(vec![
            bulk_static(b"name"),
            RespValue::BulkString(Some(g.name)),
            bulk_static(b"last-delivered-id"),
            RespValue::BulkString(Some(g.last_delivered_id.to_bytes())),
            bulk_static(b"entries-read"),
            match g.entries_read {
                Some(n) => RespValue::Integer(n as i64),
                None => RespValue::null(),
            },
            bulk_static(b"lag"),
            RespValue::Integer(g.lag as i64),
            bulk_static(b"pel-count"),
            RespValue::Integer(g.pel_count as i64),
            bulk_static(b"pending"),
            RespValue::Array(pending),
            bulk_static(b"consumers"),
            RespValue::Array(consumers),
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

    /// XADD key [NOMKSTREAM] [MAXLEN|MINID [~|=] threshold [LIMIT count]] [* | ID] field value ...
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
        let mut nomkstream = false;
        let mut maxlen: Option<usize> = None;
        let mut minid: Option<StreamId> = None;
        let mut limit: Option<usize> = None;

        // Optional NOMKSTREAM / MAXLEN / MINID / LIMIT (in any order before the ID).
        while i < args.len() {
            let tok = match args[i].as_bulk_string() {
                Some(s) => s,
                None => break,
            };
            if tok.eq_ignore_ascii_case(b"NOMKSTREAM") {
                nomkstream = true;
                i += 1;
                continue;
            }
            if tok.eq_ignore_ascii_case(b"MAXLEN") {
                if minid.is_some() {
                    return Ok(RespValue::error("ERR syntax error"));
                }
                i += 1;
                if let Some(approx) = args.get(i).and_then(|a| a.as_bulk_string()) {
                    if approx.as_ref() == b"~" || approx.as_ref() == b"=" {
                        i += 1;
                    }
                }
                let count = match args.get(i).and_then(|a| a.as_bulk_string()) {
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
                    None => {
                        return Ok(RespValue::error(
                            "ERR wrong number of arguments for 'xadd' command",
                        ));
                    }
                };
                maxlen = Some(count);
                i += 1;
                continue;
            }
            if tok.eq_ignore_ascii_case(b"MINID") {
                if maxlen.is_some() {
                    return Ok(RespValue::error("ERR syntax error"));
                }
                i += 1;
                if let Some(approx) = args.get(i).and_then(|a| a.as_bulk_string()) {
                    if approx.as_ref() == b"~" || approx.as_ref() == b"=" {
                        i += 1;
                    }
                }
                let id_s = match args.get(i).and_then(|a| a.as_bulk_string()) {
                    Some(b) => String::from_utf8_lossy(b).into_owned(),
                    None => {
                        return Ok(RespValue::error(
                            "ERR wrong number of arguments for 'xadd' command",
                        ));
                    }
                };
                let id = match StreamId::parse_explicit(&id_s).or_else(|| StreamId::parse(&id_s)) {
                    Some(id) => id,
                    None => {
                        return Ok(RespValue::error(
                            "ERR Invalid stream ID specified as stream command argument",
                        ));
                    }
                };
                minid = Some(id);
                i += 1;
                continue;
            }
            if tok.eq_ignore_ascii_case(b"LIMIT") {
                i += 1;
                let n = match args.get(i).and_then(|a| a.as_bulk_string()) {
                    Some(b) => {
                        match std::str::from_utf8(b).ok().and_then(|s| s.parse::<i64>().ok()) {
                            Some(v) if v >= 0 => v as usize,
                            _ => {
                                return Ok(RespValue::error(
                                    "ERR value is not an integer or out of range",
                                ));
                            }
                        }
                    }
                    None => {
                        return Ok(RespValue::error(
                            "ERR wrong number of arguments for 'xadd' command",
                        ));
                    }
                };
                limit = Some(n);
                i += 1;
                continue;
            }
            break;
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

        // NOMKSTREAM: do not create a missing key; return null bulk.
        if nomkstream {
            match self.cache.key_type(&key) {
                KeyType::None => return Ok(RespValue::null()),
                KeyType::Stream => {}
                _ => return Ok(RespValue::error(Error::WrongType.to_resp_string())),
            }
        }

        let est: usize = fields.iter().map(|(k, v)| k.len() + v.len() + 64).sum();
        if let Err(e) = self.cache.ensure_non_string_capacity(est) {
            return Ok(RespValue::error(e.to_resp_string()));
        }

        let stream = if nomkstream {
            match self.cache.get_stream(&key) {
                Some(s) => s,
                None => return Ok(RespValue::null()),
            }
        } else {
            match self.cache.get_or_create_stream(&key) {
                Ok(s) => s,
                Err(Error::WrongType) => {
                    return Ok(RespValue::error(Error::WrongType.to_resp_string()));
                }
                Err(e) => return Ok(RespValue::error(e.to_resp_string())),
            }
        };

        // LIMIT without MAXLEN/MINID is a syntax error.
        if limit.is_some() && maxlen.is_none() && minid.is_none() {
            return Ok(RespValue::error("ERR syntax error"));
        }

        let mut s = stream.write();
        let before = crate::memory::estimate_keyed_object(key.len(), s.memory_size());
        let id = match s.xadd_with_trim(&id_spec, fields, maxlen, minid, limit) {
            Ok(id) => id,
            Err(e) => return Ok(RespValue::error(e)),
        };
        let after = crate::memory::estimate_keyed_object(key.len(), s.memory_size());
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
                    .map(|s| s.read().len() as i64)
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
                let s = stream.read();
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
                let mut s = stream.write();
                let before = crate::memory::estimate_keyed_object(key.len(), s.memory_size());
                let n = s.xdel(&ids) as i64;
                let after = crate::memory::estimate_keyed_object(key.len(), s.memory_size());
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

    /// XTRIM key MAXLEN|MINID [~|=] threshold [LIMIT count]
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
        i += 1;
        if let Some(tok) = args.get(i).and_then(|a| a.as_bulk_string()) {
            if tok.as_ref() == b"~" || tok.as_ref() == b"=" {
                i += 1;
            }
        }

        enum TrimKind {
            Maxlen(usize),
            Minid(StreamId),
        }
        let trim = match strategy.as_str() {
            "MAXLEN" => {
                let count = match args.get(i).and_then(|a| a.as_bulk_string()) {
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
                i += 1;
                TrimKind::Maxlen(count)
            }
            "MINID" => {
                let id_s = match args.get(i).and_then(|a| a.as_bulk_string()) {
                    Some(b) => String::from_utf8_lossy(b).into_owned(),
                    None => return Ok(RespValue::error("ERR syntax error")),
                };
                let id = match StreamId::parse_explicit(&id_s).or_else(|| StreamId::parse(&id_s)) {
                    Some(id) => id,
                    None => {
                        return Ok(RespValue::error(
                            "ERR Invalid stream ID specified as stream command argument",
                        ));
                    }
                };
                i += 1;
                TrimKind::Minid(id)
            }
            _ => return Ok(RespValue::error("ERR syntax error")),
        };

        // Optional LIMIT count
        let mut limit: Option<usize> = None;
        if i < args.len() {
            let opt = match args[i].as_bulk_string() {
                Some(b) => String::from_utf8_lossy(b).to_ascii_uppercase(),
                None => return Ok(RespValue::error("ERR syntax error")),
            };
            if opt == "LIMIT" {
                i += 1;
                let n = match args.get(i).and_then(|a| a.as_bulk_string()) {
                    Some(b) => {
                        match std::str::from_utf8(b).ok().and_then(|s| s.parse::<i64>().ok()) {
                            Some(v) if v >= 0 => v as usize,
                            _ => {
                                return Ok(RespValue::error(
                                    "ERR value is not an integer or out of range",
                                ));
                            }
                        }
                    }
                    None => return Ok(RespValue::error("ERR syntax error")),
                };
                limit = Some(n);
                i += 1;
            }
        }
        if i != args.len() {
            return Ok(RespValue::error("ERR syntax error"));
        }

        match self.cache.key_type(&key) {
            KeyType::None => Ok(RespValue::Integer(0)),
            KeyType::Stream => {
                let stream = match self.cache.get_stream(&key) {
                    Some(s) => s,
                    None => return Ok(RespValue::Integer(0)),
                };
                let mut s = stream.write();
                let before = crate::memory::estimate_keyed_object(key.len(), s.memory_size());
                let n = match trim {
                    TrimKind::Maxlen(count) => s.trim_maxlen_limit(count, limit) as i64,
                    TrimKind::Minid(id) => s.trim_minid_limit(id, limit) as i64,
                };
                let after = crate::memory::estimate_keyed_object(key.len(), s.memory_size());
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
                    .map(|s| { let g = s.read(); g.last_id() })
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
            let s = stream.read();
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

    /// XGROUP CREATE key groupname id|$ [MKSTREAM] [ENTRIESREAD entries-read]
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
                // Optional trailing flags: MKSTREAM, ENTRIESREAD <n> (any order).
                let mut mkstream = false;
                let mut entries_read: Option<u64> = None;
                let mut i = 4;
                while i < args.len() {
                    let opt = match args[i].as_bulk_string() {
                        Some(b) => String::from_utf8_lossy(b).to_ascii_uppercase(),
                        None => {
                            return Ok(RespValue::error("ERR syntax error"));
                        }
                    };
                    match opt.as_str() {
                        "MKSTREAM" => {
                            mkstream = true;
                            i += 1;
                        }
                        "ENTRIESREAD" => {
                            if i + 1 >= args.len() {
                                return Ok(RespValue::error(
                                    "ERR syntax error",
                                ));
                            }
                            let n = match self.parse_integer(&args[i + 1]) {
                                Ok(v) if v >= 0 => v as u64,
                                _ => {
                                    return Ok(RespValue::error(
                                        "ERR value is not an integer or out of range",
                                    ));
                                }
                            };
                            entries_read = Some(n);
                            i += 2;
                        }
                        _ => {
                            return Ok(RespValue::error("ERR syntax error"));
                        }
                    }
                }

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
                        .map(|s| { let g = s.read(); g.last_id() })
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
                let mut s = stream.write();
                match s.group_create(group, id, mkstream, entries_read) {
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
                        let mut s = stream.write();
                        Ok(RespValue::Integer(if s.group_destroy(&group) { 1 } else { 0 }))
                    }
                    _ => Ok(RespValue::error(Error::WrongType.to_resp_string())),
                }
            }
            "SETID" => {
                // XGROUP SETID key groupname id|$ [ENTRIESREAD entries-read]
                if args.len() < 4 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'xgroup|setid' command",
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
                let mut entries_read: Option<u64> = None;
                let mut i = 4;
                while i < args.len() {
                    let opt = match args[i].as_bulk_string() {
                        Some(b) => String::from_utf8_lossy(b).to_ascii_uppercase(),
                        None => return Ok(RespValue::error("ERR syntax error")),
                    };
                    match opt.as_str() {
                        "ENTRIESREAD" => {
                            if i + 1 >= args.len() {
                                return Ok(RespValue::error("ERR syntax error"));
                            }
                            let n = match self.parse_integer(&args[i + 1]) {
                                Ok(v) if v >= 0 => v as u64,
                                _ => {
                                    return Ok(RespValue::error(
                                        "ERR value is not an integer or out of range",
                                    ));
                                }
                            };
                            entries_read = Some(n);
                            i += 2;
                        }
                        _ => return Ok(RespValue::error("ERR syntax error")),
                    }
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
                        let id = if id_s == "$" {
                            stream.read().last_id()
                        } else if id_s == "0" || id_s == "0-0" {
                            StreamId::ZERO
                        } else {
                            match StreamId::parse_explicit(&id_s).or_else(|| StreamId::parse(&id_s))
                            {
                                Some(id) => id,
                                None => {
                                    return Ok(RespValue::error(
                                        "ERR Invalid stream ID specified as stream command argument",
                                    ));
                                }
                            }
                        };
                        let mut s = stream.write();
                        match s.group_setid(&group, id, entries_read) {
                            Ok(()) => Ok(RespValue::ok()),
                            Err(e) => {
                                if e.starts_with("NOGROUP") {
                                    Ok(RespValue::error(format!(
                                        "NOGROUP No such key '{}' or consumer group",
                                        String::from_utf8_lossy(&key)
                                    )))
                                } else {
                                    Ok(RespValue::error(e))
                                }
                            }
                        }
                    }
                    _ => Ok(RespValue::error(Error::WrongType.to_resp_string())),
                }
            }
            "CREATECONSUMER" => {
                // XGROUP CREATECONSUMER key groupname consumername
                if args.len() != 4 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'xgroup|createconsumer' command",
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
                let consumer = match args[3].as_bulk_string() {
                    Some(c) => c.clone(),
                    None => return Ok(RespValue::error("ERR invalid consumer name")),
                };
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
                        let mut s = stream.write();
                        match s.group_create_consumer(&group, &consumer) {
                            Ok(created) => Ok(RespValue::Integer(if created { 1 } else { 0 })),
                            Err(e) => {
                                if e.starts_with("NOGROUP") {
                                    Ok(RespValue::error(format!(
                                        "NOGROUP No such key '{}' or consumer group",
                                        String::from_utf8_lossy(&key)
                                    )))
                                } else {
                                    Ok(RespValue::error(e))
                                }
                            }
                        }
                    }
                    _ => Ok(RespValue::error(Error::WrongType.to_resp_string())),
                }
            }
            "DELCONSUMER" => {
                // XGROUP DELCONSUMER key groupname consumername
                if args.len() != 4 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'xgroup|delconsumer' command",
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
                let consumer = match args[3].as_bulk_string() {
                    Some(c) => c.clone(),
                    None => return Ok(RespValue::error("ERR invalid consumer name")),
                };
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
                        let mut s = stream.write();
                        match s.group_del_consumer(&group, &consumer) {
                            Ok(n) => Ok(RespValue::Integer(n as i64)),
                            Err(e) => {
                                if e.starts_with("NOGROUP") {
                                    Ok(RespValue::error(format!(
                                        "NOGROUP No such key '{}' or consumer group",
                                        String::from_utf8_lossy(&key)
                                    )))
                                } else {
                                    Ok(RespValue::error(e))
                                }
                            }
                        }
                    }
                    _ => Ok(RespValue::error(Error::WrongType.to_resp_string())),
                }
            }
            "HELP" => Ok(RespValue::Array(vec![
                bulk_static(b"XGROUP <subcommand> [<arg> ...]. Subcommands are:"),
                bulk_static(b"CREATE <key> <groupname> <id|$> [MKSTREAM] [ENTRIESREAD <n>]"),
                bulk_static(b"DESTROY <key> <groupname>"),
                bulk_static(b"CREATECONSUMER <key> <groupname> <consumername>"),
                bulk_static(b"DELCONSUMER <key> <groupname> <consumername>"),
                bulk_static(b"SETID <key> <groupname> <id|$> [ENTRIESREAD <n>]"),
                bulk_static(b"HELP -- print this help"),
            ])),
            _ => Ok(RespValue::error(format!(
                "ERR unknown subcommand '{}'. Try XGROUP HELP.",
                sub
            ))),
        }
    }

    /// XREADGROUP GROUP group consumer [COUNT count] [BLOCK ms] [NOACK]
    /// STREAMS key [key ...] id [id ...]
    ///
    /// BLOCK waits for new messages when reading with `>` and nothing is available.
    /// NOACK skips PEL insertion for newly delivered (`>`) messages.
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
        let mut noack = false;
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
                "NOACK" => {
                    noack = true;
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
        match self.xreadgroup_once(&group, &consumer, &keys, &id_specs, count, noack)? {
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
            match self.xreadgroup_once(&group, &consumer, &keys, &id_specs, count, noack)? {
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
        noack: bool,
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
            let mut s = stream.write();
            let entries = match s.xreadgroup_opts(group, consumer, id_s, count, noack) {
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
                let mut s = stream.write();
                match s.xack(&group, &ids) {
                    Ok(n) => Ok(RespValue::Integer(n as i64)),
                    Err(e) => Ok(RespValue::error(e)),
                }
            }
            _ => Ok(RespValue::error(Error::WrongType.to_resp_string())),
        }
    }

    /// XPENDING key group
    /// XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
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
                let s = stream.read();

                // Summary form
                if args.len() == 2 {
                    return match s.xpending_summary(&group) {
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
                        Err(e) => {
                            if e.starts_with("NOGROUP") {
                                Ok(RespValue::error(format!(
                                    "NOGROUP No such key '{}' or consumer group",
                                    String::from_utf8_lossy(&key)
                                )))
                            } else {
                                Ok(RespValue::error(e))
                            }
                        }
                    };
                }

                // Extended: [IDLE min-idle] start end count [consumer]
                let mut i = 2;
                let mut min_idle_ms: u64 = 0;
                if let Some(tok) = args.get(i).and_then(|a| a.as_bulk_string()) {
                    if tok.eq_ignore_ascii_case(b"IDLE") {
                        i += 1;
                        min_idle_ms = match args.get(i).and_then(|a| a.as_bulk_string()) {
                            Some(b) => match std::str::from_utf8(b)
                                .ok()
                                .and_then(|s| s.parse::<i64>().ok())
                            {
                                Some(n) if n >= 0 => n as u64,
                                _ => {
                                    return Ok(RespValue::error(
                                        "ERR value is not an integer or out of range",
                                    ));
                                }
                            },
                            None => return Ok(RespValue::error("ERR syntax error")),
                        };
                        i += 1;
                    }
                }
                if args.len() < i + 3 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'xpending' command",
                    ));
                }
                let start_s = match args[i].as_bulk_string() {
                    Some(b) => String::from_utf8_lossy(b).into_owned(),
                    None => return Ok(RespValue::error("ERR invalid stream ID")),
                };
                let end_s = match args[i + 1].as_bulk_string() {
                    Some(b) => String::from_utf8_lossy(b).into_owned(),
                    None => return Ok(RespValue::error("ERR invalid stream ID")),
                };
                let count = match args[i + 2].as_bulk_string() {
                    Some(b) => match std::str::from_utf8(b)
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                    {
                        Some(n) if n >= 0 => n as usize,
                        _ => {
                            return Ok(RespValue::error(
                                "ERR value is not an integer or out of range",
                            ));
                        }
                    },
                    None => {
                        return Ok(RespValue::error(
                            "ERR value is not an integer or out of range",
                        ));
                    }
                };
                let consumer = if args.len() > i + 3 {
                    match args[i + 3].as_bulk_string() {
                        Some(c) => Some(c.clone()),
                        None => return Ok(RespValue::error("ERR invalid consumer name")),
                    }
                } else {
                    None
                };
                if args.len() > i + 4 {
                    return Ok(RespValue::error("ERR syntax error"));
                }

                let start = match Self::parse_stream_id_bound(&start_s) {
                    Ok(id) => id,
                    Err(e) => return Ok(RespValue::error(e)),
                };
                let end = match Self::parse_stream_id_bound(&end_s) {
                    Ok(id) => id,
                    Err(e) => return Ok(RespValue::error(e)),
                };

                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);

                match s.xpending_range(
                    &group,
                    start,
                    end,
                    count,
                    consumer.as_ref(),
                    min_idle_ms,
                ) {
                    Ok(entries) => {
                        let rows: Vec<RespValue> = entries
                            .into_iter()
                            .map(|pe| {
                                let idle = now.saturating_sub(pe.delivery_time_ms);
                                RespValue::Array(vec![
                                    RespValue::BulkString(Some(pe.id.to_bytes())),
                                    RespValue::BulkString(Some(pe.consumer)),
                                    RespValue::Integer(idle as i64),
                                    RespValue::Integer(pe.delivery_count as i64),
                                ])
                            })
                            .collect();
                        Ok(RespValue::Array(rows))
                    }
                    Err(e) => {
                        if e.starts_with("NOGROUP") {
                            Ok(RespValue::error(format!(
                                "NOGROUP No such key '{}' or consumer group",
                                String::from_utf8_lossy(&key)
                            )))
                        } else {
                            Ok(RespValue::error(e))
                        }
                    }
                }
            }
            _ => Ok(RespValue::error(Error::WrongType.to_resp_string())),
        }
    }

    /// XCLAIM key group consumer min-idle-time id [id ...]
    /// [IDLE ms] [TIME ms-unix-time] [RETRYCOUNT count] [FORCE] [JUSTID]
    pub(super) fn handle_xclaim(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 5 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'xclaim' command",
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
        let consumer = match args[2].as_bulk_string() {
            Some(c) => c.clone(),
            None => return Ok(RespValue::error("ERR invalid consumer name")),
        };
        let min_idle = match args[3].as_bulk_string() {
            Some(b) => match std::str::from_utf8(b).ok().and_then(|s| s.parse::<i64>().ok()) {
                Some(n) if n >= 0 => n as u64,
                _ => {
                    return Ok(RespValue::error(
                        "ERR value is not an integer or out of range",
                    ));
                }
            },
            None => {
                return Ok(RespValue::error(
                    "ERR value is not an integer or out of range",
                ));
            }
        };

        let mut force = false;
        let mut just_id = false;
        let mut time_ms: Option<u64> = None;
        let mut idle_ms: Option<u64> = None;
        let mut retrycount: Option<u64> = None;
        let mut ids: Vec<StreamId> = Vec::new();
        let mut i = 4;
        while i < args.len() {
            let tok = match args[i].as_bulk_string() {
                Some(b) => b,
                None => return Ok(RespValue::error("ERR syntax error")),
            };
            let upper = String::from_utf8_lossy(tok).to_ascii_uppercase();
            match upper.as_str() {
                "FORCE" => {
                    force = true;
                    i += 1;
                }
                "JUSTID" => {
                    just_id = true;
                    i += 1;
                }
                "IDLE" => {
                    if i + 1 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    idle_ms = match args[i + 1].as_bulk_string() {
                        Some(b) => match std::str::from_utf8(b)
                            .ok()
                            .and_then(|s| s.parse::<i64>().ok())
                        {
                            Some(n) if n >= 0 => Some(n as u64),
                            _ => {
                                return Ok(RespValue::error(
                                    "ERR value is not an integer or out of range",
                                ));
                            }
                        },
                        None => {
                            return Ok(RespValue::error(
                                "ERR value is not an integer or out of range",
                            ));
                        }
                    };
                    i += 2;
                }
                "TIME" => {
                    if i + 1 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    time_ms = match args[i + 1].as_bulk_string() {
                        Some(b) => match std::str::from_utf8(b)
                            .ok()
                            .and_then(|s| s.parse::<i64>().ok())
                        {
                            Some(n) if n >= 0 => Some(n as u64),
                            _ => {
                                return Ok(RespValue::error(
                                    "ERR value is not an integer or out of range",
                                ));
                            }
                        },
                        None => {
                            return Ok(RespValue::error(
                                "ERR value is not an integer or out of range",
                            ));
                        }
                    };
                    i += 2;
                }
                "RETRYCOUNT" => {
                    if i + 1 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    retrycount = match args[i + 1].as_bulk_string() {
                        Some(b) => match std::str::from_utf8(b)
                            .ok()
                            .and_then(|s| s.parse::<i64>().ok())
                        {
                            Some(n) if n >= 0 => Some(n as u64),
                            _ => {
                                return Ok(RespValue::error(
                                    "ERR value is not an integer or out of range",
                                ));
                            }
                        },
                        None => {
                            return Ok(RespValue::error(
                                "ERR value is not an integer or out of range",
                            ));
                        }
                    };
                    i += 2;
                }
                "LASTID" => {
                    // Accepted for Redis compatibility; no-op (no XCLAIM LASTID tracking yet).
                    i += 2;
                }
                _ => {
                    let s = String::from_utf8_lossy(tok);
                    match StreamId::parse_explicit(&s).or_else(|| StreamId::parse(&s)) {
                        Some(id) => ids.push(id),
                        None => {
                            return Ok(RespValue::error(
                                "ERR Invalid stream ID specified as stream command argument",
                            ));
                        }
                    }
                    i += 1;
                }
            }
        }
        if ids.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'xclaim' command",
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
                let mut s = stream.write();
                let opts = XClaimOpts {
                    min_idle_ms: min_idle,
                    force,
                    time_ms,
                    idle_ms,
                    retrycount,
                    just_id,
                };
                let claimed = match s.xclaim(&group, &consumer, &ids, &opts) {
                    Ok(c) => c,
                    Err(e) => {
                        if e.starts_with("NOGROUP") {
                            return Ok(RespValue::error(format!(
                                "NOGROUP No such key '{}' or consumer group",
                                String::from_utf8_lossy(&key)
                            )));
                        }
                        return Ok(RespValue::error(e));
                    }
                };
                if just_id {
                    let arr: Vec<RespValue> = claimed
                        .into_iter()
                        .map(|id| RespValue::BulkString(Some(id.to_bytes())))
                        .collect();
                    Ok(RespValue::Array(arr))
                } else {
                    Ok(Self::claimed_entries_resp(&s, &claimed))
                }
            }
            _ => Ok(RespValue::error(Error::WrongType.to_resp_string())),
        }
    }

    fn claimed_entries_resp(
        stream: &crate::stream_type::RedisStream,
        ids: &[StreamId],
    ) -> RespValue {
        let mut arr = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(entry) = stream.get_entry(id) {
                arr.push(Self::stream_entry_to_resp(entry));
            }
        }
        RespValue::Array(arr)
    }

    /// XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
    pub(super) fn handle_xautoclaim(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 5 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'xautoclaim' command",
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
        let consumer = match args[2].as_bulk_string() {
            Some(c) => c.clone(),
            None => return Ok(RespValue::error("ERR invalid consumer name")),
        };
        let min_idle = match args[3].as_bulk_string() {
            Some(b) => match std::str::from_utf8(b).ok().and_then(|s| s.parse::<i64>().ok()) {
                Some(n) if n >= 0 => n as u64,
                _ => {
                    return Ok(RespValue::error(
                        "ERR value is not an integer or out of range",
                    ));
                }
            },
            None => {
                return Ok(RespValue::error(
                    "ERR value is not an integer or out of range",
                ));
            }
        };
        let start_s = match args[4].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR invalid stream ID")),
        };
        let start = if start_s == "-" {
            StreamId::MIN
        } else {
            match StreamId::parse_explicit(&start_s).or_else(|| StreamId::parse(&start_s)) {
                Some(id) => id,
                None => {
                    return Ok(RespValue::error(
                        "ERR Invalid stream ID specified as stream command argument",
                    ));
                }
            }
        };

        let mut count: usize = 100; // Redis default
        let mut just_id = false;
        let mut i = 5;
        while i < args.len() {
            let tok = match args[i].as_bulk_string() {
                Some(b) => String::from_utf8_lossy(b).to_ascii_uppercase(),
                None => return Ok(RespValue::error("ERR syntax error")),
            };
            match tok.as_str() {
                "COUNT" => {
                    if i + 1 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    count = match args[i + 1].as_bulk_string() {
                        Some(b) => match std::str::from_utf8(b)
                            .ok()
                            .and_then(|s| s.parse::<i64>().ok())
                        {
                            Some(n) if n > 0 => n as usize,
                            _ => {
                                return Ok(RespValue::error(
                                    "ERR COUNT must be > 0",
                                ));
                            }
                        },
                        None => {
                            return Ok(RespValue::error(
                                "ERR value is not an integer or out of range",
                            ));
                        }
                    };
                    i += 2;
                }
                "JUSTID" => {
                    just_id = true;
                    i += 1;
                }
                _ => return Ok(RespValue::error("ERR syntax error")),
            }
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
                let mut s = stream.write();
                let (next, claimed, deleted) =
                    match s.xautoclaim(&group, &consumer, min_idle, start, count, just_id) {
                        Ok(v) => v,
                        Err(e) => {
                            if e.starts_with("NOGROUP") {
                                return Ok(RespValue::error(format!(
                                    "NOGROUP No such key '{}' or consumer group",
                                    String::from_utf8_lossy(&key)
                                )));
                            }
                            return Ok(RespValue::error(e));
                        }
                    };

                let messages = if just_id {
                    RespValue::Array(
                        claimed
                            .into_iter()
                            .map(|id| RespValue::BulkString(Some(id.to_bytes())))
                            .collect(),
                    )
                } else {
                    Self::claimed_entries_resp(&s, &claimed)
                };
                let deleted_arr = RespValue::Array(
                    deleted
                        .into_iter()
                        .map(|id| RespValue::BulkString(Some(id.to_bytes())))
                        .collect(),
                );
                Ok(RespValue::Array(vec![
                    RespValue::BulkString(Some(next.to_bytes())),
                    messages,
                    deleted_arr,
                ]))
            }
            _ => Ok(RespValue::error(Error::WrongType.to_resp_string())),
        }
    }

    /// XSETID key last-id [ENTRIESADDED n] [MAXDELETEDID id]
    pub(super) fn handle_xsetid(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'xsetid' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let id_s = match args[1].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR invalid stream ID")),
        };
        let id = match StreamId::parse_explicit(&id_s).or_else(|| StreamId::parse(&id_s)) {
            Some(id) => id,
            None => {
                return Ok(RespValue::error(
                    "ERR Invalid stream ID specified as stream command argument",
                ));
            }
        };
        let mut entries_added: Option<u64> = None;
        let mut max_deleted: Option<StreamId> = None;
        let mut i = 2;
        while i < args.len() {
            let opt = match args[i].as_bulk_string() {
                Some(b) => String::from_utf8_lossy(b).to_ascii_uppercase(),
                None => return Ok(RespValue::error("ERR syntax error")),
            };
            match opt.as_str() {
                "ENTRIESADDED" => {
                    if i + 1 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    let n = match self.parse_integer(&args[i + 1]) {
                        Ok(v) if v >= 0 => v as u64,
                        _ => {
                            return Ok(RespValue::error(
                                "ERR value is not an integer or out of range",
                            ));
                        }
                    };
                    entries_added = Some(n);
                    i += 2;
                }
                "MAXDELETEDID" => {
                    if i + 1 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    let mid_s = match args[i + 1].as_bulk_string() {
                        Some(b) => String::from_utf8_lossy(b).into_owned(),
                        None => return Ok(RespValue::error("ERR invalid stream ID")),
                    };
                    let mid = match StreamId::parse_explicit(&mid_s)
                        .or_else(|| StreamId::parse(&mid_s))
                    {
                        Some(id) => id,
                        None => {
                            return Ok(RespValue::error(
                                "ERR Invalid stream ID specified as stream command argument",
                            ));
                        }
                    };
                    max_deleted = Some(mid);
                    i += 2;
                }
                _ => return Ok(RespValue::error("ERR syntax error")),
            }
        }
        match self.cache.key_type(&key) {
            KeyType::None => Ok(RespValue::error("ERR no such key")),
            KeyType::Stream => {
                let stream = match self.cache.get_stream(&key) {
                    Some(s) => s,
                    None => return Ok(RespValue::error("ERR no such key")),
                };
                let mut s = stream.write();
                match s.xsetid(id, entries_added, max_deleted) {
                    Ok(()) => Ok(RespValue::ok()),
                    Err(e) => Ok(RespValue::error(e)),
                }
            }
            _ => Ok(RespValue::error(Error::WrongType.to_resp_string())),
        }
    }

    /// XINFO STREAM key
    /// XINFO GROUPS key
    /// XINFO CONSUMERS key groupname
    pub(super) fn handle_xinfo(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'xinfo' command",
            ));
        }
        let sub = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_ascii_uppercase(),
            None => return Ok(RespValue::error("ERR syntax error")),
        };
        match sub.as_str() {
            "STREAM" => {
                if args.len() < 2 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'xinfo|stream' command",
                    ));
                }
                let key = match args[1].as_bulk_string() {
                    Some(k) => k.clone(),
                    None => return Ok(RespValue::error("ERR invalid key")),
                };
                // Optional: FULL [COUNT count]
                let mut full = false;
                let mut full_count: Option<usize> = None;
                let mut i = 2;
                while i < args.len() {
                    let opt = match args[i].as_bulk_string() {
                        Some(b) => String::from_utf8_lossy(b).to_ascii_uppercase(),
                        None => return Ok(RespValue::error("ERR syntax error")),
                    };
                    match opt.as_str() {
                        "FULL" => {
                            full = true;
                            i += 1;
                        }
                        "COUNT" => {
                            if i + 1 >= args.len() {
                                return Ok(RespValue::error("ERR syntax error"));
                            }
                            let n = match self.parse_integer(&args[i + 1]) {
                                Ok(v) if v >= 0 => v as usize,
                                _ => {
                                    return Ok(RespValue::error(
                                        "ERR value is not an integer or out of range",
                                    ));
                                }
                            };
                            full_count = Some(n);
                            i += 2;
                        }
                        _ => return Ok(RespValue::error("ERR syntax error")),
                    }
                }
                // COUNT without FULL is invalid; Redis requires FULL for COUNT.
                if full_count.is_some() && !full {
                    return Ok(RespValue::error("ERR syntax error"));
                }
                // Redis default COUNT for FULL is 10.
                if full && full_count.is_none() {
                    full_count = Some(10);
                }
                match self.cache.key_type(&key) {
                    KeyType::None => Ok(RespValue::error("ERR no such key")),
                    KeyType::Stream => {
                        let stream = match self.cache.get_stream(&key) {
                            Some(s) => s,
                            None => return Ok(RespValue::error("ERR no such key")),
                        };
                        let s = stream.read();
                        let info = s.xinfo_stream();
                        let mut reply = vec![
                            bulk_static(b"length"),
                            RespValue::Integer(info.length as i64),
                            bulk_static(b"radix-tree-keys"),
                            RespValue::Integer(info.length as i64),
                            bulk_static(b"radix-tree-nodes"),
                            RespValue::Integer(info.length.saturating_add(1) as i64),
                            bulk_static(b"last-generated-id"),
                            RespValue::BulkString(Some(info.last_generated_id.to_bytes())),
                            bulk_static(b"max-deleted-entry-id"),
                            RespValue::BulkString(Some(info.max_deleted_entry_id.to_bytes())),
                            bulk_static(b"entries-added"),
                            RespValue::Integer(info.entries_added as i64),
                        ];
                        if full {
                            let entries = s.xinfo_stream_entries(full_count);
                            let entry_resp: Vec<RespValue> = entries
                                .iter()
                                .map(|e| Self::stream_entry_to_resp(e))
                                .collect();
                            let groups = s.xinfo_stream_full_groups(full_count);
                            let groups_resp: Vec<RespValue> = groups
                                .into_iter()
                                .map(|g| Self::xinfo_full_group_to_resp(g))
                                .collect();
                            reply.push(bulk_static(b"entries"));
                            reply.push(RespValue::Array(entry_resp));
                            reply.push(bulk_static(b"groups"));
                            reply.push(RespValue::Array(groups_resp));
                        } else {
                            let first = match info.first_entry {
                                Some(ref e) => Self::stream_entry_to_resp(e),
                                None => RespValue::null(),
                            };
                            let last = match info.last_entry {
                                Some(ref e) => Self::stream_entry_to_resp(e),
                                None => RespValue::null(),
                            };
                            reply.push(bulk_static(b"groups"));
                            reply.push(RespValue::Integer(info.groups as i64));
                            reply.push(bulk_static(b"first-entry"));
                            reply.push(first);
                            reply.push(bulk_static(b"last-entry"));
                            reply.push(last);
                        }
                        Ok(RespValue::Array(reply))
                    }
                    _ => Ok(RespValue::error(Error::WrongType.to_resp_string())),
                }
            }
            "GROUPS" => {
                if args.len() != 2 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'xinfo|groups' command",
                    ));
                }
                let key = match args[1].as_bulk_string() {
                    Some(k) => k.clone(),
                    None => return Ok(RespValue::error("ERR invalid key")),
                };
                match self.cache.key_type(&key) {
                    KeyType::None => Ok(RespValue::error("ERR no such key")),
                    KeyType::Stream => {
                        let stream = match self.cache.get_stream(&key) {
                            Some(s) => s,
                            None => return Ok(RespValue::error("ERR no such key")),
                        };
                        let groups = stream.read().xinfo_groups();
                        let rows: Vec<RespValue> = groups
                            .into_iter()
                            .map(|g| {
                                RespValue::Array(vec![
                                    bulk_static(b"name"),
                                    RespValue::BulkString(Some(g.name)),
                                    bulk_static(b"consumers"),
                                    RespValue::Integer(g.consumers as i64),
                                    bulk_static(b"pending"),
                                    RespValue::Integer(g.pending as i64),
                                    bulk_static(b"last-delivered-id"),
                                    RespValue::BulkString(Some(g.last_delivered_id.to_bytes())),
                                    bulk_static(b"entries-read"),
                                    match g.entries_read {
                                        Some(n) => RespValue::Integer(n as i64),
                                        None => RespValue::null(),
                                    },
                                    bulk_static(b"lag"),
                                    RespValue::Integer(g.lag as i64),
                                ])
                            })
                            .collect();
                        Ok(RespValue::Array(rows))
                    }
                    _ => Ok(RespValue::error(Error::WrongType.to_resp_string())),
                }
            }
            "CONSUMERS" => {
                if args.len() != 3 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'xinfo|consumers' command",
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
                        let s = stream.read();
                        match s.xinfo_consumers(&group) {
                            Ok(consumers) => {
                                let rows: Vec<RespValue> = consumers
                                    .into_iter()
                                    .map(|c| {
                                        RespValue::Array(vec![
                                            bulk_static(b"name"),
                                            RespValue::BulkString(Some(c.name)),
                                            bulk_static(b"pending"),
                                            RespValue::Integer(c.pending as i64),
                                            bulk_static(b"idle"),
                                            RespValue::Integer(c.idle_ms as i64),
                                            bulk_static(b"inactive"),
                                            RespValue::Integer(c.inactive_ms as i64),
                                        ])
                                    })
                                    .collect();
                                Ok(RespValue::Array(rows))
                            }
                            Err(e) => {
                                if e.starts_with("NOGROUP") {
                                    Ok(RespValue::error(format!(
                                        "NOGROUP No such key '{}' or consumer group",
                                        String::from_utf8_lossy(&key)
                                    )))
                                } else {
                                    Ok(RespValue::error(e))
                                }
                            }
                        }
                    }
                    _ => Ok(RespValue::error(Error::WrongType.to_resp_string())),
                }
            }
            "HELP" => Ok(RespValue::Array(vec![
                bulk_static(b"STREAM <key> [FULL [COUNT <count>]]"),
                bulk_static(b"GROUPS <key>"),
                bulk_static(b"CONSUMERS <key> <groupname>"),
                bulk_static(b"HELP"),
            ])),
            _ => Ok(RespValue::error(format!(
                "ERR unknown subcommand '{}'. Try XINFO HELP.",
                sub
            ))),
        }
    }
}

fn bulk_static(s: &'static [u8]) -> RespValue {
    RespValue::BulkString(Some(Bytes::from_static(s)))
}

/// Outcome of one non-blocking XREADGROUP attempt.
enum XReadGroupOutcome {
    Data(RespValue),
    Empty,
    Error(RespValue),
}
