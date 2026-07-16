use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::memory::MemoryCategory;
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

    /// SCAN cursor [MATCH pattern] [COUNT count]
    pub(super) fn handle_scan(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'scan' command",
            ));
        }

        let cursor = match self.parse_integer(&args[0]) {
            Ok(c) if c >= 0 => c as u64,
            _ => {
                return Ok(RespValue::error("ERR invalid cursor"));
            }
        };

        let (pattern, count) = match self.parse_scan_options(&args[1..]) {
            Ok(v) => v,
            Err(e) => return Ok(e),
        };

        let pattern_ref = pattern.as_deref();
        let (next_cursor, keys) = self.cache.scan(cursor, pattern_ref, count);

        let resp_keys: Vec<RespValue> = keys
            .into_iter()
            .map(|k| RespValue::BulkString(Some(k)))
            .collect();

        Ok(scan_reply(next_cursor, resp_keys))
    }

    /// Parse optional `MATCH pattern` / `COUNT n` after the cursor (or key+cursor).
    pub(super) fn parse_scan_options(
        &self,
        args: &[RespValue],
    ) -> std::result::Result<(Option<String>, usize), RespValue> {
        let mut pattern: Option<String> = None;
        let mut count: usize = 10; // Redis-compatible default
        let mut i = 0;
        while i < args.len() {
            let opt = match args[i].as_bulk_string() {
                Some(s) => String::from_utf8_lossy(s).to_uppercase(),
                None => return Err(RespValue::error("ERR syntax error")),
            };
            match opt.as_str() {
                "MATCH" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(RespValue::error("ERR syntax error"));
                    }
                    match args[i].as_bulk_string() {
                        Some(s) => pattern = Some(String::from_utf8_lossy(s).into_owned()),
                        None => return Err(RespValue::error("ERR syntax error")),
                    }
                }
                "COUNT" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(RespValue::error("ERR syntax error"));
                    }
                    match self.parse_integer(&args[i]) {
                        Ok(c) if c > 0 => count = c as usize,
                        _ => {
                            return Err(RespValue::error(
                                "ERR value is not an integer or out of range",
                            ));
                        }
                    }
                }
                _ => return Err(RespValue::error("ERR syntax error")),
            }
            i += 1;
        }
        Ok((pattern, count))
    }

    /// SELECT index — switch the connection to another logical database.
    pub(super) fn handle_select(&mut self, args: &[RespValue]) -> Result<RespValue> {
        // Defense-in-depth: cluster gate also rejects SELECT.
        if self.cluster.is_some() {
            return Ok(RespValue::error(
                "ERR SELECT is not allowed in cluster mode",
            ));
        }
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'select' command",
            ));
        }
        let index = match self.parse_integer(&args[0]) {
            Ok(i) if i >= 0 => i as usize,
            _ => {
                return Ok(RespValue::error("ERR DB index is out of range"));
            }
        };
        let Some(db) = self.databases.get(index) else {
            return Ok(RespValue::error("ERR DB index is out of range"));
        };
        if index != self.selected_db {
            // Drop watches on the previous keyspace (safe; keys are DB-scoped).
            self.clear_watches();
            self.selected_db = index;
            self.cache = db;
        }
        Ok(RespValue::ok())
    }

    /// FLUSHDB — clear only the currently selected database.
    pub(super) fn handle_flushdb(&self, _args: &[RespValue]) -> Result<RespValue> {
        self.cache.flush();
        Ok(RespValue::ok())
    }

    /// FLUSHALL — clear every logical database.
    pub(super) fn handle_flushall(&self, _args: &[RespValue]) -> Result<RespValue> {
        self.databases.flush_all();
        Ok(RespValue::ok())
    }

    /// MEMORY USAGE key [SAMPLES count]
    /// Returns estimated bytes for `key`, or null if missing. SAMPLES is accepted and ignored.
    pub(super) fn handle_memory(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'memory' command",
            ));
        }
        let sub = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_ascii_uppercase(),
            None => return Ok(RespValue::error("ERR syntax error")),
        };
        match sub.as_str() {
            "USAGE" => self.handle_memory_usage(&args[1..]),
            "STATS" => self.handle_memory_stats(&args[1..]),
            "DOCTOR" => self.handle_memory_doctor(&args[1..]),
            "PURGE" => self.handle_memory_purge(&args[1..]),
            "HELP" => Ok(RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from_static(
                    b"MEMORY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"USAGE <key> [SAMPLES <count>] -- estimate memory use of a key",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"STATS -- overview of tracked memory by category",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"DOCTOR -- human-readable memory health notes",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"PURGE -- best-effort allocator trim (no-op if unsupported)",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"HELP -- print this help",
                ))),
            ])),
            _ => Ok(RespValue::error(format!(
                "ERR unknown subcommand '{}'. Try MEMORY HELP.",
                sub
            ))),
        }
    }

    /// MEMORY STATS — flat array of field/value pairs (Redis-compatible shape).
    fn handle_memory_stats(&self, args: &[RespValue]) -> Result<RespValue> {
        if !args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'memory|stats' command",
            ));
        }
        use crate::memory::MemoryCategory;
        let used = self.cache.memory_usage();
        let max = self.cache.max_memory();
        let pairs: Vec<(&str, i64)> = vec![
            ("peak.allocated", used as i64),
            ("total.allocated", used as i64),
            ("startup.allocated", 0),
            ("keys.bytes-per-key", 0),
            ("dataset.bytes", used as i64),
            (
                "dataset.percentage",
                if max == 0 {
                    0
                } else {
                    ((used as f64 / max as f64) * 100.0) as i64
                },
            ),
            ("allocator.allocated", used as i64),
            ("allocator.active", used as i64),
            ("allocator.resident", used as i64),
            (
                "memory.cache",
                self.cache.category_memory(MemoryCategory::Cache) as i64,
            ),
            (
                "memory.hashes",
                self.cache.category_memory(MemoryCategory::Hashes) as i64,
            ),
            (
                "memory.lists",
                self.cache.category_memory(MemoryCategory::Lists) as i64,
            ),
            (
                "memory.sets",
                self.cache.category_memory(MemoryCategory::Sets) as i64,
            ),
            (
                "memory.sorted_sets",
                self.cache.category_memory(MemoryCategory::SortedSets) as i64,
            ),
            (
                "memory.geo_sets",
                self.cache.category_memory(MemoryCategory::GeoSets) as i64,
            ),
            (
                "memory.streams",
                self.cache.category_memory(MemoryCategory::Streams) as i64,
            ),
            (
                "memory.pubsub",
                self.cache.category_memory(MemoryCategory::PubSub) as i64,
            ),
            (
                "memory.search",
                self.cache.category_memory(MemoryCategory::Search) as i64,
            ),
            ("maxmemory", max as i64),
            ("db.size", self.cache.dbsize() as i64),
        ];
        let mut out = Vec::with_capacity(pairs.len() * 2);
        for (k, v) in pairs {
            out.push(RespValue::BulkString(Some(Bytes::from(k))));
            out.push(RespValue::Integer(v));
        }
        Ok(RespValue::Array(out))
    }

    /// MEMORY DOCTOR — short advisory text.
    fn handle_memory_doctor(&self, args: &[RespValue]) -> Result<RespValue> {
        if !args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'memory|doctor' command",
            ));
        }
        let used = self.cache.memory_usage();
        let max = self.cache.max_memory();
        let mut notes = Vec::new();
        if max == 0 {
            notes.push(
                "Maxmemory is not configured; Kore will not evict under pressure.".to_string(),
            );
        } else {
            let pct = (used as f64 / max as f64) * 100.0;
            if pct > 90.0 {
                notes.push(format!(
                    "High memory utilization: {:.1}% of maxmemory used.",
                    pct
                ));
            } else if pct > 70.0 {
                notes.push(format!(
                    "Moderate memory utilization: {:.1}% of maxmemory used.",
                    pct
                ));
            } else {
                notes.push(format!(
                    "Memory utilization is healthy: {:.1}% of maxmemory used.",
                    pct
                ));
            }
        }
        notes.push(format!(
            "Tracked dataset ≈ {} bytes across {} keys (DB {}).",
            used,
            self.cache.dbsize(),
            self.selected_db
        ));
        notes.push(
            "Kore uses structural size estimates (not jemalloc RSS); MEMORY STATS reflects those counters."
                .into(),
        );
        Ok(RespValue::BulkString(Some(Bytes::from(notes.join("\n")))))
    }

    /// MEMORY PURGE — Redis frees jemalloc dirty pages; we return OK (best-effort no-op).
    fn handle_memory_purge(&self, args: &[RespValue]) -> Result<RespValue> {
        if !args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'memory|purge' command",
            ));
        }
        // No jemalloc control in Kore; acknowledge for client compatibility.
        Ok(RespValue::ok())
    }

    /// SLOWLOG GET [count] | LEN | RESET | HELP
    pub(super) fn handle_slowlog(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'slowlog' command",
            ));
        }
        let sub = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_ascii_uppercase(),
            None => return Ok(RespValue::error("ERR syntax error")),
        };
        match sub.as_str() {
            "GET" => {
                let count = if args.len() == 1 {
                    10usize
                } else if args.len() == 2 {
                    match self.parse_integer(&args[1]) {
                        Ok(n) if n >= 0 => n as usize,
                        Ok(_) => usize::MAX, // negative → all (Redis)
                        Err(_) => {
                            return Ok(RespValue::error(
                                "ERR value is not an integer or out of range",
                            ))
                        }
                    }
                } else {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'slowlog|get' command",
                    ));
                };
                let entries = self.cache.slowlog.get(count);
                let arr: Vec<RespValue> = entries
                    .into_iter()
                    .map(|e| {
                        let argv: Vec<RespValue> = e
                            .argv
                            .into_iter()
                            .map(|a| RespValue::BulkString(Some(a)))
                            .collect();
                        RespValue::Array(vec![
                            RespValue::Integer(e.id as i64),
                            RespValue::Integer(e.timestamp),
                            RespValue::Integer(e.duration_us),
                            RespValue::Array(argv),
                        ])
                    })
                    .collect();
                Ok(RespValue::Array(arr))
            }
            "LEN" => {
                if args.len() != 1 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'slowlog|len' command",
                    ));
                }
                Ok(RespValue::Integer(self.cache.slowlog.len() as i64))
            }
            "RESET" => {
                if args.len() != 1 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'slowlog|reset' command",
                    ));
                }
                self.cache.slowlog.reset();
                Ok(RespValue::ok())
            }
            "HELP" => Ok(RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from_static(
                    b"SLOWLOG <subcommand> [<arg> ...]. Subcommands are:",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"GET [<count>] -- return newest slow log entries",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"LEN -- number of entries in the slow log",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"RESET -- clear the slow log",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(b"HELP -- print this help"))),
            ])),
            _ => Ok(RespValue::error(format!(
                "ERR unknown subcommand '{}'. Try SLOWLOG HELP.",
                sub
            ))),
        }
    }

    fn handle_memory_usage(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'memory|usage' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        // Optional SAMPLES count (Redis samples nested structures; we ignore).
        let mut i = 1;
        while i < args.len() {
            let opt = match args[i].as_bulk_string() {
                Some(s) => String::from_utf8_lossy(s).to_ascii_uppercase(),
                None => return Ok(RespValue::error("ERR syntax error")),
            };
            if opt != "SAMPLES" {
                return Ok(RespValue::error("ERR syntax error"));
            }
            i += 1;
            if i >= args.len() {
                return Ok(RespValue::error("ERR syntax error"));
            }
            if self.parse_integer(&args[i]).is_err() {
                return Ok(RespValue::error(
                    "ERR value is not an integer or out of range",
                ));
            }
            i += 1;
        }

        match self.key_memory_bytes(key) {
            Some(n) => Ok(RespValue::Integer(n as i64)),
            None => Ok(RespValue::null()),
        }
    }

    /// Estimated accounted size of a key (same helpers as maxmemory tracking).
    fn key_memory_bytes(&self, key: &Bytes) -> Option<usize> {
        use crate::cache::KeyType;
        use crate::entry::LoadOptions;
        match self.cache.key_type(key) {
            KeyType::None => None,
            KeyType::String => {
                let entry = self.cache.load(key, LoadOptions::default()).ok()??;
                Some(entry.size())
            }
            KeyType::Hash => {
                let h = self.cache.get_hash(key)?;
                let content = h.read().memory_size();
                Some(crate::memory::estimate_keyed_object(key.len(), content))
            }
            KeyType::List => {
                let l = self.cache.get_list(key)?;
                let content = l.read().memory_size();
                Some(crate::memory::estimate_keyed_object(key.len(), content))
            }
            KeyType::Set => {
                let s = self.cache.get_set(key)?;
                let content = s.read().memory_size();
                Some(crate::memory::estimate_keyed_object(key.len(), content))
            }
            KeyType::ZSet => {
                let z = self.cache.get_sorted_set(key)?;
                let content = z.read().memory_size();
                Some(crate::memory::estimate_keyed_object(key.len(), content))
            }
            KeyType::Geo => {
                let g = self.cache.get_geo_set(key)?;
                let content = g.read().memory_usage();
                Some(crate::memory::estimate_keyed_object(key.len(), content))
            }
            KeyType::Stream => {
                let s = self.cache.get_stream(key)?;
                let content = s.read().memory_size();
                Some(crate::memory::estimate_keyed_object(key.len(), content))
            }
        }
    }

    /// OBJECT ENCODING key — report internal encoding name (Redis-compatible labels).
    pub(super) fn handle_object(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'object' command",
            ));
        }
        let sub = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_ascii_uppercase(),
            None => return Ok(RespValue::error("ERR syntax error")),
        };
        match sub.as_str() {
            "ENCODING" => self.handle_object_encoding(&args[1..]),
            "IDLETIME" => self.handle_object_idletime(&args[1..]),
            "REFCOUNT" => self.handle_object_refcount(&args[1..]),
            "FREQ" => self.handle_object_freq(&args[1..]),
            "HELP" => Ok(RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from_static(
                    b"OBJECT <subcommand> key. Subcommands are:",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"ENCODING <key> -- report the encoding used to store the key",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"IDLETIME <key> -- idle time in seconds (string keys)",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"REFCOUNT <key> -- approximate object refcount (1 when present)",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"FREQ <key> -- logarithmic LFU counter (string keys)",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"HELP -- print this help",
                ))),
            ])),
            _ => Ok(RespValue::error(format!(
                "ERR unknown subcommand '{}'. Try OBJECT HELP.",
                sub
            ))),
        }
    }

    fn handle_object_encoding(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'object|encoding' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        use crate::cache::KeyType;
        let enc: Option<&'static str> = match self.cache.key_type(key) {
            KeyType::None => None,
            // Kore stores all strings as raw byte blobs (no int/embstr specializations).
            KeyType::String => Some("raw"),
            KeyType::Hash => Some("hashtable"),
            KeyType::List => Some("quicklist"),
            KeyType::Set => Some("hashtable"),
            KeyType::ZSet | KeyType::Geo => Some("skiplist"),
            KeyType::Stream => Some("stream"),
        };
        match enc {
            Some(s) => Ok(RespValue::BulkString(Some(Bytes::from_static(s.as_bytes())))),
            None => Ok(RespValue::null()),
        }
    }

    /// OBJECT IDLETIME key — seconds since last access (string keys; typed → 0).
    fn handle_object_idletime(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'object|idletime' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        use crate::cache::KeyType;
        use crate::entry::LoadOptions;
        use std::time::Instant;
        match self.cache.key_type(key) {
            KeyType::None => Ok(RespValue::null()),
            KeyType::String => {
                let entry = match self.cache.load(
                    key,
                    LoadOptions {
                        touch: false,
                        with_cas: false,
                    },
                )? {
                    Some(e) => e,
                    None => return Ok(RespValue::null()),
                };
                let idle = Instant::now()
                    .saturating_duration_since(entry.last_access_time())
                    .as_secs() as i64;
                Ok(RespValue::Integer(idle))
            }
            // Typed keys do not track LRU idle yet — report 0 (recently used).
            _ => Ok(RespValue::Integer(0)),
        }
    }

    /// OBJECT REFCOUNT key — Redis returns allocator refcount; we report 1 if present.
    fn handle_object_refcount(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'object|refcount' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        use crate::cache::KeyType;
        if matches!(self.cache.key_type(key), KeyType::None) {
            Ok(RespValue::null())
        } else {
            Ok(RespValue::Integer(1))
        }
    }

    /// OBJECT FREQ key — logarithmic LFU counter for string keys (0 for typed).
    fn handle_object_freq(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'object|freq' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        use crate::cache::KeyType;
        use crate::entry::LoadOptions;
        match self.cache.key_type(key) {
            KeyType::None => Ok(RespValue::null()),
            KeyType::String => {
                let entry = match self.cache.load(
                    key,
                    LoadOptions {
                        touch: false,
                        with_cas: false,
                    },
                )? {
                    Some(e) => e,
                    None => return Ok(RespValue::null()),
                };
                let freq = entry.lfu_freq(self.cache.lfu_decay_time()) as i64;
                Ok(RespValue::Integer(freq))
            }
            _ => Ok(RespValue::Integer(0)),
        }
    }

    /// SWAPDB index1 index2 — atomically swap two logical database keyspaces.
    pub(super) fn handle_swapdb(&mut self, args: &[RespValue]) -> Result<RespValue> {
        if self.cluster.is_some() {
            return Ok(RespValue::error(
                "ERR SWAPDB is not allowed in cluster mode",
            ));
        }
        if args.len() != 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'swapdb' command",
            ));
        }
        let a = match self.parse_integer(&args[0]) {
            Ok(i) if i >= 0 => i as usize,
            _ => return Ok(RespValue::error("ERR DB index is out of range")),
        };
        let b = match self.parse_integer(&args[1]) {
            Ok(i) if i >= 0 => i as usize,
            _ => return Ok(RespValue::error("ERR DB index is out of range")),
        };
        match self.databases.swap_db(a, b) {
            Ok(()) => {
                // Watches are DB-scoped; invalidate if either side is selected.
                if self.selected_db == a || self.selected_db == b {
                    self.clear_watches();
                    // Rebind Arc in case of future map-based swap; content swap is fine either way.
                    if let Some(db) = self.databases.get(self.selected_db) {
                        self.cache = db;
                    }
                }
                Ok(RespValue::ok())
            }
            Err(msg) => Ok(RespValue::error(msg)),
        }
    }

    pub(super) fn handle_info(&self, _args: &[RespValue]) -> Result<RespValue> {
        let stats = &self.cache.stats;
        let total_cmds = stats.total_commands_processed();
        let health = self.health_status();

        let redlock_enabled = if self.config.enable_redlock { 1 } else { 0 };
        let redlock_instances = if self.config.enable_redlock {
            self.config.redlock_instance_addrs().len()
        } else {
            0
        };
        let fair_queue_section = match &self.redlock {
            Some(rl) => rl.fair_queue_info_lines(),
            None => {
                if self.config.enable_fair_queue {
                    format!(
                        "fair_queue_enabled:1\r\n\
                         fair_queue_max_size:{}\r\n",
                        self.config.fair_queue_max_size
                    )
                } else {
                    "fair_queue_enabled:0\r\n".to_string()
                }
            }
        };

        let info = format!(
            "# Server\r\n\
             kore_version:{}\r\n\
             redlock_enabled:{}\r\n\
             redlock_instances:{}\r\n\
             redlock_retry_count:{}\r\n\
             redlock_retry_delay_ms:{}\r\n\
             \r\n\
             # FairQueue\r\n\
             {}\
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
             cmd_geoadd:{}\r\n\
             cmd_geosearch:{}\r\n\
             cmd_publish:{}\r\n\
             cmd_subscribe:{}\r\n\
             cmd_unsubscribe:{}\r\n\
             cmd_psubscribe:{}\r\n\
             cmd_punsubscribe:{}\r\n\
             cmd_pubsub:{}\r\n\
             keyspace_hits:{}\r\n\
             keyspace_misses:{}\r\n\
             hit_rate:{:.2}\r\n\
             evicted_expired:{}\r\n\
             evicted_lru:{}\r\n\
             \r\n\
             # Pub/Sub\r\n\
             pubsub_messages_sent:{}\r\n\
             pubsub_channels_active:{}\r\n\
             pubsub_patterns_active:{}\r\n\
             pubsub_clients_active:{}\r\n\
             \r\n\
             # Memory\r\n\
             used_memory:{}\r\n\
             maxmemory:{}\r\n\
             maxmemory_policy:{}\r\n\
             maxentrysize:{}\r\n\
             geo_sets_memory:{}\r\n\
             memory_cache_used:{}\r\n\
             memory_cache_utilization:{:.2}%\r\n\
             memory_pubsub_used:{}\r\n\
             memory_pubsub_utilization:{:.2}%\r\n\
             memory_sorted_sets_used:{}\r\n\
             memory_sorted_sets_utilization:{:.2}%\r\n\
             memory_geo_sets_used:{}\r\n\
             memory_geo_sets_utilization:{:.2}%\r\n\
             memory_total_utilization:{:.2}%\r\n\
             \r\n\
             # Network\r\n\
             bytes_sent:{}\r\n\
             bytes_received:{}\r\n\
             total_connections:{}\r\n\
             active_connections:{}\r\n\
             \r\n\
             # Replication\r\n\
             {}\
             \r\n\
             # Health\r\n\
             {}\
             \r\n\
             # Keyspace\r\n\
             db0:keys={},geo_sets={}\r\n",
            env!("CARGO_PKG_VERSION"),
            redlock_enabled,
            redlock_instances,
            self.config.redlock_retry_count,
            self.config.redlock_retry_delay_ms,
            fair_queue_section,
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
            stats.cmd_geoadd.load(Ordering::Relaxed),
            stats.cmd_geosearch.load(Ordering::Relaxed),
            stats.cmd_publish.load(Ordering::Relaxed),
            stats.cmd_subscribe.load(Ordering::Relaxed),
            stats.cmd_unsubscribe.load(Ordering::Relaxed),
            stats.cmd_psubscribe.load(Ordering::Relaxed),
            stats.cmd_punsubscribe.load(Ordering::Relaxed),
            stats.cmd_pubsub.load(Ordering::Relaxed),
            stats.hits.load(Ordering::Relaxed),
            stats.misses.load(Ordering::Relaxed),
            stats.get_hit_rate(),
            stats.evicted_expired.load(Ordering::Relaxed),
            stats.evicted_lru.load(Ordering::Relaxed),
            stats.pubsub_messages_sent.load(Ordering::Relaxed),
            stats.pubsub_channels_active.load(Ordering::Relaxed),
            stats.pubsub_patterns_active.load(Ordering::Relaxed),
            stats.pubsub_clients_active.load(Ordering::Relaxed),
            self.cache.memory_usage(),
            self.cache.max_memory(),
            self.cache.eviction_policy().as_str(),
            self.cache.get_max_entry_size(),
            self.cache.geo_sets_memory(),
            self.cache.memory_tracker.category_memory(MemoryCategory::Cache),
            self.cache.memory_tracker.category_memory(MemoryCategory::Cache) as f64 / self.cache.max_memory() as f64 * 100.0,
            self.cache.memory_tracker.category_memory(MemoryCategory::PubSub),
            self.cache.memory_tracker.category_memory(MemoryCategory::PubSub) as f64 / self.cache.max_memory() as f64 * 100.0,
            self.cache.memory_tracker.category_memory(MemoryCategory::SortedSets),
            self.cache.memory_tracker.category_memory(MemoryCategory::SortedSets) as f64 / self.cache.max_memory() as f64 * 100.0,
            self.cache.memory_tracker.category_memory(MemoryCategory::GeoSets),
            self.cache.memory_tracker.category_memory(MemoryCategory::GeoSets) as f64 / self.cache.max_memory() as f64 * 100.0,
            self.cache.memory_tracker.utilization(),
            stats.bytes_sent.load(Ordering::Relaxed),
            stats.bytes_received.load(Ordering::Relaxed),
            stats.total_connections.load(Ordering::Relaxed),
            stats.active_connections.load(Ordering::Relaxed),
            self.replication_info_section(),
            health.to_info_lines(),
            self.cache.dbsize(),
            self.cache.geo_set_count(),
        );

        Ok(RespValue::BulkString(Some(Bytes::from(info))))
    }

    /// HEALTH [PING|FULL] — liveness / structured readiness.
    ///
    /// - `HEALTH` / `HEALTH PING` → simple OK / PONG
    /// - `HEALTH FULL` → bulk string with ready, role, memory, master_link, rdb_last_save, aof
    pub(super) fn handle_health(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::SimpleString(Bytes::from_static(b"OK")));
        }

        let sub = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_uppercase(),
            None => return Ok(RespValue::error("ERR invalid HEALTH argument")),
        };

        match sub.as_str() {
            "PING" => Ok(RespValue::SimpleString(Bytes::from_static(b"PONG"))),
            "FULL" => {
                let status = self.health_status();
                Ok(RespValue::BulkString(Some(Bytes::from(
                    status.to_info_lines(),
                ))))
            }
            _ => Ok(RespValue::error(
                "ERR unknown HEALTH subcommand. Try HEALTH, HEALTH PING, or HEALTH FULL",
            )),
        }
    }

    fn health_status(&self) -> crate::metrics::HealthStatus {
        crate::metrics::collect_health(
            &self.cache,
            self.persistence.as_ref().map(|p| p.as_ref()),
        )
    }

    fn replication_info_section(&self) -> String {
        match self.persistence.as_ref() {
            Some(p) => p.replication.info_replication(),
            None => "role:master\r\nconnected_slaves:0\r\n".to_string(),
        }
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
                        Ok(self.config_kv_reply(
                            "maxentrysize",
                            &value.to_string(),
                        ))
                    }
                    "maxmemory" | "max-memory" => {
                        let value = self.cache.max_memory();
                        Ok(self.config_kv_reply("maxmemory", &value.to_string()))
                    }
                    "save" => {
                        let value = self
                            .persistence
                            .as_ref()
                            .map(|p| p.save_rules_string())
                            .unwrap_or_default();
                        Ok(self.config_kv_reply("save", &value))
                    }
                    "maxmemory-policy" | "maxmemory_policy" => {
                        let value = self.cache.eviction_policy().as_str();
                        Ok(self.config_kv_reply("maxmemory-policy", value))
                    }
                    "lfu-log-factor" | "lfu_log_factor" => Ok(self.config_kv_reply(
                        "lfu-log-factor",
                        &self.cache.lfu_log_factor().to_string(),
                    )),
                    "lfu-decay-time" | "lfu_decay_time" => Ok(self.config_kv_reply(
                        "lfu-decay-time",
                        &self.cache.lfu_decay_time().to_string(),
                    )),
                    "slowlog-log-slower-than" | "slowlog_log_slower_than" => Ok(self
                        .config_kv_reply(
                            "slowlog-log-slower-than",
                            &self.cache.slowlog.slower_than_us().to_string(),
                        )),
                    "slowlog-max-len" | "slowlog_max_len" => Ok(self.config_kv_reply(
                        "slowlog-max-len",
                        &self.cache.slowlog.max_len().to_string(),
                    )),
                    "databases" => Ok(self.config_kv_reply(
                        "databases",
                        &self.databases.len().to_string(),
                    )),
                    "min-replicas-to-write" | "min-slaves-to-write" => {
                        let n = self
                            .persistence
                            .as_ref()
                            .map(|p| p.replication.min_replicas_to_write())
                            .unwrap_or(0);
                        Ok(self.config_kv_reply(
                            "min-replicas-to-write",
                            &n.to_string(),
                        ))
                    }
                    "min-replicas-max-lag" | "min-slaves-max-lag" => {
                        let n = self
                            .persistence
                            .as_ref()
                            .map(|p| p.replication.min_replicas_max_lag())
                            .unwrap_or(10);
                        Ok(self.config_kv_reply(
                            "min-replicas-max-lag",
                            &n.to_string(),
                        ))
                    }
                    _ => {
                        // Empty reply for unknown parameters (Redis behavior).
                        // RESP3 uses an empty map.
                        if self.protocol_version() >= 3 {
                            Ok(RespValue::Map(vec![]))
                        } else {
                            Ok(RespValue::Array(vec![]))
                        }
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
                    "maxmemory" | "max-memory" => {
                        let size: usize = value_str.parse()
                            .map_err(|_| Error::InvalidArgument("invalid size".into()))?;

                        // Apply to every logical DB so SELECT'd keyspaces honor the limit
                        match self.cache.set_max_memory(size) {
                            Ok(_) => {
                                for db in self.databases.iter() {
                                    if !std::sync::Arc::ptr_eq(db, &self.cache) {
                                        let _ = db.set_max_memory(size);
                                    }
                                }
                                Ok(RespValue::ok())
                            }
                            Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
                        }
                    }
                    "save" => {
                        let Some(p) = self.persistence.as_ref() else {
                            return Ok(RespValue::error("ERR persistence not configured"));
                        };
                        match p.set_save_rules_from_str(&value_str) {
                            Ok(()) => Ok(RespValue::ok()),
                            Err(e) => Ok(RespValue::error(e.to_resp_string())),
                        }
                    }
                    "maxmemory-policy" | "maxmemory_policy" => {
                        match self.cache.set_eviction_policy_str(&value_str) {
                            Ok(()) => {
                                let policy = self.cache.eviction_policy();
                                self.databases.set_eviction_policy_all(policy);
                                Ok(RespValue::ok())
                            }
                            Err(e) => Ok(RespValue::error(e.to_resp_string())),
                        }
                    }
                    "lfu-log-factor" | "lfu_log_factor" => {
                        let n: u64 = match value_str.parse() {
                            Ok(n) => n,
                            Err(_) => {
                                return Ok(RespValue::error(
                                    "ERR invalid lfu-log-factor value",
                                ))
                            }
                        };
                        if n > 255 {
                            return Ok(RespValue::error(
                                "ERR lfu-log-factor must be between 0 and 255",
                            ));
                        }
                        let _ = self.cache.set_lfu_log_factor(n as u8);
                        for db in self.databases.iter() {
                            if !std::sync::Arc::ptr_eq(db, &self.cache) {
                                let _ = db.set_lfu_log_factor(n as u8);
                            }
                        }
                        Ok(RespValue::ok())
                    }
                    "slowlog-log-slower-than" | "slowlog_log_slower_than" => {
                        let n: i64 = match value_str.parse() {
                            Ok(n) => n,
                            Err(_) => {
                                return Ok(RespValue::error(
                                    "ERR invalid slowlog-log-slower-than value",
                                ))
                            }
                        };
                        self.cache.slowlog.set_slower_than_us(n);
                        // Shared across multi-DB keyspaces via Arc.
                        Ok(RespValue::ok())
                    }
                    "slowlog-max-len" | "slowlog_max_len" => {
                        let n: i64 = match value_str.parse() {
                            Ok(n) if n >= 0 => n,
                            Ok(_) => {
                                return Ok(RespValue::error(
                                    "ERR slowlog-max-len must be >= 0",
                                ))
                            }
                            Err(_) => {
                                return Ok(RespValue::error(
                                    "ERR invalid slowlog-max-len value",
                                ))
                            }
                        };
                        self.cache.slowlog.set_max_len(n as usize);
                        Ok(RespValue::ok())
                    }
                    "lfu-decay-time" | "lfu_decay_time" => {
                        let n: u64 = match value_str.parse() {
                            Ok(n) => n,
                            Err(_) => {
                                return Ok(RespValue::error(
                                    "ERR invalid lfu-decay-time value",
                                ))
                            }
                        };
                        if n > 255 {
                            return Ok(RespValue::error(
                                "ERR lfu-decay-time must be between 0 and 255",
                            ));
                        }
                        let _ = self.cache.set_lfu_decay_time(n as u8);
                        for db in self.databases.iter() {
                            if !std::sync::Arc::ptr_eq(db, &self.cache) {
                                let _ = db.set_lfu_decay_time(n as u8);
                            }
                        }
                        Ok(RespValue::ok())
                    }
                    "databases" => {
                        // Redis treats `databases` as read-only at runtime
                        Ok(RespValue::error(
                            "ERR CONFIG SET failed: unsupported config parameter for set: databases",
                        ))
                    }
                    "min-replicas-to-write" | "min-slaves-to-write" => {
                        let Some(p) = self.persistence.as_ref() else {
                            return Ok(RespValue::error("ERR persistence not configured"));
                        };
                        let n: usize = match value_str.parse() {
                            Ok(n) => n,
                            Err(_) => {
                                return Ok(RespValue::error(
                                    "ERR invalid min-replicas-to-write value",
                                ))
                            }
                        };
                        p.replication.set_min_replicas_to_write(n);
                        Ok(RespValue::ok())
                    }
                    "min-replicas-max-lag" | "min-slaves-max-lag" => {
                        let Some(p) = self.persistence.as_ref() else {
                            return Ok(RespValue::error("ERR persistence not configured"));
                        };
                        let n: usize = match value_str.parse() {
                            Ok(n) => n,
                            Err(_) => {
                                return Ok(RespValue::error(
                                    "ERR invalid min-replicas-max-lag value",
                                ))
                            }
                        };
                        p.replication.set_min_replicas_max_lag(n);
                        Ok(RespValue::ok())
                    }
                    _ => Ok(RespValue::error("ERR Unsupported CONFIG parameter")),
                }
            }
            _ => Ok(RespValue::error("ERR Unknown subcommand or wrong number of arguments")),
        }
    }
}

/// Build Redis SCAN-family reply: `[next_cursor, [elements...]]`.
pub(super) fn scan_reply(next_cursor: u64, elements: Vec<RespValue>) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(Some(Bytes::from(next_cursor.to_string()))),
        RespValue::Array(elements),
    ])
}

/// Stable cursor page over a sorted list of matched items.
pub(super) fn cursor_page<T: Clone>(items: &[T], cursor: u64, count: usize) -> (u64, Vec<T>) {
    let start = cursor as usize;
    if start >= items.len() {
        return (0, Vec::new());
    }
    let end = (start + count).min(items.len());
    let batch = items[start..end].to_vec();
    let next = if end >= items.len() { 0 } else { end as u64 };
    (next, batch)
}

/// MATCH helper for type scans (field / member name).
pub(super) fn scan_name_matches(pattern: Option<&str>, name: &[u8]) -> bool {
    match pattern {
        None => true,
        Some(pat) => {
            let text = std::str::from_utf8(name).unwrap_or("");
            crate::hashmap::pattern_match(pat, text)
        }
    }
}
