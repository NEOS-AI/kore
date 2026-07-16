use super::CommandHandler;
use crate::error::Result;
use crate::protocol::RespValue;
use bytes::Bytes;

impl CommandHandler {
    pub(super) fn handle_multi(&mut self) -> Result<RespValue> {
        if self.in_multi {
            return Ok(RespValue::error("ERR MULTI calls can not be nested"));
        }
        self.in_multi = true;
        self.multi_aborted = false;
        self.multi_queue.clear();
        Ok(RespValue::ok())
    }

    pub(super) async fn handle_exec(&mut self) -> Result<RespValue> {
        if !self.in_multi {
            return Ok(RespValue::error("ERR EXEC without MULTI"));
        }

        // Snapshot queue and leave multi mode before running (matches Redis).
        let queue = std::mem::take(&mut self.multi_queue);
        let aborted = self.multi_aborted;
        self.in_multi = false;
        self.multi_aborted = false;

        if aborted {
            self.clear_watches();
            return Ok(RespValue::error(
                "EXECABORT Transaction discarded because of previous errors.",
            ));
        }

        // Optimistic lock check: any watched key changed since WATCH?
        if self.is_watch_dirty() {
            self.clear_watches();
            return Ok(RespValue::null());
        }

        self.executing_multi = true;
        let mut results = Vec::with_capacity(queue.len());
        for cmd in queue {
            // Box to allow recursive async (EXEC → handle → …).
            match Box::pin(self.handle(cmd)).await {
                Ok(resp) => results.push(resp),
                Err(e) => results.push(RespValue::error(format!("ERR {}", e))),
            }
        }
        self.executing_multi = false;

        // Redis always clears WATCH after EXEC (success or empty).
        self.clear_watches();
        Ok(RespValue::Array(results))
    }

    pub(super) fn handle_discard(&mut self) -> Result<RespValue> {
        if !self.in_multi {
            return Ok(RespValue::error("ERR DISCARD without MULTI"));
        }
        self.in_multi = false;
        self.multi_aborted = false;
        self.multi_queue.clear();
        self.clear_watches();
        Ok(RespValue::ok())
    }

    pub(super) fn handle_watch(&mut self, args: &[RespValue]) -> Result<RespValue> {
        if self.in_multi {
            return Ok(RespValue::error("ERR WATCH inside MULTI is not allowed"));
        }
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'watch' command",
            ));
        }

        for arg in args {
            let key = match arg.as_bulk_string() {
                Some(k) => k.clone(),
                None => {
                    return Ok(RespValue::error("ERR invalid key"));
                }
            };
            let gen = self.cache.watch_generation(&key);
            // Keep the earliest generation if already watched (Redis re-WATCH is fine).
            self.watched.entry(key).or_insert(gen);
        }
        Ok(RespValue::ok())
    }

    pub(super) fn handle_unwatch(&mut self) -> Result<RespValue> {
        self.clear_watches();
        Ok(RespValue::ok())
    }

    pub(super) fn clear_watches(&mut self) {
        self.watched.clear();
    }

    pub(super) fn is_watch_dirty(&self) -> bool {
        for (key, expected) in &self.watched {
            if self.cache.watch_generation(key) != *expected {
                return true;
            }
        }
        false
    }

    /// Queue a command while inside MULTI. Returns `QUEUED` or a queue-time error.
    pub(super) fn queue_multi_command(
        &mut self,
        cmd_upper: &str,
        full_value: RespValue,
    ) -> Result<RespValue> {
        // Commands that must not be queued.
        if matches!(
            cmd_upper,
            "MULTI"
                | "WATCH"
                | "SUBSCRIBE"
                | "UNSUBSCRIBE"
                | "PSUBSCRIBE"
                | "PUNSUBSCRIBE"
                | "SSUBSCRIBE"
                | "SUNSUBSCRIBE"
        ) {
            self.multi_aborted = true;
            return Ok(RespValue::error(format!(
                "ERR command '{}' cannot be used inside MULTI",
                cmd_upper
            )));
        }

        self.multi_queue.push(full_value);
        Ok(RespValue::SimpleString(Bytes::from_static(b"QUEUED")))
    }

    /// Notify WATCH trackers after a successful write.
    pub(super) fn notify_watch_after_write(&self, cmd: &str, args: &[RespValue]) {
        if matches!(cmd, "FLUSHDB" | "FLUSHALL") {
            self.cache.touch_all_watch_keys();
            return;
        }
        for key in write_keys(cmd, args) {
            self.cache.touch_watch_key(&key);
        }
    }
}

/// Extract keys affected by a write command (best-effort for WATCH).
fn write_keys(cmd: &str, args: &[RespValue]) -> Vec<Bytes> {
    match cmd {
        "SET" | "SETNX" | "GETDEL" | "GETEX" | "APPEND" | "SETRANGE" | "SETEX" | "GETSET" | "INCR"
        | "DECR" | "INCRBY" | "DECRBY" | "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" | "PERSIST" | "ZADD" | "ZREM" | "ZINCRBY"
        | "ZREMRANGEBYRANK" | "ZREMRANGEBYSCORE" | "ZREMRANGEBYLEX" | "ZPOPMIN" | "ZPOPMAX" | "GEOADD"
        | "GEOSEARCHSTORE" | "HSET" | "HMSET" | "HDEL" | "HINCRBY" | "HINCRBYFLOAT" | "LPUSH" | "RPUSH" | "LPOP"
        | "RPOP" | "LSET" | "LREM" | "LTRIM" | "LINSERT" | "SADD" | "SREM" | "SPOP" | "XADD" | "XDEL" | "XTRIM" | "XACK"
        | "XCLAIM" | "XAUTOCLAIM" | "XSETID"
        | "SETBIT" | "BITFIELD" | "PFADD" | "TOUCH" | "MOVE" => args
            .first()
            .and_then(|a| a.as_bulk_string())
            .cloned()
            .into_iter()
            .collect(),
        // SINTERSTORE/SUNIONSTORE/SDIFFSTORE destination key [key ...] — dest is first arg
        // *STORE destination … — dest is first arg
        "SINTERSTORE" | "SUNIONSTORE" | "SDIFFSTORE"
        | "ZUNIONSTORE" | "ZINTERSTORE" | "ZDIFFSTORE" => args
            .first()
            .and_then(|a| a.as_bulk_string())
            .cloned()
            .into_iter()
            .collect(),
        // SMOVE / LMOVE / BLMOVE: source + destination
        "SMOVE" | "LMOVE" | "BLMOVE" => args
            .iter()
            .take(2)
            .filter_map(|a| a.as_bulk_string().cloned())
            .collect(),
        // BITOP dest key [key ...] — dest is first arg after command
        "BITOP" => args
            .get(1)
            .and_then(|a| a.as_bulk_string())
            .cloned()
            .into_iter()
            .collect(),
        // PFMERGE dest source [source ...]
        "PFMERGE" => args
            .first()
            .and_then(|a| a.as_bulk_string())
            .cloned()
            .into_iter()
            .collect(),
        // BLPOP/BRPOP/BZPOP*: all args except the trailing timeout are keys
        "BLPOP" | "BRPOP" | "BZPOPMIN" | "BZPOPMAX" => {
            if args.len() < 2 {
                Vec::new()
            } else {
                args[..args.len() - 1]
                    .iter()
                    .filter_map(|a| a.as_bulk_string())
                    .cloned()
                    .collect()
            }
        }
        // ZMPOP numkeys key [key ...] MIN|MAX [COUNT n]
        "ZMPOP" => numkeys_keys(args, 0),
        // BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT n]
        "BZMPOP" => numkeys_keys(args, 1),
        // XGROUP CREATE/DESTROY key …
        "XGROUP" => args
            .get(1)
            .and_then(|a| a.as_bulk_string())
            .cloned()
            .into_iter()
            .collect(),
        "COPY" => args
            .iter()
            .take(2)
            .filter_map(|a| a.as_bulk_string())
            .cloned()
            .collect(),
        "DEL" | "UNLINK" => args
            .iter()
            .filter_map(|a| a.as_bulk_string())
            .cloned()
            .collect(),
        "RENAME" | "RENAMENX" => args
            .iter()
            .take(2)
            .filter_map(|a| a.as_bulk_string())
            .cloned()
            .collect(),
        "MSET" | "MSETNX" => args
            .iter()
            .step_by(2)
            .filter_map(|a| a.as_bulk_string())
            .cloned()
            .collect(),
        _ => Vec::new(),
    }
}

/// Keys after a `numkeys` integer at `args[numkeys_idx]` (e.g. ZMPOP / BZMPOP).
fn numkeys_keys(args: &[RespValue], numkeys_idx: usize) -> Vec<Bytes> {
    let Some(nk_arg) = args.get(numkeys_idx) else {
        return Vec::new();
    };
    let n = match nk_arg {
        RespValue::Integer(i) if *i > 0 => *i as usize,
        RespValue::BulkString(Some(b)) => {
            match std::str::from_utf8(b).ok().and_then(|s| s.parse::<i64>().ok()) {
                Some(i) if i > 0 => i as usize,
                _ => return Vec::new(),
            }
        }
        _ => return Vec::new(),
    };
    let start = numkeys_idx + 1;
    let end = (start + n).min(args.len());
    args[start..end]
        .iter()
        .filter_map(|a| a.as_bulk_string().cloned())
        .collect()
}
