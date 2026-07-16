use crate::error::Result;
use crate::protocol::RespValue;
use super::CommandHandler;

impl CommandHandler {
    pub(super) fn handle_expire(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'expire'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let seconds = self.parse_integer(&args[1])?;
        if seconds < 0 {
            return Ok(RespValue::error("ERR invalid expire time in 'expire' command"));
        }
        let result = self.cache.expire(key, (seconds as u64).saturating_mul(1000))?;

        Ok(RespValue::Integer(if result { 1 } else { 0 }))
    }

    pub(super) fn handle_pexpire(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'pexpire'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let ms = self.parse_integer(&args[1])?;
        if ms < 0 {
            return Ok(RespValue::error("ERR invalid expire time in 'pexpire' command"));
        }
        let result = self.cache.expire(key, ms as u64)?;

        Ok(RespValue::Integer(if result { 1 } else { 0 }))
    }

    pub(super) fn handle_expireat(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'expireat'"));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let ts = self.parse_integer(&args[1])?;
        if ts < 0 {
            return Ok(RespValue::error("ERR invalid expire time in 'expireat' command"));
        }
        // Unix seconds → ms
        let result = self
            .cache
            .expire_at_unix_ms(key, (ts as i64).saturating_mul(1000))?;
        Ok(RespValue::Integer(if result { 1 } else { 0 }))
    }

    pub(super) fn handle_pexpireat(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'pexpireat'"));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let ts = self.parse_integer(&args[1])?;
        if ts < 0 {
            return Ok(RespValue::error("ERR invalid expire time in 'pexpireat' command"));
        }
        let result = self.cache.expire_at_unix_ms(key, ts)?;
        Ok(RespValue::Integer(if result { 1 } else { 0 }))
    }

    pub(super) fn handle_persist(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'persist'"));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let removed = self.cache.persist(key);
        Ok(RespValue::Integer(if removed { 1 } else { 0 }))
    }

    pub(super) fn handle_ttl(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'ttl'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let ttl_ms = self.cache.ttl(key);
        let ttl_sec = if ttl_ms >= 0 {
            (ttl_ms / 1000) as i64
        } else {
            ttl_ms
        };

        Ok(RespValue::Integer(ttl_sec))
    }

    pub(super) fn handle_pttl(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'pttl'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let ttl = self.cache.ttl(key);
        Ok(RespValue::Integer(ttl))
    }

    /// EXPIRETIME — absolute expire as Unix seconds (-1 no expire, -2 missing).
    pub(super) fn handle_expiretime(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'expiretime'",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let ms = self.cache.expire_time_unix_ms(key);
        let out = if ms >= 0 {
            ms / 1000
        } else {
            ms
        };
        Ok(RespValue::Integer(out))
    }

    /// PEXPIRETIME — absolute expire as Unix milliseconds.
    pub(super) fn handle_pexpiretime(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'pexpiretime'",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        Ok(RespValue::Integer(self.cache.expire_time_unix_ms(key)))
    }
}
