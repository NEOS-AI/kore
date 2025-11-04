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
        let result = self.cache.expire(key, (seconds * 1000) as u64)?;

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
        let result = self.cache.expire(key, ms as u64)?;

        Ok(RespValue::Integer(if result { 1 } else { 0 }))
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
}
