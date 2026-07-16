use crate::error::{Error, Result};
use crate::protocol::RespValue;
use super::CommandHandler;

impl CommandHandler {
    pub(super) fn handle_incr(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'incr'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        if let Err(Error::WrongType) = self.cache.ensure_string_or_absent(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

        match self.cache.incr(key, 1) {
            Ok(new_value) => Ok(RespValue::Integer(new_value)),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    pub(super) fn handle_decr(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'decr'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        if let Err(Error::WrongType) = self.cache.ensure_string_or_absent(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

        match self.cache.decr(key, 1) {
            Ok(new_value) => Ok(RespValue::Integer(new_value)),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    pub(super) fn handle_incrby(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'incrby'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        if let Err(Error::WrongType) = self.cache.ensure_string_or_absent(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

        let delta = self.parse_integer(&args[1])?;

        match self.cache.incr(key, delta) {
            Ok(new_value) => Ok(RespValue::Integer(new_value)),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    pub(super) fn handle_decrby(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'decrby'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        if let Err(Error::WrongType) = self.cache.ensure_string_or_absent(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

        let delta = self.parse_integer(&args[1])?;

        match self.cache.decr(key, delta) {
            Ok(new_value) => Ok(RespValue::Integer(new_value)),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    /// INCRBYFLOAT key increment — float RMW; bulk string reply (Redis-compatible).
    pub(super) fn handle_incrbyfloat(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'incrbyfloat' command",
            ));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        if let Err(Error::WrongType) = self.cache.ensure_string_or_absent(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

        let delta = match self.parse_float(&args[1]) {
            Ok(d) if d.is_finite() => d,
            Ok(_) => {
                return Ok(RespValue::error(
                    "ERR value is not a valid float",
                ));
            }
            Err(_) => {
                return Ok(RespValue::error(
                    "ERR value is not a valid float",
                ));
            }
        };

        match self.cache.incr_by_float(key, delta) {
            Ok(v) => {
                let s = if v.fract() == 0.0 && v.is_finite() && v.abs() < 1e15 {
                    format!("{}", v as i64)
                } else {
                    format!("{}", v)
                };
                Ok(RespValue::BulkString(Some(bytes::Bytes::from(s))))
            }
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }
}
