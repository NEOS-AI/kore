use crate::error::Result;
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

        match self.cache.incr(key, 1) {
            Ok(new_value) => Ok(RespValue::Integer(new_value)),
            Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
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

        match self.cache.decr(key, 1) {
            Ok(new_value) => Ok(RespValue::Integer(new_value)),
            Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
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

        let delta = self.parse_integer(&args[1])?;

        match self.cache.incr(key, delta) {
            Ok(new_value) => Ok(RespValue::Integer(new_value)),
            Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
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

        let delta = self.parse_integer(&args[1])?;

        match self.cache.decr(key, delta) {
            Ok(new_value) => Ok(RespValue::Integer(new_value)),
            Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
        }
    }
}
