use crate::error::Result;
use crate::protocol::RespValue;
use bytes::Bytes;
use super::CommandHandler;

impl CommandHandler {
    pub(super) fn handle_ping(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            Ok(RespValue::SimpleString(Bytes::from("PONG")))
        } else if let Some(msg) = args[0].as_bulk_string() {
            Ok(RespValue::BulkString(Some(msg.clone())))
        } else {
            Ok(RespValue::error("ERR wrong number of arguments for 'ping'"))
        }
    }

    pub(super) fn handle_echo(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'echo'"));
        }

        match args[0].as_bulk_string() {
            Some(msg) => Ok(RespValue::BulkString(Some(msg.clone()))),
            None => Ok(RespValue::error("ERR invalid argument")),
        }
    }

    pub(super) fn handle_auth(&mut self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'auth'"));
        }

        let password = match args[0].as_bulk_string() {
            Some(p) => p,
            None => return Ok(RespValue::error("ERR invalid argument")),
        };

        self.cache.stats.incr(&self.cache.stats.auth_cmds);

        if self.config.auth.is_empty() {
            return Ok(RespValue::error("ERR Client sent AUTH, but no password is set"));
        }

        if password.as_ref() == self.config.auth.as_bytes() {
            self.authenticated = true;
            Ok(RespValue::ok())
        } else {
            self.cache.stats.incr(&self.cache.stats.auth_errors);
            Ok(RespValue::error("ERR invalid password"))
        }
    }
}
