use crate::acl::AuthError;
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

    /// AUTH <password> | AUTH <username> <password>
    pub(super) fn handle_auth(&mut self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() || args.len() > 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'auth'"));
        }

        let (username, password) = if args.len() == 1 {
            let password = match args[0].as_bulk_string() {
                Some(p) => p,
                None => return Ok(RespValue::error("ERR invalid argument")),
            };
            ("default".to_string(), password.clone())
        } else {
            let username = match args[0].as_bulk_string() {
                Some(u) => String::from_utf8_lossy(u).into_owned(),
                None => return Ok(RespValue::error("ERR invalid argument")),
            };
            let password = match args[1].as_bulk_string() {
                Some(p) => p.clone(),
                None => return Ok(RespValue::error("ERR invalid argument")),
            };
            (username, password)
        };

        self.cache.stats.incr(&self.cache.stats.auth_cmds);

        let pass_str = String::from_utf8_lossy(&password);
        match self.acl.authenticate(&username, &pass_str) {
            Ok(()) => {
                self.authenticated = true;
                self.username = Some(username);
                Ok(RespValue::ok())
            }
            Err(AuthError::WrongPass) | Err(AuthError::Disabled) => {
                self.cache.stats.incr(&self.cache.stats.auth_errors);
                // Redis-style: do not reveal whether user exists or is disabled.
                Ok(RespValue::error(
                    "WRONGPASS invalid username-password pair or user is disabled.",
                ))
            }
        }
    }
}
