use crate::acl::AuthError;
use crate::error::Result;
use crate::protocol::RespValue;
use bytes::Bytes;
use std::time::{SystemTime, UNIX_EPOCH};
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

    /// TIME — Redis wall-clock: array of two bulk strings [unix_sec, unix_usec].
    pub(super) fn handle_time(&self, args: &[RespValue]) -> Result<RespValue> {
        if !args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'time' command",
            ));
        }
        let dur = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let secs = dur.as_secs().to_string();
        let usecs = dur.subsec_micros().to_string();
        Ok(RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from(secs))),
            RespValue::BulkString(Some(Bytes::from(usecs))),
        ]))
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

    /// LOLWUT [version] — Redis easter-egg version art (Kore-flavored).
    pub(super) fn handle_lolwut(&self, args: &[RespValue]) -> Result<RespValue> {
        // Optional version arg accepted and ignored (Redis 6/7 art variants).
        if args.len() > 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'lolwut' command",
            ));
        }
        if args.len() == 1 {
            if self.parse_integer(&args[0]).is_err() {
                return Ok(RespValue::error(
                    "ERR value is not an integer or out of range",
                ));
            }
        }
        let ver = env!("CARGO_PKG_VERSION");
        let art = format!(
            r#"
    .--.  Kore Redis/Valkey-compatible server
   /    \   version {ver}
   | Kore|  https://github.com/NEOS-AI/kore
   \    /
    `--'
   (ok, not much art — but it is LOLWUT)
"#
        );
        Ok(RespValue::BulkString(Some(Bytes::from(art))))
    }

    /// READONLY — enable reading from cluster replicas on this connection.
    pub(super) fn handle_readonly(&mut self, args: &[RespValue]) -> Result<RespValue> {
        if !args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'readonly' command",
            ));
        }
        self.cluster_readonly = true;
        Ok(RespValue::ok())
    }

    /// READWRITE — disable READONLY mode (default).
    pub(super) fn handle_readwrite(&mut self, args: &[RespValue]) -> Result<RespValue> {
        if !args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'readwrite' command",
            ));
        }
        self.cluster_readonly = false;
        Ok(RespValue::ok())
    }
}
