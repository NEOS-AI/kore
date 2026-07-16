use crate::error::Result;
use crate::protocol::RespValue;
use super::CommandHandler;

/// Optional EXPIRE condition: NX | XX | GT | LT (at most one).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExpireCond {
    None,
    Nx,
    Xx,
    Gt,
    Lt,
}

impl CommandHandler {
    pub(super) fn handle_expire(&self, args: &[RespValue]) -> Result<RespValue> {
        self.expire_relative(args, true, "expire")
    }

    pub(super) fn handle_pexpire(&self, args: &[RespValue]) -> Result<RespValue> {
        self.expire_relative(args, false, "pexpire")
    }

    pub(super) fn handle_expireat(&self, args: &[RespValue]) -> Result<RespValue> {
        self.expire_absolute(args, true, "expireat")
    }

    pub(super) fn handle_pexpireat(&self, args: &[RespValue]) -> Result<RespValue> {
        self.expire_absolute(args, false, "pexpireat")
    }

    /// EXPIRE/PEXPIRE key ttl [NX|XX|GT|LT]
    fn expire_relative(
        &self,
        args: &[RespValue],
        seconds: bool,
        cmd: &str,
    ) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(format!(
                "ERR wrong number of arguments for '{}' command",
                cmd
            )));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let ttl_raw = match self.parse_integer(&args[1]) {
            Ok(n) => n,
            Err(_) => {
                return Ok(RespValue::error(
                    "ERR value is not an integer or out of range",
                ))
            }
        };
        if ttl_raw < 0 {
            return Ok(RespValue::error(format!(
                "ERR invalid expire time in '{}' command",
                cmd
            )));
        }
        let cond = match parse_expire_cond(&args[2..]) {
            Ok(c) => c,
            Err(e) => return Ok(RespValue::error(e)),
        };

        let ttl_ms = if seconds {
            (ttl_raw as u64).saturating_mul(1000)
        } else {
            ttl_raw as u64
        };

        let current = self.cache.ttl(key);
        if current == -2 {
            return Ok(RespValue::Integer(0));
        }
        if !expire_cond_allows(cond, current, ttl_ms as i64) {
            return Ok(RespValue::Integer(0));
        }
        let result = self.cache.expire(key, ttl_ms)?;
        Ok(RespValue::Integer(if result { 1 } else { 0 }))
    }

    /// EXPIREAT/PEXPIREAT key timestamp [NX|XX|GT|LT]
    fn expire_absolute(
        &self,
        args: &[RespValue],
        unix_seconds: bool,
        cmd: &str,
    ) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(format!(
                "ERR wrong number of arguments for '{}' command",
                cmd
            )));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let ts = match self.parse_integer(&args[1]) {
            Ok(n) => n,
            Err(_) => {
                return Ok(RespValue::error(
                    "ERR value is not an integer or out of range",
                ))
            }
        };
        if ts < 0 {
            return Ok(RespValue::error(format!(
                "ERR invalid expire time in '{}' command",
                cmd
            )));
        }
        let cond = match parse_expire_cond(&args[2..]) {
            Ok(c) => c,
            Err(e) => return Ok(RespValue::error(e)),
        };

        let expire_unix_ms = if unix_seconds {
            (ts as i64).saturating_mul(1000)
        } else {
            ts
        };

        let current = self.cache.ttl(key);
        if current == -2 {
            return Ok(RespValue::Integer(0));
        }
        // Compare remaining ms for GT/LT (absolute → remaining).
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let new_remaining = (expire_unix_ms - now_ms).max(0);
        if !expire_cond_allows(cond, current, new_remaining) {
            return Ok(RespValue::Integer(0));
        }
        let result = self.cache.expire_at_unix_ms(key, expire_unix_ms)?;
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

fn parse_expire_cond(args: &[RespValue]) -> std::result::Result<ExpireCond, String> {
    if args.is_empty() {
        return Ok(ExpireCond::None);
    }
    if args.len() > 1 {
        return Err("ERR syntax error".into());
    }
    let tok = match args[0].as_bulk_string() {
        Some(s) => String::from_utf8_lossy(s).to_ascii_uppercase(),
        None => return Err("ERR syntax error".into()),
    };
    match tok.as_str() {
        "NX" => Ok(ExpireCond::Nx),
        "XX" => Ok(ExpireCond::Xx),
        "GT" => Ok(ExpireCond::Gt),
        "LT" => Ok(ExpireCond::Lt),
        _ => Err("ERR Unsupported option provided to EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT".into()),
    }
}

/// `current_ttl_ms`: -1 no expire, >=0 remaining. `new_ttl_ms` proposed remaining.
fn expire_cond_allows(cond: ExpireCond, current_ttl_ms: i64, new_ttl_ms: i64) -> bool {
    match cond {
        ExpireCond::None => true,
        ExpireCond::Nx => current_ttl_ms < 0, // only when no expire
        ExpireCond::Xx => current_ttl_ms >= 0, // only when has expire
        // GT/LT require existing expire (Redis).
        ExpireCond::Gt => current_ttl_ms >= 0 && new_ttl_ms > current_ttl_ms,
        ExpireCond::Lt => current_ttl_ms >= 0 && new_ttl_ms < current_ttl_ms,
    }
}
