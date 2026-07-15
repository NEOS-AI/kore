//! PFADD / PFCOUNT / PFMERGE

use super::CommandHandler;
use crate::error::{Error, Result};
use crate::protocol::RespValue;

impl CommandHandler {
    pub(super) fn handle_pfadd(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'pfadd'",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let mut elements = Vec::new();
        for a in &args[1..] {
            match a.as_bulk_string() {
                Some(e) => elements.push(e.clone()),
                None => return Ok(RespValue::error("ERR invalid argument")),
            }
        }
        match self.cache.pfadd(&key, &elements) {
            Ok(n) => Ok(RespValue::Integer(n)),
            Err(Error::WrongType) => Ok(RespValue::error(Error::WrongType.to_resp_string())),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    pub(super) fn handle_pfcount(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'pfcount'",
            ));
        }
        let mut keys = Vec::new();
        for a in args {
            match a.as_bulk_string() {
                Some(k) => keys.push(k.clone()),
                None => return Ok(RespValue::error("ERR invalid key")),
            }
        }
        match self.cache.pfcount(&keys) {
            Ok(n) => Ok(RespValue::Integer(n)),
            Err(Error::WrongType) => Ok(RespValue::error(Error::WrongType.to_resp_string())),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    pub(super) fn handle_pfmerge(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'pfmerge'",
            ));
        }
        let dest = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let mut sources = Vec::new();
        for a in &args[1..] {
            match a.as_bulk_string() {
                Some(k) => sources.push(k.clone()),
                None => return Ok(RespValue::error("ERR invalid key")),
            }
        }
        match self.cache.pfmerge(&dest, &sources) {
            Ok(()) => Ok(RespValue::ok()),
            Err(Error::WrongType) => Ok(RespValue::error(Error::WrongType.to_resp_string())),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }
}
