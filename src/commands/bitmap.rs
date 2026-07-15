//! SETBIT / GETBIT / BITCOUNT / BITOP / BITPOS / BITFIELD

use super::CommandHandler;
use crate::cache::{BitOpKind, BitfieldOp, BitfieldOverflow};
use crate::error::{Error, Result};
use crate::protocol::RespValue;
use bytes::Bytes;

impl CommandHandler {
    pub(super) fn handle_setbit(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'setbit'",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let offset = match self.parse_integer(&args[1]) {
            Ok(v) if v >= 0 => v as u64,
            _ => {
                return Ok(RespValue::error(
                    "ERR bit offset is not an integer or out of range",
                ))
            }
        };
        let bit = match self.parse_integer(&args[2]) {
            Ok(v) if v == 0 || v == 1 => v as u8,
            _ => {
                return Ok(RespValue::error(
                    "ERR bit is not an integer or out of range",
                ))
            }
        };
        match self.cache.setbit(&key, offset, bit) {
            Ok(prev) => Ok(RespValue::Integer(prev)),
            Err(Error::WrongType) => Ok(RespValue::error(Error::WrongType.to_resp_string())),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    pub(super) fn handle_getbit(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'getbit'",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let offset = match self.parse_integer(&args[1]) {
            Ok(v) if v >= 0 => v as u64,
            _ => {
                return Ok(RespValue::error(
                    "ERR bit offset is not an integer or out of range",
                ))
            }
        };
        match self.cache.getbit(key, offset) {
            Ok(b) => Ok(RespValue::Integer(b)),
            Err(Error::WrongType) => Ok(RespValue::error(Error::WrongType.to_resp_string())),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    pub(super) fn handle_bitcount(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() || args.len() > 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'bitcount'",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let (start, end) = if args.len() == 1 {
            (None, None)
        } else if args.len() == 2 {
            match self.parse_integer(&args[1]) {
                Ok(s) => (Some(s), None),
                Err(_) => {
                    return Ok(RespValue::error(
                        "ERR value is not an integer or out of range",
                    ))
                }
            }
        } else {
            let s = match self.parse_integer(&args[1]) {
                Ok(v) => v,
                Err(_) => {
                    return Ok(RespValue::error(
                        "ERR value is not an integer or out of range",
                    ))
                }
            };
            let e = match self.parse_integer(&args[2]) {
                Ok(v) => v,
                Err(_) => {
                    return Ok(RespValue::error(
                        "ERR value is not an integer or out of range",
                    ))
                }
            };
            (Some(s), Some(e))
        };
        match self.cache.bitcount(key, start, end) {
            Ok(n) => Ok(RespValue::Integer(n)),
            Err(Error::WrongType) => Ok(RespValue::error(Error::WrongType.to_resp_string())),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    pub(super) fn handle_bitpos(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 || args.len() > 4 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'bitpos'",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let bit = match self.parse_integer(&args[1]) {
            Ok(v) if v == 0 || v == 1 => v as u8,
            _ => {
                return Ok(RespValue::error(
                    "ERR bit is not an integer or out of range",
                ))
            }
        };
        let start = if args.len() >= 3 {
            match self.parse_integer(&args[2]) {
                Ok(v) => Some(v),
                Err(_) => {
                    return Ok(RespValue::error(
                        "ERR value is not an integer or out of range",
                    ))
                }
            }
        } else {
            None
        };
        let end = if args.len() >= 4 {
            match self.parse_integer(&args[3]) {
                Ok(v) => Some(v),
                Err(_) => {
                    return Ok(RespValue::error(
                        "ERR value is not an integer or out of range",
                    ))
                }
            }
        } else {
            None
        };
        match self.cache.bitpos(key, bit, start, end) {
            Ok(n) => Ok(RespValue::Integer(n)),
            Err(Error::WrongType) => Ok(RespValue::error(Error::WrongType.to_resp_string())),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    pub(super) fn handle_bitop(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'bitop'",
            ));
        }
        let op_s = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_uppercase(),
            None => return Ok(RespValue::error("ERR syntax error")),
        };
        let op = match op_s.as_str() {
            "AND" => BitOpKind::And,
            "OR" => BitOpKind::Or,
            "XOR" => BitOpKind::Xor,
            "NOT" => BitOpKind::Not,
            _ => {
                return Ok(RespValue::error(
                    "ERR syntax error",
                ))
            }
        };
        let dest = match args[1].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let mut keys = Vec::new();
        for a in &args[2..] {
            match a.as_bulk_string() {
                Some(k) => keys.push(k.clone()),
                None => return Ok(RespValue::error("ERR invalid key")),
            }
        }
        match self.cache.bitop(op, &dest, &keys) {
            Ok(n) => Ok(RespValue::Integer(n)),
            Err(Error::WrongType) => Ok(RespValue::error(Error::WrongType.to_resp_string())),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    pub(super) fn handle_bitfield(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'bitfield'",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let mut i = 1;
        let mut overflow = BitfieldOverflow::Wrap;
        let mut segment: Vec<BitfieldOp> = Vec::new();
        let mut all_replies: Vec<Option<i64>> = Vec::new();

        while i < args.len() {
            let tok = match args[i].as_bulk_string() {
                Some(t) => String::from_utf8_lossy(t).to_uppercase(),
                None => return Ok(RespValue::error("ERR syntax error")),
            };
            match tok.as_str() {
                "OVERFLOW" => {
                    if i + 1 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    if !segment.is_empty() {
                        match self.cache.bitfield(&key, &segment, overflow) {
                            Ok(r) => all_replies.extend(r),
                            Err(e) => return Ok(RespValue::error(e.to_resp_string())),
                        }
                        segment.clear();
                    }
                    let m = match args[i + 1].as_bulk_string() {
                        Some(t) => String::from_utf8_lossy(t).to_uppercase(),
                        None => return Ok(RespValue::error("ERR syntax error")),
                    };
                    overflow = match m.as_str() {
                        "WRAP" => BitfieldOverflow::Wrap,
                        "SAT" => BitfieldOverflow::Sat,
                        "FAIL" => BitfieldOverflow::Fail,
                        _ => return Ok(RespValue::error("ERR Invalid OVERFLOW type specified")),
                    };
                    i += 2;
                }
                "GET" => {
                    if i + 2 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    let (signed, bits) = match parse_bf_type(&args[i + 1]) {
                        Ok(t) => t,
                        Err(e) => return Ok(RespValue::error(e)),
                    };
                    let offset = match parse_bf_offset(&args[i + 2], bits) {
                        Ok(o) => o,
                        Err(e) => return Ok(RespValue::error(e)),
                    };
                    segment.push(BitfieldOp::Get {
                        signed,
                        bits,
                        offset,
                    });
                    i += 3;
                }
                "SET" => {
                    if i + 3 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    let (signed, bits) = match parse_bf_type(&args[i + 1]) {
                        Ok(t) => t,
                        Err(e) => return Ok(RespValue::error(e)),
                    };
                    let offset = match parse_bf_offset(&args[i + 2], bits) {
                        Ok(o) => o,
                        Err(e) => return Ok(RespValue::error(e)),
                    };
                    let value = match self.parse_integer(&args[i + 3]) {
                        Ok(v) => v,
                        Err(_) => {
                            return Ok(RespValue::error(
                                "ERR value is not an integer or out of range",
                            ))
                        }
                    };
                    segment.push(BitfieldOp::Set {
                        signed,
                        bits,
                        offset,
                        value,
                    });
                    i += 4;
                }
                "INCRBY" => {
                    if i + 3 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    let (signed, bits) = match parse_bf_type(&args[i + 1]) {
                        Ok(t) => t,
                        Err(e) => return Ok(RespValue::error(e)),
                    };
                    let offset = match parse_bf_offset(&args[i + 2], bits) {
                        Ok(o) => o,
                        Err(e) => return Ok(RespValue::error(e)),
                    };
                    let increment = match self.parse_integer(&args[i + 3]) {
                        Ok(v) => v,
                        Err(_) => {
                            return Ok(RespValue::error(
                                "ERR value is not an integer or out of range",
                            ))
                        }
                    };
                    segment.push(BitfieldOp::IncrBy {
                        signed,
                        bits,
                        offset,
                        increment,
                    });
                    i += 4;
                }
                _ => return Ok(RespValue::error("ERR syntax error")),
            }
        }
        if !segment.is_empty() {
            match self.cache.bitfield(&key, &segment, overflow) {
                Ok(r) => all_replies.extend(r),
                Err(e) => return Ok(RespValue::error(e.to_resp_string())),
            }
        }

        let arr: Vec<RespValue> = all_replies
            .into_iter()
            .map(|r| match r {
                Some(n) => RespValue::Integer(n),
                None => RespValue::null(),
            })
            .collect();
        Ok(RespValue::Array(arr))
    }
}

fn parse_bf_type(v: &RespValue) -> std::result::Result<(bool, u8), String> {
    let s = match v.as_bulk_string() {
        Some(b) => String::from_utf8_lossy(b).into_owned(),
        None => return Err("ERR syntax error".into()),
    };
    if s.len() < 2 {
        return Err("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.".into());
    }
    let signed = match s.as_bytes()[0].to_ascii_lowercase() {
        b'i' => true,
        b'u' => false,
        _ => {
            return Err(
                "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is."
                    .into(),
            )
        }
    };
    let bits: u8 = match s[1..].parse() {
        Ok(n) if n >= 1 && n <= 64 => n,
        _ => {
            return Err(
                "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is."
                    .into(),
            )
        }
    };
    if !signed && bits > 63 {
        return Err(
            "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is."
                .into(),
        );
    }
    Ok((signed, bits))
}

fn parse_bf_offset(v: &RespValue, type_bits: u8) -> std::result::Result<u64, String> {
    let s = match v.as_bulk_string() {
        Some(b) => String::from_utf8_lossy(b).into_owned(),
        None => {
            // integer form
            return match v {
                RespValue::Integer(n) if *n >= 0 => Ok(*n as u64),
                _ => Err("ERR bit offset is not an integer or out of range".into()),
            };
        }
    };
    if let Some(rest) = s.strip_prefix('#') {
        let n: i64 = rest
            .parse()
            .map_err(|_| "ERR bit offset is not an integer or out of range".to_string())?;
        if n < 0 {
            return Err("ERR bit offset is not an integer or out of range".into());
        }
        return Ok((n as u64).saturating_mul(type_bits as u64));
    }
    let n: i64 = s
        .parse()
        .map_err(|_| "ERR bit offset is not an integer or out of range".to_string())?;
    if n < 0 {
        return Err("ERR bit offset is not an integer or out of range".into());
    }
    Ok(n as u64)
}
