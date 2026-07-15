use crate::cache::KeyType;
use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::search_index::DocumentField;
use bytes::Bytes;
use std::collections::HashMap;
use super::CommandHandler;

impl CommandHandler {
    fn ensure_hash_key(&self, key: &Bytes) -> Result<Option<()>> {
        match self.cache.key_type(key) {
            KeyType::None => Ok(None),
            KeyType::Hash => Ok(Some(())),
            _ => Err(Error::WrongType),
        }
    }

    /// HSET key field value [field value ...]
    pub(super) fn handle_hset(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 3 || args.len() % 2 == 0 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'hset' command",
            ));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let mut pairs = Vec::new();
        let mut i = 1;
        while i + 1 < args.len() {
            let field = match args[i].as_bulk_string() {
                Some(f) => f.clone(),
                None => return Ok(RespValue::error("ERR invalid field")),
            };
            let value = match args[i + 1].as_bulk_string() {
                Some(v) => v.clone(),
                None => return Ok(RespValue::error("ERR invalid value")),
            };
            pairs.push((field, value));
            i += 2;
        }

        let est: usize = pairs.iter().map(|(f, v)| f.len() + v.len() + 32).sum();
        if let Err(e) = self.cache.ensure_non_string_capacity(est) {
            return Ok(RespValue::error(e.to_resp_string()));
        }

        let hash = match self.cache.get_or_create_hash(&key) {
            Ok(h) => h,
            Err(Error::WrongType) => {
                return Ok(RespValue::error(Error::WrongType.to_resp_string()));
            }
            Err(e) => return Ok(RespValue::error(e.to_resp_string())),
        };

        let mut h = hash.write();
        let before = key.len() + h.memory_size();
        let mut added = 0i64;
        for (field, value) in pairs {
            if h.hset(field, value) {
                added += 1;
            }
        }
        let after = key.len() + h.memory_size();
        // Snapshot fields for search auto-index (all values as Text for MVP)
        let mut index_fields = HashMap::new();
        for (f, v) in h.hgetall() {
            let fname = String::from_utf8_lossy(&f).into_owned();
            let fval = String::from_utf8_lossy(&v).into_owned();
            index_fields.insert(fname, DocumentField::Text(fval));
        }
        drop(h);
        self.cache.account_hash_delta(before, after);

        // After successful HSET, auto-index keys matching FT index PREFIX
        self.cache.auto_index_key(&key, index_fields);

        Ok(RespValue::Integer(added))
    }

    /// HGET key field
    pub(super) fn handle_hget(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'hget' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_hash_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let field = match args[1].as_bulk_string() {
            Some(f) => f,
            None => return Ok(RespValue::error("ERR invalid field")),
        };
        match self.cache.get_hash(key) {
            Some(h) => {
                let hash = h.read();
                match hash.hget(field) {
                    Some(v) => Ok(RespValue::BulkString(Some(v))),
                    None => Ok(RespValue::null()),
                }
            }
            None => Ok(RespValue::null()),
        }
    }

    /// HMGET key field [field ...]
    pub(super) fn handle_hmget(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'hmget' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_hash_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let fields: Vec<Bytes> = args[1..]
            .iter()
            .filter_map(|a| a.as_bulk_string().cloned())
            .collect();

        match self.cache.get_hash(key) {
            Some(h) => {
                let hash = h.read();
                let vals = hash.hmget(&fields);
                Ok(RespValue::Array(
                    vals.into_iter()
                        .map(|v| match v {
                            Some(b) => RespValue::BulkString(Some(b)),
                            None => RespValue::null(),
                        })
                        .collect(),
                ))
            }
            None => Ok(RespValue::Array(
                fields.iter().map(|_| RespValue::null()).collect(),
            )),
        }
    }

    /// HDEL key field [field ...]
    pub(super) fn handle_hdel(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'hdel' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_hash_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let fields: Vec<Bytes> = args[1..]
            .iter()
            .filter_map(|a| a.as_bulk_string().cloned())
            .collect();

        let hash = match self.cache.get_hash(key) {
            Some(h) => h,
            None => return Ok(RespValue::Integer(0)),
        };
        let mut h = hash.write();
        let before = key.len() + h.memory_size();
        let removed = h.hdel(&fields) as i64;
        let empty = h.is_empty();
        let after = key.len() + h.memory_size();
        drop(h);
        self.cache.account_hash_delta(before, after);
        if empty {
            self.cache.remove_hash(key);
        }
        Ok(RespValue::Integer(removed))
    }

    /// HGETALL key
    /// RESP2: flat array of field/value pairs. RESP3: map.
    pub(super) fn handle_hgetall(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'hgetall' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_hash_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        match self.cache.get_hash(key) {
            Some(h) => {
                let hash = h.read();
                if self.protocol_version() >= 3 {
                    let pairs: Vec<(RespValue, RespValue)> = hash
                        .hgetall()
                        .into_iter()
                        .map(|(f, v)| {
                            (
                                RespValue::BulkString(Some(f)),
                                RespValue::BulkString(Some(v)),
                            )
                        })
                        .collect();
                    Ok(RespValue::Map(pairs))
                } else {
                    let mut out = Vec::new();
                    for (f, v) in hash.hgetall() {
                        out.push(RespValue::BulkString(Some(f)));
                        out.push(RespValue::BulkString(Some(v)));
                    }
                    Ok(RespValue::Array(out))
                }
            }
            None => {
                if self.protocol_version() >= 3 {
                    Ok(RespValue::Map(vec![]))
                } else {
                    Ok(RespValue::Array(vec![]))
                }
            }
        }
    }

    /// HLEN key
    pub(super) fn handle_hlen(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'hlen' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_hash_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let n = self
            .cache
            .get_hash(key)
            .map(|h| h.read().hlen())
            .unwrap_or(0);
        Ok(RespValue::Integer(n as i64))
    }

    /// HEXISTS key field
    pub(super) fn handle_hexists(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'hexists' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_hash_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let field = match args[1].as_bulk_string() {
            Some(f) => f,
            None => return Ok(RespValue::error("ERR invalid field")),
        };
        let exists = self
            .cache
            .get_hash(key)
            .map(|h| h.read().hexists(field))
            .unwrap_or(false);
        Ok(RespValue::Integer(if exists { 1 } else { 0 }))
    }

    /// HKEYS key
    pub(super) fn handle_hkeys(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'hkeys' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_hash_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        match self.cache.get_hash(key) {
            Some(h) => {
                let hash = h.read();
                Ok(RespValue::Array(
                    hash.hkeys()
                        .into_iter()
                        .map(|k| RespValue::BulkString(Some(k)))
                        .collect(),
                ))
            }
            None => Ok(RespValue::Array(vec![])),
        }
    }

    /// HVALS key
    pub(super) fn handle_hvals(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'hvals' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_hash_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        match self.cache.get_hash(key) {
            Some(h) => {
                let hash = h.read();
                Ok(RespValue::Array(
                    hash.hvals()
                        .into_iter()
                        .map(|v| RespValue::BulkString(Some(v)))
                        .collect(),
                ))
            }
            None => Ok(RespValue::Array(vec![])),
        }
    }

    /// HINCRBY key field increment
    pub(super) fn handle_hincrby(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'hincrby' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let field = match args[1].as_bulk_string() {
            Some(f) => f.clone(),
            None => return Ok(RespValue::error("ERR invalid field")),
        };
        let delta = match self.parse_integer(&args[2]) {
            Ok(d) => d,
            Err(_) => {
                return Ok(RespValue::error(
                    "ERR value is not an integer or out of range",
                ))
            }
        };

        let hash = match self.cache.get_or_create_hash(&key) {
            Ok(h) => h,
            Err(Error::WrongType) => {
                return Ok(RespValue::error(Error::WrongType.to_resp_string()));
            }
            Err(e) => return Ok(RespValue::error(e.to_resp_string())),
        };
        let mut h = hash.write();
        let before = key.len() + h.memory_size();
        match h.hincrby(field, delta) {
            Ok(v) => {
                let after = key.len() + h.memory_size();
                drop(h);
                self.cache.account_hash_delta(before, after);
                Ok(RespValue::Integer(v))
            }
            Err(msg) => Ok(RespValue::error(format!("ERR {}", msg))),
        }
    }
}
