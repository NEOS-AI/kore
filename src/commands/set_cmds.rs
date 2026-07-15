use crate::cache::KeyType;
use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::set_type::RedisSet;
use bytes::Bytes;
use super::CommandHandler;

impl CommandHandler {
    fn ensure_set_key(&self, key: &Bytes) -> Result<Option<()>> {
        match self.cache.key_type(key) {
            KeyType::None => Ok(None),
            KeyType::Set => Ok(Some(())),
            _ => Err(Error::WrongType),
        }
    }

    /// SADD key member [member ...]
    pub(super) fn handle_sadd(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'sadd' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let members: Vec<Bytes> = args[1..]
            .iter()
            .filter_map(|a| a.as_bulk_string().cloned())
            .collect();
        if members.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'sadd' command",
            ));
        }
        let est: usize = members.iter().map(|m| m.len() + 16).sum();
        if let Err(e) = self.cache.ensure_non_string_capacity(est) {
            return Ok(RespValue::error(e.to_resp_string()));
        }
        let set = match self.cache.get_or_create_set(&key) {
            Ok(s) => s,
            Err(Error::WrongType) => {
                return Ok(RespValue::error(Error::WrongType.to_resp_string()));
            }
            Err(e) => return Ok(RespValue::error(e.to_resp_string())),
        };
        let mut s = set.write().unwrap();
        let before = key.len() + s.memory_size();
        let added = s.sadd(members) as i64;
        let after = key.len() + s.memory_size();
        drop(s);
        self.cache.account_set_delta(before, after);
        Ok(RespValue::Integer(added))
    }

    /// SREM key member [member ...]
    pub(super) fn handle_srem(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'srem' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_set_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let members: Vec<Bytes> = args[1..]
            .iter()
            .filter_map(|a| a.as_bulk_string().cloned())
            .collect();
        let set = match self.cache.get_set(key) {
            Some(s) => s,
            None => return Ok(RespValue::Integer(0)),
        };
        let mut s = set.write().unwrap();
        let before = key.len() + s.memory_size();
        let removed = s.srem(members) as i64;
        let empty = s.is_empty();
        let after = key.len() + s.memory_size();
        drop(s);
        self.cache.account_set_delta(before, after);
        if empty {
            self.cache.remove_set(key);
        }
        Ok(RespValue::Integer(removed))
    }

    /// SMEMBERS key
    pub(super) fn handle_smembers(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'smembers' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_set_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        match self.cache.get_set(key) {
            Some(s) => {
                let set = s.read().unwrap();
                Ok(RespValue::Array(
                    set.smembers()
                        .into_iter()
                        .map(|m| RespValue::BulkString(Some(m)))
                        .collect(),
                ))
            }
            None => Ok(RespValue::Array(vec![])),
        }
    }

    /// SISMEMBER key member
    pub(super) fn handle_sismember(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'sismember' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_set_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let member = match args[1].as_bulk_string() {
            Some(m) => m,
            None => return Ok(RespValue::error("ERR invalid member")),
        };
        let exists = self
            .cache
            .get_set(key)
            .map(|s| s.read().unwrap().sismember(member))
            .unwrap_or(false);
        Ok(RespValue::Integer(if exists { 1 } else { 0 }))
    }

    /// SCARD key
    pub(super) fn handle_scard(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'scard' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_set_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let n = self
            .cache
            .get_set(key)
            .map(|s| s.read().unwrap().scard())
            .unwrap_or(0);
        Ok(RespValue::Integer(n as i64))
    }

    /// SINTER key [key ...]
    pub(super) fn handle_sinter(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'sinter' command",
            ));
        }
        let keys: Vec<&Bytes> = args
            .iter()
            .filter_map(|a| a.as_bulk_string())
            .collect();
        if keys.is_empty() {
            return Ok(RespValue::Array(vec![]));
        }

        // Type-check all keys; missing keys act as empty sets.
        for k in &keys {
            match self.cache.key_type(k) {
                KeyType::None | KeyType::Set => {}
                _ => return Ok(RespValue::error(Error::WrongType.to_resp_string())),
            }
        }

        // If any key is missing, intersection is empty.
        let shared: Vec<_> = match keys
            .iter()
            .map(|k| self.cache.get_set(k))
            .collect::<Option<Vec<_>>>()
        {
            Some(v) => v,
            None => return Ok(RespValue::Array(vec![])),
        };

        let locks: Vec<_> = shared.iter().map(|s| s.read().unwrap()).collect();
        let set_refs: Vec<&RedisSet> = locks.iter().map(|g| &**g).collect();
        let members = RedisSet::sinter(&set_refs);
        Ok(RespValue::Array(
            members
                .into_iter()
                .map(|m| RespValue::BulkString(Some(m)))
                .collect(),
        ))
    }
}
