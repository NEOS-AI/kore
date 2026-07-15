use crate::cache::KeyType;
use crate::error::{Error, Result};
use crate::protocol::RespValue;
use bytes::Bytes;
use super::CommandHandler;

impl CommandHandler {
    /// Return WRONGTYPE if key exists but is not a sorted set.
    fn ensure_zset_key(&self, key: &Bytes) -> Result<Option<()>> {
        match self.cache.key_type(key) {
            KeyType::None => Ok(None),
            KeyType::ZSet => Ok(Some(())),
            _ => Err(Error::WrongType),
        }
    }

    pub(super) fn handle_zadd(&self, args: &[RespValue]) -> Result<RespValue> {
        self.cache.stats.incr(&self.cache.stats.cmd_zadd);
        
        if args.len() < 3 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zadd'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        // Parse score-member pairs
        let mut pairs = Vec::new();
        let mut i = 1;
        
        while i + 1 < args.len() {
            let score = match self.parse_float(&args[i]) {
                Ok(s) => s,
                Err(_) => return Ok(RespValue::error("ERR value is not a valid float")),
            };

            let member = match args[i + 1].as_bulk_string() {
                Some(m) => m.clone(),
                None => return Ok(RespValue::error("ERR invalid member")),
            };

            pairs.push((score, member));
            i += 2;
        }

        if pairs.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zadd'"));
        }

        // Rough pre-check for member growth against maxmemory
        let est_growth: usize = pairs.iter().map(|(_, m)| m.len() + 64).sum();
        if let Err(e) = self.cache.ensure_non_string_capacity(est_growth) {
            return Ok(RespValue::error(e.to_resp_string()));
        }

        let zset = match self.cache.get_or_create_sorted_set(&key) {
            Ok(z) => z,
            Err(Error::WrongType) => {
                return Ok(RespValue::error(Error::WrongType.to_resp_string()));
            }
            Err(e) => return Ok(RespValue::error(e.to_resp_string())),
        };
        let mut set = zset.write().unwrap();
        let before = key.len() + set.memory_size();

        let mut added = 0;
        for (score, member) in pairs {
            if set.add(member, score) {
                added += 1;
            }
        }

        let after = key.len() + set.memory_size();
        drop(set);
        self.cache.account_sorted_set_delta(before, after);

        Ok(RespValue::Integer(added as i64))
    }

    pub(super) fn handle_zrange(&self, args: &[RespValue]) -> Result<RespValue> {
        self.cache.stats.incr(&self.cache.stats.cmd_zrange);
        
        if args.len() < 3 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zrange'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        if let Err(Error::WrongType) = self.ensure_zset_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

        let start = match self.parse_integer(&args[1]) {
            Ok(s) => s as isize,
            Err(_) => return Ok(RespValue::error("ERR value is not an integer or out of range")),
        };

        let stop = match self.parse_integer(&args[2]) {
            Ok(s) => s as isize,
            Err(_) => return Ok(RespValue::error("ERR value is not an integer or out of range")),
        };

        // Check for WITHSCORES option
        let with_scores = if args.len() > 3 {
            match args[3].as_bulk_string() {
                Some(opt) => {
                    let opt_str = String::from_utf8_lossy(opt).to_uppercase();
                    if opt_str == "WITHSCORES" {
                        true
                    } else {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                }
                None => return Ok(RespValue::error("ERR syntax error")),
            }
        } else {
            false
        };

        let zset = match self.cache.get_sorted_set(key) {
            Some(z) => z,
            None => return Ok(RespValue::Array(vec![])),
        };

        let set = zset.read().unwrap();
        let members = set.range(start, stop, false);

        let mut result = Vec::new();
        for scored_member in members {
            result.push(RespValue::BulkString(Some(scored_member.member)));
            if with_scores {
                result.push(RespValue::BulkString(Some(Bytes::from(scored_member.score.to_string()))));
            }
        }

        Ok(RespValue::Array(result))
    }

    pub(super) fn handle_zrevrange(&self, args: &[RespValue]) -> Result<RespValue> {
        self.cache.stats.incr(&self.cache.stats.cmd_zrevrange);
        
        if args.len() < 3 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zrevrange'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        if let Err(Error::WrongType) = self.ensure_zset_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

        let start = match self.parse_integer(&args[1]) {
            Ok(s) => s as isize,
            Err(_) => return Ok(RespValue::error("ERR value is not an integer or out of range")),
        };

        let stop = match self.parse_integer(&args[2]) {
            Ok(s) => s as isize,
            Err(_) => return Ok(RespValue::error("ERR value is not an integer or out of range")),
        };

        // Check for WITHSCORES option
        let with_scores = if args.len() > 3 {
            match args[3].as_bulk_string() {
                Some(opt) => {
                    let opt_str = String::from_utf8_lossy(opt).to_uppercase();
                    if opt_str == "WITHSCORES" {
                        true
                    } else {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                }
                None => return Ok(RespValue::error("ERR syntax error")),
            }
        } else {
            false
        };

        let zset = match self.cache.get_sorted_set(key) {
            Some(z) => z,
            None => return Ok(RespValue::Array(vec![])),
        };

        let set = zset.read().unwrap();
        let members = set.range(start, stop, true);

        let mut result = Vec::new();
        for scored_member in members {
            result.push(RespValue::BulkString(Some(scored_member.member)));
            if with_scores {
                result.push(RespValue::BulkString(Some(Bytes::from(scored_member.score.to_string()))));
            }
        }

        Ok(RespValue::Array(result))
    }

    pub(super) fn handle_zcard(&self, args: &[RespValue]) -> Result<RespValue> {
        self.cache.stats.incr(&self.cache.stats.cmd_zcard);
        
        if args.len() != 1 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zcard'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        if let Err(Error::WrongType) = self.ensure_zset_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

        let count = match self.cache.get_sorted_set(key) {
            Some(zset) => {
                let set = zset.read().unwrap();
                set.len()
            }
            None => 0,
        };

        Ok(RespValue::Integer(count as i64))
    }

    pub(super) fn handle_zscore(&self, args: &[RespValue]) -> Result<RespValue> {
        self.cache.stats.incr(&self.cache.stats.cmd_zscore);
        
        if args.len() != 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zscore'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        if let Err(Error::WrongType) = self.ensure_zset_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

        let member = match args[1].as_bulk_string() {
            Some(m) => m,
            None => return Ok(RespValue::error("ERR invalid member")),
        };

        let zset = match self.cache.get_sorted_set(key) {
            Some(z) => z,
            None => return Ok(RespValue::null()),
        };

        let set = zset.read().unwrap();
        match set.score(member) {
            Some(score) => Ok(RespValue::BulkString(Some(Bytes::from(score.to_string())))),
            None => Ok(RespValue::null()),
        }
    }

    pub(super) fn handle_zrem(&self, args: &[RespValue]) -> Result<RespValue> {
        self.cache.stats.incr(&self.cache.stats.cmd_zrem);
        
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zrem'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        if let Err(Error::WrongType) = self.ensure_zset_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

        let zset = match self.cache.get_sorted_set(key) {
            Some(z) => z,
            None => return Ok(RespValue::Integer(0)),
        };

        let mut set = zset.write().unwrap();
        let before = key.len() + set.memory_size();
        let mut removed = 0;

        for i in 1..args.len() {
            let member = match args[i].as_bulk_string() {
                Some(m) => m,
                None => continue,
            };

            if set.remove(member) {
                removed += 1;
            }
        }

        let after = key.len() + set.memory_size();
        drop(set);
        self.cache.account_sorted_set_delta(before, after);

        Ok(RespValue::Integer(removed as i64))
    }

    pub(super) fn handle_zrank(&self, args: &[RespValue]) -> Result<RespValue> {
        self.cache.stats.incr(&self.cache.stats.cmd_zrank);
        
        if args.len() != 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zrank'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        if let Err(Error::WrongType) = self.ensure_zset_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

        let member = match args[1].as_bulk_string() {
            Some(m) => m,
            None => return Ok(RespValue::error("ERR invalid member")),
        };

        let zset = match self.cache.get_sorted_set(key) {
            Some(z) => z,
            None => return Ok(RespValue::null()),
        };

        let set = zset.read().unwrap();
        match set.rank(member) {
            Some(rank) => Ok(RespValue::Integer(rank as i64)),
            None => Ok(RespValue::null()),
        }
    }

    pub(super) fn handle_zrevrank(&self, args: &[RespValue]) -> Result<RespValue> {
        self.cache.stats.incr(&self.cache.stats.cmd_zrevrank);
        
        if args.len() != 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zrevrank'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        if let Err(Error::WrongType) = self.ensure_zset_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

        let member = match args[1].as_bulk_string() {
            Some(m) => m,
            None => return Ok(RespValue::error("ERR invalid member")),
        };

        let zset = match self.cache.get_sorted_set(key) {
            Some(z) => z,
            None => return Ok(RespValue::null()),
        };

        let set = zset.read().unwrap();
        match set.rev_rank(member) {
            Some(rank) => Ok(RespValue::Integer(rank as i64)),
            None => Ok(RespValue::null()),
        }
    }

    // Helper method to parse float from RespValue
    pub(super) fn parse_float(&self, value: &RespValue) -> Result<f64> {
        match value.as_bulk_string() {
            Some(bytes) => {
                let s = String::from_utf8_lossy(bytes);
                s.parse::<f64>()
                    .map_err(|_| Error::InvalidArgument("not a valid float".into()))
            }
            None => Err(Error::InvalidArgument("not a bulk string".into())),
        }
    }
}
