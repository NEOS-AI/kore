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
        let mut s = set.write();
        let before = crate::memory::estimate_keyed_object(key.len(), s.memory_size());
        let added = s.sadd(members) as i64;
        let after = crate::memory::estimate_keyed_object(key.len(), s.memory_size());
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
        let mut s = set.write();
        let before = crate::memory::estimate_keyed_object(key.len(), s.memory_size());
        let removed = s.srem(members) as i64;
        let empty = s.is_empty();
        let after = crate::memory::estimate_keyed_object(key.len(), s.memory_size());
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
                let set = s.read();
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
            .map(|s| s.read().sismember(member))
            .unwrap_or(false);
        Ok(RespValue::Integer(if exists { 1 } else { 0 }))
    }

    /// SMISMEMBER key member [member ...]
    pub(super) fn handle_smismember(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'smismember' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_set_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let members: Vec<&Bytes> = args[1..]
            .iter()
            .filter_map(|a| a.as_bulk_string())
            .collect();
        if members.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'smismember' command",
            ));
        }
        let flags: Vec<RespValue> = match self.cache.get_set(key) {
            Some(s) => {
                let set = s.read();
                members
                    .iter()
                    .map(|m| RespValue::Integer(if set.sismember(m) { 1 } else { 0 }))
                    .collect()
            }
            None => members.iter().map(|_| RespValue::Integer(0)).collect(),
        };
        Ok(RespValue::Array(flags))
    }

    /// SINTERCARD numkeys key [key ...] [LIMIT limit]
    pub(super) fn handle_sintercard(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'sintercard' command",
            ));
        }
        let numkeys = match self.parse_integer(&args[0]) {
            Ok(n) if n > 0 => n as usize,
            _ => {
                return Ok(RespValue::error(
                    "ERR at least 1 input key is needed for SINTERCARD",
                ))
            }
        };
        if args.len() < 1 + numkeys {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'sintercard' command",
            ));
        }

        let mut keys = Vec::with_capacity(numkeys);
        for a in &args[1..1 + numkeys] {
            match a.as_bulk_string() {
                Some(k) => keys.push(k.clone()),
                None => return Ok(RespValue::error("ERR invalid key")),
            }
        }

        // Optional LIMIT limit (0 = unlimited).
        let mut limit: Option<usize> = None;
        let mut i = 1 + numkeys;
        while i < args.len() {
            let opt = match args[i].as_bulk_string() {
                Some(s) => String::from_utf8_lossy(s).to_ascii_uppercase(),
                None => return Ok(RespValue::error("ERR syntax error")),
            };
            match opt.as_str() {
                "LIMIT" => {
                    if i + 1 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    let lim = match self.parse_integer(&args[i + 1]) {
                        Ok(n) if n >= 0 => n as usize,
                        _ => {
                            return Ok(RespValue::error("ERR LIMIT can't be negative"));
                        }
                    };
                    limit = if lim == 0 { None } else { Some(lim) };
                    i += 2;
                }
                _ => return Ok(RespValue::error("ERR syntax error")),
            }
        }

        for k in &keys {
            match self.cache.key_type(k) {
                KeyType::None | KeyType::Set => {}
                _ => return Ok(RespValue::error(Error::WrongType.to_resp_string())),
            }
        }

        // Any missing key → empty intersection.
        let shared: Vec<_> = match keys
            .iter()
            .map(|k| self.cache.get_set(k))
            .collect::<Option<Vec<_>>>()
        {
            Some(v) => v,
            None => return Ok(RespValue::Integer(0)),
        };
        let locks: Vec<_> = shared.iter().map(|s| s.read()).collect();
        let set_refs: Vec<&RedisSet> = locks.iter().map(|g| &**g).collect();
        Ok(RespValue::Integer(
            RedisSet::sinter_count(&set_refs, limit) as i64,
        ))
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
            .map(|s| s.read().scard())
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
        match self.compute_set_algebra(args, SetAlgebra::Inter) {
            Ok(members) => Ok(members_to_array(members)),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    /// SUNION key [key ...]
    pub(super) fn handle_sunion(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'sunion' command",
            ));
        }
        match self.compute_set_algebra(args, SetAlgebra::Union) {
            Ok(members) => Ok(members_to_array(members)),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    /// SDIFF key [key ...]
    pub(super) fn handle_sdiff(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'sdiff' command",
            ));
        }
        match self.compute_set_algebra(args, SetAlgebra::Diff) {
            Ok(members) => Ok(members_to_array(members)),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    /// SINTERSTORE destination key [key ...]
    pub(super) fn handle_sinterstore(&self, args: &[RespValue]) -> Result<RespValue> {
        self.handle_set_store(args, SetAlgebra::Inter, "sinterstore")
    }

    /// SUNIONSTORE destination key [key ...]
    pub(super) fn handle_sunionstore(&self, args: &[RespValue]) -> Result<RespValue> {
        self.handle_set_store(args, SetAlgebra::Union, "sunionstore")
    }

    /// SDIFFSTORE destination key [key ...]
    pub(super) fn handle_sdiffstore(&self, args: &[RespValue]) -> Result<RespValue> {
        self.handle_set_store(args, SetAlgebra::Diff, "sdiffstore")
    }

    /// SMOVE source destination member
    pub(super) fn handle_smove(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'smove' command",
            ));
        }
        let source = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid source key")),
        };
        let dest = match args[1].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid destination key")),
        };
        let member = match args[2].as_bulk_string() {
            Some(m) => m.clone(),
            None => return Ok(RespValue::error("ERR invalid member")),
        };

        match self.cache.key_type(source) {
            KeyType::None => return Ok(RespValue::Integer(0)),
            KeyType::Set => {}
            _ => return Ok(RespValue::error(Error::WrongType.to_resp_string())),
        }
        match self.cache.key_type(dest) {
            KeyType::None | KeyType::Set => {}
            _ => return Ok(RespValue::error(Error::WrongType.to_resp_string())),
        }

        // Same key: no-op move; return 1 if member present.
        if source == dest {
            let present = self
                .cache
                .get_set(source)
                .map(|s| s.read().sismember(&member))
                .unwrap_or(false);
            return Ok(RespValue::Integer(if present { 1 } else { 0 }));
        }

        let src_set = match self.cache.get_set(source) {
            Some(s) => s,
            None => return Ok(RespValue::Integer(0)),
        };

        // Bail early if member is absent (no dest mutation).
        if !src_set.read().sismember(&member) {
            return Ok(RespValue::Integer(0));
        }

        // Reserve room for dest insert before mutating source.
        let est = member.len() + 16;
        if let Err(e) = self.cache.ensure_non_string_capacity(est) {
            return Ok(RespValue::error(e.to_resp_string()));
        }

        // Remove from source.
        {
            let mut s = src_set.write();
            if !s.sismember(&member) {
                return Ok(RespValue::Integer(0));
            }
            let before = crate::memory::estimate_keyed_object(source.len(), s.memory_size());
            let _ = s.srem([member.clone()]);
            let empty = s.is_empty();
            let after = crate::memory::estimate_keyed_object(source.len(), s.memory_size());
            drop(s);
            self.cache.account_set_delta(before, after);
            if empty {
                self.cache.remove_set(source);
            }
        }

        // Insert into destination.
        let dest_set = match self.cache.get_or_create_set(dest) {
            Ok(s) => s,
            Err(Error::WrongType) => {
                return Ok(RespValue::error(Error::WrongType.to_resp_string()));
            }
            Err(e) => return Ok(RespValue::error(e.to_resp_string())),
        };
        let mut d = dest_set.write();
        let before = crate::memory::estimate_keyed_object(dest.len(), d.memory_size());
        let _ = d.sadd([member]);
        let after = crate::memory::estimate_keyed_object(dest.len(), d.memory_size());
        drop(d);
        self.cache.account_set_delta(before, after);
        Ok(RespValue::Integer(1))
    }

    /// SPOP key [count]
    pub(super) fn handle_spop(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() || args.len() > 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'spop' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_set_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

        let with_count = args.len() == 2;
        let count = if with_count {
            match self.parse_integer(&args[1]) {
                Ok(n) if n >= 0 => n as usize,
                Ok(_) => {
                    return Ok(RespValue::error(
                        "ERR value is out of range, must be positive",
                    ));
                }
                Err(e) => return Ok(RespValue::error(e.to_resp_string())),
            }
        } else {
            1
        };

        let set = match self.cache.get_set(key) {
            Some(s) => s,
            None => {
                return Ok(if with_count {
                    RespValue::Array(vec![])
                } else {
                    RespValue::BulkString(None)
                });
            }
        };

        let mut s = set.write();
        if s.is_empty() {
            return Ok(if with_count {
                RespValue::Array(vec![])
            } else {
                RespValue::BulkString(None)
            });
        }
        let before = crate::memory::estimate_keyed_object(key.len(), s.memory_size());
        let popped = s.spop(count);
        let empty = s.is_empty();
        let after = crate::memory::estimate_keyed_object(key.len(), s.memory_size());
        drop(s);
        self.cache.account_set_delta(before, after);
        if empty {
            self.cache.remove_set(key);
        }

        if with_count {
            Ok(members_to_array(popped))
        } else {
            Ok(RespValue::BulkString(popped.into_iter().next()))
        }
    }

    /// SRANDMEMBER key [count]
    pub(super) fn handle_srandmember(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() || args.len() > 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'srandmember' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_set_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

        let with_count = args.len() == 2;
        let set = match self.cache.get_set(key) {
            Some(s) => s,
            None => {
                return Ok(if with_count {
                    RespValue::Array(vec![])
                } else {
                    RespValue::BulkString(None)
                });
            }
        };
        let s = set.read();
        if !with_count {
            return Ok(RespValue::BulkString(s.srandmember_one()));
        }
        let count = match self.parse_integer(&args[1]) {
            Ok(n) => n,
            Err(e) => return Ok(RespValue::error(e.to_resp_string())),
        };
        Ok(members_to_array(s.srandmember(count)))
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    fn handle_set_store(
        &self,
        args: &[RespValue],
        op: SetAlgebra,
        name: &str,
    ) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(format!(
                "ERR wrong number of arguments for '{}' command",
                name
            )));
        }
        let dest = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid destination key")),
        };
        let members = match self.compute_set_algebra(&args[1..], op) {
            Ok(m) => m,
            Err(e) => return Ok(RespValue::error(e.to_resp_string())),
        };
        let card = members.len() as i64;

        // Overwrite destination regardless of prior type (Redis semantics).
        let _ = self.cache.delete(&dest);

        if members.is_empty() {
            return Ok(RespValue::Integer(0));
        }

        let est: usize = members.iter().map(|m| m.len() + 16).sum();
        if let Err(e) = self.cache.ensure_non_string_capacity(est) {
            return Ok(RespValue::error(e.to_resp_string()));
        }
        let set = match self.cache.get_or_create_set(&dest) {
            Ok(s) => s,
            Err(e) => return Ok(RespValue::error(e.to_resp_string())),
        };
        let mut s = set.write();
        let before = crate::memory::estimate_keyed_object(dest.len(), s.memory_size());
        let _ = s.sadd(members);
        let after = crate::memory::estimate_keyed_object(dest.len(), s.memory_size());
        drop(s);
        self.cache.account_set_delta(before, after);
        Ok(RespValue::Integer(card))
    }

    /// Compute set algebra over keys in `args`. Missing keys act as empty sets
    /// (except SINTER: any missing key → empty result).
    fn compute_set_algebra(
        &self,
        args: &[RespValue],
        op: SetAlgebra,
    ) -> std::result::Result<Vec<Bytes>, Error> {
        let keys: Vec<&Bytes> = args.iter().filter_map(|a| a.as_bulk_string()).collect();
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        for k in &keys {
            match self.cache.key_type(k) {
                KeyType::None | KeyType::Set => {}
                _ => return Err(Error::WrongType),
            }
        }

        match op {
            SetAlgebra::Inter => {
                // Any missing key → empty intersection.
                let shared: Vec<_> = match keys
                    .iter()
                    .map(|k| self.cache.get_set(k))
                    .collect::<Option<Vec<_>>>()
                {
                    Some(v) => v,
                    None => return Ok(Vec::new()),
                };
                let locks: Vec<_> = shared.iter().map(|s| s.read()).collect();
                let set_refs: Vec<&RedisSet> = locks.iter().map(|g| &**g).collect();
                Ok(RedisSet::sinter(&set_refs))
            }
            SetAlgebra::Union => {
                // Snapshot members under per-set read locks, then union.
                let mut out = std::collections::HashSet::new();
                for k in &keys {
                    if let Some(s) = self.cache.get_set(k) {
                        let g = s.read();
                        for m in g.iter_members() {
                            out.insert(m);
                        }
                    }
                }
                Ok(out.into_iter().collect())
            }
            SetAlgebra::Diff => {
                // First key missing → empty.
                let first = match self.cache.get_set(keys[0]) {
                    Some(s) => s,
                    None => return Ok(Vec::new()),
                };
                let first_lock = first.read();
                let others: Vec<_> = keys[1..]
                    .iter()
                    .filter_map(|k| self.cache.get_set(k))
                    .collect();
                let other_locks: Vec<_> = others.iter().map(|s| s.read()).collect();
                let mut set_refs: Vec<&RedisSet> = Vec::with_capacity(1 + other_locks.len());
                set_refs.push(&*first_lock);
                for g in &other_locks {
                    set_refs.push(&**g);
                }
                Ok(RedisSet::sdiff(&set_refs))
            }
        }
    }
}

#[derive(Clone, Copy)]
enum SetAlgebra {
    Inter,
    Union,
    Diff,
}

fn members_to_array(members: Vec<Bytes>) -> RespValue {
    RespValue::Array(
        members
            .into_iter()
            .map(|m| RespValue::BulkString(Some(m)))
            .collect(),
    )
}

impl CommandHandler {
    /// SSCAN key cursor [MATCH pattern] [COUNT count]
    pub(super) fn handle_sscan(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'sscan' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_set_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let cursor = match self.parse_integer(&args[1]) {
            Ok(c) if c >= 0 => c as u64,
            _ => return Ok(RespValue::error("ERR invalid cursor")),
        };
        let (pattern, count) = match self.parse_scan_options(&args[2..]) {
            Ok(v) => v,
            Err(e) => return Ok(e),
        };

        let mut members: Vec<Bytes> = match self.cache.get_set(key) {
            Some(s) => s
                .read()
                .iter_members()
                .filter(|m| super::admin::scan_name_matches(pattern.as_deref(), m))
                .collect(),
            None => Vec::new(),
        };
        members.sort();
        let (next, batch) = super::admin::cursor_page(&members, cursor, count);
        let elements = batch
            .into_iter()
            .map(|m| RespValue::BulkString(Some(m)))
            .collect();
        Ok(super::admin::scan_reply(next, elements))
    }
}
