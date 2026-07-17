use crate::cache::KeyType;
use crate::error::{Error, Result};
use crate::protocol::RespValue;
use crate::sorted_set::{LexBound, ScoreBound, ScoredMember};
use bytes::Bytes;
use std::collections::HashMap;
use super::CommandHandler;

/// How ZUNION/ZINTER (and *STORE) combine scores for the same member.
#[derive(Clone, Copy)]
enum ZAggregate {
    Sum,
    Min,
    Max,
}

impl ZAggregate {
    fn apply(self, acc: Option<f64>, weighted: f64) -> f64 {
        match (self, acc) {
            (_, None) => weighted,
            (ZAggregate::Sum, Some(a)) => a + weighted,
            (ZAggregate::Min, Some(a)) => a.min(weighted),
            (ZAggregate::Max, Some(a)) => a.max(weighted),
        }
    }
}

/// Sorted-set multi-key algebra.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ZAlgebra {
    Union,
    Inter,
    Diff,
}

impl CommandHandler {
    /// Return WRONGTYPE if key exists but is not a sorted set.
    fn ensure_zset_key(&self, key: &Bytes) -> Result<Option<()>> {
        match self.cache.key_type(key) {
            KeyType::None => Ok(None),
            KeyType::ZSet => Ok(Some(())),
            _ => Err(Error::WrongType),
        }
    }

    /// ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
    pub(super) fn handle_zadd(&self, args: &[RespValue]) -> Result<RespValue> {
        self.cache.stats.incr(&self.cache.stats.cmd_zadd);

        if args.len() < 3 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zadd'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        // Leading options (before the first score).
        let mut nx = false;
        let mut xx = false;
        let mut gt = false;
        let mut lt = false;
        let mut ch = false;
        let mut incr = false;
        let mut i = 1;
        while i < args.len() {
            let opt = match args[i].as_bulk_string() {
                Some(s) => String::from_utf8_lossy(s).to_ascii_uppercase(),
                None => break,
            };
            match opt.as_str() {
                "NX" => {
                    nx = true;
                    i += 1;
                }
                "XX" => {
                    xx = true;
                    i += 1;
                }
                "GT" => {
                    gt = true;
                    i += 1;
                }
                "LT" => {
                    lt = true;
                    i += 1;
                }
                "CH" => {
                    ch = true;
                    i += 1;
                }
                "INCR" => {
                    incr = true;
                    i += 1;
                }
                _ => break,
            }
        }

        if nx && xx {
            return Ok(RespValue::error(
                "ERR XX and NX options at the same time are not compatible",
            ));
        }
        if gt && lt {
            return Ok(RespValue::error(
                "ERR GT, LT, and/or NX options at the same time are not compatible",
            ));
        }
        // Redis: NX is incompatible with GT/LT.
        if nx && (gt || lt) {
            return Ok(RespValue::error(
                "ERR GT, LT, and/or NX options at the same time are not compatible",
            ));
        }

        // Parse score-member pairs
        let mut pairs = Vec::new();
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
        if i != args.len() || pairs.is_empty() {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zadd'"));
        }
        if incr && pairs.len() != 1 {
            return Ok(RespValue::error(
                "ERR INCR option supports a single increment-element pair",
            ));
        }

        // XX on a missing key: nothing to do (do not create empty zset).
        match self.ensure_zset_key(&key) {
            Ok(None) if xx => {
                return Ok(if incr {
                    RespValue::null()
                } else {
                    RespValue::Integer(0)
                });
            }
            Ok(_) => {}
            Err(Error::WrongType) => {
                return Ok(RespValue::error(Error::WrongType.to_resp_string()));
            }
            Err(e) => return Ok(RespValue::error(e.to_resp_string())),
        }

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
        let mut set = zset.write();
        let before = crate::memory::estimate_keyed_object(key.len(), set.memory_size());

        if incr {
            let (delta, member) = pairs.into_iter().next().unwrap();
            let old = set.score(&member);
            // NX: only if missing; XX already handled for missing key above.
            if nx && old.is_some() {
                drop(set);
                return Ok(RespValue::null());
            }
            if xx && old.is_none() {
                drop(set);
                return Ok(RespValue::null());
            }
            let base = old.unwrap_or(0.0);
            let new_score = base + delta;
            if new_score.is_nan() {
                return Ok(RespValue::error(
                    "ERR resulting score is not a number (NaN)",
                ));
            }
            // GT/LT apply when updating an existing member.
            if let Some(o) = old {
                if gt && !(new_score > o) {
                    drop(set);
                    return Ok(RespValue::null());
                }
                if lt && !(new_score < o) {
                    drop(set);
                    return Ok(RespValue::null());
                }
            }
            let _ = set.add(member, new_score);
            let after = crate::memory::estimate_keyed_object(key.len(), set.memory_size());
            drop(set);
            self.cache.account_sorted_set_delta(before, after);
            self.cache.list_blockers.notify_key(&key);
            return Ok(RespValue::BulkString(Some(Bytes::from(format_score(
                new_score,
            )))));
        }

        let mut added = 0i64;
        let mut changed = 0i64;
        for (score, member) in pairs {
            let old = set.score(&member);
            match old {
                Some(o) => {
                    if nx {
                        continue;
                    }
                    if gt && !(score > o) {
                        continue;
                    }
                    if lt && !(score < o) {
                        continue;
                    }
                    // Same score → no change (Redis CH does not count).
                    if scores_equal(o, score) {
                        continue;
                    }
                    let _ = set.add(member, score);
                    changed += 1;
                }
                None => {
                    if xx {
                        continue;
                    }
                    // GT/LT only constrain updates of existing elements.
                    let _ = set.add(member, score);
                    added += 1;
                    changed += 1;
                }
            }
        }

        let after = crate::memory::estimate_keyed_object(key.len(), set.memory_size());
        let empty = set.is_empty();
        drop(set);
        self.cache.account_sorted_set_delta(before, after);
        if empty {
            // All ops skipped after create — drop empty key (Redis does not leave it).
            let _ = self.cache.delete(&key);
        } else {
            self.cache.list_blockers.notify_key(&key);
        }

        Ok(RespValue::Integer(if ch { changed } else { added }))
    }

    /// ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
    pub(super) fn handle_zrange(&self, args: &[RespValue]) -> Result<RespValue> {
        self.cache.stats.incr(&self.cache.stats.cmd_zrange);

        if args.len() < 3 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'zrange'"));
        }

        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let opts = match parse_zrange_tail_options(&args[3..], true) {
            Ok(o) => o,
            Err(e) => return Ok(RespValue::error(e)),
        };

        match self.collect_zrange_members(key, &args[1], &args[2], &opts) {
            Ok(members) => Ok(scored_members_to_resp(members, opts.with_scores)),
            Err(e) => Ok(RespValue::error(e)),
        }
    }

    /// Shared ZRANGE / ZRANGESTORE member collection.
    fn collect_zrange_members(
        &self,
        key: &Bytes,
        min_arg: &RespValue,
        max_arg: &RespValue,
        opts: &ZRangeTailOpts,
    ) -> std::result::Result<Vec<ScoredMember>, String> {
        if let Err(Error::WrongType) = self.ensure_zset_key(key) {
            return Err(Error::WrongType.to_resp_string());
        }

        let zset = match self.cache.get_sorted_set(key) {
            Some(z) => z,
            None => return Ok(Vec::new()),
        };
        let set = zset.read();

        if opts.by_lex {
            let bound_a = parse_lex_bound(min_arg)?;
            let bound_b = parse_lex_bound(max_arg)?;
            // REV with BYLEX: client passes max min — normalize like ZREVRANGEBYLEX.
            let (min, max) = if opts.reverse {
                (bound_b, bound_a)
            } else {
                (bound_a, bound_b)
            };
            Ok(set.range_by_lex(
                &min,
                &max,
                opts.reverse,
                opts.offset,
                opts.count,
            ))
        } else if opts.by_score {
            let bound_a = parse_score_bound(min_arg)?;
            let bound_b = parse_score_bound(max_arg)?;
            let (min, max) = if opts.reverse {
                (bound_b, bound_a)
            } else {
                (bound_a, bound_b)
            };
            Ok(set.range_by_score(
                min,
                max,
                opts.reverse,
                opts.offset,
                opts.count,
            ))
        } else {
            // Rank range (legacy ZRANGE / ZREVRANGE form).
            let start = match self.parse_integer(min_arg) {
                Ok(s) => s as isize,
                Err(_) => {
                    return Err("ERR value is not an integer or out of range".into());
                }
            };
            let stop = match self.parse_integer(max_arg) {
                Ok(s) => s as isize,
                Err(_) => {
                    return Err("ERR value is not an integer or out of range".into());
                }
            };
            let mut range = set.range(start, stop, opts.reverse);
            if opts.offset > 0 || opts.count.is_some() {
                if opts.offset >= range.len() {
                    range.clear();
                } else {
                    let end = match opts.count {
                        Some(c) => (opts.offset + c).min(range.len()),
                        None => range.len(),
                    };
                    range = range[opts.offset..end].to_vec();
                }
            }
            Ok(range)
        }
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

        let set = zset.read();
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
                let set = zset.read();
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

        let set = zset.read();
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

        let mut set = zset.write();
        let before = crate::memory::estimate_keyed_object(key.len(), set.memory_size());
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

        let after = crate::memory::estimate_keyed_object(key.len(), set.memory_size());
        drop(set);
        self.cache.account_sorted_set_delta(before, after);

        Ok(RespValue::Integer(removed as i64))
    }

    pub(super) fn handle_zrank(&self, args: &[RespValue]) -> Result<RespValue> {
        self.cache.stats.incr(&self.cache.stats.cmd_zrank);
        self.zrank_impl(args, false)
    }

    pub(super) fn handle_zrevrank(&self, args: &[RespValue]) -> Result<RespValue> {
        self.cache.stats.incr(&self.cache.stats.cmd_zrevrank);
        self.zrank_impl(args, true)
    }

    /// ZRANK / ZREVRANK key member [WITHSCORE]
    /// WITHSCORE → array `[rank, score-bulk]` (Redis 7.2+); else integer rank / null.
    fn zrank_impl(&self, args: &[RespValue], reverse: bool) -> Result<RespValue> {
        let cmd = if reverse { "zrevrank" } else { "zrank" };
        if args.len() < 2 || args.len() > 3 {
            return Ok(RespValue::error(format!(
                "ERR wrong number of arguments for '{}' command",
                cmd
            )));
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

        let with_score = if args.len() == 3 {
            let opt = match args[2].as_bulk_string() {
                Some(s) => String::from_utf8_lossy(s).to_ascii_uppercase(),
                None => return Ok(RespValue::error("ERR syntax error")),
            };
            if opt != "WITHSCORE" {
                return Ok(RespValue::error("ERR syntax error"));
            }
            true
        } else {
            false
        };

        let zset = match self.cache.get_sorted_set(key) {
            Some(z) => z,
            None => return Ok(RespValue::null()),
        };

        let set = zset.read();
        let rank = if reverse {
            set.rev_rank(member)
        } else {
            set.rank(member)
        };
        match rank {
            Some(rank) => {
                if with_score {
                    let score = set.score(member).unwrap_or(0.0);
                    Ok(RespValue::Array(vec![
                        RespValue::Integer(rank as i64),
                        RespValue::BulkString(Some(Bytes::from(format_score(score)))),
                    ]))
                } else {
                    Ok(RespValue::Integer(rank as i64))
                }
            }
            None => Ok(RespValue::null()),
        }
    }

    /// ZINCRBY key increment member
    pub(super) fn handle_zincrby(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'zincrby' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let incr = match self.parse_float(&args[1]) {
            Ok(v) => v,
            Err(_) => return Ok(RespValue::error("ERR value is not a valid float")),
        };
        if incr.is_nan() {
            return Ok(RespValue::error("ERR resulting score is not a number (NaN)"));
        }
        let member = match args[2].as_bulk_string() {
            Some(m) => m.clone(),
            None => return Ok(RespValue::error("ERR invalid member")),
        };

        let est = member.len() + 64;
        if let Err(e) = self.cache.ensure_non_string_capacity(est) {
            return Ok(RespValue::error(e.to_resp_string()));
        }
        let zset = match self.cache.get_or_create_sorted_set(&key) {
            Ok(z) => z,
            Err(Error::WrongType) => {
                return Ok(RespValue::error(Error::WrongType.to_resp_string()));
            }
            Err(e) => return Ok(RespValue::error(e.to_resp_string())),
        };
        let mut set = zset.write();
        // Reject NaN result (Redis)
        let old = set.score(&member).unwrap_or(0.0);
        if (old + incr).is_nan() {
            return Ok(RespValue::error("ERR resulting score is not a number (NaN)"));
        }
        let before = crate::memory::estimate_keyed_object(key.len(), set.memory_size());
        let new_score = set.incr_by(member, incr);
        let after = crate::memory::estimate_keyed_object(key.len(), set.memory_size());
        drop(set);
        self.cache.account_sorted_set_delta(before, after);
        self.cache.list_blockers.notify_key(&key);
        Ok(RespValue::BulkString(Some(Bytes::from(format_score(new_score)))))
    }

    /// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
    pub(super) fn handle_zrangebyscore(&self, args: &[RespValue]) -> Result<RespValue> {
        self.handle_zrangebyscore_impl(args, false, "zrangebyscore")
    }

    /// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
    pub(super) fn handle_zrevrangebyscore(&self, args: &[RespValue]) -> Result<RespValue> {
        self.handle_zrangebyscore_impl(args, true, "zrevrangebyscore")
    }

    /// ZCOUNT key min max
    pub(super) fn handle_zcount(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'zcount' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_zset_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let min = match parse_score_bound(&args[1]) {
            Ok(b) => b,
            Err(e) => return Ok(RespValue::error(e)),
        };
        let max = match parse_score_bound(&args[2]) {
            Ok(b) => b,
            Err(e) => return Ok(RespValue::error(e)),
        };
        let n = match self.cache.get_sorted_set(key) {
            Some(z) => z.read().count_by_score(min, max),
            None => 0,
        };
        Ok(RespValue::Integer(n as i64))
    }

    /// ZREMRANGEBYRANK key start stop
    pub(super) fn handle_zremrangebyrank(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'zremrangebyrank' command",
            ));
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
            Err(_) => {
                return Ok(RespValue::error(
                    "ERR value is not an integer or out of range",
                ));
            }
        };
        let stop = match self.parse_integer(&args[2]) {
            Ok(s) => s as isize,
            Err(_) => {
                return Ok(RespValue::error(
                    "ERR value is not an integer or out of range",
                ));
            }
        };
        let zset = match self.cache.get_sorted_set(key) {
            Some(z) => z,
            None => return Ok(RespValue::Integer(0)),
        };
        let mut set = zset.write();
        let before = crate::memory::estimate_keyed_object(key.len(), set.memory_size());
        let removed = set.remove_range_by_rank(start, stop);
        let empty = set.is_empty();
        let after = crate::memory::estimate_keyed_object(key.len(), set.memory_size());
        drop(set);
        self.cache.account_sorted_set_delta(before, after);
        if empty {
            self.cache.remove_sorted_set(key);
        }
        Ok(RespValue::Integer(removed as i64))
    }

    /// ZREMRANGEBYSCORE key min max
    pub(super) fn handle_zremrangebyscore(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'zremrangebyscore' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_zset_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let min = match parse_score_bound(&args[1]) {
            Ok(b) => b,
            Err(e) => return Ok(RespValue::error(e)),
        };
        let max = match parse_score_bound(&args[2]) {
            Ok(b) => b,
            Err(e) => return Ok(RespValue::error(e)),
        };
        let zset = match self.cache.get_sorted_set(key) {
            Some(z) => z,
            None => return Ok(RespValue::Integer(0)),
        };
        let mut set = zset.write();
        let before = crate::memory::estimate_keyed_object(key.len(), set.memory_size());
        let removed = set.remove_range_by_score(min, max);
        let empty = set.is_empty();
        let after = crate::memory::estimate_keyed_object(key.len(), set.memory_size());
        drop(set);
        self.cache.account_sorted_set_delta(before, after);
        if empty {
            self.cache.remove_sorted_set(key);
        }
        Ok(RespValue::Integer(removed as i64))
    }

    /// ZMSCORE key member [member ...]
    pub(super) fn handle_zmscore(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'zmscore' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_zset_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

        let members: Vec<&Bytes> = args[1..]
            .iter()
            .filter_map(|a| a.as_bulk_string())
            .collect();
        if members.len() != args.len() - 1 {
            return Ok(RespValue::error("ERR invalid member"));
        }

        let scores: Vec<Option<f64>> = match self.cache.get_sorted_set(key) {
            Some(z) => {
                let set = z.read();
                members.iter().map(|m| set.score(m)).collect()
            }
            None => members.iter().map(|_| None).collect(),
        };

        let result: Vec<RespValue> = scores
            .into_iter()
            .map(|opt| match opt {
                Some(s) => RespValue::BulkString(Some(Bytes::from(format_score(s)))),
                None => RespValue::null(),
            })
            .collect();
        Ok(RespValue::Array(result))
    }

    /// ZRANDMEMBER key [count [WITHSCORES]]
    pub(super) fn handle_zrandmember(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() || args.len() > 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'zrandmember' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_zset_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

        let with_count = args.len() >= 2;
        let mut with_scores = false;
        if args.len() == 3 {
            let opt = match args[2].as_bulk_string() {
                Some(s) => String::from_utf8_lossy(s).to_ascii_uppercase(),
                None => return Ok(RespValue::error("ERR syntax error")),
            };
            if opt != "WITHSCORES" {
                return Ok(RespValue::error("ERR syntax error"));
            }
            with_scores = true;
        }

        let zset = match self.cache.get_sorted_set(key) {
            Some(z) => z,
            None => {
                return Ok(if with_count {
                    RespValue::Array(vec![])
                } else {
                    RespValue::null()
                });
            }
        };
        let set = zset.read();

        if !with_count {
            return Ok(match set.randmember_one() {
                Some(sm) => RespValue::BulkString(Some(sm.member)),
                None => RespValue::null(),
            });
        }

        let count = match self.parse_integer(&args[1]) {
            Ok(n) => n,
            Err(e) => return Ok(RespValue::error(e.to_resp_string())),
        };
        Ok(scored_members_to_resp(set.randmember(count), with_scores))
    }

    /// ZRANGEBYLEX key min max [LIMIT offset count]
    pub(super) fn handle_zrangebylex(&self, args: &[RespValue]) -> Result<RespValue> {
        self.handle_zrangebylex_impl(args, false, "zrangebylex")
    }

    /// ZREVRANGEBYLEX key max min [LIMIT offset count]
    pub(super) fn handle_zrevrangebylex(&self, args: &[RespValue]) -> Result<RespValue> {
        self.handle_zrangebylex_impl(args, true, "zrevrangebylex")
    }

    /// ZLEXCOUNT key min max
    pub(super) fn handle_zlexcount(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'zlexcount' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_zset_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let min = match parse_lex_bound(&args[1]) {
            Ok(b) => b,
            Err(e) => return Ok(RespValue::error(e)),
        };
        let max = match parse_lex_bound(&args[2]) {
            Ok(b) => b,
            Err(e) => return Ok(RespValue::error(e)),
        };
        let n = match self.cache.get_sorted_set(key) {
            Some(z) => z.read().count_by_lex(&min, &max),
            None => 0,
        };
        Ok(RespValue::Integer(n as i64))
    }

    /// ZREMRANGEBYLEX key min max
    pub(super) fn handle_zremrangebylex(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'zremrangebylex' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_zset_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let min = match parse_lex_bound(&args[1]) {
            Ok(b) => b,
            Err(e) => return Ok(RespValue::error(e)),
        };
        let max = match parse_lex_bound(&args[2]) {
            Ok(b) => b,
            Err(e) => return Ok(RespValue::error(e)),
        };
        let zset = match self.cache.get_sorted_set(key) {
            Some(z) => z,
            None => return Ok(RespValue::Integer(0)),
        };
        let mut set = zset.write();
        let before = crate::memory::estimate_keyed_object(key.len(), set.memory_size());
        let removed = set.remove_range_by_lex(&min, &max);
        let empty = set.is_empty();
        let after = crate::memory::estimate_keyed_object(key.len(), set.memory_size());
        drop(set);
        self.cache.account_sorted_set_delta(before, after);
        if empty {
            self.cache.remove_sorted_set(key);
        }
        Ok(RespValue::Integer(removed as i64))
    }

    fn handle_zrangebylex_impl(
        &self,
        args: &[RespValue],
        reverse: bool,
        name: &str,
    ) -> Result<RespValue> {
        if args.len() < 3 {
            return Ok(RespValue::error(format!(
                "ERR wrong number of arguments for '{}' command",
                name
            )));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_zset_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

        // ZRANGEBYLEX: min max; ZREVRANGEBYLEX: max min — normalize to min/max.
        let bound_a = match parse_lex_bound(&args[1]) {
            Ok(b) => b,
            Err(e) => return Ok(RespValue::error(e)),
        };
        let bound_b = match parse_lex_bound(&args[2]) {
            Ok(b) => b,
            Err(e) => return Ok(RespValue::error(e)),
        };
        let (min, max) = if reverse {
            (bound_b, bound_a)
        } else {
            (bound_a, bound_b)
        };

        let mut offset: usize = 0;
        let mut count: Option<usize> = None;
        let mut i = 3;
        while i < args.len() {
            let opt = match args[i].as_bulk_string() {
                Some(s) => String::from_utf8_lossy(s).to_uppercase(),
                None => return Ok(RespValue::error("ERR syntax error")),
            };
            match opt.as_str() {
                "LIMIT" => {
                    if i + 2 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    offset = match self.parse_integer(&args[i + 1]) {
                        Ok(n) if n >= 0 => n as usize,
                        _ => {
                            return Ok(RespValue::error(
                                "ERR value is not an integer or out of range",
                            ));
                        }
                    };
                    let c = match self.parse_integer(&args[i + 2]) {
                        Ok(n) => n,
                        _ => {
                            return Ok(RespValue::error(
                                "ERR value is not an integer or out of range",
                            ));
                        }
                    };
                    count = if c < 0 { None } else { Some(c as usize) };
                    i += 3;
                }
                _ => return Ok(RespValue::error("ERR syntax error")),
            }
        }

        let members = match self.cache.get_sorted_set(key) {
            Some(z) => z
                .read()
                .range_by_lex(&min, &max, reverse, offset, count),
            None => Vec::new(),
        };
        Ok(scored_members_to_resp(members, false))
    }

    fn handle_zrangebyscore_impl(
        &self,
        args: &[RespValue],
        reverse: bool,
        name: &str,
    ) -> Result<RespValue> {
        if args.len() < 3 {
            return Ok(RespValue::error(format!(
                "ERR wrong number of arguments for '{}' command",
                name
            )));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_zset_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }

        // ZRANGEBYSCORE: min max; ZREVRANGEBYSCORE: max min (still score bounds).
        let bound_a = match parse_score_bound(&args[1]) {
            Ok(b) => b,
            Err(e) => return Ok(RespValue::error(e)),
        };
        let bound_b = match parse_score_bound(&args[2]) {
            Ok(b) => b,
            Err(e) => return Ok(RespValue::error(e)),
        };
        let (min, max) = if reverse {
            // args are max min — normalize to min/max for filtering
            (bound_b, bound_a)
        } else {
            (bound_a, bound_b)
        };

        let mut with_scores = false;
        let mut offset: usize = 0;
        let mut count: Option<usize> = None;
        let mut i = 3;
        while i < args.len() {
            let opt = match args[i].as_bulk_string() {
                Some(s) => String::from_utf8_lossy(s).to_uppercase(),
                None => return Ok(RespValue::error("ERR syntax error")),
            };
            match opt.as_str() {
                "WITHSCORES" => {
                    with_scores = true;
                    i += 1;
                }
                "LIMIT" => {
                    if i + 2 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    offset = match self.parse_integer(&args[i + 1]) {
                        Ok(n) if n >= 0 => n as usize,
                        _ => {
                            return Ok(RespValue::error(
                                "ERR value is not an integer or out of range",
                            ));
                        }
                    };
                    let c = match self.parse_integer(&args[i + 2]) {
                        Ok(n) => n,
                        _ => {
                            return Ok(RespValue::error(
                                "ERR value is not an integer or out of range",
                            ));
                        }
                    };
                    // Redis: negative count means "all from offset"
                    count = if c < 0 { None } else { Some(c as usize) };
                    i += 3;
                }
                _ => return Ok(RespValue::error("ERR syntax error")),
            }
        }

        let members = match self.cache.get_sorted_set(key) {
            Some(z) => z
                .read()
                .range_by_score(min, max, reverse, offset, count),
            None => Vec::new(),
        };
        Ok(scored_members_to_resp(members, with_scores))
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

fn parse_score_bound(value: &RespValue) -> std::result::Result<ScoreBound, String> {
    match value.as_bulk_string() {
        Some(bytes) => {
            let s = String::from_utf8_lossy(bytes);
            ScoreBound::parse(&s).map_err(|_| "ERR min or max is not a float".to_string())
        }
        None => Err("ERR min or max is not a float".to_string()),
    }
}

fn parse_lex_bound(value: &RespValue) -> std::result::Result<LexBound, String> {
    match value.as_bulk_string() {
        Some(bytes) => LexBound::parse(bytes)
            .map_err(|_| "ERR min or max not valid string range item".to_string()),
        None => Err("ERR min or max not valid string range item".to_string()),
    }
}

fn format_score(score: f64) -> String {
    // Prefer integer-looking scores without trailing .0 when exact.
    if score.fract() == 0.0 && score.is_finite() && score.abs() < 1e15 {
        format!("{}", score as i64)
    } else {
        // Redis-style: enough precision, trim trailing zeros lightly via default Display.
        let s = format!("{}", score);
        s
    }
}

/// Score equality for ZADD CH (treat NaN == NaN like SortedSet::add).
fn scores_equal(a: f64, b: f64) -> bool {
    (a.is_nan() && b.is_nan()) || a == b
}

/// Trailing options shared by ZRANGE / ZRANGESTORE.
struct ZRangeTailOpts {
    by_score: bool,
    by_lex: bool,
    reverse: bool,
    offset: usize,
    count: Option<usize>,
    with_scores: bool,
}

/// Parse `[BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]` (WITHSCORES only when allowed).
fn parse_zrange_tail_options(
    args: &[RespValue],
    allow_withscores: bool,
) -> std::result::Result<ZRangeTailOpts, String> {
    let mut opts = ZRangeTailOpts {
        by_score: false,
        by_lex: false,
        reverse: false,
        offset: 0,
        count: None,
        with_scores: false,
    };
    let mut i = 0;
    while i < args.len() {
        let opt = match args[i].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_ascii_uppercase(),
            None => return Err("ERR syntax error".into()),
        };
        match opt.as_str() {
            "BYSCORE" => {
                if opts.by_lex {
                    return Err("ERR syntax error".into());
                }
                opts.by_score = true;
                i += 1;
            }
            "BYLEX" => {
                if opts.by_score {
                    return Err("ERR syntax error".into());
                }
                opts.by_lex = true;
                i += 1;
            }
            "REV" => {
                opts.reverse = true;
                i += 1;
            }
            "LIMIT" => {
                if i + 2 >= args.len() {
                    return Err("ERR syntax error".into());
                }
                let off = match args[i + 1].as_bulk_string() {
                    Some(b) => String::from_utf8_lossy(b)
                        .parse::<i64>()
                        .map_err(|_| "ERR value is not an integer or out of range".to_string())?,
                    None => match args[i + 1].as_integer() {
                        Some(n) => n,
                        None => {
                            return Err("ERR value is not an integer or out of range".into());
                        }
                    },
                };
                if off < 0 {
                    return Err("ERR value is not an integer or out of range".into());
                }
                let c = match args[i + 2].as_bulk_string() {
                    Some(b) => String::from_utf8_lossy(b)
                        .parse::<i64>()
                        .map_err(|_| "ERR value is not an integer or out of range".to_string())?,
                    None => match args[i + 2].as_integer() {
                        Some(n) => n,
                        None => {
                            return Err("ERR value is not an integer or out of range".into());
                        }
                    },
                };
                opts.offset = off as usize;
                opts.count = if c < 0 { None } else { Some(c as usize) };
                i += 3;
            }
            "WITHSCORES" if allow_withscores => {
                opts.with_scores = true;
                i += 1;
            }
            _ => return Err("ERR syntax error".into()),
        }
    }
    Ok(opts)
}

fn scored_members_to_resp(members: Vec<ScoredMember>, with_scores: bool) -> RespValue {
    let mut result = Vec::with_capacity(members.len() * if with_scores { 2 } else { 1 });
    for sm in members {
        result.push(RespValue::BulkString(Some(sm.member)));
        if with_scores {
            result.push(RespValue::BulkString(Some(Bytes::from(format_score(
                sm.score,
            )))));
        }
    }
    RespValue::Array(result)
}

impl CommandHandler {
    /// ZSCAN key cursor [MATCH pattern] [COUNT count]
    pub(super) fn handle_zscan(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'zscan' command",
            ));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_zset_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let cursor = match self.parse_integer(&args[1]) {
            Ok(c) if c >= 0 => c as u64,
            _ => return Ok(RespValue::error("ERR invalid cursor")),
        };
        let (pattern, count, _type) = match self.parse_scan_options(&args[2..]) {
            Ok(v) => v,
            Err(e) => return Ok(e),
        };

        let mut members: Vec<(Bytes, f64)> = match self.cache.get_sorted_set(key) {
            Some(z) => z
                .read()
                .iter_members()
                .filter(|(m, _)| super::admin::scan_name_matches(pattern.as_deref(), m))
                .collect(),
            None => Vec::new(),
        };
        members.sort_by(|a, b| a.0.cmp(&b.0));
        let (next, batch) = super::admin::cursor_page(&members, cursor, count);
        let mut elements = Vec::with_capacity(batch.len() * 2);
        for (m, score) in batch {
            elements.push(RespValue::BulkString(Some(m)));
            elements.push(RespValue::BulkString(Some(Bytes::from(format_score(score)))));
        }
        Ok(super::admin::scan_reply(next, elements))
    }

    /// ZUNION numkeys key [key ...] [WEIGHTS …] [AGGREGATE …] [WITHSCORES]
    pub(super) fn handle_zunion(&self, args: &[RespValue]) -> Result<RespValue> {
        self.handle_zset_algebra(args, ZAlgebra::Union, false, "zunion")
    }

    /// ZINTER numkeys key [key ...] [WEIGHTS …] [AGGREGATE …] [WITHSCORES]
    pub(super) fn handle_zinter(&self, args: &[RespValue]) -> Result<RespValue> {
        self.handle_zset_algebra(args, ZAlgebra::Inter, false, "zinter")
    }

    /// ZDIFF numkeys key [key ...] [WITHSCORES]
    pub(super) fn handle_zdiff(&self, args: &[RespValue]) -> Result<RespValue> {
        self.handle_zset_algebra(args, ZAlgebra::Diff, false, "zdiff")
    }

    /// ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS …] [AGGREGATE …]
    pub(super) fn handle_zunionstore(&self, args: &[RespValue]) -> Result<RespValue> {
        self.handle_zset_algebra(args, ZAlgebra::Union, true, "zunionstore")
    }

    /// ZINTERSTORE destination numkeys key [key ...] [WEIGHTS …] [AGGREGATE …]
    pub(super) fn handle_zinterstore(&self, args: &[RespValue]) -> Result<RespValue> {
        self.handle_zset_algebra(args, ZAlgebra::Inter, true, "zinterstore")
    }

    /// ZDIFFSTORE destination numkeys key [key ...]
    pub(super) fn handle_zdiffstore(&self, args: &[RespValue]) -> Result<RespValue> {
        self.handle_zset_algebra(args, ZAlgebra::Diff, true, "zdiffstore")
    }

    /// ZINTERCARD numkeys key [key ...] [LIMIT limit]
    pub(super) fn handle_zintercard(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'zintercard' command",
            ));
        }
        let numkeys = match self.parse_integer(&args[0]) {
            Ok(n) if n > 0 => n as usize,
            _ => {
                return Ok(RespValue::error(
                    "ERR at least 1 input key is needed for ZINTERCARD",
                ))
            }
        };
        if args.len() < 1 + numkeys {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'zintercard' command",
            ));
        }

        let mut keys = Vec::with_capacity(numkeys);
        for a in &args[1..1 + numkeys] {
            match a.as_bulk_string() {
                Some(k) => keys.push(k.clone()),
                None => return Ok(RespValue::error("ERR invalid key")),
            }
        }

        // Optional LIMIT limit (single integer; 0 = no limit).
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
                            return Ok(RespValue::error(
                                "ERR LIMIT can't be negative",
                            ))
                        }
                    };
                    // Redis: LIMIT 0 means unlimited.
                    limit = if lim == 0 { None } else { Some(lim) };
                    i += 2;
                }
                _ => return Ok(RespValue::error("ERR syntax error")),
            }
        }

        let snapshots = match self.snapshot_zsets(&keys, true) {
            Ok(Some(s)) => s,
            Ok(None) => return Ok(RespValue::Integer(0)),
            Err(e) => return Ok(RespValue::error(e)),
        };

        Ok(RespValue::Integer(count_zinter(&snapshots, limit) as i64))
    }

    /// Shared path for ZUNION/ZINTER/ZDIFF and *STORE variants.
    /// Store forms take `destination` as the first argument.
    fn handle_zset_algebra(
        &self,
        args: &[RespValue],
        op: ZAlgebra,
        store: bool,
        name: &str,
    ) -> Result<RespValue> {
        // store: dest + numkeys + ≥1 key; read: numkeys + ≥1 key
        let min_args = if store { 3 } else { 2 };
        if args.len() < min_args {
            return Ok(RespValue::error(format!(
                "ERR wrong number of arguments for '{}' command",
                name
            )));
        }

        let (dest, rest) = if store {
            let d = match args[0].as_bulk_string() {
                Some(k) => k.clone(),
                None => return Ok(RespValue::error("ERR invalid destination key")),
            };
            (Some(d), &args[1..])
        } else {
            (None, args)
        };

        let numkeys = match self.parse_integer(&rest[0]) {
            Ok(n) if n > 0 => n as usize,
            _ => {
                return Ok(RespValue::error(
                    "ERR at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE",
                ))
            }
        };
        if rest.len() < 1 + numkeys {
            return Ok(RespValue::error(format!(
                "ERR wrong number of arguments for '{}' command",
                name
            )));
        }

        let mut keys = Vec::with_capacity(numkeys);
        for a in &rest[1..1 + numkeys] {
            match a.as_bulk_string() {
                Some(k) => keys.push(k.clone()),
                None => return Ok(RespValue::error("ERR invalid key")),
            }
        }

        let allow_weights = op != ZAlgebra::Diff;
        let allow_withscores = !store;
        let (weights, aggregate, with_scores) = match parse_zset_op_options(
            &rest[1 + numkeys..],
            numkeys,
            allow_weights,
            allow_withscores,
        ) {
            Ok(v) => v,
            Err(e) => return Ok(RespValue::error(e)),
        };

        let snapshots = match self.snapshot_zsets(&keys, op == ZAlgebra::Inter) {
            Ok(Some(s)) => s,
            Ok(None) => {
                // Early empty (inter with missing/empty source).
                if store {
                    return self.store_zset_result(dest.as_ref().unwrap(), Vec::new());
                }
                return Ok(RespValue::Array(vec![]));
            }
            Err(e) => return Ok(RespValue::error(e)),
        };

        let mut result = match op {
            ZAlgebra::Union => compute_zunion(&snapshots, &weights, aggregate),
            ZAlgebra::Inter => compute_zinter(&snapshots, &weights, aggregate),
            ZAlgebra::Diff => compute_zdiff(&snapshots),
        };

        if result.iter().any(|(_, s)| s.is_nan()) {
            return Ok(RespValue::error(
                "ERR resulting score is not a number (NaN)",
            ));
        }

        if store {
            return self.store_zset_result(dest.as_ref().unwrap(), result);
        }

        // Read path: sort by score ascending, then member (Redis-compatible).
        result.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        let members: Vec<ScoredMember> = result
            .into_iter()
            .map(|(m, s)| ScoredMember::new(m, s))
            .collect();
        Ok(scored_members_to_resp(members, with_scores))
    }

    /// Snapshot source zsets. Returns `Ok(None)` for early-empty intersection.
    /// Missing keys are empty; wrong type → error string.
    fn snapshot_zsets(
        &self,
        keys: &[Bytes],
        inter: bool,
    ) -> std::result::Result<Option<Vec<HashMap<Bytes, f64>>>, String> {
        let mut snapshots: Vec<HashMap<Bytes, f64>> = Vec::with_capacity(keys.len());
        for k in keys {
            match self.cache.key_type(k) {
                KeyType::None => {
                    if inter {
                        return Ok(None);
                    }
                    snapshots.push(HashMap::new());
                }
                KeyType::ZSet => {
                    let map = match self.cache.get_sorted_set(k) {
                        Some(z) => z.read().iter_members().collect(),
                        None => HashMap::new(),
                    };
                    if inter && map.is_empty() {
                        return Ok(None);
                    }
                    snapshots.push(map);
                }
                _ => return Err(Error::WrongType.to_resp_string()),
            }
        }
        Ok(Some(snapshots))
    }

    /// ZRANGESTORE destination source min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]
    /// Stores a range of `source` into `destination` (overwrites any type). Returns cardinality.
    pub(super) fn handle_zrangestore(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 4 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'zrangestore' command",
            ));
        }
        let dest = match args[0].as_bulk_string() {
            Some(k) => k.clone(),
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        let source = match args[1].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };

        let opts = match parse_zrange_tail_options(&args[4..], false) {
            Ok(o) => o,
            Err(e) => return Ok(RespValue::error(e)),
        };

        let members = match self.collect_zrange_members(source, &args[2], &args[3], &opts) {
            Ok(m) => m,
            Err(e) => return Ok(RespValue::error(e)),
        };

        let pairs: Vec<(Bytes, f64)> = members
            .into_iter()
            .map(|sm| (sm.member, sm.score))
            .collect();
        self.store_zset_result(&dest, pairs)
    }

    /// Overwrite `dest` with a zset of `members` (score pairs). Empty → delete key, return 0.
    fn store_zset_result(
        &self,
        dest: &Bytes,
        members: Vec<(Bytes, f64)>,
    ) -> Result<RespValue> {
        let card = members.len() as i64;
        let _ = self.cache.delete(dest);
        if members.is_empty() {
            return Ok(RespValue::Integer(0));
        }

        let est: usize = members.iter().map(|(m, _)| m.len() + 64).sum();
        if let Err(e) = self.cache.ensure_non_string_capacity(est) {
            return Ok(RespValue::error(e.to_resp_string()));
        }
        let zset = match self.cache.get_or_create_sorted_set(dest) {
            Ok(z) => z,
            Err(e) => return Ok(RespValue::error(e.to_resp_string())),
        };
        let mut set = zset.write();
        let before = crate::memory::estimate_keyed_object(dest.len(), set.memory_size());
        for (member, score) in members {
            let _ = set.add(member, score);
        }
        let after = crate::memory::estimate_keyed_object(dest.len(), set.memory_size());
        drop(set);
        self.cache.account_sorted_set_delta(before, after);
        self.cache.list_blockers.notify_key(dest);
        Ok(RespValue::Integer(card))
    }

    /// ZPOPMIN key [count]
    pub(super) fn handle_zpopmin(&self, args: &[RespValue]) -> Result<RespValue> {
        self.handle_zpop(args, true, "zpopmin")
    }

    /// ZPOPMAX key [count]
    pub(super) fn handle_zpopmax(&self, args: &[RespValue]) -> Result<RespValue> {
        self.handle_zpop(args, false, "zpopmax")
    }

    fn handle_zpop(
        &self,
        args: &[RespValue],
        min: bool,
        name: &str,
    ) -> Result<RespValue> {
        if args.is_empty() || args.len() > 2 {
            return Ok(RespValue::error(format!(
                "ERR wrong number of arguments for '{}' command",
                name
            )));
        }
        let key = match args[0].as_bulk_string() {
            Some(k) => k,
            None => return Ok(RespValue::error("ERR invalid key")),
        };
        if let Err(Error::WrongType) = self.ensure_zset_key(key) {
            return Ok(RespValue::error(Error::WrongType.to_resp_string()));
        }
        let count = if args.len() == 2 {
            match self.parse_integer(&args[1]) {
                Ok(c) if c >= 0 => c as usize,
                Ok(_) => {
                    return Ok(RespValue::error(
                        "ERR value is out of range, must be positive",
                    ))
                }
                Err(_) => {
                    return Ok(RespValue::error(
                        "ERR value is not an integer or out of range",
                    ))
                }
            }
        } else {
            1
        };

        let zset = match self.cache.get_sorted_set(key) {
            Some(z) => z,
            None => return Ok(RespValue::Array(vec![])),
        };
        let mut set = zset.write();
        let before = crate::memory::estimate_keyed_object(key.len(), set.memory_size());
        let popped = if min {
            set.pop_min(count)
        } else {
            set.pop_max(count)
        };
        let empty = set.is_empty();
        let after = crate::memory::estimate_keyed_object(key.len(), set.memory_size());
        drop(set);
        self.cache.account_sorted_set_delta(before, after);
        if empty {
            self.cache.remove_sorted_set(key);
        }
        Ok(scored_members_to_resp(popped, true))
    }

    /// ZMPOP numkeys key [key ...] <MIN|MAX> [COUNT count]
    pub(super) fn handle_zmpop(&self, args: &[RespValue]) -> Result<RespValue> {
        match parse_zmpop_args(self, args, "zmpop") {
            Ok((keys, min, count)) => {
                for key in &keys {
                    if let Err(Error::WrongType) = self.ensure_zset_key(key) {
                        return Ok(RespValue::error(Error::WrongType.to_resp_string()));
                    }
                }
                match self.try_zmpop(&keys, min, count) {
                    Some((key, members)) => Ok(zmpop_reply(key, members)),
                    None => Ok(RespValue::null()),
                }
            }
            Err(e) => Ok(RespValue::error(e)),
        }
    }

    /// BZMPOP timeout numkeys key [key ...] <MIN|MAX> [COUNT count]
    pub(super) async fn handle_bzmpop(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 4 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'bzmpop' command",
            ));
        }
        let timeout_secs = match Self::parse_timeout_seconds(&args[0]) {
            Ok(t) => t,
            Err(e) => return Ok(RespValue::error(e)),
        };
        let (keys, min, count) = match parse_zmpop_args(self, &args[1..], "bzmpop") {
            Ok(v) => v,
            Err(e) => return Ok(RespValue::error(e)),
        };

        for key in &keys {
            if let Err(Error::WrongType) = self.ensure_zset_key(key) {
                return Ok(RespValue::error(Error::WrongType.to_resp_string()));
            }
        }

        if let Some((key, members)) = self.try_zmpop(&keys, min, count) {
            return Ok(zmpop_reply(key, members));
        }

        // Inside MULTI: never block (Redis).
        if self.executing_multi {
            return Ok(RespValue::null());
        }

        let block_forever = timeout_secs == 0.0;
        let deadline = if block_forever {
            None
        } else {
            Some(std::time::Instant::now() + std::time::Duration::from_secs_f64(timeout_secs))
        };

        let (waiter_id, notify) = self.cache.list_blockers.register(&keys);

        let result = loop {
            if let Some((key, members)) = self.try_zmpop(&keys, min, count) {
                break Ok(zmpop_reply(key, members));
            }

            if let Some(dl) = deadline {
                let now = std::time::Instant::now();
                if now >= dl {
                    break Ok(RespValue::null());
                }
                let remaining = dl - now;
                match tokio::time::timeout(remaining, notify.notified()).await {
                    Ok(()) => continue,
                    Err(_) => break Ok(RespValue::null()),
                }
            } else {
                notify.notified().await;
            }
        };

        self.cache.list_blockers.unregister(waiter_id, &keys);
        result
    }

    /// BZPOPMIN key [key ...] timeout
    pub(super) async fn handle_bzpopmin(&self, args: &[RespValue]) -> Result<RespValue> {
        self.handle_blocking_zpop(args, true).await
    }

    /// BZPOPMAX key [key ...] timeout
    pub(super) async fn handle_bzpopmax(&self, args: &[RespValue]) -> Result<RespValue> {
        self.handle_blocking_zpop(args, false).await
    }

    async fn handle_blocking_zpop(
        &self,
        args: &[RespValue],
        min: bool,
    ) -> Result<RespValue> {
        let cmd = if min { "bzpopmin" } else { "bzpopmax" };
        if args.len() < 2 {
            return Ok(RespValue::error(format!(
                "ERR wrong number of arguments for '{}' command",
                cmd
            )));
        }

        let timeout_arg = &args[args.len() - 1];
        let timeout_secs = match Self::parse_timeout_seconds(timeout_arg) {
            Ok(t) => t,
            Err(e) => return Ok(RespValue::error(e)),
        };

        let mut keys = Vec::with_capacity(args.len() - 1);
        for a in &args[..args.len() - 1] {
            match a.as_bulk_string() {
                Some(k) => keys.push(k.clone()),
                None => return Ok(RespValue::error("ERR invalid key")),
            }
        }

        for key in &keys {
            if let Err(Error::WrongType) = self.ensure_zset_key(key) {
                return Ok(RespValue::error(Error::WrongType.to_resp_string()));
            }
        }

        if let Some((key, members)) = self.try_zmpop(&keys, min, 1) {
            if let Some(sm) = members.into_iter().next() {
                return Ok(bzpop_reply(key, sm.member, sm.score));
            }
        }

        if self.executing_multi {
            return Ok(RespValue::null_array());
        }

        let block_forever = timeout_secs == 0.0;
        let deadline = if block_forever {
            None
        } else {
            Some(std::time::Instant::now() + std::time::Duration::from_secs_f64(timeout_secs))
        };

        let (waiter_id, notify) = self.cache.list_blockers.register(&keys);

        let result = loop {
            if let Some((key, members)) = self.try_zmpop(&keys, min, 1) {
                if let Some(sm) = members.into_iter().next() {
                    break Ok(bzpop_reply(key, sm.member, sm.score));
                }
            }

            if let Some(dl) = deadline {
                let now = std::time::Instant::now();
                if now >= dl {
                    break Ok(RespValue::null_array());
                }
                let remaining = dl - now;
                match tokio::time::timeout(remaining, notify.notified()).await {
                    Ok(()) => continue,
                    Err(_) => break Ok(RespValue::null_array()),
                }
            } else {
                notify.notified().await;
            }
        };

        self.cache.list_blockers.unregister(waiter_id, &keys);
        result
    }

    /// Pop up to `count` members from the first non-empty zset among `keys` (left-to-right).
    fn try_zmpop(
        &self,
        keys: &[Bytes],
        min: bool,
        count: usize,
    ) -> Option<(Bytes, Vec<ScoredMember>)> {
        if count == 0 {
            return None;
        }
        for key in keys {
            if !matches!(self.cache.key_type(key), KeyType::ZSet) {
                continue;
            }
            let Some(zset) = self.cache.get_sorted_set(key) else {
                continue;
            };
            let mut set = zset.write();
            if set.is_empty() {
                continue;
            }
            let before = crate::memory::estimate_keyed_object(key.len(), set.memory_size());
            let popped = if min {
                set.pop_min(count)
            } else {
                set.pop_max(count)
            };
            let empty = set.is_empty();
            let after = crate::memory::estimate_keyed_object(key.len(), set.memory_size());
            drop(set);
            self.cache.account_sorted_set_delta(before, after);
            if empty {
                self.cache.remove_sorted_set(key);
            }
            if !popped.is_empty() {
                return Some((key.clone(), popped));
            }
        }
        None
    }
}

fn bzpop_reply(key: Bytes, member: Bytes, score: f64) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(Some(key)),
        RespValue::BulkString(Some(member)),
        RespValue::BulkString(Some(Bytes::from(format_score(score)))),
    ])
}

/// ZMPOP / BZMPOP reply: `[key, [[member, score], ...]]`.
fn zmpop_reply(key: Bytes, members: Vec<ScoredMember>) -> RespValue {
    let pairs: Vec<RespValue> = members
        .into_iter()
        .map(|sm| {
            RespValue::Array(vec![
                RespValue::BulkString(Some(sm.member)),
                RespValue::BulkString(Some(Bytes::from(format_score(sm.score)))),
            ])
        })
        .collect();
    RespValue::Array(vec![
        RespValue::BulkString(Some(key)),
        RespValue::Array(pairs),
    ])
}

/// Parse `numkeys key [key ...] <MIN|MAX> [COUNT count]` for ZMPOP/BZMPOP.
fn parse_zmpop_args(
    handler: &CommandHandler,
    args: &[RespValue],
    name: &str,
) -> std::result::Result<(Vec<Bytes>, bool, usize), String> {
    if args.len() < 3 {
        return Err(format!(
            "ERR wrong number of arguments for '{}' command",
            name
        ));
    }
    let numkeys = match handler.parse_integer(&args[0]) {
        Ok(n) if n > 0 => n as usize,
        _ => {
            return Err(
                "ERR numkeys should be greater than 0".into(),
            )
        }
    };
    if args.len() < 1 + numkeys + 1 {
        return Err(format!(
            "ERR wrong number of arguments for '{}' command",
            name
        ));
    }

    let mut keys = Vec::with_capacity(numkeys);
    for a in &args[1..1 + numkeys] {
        match a.as_bulk_string() {
            Some(k) => keys.push(k.clone()),
            None => return Err("ERR invalid key".into()),
        }
    }

    let where_token = match args[1 + numkeys].as_bulk_string() {
        Some(s) => String::from_utf8_lossy(s).to_ascii_uppercase(),
        None => return Err("ERR syntax error".into()),
    };
    let min = match where_token.as_str() {
        "MIN" => true,
        "MAX" => false,
        _ => return Err("ERR syntax error".into()),
    };

    let mut count: usize = 1;
    let mut i = 2 + numkeys;
    while i < args.len() {
        let opt = match args[i].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_ascii_uppercase(),
            None => return Err("ERR syntax error".into()),
        };
        match opt.as_str() {
            "COUNT" => {
                if i + 1 >= args.len() {
                    return Err("ERR syntax error".into());
                }
                count = match handler.parse_integer(&args[i + 1]) {
                    Ok(n) if n > 0 => n as usize,
                    Ok(_) => {
                        return Err("ERR count should be greater than 0".into())
                    }
                    Err(_) => {
                        return Err(
                            "ERR value is not an integer or out of range".into(),
                        )
                    }
                };
                i += 2;
            }
            _ => return Err("ERR syntax error".into()),
        }
    }

    Ok((keys, min, count))
}

/// Parse trailing WEIGHTS / AGGREGATE / WITHSCORES options for zset multi-key ops.
fn parse_zset_op_options(
    rest: &[RespValue],
    numkeys: usize,
    allow_weights_agg: bool,
    allow_withscores: bool,
) -> std::result::Result<(Vec<f64>, ZAggregate, bool), String> {
    let mut weights = vec![1.0_f64; numkeys];
    let mut aggregate = ZAggregate::Sum;
    let mut with_scores = false;
    let mut i = 0;
    while i < rest.len() {
        let token = match rest[i].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).to_ascii_uppercase(),
            None => return Err("ERR syntax error".into()),
        };
        match token.as_str() {
            "WEIGHTS" => {
                if !allow_weights_agg {
                    return Err("ERR syntax error".into());
                }
                i += 1;
                if rest.len() < i + numkeys {
                    return Err("ERR syntax error".into());
                }
                for w in 0..numkeys {
                    let s = match rest[i + w].as_bulk_string() {
                        Some(b) => String::from_utf8_lossy(b),
                        None => return Err("ERR weight value is not a float".into()),
                    };
                    let v: f64 = s
                        .parse()
                        .map_err(|_| "ERR weight value is not a float".to_string())?;
                    if v.is_nan() {
                        return Err("ERR weight value is not a float".into());
                    }
                    weights[w] = v;
                }
                i += numkeys;
            }
            "AGGREGATE" => {
                if !allow_weights_agg {
                    return Err("ERR syntax error".into());
                }
                i += 1;
                if i >= rest.len() {
                    return Err("ERR syntax error".into());
                }
                let agg = match rest[i].as_bulk_string() {
                    Some(b) => String::from_utf8_lossy(b).to_ascii_uppercase(),
                    None => return Err("ERR syntax error".into()),
                };
                aggregate = match agg.as_str() {
                    "SUM" => ZAggregate::Sum,
                    "MIN" => ZAggregate::Min,
                    "MAX" => ZAggregate::Max,
                    _ => {
                        return Err(
                            "ERR AGGREGATE supports SUM, MIN, or MAX only".into(),
                        )
                    }
                };
                i += 1;
            }
            "WITHSCORES" => {
                if !allow_withscores {
                    return Err("ERR syntax error".into());
                }
                with_scores = true;
                i += 1;
            }
            _ => return Err("ERR syntax error".into()),
        }
    }
    Ok((weights, aggregate, with_scores))
}

/// Members in the first set that are absent from every subsequent set.
/// Scores are taken from the first set only.
fn compute_zdiff(snapshots: &[HashMap<Bytes, f64>]) -> Vec<(Bytes, f64)> {
    if snapshots.is_empty() {
        return Vec::new();
    }
    let first = &snapshots[0];
    let mut out = Vec::new();
    'member: for (member, score) in first {
        for other in &snapshots[1..] {
            if other.contains_key(member) {
                continue 'member;
            }
        }
        out.push((member.clone(), *score));
    }
    out
}

fn compute_zunion(
    snapshots: &[HashMap<Bytes, f64>],
    weights: &[f64],
    aggregate: ZAggregate,
) -> Vec<(Bytes, f64)> {
    let mut acc: HashMap<Bytes, f64> = HashMap::new();
    for (idx, snap) in snapshots.iter().enumerate() {
        let w = weights[idx];
        for (member, score) in snap {
            let weighted = score * w;
            let entry = acc.entry(member.clone());
            match entry {
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    let new_score = aggregate.apply(Some(*e.get()), weighted);
                    e.insert(new_score);
                }
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert(aggregate.apply(None, weighted));
                }
            }
        }
    }
    acc.into_iter().collect()
}

fn compute_zinter(
    snapshots: &[HashMap<Bytes, f64>],
    weights: &[f64],
    aggregate: ZAggregate,
) -> Vec<(Bytes, f64)> {
    if snapshots.is_empty() {
        return Vec::new();
    }
    // Start from the smallest set for fewer candidates.
    let mut order: Vec<usize> = (0..snapshots.len()).collect();
    order.sort_by_key(|&i| snapshots[i].len());
    let first = order[0];
    let mut out = Vec::new();
    'member: for (member, first_score) in &snapshots[first] {
        let mut score = aggregate.apply(None, first_score * weights[first]);
        for &idx in &order[1..] {
            match snapshots[idx].get(member) {
                Some(s) => {
                    score = aggregate.apply(Some(score), s * weights[idx]);
                }
                None => continue 'member,
            }
        }
        out.push((member.clone(), score));
    }
    out
}

/// Count intersection cardinality; stop early when `limit` is reached.
fn count_zinter(snapshots: &[HashMap<Bytes, f64>], limit: Option<usize>) -> usize {
    if snapshots.is_empty() {
        return 0;
    }
    let mut order: Vec<usize> = (0..snapshots.len()).collect();
    order.sort_by_key(|&i| snapshots[i].len());
    let first = order[0];
    let mut n = 0usize;
    'member: for member in snapshots[first].keys() {
        for &idx in &order[1..] {
            if !snapshots[idx].contains_key(member) {
                continue 'member;
            }
        }
        n += 1;
        if let Some(lim) = limit {
            if n >= lim {
                return n;
            }
        }
    }
    n
}
