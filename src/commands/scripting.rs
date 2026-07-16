//! EVAL / EVALSHA / SCRIPT — Redis Lua scripting (mlua Lua 5.4).

use super::CommandHandler;
use crate::error::Result;
use crate::protocol::RespValue;
use crate::scripting::script_sha1;
use bytes::Bytes;
use mlua::{Lua, Value as LuaValue};
use std::sync::atomic::{AtomicBool, Ordering};

/// Commands that may be invoked via `redis.call` / `redis.pcall` inside scripts.
/// Blocking / multi / admin / nested-script commands are excluded.
const SCRIPT_CALL_ALLOWLIST: &[&str] = &[
    "PING", "ECHO", //
    "GET", "SET", "DEL", "EXISTS", "TYPE", "MGET", "MSET", "MSETNX", "APPEND", "STRLEN", "LCS",
    "GETRANGE", "SUBSTR", "SETRANGE", "SETEX", "PSETEX", "DUMP", "RESTORE",
    "GETSET", "UNLINK", "RENAME", "RENAMENX", "SETNX", "GETDEL", "GETEX", //
    "INCR", "DECR", "INCRBY", "DECRBY", "INCRBYFLOAT", //
    "TIME", //
    "EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT", "PERSIST", "TTL", "PTTL", "EXPIRETIME", "PEXPIRETIME", //
    "HSET", "HSETNX", "HMSET", "HGET", "HMGET", "HDEL", "HGETDEL", "HGETALL", "HLEN", "HEXISTS", "HKEYS", "HVALS", //
    "HINCRBY", "HINCRBYFLOAT", "HSTRLEN", "HRANDFIELD", "HSCAN", //
    "OBJECT", "MEMORY", //
    "LPUSH", "RPUSH", "LPUSHX", "RPUSHX", "LPOP", "RPOP", "LRANGE", "LLEN", "LINDEX", "LSET", "LREM", "LTRIM", "LINSERT", //
    "LPOS", "LMOVE", "RPOPLPUSH", "LMPOP", "SORT", //
    "LOLWUT", //
    "SADD", "SREM", "SMEMBERS", "SISMEMBER", "SMISMEMBER", "SCARD", "SINTER", "SINTERCARD", "SUNION", "SDIFF", //
    "SINTERSTORE", "SUNIONSTORE", "SDIFFSTORE", "SMOVE", "SPOP", "SRANDMEMBER", "SSCAN", //
    "ZADD", "ZRANGE", "ZRANGESTORE", "ZREVRANGE", "ZCARD", "ZSCORE", "ZMSCORE", "ZREM", "ZRANK", "ZREVRANK", //
    "ZINCRBY", "ZRANGEBYSCORE", "ZREVRANGEBYSCORE", "ZCOUNT", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE", "ZSCAN", //
    "ZRANGEBYLEX", "ZREVRANGEBYLEX", "ZLEXCOUNT", "ZREMRANGEBYLEX", "ZRANDMEMBER", //
    "ZUNION", "ZINTER", "ZDIFF", "ZINTERCARD", "ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE", //
    "ZPOPMIN", "ZPOPMAX", "ZMPOP", //
    "SETBIT", "GETBIT", "BITCOUNT", "BITPOS", "BITOP", "BITFIELD", "BITFIELD_RO", //
    "PFADD", "PFCOUNT", "PFMERGE", //
    "DBSIZE", "KEYS", "SWAPDB", //
    "GEORADIUS_RO", "GEORADIUSBYMEMBER_RO", //
];

impl CommandHandler {
    /// EVAL script numkeys [key ...] [arg ...]
    pub(super) fn handle_eval(&mut self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error("ERR wrong number of arguments for 'eval'"));
        }
        let script = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).into_owned(),
            None => return Ok(RespValue::error("ERR invalid script")),
        };
        // Auto-cache so EVALSHA works after EVAL (Redis does this).
        let _ = self.script_cache.load(&script);
        self.eval_script_body(&script, &args[1..])
    }

    /// EVALSHA sha1 numkeys [key ...] [arg ...]
    pub(super) fn handle_evalsha(&mut self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'evalsha'",
            ));
        }
        let sha = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).into_owned(),
            None => return Ok(RespValue::error("ERR invalid sha1")),
        };
        let script = match self.script_cache.get(&sha) {
            Some(s) => s,
            None => {
                return Ok(RespValue::error(format!(
                    "NOSCRIPT No matching script. Please use EVAL. ({})",
                    sha.to_ascii_lowercase()
                )));
            }
        };
        self.eval_script_body(&script, &args[1..])
    }

    /// SCRIPT LOAD | EXISTS | FLUSH | KILL
    pub(super) fn handle_script(&mut self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'script'",
            ));
        }
        let sub = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_uppercase(),
            None => return Ok(RespValue::error("ERR invalid SCRIPT subcommand")),
        };
        match sub.as_str() {
            "LOAD" => {
                if args.len() != 2 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'script|load'",
                    ));
                }
                let body = match args[1].as_bulk_string() {
                    Some(s) => String::from_utf8_lossy(s).into_owned(),
                    None => return Ok(RespValue::error("ERR invalid script")),
                };
                // Validate syntax by compiling once.
                if let Err(e) = compile_check(&body) {
                    return Ok(RespValue::error(format!("ERR Error compiling script: {}", e)));
                }
                let sha = self.script_cache.load(&body);
                Ok(RespValue::BulkString(Some(Bytes::from(sha))))
            }
            "EXISTS" => {
                if args.len() < 2 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'script|exists'",
                    ));
                }
                let mut shas = Vec::with_capacity(args.len() - 1);
                for a in &args[1..] {
                    match a.as_bulk_string() {
                        Some(s) => shas.push(String::from_utf8_lossy(s).into_owned()),
                        None => return Ok(RespValue::error("ERR invalid sha1")),
                    }
                }
                let flags = self.script_cache.exists(&shas);
                Ok(RespValue::Array(
                    flags.into_iter().map(RespValue::Integer).collect(),
                ))
            }
            "FLUSH" => {
                // Optional ASYNC|SYNC ignored (always sync for MVP).
                if args.len() > 2 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'script|flush'",
                    ));
                }
                if args.len() == 2 {
                    let mode = match args[1].as_bulk_string() {
                        Some(s) => String::from_utf8_lossy(s).to_uppercase(),
                        None => return Ok(RespValue::error("ERR syntax error")),
                    };
                    if mode != "ASYNC" && mode != "SYNC" {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                }
                self.script_cache.flush();
                Ok(RespValue::ok())
            }
            "KILL" => {
                // No long-running script tracking yet.
                Ok(RespValue::error(
                    "NOTBUSY No scripts in execution right now.",
                ))
            }
            _ => Ok(RespValue::error(format!(
                "ERR unknown subcommand or wrong number of arguments for '{}'. \
                 Try SCRIPT HELP.",
                sub.to_ascii_lowercase()
            ))),
        }
    }

    /// Parse numkeys + KEYS/ARGV and run `source` under mlua with redis.call.
    fn eval_script_body(&mut self, source: &str, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'eval'",
            ));
        }
        let numkeys = match self.parse_integer(&args[0]) {
            Ok(n) if n >= 0 => n as usize,
            Ok(_) => {
                return Ok(RespValue::error(
                    "ERR Number of keys can't be negative",
                ));
            }
            Err(_) => {
                return Ok(RespValue::error(
                    "ERR value is not an integer or out of range",
                ));
            }
        };
        let rest = &args[1..];
        if rest.len() < numkeys {
            return Ok(RespValue::error(
                "ERR Number of keys can't be greater than number of args",
            ));
        }
        let mut keys = Vec::with_capacity(numkeys);
        for k in &rest[..numkeys] {
            match k.as_bulk_string() {
                Some(b) => keys.push(b.clone()),
                None => return Ok(RespValue::error("ERR invalid key")),
            }
        }
        let mut argv = Vec::with_capacity(rest.len() - numkeys);
        for a in &rest[numkeys..] {
            match a.as_bulk_string() {
                Some(b) => argv.push(b.clone()),
                None => {
                    // Allow integers as ARGV (some clients send them that way).
                    if let Some(i) = a.as_integer() {
                        argv.push(Bytes::from(i.to_string()));
                    } else {
                        return Ok(RespValue::error("ERR invalid argument"));
                    }
                }
            }
        }

        match self.run_lua(source, &keys, &argv) {
            Ok(v) => Ok(v),
            Err(e) => Ok(RespValue::error(e)),
        }
    }

    fn run_lua(
        &mut self,
        source: &str,
        keys: &[Bytes],
        argv: &[Bytes],
    ) -> std::result::Result<RespValue, String> {
        let lua = Lua::new();

        // KEYS / ARGV as 1-based arrays (Redis convention).
        let keys_tbl = lua.create_table().map_err(|e| e.to_string())?;
        for (i, k) in keys.iter().enumerate() {
            keys_tbl
                .set(i + 1, bytes_to_lua_string(&lua, k)?)
                .map_err(|e| e.to_string())?;
        }
        let argv_tbl = lua.create_table().map_err(|e| e.to_string())?;
        for (i, a) in argv.iter().enumerate() {
            argv_tbl
                .set(i + 1, bytes_to_lua_string(&lua, a)?)
                .map_err(|e| e.to_string())?;
        }
        lua.globals()
            .set("KEYS", keys_tbl)
            .map_err(|e| e.to_string())?;
        lua.globals()
            .set("ARGV", argv_tbl)
            .map_err(|e| e.to_string())?;

        // redis.call / redis.pcall via raw pointer (Lua is single-threaded here).
        // Safety: handler is not moved; nested EVAL is denied in dispatch.
        let handler_ptr = self as *mut CommandHandler;
        let in_call = AtomicBool::new(false);

        let call_fn = {
            let in_call = &in_call as *const AtomicBool;
            lua.create_function(move |lua_ctx, args: mlua::Variadic<LuaValue>| {
                let in_call = unsafe { &*in_call };
                if in_call.swap(true, Ordering::SeqCst) {
                    return Err(mlua::Error::runtime(
                        "ERR redis.call re-entry is not allowed",
                    ));
                }
                let result = (|| {
                    let h = unsafe { &mut *handler_ptr };
                    dispatch_redis_call(lua_ctx, h, args, false)
                })();
                in_call.store(false, Ordering::SeqCst);
                result
            })
            .map_err(|e| e.to_string())?
        };

        let pcall_fn = {
            let in_call = &in_call as *const AtomicBool;
            lua.create_function(move |lua_ctx, args: mlua::Variadic<LuaValue>| {
                let in_call = unsafe { &*in_call };
                if in_call.swap(true, Ordering::SeqCst) {
                    return Err(mlua::Error::runtime(
                        "ERR redis.call re-entry is not allowed",
                    ));
                }
                let result = (|| {
                    let h = unsafe { &mut *handler_ptr };
                    dispatch_redis_call(lua_ctx, h, args, true)
                })();
                in_call.store(false, Ordering::SeqCst);
                result
            })
            .map_err(|e| e.to_string())?
        };

        let redis_tbl = lua.create_table().map_err(|e| e.to_string())?;
        redis_tbl
            .set("call", call_fn)
            .map_err(|e| e.to_string())?;
        redis_tbl
            .set("pcall", pcall_fn)
            .map_err(|e| e.to_string())?;
        // redis.status_reply / redis.error_reply helpers (optional Redis API).
        let status_fn = lua
            .create_function(|lua_ctx, msg: String| {
                let t = lua_ctx.create_table()?;
                t.set("ok", msg)?;
                Ok(t)
            })
            .map_err(|e| e.to_string())?;
        let error_fn = lua
            .create_function(|lua_ctx, msg: String| {
                let t = lua_ctx.create_table()?;
                t.set("err", msg)?;
                Ok(t)
            })
            .map_err(|e| e.to_string())?;
        redis_tbl
            .set("status_reply", status_fn)
            .map_err(|e| e.to_string())?;
        redis_tbl
            .set("error_reply", error_fn)
            .map_err(|e| e.to_string())?;
        lua.globals()
            .set("redis", redis_tbl)
            .map_err(|e| e.to_string())?;

        let chunk = lua
            .load(source)
            .set_name("user_script")
            .into_function()
            .map_err(|e| format!("Error compiling script: {}", e))?;

        let ret: LuaValue = chunk.call(()).map_err(|e| {
            // Strip mlua noise; keep Redis-style prefix.
            let msg = e.to_string();
            if msg.contains("NOSCRIPT") || msg.starts_with("ERR ") || msg.starts_with("WRONGTYPE") {
                msg
            } else {
                format!("ERR user_script: {}", msg)
            }
        })?;

        lua_value_to_resp(ret)
    }

    /// Synchronous command dispatch used by redis.call (no ACL re-check, no AOF per-op).
    pub(super) fn script_dispatch_sync(
        &mut self,
        cmd_upper: &str,
        args: &[RespValue],
    ) -> Result<RespValue> {
        match cmd_upper {
            "PING" => self.handle_ping(args),
            "ECHO" => self.handle_echo(args),
            "SET" => self.handle_set(args),
            "GET" => self.handle_get(args),
            "DEL" => self.handle_del(args),
            "EXISTS" => self.handle_exists(args),
            "TYPE" => self.handle_type(args),
            "MGET" => self.handle_mget(args),
            "MSET" => self.handle_mset(args),
            "MSETNX" => self.handle_msetnx(args),
            "APPEND" => self.handle_append(args),
            "STRLEN" => self.handle_strlen(args),
            "GETRANGE" => self.handle_getrange(args),
            "SUBSTR" => self.handle_substr(args),
            "SETRANGE" => self.handle_setrange(args),
            "SETEX" => self.handle_setex(args),
            "PSETEX" => self.handle_psetex(args),
            "GETSET" => self.handle_getset(args),
            "UNLINK" => self.handle_unlink(args),
            "RENAME" => self.handle_rename(args),
            "RENAMENX" => self.handle_renamenx(args),
            "SETNX" => self.handle_setnx(args),
            "GETDEL" => self.handle_getdel(args),
            "GETEX" => self.handle_getex(args),
            "LCS" => self.handle_lcs(args),
            "OBJECT" => self.handle_object(args),
            "MEMORY" => self.handle_memory(args),
            "TIME" => self.handle_time(args),
            "INCR" => self.handle_incr(args),
            "DECR" => self.handle_decr(args),
            "INCRBY" => self.handle_incrby(args),
            "DECRBY" => self.handle_decrby(args),
            "INCRBYFLOAT" => self.handle_incrbyfloat(args),
            "EXPIRE" => self.handle_expire(args),
            "PEXPIRE" => self.handle_pexpire(args),
            "EXPIREAT" => self.handle_expireat(args),
            "PEXPIREAT" => self.handle_pexpireat(args),
            "PERSIST" => self.handle_persist(args),
            "TTL" => self.handle_ttl(args),
            "PTTL" => self.handle_pttl(args),
            "EXPIRETIME" => self.handle_expiretime(args),
            "PEXPIRETIME" => self.handle_pexpiretime(args),
            "HSET" => self.handle_hset(args),
            "HSETNX" => self.handle_hsetnx(args),
            "HMSET" => self.handle_hmset(args),
            "HGET" => self.handle_hget(args),
            "HMGET" => self.handle_hmget(args),
            "HDEL" => self.handle_hdel(args),
            "HGETDEL" => self.handle_hgetdel(args),
            "HGETALL" => self.handle_hgetall(args),
            "HLEN" => self.handle_hlen(args),
            "HEXISTS" => self.handle_hexists(args),
            "HKEYS" => self.handle_hkeys(args),
            "HVALS" => self.handle_hvals(args),
            "HINCRBY" => self.handle_hincrby(args),
            "HINCRBYFLOAT" => self.handle_hincrbyfloat(args),
            "HSTRLEN" => self.handle_hstrlen(args),
            "HRANDFIELD" => self.handle_hrandfield(args),
            "HSCAN" => self.handle_hscan(args),
            "LPUSH" => self.handle_lpush(args),
            "RPUSH" => self.handle_rpush(args),
            "LPUSHX" => self.handle_lpushx(args),
            "RPUSHX" => self.handle_rpushx(args),
            "LPOP" => self.handle_lpop(args),
            "RPOP" => self.handle_rpop(args),
            "LRANGE" => self.handle_lrange(args),
            "LLEN" => self.handle_llen(args),
            "LINDEX" => self.handle_lindex(args),
            "LSET" => self.handle_lset(args),
            "LREM" => self.handle_lrem(args),
            "LTRIM" => self.handle_ltrim(args),
            "LINSERT" => self.handle_linsert(args),
            "LPOS" => self.handle_lpos(args),
            "LMOVE" => self.handle_lmove(args),
            "RPOPLPUSH" => self.handle_rpoplpush(args),
            "LMPOP" => self.handle_lmpop(args),
            "SORT" => self.handle_sort(args),
            "LOLWUT" => self.handle_lolwut(args),
            "SADD" => self.handle_sadd(args),
            "SREM" => self.handle_srem(args),
            "SMEMBERS" => self.handle_smembers(args),
            "SISMEMBER" => self.handle_sismember(args),
            "SMISMEMBER" => self.handle_smismember(args),
            "SCARD" => self.handle_scard(args),
            "SINTER" => self.handle_sinter(args),
            "SINTERCARD" => self.handle_sintercard(args),
            "SUNION" => self.handle_sunion(args),
            "SDIFF" => self.handle_sdiff(args),
            "SINTERSTORE" => self.handle_sinterstore(args),
            "SUNIONSTORE" => self.handle_sunionstore(args),
            "SDIFFSTORE" => self.handle_sdiffstore(args),
            "SMOVE" => self.handle_smove(args),
            "SPOP" => self.handle_spop(args),
            "SRANDMEMBER" => self.handle_srandmember(args),
            "SSCAN" => self.handle_sscan(args),
            "ZADD" => self.handle_zadd(args),
            "ZRANGE" => self.handle_zrange(args),
            "ZRANGESTORE" => self.handle_zrangestore(args),
            "ZREVRANGE" => self.handle_zrevrange(args),
            "ZCARD" => self.handle_zcard(args),
            "ZSCORE" => self.handle_zscore(args),
            "ZMSCORE" => self.handle_zmscore(args),
            "ZREM" => self.handle_zrem(args),
            "ZRANK" => self.handle_zrank(args),
            "ZREVRANK" => self.handle_zrevrank(args),
            "ZINCRBY" => self.handle_zincrby(args),
            "ZRANGEBYSCORE" => self.handle_zrangebyscore(args),
            "ZREVRANGEBYSCORE" => self.handle_zrevrangebyscore(args),
            "ZCOUNT" => self.handle_zcount(args),
            "ZREMRANGEBYRANK" => self.handle_zremrangebyrank(args),
            "ZREMRANGEBYSCORE" => self.handle_zremrangebyscore(args),
            "ZRANGEBYLEX" => self.handle_zrangebylex(args),
            "ZREVRANGEBYLEX" => self.handle_zrevrangebylex(args),
            "ZLEXCOUNT" => self.handle_zlexcount(args),
            "ZREMRANGEBYLEX" => self.handle_zremrangebylex(args),
            "ZRANDMEMBER" => self.handle_zrandmember(args),
            "ZSCAN" => self.handle_zscan(args),
            "ZUNION" => self.handle_zunion(args),
            "ZINTER" => self.handle_zinter(args),
            "ZDIFF" => self.handle_zdiff(args),
            "ZINTERCARD" => self.handle_zintercard(args),
            "ZUNIONSTORE" => self.handle_zunionstore(args),
            "ZINTERSTORE" => self.handle_zinterstore(args),
            "ZDIFFSTORE" => self.handle_zdiffstore(args),
            "ZPOPMIN" => self.handle_zpopmin(args),
            "ZPOPMAX" => self.handle_zpopmax(args),
            "ZMPOP" => self.handle_zmpop(args),
            "SETBIT" => self.handle_setbit(args),
            "GETBIT" => self.handle_getbit(args),
            "BITCOUNT" => self.handle_bitcount(args),
            "BITPOS" => self.handle_bitpos(args),
            "BITOP" => self.handle_bitop(args),
            "BITFIELD" => self.handle_bitfield(args),
            "BITFIELD_RO" => self.handle_bitfield_ro(args),
            "GEORADIUS_RO" => self.handle_georadius_ro(args),
            "GEORADIUSBYMEMBER_RO" => self.handle_georadiusbymember_ro(args),
            "SWAPDB" => self.handle_swapdb(args),
            "DUMP" => self.handle_dump(args),
            "RESTORE" => self.handle_restore(args),
            "PFADD" => self.handle_pfadd(args),
            "PFCOUNT" => self.handle_pfcount(args),
            "PFMERGE" => self.handle_pfmerge(args),
            "DBSIZE" => self.handle_dbsize(args),
            "KEYS" => self.handle_keys(args),
            _ => Ok(RespValue::error(format!(
                "ERR This Redis command is not allowed from scripts: {}",
                cmd_upper
            ))),
        }
    }
}

fn compile_check(source: &str) -> std::result::Result<(), String> {
    let lua = Lua::new();
    lua.load(source)
        .set_name("user_script")
        .into_function()
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn bytes_to_lua_string(_lua: &Lua, b: &Bytes) -> std::result::Result<String, String> {
    // Prefer UTF-8; fall back to lossy for binary keys/values.
    Ok(String::from_utf8_lossy(b).into_owned())
}

fn dispatch_redis_call(
    lua: &Lua,
    handler: &mut CommandHandler,
    args: mlua::Variadic<LuaValue>,
    is_pcall: bool,
) -> mlua::Result<LuaValue> {
    if args.is_empty() {
        let err = "ERR wrong number of arguments for 'redis.call'";
        if is_pcall {
            return pcall_err(lua, err);
        }
        return Err(mlua::Error::runtime(err));
    }

    let cmd = match &args[0] {
        LuaValue::String(s) => s.to_str().map_err(mlua::Error::external)?.to_ascii_uppercase(),
        other => {
            let err = format!("ERR Lua redis.call command must be a string, got {:?}", other);
            if is_pcall {
                return pcall_err(lua, &err);
            }
            return Err(mlua::Error::runtime(err));
        }
    };

    if !SCRIPT_CALL_ALLOWLIST.iter().any(|c| *c == cmd.as_str()) {
        let err = format!("ERR This Redis command is not allowed from scripts: {}", cmd);
        if is_pcall {
            return pcall_err(lua, &err);
        }
        return Err(mlua::Error::runtime(err));
    }

    let mut resp_args = Vec::with_capacity(args.len() - 1);
    for v in args.iter().skip(1) {
        match lua_arg_to_bulk(v) {
            Ok(b) => resp_args.push(RespValue::BulkString(Some(b))),
            Err(e) => {
                if is_pcall {
                    return pcall_err(lua, &e);
                }
                return Err(mlua::Error::runtime(e));
            }
        }
    }

    let result = match handler.script_dispatch_sync(&cmd, &resp_args) {
        Ok(r) => r,
        Err(e) => {
            let msg = e.to_string();
            if is_pcall {
                return pcall_err(lua, &msg);
            }
            return Err(mlua::Error::runtime(msg));
        }
    };

    match &result {
        RespValue::Error(e) => {
            let msg = String::from_utf8_lossy(e).into_owned();
            if is_pcall {
                pcall_err(lua, &msg)
            } else {
                Err(mlua::Error::runtime(msg))
            }
        }
        other => {
            let lv = resp_to_lua(lua, other)?;
            if is_pcall {
                // redis.pcall success: return value directly (Redis returns multi values
                // with true first only for Lua pcall, not redis.pcall — redis.pcall
                // returns the reply or {err=...}).
                Ok(lv)
            } else {
                Ok(lv)
            }
        }
    }
}

fn pcall_err(lua: &Lua, msg: &str) -> mlua::Result<LuaValue> {
    let t = lua.create_table()?;
    t.set("err", msg)?;
    Ok(LuaValue::Table(t))
}

fn lua_arg_to_bulk(v: &LuaValue) -> std::result::Result<Bytes, String> {
    match v {
        LuaValue::String(s) => {
            let bytes = s.as_bytes();
            Ok(Bytes::copy_from_slice(bytes.as_ref()))
        }
        LuaValue::Integer(i) => Ok(Bytes::from(i.to_string())),
        LuaValue::Number(n) => {
            if n.fract() == 0.0 && *n >= i64::MIN as f64 && *n <= i64::MAX as f64 {
                Ok(Bytes::from((*n as i64).to_string()))
            } else {
                Ok(Bytes::from(n.to_string()))
            }
        }
        LuaValue::Boolean(true) => Ok(Bytes::from_static(b"1")),
        LuaValue::Boolean(false) => Ok(Bytes::from_static(b"0")),
        LuaValue::Nil => Err("ERR Lua redis.call argument cannot be nil".into()),
        _ => Err("ERR Lua redis.call argument must be a string or number".into()),
    }
}

/// Convert a Redis reply into a Lua value (redis.call return mapping).
fn resp_to_lua(lua: &Lua, v: &RespValue) -> mlua::Result<LuaValue> {
    match v {
        RespValue::SimpleString(s) => {
            // Status replies become {ok = "..."} tables (Redis Lua convention).
            let t = lua.create_table()?;
            let s = String::from_utf8_lossy(s).into_owned();
            t.set("ok", s)?;
            Ok(LuaValue::Table(t))
        }
        RespValue::Error(e) => {
            // Should be handled by caller; still map if needed.
            let t = lua.create_table()?;
            t.set("err", String::from_utf8_lossy(e).into_owned())?;
            Ok(LuaValue::Table(t))
        }
        RespValue::Integer(i) => Ok(LuaValue::Integer(*i)),
        RespValue::BulkString(None) | RespValue::Null | RespValue::NullArray => {
            // Redis: nil bulk → false in Lua
            Ok(LuaValue::Boolean(false))
        }
        RespValue::BulkString(Some(b)) => {
            let s = lua.create_string(b.as_ref())?;
            Ok(LuaValue::String(s))
        }
        RespValue::Array(arr) => {
            let t = lua.create_table()?;
            for (i, item) in arr.iter().enumerate() {
                t.set(i + 1, resp_to_lua(lua, item)?)?;
            }
            Ok(LuaValue::Table(t))
        }
        RespValue::Bool(b) => Ok(LuaValue::Boolean(*b)),
        RespValue::Map(pairs) => {
            let t = lua.create_table()?;
            let mut i = 1;
            for (k, val) in pairs {
                t.set(i, resp_to_lua(lua, k)?)?;
                t.set(i + 1, resp_to_lua(lua, val)?)?;
                i += 2;
            }
            Ok(LuaValue::Table(t))
        }
        RespValue::Push(arr) => {
            let t = lua.create_table()?;
            for (i, item) in arr.iter().enumerate() {
                t.set(i + 1, resp_to_lua(lua, item)?)?;
            }
            Ok(LuaValue::Table(t))
        }
        RespValue::Multiple(vals) => {
            // Flatten to array of replies.
            let t = lua.create_table()?;
            for (i, item) in vals.iter().enumerate() {
                t.set(i + 1, resp_to_lua(lua, item)?)?;
            }
            Ok(LuaValue::Table(t))
        }
    }
}

/// Convert a Lua script return value into a RESP reply (Redis conventions).
fn lua_value_to_resp(v: LuaValue) -> std::result::Result<RespValue, String> {
    match v {
        LuaValue::Nil => Ok(RespValue::null()),
        LuaValue::Boolean(false) => Ok(RespValue::null()),
        LuaValue::Boolean(true) => Ok(RespValue::Integer(1)),
        LuaValue::Integer(i) => Ok(RespValue::Integer(i)),
        LuaValue::Number(n) => {
            if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
                Ok(RespValue::Integer(n as i64))
            } else {
                Ok(RespValue::BulkString(Some(Bytes::from(n.to_string()))))
            }
        }
        LuaValue::String(s) => {
            let bytes = Bytes::copy_from_slice(s.as_bytes().as_ref());
            Ok(RespValue::BulkString(Some(bytes)))
        }
        LuaValue::Table(t) => {
            // Status / error reply tables.
            if let Ok(LuaValue::String(ok)) = t.get::<LuaValue>("ok") {
                let s = ok.to_str().map_err(|e| e.to_string())?.to_string();
                return Ok(RespValue::SimpleString(Bytes::from(s)));
            }
            if let Ok(LuaValue::String(err)) = t.get::<LuaValue>("err") {
                let s = err.to_str().map_err(|e| e.to_string())?.to_string();
                return Ok(RespValue::error(s));
            }
            // Array-like table: consecutive integer keys from 1.
            let mut items = Vec::new();
            let mut i = 1i64;
            loop {
                let val: LuaValue = t.get(i).map_err(|e| e.to_string())?;
                if matches!(val, LuaValue::Nil) {
                    break;
                }
                items.push(lua_value_to_resp(val)?);
                i += 1;
                if i > 1_000_000 {
                    return Err("ERR Lua table too large".into());
                }
            }
            Ok(RespValue::Array(items))
        }
        other => Err(format!(
            "ERR Lua script returned unsupported value type: {}",
            other.type_name()
        )),
    }
}

/// Public helper for tests / docs.
#[allow(dead_code)]
pub fn sha_of(script: &str) -> String {
    script_sha1(script)
}
