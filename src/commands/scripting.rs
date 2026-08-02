//! EVAL / EVALSHA / EVAL_RO / EVALSHA_RO / SCRIPT / FUNCTION / FCALL — Lua scripting
//! and Redis Functions (mlua Lua 5.4).

use super::{is_write_command, CommandHandler};
use crate::error::Result;
use crate::protocol::RespValue;
use crate::scripting::{
    parse_function_shebang, script_sha1, strip_function_shebang, FunctionLibrary,
    FunctionMeta,
};
use bytes::Bytes;
use mlua::{Lua, Value as LuaValue};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

/// Commands that may be invoked via `redis.call` / `redis.pcall` inside scripts.
/// Blocking / multi / admin / nested-script commands are excluded.
const SCRIPT_CALL_ALLOWLIST: &[&str] = &[
    "PING", "ECHO", //
    "GET", "SET", "DEL", "EXISTS", "TYPE", "MGET", "MSET", "MSETNX", "APPEND", "STRLEN", "LCS",
    "GETRANGE", "SUBSTR", "SETRANGE", "SETEX", "PSETEX", "DUMP", "RESTORE",
    "GETSET", "UNLINK", "RENAME", "RENAMENX", "SETNX", "GETDEL", "GETEX", "COPY", "MOVE", //
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
    "DBSIZE", "KEYS", "SWAPDB", "SELECT", "FLUSHDB", //
    "GEOADD", "GEODIST", "GEOPOS", "GEOHASH", "GEOSEARCH", "GEOSEARCHSTORE", //
    "GEORADIUS", "GEORADIUSBYMEMBER", "GEORADIUS_RO", "GEORADIUSBYMEMBER_RO", //
    "XADD", "XLEN", "XRANGE", "XREVRANGE", "XDEL", "XTRIM", "XACK", //
    "TOUCH", "RANDOMKEY", "SCAN", //
];

impl CommandHandler {
    /// EVAL script numkeys [key ...] [arg ...]
    pub(super) fn handle_eval(&mut self, args: &[RespValue]) -> Result<RespValue> {
        self.eval_from_source(args, false, "eval")
    }

    /// EVAL_RO — read-only EVAL (write redis.call rejected).
    pub(super) fn handle_eval_ro(&mut self, args: &[RespValue]) -> Result<RespValue> {
        self.eval_from_source(args, true, "eval_ro")
    }

    /// EVALSHA sha1 numkeys [key ...] [arg ...]
    pub(super) fn handle_evalsha(&mut self, args: &[RespValue]) -> Result<RespValue> {
        self.eval_from_sha(args, false, "evalsha")
    }

    /// EVALSHA_RO — read-only EVALSHA.
    pub(super) fn handle_evalsha_ro(&mut self, args: &[RespValue]) -> Result<RespValue> {
        self.eval_from_sha(args, true, "evalsha_ro")
    }

    fn eval_from_source(
        &mut self,
        args: &[RespValue],
        readonly: bool,
        cmd: &str,
    ) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(format!(
                "ERR wrong number of arguments for '{}'",
                cmd
            )));
        }
        let script = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).into_owned(),
            None => return Ok(RespValue::error("ERR invalid script")),
        };
        // Auto-cache so EVALSHA / EVALSHA_RO work after EVAL / EVAL_RO (Redis does this).
        let _ = self.script_cache.load(&script);
        self.eval_script_body(&script, &args[1..], readonly, cmd)
    }

    fn eval_from_sha(
        &mut self,
        args: &[RespValue],
        readonly: bool,
        cmd: &str,
    ) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(format!(
                "ERR wrong number of arguments for '{}'",
                cmd
            )));
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
        self.eval_script_body(&script, &args[1..], readonly, cmd)
    }

    /// FUNCTION HELP | LIST | LOAD | DELETE | FLUSH | DUMP | RESTORE | STATS | KILL
    pub(super) fn handle_function(&mut self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'function' command",
            ));
        }
        let sub = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_ascii_uppercase(),
            None => return Ok(RespValue::error("ERR syntax error")),
        };
        match sub.as_str() {
            "LIST" => self.function_list(&args[1..]),
            "STATS" => {
                // Minimal stats map (RESP2 flat field/value array).
                let engines = RespValue::Array(vec![
                    RespValue::BulkString(Some(Bytes::from_static(b"LUA"))),
                    RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from_static(b"libraries_count"))),
                        RespValue::Integer(self.function_libs.library_count() as i64),
                        RespValue::BulkString(Some(Bytes::from_static(b"functions_count"))),
                        RespValue::Integer(
                            self.function_libs
                                .list()
                                .iter()
                                .map(|l| l.functions.len() as i64)
                                .sum(),
                        ),
                    ]),
                ]);
                Ok(RespValue::Array(vec![
                    RespValue::BulkString(Some(Bytes::from_static(b"running_script"))),
                    RespValue::null(),
                    RespValue::BulkString(Some(Bytes::from_static(b"engines"))),
                    engines,
                ]))
            }
            "LOAD" => self.function_load(&args[1..]),
            "DELETE" => self.function_delete(&args[1..]),
            "FLUSH" => self.function_flush(&args[1..]),
            "DUMP" => {
                if args.len() != 1 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'function|dump'",
                    ));
                }
                let payload = self.function_libs.dump();
                Ok(RespValue::BulkString(Some(Bytes::from(payload))))
            }
            "RESTORE" => self.function_restore(&args[1..]),
            "KILL" => Ok(RespValue::error(
                "NOTBUSY No scripts in execution right now.",
            )),
            "HELP" => Ok(RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from_static(
                    b"FUNCTION <subcommand> [<arg> ...]. Subcommands are:",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"LIST [LIBRARYNAME name] [WITHCODE] -- list loaded libraries",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"LOAD [REPLACE] <library-code> -- load a Lua library (#!lua name=...)",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"DELETE <library-name> -- delete a library",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"FLUSH [ASYNC|SYNC] -- delete all libraries",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"DUMP -- serialize all libraries",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"RESTORE <payload> [FLUSH|APPEND|REPLACE] -- restore libraries",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"STATS -- function runtime stats",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"KILL -- kill the currently executing function (NOTBUSY if none)",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"HELP -- print this help",
                ))),
            ])),
            _ => Ok(RespValue::error(format!(
                "ERR unknown subcommand '{}'. Try FUNCTION HELP.",
                sub
            ))),
        }
    }

    fn function_list(&self, args: &[RespValue]) -> Result<RespValue> {
        let mut libraryname: Option<String> = None;
        let mut withcode = false;
        let mut i = 0;
        while i < args.len() {
            let tok = match args[i].as_bulk_string() {
                Some(b) => String::from_utf8_lossy(b).to_ascii_uppercase(),
                None => return Ok(RespValue::error("ERR syntax error")),
            };
            match tok.as_str() {
                "WITHCODE" => {
                    withcode = true;
                    i += 1;
                }
                "LIBRARYNAME" => {
                    if i + 1 >= args.len() {
                        return Ok(RespValue::error("ERR syntax error"));
                    }
                    libraryname = match args[i + 1].as_bulk_string() {
                        Some(b) => Some(String::from_utf8_lossy(b).into_owned()),
                        None => return Ok(RespValue::error("ERR syntax error")),
                    };
                    i += 2;
                }
                _ => {
                    return Ok(RespValue::error("ERR syntax error"));
                }
            }
        }
        let libs = self
            .function_libs
            .list_filtered(libraryname.as_deref());
        let mut out = Vec::with_capacity(libs.len());
        for lib in libs {
            let mut entry = vec![
                RespValue::BulkString(Some(Bytes::from_static(b"library_name"))),
                RespValue::BulkString(Some(Bytes::from(lib.name.clone()))),
                RespValue::BulkString(Some(Bytes::from_static(b"engine"))),
                RespValue::BulkString(Some(Bytes::from(lib.engine.clone()))),
                RespValue::BulkString(Some(Bytes::from_static(b"functions"))),
            ];
            let mut fns = Vec::with_capacity(lib.functions.len());
            for f in &lib.functions {
                let flags: Vec<RespValue> = f
                    .flags
                    .iter()
                    .map(|fl| RespValue::BulkString(Some(Bytes::from(fl.clone()))))
                    .collect();
                fns.push(RespValue::Array(vec![
                    RespValue::BulkString(Some(Bytes::from_static(b"name"))),
                    RespValue::BulkString(Some(Bytes::from(f.name.clone()))),
                    RespValue::BulkString(Some(Bytes::from_static(b"description"))),
                    RespValue::BulkString(Some(Bytes::from(f.description.clone()))),
                    RespValue::BulkString(Some(Bytes::from_static(b"flags"))),
                    RespValue::Array(flags),
                ]));
            }
            entry.push(RespValue::Array(fns));
            if withcode {
                entry.push(RespValue::BulkString(Some(Bytes::from_static(
                    b"library_code",
                ))));
                entry.push(RespValue::BulkString(Some(Bytes::from(lib.code.clone()))));
            }
            out.push(RespValue::Array(entry));
        }
        Ok(RespValue::Array(out))
    }

    fn function_load(&mut self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'function|load'",
            ));
        }
        let mut replace = false;
        let mut idx = 0;
        if let Some(b) = args[0].as_bulk_string() {
            if String::from_utf8_lossy(b).eq_ignore_ascii_case("REPLACE") {
                replace = true;
                idx = 1;
            }
        }
        if idx >= args.len() || args.len() - idx != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'function|load'",
            ));
        }
        let code = match args[idx].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR invalid library code")),
        };
        match self.load_function_library(&code, replace) {
            Ok(name) => Ok(RespValue::BulkString(Some(Bytes::from(name)))),
            Err(e) => Ok(RespValue::error(e)),
        }
    }

    fn function_delete(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'function|delete'",
            ));
        }
        let name = match args[0].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR invalid library name")),
        };
        match self.function_libs.delete(&name) {
            Ok(()) => Ok(RespValue::ok()),
            Err(e) => Ok(RespValue::error(e)),
        }
    }

    fn function_flush(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() > 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'function|flush'",
            ));
        }
        if args.len() == 1 {
            let mode = match args[0].as_bulk_string() {
                Some(s) => String::from_utf8_lossy(s).to_uppercase(),
                None => return Ok(RespValue::error("ERR syntax error")),
            };
            if mode != "ASYNC" && mode != "SYNC" {
                return Ok(RespValue::error("ERR syntax error"));
            }
        }
        self.function_libs.flush();
        Ok(RespValue::ok())
    }

    fn function_restore(&mut self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() || args.len() > 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'function|restore'",
            ));
        }
        let payload = match args[0].as_bulk_string() {
            Some(b) => b.to_vec(),
            None => return Ok(RespValue::error("ERR invalid payload")),
        };
        let mode = if args.len() == 2 {
            match args[1].as_bulk_string() {
                Some(s) => String::from_utf8_lossy(s).to_ascii_lowercase(),
                None => return Ok(RespValue::error("ERR syntax error")),
            }
        } else {
            "append".to_string()
        };
        if mode != "flush" && mode != "append" && mode != "replace" {
            return Ok(RespValue::error("ERR syntax error"));
        }
        let pairs = match crate::scripting::FunctionLibraryStore::parse_dump(&payload) {
            Ok(p) => p,
            Err(e) => return Ok(RespValue::error(e)),
        };
        if mode == "flush" {
            self.function_libs.flush();
        }
        for (_name, code) in pairs {
            let replace = mode == "replace" || mode == "flush";
            if let Err(e) = self.load_function_library(&code, replace) {
                // APPEND: fail on conflict (load without replace).
                return Ok(RespValue::error(e));
            }
        }
        Ok(RespValue::ok())
    }

    /// Parse shebang, run library to capture `redis.register_function`, store.
    fn load_function_library(
        &mut self,
        code: &str,
        replace: bool,
    ) -> std::result::Result<String, String> {
        let shebang = parse_function_shebang(code)?;
        let metas = discover_registered_functions(code)?;
        if metas.is_empty() {
            return Err(
                "ERR No functions registered. Use redis.register_function.".into(),
            );
        }
        let lib = FunctionLibrary {
            name: shebang.name.clone(),
            engine: shebang.engine,
            code: code.to_string(),
            functions: metas,
        };
        self.function_libs.load(lib, replace)?;
        Ok(shebang.name)
    }

    /// FCALL function numkeys [key ...] [arg ...]
    pub(super) fn handle_fcall(&mut self, args: &[RespValue]) -> Result<RespValue> {
        self.fcall_exec(args, false, "fcall")
    }

    /// FCALL_RO — read-only FCALL (requires `no-writes` flag; write redis.call denied).
    pub(super) fn handle_fcall_ro(&mut self, args: &[RespValue]) -> Result<RespValue> {
        self.fcall_exec(args, true, "fcall_ro")
    }

    fn fcall_exec(
        &mut self,
        args: &[RespValue],
        readonly: bool,
        cmd: &str,
    ) -> Result<RespValue> {
        if args.len() < 2 {
            return Ok(RespValue::error(format!(
                "ERR wrong number of arguments for '{}' command",
                cmd
            )));
        }
        let name = match args[0].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR Function not found")),
        };
        let lib = match self.function_libs.find_function(&name) {
            Some(l) => l,
            None => {
                return Ok(RespValue::error(format!(
                    "ERR Function not found",
                )));
            }
        };
        let meta = lib
            .functions
            .iter()
            .find(|f| f.name == name)
            .cloned()
            .unwrap_or(FunctionMeta {
                name: name.clone(),
                description: String::new(),
                flags: vec![],
            });
        if readonly {
            let no_writes = meta
                .flags
                .iter()
                .any(|f| f.eq_ignore_ascii_case("no-writes"));
            if !no_writes {
                return Ok(RespValue::error(
                    "ERR Can not execute a function with the specified flags using fcall_ro",
                ));
            }
        }

        // Parse numkeys / KEYS / ARGV like EVAL.
        let numkeys = match self.parse_integer(&args[1]) {
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
        let rest = &args[2..];
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
                    if let Some(i) = a.as_integer() {
                        argv.push(Bytes::from(i.to_string()));
                    } else {
                        return Ok(RespValue::error("ERR invalid argument"));
                    }
                }
            }
        }

        match self.run_function(&lib.code, &name, &keys, &argv, readonly) {
            Ok(v) => Ok(v),
            Err(e) => Ok(RespValue::error(e)),
        }
    }

    /// Re-exec library code, then invoke the named registered function(keys, args).
    fn run_function(
        &mut self,
        library_code: &str,
        function_name: &str,
        keys: &[Bytes],
        argv: &[Bytes],
        readonly: bool,
    ) -> std::result::Result<RespValue, String> {
        let lua = Lua::new();
        let handler_ptr = self as *mut CommandHandler;
        let in_call = AtomicBool::new(false);

        // Captured callbacks: name → Function (stored via Arc/Mutex for registry).
        let registry: Arc<StdMutex<HashMap<String, mlua::Function>>> =
            Arc::new(StdMutex::new(HashMap::new()));

        install_redis_table(&lua, handler_ptr, &in_call, readonly, Some(&registry))?;

        // Execute library body (shebang stripped — Lua rejects #!).
        let body = strip_function_shebang(library_code);
        lua.load(body)
            .set_name("function_library")
            .exec()
            .map_err(|e| format!("ERR Error running script: {}", e))?;

        let callback = {
            let map = registry
                .lock()
                .map_err(|_| "ERR function registry lock poisoned".to_string())?;
            map.get(function_name)
                .cloned()
                .ok_or_else(|| "ERR Function not found".to_string())?
        };

        // Build keys / args tables (1-based). Redis Functions use keys/args params,
        // not global KEYS/ARGV — but we still set globals for library convenience.
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

        let ret: LuaValue = callback
            .call((keys_tbl, argv_tbl))
            .map_err(|e| {
                let msg = e.to_string();
                if msg.contains("NOSCRIPT")
                    || msg.starts_with("ERR ")
                    || msg.starts_with("WRONGTYPE")
                {
                    msg
                } else {
                    format!("ERR function_lib: {}", msg)
                }
            })?;

        lua_value_to_resp(ret)
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
            "HELP" => Ok(RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from_static(
                    b"SCRIPT <subcommand> [<arg> ...]. Subcommands are:",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"LOAD <script> -- load a script into the cache, return its SHA1",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"EXISTS <sha1> [<sha1> ...] -- return array of 0/1 existence flags",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"FLUSH [ASYNC|SYNC] -- flush the script cache",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"KILL -- kill the currently executing script (NOTBUSY if none)",
                ))),
                RespValue::BulkString(Some(Bytes::from_static(
                    b"HELP -- print this help",
                ))),
            ])),
            _ => Ok(RespValue::error(format!(
                "ERR unknown subcommand or wrong number of arguments for '{}'. \
                 Try SCRIPT HELP.",
                sub.to_ascii_lowercase()
            ))),
        }
    }

    /// Parse numkeys + KEYS/ARGV and run `source` under mlua with redis.call.
    fn eval_script_body(
        &mut self,
        source: &str,
        args: &[RespValue],
        readonly: bool,
        cmd: &str,
    ) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(format!(
                "ERR wrong number of arguments for '{}'",
                cmd
            )));
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

        match self.run_lua(source, &keys, &argv, readonly) {
            Ok(v) => Ok(v),
            Err(e) => Ok(RespValue::error(e)),
        }
    }

    fn run_lua(
        &mut self,
        source: &str,
        keys: &[Bytes],
        argv: &[Bytes],
        readonly: bool,
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
        install_redis_table(&lua, handler_ptr, &in_call, readonly, None)?;

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
            "COPY" => self.handle_copy(args),
            "MOVE" => self.handle_move(args),
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
            "GEOADD" => self.handle_geoadd(args),
            "GEODIST" => self.handle_geodist(args),
            "GEOPOS" => self.handle_geopos(args),
            "GEOHASH" => self.handle_geohash(args),
            "GEOSEARCH" => self.handle_geosearch(args),
            "GEOSEARCHSTORE" => self.handle_geosearchstore(args),
            "GEORADIUS" => self.handle_georadius(args),
            "GEORADIUSBYMEMBER" => self.handle_georadiusbymember(args),
            "GEORADIUS_RO" => self.handle_georadius_ro(args),
            "GEORADIUSBYMEMBER_RO" => self.handle_georadiusbymember_ro(args),
            "XADD" => self.handle_xadd(args),
            "XLEN" => self.handle_xlen(args),
            "XRANGE" => self.handle_xrange(args),
            "XREVRANGE" => self.handle_xrevrange(args),
            "XDEL" => self.handle_xdel(args),
            "XTRIM" => self.handle_xtrim(args),
            "XACK" => self.handle_xack(args),
            "TOUCH" => self.handle_touch(args),
            "RANDOMKEY" => self.handle_randomkey(args),
            "SCAN" => self.handle_scan(args),
            "SWAPDB" => self.handle_swapdb(args),
            "SELECT" => self.handle_select(args),
            "FLUSHDB" => self.handle_flushdb(args),
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

/// Install `redis.call` / `redis.pcall` / helpers, and optionally
/// `redis.register_function` (Redis Functions libraries).
///
/// When `registry` is `Some`, callbacks are stored for later FCALL.
/// When `None` (EVAL path), `register_function` is omitted.
fn install_redis_table(
    lua: &Lua,
    handler_ptr: *mut CommandHandler,
    in_call: &AtomicBool,
    readonly: bool,
    registry: Option<&Arc<StdMutex<HashMap<String, mlua::Function>>>>,
) -> std::result::Result<(), String> {
    let call_fn = {
        let in_call = in_call as *const AtomicBool;
        lua.create_function(move |lua_ctx, args: mlua::Variadic<LuaValue>| {
            let in_call = unsafe { &*in_call };
            if in_call.swap(true, Ordering::SeqCst) {
                return Err(mlua::Error::runtime(
                    "ERR redis.call re-entry is not allowed",
                ));
            }
            let result = (|| {
                let h = unsafe { &mut *handler_ptr };
                dispatch_redis_call(lua_ctx, h, args, false, readonly)
            })();
            in_call.store(false, Ordering::SeqCst);
            result
        })
        .map_err(|e| e.to_string())?
    };

    let pcall_fn = {
        let in_call = in_call as *const AtomicBool;
        lua.create_function(move |lua_ctx, args: mlua::Variadic<LuaValue>| {
            let in_call = unsafe { &*in_call };
            if in_call.swap(true, Ordering::SeqCst) {
                return Err(mlua::Error::runtime(
                    "ERR redis.call re-entry is not allowed",
                ));
            }
            let result = (|| {
                let h = unsafe { &mut *handler_ptr };
                dispatch_redis_call(lua_ctx, h, args, true, readonly)
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

    if let Some(reg) = registry {
        let meta_out: Arc<StdMutex<Vec<FunctionMeta>>> =
            Arc::new(StdMutex::new(Vec::new()));
        install_register_function_dual(
            lua,
            &redis_tbl,
            Arc::clone(reg),
            meta_out,
        )?;
    }

    lua.globals()
        .set("redis", redis_tbl)
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Two-arg `redis.register_function(name, callback)` via Variadic wrapper installed
/// at discovery/load time.
fn install_register_function_dual(
    lua: &Lua,
    redis_tbl: &mlua::Table,
    registry: Arc<StdMutex<HashMap<String, mlua::Function>>>,
    meta_out: Arc<StdMutex<Vec<FunctionMeta>>>,
) -> std::result::Result<(), String> {
    let register_fn = lua
        .create_function(move |lua_ctx, args: mlua::Variadic<LuaValue>| {
            if args.is_empty() {
                return Err(mlua::Error::runtime(
                    "ERR wrong number of arguments to redis.register_function",
                ));
            }
            // Two-arg form: name, callback
            if args.len() >= 2 {
                if let (LuaValue::String(name_s), LuaValue::Function(cb)) =
                    (&args[0], &args[1])
                {
                    let name = name_s
                        .to_str()
                        .map_err(mlua::Error::external)?
                        .to_string();
                    if name.is_empty() {
                        return Err(mlua::Error::runtime(
                            "ERR Function name cannot be empty",
                        ));
                    }
                    {
                        let mut map = registry.lock().map_err(|e| {
                            mlua::Error::runtime(format!("registry lock: {}", e))
                        })?;
                        if map.contains_key(&name) {
                            return Err(mlua::Error::runtime(format!(
                                "ERR Function {} already registered",
                                name
                            )));
                        }
                        map.insert(name.clone(), cb.clone());
                    }
                    let mut metas = meta_out.lock().map_err(|e| {
                        mlua::Error::runtime(format!("meta lock: {}", e))
                    })?;
                    metas.push(FunctionMeta {
                        name,
                        description: String::new(),
                        flags: vec![],
                    });
                    return Ok(());
                }
            }
            // Table form
            let arg = args.into_iter().next().unwrap();
            match arg {
                LuaValue::Table(t) => {
                    let name: String = t
                        .get::<LuaValue>("function_name")
                        .ok()
                        .and_then(|v| match v {
                            LuaValue::String(s) => {
                                s.to_str().ok().map(|x| x.to_string())
                            }
                            _ => None,
                        })
                        .ok_or_else(|| {
                            mlua::Error::runtime(
                                "ERR register_function: missing function_name",
                            )
                        })?;
                    let callback: mlua::Function = t
                        .get::<LuaValue>("callback")
                        .ok()
                        .and_then(|v| match v {
                            LuaValue::Function(f) => Some(f),
                            _ => None,
                        })
                        .ok_or_else(|| {
                            mlua::Error::runtime(
                                "ERR register_function: missing callback",
                            )
                        })?;
                    let description = t
                        .get::<LuaValue>("description")
                        .ok()
                        .and_then(|v| match v {
                            LuaValue::String(s) => {
                                s.to_str().ok().map(|x| x.to_string())
                            }
                            _ => None,
                        })
                        .unwrap_or_default();
                    let mut flags = Vec::new();
                    if let Ok(LuaValue::Table(ft)) = t.get::<LuaValue>("flags") {
                        let mut i = 1i64;
                        loop {
                            match ft.get::<LuaValue>(i) {
                                Ok(LuaValue::String(s)) => {
                                    if let Ok(ss) = s.to_str() {
                                        flags.push(ss.to_string());
                                    }
                                    i += 1;
                                }
                                Ok(LuaValue::Nil) | Err(_) => break,
                                Ok(_) => i += 1,
                            }
                            if i > 64 {
                                break;
                            }
                        }
                    }
                    {
                        let mut map = registry.lock().map_err(|e| {
                            mlua::Error::runtime(format!("registry lock: {}", e))
                        })?;
                        if map.contains_key(&name) {
                            return Err(mlua::Error::runtime(format!(
                                "ERR Function {} already registered",
                                name
                            )));
                        }
                        map.insert(name.clone(), callback);
                    }
                    let mut metas = meta_out.lock().map_err(|e| {
                        mlua::Error::runtime(format!("meta lock: {}", e))
                    })?;
                    metas.push(FunctionMeta {
                        name,
                        description,
                        flags,
                    });
                    let _ = lua_ctx;
                    Ok(())
                }
                _ => Err(mlua::Error::runtime(
                    "ERR redis.register_function expects (name, callback) or a table",
                )),
            }
        })
        .map_err(|e| e.to_string())?;
    redis_tbl
        .set("register_function", register_fn)
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Run library source at LOAD time to collect registered function metadata.
fn discover_registered_functions(
    code: &str,
) -> std::result::Result<Vec<FunctionMeta>, String> {
    let lua = Lua::new();
    let registry: Arc<StdMutex<HashMap<String, mlua::Function>>> =
        Arc::new(StdMutex::new(HashMap::new()));
    let meta_out: Arc<StdMutex<Vec<FunctionMeta>>> =
        Arc::new(StdMutex::new(Vec::new()));

    // Minimal redis table: register_function + stubs for call/pcall/status/error.
    let redis_tbl = lua.create_table().map_err(|e| e.to_string())?;
    install_register_function_dual(
        &lua,
        &redis_tbl,
        Arc::clone(&registry),
        Arc::clone(&meta_out),
    )?;

    // Stubs so libraries that touch redis.call at load time fail clearly.
    let deny = lua
        .create_function(|_, ()| -> mlua::Result<()> {
            Err(mlua::Error::runtime(
                "ERR redis.call is not allowed while loading a function library",
            ))
        })
        .map_err(|e| e.to_string())?;
    // Use variadic deny for call/pcall
    let deny_call = lua
        .create_function(|_, _args: mlua::Variadic<LuaValue>| -> mlua::Result<LuaValue> {
            Err(mlua::Error::runtime(
                "ERR redis.call is not allowed while loading a function library",
            ))
        })
        .map_err(|e| e.to_string())?;
    redis_tbl
        .set("call", deny_call.clone())
        .map_err(|e| e.to_string())?;
    redis_tbl
        .set("pcall", deny_call)
        .map_err(|e| e.to_string())?;
    let _ = deny;
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

    let body = strip_function_shebang(code);
    lua.load(body)
        .set_name("function_library_load")
        .exec()
        .map_err(|e| format!("ERR Error compiling script: {}", e))?;

    let metas = meta_out
        .lock()
        .map_err(|e| format!("meta lock: {}", e))?
        .clone();
    Ok(metas)
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
    readonly: bool,
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

    // EVAL_RO / EVALSHA_RO: reject write commands (Redis-compatible message).
    if readonly && is_write_command(&cmd) {
        let err = "ERR Write commands are not allowed from read-only scripts.";
        if is_pcall {
            return pcall_err(lua, err);
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
