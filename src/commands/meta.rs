//! CLIENT / COMMAND / HELLO — Redis client handshake & introspection.

use super::CommandHandler;
use crate::error::Result;
use crate::protocol::RespValue;
use bytes::Bytes;

/// Static command catalog entry (Redis COMMAND reply shape, simplified).
struct CmdSpec {
    name: &'static str,
    /// Arity: positive = exact, negative = minimum (|arity| args including command name).
    arity: i64,
    flags: &'static [&'static str],
    first_key: i64,
    last_key: i64,
    step: i64,
}

/// Known commands for COMMAND / COMMAND COUNT / COMMAND INFO.
const COMMAND_SPECS: &[CmdSpec] = &[
    CmdSpec { name: "ping", arity: -1, flags: &["fast", "stale"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "echo", arity: 2, flags: &["fast"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "time", arity: 1, flags: &["loading", "stale", "fast", "random"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "auth", arity: -2, flags: &["noscript", "loading", "stale", "fast", "no_auth", "ok_loading"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "quit", arity: -1, flags: &["admin", "noscript", "loading", "stale", "fast"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "reset", arity: 1, flags: &["noscript", "loading", "stale", "fast"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "hello", arity: -1, flags: &["noscript", "loading", "stale", "fast", "no_auth"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "command", arity: -1, flags: &["loading", "stale", "random"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "client", arity: -2, flags: &["admin", "noscript", "loading", "stale", "random"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "acl", arity: -2, flags: &["admin", "noscript", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "cluster", arity: -2, flags: &["admin", "random", "stale"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "asking", arity: 1, flags: &["fast"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "lolwut", arity: -1, flags: &["readonly", "fast"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "readonly", arity: 1, flags: &["fast", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "readwrite", arity: 1, flags: &["fast", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
    // SORT: source key always; STORE dest is movable (handled in GETKEYS).
    CmdSpec { name: "sort", arity: -2, flags: &["write", "denyoom", "movablekeys"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "set", arity: -3, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "get", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "del", arity: -2, flags: &["write"], first_key: 1, last_key: -1, step: 1 },
    CmdSpec { name: "exists", arity: -2, flags: &["readonly", "fast"], first_key: 1, last_key: -1, step: 1 },
    CmdSpec { name: "type", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "mget", arity: -2, flags: &["readonly", "fast"], first_key: 1, last_key: -1, step: 1 },
    CmdSpec { name: "mset", arity: -3, flags: &["write", "denyoom"], first_key: 1, last_key: -1, step: 2 },
    CmdSpec { name: "msetnx", arity: -3, flags: &["write", "denyoom"], first_key: 1, last_key: -1, step: 2 },
    CmdSpec { name: "append", arity: 3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "strlen", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "lcs", arity: -3, flags: &["readonly"], first_key: 1, last_key: 2, step: 1 },
    CmdSpec { name: "dump", arity: 2, flags: &["readonly", "random"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "restore", arity: -4, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "getrange", arity: 4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "substr", arity: 4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "setrange", arity: 4, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "setex", arity: 4, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "psetex", arity: 4, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "getset", arity: 3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "incrbyfloat", arity: 3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "unlink", arity: -2, flags: &["write", "fast"], first_key: 1, last_key: -1, step: 1 },
    CmdSpec { name: "rename", arity: 3, flags: &["write"], first_key: 1, last_key: 2, step: 1 },
    CmdSpec { name: "renamenx", arity: 3, flags: &["write", "fast"], first_key: 1, last_key: 2, step: 1 },
    CmdSpec { name: "move", arity: 3, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "copy", arity: -3, flags: &["write", "denyoom"], first_key: 1, last_key: 2, step: 1 },
    CmdSpec { name: "randomkey", arity: 1, flags: &["readonly", "random"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "touch", arity: -2, flags: &["write", "fast"], first_key: 1, last_key: -1, step: 1 },
    CmdSpec { name: "setnx", arity: 3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "getdel", arity: 2, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "getex", arity: -2, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "incr", arity: 2, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "decr", arity: 2, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "incrby", arity: 3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "decrby", arity: 3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    // EXPIRE family accepts optional NX|XX|GT|LT (arity still -3 minimum).
    CmdSpec { name: "expire", arity: -3, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "pexpire", arity: -3, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "expireat", arity: -3, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "pexpireat", arity: -3, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "persist", arity: 2, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "ttl", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "pttl", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "expiretime", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "pexpiretime", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "select", arity: 2, flags: &["loading", "fast"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "swapdb", arity: 3, flags: &["write", "fast"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "dbsize", arity: 1, flags: &["readonly", "fast"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "keys", arity: 2, flags: &["readonly", "sort_for_script"], first_key: 0, last_key: 0, step: 0 },
    // SCAN: MATCH / COUNT / TYPE
    CmdSpec { name: "scan", arity: -2, flags: &["readonly", "random"], first_key: 0, last_key: 0, step: 0 },
    // FLUSHDB/FLUSHALL accept optional ASYNC|SYNC
    CmdSpec { name: "flushdb", arity: -1, flags: &["write"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "flushall", arity: -1, flags: &["write"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "info", arity: -1, flags: &["loading", "stale", "random"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "health", arity: -1, flags: &["loading", "stale", "fast", "random"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "config", arity: -2, flags: &["admin", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
    // MEMORY USAGE key [SAMPLES n] — first_key points at USAGE's key (arg index 2 in full command).
    CmdSpec { name: "memory", arity: -2, flags: &["readonly", "random"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "object", arity: -2, flags: &["readonly", "random"], first_key: 2, last_key: 2, step: 1 },
    CmdSpec { name: "slowlog", arity: -2, flags: &["admin", "random", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "latency", arity: -2, flags: &["admin", "noscript", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "module", arity: -2, flags: &["admin", "noscript", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "save", arity: 1, flags: &["admin", "noscript"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "bgsave", arity: -1, flags: &["admin", "noscript"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "lastsave", arity: 1, flags: &["loading", "stale", "fast", "admin"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "bgrewriteaof", arity: 1, flags: &["admin", "noscript"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "multi", arity: 1, flags: &["noscript", "loading", "stale", "fast"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "exec", arity: 1, flags: &["noscript", "loading", "stale", "skip_slowlog", "skip_monitor"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "discard", arity: 1, flags: &["noscript", "loading", "stale", "fast"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "watch", arity: -2, flags: &["noscript", "loading", "stale", "fast"], first_key: 1, last_key: -1, step: 1 },
    CmdSpec { name: "unwatch", arity: 1, flags: &["noscript", "loading", "stale", "fast"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "hset", arity: -4, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hsetnx", arity: 4, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hget", arity: 3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hmget", arity: -3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hdel", arity: -3, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hgetdel", arity: -3, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hgetall", arity: 2, flags: &["readonly", "random"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hlen", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hexists", arity: 3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hkeys", arity: 2, flags: &["readonly", "sort_for_script"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hvals", arity: 2, flags: &["readonly", "sort_for_script"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hincrby", arity: 4, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hincrbyfloat", arity: 4, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hstrlen", arity: 3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hmset", arity: -4, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hrandfield", arity: -2, flags: &["readonly", "random"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hscan", arity: -3, flags: &["readonly", "random"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "lpush", arity: -3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "rpush", arity: -3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "lpushx", arity: -3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "rpushx", arity: -3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "lpop", arity: -2, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "rpop", arity: -2, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "blpop", arity: -3, flags: &["write", "blocking"], first_key: 1, last_key: -2, step: 1 },
    CmdSpec { name: "brpop", arity: -3, flags: &["write", "blocking"], first_key: 1, last_key: -2, step: 1 },
    CmdSpec { name: "lrange", arity: 4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "llen", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "lindex", arity: 3, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "lset", arity: 4, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "lrem", arity: 4, flags: &["write"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "ltrim", arity: 4, flags: &["write"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "linsert", arity: 5, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "lpos", arity: -3, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "lmove", arity: 5, flags: &["write", "denyoom"], first_key: 1, last_key: 2, step: 1 },
    CmdSpec { name: "blmove", arity: 6, flags: &["write", "denyoom", "blocking"], first_key: 1, last_key: 2, step: 1 },
    CmdSpec { name: "rpoplpush", arity: 3, flags: &["write", "denyoom"], first_key: 1, last_key: 2, step: 1 },
    CmdSpec { name: "brpoplpush", arity: 4, flags: &["write", "denyoom", "blocking"], first_key: 1, last_key: 2, step: 1 },
    CmdSpec { name: "lmpop", arity: -4, flags: &["write", "movablekeys"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "blmpop", arity: -5, flags: &["write", "blocking", "movablekeys"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "sadd", arity: -3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "srem", arity: -3, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "smembers", arity: 2, flags: &["readonly", "sort_for_script"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "sismember", arity: 3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "smismember", arity: -3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "scard", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "sinter", arity: -2, flags: &["readonly", "sort_for_script"], first_key: 1, last_key: -1, step: 1 },
    CmdSpec { name: "sintercard", arity: -3, flags: &["readonly", "movablekeys"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "sunion", arity: -2, flags: &["readonly", "sort_for_script"], first_key: 1, last_key: -1, step: 1 },
    CmdSpec { name: "sdiff", arity: -2, flags: &["readonly", "sort_for_script"], first_key: 1, last_key: -1, step: 1 },
    CmdSpec { name: "sinterstore", arity: -3, flags: &["write", "denyoom"], first_key: 1, last_key: -1, step: 1 },
    CmdSpec { name: "sunionstore", arity: -3, flags: &["write", "denyoom"], first_key: 1, last_key: -1, step: 1 },
    CmdSpec { name: "sdiffstore", arity: -3, flags: &["write", "denyoom"], first_key: 1, last_key: -1, step: 1 },
    CmdSpec { name: "smove", arity: 4, flags: &["write", "fast"], first_key: 1, last_key: 2, step: 1 },
    CmdSpec { name: "spop", arity: -2, flags: &["write", "random", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "srandmember", arity: -2, flags: &["readonly", "random"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "sscan", arity: -3, flags: &["readonly", "random"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "xadd", arity: -5, flags: &["write", "denyoom", "fast", "random"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "xlen", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "xrange", arity: -4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "xrevrange", arity: -4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "xdel", arity: -3, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "xtrim", arity: -4, flags: &["write"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "xread", arity: -3, flags: &["readonly", "blocking", "movablekeys"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "xgroup", arity: -2, flags: &["write", "admin"], first_key: 2, last_key: 2, step: 1 },
    CmdSpec { name: "xreadgroup", arity: -7, flags: &["write", "blocking", "movablekeys"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "xack", arity: -4, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "xpending", arity: -3, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "xclaim", arity: -6, flags: &["write", "random"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "xautoclaim", arity: -6, flags: &["write", "random"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "xsetid", arity: 3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "xinfo", arity: -2, flags: &["readonly", "random"], first_key: 2, last_key: 2, step: 1 },
    // ZADD accepts optional NX|XX|GT|LT|CH|INCR before score-member pairs.
    CmdSpec { name: "zadd", arity: -4, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    // ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
    CmdSpec { name: "zrange", arity: -4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zrangestore", arity: -5, flags: &["write", "denyoom"], first_key: 1, last_key: 2, step: 1 },
    CmdSpec { name: "zrevrange", arity: -4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zcard", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zscore", arity: 3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zmscore", arity: -3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zrem", arity: -3, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zrank", arity: -3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zrevrank", arity: -3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zincrby", arity: 4, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zrangebyscore", arity: -4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zrevrangebyscore", arity: -4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zcount", arity: 4, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zremrangebyrank", arity: 4, flags: &["write"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zremrangebyscore", arity: 4, flags: &["write"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zrangebylex", arity: -4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zrevrangebylex", arity: -4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zlexcount", arity: 4, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zremrangebylex", arity: 4, flags: &["write"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zrandmember", arity: -2, flags: &["readonly", "random"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zscan", arity: -3, flags: &["readonly", "random"], first_key: 1, last_key: 1, step: 1 },
    // numkeys + optional WEIGHTS/AGGREGATE/WITHSCORES make full key ranges movable.
    CmdSpec { name: "zunion", arity: -3, flags: &["readonly", "movablekeys"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "zinter", arity: -3, flags: &["readonly", "movablekeys"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "zdiff", arity: -3, flags: &["readonly", "movablekeys"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "zintercard", arity: -3, flags: &["readonly", "movablekeys"], first_key: 0, last_key: 0, step: 0 },
    // Store forms: expose dest only (numkeys complicates static key ranges).
    CmdSpec { name: "zunionstore", arity: -4, flags: &["write", "denyoom", "movablekeys"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zinterstore", arity: -4, flags: &["write", "denyoom", "movablekeys"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zdiffstore", arity: -4, flags: &["write", "denyoom", "movablekeys"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zpopmin", arity: -2, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zpopmax", arity: -2, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zmpop", arity: -4, flags: &["write", "movablekeys"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "bzpopmin", arity: -3, flags: &["write", "blocking", "fast"], first_key: 1, last_key: -2, step: 1 },
    CmdSpec { name: "bzpopmax", arity: -3, flags: &["write", "blocking", "fast"], first_key: 1, last_key: -2, step: 1 },
    CmdSpec { name: "bzmpop", arity: -5, flags: &["write", "blocking", "movablekeys"], first_key: 0, last_key: 0, step: 0 },
    // Geospatial
    CmdSpec { name: "geoadd", arity: -5, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "geosearch", arity: -7, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "geosearchstore", arity: -8, flags: &["write", "denyoom"], first_key: 1, last_key: 2, step: 1 },
    CmdSpec { name: "geodist", arity: -4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "geopos", arity: -2, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "geohash", arity: -2, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "georadius", arity: -6, flags: &["write", "movablekeys"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "georadius_ro", arity: -6, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "georadiusbymember", arity: -5, flags: &["write", "movablekeys"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "georadiusbymember_ro", arity: -5, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "publish", arity: 3, flags: &["pubsub", "loading", "stale", "fast"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "subscribe", arity: -2, flags: &["pubsub", "noscript", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "unsubscribe", arity: -1, flags: &["pubsub", "noscript", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "psubscribe", arity: -2, flags: &["pubsub", "noscript", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "punsubscribe", arity: -1, flags: &["pubsub", "noscript", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "pubsub", arity: -2, flags: &["pubsub", "random", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
    // Redis 7.0+ Shard Pub/Sub
    CmdSpec { name: "spublish", arity: 3, flags: &["pubsub", "loading", "stale", "fast"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "ssubscribe", arity: -2, flags: &["pubsub", "noscript", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "sunsubscribe", arity: -1, flags: &["pubsub", "noscript", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "replicaof", arity: -3, flags: &["admin", "noscript", "stale"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "slaveof", arity: -3, flags: &["admin", "noscript", "stale"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "failover", arity: -1, flags: &["admin", "noscript", "stale"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "sync", arity: 1, flags: &["admin", "noscript"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "psync", arity: 3, flags: &["admin", "noscript"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "replconf", arity: -1, flags: &["admin", "noscript", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "role", arity: 1, flags: &["readonly", "fast", "noscript", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "wait", arity: 3, flags: &["noscript"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "setbit", arity: 4, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "getbit", arity: 3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "bitcount", arity: -2, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "bitpos", arity: -3, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "bitop", arity: -4, flags: &["write", "denyoom"], first_key: 2, last_key: -1, step: 1 },
    CmdSpec { name: "bitfield", arity: -2, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "bitfield_ro", arity: -2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "pfadd", arity: -2, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "pfcount", arity: -2, flags: &["readonly", "random"], first_key: 1, last_key: -1, step: 1 },
    CmdSpec { name: "pfmerge", arity: -2, flags: &["write", "denyoom"], first_key: 1, last_key: -1, step: 1 },
    // Lua scripting (keys are dynamic via numkeys; movablekeys in full Redis)
    CmdSpec { name: "eval", arity: -3, flags: &["noscript", "movablekeys"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "eval_ro", arity: -3, flags: &["readonly", "noscript", "movablekeys"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "evalsha", arity: -3, flags: &["noscript", "movablekeys"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "evalsha_ro", arity: -3, flags: &["readonly", "noscript", "movablekeys"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "script", arity: -2, flags: &["noscript"], first_key: 0, last_key: 0, step: 0 },
];

fn bulk(s: impl Into<Bytes>) -> RespValue {
    RespValue::BulkString(Some(s.into()))
}

fn parse_on_off(value: &RespValue) -> std::result::Result<bool, String> {
    match value.as_bulk_string() {
        Some(s) => match String::from_utf8_lossy(s).to_ascii_uppercase().as_str() {
            "ON" => Ok(true),
            "OFF" => Ok(false),
            _ => Err("ERR syntax error, try CLIENT [NO-EVICT|NO-TOUCH] ON|OFF".into()),
        },
        None => Err("ERR syntax error".into()),
    }
}

fn spec_to_reply(spec: &CmdSpec) -> RespValue {
    let flags: Vec<RespValue> = spec
        .flags
        .iter()
        .map(|f| RespValue::SimpleString(Bytes::from_static(f.as_bytes())))
        .collect();
    // Redis COMMAND entry: name, arity, flags, first, last, step, [acl cats...], [tips], [key specs]
    // Minimal 6-field form is accepted by most clients.
    RespValue::Array(vec![
        bulk(spec.name),
        RespValue::Integer(spec.arity),
        RespValue::Array(flags),
        RespValue::Integer(spec.first_key),
        RespValue::Integer(spec.last_key),
        RespValue::Integer(spec.step),
    ])
}

fn find_spec(name: &str) -> Option<&'static CmdSpec> {
    let lower = name.to_ascii_lowercase();
    COMMAND_SPECS.iter().find(|s| s.name == lower)
}

/// Return (first_key, last_key, step) for ACL key checks. Indexes are Redis-style
/// (1-based into the argument list after the command name).
pub(super) fn command_key_spec(name: &str) -> Option<(i64, i64, i64)> {
    find_spec(name).map(|s| (s.first_key, s.last_key, s.step))
}

impl CommandHandler {
    /// HELLO [protover] [AUTH password | AUTH user password] [SETNAME name]
    /// Supports RESP2 (proto 2) and RESP3 (proto 3). Other versions → NOPROTO.
    pub(super) async fn handle_hello(&mut self, args: &[RespValue]) -> Result<RespValue> {
        let mut i = 0;
        let mut proto: i64 = 2;

        // Optional leading protocol version
        if let Some(first) = args.first() {
            if let Ok(v) = self.parse_integer(first) {
                proto = v;
                i = 1;
            }
        }

        if proto != 2 && proto != 3 {
            return Ok(RespValue::error(
                "NOPROTO sorry, this protocol version is not supported",
            ));
        }

        while i < args.len() {
            let opt = match args[i].as_bulk_string() {
                Some(s) => String::from_utf8_lossy(s).to_uppercase(),
                None => return Ok(RespValue::error("ERR Syntax error in HELLO option")),
            };
            match opt.as_str() {
                "AUTH" => {
                    // Collect up to 2 non-keyword args: password, or username + password
                    let mut creds: Vec<Bytes> = Vec::new();
                    let mut j = i + 1;
                    while j < args.len() && creds.len() < 2 {
                        let Some(b) = args[j].as_bulk_string() else {
                            return Ok(RespValue::error(
                                "ERR Syntax error in HELLO option AUTH",
                            ));
                        };
                        if is_hello_keyword(b) {
                            break;
                        }
                        creds.push(b.clone());
                        j += 1;
                    }
                    let (username, password) = match creds.len() {
                        1 => ("default".to_string(), &creds[0]),
                        2 => (
                            String::from_utf8_lossy(&creds[0]).into_owned(),
                            &creds[1],
                        ),
                        _ => {
                            return Ok(RespValue::error(
                                "ERR Syntax error in HELLO option AUTH",
                            ));
                        }
                    };

                    let pass_str = String::from_utf8_lossy(password);
                    match self.acl.authenticate(&username, &pass_str) {
                        Ok(()) => {
                            self.authenticated = true;
                            self.username = Some(username);
                        }
                        Err(_) => {
                            self.cache.stats.incr(&self.cache.stats.auth_errors);
                            return Ok(RespValue::error(
                                "WRONGPASS invalid username-password pair or user is disabled.",
                            ));
                        }
                    }
                    i = j;
                }
                "SETNAME" => {
                    if i + 1 >= args.len() {
                        return Ok(RespValue::error(
                            "ERR Syntax error in HELLO option SETNAME",
                        ));
                    }
                    match args[i + 1].as_bulk_string() {
                        Some(name) => {
                            self.client_name = Some(name.clone());
                            i += 2;
                        }
                        None => {
                            return Ok(RespValue::error(
                                "ERR Syntax error in HELLO option SETNAME",
                            ));
                        }
                    }
                }
                _ => {
                    return Ok(RespValue::error(format!(
                        "ERR Syntax error in HELLO option '{}'",
                        opt
                    )));
                }
            }
        }

        // If password required and still not authenticated
        if !self.authenticated {
            return Ok(RespValue::error("NOAUTH Authentication required."));
        }

        self.protocol_version = proto as u8;
        // Fan-out path uses per-client protocol for push vs array.
        if let Some(id) = self.client_id {
            self.cache
                .pubsub
                .set_client_protocol(id, self.protocol_version)
                .await;
        }
        Ok(self.hello_reply(proto))
    }

    fn hello_reply(&self, proto: i64) -> RespValue {
        let version = env!("CARGO_PKG_VERSION");
        let id = self.client_id.unwrap_or(0) as i64;
        let role = self
            .persistence
            .as_ref()
            .map(|p| {
                if p.replication.is_replica() {
                    "replica"
                } else {
                    "master"
                }
            })
            .unwrap_or("master");

        let mode = if self.cluster.is_some() {
            "cluster"
        } else {
            "standalone"
        };

        if proto == 3 {
            // RESP3 map
            return RespValue::Map(vec![
                (bulk("server"), bulk("kore")),
                (bulk("version"), bulk(version)),
                (bulk("proto"), RespValue::Integer(proto)),
                (bulk("id"), RespValue::Integer(id)),
                (bulk("mode"), bulk(mode)),
                (bulk("role"), bulk(role)),
                (bulk("modules"), RespValue::Array(vec![])),
            ]);
        }

        // RESP2: flat array of key/value pairs (map-like)
        RespValue::Array(vec![
            bulk("server"),
            bulk("kore"),
            bulk("version"),
            bulk(version),
            bulk("proto"),
            RespValue::Integer(proto),
            bulk("id"),
            RespValue::Integer(id),
            bulk("mode"),
            bulk(mode),
            bulk("role"),
            bulk(role),
            bulk("modules"),
            RespValue::Array(vec![]),
        ])
    }

    /// CLIENT subcommands: ID, SETNAME, GETNAME, SETINFO, LIST, INFO
    pub(super) fn handle_client(&mut self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'client' command",
            ));
        }
        let sub = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_uppercase(),
            None => return Ok(RespValue::error("ERR unknown subcommand")),
        };

        match sub.as_str() {
            "ID" => {
                let id = self.client_id.unwrap_or(0) as i64;
                Ok(RespValue::Integer(id))
            }
            "SETNAME" => {
                if args.len() != 2 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'client|setname' command",
                    ));
                }
                match args[1].as_bulk_string() {
                    Some(name) => {
                        // Empty name clears (Redis behavior)
                        if name.is_empty() {
                            self.client_name = None;
                        } else {
                            self.client_name = Some(name.clone());
                        }
                        Ok(RespValue::ok())
                    }
                    None => Ok(RespValue::error("ERR invalid client name")),
                }
            }
            "GETNAME" => match &self.client_name {
                Some(n) => Ok(RespValue::BulkString(Some(n.clone()))),
                None => Ok(RespValue::null()),
            },
            "SETINFO" => {
                // Redis 7.2+: lib-name / lib-ver — accept and ignore for client compat
                if args.len() != 3 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'client|setinfo' command",
                    ));
                }
                Ok(RespValue::ok())
            }
            "LIST" => {
                // Single-line for this connection (full multi-client registry is future work)
                let id = self.client_id.unwrap_or(0);
                let name = self
                    .client_name
                    .as_ref()
                    .map(|n| String::from_utf8_lossy(n).into_owned())
                    .unwrap_or_default();
                let line = format!(
                    "id={} addr= name={} db={} sub={} psub=0 resp={} no-evict={} no-touch={}\n",
                    id,
                    name,
                    self.selected_db,
                    self.pubsub_subscriptions,
                    self.protocol_version,
                    if self.client_no_evict { 1 } else { 0 },
                    if self.client_no_touch { 1 } else { 0 },
                );
                Ok(bulk(line))
            }
            "INFO" => {
                let id = self.client_id.unwrap_or(0);
                let name = self
                    .client_name
                    .as_ref()
                    .map(|n| String::from_utf8_lossy(n).into_owned())
                    .unwrap_or_default();
                let info = format!(
                    "id={}\nname={}\ndb={}\nsub={}\npsub=0\nresp={}\nno-evict={}\nno-touch={}\n",
                    id,
                    name,
                    self.selected_db,
                    self.pubsub_subscriptions,
                    self.protocol_version,
                    if self.client_no_evict { 1 } else { 0 },
                    if self.client_no_touch { 1 } else { 0 },
                );
                Ok(bulk(info))
            }
            "REPLY" => self.client_reply(&args[1..]),
            "NO-EVICT" => self.client_no_evict_cmd(&args[1..]),
            "NO-TOUCH" => self.client_no_touch_cmd(&args[1..]),
            "GETREDIR" => self.client_getredir(&args[1..]),
            "TRACKINGINFO" => self.client_trackinginfo(&args[1..]),
            "KILL" | "PAUSE" | "UNPAUSE" | "TRACKING" | "CACHING" => Ok(RespValue::error(
                format!("ERR CLIENT {} is not supported yet", sub),
            )),
            _ => Ok(RespValue::error(format!(
                "ERR unknown subcommand '{}'. Try CLIENT HELP.",
                sub
            ))),
        }
    }

    /// CLIENT GETREDIR — client-side caching redirect target, or -1 when tracking is off.
    fn client_getredir(&self, args: &[RespValue]) -> Result<RespValue> {
        if !args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'client|getredir' command",
            ));
        }
        // Client tracking not implemented → always -1 (Redis: no redirect).
        Ok(RespValue::Integer(-1))
    }

    /// CLIENT TRACKINGINFO — report tracking state (off until TRACKING is implemented).
    fn client_trackinginfo(&self, args: &[RespValue]) -> Result<RespValue> {
        if !args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'client|trackinginfo' command",
            ));
        }
        // Redis returns a map/array of field-value pairs.
        Ok(RespValue::Array(vec![
            bulk("flags"),
            RespValue::Array(vec![bulk("off")]),
            bulk("redirect"),
            RespValue::Integer(-1),
            bulk("prefixes"),
            RespValue::Array(vec![]),
        ]))
    }

    /// CLIENT NO-EVICT ON|OFF
    fn client_no_evict_cmd(&mut self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'client|no-evict' command",
            ));
        }
        match parse_on_off(&args[0]) {
            Ok(on) => {
                self.client_no_evict = on;
                Ok(RespValue::ok())
            }
            Err(e) => Ok(RespValue::error(e)),
        }
    }

    /// CLIENT NO-TOUCH ON|OFF
    fn client_no_touch_cmd(&mut self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'client|no-touch' command",
            ));
        }
        match parse_on_off(&args[0]) {
            Ok(on) => {
                self.client_no_touch = on;
                Ok(RespValue::ok())
            }
            Err(e) => Ok(RespValue::error(e)),
        }
    }

    /// CLIENT REPLY ON|OFF|SKIP
    fn client_reply(&mut self, args: &[RespValue]) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'client|reply' command",
            ));
        }
        let mode = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_ascii_uppercase(),
            None => return Ok(RespValue::error("ERR syntax error")),
        };
        match mode.as_str() {
            "ON" => {
                self.client_reply_off = false;
                self.client_reply_skip = false;
                Ok(RespValue::ok())
            }
            "OFF" => {
                self.client_reply_off = true;
                Ok(RespValue::ok())
            }
            "SKIP" => {
                // Next command (not this one) will suppress its reply.
                self.client_reply_skip = true;
                Ok(RespValue::ok())
            }
            _ => Ok(RespValue::error(
                "ERR syntax error, try CLIENT (LIST | KILL | GETNAME | SETNAME | REPLY | ...)",
            )),
        }
    }

    /// COMMAND [COUNT | INFO cmd... | ...]
    pub(super) fn handle_command(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            // Full catalog
            let list: Vec<RespValue> = COMMAND_SPECS.iter().map(spec_to_reply).collect();
            return Ok(RespValue::Array(list));
        }

        let sub = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_uppercase(),
            None => return Ok(RespValue::error("ERR unknown subcommand")),
        };

        match sub.as_str() {
            "COUNT" => Ok(RespValue::Integer(COMMAND_SPECS.len() as i64)),
            "INFO" => {
                if args.len() < 2 {
                    return Ok(RespValue::Array(vec![]));
                }
                let mut out = Vec::with_capacity(args.len() - 1);
                for arg in &args[1..] {
                    let name = match arg.as_bulk_string() {
                        Some(n) => String::from_utf8_lossy(n),
                        None => {
                            out.push(RespValue::null());
                            continue;
                        }
                    };
                    match find_spec(&name) {
                        Some(spec) => out.push(spec_to_reply(spec)),
                        None => out.push(RespValue::null()),
                    }
                }
                Ok(RespValue::Array(out))
            }
            "LIST" => {
                // COMMAND LIST → array of command name bulk strings
                let names: Vec<RespValue> = COMMAND_SPECS.iter().map(|s| bulk(s.name)).collect();
                Ok(RespValue::Array(names))
            }
            "GETKEYS" => self.command_getkeys(&args[1..]),
            "GETKEYSANDFLAGS" => self.command_getkeysandflags(&args[1..]),
            "DOCS" => self.command_docs(&args[1..]),
            // Bare COMMAND with unknown first arg: try as INFO
            _ => {
                // Redis: COMMAND <name> is not valid; only subcommands
                Ok(RespValue::error(format!(
                    "ERR unknown subcommand '{}'. Try COMMAND HELP.",
                    sub
                )))
            }
        }
    }

    /// COMMAND GETKEYS <command> [arg ...] — extract key arguments via catalog specs.
    fn command_getkeys(&self, args: &[RespValue]) -> Result<RespValue> {
        match self.extract_command_keys(args) {
            Ok(keys) => Ok(RespValue::Array(
                keys.into_iter()
                    .map(|b| RespValue::BulkString(Some(b)))
                    .collect(),
            )),
            Err(e) => Ok(RespValue::error(e)),
        }
    }

    /// COMMAND GETKEYSANDFLAGS <command> [arg ...] — keys with per-key access flags.
    fn command_getkeysandflags(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'command|getkeysandflags' command",
            ));
        }
        let cmd_name = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_ascii_lowercase(),
            None => {
                return Ok(RespValue::error("ERR Invalid command specified"));
            }
        };
        let keys = match self.extract_command_keys(args) {
            Ok(k) => k,
            Err(e) => return Ok(RespValue::error(e)),
        };
        let flags = key_flags_for_command(&cmd_name);
        let out: Vec<RespValue> = keys
            .into_iter()
            .map(|k| {
                RespValue::Array(vec![
                    RespValue::BulkString(Some(k)),
                    RespValue::Array(
                        flags
                            .iter()
                            .map(|f| RespValue::BulkString(Some(Bytes::from_static(f.as_bytes()))))
                            .collect(),
                    ),
                ])
            })
            .collect();
        Ok(RespValue::Array(out))
    }

    /// COMMAND DOCS [command-name ...] — minimal docs map from the catalog.
    fn command_docs(&self, args: &[RespValue]) -> Result<RespValue> {
        let specs: Vec<&CmdSpec> = if args.is_empty() {
            COMMAND_SPECS.iter().collect()
        } else {
            let mut out = Vec::with_capacity(args.len());
            for arg in args {
                let name = match arg.as_bulk_string() {
                    Some(n) => String::from_utf8_lossy(n),
                    None => continue,
                };
                if let Some(spec) = find_spec(&name) {
                    out.push(spec);
                }
            }
            out
        };
        // RESP2 map: flat [name, doc, name, doc, ...]
        let mut reply = Vec::with_capacity(specs.len() * 2);
        for spec in specs {
            reply.push(bulk(spec.name));
            reply.push(spec_to_docs(spec));
        }
        Ok(RespValue::Array(reply))
    }

    /// Shared key extraction for GETKEYS / GETKEYSANDFLAGS.
    /// Args are `[command, arg...]`. Returns Err string on invalid command.
    fn extract_command_keys(&self, args: &[RespValue]) -> std::result::Result<Vec<Bytes>, String> {
        if args.is_empty() {
            return Err("ERR wrong number of arguments for 'command|getkeys' command".into());
        }
        let cmd_name = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_ascii_lowercase(),
            None => return Err("ERR Invalid command specified".into()),
        };
        let cmd_args = &args[1..];

        // EVAL / EVALSHA / EVAL_RO / EVALSHA_RO: keys follow numkeys.
        if cmd_name == "eval"
            || cmd_name == "evalsha"
            || cmd_name == "eval_ro"
            || cmd_name == "evalsha_ro"
        {
            return Ok(extract_eval_keys_for_getkeys(cmd_args));
        }

        // SORT key … [STORE dest] — source + optional destination.
        if cmd_name == "sort" {
            return Ok(extract_sort_keys_for_getkeys(cmd_args));
        }

        let Some(spec) = find_spec(&cmd_name) else {
            return Err("ERR Invalid command specified".into());
        };
        if spec.first_key <= 0 {
            return Ok(Vec::new());
        }
        Ok(extract_keys_from_spec(
            cmd_args,
            spec.first_key,
            spec.last_key,
            spec.step,
        ))
    }
}

/// Approximate Redis key-spec flags from command-level flags.
fn key_flags_for_command(cmd_name: &str) -> Vec<&'static str> {
    let Some(spec) = find_spec(cmd_name) else {
        return vec!["RW", "access"];
    };
    let is_write = spec.flags.iter().any(|f| *f == "write");
    let is_readonly = spec.flags.iter().any(|f| *f == "readonly");
    if is_write {
        vec!["RW", "access", "update"]
    } else if is_readonly {
        vec!["RO", "access"]
    } else {
        vec!["RW", "access"]
    }
}

/// Build a COMMAND DOCS entry (RESP2 map as flat array) from CmdSpec.
fn spec_to_docs(spec: &CmdSpec) -> RespValue {
    let flags: Vec<RespValue> = spec
        .flags
        .iter()
        .map(|f| RespValue::BulkString(Some(Bytes::from_static(f.as_bytes()))))
        .collect();
    let group = docs_group_for(spec);
    let summary = format!("{} command", spec.name);
    RespValue::Array(vec![
        bulk("summary"),
        RespValue::BulkString(Some(Bytes::from(summary))),
        bulk("group"),
        bulk(group),
        bulk("arity"),
        RespValue::Integer(spec.arity),
        bulk("flags"),
        RespValue::Array(flags),
    ])
}

fn docs_group_for(spec: &CmdSpec) -> &'static str {
    if spec.flags.iter().any(|f| *f == "admin") {
        return "server";
    }
    if spec.flags.iter().any(|f| *f == "pubsub") {
        return "pubsub";
    }
    if spec.flags.iter().any(|f| *f == "scripting") {
        return "scripting";
    }
    // Heuristic by name prefix / known families.
    let n = spec.name;
    if n.starts_with('x') && (n.starts_with("xadd") || n.starts_with("xread") || n.starts_with("xgroup")
        || n.starts_with("xinfo") || n.starts_with("xack") || n.starts_with("xclaim")
        || n.starts_with("xpending") || n.starts_with("xrange") || n.starts_with("xlen")
        || n.starts_with("xdel") || n.starts_with("xtrim") || n.starts_with("xsetid")
        || n.starts_with("xautoclaim") || n.starts_with("xrevrange"))
    {
        return "stream";
    }
    if n.starts_with('z') {
        return "sorted_set";
    }
    if n.starts_with('h') && n != "hello" {
        return "hash";
    }
    if n.starts_with('s')
        && matches!(
            n,
            "sadd"
                | "srem"
                | "smembers"
                | "sismember"
                | "scard"
                | "sinter"
                | "sunion"
                | "sdiff"
                | "sinterstore"
                | "sunionstore"
                | "sdiffstore"
                | "smove"
                | "spop"
                | "srandmember"
                | "smismember"
                | "sintercard"
        )
    {
        return "set";
    }
    if n.starts_with('l')
        || n == "rpush"
        || n == "rpop"
        || n == "rpoplpush"
        || n == "brpoplpush"
        || n == "blpop"
        || n == "brpop"
        || n == "blmove"
        || n == "lmpop"
        || n == "blmpop"
    {
        return "list";
    }
    if n.starts_with("geo") {
        return "geo";
    }
    if n.starts_with("pf") {
        return "hyperloglog";
    }
    if n.starts_with("bit") || n == "setbit" || n == "getbit" {
        return "bitmap";
    }
    if n.starts_with("eval") || n.starts_with("script") {
        return "scripting";
    }
    if matches!(
        n,
        "get" | "set"
            | "mget"
            | "mset"
            | "msetnx"
            | "append"
            | "strlen"
            | "getrange"
            | "setrange"
            | "substr"
            | "setex"
            | "psetex"
            | "getset"
            | "setnx"
            | "getdel"
            | "getex"
            | "incr"
            | "decr"
            | "incrby"
            | "decrby"
            | "incrbyfloat"
            | "lcs"
    ) {
        return "string";
    }
    "server"
}

/// Extract keys for COMMAND GETKEYS using Redis first/last/step on args after command name.
fn extract_keys_from_spec(
    args: &[RespValue],
    first_key: i64,
    last_key: i64,
    step: i64,
) -> Vec<Bytes> {
    if first_key <= 0 || args.is_empty() {
        return Vec::new();
    }
    let step = if step <= 0 { 1 } else { step as usize };
    let first = (first_key as usize).saturating_sub(1);
    if first >= args.len() {
        return Vec::new();
    }
    let last = if last_key >= 0 {
        (last_key as usize).saturating_sub(1).min(args.len() - 1)
    } else {
        let from_end = (-last_key) as usize;
        if from_end > args.len() {
            return Vec::new();
        }
        args.len() - from_end
    };
    if first > last {
        return Vec::new();
    }
    let mut keys = Vec::new();
    let mut i = first;
    while i <= last {
        if let Some(b) = args[i].as_bulk_string() {
            keys.push(b.clone());
        }
        i += step;
    }
    keys
}

/// SORT args for GETKEYS: [key, …options…, STORE dest?]
fn extract_sort_keys_for_getkeys(args: &[RespValue]) -> Vec<Bytes> {
    if args.is_empty() {
        return Vec::new();
    }
    let mut keys = Vec::new();
    if let Some(k) = args[0].as_bulk_string() {
        keys.push(k.clone());
    }
    let mut i = 1;
    while i < args.len() {
        let opt = match args[i].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_ascii_uppercase(),
            None => {
                i += 1;
                continue;
            }
        };
        match opt.as_str() {
            "STORE" => {
                if i + 1 < args.len() {
                    if let Some(d) = args[i + 1].as_bulk_string() {
                        keys.push(d.clone());
                    }
                }
                i += 2;
            }
            "BY" | "GET" => i += 2,
            "LIMIT" => i += 3,
            "ASC" | "DESC" | "ALPHA" => i += 1,
            _ => i += 1,
        }
    }
    keys
}

/// EVAL/EVALSHA args for GETKEYS: [script|sha, numkeys, key…, arg…]
fn extract_eval_keys_for_getkeys(args: &[RespValue]) -> Vec<Bytes> {
    if args.len() < 2 {
        return Vec::new();
    }
    let numkeys = match args[1].as_integer() {
        Some(n) if n >= 0 => n as usize,
        Some(_) => return Vec::new(),
        None => match args[1].as_bulk_string() {
            Some(s) => match std::str::from_utf8(s)
                .ok()
                .and_then(|t| t.parse::<i64>().ok())
            {
                Some(n) if n >= 0 => n as usize,
                _ => return Vec::new(),
            },
            None => return Vec::new(),
        },
    };
    let key_slice = &args[2..];
    if key_slice.len() < numkeys {
        return Vec::new();
    }
    key_slice[..numkeys]
        .iter()
        .filter_map(|k| k.as_bulk_string().cloned())
        .collect()
}

fn is_hello_keyword(b: &Bytes) -> bool {
    matches!(
        String::from_utf8_lossy(b).to_uppercase().as_str(),
        "AUTH" | "SETNAME"
    )
}
