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
    CmdSpec { name: "auth", arity: -2, flags: &["noscript", "loading", "stale", "fast", "no_auth", "ok_loading"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "quit", arity: -1, flags: &["admin", "noscript", "loading", "stale", "fast"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "reset", arity: 1, flags: &["noscript", "loading", "stale", "fast"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "hello", arity: -1, flags: &["noscript", "loading", "stale", "fast", "no_auth"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "command", arity: -1, flags: &["loading", "stale", "random"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "client", arity: -2, flags: &["admin", "noscript", "loading", "stale", "random"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "acl", arity: -2, flags: &["admin", "noscript", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "cluster", arity: -2, flags: &["admin", "random", "stale"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "asking", arity: 1, flags: &["fast"], first_key: 0, last_key: 0, step: 0 },
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
    CmdSpec { name: "getrange", arity: 4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "setrange", arity: 4, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "setex", arity: 4, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "getset", arity: 3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
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
    CmdSpec { name: "dbsize", arity: 1, flags: &["readonly", "fast"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "keys", arity: 2, flags: &["readonly", "sort_for_script"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "scan", arity: -2, flags: &["readonly", "random"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "flushdb", arity: -1, flags: &["write"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "flushall", arity: -1, flags: &["write"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "info", arity: -1, flags: &["loading", "stale", "random"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "health", arity: -1, flags: &["loading", "stale", "fast", "random"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "config", arity: -2, flags: &["admin", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
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
    CmdSpec { name: "hget", arity: 3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hmget", arity: -3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hdel", arity: -3, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hgetall", arity: 2, flags: &["readonly", "random"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hlen", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hexists", arity: 3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hkeys", arity: 2, flags: &["readonly", "sort_for_script"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hvals", arity: 2, flags: &["readonly", "sort_for_script"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hincrby", arity: 4, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hincrbyfloat", arity: 4, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hstrlen", arity: 3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hmset", arity: -4, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "hscan", arity: -3, flags: &["readonly", "random"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "lpush", arity: -3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "rpush", arity: -3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
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
    CmdSpec { name: "sadd", arity: -3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "srem", arity: -3, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "smembers", arity: 2, flags: &["readonly", "sort_for_script"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "sismember", arity: 3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "scard", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "sinter", arity: -2, flags: &["readonly", "sort_for_script"], first_key: 1, last_key: -1, step: 1 },
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
    CmdSpec { name: "zadd", arity: -4, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zrange", arity: -4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zrevrange", arity: -4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zcard", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zscore", arity: 3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zrem", arity: -3, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zrank", arity: -3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zrevrank", arity: -3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zincrby", arity: 4, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zrangebyscore", arity: -4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zrevrangebyscore", arity: -4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zcount", arity: 4, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zremrangebyrank", arity: 4, flags: &["write"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zremrangebyscore", arity: 4, flags: &["write"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zscan", arity: -3, flags: &["readonly", "random"], first_key: 1, last_key: 1, step: 1 },
    // numkeys + optional WEIGHTS/AGGREGATE make full key ranges movable; expose dest only.
    CmdSpec { name: "zunionstore", arity: -4, flags: &["write", "denyoom", "movablekeys"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "zinterstore", arity: -4, flags: &["write", "denyoom", "movablekeys"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "publish", arity: 3, flags: &["pubsub", "loading", "stale", "fast"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "subscribe", arity: -2, flags: &["pubsub", "noscript", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "unsubscribe", arity: -1, flags: &["pubsub", "noscript", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "psubscribe", arity: -2, flags: &["pubsub", "noscript", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "punsubscribe", arity: -1, flags: &["pubsub", "noscript", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "pubsub", arity: -2, flags: &["pubsub", "random", "loading", "stale"], first_key: 0, last_key: 0, step: 0 },
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
    CmdSpec { name: "pfadd", arity: -2, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CmdSpec { name: "pfcount", arity: -2, flags: &["readonly", "random"], first_key: 1, last_key: -1, step: 1 },
    CmdSpec { name: "pfmerge", arity: -2, flags: &["write", "denyoom"], first_key: 1, last_key: -1, step: 1 },
    // Lua scripting (keys are dynamic via numkeys; movablekeys in full Redis)
    CmdSpec { name: "eval", arity: -3, flags: &["noscript", "movablekeys"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "evalsha", arity: -3, flags: &["noscript", "movablekeys"], first_key: 0, last_key: 0, step: 0 },
    CmdSpec { name: "script", arity: -2, flags: &["noscript"], first_key: 0, last_key: 0, step: 0 },
];

fn bulk(s: impl Into<Bytes>) -> RespValue {
    RespValue::BulkString(Some(s.into()))
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
                    "id={} addr= name={} db=0 sub={} psub=0\n",
                    id, name, self.pubsub_subscriptions
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
                    "id={}\nname={}\ndb=0\nsub={}\npsub=0\nresp={}\n",
                    id,
                    name,
                    self.pubsub_subscriptions,
                    self.protocol_version
                );
                Ok(bulk(info))
            }
            "KILL" | "PAUSE" | "UNPAUSE" | "REPLY" | "NO-EVICT" | "NO-TOUCH" | "TRACKING"
            | "CACHING" | "GETREDIR" | "TRACKINGINFO" => Ok(RespValue::error(format!(
                "ERR CLIENT {} is not supported yet",
                sub
            ))),
            _ => Ok(RespValue::error(format!(
                "ERR unknown subcommand '{}'. Try CLIENT HELP.",
                sub
            ))),
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
            "DOCS" | "GETKEYS" | "GETKEYSANDFLAGS" => Ok(RespValue::error(format!(
                "ERR COMMAND {} is not supported yet",
                sub
            ))),
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
}

fn is_hello_keyword(b: &Bytes) -> bool {
    matches!(
        String::from_utf8_lossy(b).to_uppercase().as_str(),
        "AUTH" | "SETNAME"
    )
}
