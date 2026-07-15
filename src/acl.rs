//! ACL (Access Control List) — users, passwords, command & key permissions.
//!
//! MVP subset of Redis ACL: no LOAD/SAVE, DELUSER, GENPASS, or channel ACL.

use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Shared ACL state for all connections on a server.
#[derive(Debug)]
pub struct AclStore {
    inner: RwLock<AclInner>,
}

#[derive(Debug)]
struct AclInner {
    users: HashMap<String, AclUser>,
}

/// Per-user ACL configuration.
#[derive(Debug, Clone)]
pub struct AclUser {
    pub name: String,
    pub enabled: bool,
    pub nopass: bool,
    /// Plaintext passwords (MVP; Redis stores SHA256 hashes).
    pub passwords: Vec<String>,
    /// When true, all commands allowed except those in `disallowed_commands`.
    pub all_commands: bool,
    pub allowed_commands: HashSet<String>,
    pub disallowed_commands: HashSet<String>,
    /// When true, all keys allowed.
    pub all_keys: bool,
    /// Glob-style key patterns (without leading `~`).
    pub key_patterns: Vec<String>,
    /// Rule fragments for LIST / GETUSER display.
    pub command_desc: String,
    pub keys_desc: String,
}

impl AclUser {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            enabled: false,
            nopass: false,
            passwords: Vec::new(),
            all_commands: false,
            allowed_commands: HashSet::new(),
            disallowed_commands: HashSet::new(),
            all_keys: false,
            key_patterns: Vec::new(),
            command_desc: String::new(),
            keys_desc: String::new(),
        }
    }

    /// Superuser-style default: on, all commands, all keys.
    fn default_superuser(auth: &str) -> Self {
        let mut u = Self::new("default");
        u.enabled = true;
        u.all_commands = true;
        u.all_keys = true;
        u.command_desc = "+@all".to_string();
        u.keys_desc = "~*".to_string();
        if auth.is_empty() {
            u.nopass = true;
        } else {
            u.nopass = false;
            u.passwords.push(auth.to_string());
        }
        u
    }

    pub fn check_password(&self, password: &str) -> bool {
        if !self.enabled {
            return false;
        }
        if self.nopass {
            return true;
        }
        self.passwords.iter().any(|p| p == password)
    }

    pub fn can_execute(&self, cmd: &str) -> bool {
        let cmd = cmd.to_ascii_lowercase();
        if self.disallowed_commands.contains(&cmd) {
            return false;
        }
        if self.all_commands {
            return true;
        }
        self.allowed_commands.contains(&cmd)
    }

    pub fn can_access_key(&self, key: &str) -> bool {
        if self.all_keys {
            return true;
        }
        if self.key_patterns.is_empty() {
            return false;
        }
        self.key_patterns.iter().any(|p| glob_match(p, key))
    }

    /// Apply a single SETUSER rule token.
    fn apply_rule(&mut self, rule: &str) -> Result<(), String> {
        if rule.is_empty() {
            return Ok(());
        }
        match rule {
            "on" => {
                self.enabled = true;
                Ok(())
            }
            "off" => {
                self.enabled = false;
                Ok(())
            }
            "nopass" => {
                self.nopass = true;
                self.passwords.clear();
                Ok(())
            }
            "resetpass" => {
                self.nopass = false;
                self.passwords.clear();
                Ok(())
            }
            "resetkeys" => {
                self.all_keys = false;
                self.key_patterns.clear();
                self.keys_desc.clear();
                Ok(())
            }
            "allkeys" => {
                self.all_keys = true;
                self.key_patterns.clear();
                self.keys_desc = "~*".to_string();
                Ok(())
            }
            "allcommands" | "+@all" => {
                self.all_commands = true;
                self.allowed_commands.clear();
                self.disallowed_commands.clear();
                self.command_desc = "+@all".to_string();
                Ok(())
            }
            "nocommands" | "-@all" => {
                self.all_commands = false;
                self.allowed_commands.clear();
                self.disallowed_commands.clear();
                self.command_desc = "-@all".to_string();
                Ok(())
            }
            "reset" => {
                let name = self.name.clone();
                *self = Self::new(name);
                Ok(())
            }
            r if r.starts_with('>') => {
                let pass = &r[1..];
                if pass.is_empty() {
                    return Err("ERR password can't be empty".into());
                }
                self.nopass = false;
                if !self.passwords.iter().any(|p| p == pass) {
                    self.passwords.push(pass.to_string());
                }
                Ok(())
            }
            r if r.starts_with('<') => {
                // Remove password
                let pass = &r[1..];
                self.passwords.retain(|p| p != pass);
                Ok(())
            }
            r if r.starts_with('~') => {
                let pattern = &r[1..];
                if pattern == "*" {
                    self.all_keys = true;
                    self.key_patterns.clear();
                    self.keys_desc = "~*".to_string();
                } else {
                    self.all_keys = false;
                    if !self.key_patterns.iter().any(|p| p == pattern) {
                        self.key_patterns.push(pattern.to_string());
                    }
                    // Rebuild keys_desc
                    self.keys_desc = self
                        .key_patterns
                        .iter()
                        .map(|p| format!("~{}", p))
                        .collect::<Vec<_>>()
                        .join(" ");
                }
                Ok(())
            }
            r if r.starts_with("+@") => {
                let cat = &r[2..];
                let cmds = category_commands(cat)?;
                if self.all_commands {
                    // remove from disallowed
                    for c in &cmds {
                        self.disallowed_commands.remove(c);
                    }
                } else {
                    for c in cmds {
                        self.allowed_commands.insert(c);
                    }
                }
                append_cmd_desc(&mut self.command_desc, r);
                Ok(())
            }
            r if r.starts_with("-@") => {
                let cat = &r[2..];
                let cmds = category_commands(cat)?;
                if cat == "all" {
                    self.all_commands = false;
                    self.allowed_commands.clear();
                    self.disallowed_commands.clear();
                    self.command_desc = "-@all".to_string();
                    return Ok(());
                }
                if self.all_commands {
                    for c in cmds {
                        self.disallowed_commands.insert(c);
                    }
                } else {
                    for c in &cmds {
                        self.allowed_commands.remove(c);
                    }
                }
                append_cmd_desc(&mut self.command_desc, r);
                Ok(())
            }
            r if r.starts_with('+') => {
                let cmd = r[1..].to_ascii_lowercase();
                if cmd.is_empty() {
                    return Err("ERR invalid command rule".into());
                }
                self.disallowed_commands.remove(&cmd);
                if !self.all_commands {
                    self.allowed_commands.insert(cmd);
                }
                append_cmd_desc(&mut self.command_desc, r);
                Ok(())
            }
            r if r.starts_with('-') => {
                let cmd = r[1..].to_ascii_lowercase();
                if cmd.is_empty() {
                    return Err("ERR invalid command rule".into());
                }
                if self.all_commands {
                    self.disallowed_commands.insert(cmd);
                } else {
                    self.allowed_commands.remove(&cmd);
                }
                append_cmd_desc(&mut self.command_desc, r);
                Ok(())
            }
            // Skip channel rules silently for MVP (&*, resetchannels, …)
            r if r.starts_with('&') || r == "allchannels" || r == "resetchannels" || r == "sanitize-payload" || r == "skip-sanitize-payload" => {
                Ok(())
            }
            _ => Err(format!("ERR Error in ACL SETUSER modifier '{}'", rule)),
        }
    }

    pub fn to_list_entry(&self) -> String {
        let mut parts = vec![format!("user {}", self.name)];
        parts.push(if self.enabled {
            "on".into()
        } else {
            "off".into()
        });
        if self.nopass {
            parts.push("nopass".into());
        } else {
            for p in &self.passwords {
                // Redis uses hashes; we surface plaintext with > for MVP visibility in tests
                parts.push(format!(">{}", p));
            }
        }
        if self.keys_desc.is_empty() {
            if self.all_keys {
                parts.push("~*".into());
            }
        } else {
            parts.push(self.keys_desc.clone());
        }
        if self.command_desc.is_empty() {
            if self.all_commands {
                parts.push("+@all".into());
            } else {
                parts.push("-@all".into());
            }
        } else {
            parts.push(self.command_desc.clone());
        }
        parts.join(" ")
    }
}

fn append_cmd_desc(desc: &mut String, rule: &str) {
    if !desc.is_empty() {
        desc.push(' ');
    }
    desc.push_str(rule);
}

impl AclStore {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(AclInner {
                users: HashMap::new(),
            }),
        }
    }

    /// Build ACL store from `--auth` / config.auth.
    /// empty → default user nopass + all; non-empty → password required, +@all.
    pub fn from_auth(auth: &str) -> Self {
        let store = Self::new();
        {
            let mut inner = store.inner.write();
            let user = AclUser::default_superuser(auth);
            inner.users.insert("default".to_string(), user);
        }
        store
    }

    pub fn from_auth_arc(auth: &str) -> Arc<Self> {
        Arc::new(Self::from_auth(auth))
    }

    pub fn authenticate(&self, username: &str, password: &str) -> Result<(), AuthError> {
        let inner = self.inner.read();
        let user = match inner.users.get(username) {
            Some(u) => u,
            None => return Err(AuthError::WrongPass),
        };
        if !user.enabled {
            return Err(AuthError::Disabled);
        }
        if user.check_password(password) {
            Ok(())
        } else {
            Err(AuthError::WrongPass)
        }
    }

    pub fn user_enabled(&self, username: &str) -> bool {
        self.inner
            .read()
            .users
            .get(username)
            .map(|u| u.enabled)
            .unwrap_or(false)
    }

    /// Whether new connections may auto-authenticate as `default` (enabled + nopass).
    pub fn default_allows_nopass(&self) -> bool {
        self.inner
            .read()
            .users
            .get("default")
            .map(|u| u.enabled && u.nopass)
            .unwrap_or(false)
    }

    pub fn get_user(&self, username: &str) -> Option<AclUser> {
        self.inner.read().users.get(username).cloned()
    }

    pub fn can_execute(&self, username: &str, cmd: &str) -> bool {
        self.inner
            .read()
            .users
            .get(username)
            .map(|u| u.can_execute(cmd))
            .unwrap_or(false)
    }

    pub fn can_access_key(&self, username: &str, key: &str) -> bool {
        self.inner
            .read()
            .users
            .get(username)
            .map(|u| u.can_access_key(key))
            .unwrap_or(false)
    }

    /// Apply SETUSER rules. Creates the user if missing.
    pub fn setuser(&self, username: &str, rules: &[&str]) -> Result<(), String> {
        if username.is_empty() {
            return Err("ERR Username can't be empty".into());
        }
        let mut inner = self.inner.write();
        let user = inner
            .users
            .entry(username.to_string())
            .or_insert_with(|| AclUser::new(username));
        for rule in rules {
            user.apply_rule(rule)?;
        }
        Ok(())
    }

    pub fn list_users(&self) -> Vec<String> {
        let inner = self.inner.read();
        let mut names: Vec<_> = inner.users.keys().cloned().collect();
        names.sort();
        names
            .into_iter()
            .filter_map(|n| inner.users.get(&n).map(|u| u.to_list_entry()))
            .collect()
    }

    pub fn usernames(&self) -> Vec<String> {
        let inner = self.inner.read();
        let mut names: Vec<_> = inner.users.keys().cloned().collect();
        names.sort();
        names
    }
}

impl Default for AclStore {
    fn default() -> Self {
        Self::from_auth("")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthError {
    WrongPass,
    Disabled,
}

/// Glob match with `*` and `?` (Redis-style key patterns).
pub fn glob_match(pattern: &str, text: &str) -> bool {
    glob_match_bytes(pattern.as_bytes(), text.as_bytes())
}

fn glob_match_bytes(pattern: &[u8], text: &[u8]) -> bool {
    let mut pi = 0;
    let mut ti = 0;
    let mut star_pi = None;
    let mut star_ti = 0;

    while ti < text.len() {
        if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == text[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_pi = Some(pi);
            star_ti = ti;
            pi += 1;
        } else if let Some(sp) = star_pi {
            pi = sp + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }
    pi == pattern.len()
}

/// Static ACL categories for ACL CAT.
pub fn category_names() -> &'static [&'static str] {
    &[
        "keyspace",
        "read",
        "write",
        "set",
        "sortedset",
        "list",
        "hash",
        "string",
        "bitmap",
        "hyperloglog",
        "geo",
        "stream",
        "pubsub",
        "admin",
        "fast",
        "slow",
        "blocking",
        "dangerous",
        "connection",
        "transaction",
        "scripting",
        "all",
    ]
}

/// Commands belonging to a category (coarse MVP mapping).
pub fn category_commands(cat: &str) -> Result<Vec<String>, String> {
    let cat = cat.trim_start_matches('@').to_ascii_lowercase();
    let list: &[&str] = match cat.as_str() {
        "all" => {
            // Expand to union of known categories' commands used below
            return Ok(all_known_commands());
        }
        "read" => &[
            "get", "mget", "exists", "type", "strlen", "ttl", "pttl", "keys", "scan", "dbsize",
            "hget", "hmget", "hgetall", "hlen", "hexists", "hkeys", "hvals", "lrange", "llen",
            "lindex", "smembers", "sismember", "scard", "sinter", "zrange", "zrevrange", "zcard",
            "zscore", "zrank", "zrevrank", "xlen", "xrange", "xrevrange", "xread", "xpending",
            "geopos", "geodist", "geohash", "geosearch", "info", "role", "lastsave", "object",
            "memory", "dump", "strlen",
        ],
        "write" => &[
            "set", "del", "mset", "append", "setex", "getset", "unlink", "rename", "renamenx",
            "setnx", "getdel", "getex", "incr", "decr", "incrby", "decrby", "expire", "pexpire",
            "hset", "hdel", "hincrby", "lpush", "rpush", "lpop", "rpop", "blpop", "brpop", "lset",
            "sadd", "srem", "zadd", "zrem", "xadd", "xdel", "xtrim", "xgroup", "xack", "xreadgroup",
            "geoadd", "geosearchstore", "georadius", "georadiusbymember", "flushdb", "flushall",
        ],
        "admin" => &[
            "acl", "config", "save", "bgsave", "bgrewriteaof", "lastsave", "replicaof", "slaveof",
            "failover", "sync", "psync", "replconf", "client", "flushall", "flushdb", "shutdown",
            "debug", "module", "slowlog", "monitor", "command", "info", "latency",
        ],
        "dangerous" => &[
            "flushall", "flushdb", "keys", "config", "replicaof", "slaveof", "save", "shutdown",
            "debug", "acl", "migrate", "restore", "sort",
        ],
        "connection" => &[
            "auth", "hello", "ping", "echo", "quit", "reset", "select", "client", "command",
        ],
        "pubsub" => &[
            "publish", "subscribe", "unsubscribe", "psubscribe", "punsubscribe", "pubsub",
            "ssubscribe", "sunsubscribe", "spublish",
        ],
        "keyspace" => &[
            "del", "exists", "expire", "pexpire", "keys", "scan", "move", "rename", "renamenx",
            "type", "unlink", "ttl", "pttl", "dbsize", "flushdb", "flushall",
        ],
        "string" => &[
            "get", "set", "mget", "mset", "append", "strlen", "setex", "setnx", "getset", "getdel",
            "getex", "incr", "decr", "incrby", "decrby",
        ],
        "hash" => &[
            "hset", "hget", "hmget", "hdel", "hgetall", "hlen", "hexists", "hkeys", "hvals",
            "hincrby",
        ],
        "list" => &[
            "lpush", "rpush", "lpop", "rpop", "blpop", "brpop", "lrange", "llen", "lindex", "lset",
        ],
        "set" => &["sadd", "srem", "smembers", "sismember", "scard", "sinter"],
        "sortedset" => &[
            "zadd", "zrange", "zrevrange", "zcard", "zscore", "zrem", "zrank", "zrevrank",
        ],
        "stream" => &[
            "xadd", "xlen", "xrange", "xrevrange", "xdel", "xtrim", "xread", "xgroup", "xreadgroup",
            "xack", "xpending",
        ],
        "geo" => &[
            "geoadd", "geosearch", "geosearchstore", "geodist", "geopos", "geohash", "georadius",
            "georadiusbymember",
        ],
        "transaction" => &["multi", "exec", "discard", "watch", "unwatch"],
        "fast" | "slow" | "blocking" | "bitmap" | "hyperloglog" | "scripting" => &[],
        _ => return Err(format!("ERR Unknown category '{}'", cat)),
    };
    Ok(list.iter().map(|s| (*s).to_string()).collect())
}

fn all_known_commands() -> Vec<String> {
    let mut set = HashSet::new();
    for cat in [
        "read",
        "write",
        "admin",
        "connection",
        "pubsub",
        "transaction",
        "geo",
        "stream",
        "sortedset",
        "set",
        "list",
        "hash",
        "string",
        "keyspace",
        "dangerous",
    ] {
        if let Ok(cmds) = category_commands(cat) {
            set.extend(cmds);
        }
    }
    // Always include acl itself
    set.insert("acl".to_string());
    let mut v: Vec<_> = set.into_iter().collect();
    v.sort();
    v
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn glob_basics() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("allowed:*", "allowed:1"));
        assert!(!glob_match("allowed:*", "secret"));
        assert!(glob_match("pre?ix", "prefix"));
    }

    #[test]
    fn default_from_auth() {
        let open = AclStore::from_auth("");
        let u = open.get_user("default").unwrap();
        assert!(u.nopass);
        assert!(u.all_commands);
        assert!(u.check_password("anything"));

        let locked = AclStore::from_auth("secret");
        let u = locked.get_user("default").unwrap();
        assert!(!u.nopass);
        assert!(u.check_password("secret"));
        assert!(!u.check_password("wrong"));
    }
}
