//! ACL (Access Control List) — users, passwords, command, key & channel permissions.
//!
//! Subset of Redis ACL including LOAD/SAVE, DELUSER, and channel patterns.
//! GENPASS is not implemented.

use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Shared ACL state for all connections on a server.
#[derive(Debug)]
pub struct AclStore {
    inner: RwLock<AclInner>,
    /// Path used by ACL LOAD / ACL SAVE (empty = not configured).
    aclfile: RwLock<PathBuf>,
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
    /// When true, all pub/sub channels allowed.
    pub all_channels: bool,
    /// Glob-style channel patterns (without leading `&`).
    pub channel_patterns: Vec<String>,
    /// Rule fragments for LIST / GETUSER display.
    pub command_desc: String,
    pub keys_desc: String,
    pub channels_desc: String,
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
            all_channels: false,
            channel_patterns: Vec::new(),
            command_desc: String::new(),
            keys_desc: String::new(),
            channels_desc: String::new(),
        }
    }

    /// Superuser-style default: on, all commands, all keys, all channels.
    fn default_superuser(auth: &str) -> Self {
        let mut u = Self::new("default");
        u.enabled = true;
        u.all_commands = true;
        u.all_keys = true;
        u.all_channels = true;
        u.command_desc = "+@all".to_string();
        u.keys_desc = "~*".to_string();
        u.channels_desc = "&*".to_string();
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

    pub fn can_access_channel(&self, channel: &str) -> bool {
        if self.all_channels {
            return true;
        }
        if self.channel_patterns.is_empty() {
            return false;
        }
        self.channel_patterns
            .iter()
            .any(|p| glob_match(p, channel))
    }

    fn rebuild_channels_desc(&mut self) {
        if self.all_channels {
            self.channels_desc = "&*".to_string();
        } else {
            self.channels_desc = self
                .channel_patterns
                .iter()
                .map(|p| format!("&{}", p))
                .collect::<Vec<_>>()
                .join(" ");
        }
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
            "allchannels" => {
                self.all_channels = true;
                self.channel_patterns.clear();
                self.channels_desc = "&*".to_string();
                Ok(())
            }
            "resetchannels" => {
                self.all_channels = false;
                self.channel_patterns.clear();
                self.channels_desc.clear();
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
            // Payload flags are accepted and ignored (Redis compatibility).
            "sanitize-payload" | "skip-sanitize-payload" => Ok(()),
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
            r if r.starts_with('&') => {
                let pattern = &r[1..];
                if pattern == "*" {
                    self.all_channels = true;
                    self.channel_patterns.clear();
                    self.channels_desc = "&*".to_string();
                } else {
                    self.all_channels = false;
                    if !self.channel_patterns.iter().any(|p| p == pattern) {
                        self.channel_patterns.push(pattern.to_string());
                    }
                    self.rebuild_channels_desc();
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
        if self.channels_desc.is_empty() {
            if self.all_channels {
                parts.push("&*".into());
            }
        } else {
            parts.push(self.channels_desc.clone());
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
            aclfile: RwLock::new(PathBuf::new()),
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

    /// Set the ACL file path used by LOAD / SAVE.
    pub fn set_aclfile(&self, path: impl Into<PathBuf>) {
        *self.aclfile.write() = path.into();
    }

    pub fn aclfile(&self) -> PathBuf {
        self.aclfile.read().clone()
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

    pub fn can_access_channel(&self, username: &str, channel: &str) -> bool {
        self.inner
            .read()
            .users
            .get(username)
            .map(|u| u.can_access_channel(channel))
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

    /// Delete users. Returns how many were removed.
    /// The `default` user cannot be deleted (Redis behavior).
    pub fn deluser(&self, usernames: &[&str]) -> Result<usize, String> {
        let mut inner = self.inner.write();
        let mut deleted = 0usize;
        for name in usernames {
            if *name == "default" {
                return Err("ERR The 'default' user cannot be removed".into());
            }
            if name.is_empty() {
                continue;
            }
            if inner.users.remove(*name).is_some() {
                deleted += 1;
            }
        }
        Ok(deleted)
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

    /// Save ACL rules to the configured aclfile (Redis ACL SAVE format).
    pub fn save(&self) -> Result<(), String> {
        let path = self.aclfile.read().clone();
        if path.as_os_str().is_empty() {
            return Err(
                "ERR This server is not configured to use an ACL file. You may want to specify users via the ACL SETUSER command and then issue a CONFIG SET aclfile <filename> (or use Redis 6+ with aclfile in conf) / restart with --aclfile".into(),
            );
        }
        self.save_to_path(&path)
    }

    pub fn save_to_path(&self, path: &Path) -> Result<(), String> {
        let content = self.list_users().join("\n") + "\n";
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    format!("ERR Can't create ACL file directory: {}", e)
                })?;
            }
        }
        std::fs::write(path, content)
            .map_err(|e| format!("ERR There was an error trying to save the ACLs: {}", e))?;
        Ok(())
    }

    /// Load ACL rules from the configured aclfile, replacing current users.
    /// Requires a valid `default` user in the file (Redis behavior).
    pub fn load(&self) -> Result<(), String> {
        let path = self.aclfile.read().clone();
        if path.as_os_str().is_empty() {
            return Err(
                "ERR This server is not configured to use an ACL file. You may want to specify users via the ACL SETUSER command and then issue a CONFIG SET aclfile <filename> (or use Redis 6+ with aclfile in conf) / restart with --aclfile".into(),
            );
        }
        self.load_from_path(&path)
    }

    pub fn load_from_path(&self, path: &Path) -> Result<(), String> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            format!(
                "ERR / This server is not configured to use an ACL file. ({})",
                e
            )
        })?;
        self.load_from_str(&content)
    }

    /// Parse Redis-style ACL file content and replace the user table.
    pub fn load_from_str(&self, content: &str) -> Result<(), String> {
        let mut users: HashMap<String, AclUser> = HashMap::new();
        for (lineno, raw_line) in content.lines().enumerate() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let tokens: Vec<&str> = line.split_whitespace().collect();
            if tokens.is_empty() {
                continue;
            }
            if !tokens[0].eq_ignore_ascii_case("user") {
                return Err(format!(
                    "ERR Error in ACL file line {}: expected 'user'",
                    lineno + 1
                ));
            }
            if tokens.len() < 2 {
                return Err(format!(
                    "ERR Error in ACL file line {}: missing username",
                    lineno + 1
                ));
            }
            let username = tokens[1];
            let mut user = AclUser::new(username);
            for rule in &tokens[2..] {
                user.apply_rule(rule).map_err(|e| {
                    format!("ERR Error in ACL file line {}: {}", lineno + 1, e)
                })?;
            }
            users.insert(username.to_string(), user);
        }
        if !users.contains_key("default") {
            return Err(
                "ERR The ACL file doesn't contain a 'default' user definition. Aborting.".into(),
            );
        }
        *self.inner.write() = AclInner { users };
        Ok(())
    }

    /// Best-effort boot load: if aclfile is set and exists, load it.
    /// Returns Ok(false) when skipped (empty path or missing file).
    pub fn try_load_on_boot(&self) -> Result<bool, String> {
        let path = self.aclfile.read().clone();
        if path.as_os_str().is_empty() {
            return Ok(false);
        }
        if !path.exists() {
            return Ok(false);
        }
        self.load_from_path(&path)?;
        Ok(true)
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
            "get", "mget", "exists", "type", "strlen", "getrange", "ttl", "pttl", "keys", "scan", "dbsize",
            "hget", "hmget", "hgetall", "hlen", "hexists", "hkeys", "hvals", "lrange", "llen",
            "lindex", "lpos", "smembers", "sismember", "scard", "sinter", "sunion", "sdiff", "srandmember", "sscan",
            "zrange", "zrevrange", "zcard",
            "zscore", "zrank", "zrevrank", "zrangebyscore", "zrevrangebyscore", "zcount", "zscan",
            "hscan",
            "xlen", "xrange", "xrevrange", "xread", "xpending",
            "geopos", "geodist", "geohash", "geosearch", "info", "role", "lastsave", "object",
            "memory", "dump", "strlen", "getbit", "bitcount", "bitpos", "pfcount",
        ],
        "write" => &[
            "set", "del", "mset", "msetnx", "append", "setrange", "setex", "getset", "unlink", "rename", "renamenx",
            "setnx", "getdel", "getex", "incr", "decr", "incrby", "decrby", "expire", "pexpire", "expireat", "pexpireat", "persist", "expiretime", "pexpiretime",
            "hset", "hdel", "hincrby", "lpush", "rpush", "lpop", "rpop", "blpop", "brpop", "lset",
            "lrem", "ltrim", "linsert", "lmove", "blmove",
            "sadd", "srem", "sinterstore", "sunionstore", "sdiffstore", "smove", "spop",
            "zadd", "zrem", "zincrby", "zremrangebyrank", "zremrangebyscore",
            "zunionstore", "zinterstore",
            "xadd", "xdel", "xtrim", "xgroup", "xack", "xreadgroup",
            "geoadd", "geosearchstore", "georadius", "georadiusbymember", "flushdb", "flushall",
            "setbit", "bitop", "bitfield", "pfadd", "pfmerge",
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
            "del", "exists", "expire", "pexpire", "expireat", "pexpireat", "persist", "expiretime", "pexpiretime", "keys", "scan", "move", "copy", "randomkey", "touch", "rename", "renamenx",
            "type", "unlink", "ttl", "pttl", "dbsize", "flushdb", "flushall",
        ],
        "string" => &[
            "get", "set", "mget", "mset", "msetnx", "append", "strlen", "getrange", "setrange",
            "setex", "setnx", "getset", "getdel",
            "getex", "incr", "decr", "incrby", "decrby",
        ],
        "bitmap" => &[
            "setbit", "getbit", "bitcount", "bitpos", "bitop", "bitfield",
        ],
        "hyperloglog" => &["pfadd", "pfcount", "pfmerge"],
        "hash" => &[
            "hset", "hget", "hmget", "hdel", "hgetall", "hlen", "hexists", "hkeys", "hvals",
            "hincrby", "hscan",
        ],
        "list" => &[
            "lpush", "rpush", "lpop", "rpop", "blpop", "brpop", "lrange", "llen", "lindex", "lset",
            "lrem", "ltrim", "linsert", "lpos", "lmove", "blmove",
        ],
        "set" => &[
            "sadd", "srem", "smembers", "sismember", "scard", "sinter", "sunion", "sdiff",
            "sinterstore", "sunionstore", "sdiffstore", "smove", "spop", "srandmember", "sscan",
        ],
        "sortedset" => &[
            "zadd", "zrange", "zrevrange", "zcard", "zscore", "zrem", "zrank", "zrevrank",
            "zincrby", "zrangebyscore", "zrevrangebyscore", "zcount",
            "zremrangebyrank", "zremrangebyscore", "zscan",
            "zunionstore", "zinterstore",
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
        "scripting" => &["eval", "evalsha", "script"],
        "fast" | "slow" | "blocking" => &[],
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
        "bitmap",
        "hyperloglog",
        "keyspace",
        "dangerous",
        "scripting",
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
