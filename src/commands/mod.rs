mod basic;
mod key_value;
mod counter;
mod expiration;
mod admin;
mod sorted_set;
mod geospatial;
mod hash;
mod list;
mod set_cmds;
mod stream;
mod pubsub;
mod search;
mod persistence;
mod transaction;
mod meta;
mod acl;
mod cluster;
mod bitmap;
mod hyperloglog;
mod scripting;

use crate::acl::AclStore;
use crate::cache::Cache;
use crate::cluster::{key_hash_slot, ClusterState};
use crate::config::Config;
use crate::databases::Databases;
use crate::error::{Error, Result};
use crate::persistence::PersistenceManager;
use crate::protocol::RespValue;
use crate::pubsub::ClientId;
use crate::redlock::Redlock;
use crate::scripting::ScriptCache;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct CommandHandler {
    /// Currently selected logical database keyspace.
    cache: Arc<Cache>,
    /// All logical databases (SELECT target).
    databases: Arc<Databases>,
    /// Index of the currently selected DB (0-based).
    selected_db: usize,
    config: Arc<Config>,
    persistence: Option<Arc<PersistenceManager>>,
    /// Shared ACL user store (server-wide).
    acl: Arc<AclStore>,
    authenticated: bool,
    /// Authenticated ACL username (e.g. "default").
    username: Option<String>,
    client_id: Option<ClientId>,
    /// Number of active regular pub/sub subscriptions (channels + patterns).
    pubsub_subscriptions: usize,
    /// Number of active shard channel subscriptions (Redis 7.0+ Shard Pub/Sub).
    /// Tracked separately to avoid the double-counting bug (CR-2).
    shard_subscriptions: usize,
    /// After SYNC: network drains this channel to the replica socket.
    pub pending_replica_feed: Option<mpsc::Receiver<Bytes>>,
    /// After SYNC: pre-serialized RESP response (RDB bulk string).
    pub pending_raw_response: Option<Bytes>,
    /// Inside MULTI … EXEC block.
    in_multi: bool,
    /// True while EXEC is replaying the queue (bypass re-queue).
    executing_multi: bool,
    /// Queue-time error occurred; EXEC will EXECABORT.
    multi_aborted: bool,
    /// Queued full command arrays while in MULTI.
    multi_queue: Vec<RespValue>,
    /// WATCH'd keys → generation at WATCH time (current DB only).
    watched: HashMap<Bytes, u64>,
    /// CLIENT SETNAME / HELLO SETNAME connection name.
    client_name: Option<Bytes>,
    /// Pending REPLCONF ip-address for the next SYNC/PSYNC on this connection.
    replica_announce_ip: Option<String>,
    /// Pending REPLCONF listening-port for the next SYNC/PSYNC on this connection.
    replica_announce_port: Option<u16>,
    /// Cluster topology when `--cluster-enabled` (shared across connections).
    cluster: Option<Arc<ClusterState>>,
    /// ASKING one-shot flag (allows next command against IMPORTING slots).
    asking: bool,
    /// Negotiated RESP protocol version (2 default; 3 after HELLO 3).
    protocol_version: u8,
    /// Optional Redlock (for INFO fair-queue metrics).
    redlock: Option<Arc<Redlock>>,
    /// Shared SCRIPT LOAD / EVALSHA cache (server-wide).
    script_cache: Arc<ScriptCache>,
    /// CLIENT REPLY OFF — suppress all replies until ON.
    client_reply_off: bool,
    /// CLIENT REPLY SKIP — suppress the next command's reply once.
    client_reply_skip: bool,
    /// After `handle`, network checks this to omit writing a response.
    suppress_reply: bool,
    /// READONLY mode for cluster replica reads on this connection.
    cluster_readonly: bool,
    /// CLIENT NO-EVICT ON — keys written by this client prefer not to be evicted (flag stored).
    client_no_evict: bool,
    /// CLIENT NO-TOUCH ON — reads do not update LRU/LFU idle times.
    client_no_touch: bool,
    /// CLIENT TRACKING ON — client-side caching enabled (no invalidation push yet).
    client_tracking: bool,
    /// CLIENT TRACKING REDIRECT target client id (-1 = none).
    client_tracking_redirect: i64,
    /// CLIENT TRACKING PREFIX list.
    client_tracking_prefixes: Vec<Bytes>,
    /// CLIENT TRACKING BCAST mode.
    client_tracking_bcast: bool,
    /// CLIENT TRACKING OPTIN mode.
    client_tracking_optin: bool,
    /// CLIENT TRACKING OPTOUT mode.
    client_tracking_optout: bool,
    /// CLIENT TRACKING NOLOOP mode.
    client_tracking_noloop: bool,
    /// CLIENT CACHING YES|NO (opt-in/opt-out next-command flag).
    client_caching: Option<bool>,
    /// After writing the reply, network should close this connection (QUIT / SHUTDOWN / KILL).
    close_after_reply: bool,
    /// Optional server-wide shutdown trigger (SHUTDOWN command).
    shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
    /// When SHUTDOWN NOSAVE is used, set so the accept loop skips SAVE.
    shutdown_nosave: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
}

impl CommandHandler {
    pub fn new(cache: Arc<Cache>, config: Arc<Config>) -> Self {
        Self::with_persistence(cache, config, None)
    }

    pub fn with_persistence(
        cache: Arc<Cache>,
        config: Arc<Config>,
        persistence: Option<Arc<PersistenceManager>>,
    ) -> Self {
        Self::with_databases(Databases::single(cache), config, persistence)
    }

    /// Build a handler over a multi-DB set (starts on DB 0).
    pub fn with_databases(
        databases: Arc<Databases>,
        config: Arc<Config>,
        persistence: Option<Arc<PersistenceManager>>,
    ) -> Self {
        let acl = AclStore::from_auth_arc(&config.auth);
        Self::with_databases_and_acl(databases, config, persistence, acl)
    }

    /// Build a handler with a shared ACL store (all connections on a server).
    pub fn with_databases_and_acl(
        databases: Arc<Databases>,
        config: Arc<Config>,
        persistence: Option<Arc<PersistenceManager>>,
        acl: Arc<AclStore>,
    ) -> Self {
        // Wire configured ACL file path (LOAD/SAVE) without clobbering a path already set.
        if !config.aclfile.is_empty() && acl.aclfile().as_os_str().is_empty() {
            acl.set_aclfile(&config.aclfile);
        }
        // Auto-auth as default only when live ACL allows nopass (not just startup --auth).
        let authenticated = acl.default_allows_nopass();
        let username = if authenticated {
            Some("default".to_string())
        } else {
            None
        };
        let cache = databases.db0();
        Self {
            cache,
            databases,
            selected_db: 0,
            config,
            persistence,
            acl,
            authenticated,
            username,
            client_id: None,
            pubsub_subscriptions: 0,
            shard_subscriptions: 0,
            pending_replica_feed: None,
            pending_raw_response: None,
            in_multi: false,
            executing_multi: false,
            multi_aborted: false,
            multi_queue: Vec::new(),
            watched: HashMap::new(),
            client_name: None,
            replica_announce_ip: None,
            replica_announce_port: None,
            cluster: None,
            asking: false,
            protocol_version: 2,
            redlock: None,
            script_cache: ScriptCache::shared(),
            client_reply_off: false,
            client_reply_skip: false,
            suppress_reply: false,
            cluster_readonly: false,
            client_no_evict: false,
            client_no_touch: false,
            client_tracking: false,
            client_tracking_redirect: -1,
            client_tracking_prefixes: Vec::new(),
            client_tracking_bcast: false,
            client_tracking_optin: false,
            client_tracking_optout: false,
            client_tracking_noloop: false,
            client_caching: None,
            close_after_reply: false,
            shutdown_tx: None,
            shutdown_nosave: None,
        }
    }

    /// Whether the network layer should close the connection after the current reply.
    pub fn take_close_after_reply(&mut self) -> bool {
        let c = self.close_after_reply;
        self.close_after_reply = false;
        c
    }

    /// Wire the process-wide shutdown channel (server accept loop).
    pub fn with_shutdown(
        mut self,
        shutdown_tx: tokio::sync::watch::Sender<bool>,
        nosave: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        self.shutdown_tx = Some(shutdown_tx);
        self.shutdown_nosave = Some(nosave);
        self
    }

    /// Load options honoring CLIENT NO-TOUCH.
    pub(crate) fn load_options(&self) -> crate::entry::LoadOptions {
        crate::entry::LoadOptions {
            touch: !self.client_no_touch,
            with_cas: false,
        }
    }

    /// Whether the last `handle` result should be omitted from the wire (CLIENT REPLY).
    pub fn take_suppress_reply(&mut self) -> bool {
        let s = self.suppress_reply;
        self.suppress_reply = false;
        s
    }

    /// Attach Redlock for INFO metrics (fair queue stats).
    pub fn with_redlock(mut self, redlock: Option<Arc<Redlock>>) -> Self {
        self.redlock = redlock;
        self
    }

    /// Share a SCRIPT LOAD cache across connections (server path).
    pub fn with_script_cache(mut self, script_cache: Arc<ScriptCache>) -> Self {
        self.script_cache = script_cache;
        self
    }

    /// Shared Lua script cache.
    pub fn script_cache(&self) -> &Arc<ScriptCache> {
        &self.script_cache
    }

    /// Redlock reference if present.
    pub fn redlock(&self) -> Option<&Arc<Redlock>> {
        self.redlock.as_ref()
    }

    /// Current RESP protocol version (2 or 3).
    pub fn protocol_version(&self) -> u8 {
        self.protocol_version
    }

    /// CONFIG GET style key/value reply: flat array (RESP2) or map (RESP3).
    pub(super) fn config_kv_reply(&self, key: &str, value: &str) -> RespValue {
        self.config_kvs_reply(vec![(key.to_string(), value.to_string())])
    }

    /// Multi-pair CONFIG GET reply (RESP2 flat array or RESP3 map).
    pub(super) fn config_kvs_reply(&self, pairs: Vec<(String, String)>) -> RespValue {
        if self.protocol_version >= 3 {
            let map: Vec<(RespValue, RespValue)> = pairs
                .into_iter()
                .map(|(k, v)| {
                    (
                        RespValue::BulkString(Some(Bytes::from(k))),
                        RespValue::BulkString(Some(Bytes::from(v))),
                    )
                })
                .collect();
            RespValue::Map(map)
        } else {
            let mut arr = Vec::with_capacity(pairs.len() * 2);
            for (k, v) in pairs {
                arr.push(RespValue::BulkString(Some(Bytes::from(k))));
                arr.push(RespValue::BulkString(Some(Bytes::from(v))));
            }
            RespValue::Array(arr)
        }
    }

    /// Snapshot of supported CONFIG parameters (canonical names).
    pub(super) fn config_known_params(&self) -> Vec<(String, String)> {
        let save = self
            .persistence
            .as_ref()
            .map(|p| p.save_rules_string())
            .unwrap_or_default();
        let min_replicas = self
            .persistence
            .as_ref()
            .map(|p| p.replication.min_replicas_to_write())
            .unwrap_or(0);
        let min_lag = self
            .persistence
            .as_ref()
            .map(|p| p.replication.min_replicas_max_lag())
            .unwrap_or(10);
        let appendonly = self
            .persistence
            .as_ref()
            .map(|p| p.appendonly())
            .unwrap_or(self.config.appendonly);
        vec![
            (
                "maxentrysize".into(),
                self.cache.get_max_entry_size().to_string(),
            ),
            ("maxmemory".into(), self.cache.max_memory().to_string()),
            ("save".into(), save),
            (
                "maxmemory-policy".into(),
                self.cache.eviction_policy().as_str().to_string(),
            ),
            (
                "lfu-log-factor".into(),
                self.cache.lfu_log_factor().to_string(),
            ),
            (
                "lfu-decay-time".into(),
                self.cache.lfu_decay_time().to_string(),
            ),
            (
                "slowlog-log-slower-than".into(),
                self.cache.slowlog.slower_than_us().to_string(),
            ),
            (
                "slowlog-max-len".into(),
                self.cache.slowlog.max_len().to_string(),
            ),
            (
                "acllog-max-len".into(),
                self.cache.acl_log.max_len().to_string(),
            ),
            ("databases".into(), self.databases.len().to_string()),
            (
                "min-replicas-to-write".into(),
                min_replicas.to_string(),
            ),
            (
                "min-replicas-max-lag".into(),
                min_lag.to_string(),
            ),
            // Ops / networking / persistence paths (CONFIG GET read-only snapshot)
            ("port".into(), self.config.port.to_string()),
            ("bind".into(), self.config.host.clone()),
            ("dir".into(), self.config.dir.clone()),
            ("dbfilename".into(), self.config.dbfilename.clone()),
            (
                "appendonly".into(),
                if appendonly { "yes".into() } else { "no".into() },
            ),
            (
                "appendfilename".into(),
                self.config.appendfilename.clone(),
            ),
            (
                "unixsocket".into(),
                self.config.unixsocket.clone(),
            ),
            (
                "cluster-enabled".into(),
                if self.config.cluster_enabled {
                    "yes".into()
                } else {
                    "no".into()
                },
            ),
        ]
    }

    /// True if `pattern` matches the canonical CONFIG name or a known alias.
    pub(super) fn config_param_matches(pattern: &str, canonical: &str) -> bool {
        if crate::hashmap::pattern_match(pattern, canonical) {
            return true;
        }
        for alias in config_param_aliases(canonical) {
            if crate::hashmap::pattern_match(pattern, alias) {
                return true;
            }
        }
        false
    }
}

/// Alternate names accepted by CONFIG GET (reply always uses the canonical key).
fn config_param_aliases(canonical: &str) -> &'static [&'static str] {
    match canonical {
        "maxentrysize" => &["max-entry-size"],
        "maxmemory" => &["max-memory"],
        "maxmemory-policy" => &["maxmemory_policy"],
        "lfu-log-factor" => &["lfu_log_factor"],
        "lfu-decay-time" => &["lfu_decay_time"],
        "slowlog-log-slower-than" => &["slowlog_log_slower_than"],
        "slowlog-max-len" => &["slowlog_max_len"],
        "acllog-max-len" => &["acllog_max_len", "acl-log-max-len"],
        "min-replicas-to-write" => &["min-slaves-to-write"],
        "min-replicas-max-lag" => &["min-slaves-max-lag"],
        "bind" => &["host"],
        "cluster-enabled" => &["cluster_enabled"],
        "appendfilename" => &["append-filename"],
        "dbfilename" => &["db-filename"],
        "unixsocket" => &["unix-socket"],
        _ => &[],
    }
}

// Keep methods on CommandHandler contiguous for the rest of the impl.
impl CommandHandler {
    /// Convert subscribe confirmation frames to Push when on RESP3.
    fn maybe_pubsub_push_frames(&self, responses: Vec<RespValue>) -> Vec<RespValue> {
        if self.protocol_version < 3 {
            return responses;
        }
        responses
            .into_iter()
            .map(|r| match r {
                RespValue::Array(a) => RespValue::Push(a),
                other => other,
            })
            .collect()
    }

    /// Attach cluster state (server path when `--cluster-enabled`).
    pub fn with_cluster(mut self, cluster: Option<Arc<ClusterState>>) -> Self {
        self.cluster = cluster;
        self
    }

    /// Shared cluster state, if any.
    pub fn cluster(&self) -> Option<&Arc<ClusterState>> {
        self.cluster.as_ref()
    }

    /// Currently selected database index.
    pub fn selected_db(&self) -> usize {
        self.selected_db
    }

    /// Access the multi-DB collection.
    pub fn databases(&self) -> &Arc<Databases> {
        &self.databases
    }

    pub fn set_client_id(&mut self, client_id: ClientId) {
        self.client_id = Some(client_id);
    }

    pub fn client_id(&self) -> Option<ClientId> {
        self.client_id
    }

    /// Take pending raw response (SYNC).
    pub fn take_raw_response(&mut self) -> Option<Bytes> {
        self.pending_raw_response.take()
    }

    /// Take pending replica feed receiver (SYNC).
    pub fn take_replica_feed(&mut self) -> Option<mpsc::Receiver<Bytes>> {
        self.pending_replica_feed.take()
    }

    /// Shared persistence manager for this connection (if configured).
    pub fn persistence(&self) -> Option<&Arc<PersistenceManager>> {
        self.persistence.as_ref()
    }

    /// Pending `REPLCONF ip-address` for the next SYNC/PSYNC on this connection.
    pub fn replica_announce_ip(&self) -> Option<&str> {
        self.replica_announce_ip.as_deref()
    }

    /// Pending `REPLCONF listening-port` for the next SYNC/PSYNC on this connection.
    pub fn replica_announce_port(&self) -> Option<u16> {
        self.replica_announce_port
    }

    /// Log write to AOF + replicas when the command mutated data successfully.
    fn maybe_persist_write(&self, cmd: &str, args: &[RespValue], response: &RespValue) {
        if !is_write_command(cmd) {
            return;
        }
        if !response_indicates_success(response) {
            return;
        }
        // Replicas should not re-persist/re-propagate (avoid loops)
        if let Some(p) = self.persistence.as_ref() {
            if p.replication.is_replica() {
                return;
            }
            let mut argv = Vec::with_capacity(args.len() + 1);
            argv.push(Bytes::from(cmd.to_string()));
            for a in args {
                if let Some(b) = a.as_bulk_string() {
                    argv.push(b.clone());
                } else if let Some(i) = a.as_integer() {
                    argv.push(Bytes::from(i.to_string()));
                }
            }
            p.on_write_command(self.selected_db, &argv);
        }
    }

    /// Returns true when the client is in Pub/Sub mode (regular or shard subscriptions).
    fn in_pubsub_mode(&self) -> bool {
        self.pubsub_subscriptions > 0 || self.shard_subscriptions > 0
    }

    pub async fn handle(&mut self, value: RespValue) -> Result<RespValue> {
        self.suppress_reply = false;
        let handle_start = std::time::Instant::now();

        let args = match value.as_array() {
            Some(arr) => arr,
            None => return Ok(RespValue::error("ERR invalid command format")),
        };

        if args.is_empty() {
            return Ok(RespValue::error("ERR empty command"));
        }

        let cmd = match args[0].as_bulk_string() {
            Some(s) => s,
            None => return Ok(RespValue::error("ERR invalid command")),
        };

        // Stack buffer for ASCII uppercase — avoids heap alloc on the hot path.
        let mut cmd_buf = [0u8; 64];
        let cmd_upper_cow = ascii_uppercase_cmd(cmd, &mut cmd_buf);
        let cmd_upper = cmd_upper_cow.as_ref();

        // CLIENT REPLY: decide suppress before any early return (except CLIENT REPLY itself).
        let is_client_reply = cmd_upper == "CLIENT"
            && args
                .get(1)
                .and_then(|a| a.as_bulk_string())
                .map(|s| s.eq_ignore_ascii_case(b"REPLY"))
                .unwrap_or(false);
        if !is_client_reply {
            if self.client_reply_off {
                self.suppress_reply = true;
            } else if self.client_reply_skip {
                self.client_reply_skip = false;
                self.suppress_reply = true;
            }
        }

        // AUTH / HELLO don't require prior authentication (HELLO may AUTH inline)
        if cmd_upper == "AUTH" {
            return self.handle_auth(&args[1..]);
        }
        if cmd_upper == "HELLO" {
            return self.handle_hello(&args[1..]).await;
        }

        // Check authentication
        if !self.authenticated {
            return Ok(RespValue::error("NOAUTH Authentication required"));
        }

        // ACL: command + key permission checks (after auth)
        if let Some(deny) = self.check_acl_permission(cmd_upper, &args[1..]) {
            return Ok(deny);
        }

        // ASKING one-shot: capture flag for this command, then clear (except ASKING itself).
        let asking_flag = self.asking;
        if cmd_upper != "ASKING" {
            self.asking = false;
        }

        // Cluster gate (after ACL): CROSSSLOT / MOVED / ASK
        if let Some(redir) =
            self.check_cluster_redirect(cmd_upper, &args[1..], asking_flag)
        {
            return Ok(redir);
        }

        // ── Transaction control (always immediate; never queued) ───────────────
        match cmd_upper {
            "MULTI" => return self.handle_multi(),
            "EXEC" => return self.handle_exec().await,
            "DISCARD" => return self.handle_discard(),
            "WATCH" => return self.handle_watch(&args[1..]),
            "UNWATCH" => return self.handle_unwatch(),
            _ => {}
        }

        // Reject writes on readonly replica (except SYNC is primary-only).
        // SORT is a write only when STORE is present (Redis replica semantics).
        let is_write = is_write_command(cmd_upper)
            || (cmd_upper == "SORT" && sort_has_store(&args[1..]));
        if is_write {
            if let Some(p) = self.persistence.as_ref() {
                if p.replication.readonly() {
                    return Ok(RespValue::error(
                        "READONLY You can't write against a read only replica.",
                    ));
                }
                // min-replicas-to-write: refuse when not enough fresh replicas.
                if !p.replication.writes_allowed_by_min_replicas() {
                    return Ok(RespValue::error(
                        "NOREPLICAS Not enough good replicas to write.",
                    ));
                }
            }
        }

        // Queue non-control commands while inside MULTI (unless replaying EXEC).
        if self.in_multi && !self.executing_multi {
            return self.queue_multi_command(cmd_upper, value);
        }

        // ── Pub/Sub mode enforcement (Redis spec) ──────────────────────────────
        // Once a client has at least one active subscription only the listed
        // commands are accepted.  PING has a special array-reply in this mode.
        if self.in_pubsub_mode() {
            match cmd_upper {
                "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE"
                | "SSUBSCRIBE" | "SUNSUBSCRIBE"
                | "RESET" | "QUIT" => {}
                "PING" => {
                    // In Pub/Sub mode PING returns *2\r\n$4\r\npong\r\n$<n>\r\n<msg>\r\n
                    let msg = args.get(1)
                        .and_then(|v| v.as_bulk_string())
                        .cloned()
                        .unwrap_or_default();
                    return Ok(RespValue::Array(vec![
                        RespValue::BulkString(Some(bytes::Bytes::from_static(b"pong"))),
                        RespValue::BulkString(Some(msg)),
                    ]));
                }
                _ => {
                    return Ok(RespValue::error(
                        "ERR Command not allowed inside a subscribed context. \
                         Did you mean SUBSCRIBE / PSUBSCRIBE?",
                    ));
                }
            }
        }

        let result = match cmd_upper {
            // Basic commands
            "PING" => self.handle_ping(&args[1..]),
            "ECHO" => self.handle_echo(&args[1..]),
            "TIME" => self.handle_time(&args[1..]),
            "LOLWUT" => self.handle_lolwut(&args[1..]),
            "READONLY" => self.handle_readonly(&args[1..]),
            "READWRITE" => self.handle_readwrite(&args[1..]),
            "QUIT" => {
                self.close_after_reply = true;
                Ok(RespValue::ok())
            }
            "SHUTDOWN" => self.handle_shutdown(&args[1..]),
            "DEBUG" => self.handle_debug(&args[1..]),

            // Client handshake / introspection
            "CLIENT" => self.handle_client(&args[1..]),
            "COMMAND" => self.handle_command(&args[1..]),
            "ACL" => self.handle_acl(&args[1..]),
            // Cluster
            "CLUSTER" => self.handle_cluster(&args[1..]).await,
            "ASKING" => self.handle_asking(&args[1..]),
            // HELLO handled before auth gate

            // RESET: exit pub/sub mode, multi, and watches (Redis 6.2+)
            // Also re-selects DB 0 (Redis RESET behavior).
            "RESET" => {
                self.pubsub_subscriptions = 0;
                self.shard_subscriptions = 0;
                self.in_multi = false;
                self.multi_aborted = false;
                self.multi_queue.clear();
                self.clear_watches();
                self.client_name = None;
                self.selected_db = 0;
                self.cache = self.databases.db0();
                self.protocol_version = 2;
                self.asking = false;
                self.cluster_readonly = false;
                self.client_no_evict = false;
                self.client_no_touch = false;
                self.client_tracking = false;
                self.client_tracking_redirect = -1;
                self.client_tracking_prefixes.clear();
                self.client_tracking_bcast = false;
                self.client_tracking_optin = false;
                self.client_tracking_optout = false;
                self.client_tracking_noloop = false;
                self.client_caching = None;
                if let Some(id) = self.client_id {
                    self.cache.pubsub.set_client_protocol(id, 2).await;
                }
                Ok(RespValue::SimpleString(bytes::Bytes::from_static(b"RESET")))
            }

            // Key-Value commands
            "SET" => self.handle_set(&args[1..]),
            "GET" => self.handle_get(&args[1..]),
            "DEL" => self.handle_del(&args[1..]),
            "EXISTS" => self.handle_exists(&args[1..]),
            "TYPE" => self.handle_type(&args[1..]),
            "MGET" => self.handle_mget(&args[1..]),
            "MSET" => self.handle_mset(&args[1..]),
            "MSETNX" => self.handle_msetnx(&args[1..]),
            "APPEND" => self.handle_append(&args[1..]),
            "STRLEN" => self.handle_strlen(&args[1..]),
            "GETRANGE" => self.handle_getrange(&args[1..]),
            "SUBSTR" => self.handle_substr(&args[1..]),
            "SETRANGE" => self.handle_setrange(&args[1..]),
            "SETEX" => self.handle_setex(&args[1..]),
            "PSETEX" => self.handle_psetex(&args[1..]),
            "GETSET" => self.handle_getset(&args[1..]),
            "UNLINK" => self.handle_unlink(&args[1..]),
            "RENAME" => self.handle_rename(&args[1..]),
            "RENAMENX" => self.handle_renamenx(&args[1..]),
            "MOVE" => self.handle_move(&args[1..]),
            "COPY" => self.handle_copy(&args[1..]),
            "RANDOMKEY" => self.handle_randomkey(&args[1..]),
            "TOUCH" => self.handle_touch(&args[1..]),

            // Distributed lock commands
            "SETNX" => self.handle_setnx(&args[1..]),
            "GETDEL" => self.handle_getdel(&args[1..]),
            "GETEX" => self.handle_getex(&args[1..]),
            "LCS" => self.handle_lcs(&args[1..]),
            "DUMP" => self.handle_dump(&args[1..]),
            "RESTORE" => self.handle_restore(&args[1..]),

            // Counter commands
            "INCR" => self.handle_incr(&args[1..]),
            "DECR" => self.handle_decr(&args[1..]),
            "INCRBY" => self.handle_incrby(&args[1..]),
            "DECRBY" => self.handle_decrby(&args[1..]),
            "INCRBYFLOAT" => self.handle_incrbyfloat(&args[1..]),

            // Bitmap commands
            "SETBIT" => self.handle_setbit(&args[1..]),
            "GETBIT" => self.handle_getbit(&args[1..]),
            "BITCOUNT" => self.handle_bitcount(&args[1..]),
            "BITPOS" => self.handle_bitpos(&args[1..]),
            "BITOP" => self.handle_bitop(&args[1..]),
            "BITFIELD" => self.handle_bitfield(&args[1..]),
            "BITFIELD_RO" => self.handle_bitfield_ro(&args[1..]),

            // HyperLogLog
            "PFADD" => self.handle_pfadd(&args[1..]),
            "PFCOUNT" => self.handle_pfcount(&args[1..]),
            "PFMERGE" => self.handle_pfmerge(&args[1..]),

            // Expiration commands
            "EXPIRE" => self.handle_expire(&args[1..]),
            "PEXPIRE" => self.handle_pexpire(&args[1..]),
            "EXPIREAT" => self.handle_expireat(&args[1..]),
            "PEXPIREAT" => self.handle_pexpireat(&args[1..]),
            "PERSIST" => self.handle_persist(&args[1..]),
            "TTL" => self.handle_ttl(&args[1..]),
            "PTTL" => self.handle_pttl(&args[1..]),
            "EXPIRETIME" => self.handle_expiretime(&args[1..]),
            "PEXPIRETIME" => self.handle_pexpiretime(&args[1..]),

            // Admin commands
            "SELECT" => self.handle_select(&args[1..]),
            "SWAPDB" => self.handle_swapdb(&args[1..]),
            "DBSIZE" => self.handle_dbsize(&args[1..]),
            "KEYS" => self.handle_keys(&args[1..]),
            "SCAN" => self.handle_scan(&args[1..]),
            "FLUSHDB" => self.handle_flushdb(&args[1..]),
            "FLUSHALL" => self.handle_flushall(&args[1..]),
            "INFO" => self.handle_info(&args[1..]),
            "HEALTH" => self.handle_health(&args[1..]),
            "SWEEP" => self.handle_sweep(&args[1..]),
            "CONFIG" => self.handle_config(&args[1..]),
            "MEMORY" => self.handle_memory(&args[1..]),
            "OBJECT" => self.handle_object(&args[1..]),
            "SLOWLOG" => self.handle_slowlog(&args[1..]),
            "LATENCY" => self.handle_latency(&args[1..]),
            "MODULE" => self.handle_module(&args[1..]),

            // Persistence
            "SAVE" => self.handle_save(&args[1..]),
            "BGSAVE" => self.handle_bgsave(&args[1..]),
            "LASTSAVE" => self.handle_lastsave(&args[1..]),
            "BGREWRITEAOF" => self.handle_bgrewriteaof(&args[1..]),
            "SYNC" => self.handle_sync(&args[1..]),
            "PSYNC" => self.handle_psync(&args[1..]),
            "REPLCONF" => self.handle_replconf(&args[1..]),
            "ROLE" => self.handle_role(&args[1..]),
            "REPLICAOF" | "SLAVEOF" => self.handle_replicaof(&args[1..]),
            "FAILOVER" => self.handle_failover(&args[1..]).await,
            "WAIT" => self.handle_wait(&args[1..]).await,

            // Sorted Set commands
            "ZADD" => self.handle_zadd(&args[1..]),
            "ZRANGE" => self.handle_zrange(&args[1..]),
            "ZRANGESTORE" => self.handle_zrangestore(&args[1..]),
            "ZREVRANGE" => self.handle_zrevrange(&args[1..]),
            "ZCARD" => self.handle_zcard(&args[1..]),
            "ZSCORE" => self.handle_zscore(&args[1..]),
            "ZMSCORE" => self.handle_zmscore(&args[1..]),
            "ZREM" => self.handle_zrem(&args[1..]),
            "ZRANK" => self.handle_zrank(&args[1..]),
            "ZREVRANK" => self.handle_zrevrank(&args[1..]),
            "ZINCRBY" => self.handle_zincrby(&args[1..]),
            "ZRANGEBYSCORE" => self.handle_zrangebyscore(&args[1..]),
            "ZREVRANGEBYSCORE" => self.handle_zrevrangebyscore(&args[1..]),
            "ZCOUNT" => self.handle_zcount(&args[1..]),
            "ZREMRANGEBYRANK" => self.handle_zremrangebyrank(&args[1..]),
            "ZREMRANGEBYSCORE" => self.handle_zremrangebyscore(&args[1..]),
            "ZRANGEBYLEX" => self.handle_zrangebylex(&args[1..]),
            "ZREVRANGEBYLEX" => self.handle_zrevrangebylex(&args[1..]),
            "ZLEXCOUNT" => self.handle_zlexcount(&args[1..]),
            "ZREMRANGEBYLEX" => self.handle_zremrangebylex(&args[1..]),
            "ZRANDMEMBER" => self.handle_zrandmember(&args[1..]),
            "ZSCAN" => self.handle_zscan(&args[1..]),
            "ZUNION" => self.handle_zunion(&args[1..]),
            "ZINTER" => self.handle_zinter(&args[1..]),
            "ZDIFF" => self.handle_zdiff(&args[1..]),
            "ZINTERCARD" => self.handle_zintercard(&args[1..]),
            "ZUNIONSTORE" => self.handle_zunionstore(&args[1..]),
            "ZINTERSTORE" => self.handle_zinterstore(&args[1..]),
            "ZDIFFSTORE" => self.handle_zdiffstore(&args[1..]),
            "ZPOPMIN" => self.handle_zpopmin(&args[1..]),
            "ZPOPMAX" => self.handle_zpopmax(&args[1..]),
            "ZMPOP" => self.handle_zmpop(&args[1..]),
            "BZPOPMIN" => self.handle_bzpopmin(&args[1..]).await,
            "BZPOPMAX" => self.handle_bzpopmax(&args[1..]).await,
            "BZMPOP" => self.handle_bzmpop(&args[1..]).await,

            // Geospatial commands
            "GEOADD" => self.handle_geoadd(&args[1..]),
            "GEOSEARCH" => self.handle_geosearch(&args[1..]),
            "GEOSEARCHSTORE" => self.handle_geosearchstore(&args[1..]),
            "GEODIST" => self.handle_geodist(&args[1..]),
            "GEOPOS" => self.handle_geopos(&args[1..]),
            "GEOHASH" => self.handle_geohash(&args[1..]),
            "GEORADIUS" => self.handle_georadius(&args[1..]),
            "GEORADIUS_RO" => self.handle_georadius_ro(&args[1..]),
            "GEORADIUSBYMEMBER" => self.handle_georadiusbymember(&args[1..]),
            "GEORADIUSBYMEMBER_RO" => self.handle_georadiusbymember_ro(&args[1..]),

            // Hash commands
            "HSET" => self.handle_hset(&args[1..]),
            "HSETNX" => self.handle_hsetnx(&args[1..]),
            "HGET" => self.handle_hget(&args[1..]),
            "HMGET" => self.handle_hmget(&args[1..]),
            "HDEL" => self.handle_hdel(&args[1..]),
            "HGETDEL" => self.handle_hgetdel(&args[1..]),
            "HGETALL" => self.handle_hgetall(&args[1..]),
            "HLEN" => self.handle_hlen(&args[1..]),
            "HEXISTS" => self.handle_hexists(&args[1..]),
            "HKEYS" => self.handle_hkeys(&args[1..]),
            "HVALS" => self.handle_hvals(&args[1..]),
            "HINCRBY" => self.handle_hincrby(&args[1..]),
            "HINCRBYFLOAT" => self.handle_hincrbyfloat(&args[1..]),
            "HSTRLEN" => self.handle_hstrlen(&args[1..]),
            "HMSET" => self.handle_hmset(&args[1..]),
            "HRANDFIELD" => self.handle_hrandfield(&args[1..]),
            "HSCAN" => self.handle_hscan(&args[1..]),

            // List commands
            "LPUSH" => self.handle_lpush(&args[1..]),
            "RPUSH" => self.handle_rpush(&args[1..]),
            "LPUSHX" => self.handle_lpushx(&args[1..]),
            "RPUSHX" => self.handle_rpushx(&args[1..]),
            "LPOP" => self.handle_lpop(&args[1..]),
            "RPOP" => self.handle_rpop(&args[1..]),
            "BLPOP" => self.handle_blpop(&args[1..]).await,
            "BRPOP" => self.handle_brpop(&args[1..]).await,
            "LRANGE" => self.handle_lrange(&args[1..]),
            "LLEN" => self.handle_llen(&args[1..]),
            "LINDEX" => self.handle_lindex(&args[1..]),
            "LSET" => self.handle_lset(&args[1..]),
            "LREM" => self.handle_lrem(&args[1..]),
            "LTRIM" => self.handle_ltrim(&args[1..]),
            "LINSERT" => self.handle_linsert(&args[1..]),
            "LPOS" => self.handle_lpos(&args[1..]),
            "LMOVE" => self.handle_lmove(&args[1..]),
            "BLMOVE" => self.handle_blmove(&args[1..]).await,
            "RPOPLPUSH" => self.handle_rpoplpush(&args[1..]),
            "BRPOPLPUSH" => self.handle_brpoplpush(&args[1..]).await,
            "LMPOP" => self.handle_lmpop(&args[1..]),
            "BLMPOP" => self.handle_blmpop(&args[1..]).await,
            "SORT" => self.handle_sort(&args[1..]),

            // Set commands
            "SADD" => self.handle_sadd(&args[1..]),
            "SREM" => self.handle_srem(&args[1..]),
            "SMEMBERS" => self.handle_smembers(&args[1..]),
            "SISMEMBER" => self.handle_sismember(&args[1..]),
            "SMISMEMBER" => self.handle_smismember(&args[1..]),
            "SCARD" => self.handle_scard(&args[1..]),
            "SINTER" => self.handle_sinter(&args[1..]),
            "SINTERCARD" => self.handle_sintercard(&args[1..]),
            "SUNION" => self.handle_sunion(&args[1..]),
            "SDIFF" => self.handle_sdiff(&args[1..]),
            "SINTERSTORE" => self.handle_sinterstore(&args[1..]),
            "SUNIONSTORE" => self.handle_sunionstore(&args[1..]),
            "SDIFFSTORE" => self.handle_sdiffstore(&args[1..]),
            "SMOVE" => self.handle_smove(&args[1..]),
            "SPOP" => self.handle_spop(&args[1..]),
            "SRANDMEMBER" => self.handle_srandmember(&args[1..]),
            "SSCAN" => self.handle_sscan(&args[1..]),

            // Stream commands
            "XADD" => self.handle_xadd(&args[1..]),
            "XLEN" => self.handle_xlen(&args[1..]),
            "XRANGE" => self.handle_xrange(&args[1..]),
            "XREVRANGE" => self.handle_xrevrange(&args[1..]),
            "XDEL" => self.handle_xdel(&args[1..]),
            "XTRIM" => self.handle_xtrim(&args[1..]),
            "XREAD" => self.handle_xread(&args[1..]).await,
            "XGROUP" => self.handle_xgroup(&args[1..]),
            "XREADGROUP" => self.handle_xreadgroup(&args[1..]).await,
            "XACK" => self.handle_xack(&args[1..]),
            "XPENDING" => self.handle_xpending(&args[1..]),
            "XCLAIM" => self.handle_xclaim(&args[1..]),
            "XAUTOCLAIM" => self.handle_xautoclaim(&args[1..]),
            "XSETID" => self.handle_xsetid(&args[1..]),
            "XINFO" => self.handle_xinfo(&args[1..]),

            // Pub/Sub commands (async — no block_in_place)
            "PUBLISH" => self.handle_publish(&args[1..]).await,
            "SUBSCRIBE" => self.handle_subscribe(&args[1..]).await,
            "UNSUBSCRIBE" => self.handle_unsubscribe(&args[1..]).await,
            "PSUBSCRIBE" => self.handle_psubscribe(&args[1..]).await,
            "PUNSUBSCRIBE" => self.handle_punsubscribe(&args[1..]).await,
            "PUBSUB" => self.handle_pubsub(&args[1..]).await,

            // Search commands
            "FT.CREATE" => self.handle_ft_create(&args[1..]),
            "FT.DROPINDEX" => self.handle_ft_dropindex(&args[1..]),
            "FT._LIST" => self.handle_ft_list(&args[1..]),
            "FT.INFO" => self.handle_ft_info(&args[1..]),
            "FT.SEARCH" => self.handle_ft_search(&args[1..]),
            "FT.TAGVALS" => self.handle_ft_tagvals(&args[1..]),
            "FT.ALIASADD" => self.handle_ft_aliasadd(&args[1..]),
            "FT.ALIASDEL" => self.handle_ft_aliasdel(&args[1..]),
            "FT.ALIASUPDATE" => self.handle_ft_aliasupdate(&args[1..]),

            // Shard Pub/Sub commands (Redis 7.0+)
            "SSUBSCRIBE" => self.handle_ssubscribe(&args[1..]).await,
            "SUNSUBSCRIBE" => self.handle_sunsubscribe(&args[1..]).await,
            "SPUBLISH" => self.handle_spublish(&args[1..]).await,

            // Lua scripting / Redis Functions (FUNCTION is a stub; use EVAL for scripts)
            "EVAL" => self.handle_eval(&args[1..]),
            "EVAL_RO" => self.handle_eval_ro(&args[1..]),
            "EVALSHA" => self.handle_evalsha(&args[1..]),
            "EVALSHA_RO" => self.handle_evalsha_ro(&args[1..]),
            "SCRIPT" => self.handle_script(&args[1..]),
            "FUNCTION" => self.handle_function(&args[1..]),
            "FCALL" => self.handle_fcall(&args[1..]),
            "FCALL_RO" => self.handle_fcall_ro(&args[1..]),

            _ => Ok(RespValue::error(format!("ERR unknown command '{}'", cmd_upper))),
        };

        if let Ok(ref resp) = result {
            // SORT only mutates (and must be AOF/repl propagated) when STORE is used.
            if cmd_upper == "SORT" {
                if sort_has_store(&args[1..]) {
                    self.maybe_persist_write(cmd_upper, &args[1..], resp);
                    if response_indicates_success(resp) {
                        self.notify_watch_after_write(cmd_upper, &args[1..]);
                    }
                }
            } else {
                self.maybe_persist_write(cmd_upper, &args[1..], resp);
                if is_write_command(cmd_upper)
                    && response_indicates_success(resp)
                    && !is_noop_write(cmd_upper, resp)
                {
                    self.notify_watch_after_write(cmd_upper, &args[1..]);
                }
            }
        }

        // Slow log (skip SLOWLOG itself and transaction control).
        if !matches!(
            cmd_upper,
            "SLOWLOG" | "EXEC" | "MULTI" | "DISCARD" | "WATCH" | "UNWATCH"
        ) {
            let duration_us = handle_start.elapsed().as_micros() as i64;
            let argv: Vec<Bytes> = args
                .iter()
                .filter_map(|a| a.as_bulk_string().cloned())
                .collect();
            self.cache.slowlog.maybe_push(duration_us, argv);
        }

        result
    }

    /// Check ACL command + key + channel permissions for the authenticated user.
    /// Returns `Some(error)` when denied, `None` when allowed.
    fn check_acl_permission(&self, cmd_upper: &str, args: &[RespValue]) -> Option<RespValue> {
        let username = self.username.as_deref().unwrap_or("default");
        let cmd_lower = cmd_upper.to_ascii_lowercase();
        let client_id = self.client_id.unwrap_or(0);

        if !self.acl.can_execute(username, &cmd_lower) {
            self.cache
                .acl_log
                .push("command", &cmd_lower, username, client_id);
            return Some(RespValue::error(format!(
                "NOPERM this user has no permissions to run the '{}' command",
                cmd_lower
            )));
        }

        // Key permission checks using COMMAND_SPECS first_key/last_key/step when available.
        // EVAL/EVALSHA: keys are dynamic (numkeys after script/sha).
        let script_keys = extract_eval_keys(cmd_upper, args);
        if let Some(keys) = script_keys {
            for key in keys {
                if !self.acl.can_access_key(username, &key) {
                    self.cache
                        .acl_log
                        .push("key", &key, username, client_id);
                    return Some(RespValue::error(
                        "NOPERM this user has no permissions to access one of the keys used as arguments",
                    ));
                }
            }
        } else if let Some((first_key, last_key, step)) = meta::command_key_spec(&cmd_lower) {
            if first_key > 0 {
                let keys = extract_command_keys(args, first_key, last_key, step);
                for key in keys {
                    if !self.acl.can_access_key(username, &key) {
                        self.cache
                            .acl_log
                            .push("key", &key, username, client_id);
                        return Some(RespValue::error(
                            "NOPERM this user has no permissions to access one of the keys used as arguments",
                        ));
                    }
                }
            }
        }

        // Channel permission checks for pub/sub commands.
        if let Some(channels) = extract_pubsub_channels(cmd_upper, args) {
            for ch in channels {
                if !self.acl.can_access_channel(username, &ch) {
                    self.cache
                        .acl_log
                        .push("channel", &ch, username, client_id);
                    return Some(RespValue::error(
                        "NOPERM this user has no permissions to access one of the channels used as arguments",
                    ));
                }
            }
        }

        None
    }

    /// Cluster slot / redirect checks (CROSSSLOT, MOVED, ASK).
    /// Returns `Some(error)` when the command must not run locally.
    fn check_cluster_redirect(
        &self,
        cmd_upper: &str,
        args: &[RespValue],
        asking: bool,
    ) -> Option<RespValue> {
        let cluster = self.cluster.as_ref()?;

        // SELECT / MOVE are multi-DB; not allowed in cluster mode.
        if cmd_upper == "SELECT" {
            return Some(RespValue::error(
                "ERR SELECT is not allowed in cluster mode",
            ));
        }
        if cmd_upper == "MOVE" {
            return Some(RespValue::error(
                "ERR MOVE is not allowed in cluster mode",
            ));
        }

        // EVAL/EVALSHA: keys follow numkeys (args: script/sha, numkeys, key…).
        let keys = if let Some(k) = extract_eval_key_bytes(cmd_upper, args) {
            k
        } else {
            // Commands without key specs (or first_key=0) are not redirected.
            let cmd_lower = cmd_upper.to_ascii_lowercase();
            let (first_key, last_key, step) = meta::command_key_spec(&cmd_lower)?;
            if first_key <= 0 {
                return None;
            }
            extract_command_key_bytes(args, first_key, last_key, step)
        };
        if keys.is_empty() {
            return None;
        }

        // CROSSSLOT: multi-key commands must hash to one slot.
        let mut slot: Option<u16> = None;
        for key in &keys {
            let s = key_hash_slot(key);
            match slot {
                None => slot = Some(s),
                Some(prev) if prev != s => {
                    return Some(RespValue::error(
                        "CROSSSLOT Keys in request don't hash to the same slot",
                    ));
                }
                _ => {}
            }
        }
        let slot = slot?;

        // IMPORTING + ASKING: serve one-shot even if we don't stably own the slot.
        if asking && cluster.is_importing(slot) {
            return None;
        }

        // Not owned → MOVED to current owner.
        if !cluster.owns_slot(slot) {
            if let Some(t) = cluster.moved_target(slot) {
                return Some(RespValue::error(format!(
                    "MOVED {} {}:{}",
                    t.slot, t.ip, t.port
                )));
            }
            return Some(RespValue::error(format!(
                "CLUSTERDOWN Hash slot not served ({})",
                slot
            )));
        }

        // Owned but MIGRATING and key missing → ASK destination.
        if cluster.is_migrating(slot) {
            let any_missing = keys.iter().any(|k| !self.cache.exists(k));
            if any_missing {
                if let Some(t) = cluster.ask_target(slot) {
                    return Some(RespValue::error(format!(
                        "ASK {} {}:{}",
                        t.slot, t.ip, t.port
                    )));
                }
            }
        }

        None
    }

    // Helper method for parsing integers
    pub(crate) fn parse_integer(&self, value: &RespValue) -> Result<i64> {
        if let Some(i) = value.as_integer() {
            return Ok(i);
        }

        if let Some(s) = value.as_bulk_string() {
            let s = std::str::from_utf8(s)
                .map_err(|_| Error::InvalidArgument("invalid UTF-8".into()))?;
            return s
                .parse::<i64>()
                .map_err(|_| Error::InvalidArgument("invalid integer".into()));
        }

        Err(Error::InvalidArgument("expected integer".into()))
    }

    // Pub/Sub command handlers (fully async)
    async fn handle_publish(&self, args: &[RespValue]) -> Result<RespValue> {
        self.cache.cmd_publish(args).await
    }

    async fn handle_subscribe(&mut self, args: &[RespValue]) -> Result<RespValue> {
        let client_id = self.client_id.ok_or_else(|| {
            Error::InvalidArgument("client not registered for pub/sub".into())
        })?;

        let responses = self.maybe_pubsub_push_frames(
            self.cache.cmd_subscribe(client_id, args).await?,
        );

        // Track subscription count from the last response's integer field.
        if let Some(n) = pubsub_count_from_frame(responses.last()) {
            self.pubsub_subscriptions = n as usize;
        }

        // Each confirmation must be sent as a separate top-level RESP frame.
        if responses.len() == 1 {
            Ok(responses.into_iter().next().unwrap())
        } else {
            Ok(RespValue::Multiple(responses))
        }
    }

    async fn handle_unsubscribe(&mut self, args: &[RespValue]) -> Result<RespValue> {
        let client_id = self.client_id.ok_or_else(|| {
            Error::InvalidArgument("client not registered for pub/sub".into())
        })?;

        let responses = self.maybe_pubsub_push_frames(
            self.cache.cmd_unsubscribe(client_id, args).await?,
        );

        if let Some(n) = pubsub_count_from_frame(responses.last()) {
            self.pubsub_subscriptions = n as usize;
        }

        if responses.len() == 1 {
            Ok(responses.into_iter().next().unwrap())
        } else {
            Ok(RespValue::Multiple(responses))
        }
    }

    async fn handle_psubscribe(&mut self, args: &[RespValue]) -> Result<RespValue> {
        let client_id = self.client_id.ok_or_else(|| {
            Error::InvalidArgument("client not registered for pub/sub".into())
        })?;

        let responses = self.maybe_pubsub_push_frames(
            self.cache.cmd_psubscribe(client_id, args).await?,
        );

        if let Some(n) = pubsub_count_from_frame(responses.last()) {
            self.pubsub_subscriptions = n as usize;
        }

        if responses.len() == 1 {
            Ok(responses.into_iter().next().unwrap())
        } else {
            Ok(RespValue::Multiple(responses))
        }
    }

    async fn handle_punsubscribe(&mut self, args: &[RespValue]) -> Result<RespValue> {
        let client_id = self.client_id.ok_or_else(|| {
            Error::InvalidArgument("client not registered for pub/sub".into())
        })?;

        let responses = self.maybe_pubsub_push_frames(
            self.cache.cmd_punsubscribe(client_id, args).await?,
        );

        if let Some(n) = pubsub_count_from_frame(responses.last()) {
            self.pubsub_subscriptions = n as usize;
        }

        if responses.len() == 1 {
            Ok(responses.into_iter().next().unwrap())
        } else {
            Ok(RespValue::Multiple(responses))
        }
    }

    async fn handle_pubsub(&self, args: &[RespValue]) -> Result<RespValue> {
        self.cache.cmd_pubsub(args).await
    }

    async fn handle_ssubscribe(&mut self, args: &[RespValue]) -> Result<RespValue> {
        let client_id = self.client_id.ok_or_else(|| {
            Error::InvalidArgument("client not registered for pub/sub".into())
        })?;

        let responses = self.maybe_pubsub_push_frames(
            self.cache.cmd_ssubscribe(client_id, args).await?,
        );

        if let Some(n) = pubsub_count_from_frame(responses.last()) {
            // n is the absolute total shard-channel count for this client
            self.shard_subscriptions = n as usize;
        }

        if responses.len() == 1 {
            Ok(responses.into_iter().next().unwrap())
        } else {
            Ok(RespValue::Multiple(responses))
        }
    }

    async fn handle_sunsubscribe(&mut self, args: &[RespValue]) -> Result<RespValue> {
        let client_id = self.client_id.ok_or_else(|| {
            Error::InvalidArgument("client not registered for pub/sub".into())
        })?;

        let responses = self.maybe_pubsub_push_frames(
            self.cache.cmd_sunsubscribe(client_id, args).await?,
        );

        if let Some(n) = pubsub_count_from_frame(responses.last()) {
            // n is the remaining absolute shard-channel count for this client
            self.shard_subscriptions = n as usize;
        }

        if responses.len() == 1 {
            Ok(responses.into_iter().next().unwrap())
        } else {
            Ok(RespValue::Multiple(responses))
        }
    }

    async fn handle_spublish(&self, args: &[RespValue]) -> Result<RespValue> {
        self.cache.cmd_spublish(args).await
    }
}

/// Uppercase an ASCII command name into `buf` when it fits (no heap).
/// Falls back to a heap `String` only for oversized names.
fn ascii_uppercase_cmd<'a>(cmd: &[u8], buf: &'a mut [u8; 64]) -> std::borrow::Cow<'a, str> {
    if cmd.len() <= buf.len() {
        for (i, &b) in cmd.iter().enumerate() {
            buf[i] = b.to_ascii_uppercase();
        }
        // ASCII uppercase is always valid UTF-8.
        let s = std::str::from_utf8(&buf[..cmd.len()]).unwrap_or("");
        std::borrow::Cow::Borrowed(s)
    } else {
        std::borrow::Cow::Owned(String::from_utf8_lossy(cmd).to_uppercase())
    }
}

/// Read the subscription count (3rd element) from a subscribe confirmation frame.
fn pubsub_count_from_frame(frame: Option<&RespValue>) -> Option<i64> {
    let arr = match frame? {
        RespValue::Array(a) | RespValue::Push(a) => a,
        _ => return None,
    };
    match arr.get(2) {
        Some(RespValue::Integer(n)) => Some(*n),
        _ => None,
    }
}

/// True when SORT args include STORE (write path for replica/AOF/WATCH).
fn sort_has_store(args: &[RespValue]) -> bool {
    let mut i = 1; // skip key
    while i < args.len() {
        let opt = match args[i].as_bulk_string() {
            Some(s) => s,
            None => {
                i += 1;
                continue;
            }
        };
        let upper = String::from_utf8_lossy(opt).to_ascii_uppercase();
        match upper.as_str() {
            "STORE" | "BY" | "GET" => {
                // STORE / BY / GET take one following argument
                i += 2;
                if upper == "STORE" {
                    return true;
                }
            }
            "LIMIT" => i += 3,
            "ASC" | "DESC" | "ALPHA" => i += 1,
            _ => i += 1,
        }
    }
    false
}

/// Whether `cmd` mutates keyspace / server state (for replica write gate + EVAL_RO).
pub(super) fn is_write_command(cmd: &str) -> bool {
    matches!(
        cmd,
        "SET"
            | "DEL"
            | "MSET"
            | "MSETNX"
            | "SETRANGE"
            | "SETNX"
            | "GETDEL"
            | "GETEX"
            | "HGETDEL"
            | "APPEND"
            | "SETEX"
            | "PSETEX"
            | "GETSET"
            | "INCRBYFLOAT"
            | "UNLINK"
            | "ZRANGESTORE"
            | "LPUSHX"
            | "RPUSHX"
            | "RENAME"
            | "RENAMENX"
            | "MOVE"
            | "COPY"
            | "TOUCH"
            | "INCR"
            | "DECR"
            | "INCRBY"
            | "DECRBY"
            | "EXPIRE"
            | "PEXPIRE"
            | "EXPIREAT"
            | "PEXPIREAT"
            | "PERSIST"
            | "FLUSHDB"
            | "FLUSHALL"
            | "ZADD"
            | "ZREM"
            | "ZINCRBY"
            | "ZREMRANGEBYRANK"
            | "ZREMRANGEBYSCORE"
            | "ZREMRANGEBYLEX"
            | "ZUNIONSTORE"
            | "ZINTERSTORE"
            | "ZDIFFSTORE"
            | "ZPOPMIN"
            | "ZPOPMAX"
            | "ZMPOP"
            | "BZPOPMIN"
            | "BZPOPMAX"
            | "BZMPOP"
            | "GEOADD"
            | "GEOSEARCHSTORE"
            | "GEORADIUS"
            | "GEORADIUSBYMEMBER"
            | "SWAPDB"
            | "RESTORE"
            | "HSET"
            | "HSETNX"
            | "HMSET"
            | "HDEL"
            | "HINCRBY"
            | "HINCRBYFLOAT"
            | "LPUSH"
            | "RPUSH"
            | "LPOP"
            | "RPOP"
            | "BLPOP"
            | "BRPOP"
            | "LSET"
            | "LREM"
            | "LTRIM"
            | "LINSERT"
            | "LMOVE"
            | "BLMOVE"
            | "RPOPLPUSH"
            | "BRPOPLPUSH"
            | "LMPOP"
            | "BLMPOP"
            | "SADD"
            | "SREM"
            | "SINTERSTORE"
            | "SUNIONSTORE"
            | "SDIFFSTORE"
            | "SMOVE"
            | "SPOP"
            | "XADD"
            | "XDEL"
            | "XTRIM"
            | "XGROUP"
            | "XACK"
            | "XCLAIM"
            | "XAUTOCLAIM"
            | "XSETID"
            // XREADGROUP mutates PEL / last_delivered
            | "XREADGROUP"
            | "SETBIT"
            | "BITOP"
            | "BITFIELD"
            | "PFADD"
            | "PFMERGE"
            // Scripts may mutate; propagate whole EVAL/EVALSHA (Redis-style).
            // EVAL_RO / EVALSHA_RO are intentionally omitted (read-only).
            | "EVAL"
            | "EVALSHA"
            // FCALL may mutate; FCALL_RO is read-only.
            | "FCALL"
            | "FUNCTION"
    )
}

fn response_indicates_success(response: &RespValue) -> bool {
    match response {
        RespValue::Error(_) => false,
        // SET NX failure returns null bulk string
        RespValue::BulkString(None) => false,
        _ => true,
    }
}

/// Writes that returned success-shaped replies but did not mutate (e.g. RENAMENX=0).
fn is_noop_write(cmd: &str, response: &RespValue) -> bool {
    match (cmd, response) {
        ("RENAMENX", RespValue::Integer(0)) => true,
        ("MOVE", RespValue::Integer(0)) => true,
        ("COPY", RespValue::Integer(0)) => true,
        ("SETNX", RespValue::Integer(0)) => true,
        ("MSETNX", RespValue::Integer(0)) => true,
        ("SMOVE", RespValue::Integer(0)) => true,
        _ => false,
    }
}

/// EVAL / EVALSHA key arguments as strings for ACL.
fn extract_eval_keys(cmd_upper: &str, args: &[RespValue]) -> Option<Vec<String>> {
    extract_eval_key_bytes(cmd_upper, args).map(|keys| {
        keys.into_iter()
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .collect()
    })
}

/// EVAL / EVALSHA / EVAL_RO / EVALSHA_RO: args are [script|sha, numkeys, key1..keyN, arg…].
fn extract_eval_key_bytes(cmd_upper: &str, args: &[RespValue]) -> Option<Vec<Bytes>> {
    if cmd_upper != "EVAL"
        && cmd_upper != "EVALSHA"
        && cmd_upper != "EVAL_RO"
        && cmd_upper != "EVALSHA_RO"
    {
        return None;
    }
    if args.len() < 2 {
        return Some(Vec::new());
    }
    let numkeys = match args[1].as_integer() {
        Some(n) if n >= 0 => n as usize,
        Some(_) => return Some(Vec::new()),
        None => match args[1].as_bulk_string() {
            Some(s) => match std::str::from_utf8(s).ok().and_then(|t| t.parse::<i64>().ok()) {
                Some(n) if n >= 0 => n as usize,
                _ => return Some(Vec::new()),
            },
            None => return Some(Vec::new()),
        },
    };
    let key_slice = &args[2..];
    if key_slice.len() < numkeys {
        return Some(Vec::new());
    }
    let mut keys = Vec::with_capacity(numkeys);
    for k in &key_slice[..numkeys] {
        if let Some(b) = k.as_bulk_string() {
            keys.push(b.clone());
        }
    }
    Some(keys)
}

/// Extract key argument strings using Redis COMMAND first_key/last_key/step.
/// `first_key`/`last_key` are 1-based indexes into the command arguments (not including the command name).
fn extract_command_keys(
    args: &[RespValue],
    first_key: i64,
    last_key: i64,
    step: i64,
) -> Vec<String> {
    extract_command_key_bytes(args, first_key, last_key, step)
        .into_iter()
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .collect()
}

/// Extract channel names for pub/sub ACL checks.
/// Returns `None` when the command is not channel-scoped.
fn extract_pubsub_channels(cmd_upper: &str, args: &[RespValue]) -> Option<Vec<String>> {
    let channels: Vec<String> = match cmd_upper {
        // All args are channel names
        "SUBSCRIBE" | "UNSUBSCRIBE" | "SSUBSCRIBE" | "SUNSUBSCRIBE" | "SPUBLISH" => args
            .iter()
            .filter_map(|a| {
                a.as_bulk_string()
                    .map(|b| String::from_utf8_lossy(b).into_owned())
            })
            .collect(),
        // Patterns are treated as channel patterns for ACL (same glob rules)
        "PSUBSCRIBE" | "PUNSUBSCRIBE" => args
            .iter()
            .filter_map(|a| {
                a.as_bulk_string()
                    .map(|b| String::from_utf8_lossy(b).into_owned())
            })
            .collect(),
        // First arg is channel
        "PUBLISH" => {
            if let Some(b) = args.first().and_then(|a| a.as_bulk_string()) {
                vec![String::from_utf8_lossy(b).into_owned()]
            } else {
                Vec::new()
            }
        }
        _ => return None,
    };
    // Empty UNSUBSCRIBE (no args) unsubscribes all — no channel check needed.
    Some(channels)
}

/// Extract key argument bytes using Redis COMMAND first_key/last_key/step.
fn extract_command_key_bytes(
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
        // Negative last_key: count from end (Redis: -1 = last arg, -2 = second last, …)
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
