//! SAVE / BGSAVE / LASTSAVE / BGREWRITEAOF / SYNC / PSYNC / REPLICAOF / FAILOVER / ROLE / REPLCONF

use crate::error::Result;
use crate::persistence::replication::{ReplicationManager, SyncStart};
use crate::protocol::RespValue;
use bytes::Bytes;
use super::CommandHandler;

impl CommandHandler {
    pub(super) fn handle_save(&self, _args: &[RespValue]) -> Result<RespValue> {
        let Some(p) = self.persistence.as_ref() else {
            return Ok(RespValue::error("ERR persistence not configured"));
        };
        match p.save(&self.databases) {
            Ok(()) => Ok(RespValue::ok()),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    pub(super) fn handle_bgsave(&self, _args: &[RespValue]) -> Result<RespValue> {
        let Some(p) = self.persistence.as_ref() else {
            return Ok(RespValue::error("ERR persistence not configured"));
        };
        if p.bgsave(self.databases.clone()) {
            Ok(RespValue::SimpleString(Bytes::from_static(
                b"Background saving started",
            )))
        } else {
            Ok(RespValue::error("ERR Background save already in progress"))
        }
    }

    pub(super) fn handle_lastsave(&self, _args: &[RespValue]) -> Result<RespValue> {
        let ts = self
            .persistence
            .as_ref()
            .map(|p| p.last_save_unix())
            .unwrap_or(0);
        Ok(RespValue::Integer(ts as i64))
    }

    pub(super) fn handle_bgrewriteaof(&self, _args: &[RespValue]) -> Result<RespValue> {
        let Some(p) = self.persistence.as_ref() else {
            return Ok(RespValue::error("ERR persistence not configured"));
        };
        match p.rewrite_aof(&self.databases) {
            Ok(()) => Ok(RespValue::SimpleString(Bytes::from_static(
                b"Background append only file rewriting started",
            ))),
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    /// SYNC — full resync for replicas (legacy). Registers a feed channel.
    pub(super) fn handle_sync(&mut self, _args: &[RespValue]) -> Result<RespValue> {
        let Some(p) = self.persistence.as_ref() else {
            return Ok(RespValue::error("ERR persistence not configured"));
        };
        let host = self.replica_announce_ip.clone();
        let port = self.replica_announce_port;
        match p
            .replication
            .start_full_sync_announced(&self.databases, host, port)
        {
            Ok((response_bytes, rx)) => {
                self.pending_replica_feed = Some(rx);
                self.pending_raw_response = Some(response_bytes);
                Ok(RespValue::ok())
            }
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    /// PSYNC replicationid offset — full or partial resync.
    ///
    /// `PSYNC ? -1` forces a full resync. Matching replid + offset still in the
    /// primary backlog yields `+CONTINUE` + backlog + live feed.
    pub(super) fn handle_psync(&mut self, args: &[RespValue]) -> Result<RespValue> {
        let Some(p) = self.persistence.as_ref() else {
            return Ok(RespValue::error("ERR persistence not configured"));
        };
        if args.len() != 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'psync' command",
            ));
        }
        let replid = match args[0].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR invalid replid")),
        };
        let offset = match self.parse_integer(&args[1]) {
            Ok(o) => o,
            Err(_) => {
                // Also accept bulk string integers
                match args[1]
                    .as_bulk_string()
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(o) => o,
                    None => {
                        return Ok(RespValue::error(
                            "ERR value is not an integer or out of range",
                        ))
                    }
                }
            }
        };

        let host = self.replica_announce_ip.clone();
        let port = self.replica_announce_port;
        match p
            .replication
            .start_psync_announced(&self.databases, &replid, offset, host, port)
        {
            Ok(SyncStart::Full {
                raw_response,
                feed,
            })
            | Ok(SyncStart::Partial {
                raw_response,
                feed,
            }) => {
                self.pending_replica_feed = Some(feed);
                self.pending_raw_response = Some(raw_response);
                Ok(RespValue::ok())
            }
            Err(e) => Ok(RespValue::error(e.to_resp_string())),
        }
    }

    /// REPLCONF — accepted for Redis replica handshake compatibility.
    ///
    /// Tracks `listening-port` / `ip-address` on this connection so the next
    /// SYNC/PSYNC can associate identity with the replica feed (for FAILOVER TO).
    pub(super) fn handle_replconf(&mut self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'replconf' command",
            ));
        }
        // GETACK → reply with ACK <offset> (primary probing replica)
        let sub = args[0]
            .as_bulk_string()
            .map(|b| String::from_utf8_lossy(b).to_uppercase())
            .unwrap_or_default();
        if sub == "GETACK" {
            let off = self
                .persistence
                .as_ref()
                .map(|p| p.replication.replica_offset())
                .unwrap_or(0);
            return Ok(RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"REPLCONF"))),
                RespValue::BulkString(Some(Bytes::from_static(b"ACK"))),
                RespValue::BulkString(Some(Bytes::from(off.to_string()))),
            ]));
        }
        if sub == "LISTENING-PORT" {
            if let Some(port_arg) = args.get(1).and_then(|v| v.as_bulk_string()) {
                if let Ok(s) = std::str::from_utf8(port_arg) {
                    if let Ok(p) = s.parse::<u16>() {
                        self.replica_announce_port = Some(p);
                    }
                }
            }
            return Ok(RespValue::ok());
        }
        if sub == "IP-ADDRESS" {
            if let Some(ip_arg) = args.get(1).and_then(|v| v.as_bulk_string()) {
                self.replica_announce_ip =
                    Some(String::from_utf8_lossy(ip_arg).into_owned());
            }
            return Ok(RespValue::ok());
        }
        // capa, ack, etc. → OK
        Ok(RespValue::ok())
    }

    /// ROLE — master/slave identity for clients and tooling.
    pub(super) fn handle_role(&self, _args: &[RespValue]) -> Result<RespValue> {
        let Some(p) = self.persistence.as_ref() else {
            // No persistence → standalone master with empty offset
            return Ok(RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"master"))),
                RespValue::Integer(0),
                RespValue::Array(vec![]),
            ]));
        };
        Ok(p.replication.role_reply())
    }

    /// REPLICAOF host port | REPLICAOF NO ONE
    pub(super) fn handle_replicaof(&self, args: &[RespValue]) -> Result<RespValue> {
        let Some(p) = self.persistence.as_ref() else {
            return Ok(RespValue::error("ERR persistence not configured"));
        };

        if args.len() == 2 {
            let a0 = args[0]
                .as_bulk_string()
                .map(|b| String::from_utf8_lossy(b).to_string())
                .unwrap_or_default();
            let a1 = args[1]
                .as_bulk_string()
                .map(|b| String::from_utf8_lossy(b).to_string())
                .unwrap_or_default();

            if a0.eq_ignore_ascii_case("NO") && a1.eq_ignore_ascii_case("ONE") {
                p.replication.set_replicaof(None);
                return Ok(RespValue::ok());
            }

            let host = a0;
            let port = a1;
            if port.parse::<u16>().is_err() {
                return Ok(RespValue::error("ERR Invalid port"));
            }
            p.replication
                .set_replicaof(Some(format!("{}:{}", host, port)));
            return Ok(RespValue::ok());
        }

        Ok(RespValue::error(
            "ERR wrong number of arguments for 'replicaof' command",
        ))
    }

    /// FAILOVER — bare promote on replica, or coordinated `FAILOVER TO` on master.
    ///
    /// - Bare `FAILOVER` (replica only): local `promote_to_master()`.
    /// - `FAILOVER TO <host> <port> [TIMEOUT ms]` (master only): connect to target,
    ///   send bare FAILOVER, demote self on success.
    ///
    /// **MVP-lite race**: no offset catch-up wait before promoting the target.
    pub(super) async fn handle_failover(&self, args: &[RespValue]) -> Result<RespValue> {
        let Some(p) = self.persistence.as_ref() else {
            return Ok(RespValue::error("ERR persistence not configured"));
        };

        if args.is_empty() {
            if !p.replication.is_replica() {
                return Ok(RespValue::error(
                    "ERR FAILOVER is only allowed on replicas",
                ));
            }
            // Best-effort disconnect from primary + full promote state reset
            p.replication.promote_to_master();
            return Ok(RespValue::ok());
        }

        let sub = args[0]
            .as_bulk_string()
            .map(|b| String::from_utf8_lossy(b).to_uppercase())
            .unwrap_or_default();

        if sub == "TO" {
            return self.handle_failover_to(&p.replication, &args[1..]).await;
        }

        if sub == "ABORT" || sub == "TIMEOUT" || sub == "FORCE" {
            return Ok(RespValue::error(
                "ERR FAILOVER options other than bare FAILOVER or FAILOVER TO are not supported",
            ));
        }

        Ok(RespValue::error(
            "ERR wrong number of arguments for 'failover' command",
        ))
    }

    /// Parse and run `FAILOVER TO host port [TIMEOUT ms]`.
    async fn handle_failover_to(
        &self,
        repl: &ReplicationManager,
        args: &[RespValue],
    ) -> Result<RespValue> {
        // FAILOVER TO is master-only
        if repl.is_replica() {
            return Ok(RespValue::error(
                "ERR FAILOVER TO is only allowed on the master",
            ));
        }

        if args.len() < 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'failover' command",
            ));
        }

        let host = match args[0].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => {
                return Ok(RespValue::error(
                    "ERR wrong number of arguments for 'failover' command",
                ))
            }
        };
        let port_s = match args[1].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR Invalid port")),
        };
        let port: u16 = match port_s.parse() {
            Ok(p) => p,
            Err(_) => return Ok(RespValue::error("ERR Invalid port")),
        };

        let mut timeout_ms = ReplicationManager::FAILOVER_DEFAULT_TIMEOUT_MS;
        let mut i = 2;
        while i < args.len() {
            let opt = args[i]
                .as_bulk_string()
                .map(|b| String::from_utf8_lossy(b).to_uppercase())
                .unwrap_or_default();
            if opt == "TIMEOUT" {
                let Some(val) = args.get(i + 1) else {
                    return Ok(RespValue::error(
                        "ERR FAILOVER TIMEOUT requires a milliseconds value",
                    ));
                };
                let ms = match self.parse_integer(val) {
                    Ok(n) if n >= 0 => n as u64,
                    Ok(_) => {
                        return Ok(RespValue::error(
                            "ERR FAILOVER TIMEOUT must be a non-negative integer",
                        ))
                    }
                    Err(_) => {
                        // bulk string integer
                        match val
                            .as_bulk_string()
                            .and_then(|b| std::str::from_utf8(b).ok())
                            .and_then(|s| s.parse::<i64>().ok())
                        {
                            Some(n) if n >= 0 => n as u64,
                            _ => {
                                return Ok(RespValue::error(
                                    "ERR FAILOVER TIMEOUT is not an integer or out of range",
                                ))
                            }
                        }
                    }
                };
                timeout_ms = ms;
                i += 2;
            } else {
                return Ok(RespValue::error(
                    "ERR syntax error: unsupported FAILOVER TO option",
                ));
            }
        }

        match repl.coordinated_failover_to(&host, port, timeout_ms).await {
            Ok(()) => Ok(RespValue::ok()),
            Err(e) => Ok(RespValue::error(e)),
        }
    }
}
