//! SENTINEL command handlers (Batch EW/EX/EZ/FA).

use super::CommandHandler;
use crate::error::Result;
use crate::protocol::RespValue;
use crate::sentinel::{
    count_reachable_sentinels, master_fields, meet_sentinel, peer_fields, try_failover,
    MasterInfo, SentinelState,
};
use bytes::Bytes;
use std::sync::Arc;

impl CommandHandler {
    /// `SENTINEL <subcommand> …`
    pub(super) async fn handle_sentinel(&self, args: &[RespValue]) -> Result<RespValue> {
        let Some(sentinel) = self.sentinel.as_ref() else {
            return Ok(RespValue::error(
                "ERR This instance has Sentinel support disabled",
            ));
        };
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'sentinel' command",
            ));
        }
        let sub = match args[0].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).to_uppercase(),
            None => return Ok(RespValue::error("ERR unknown subcommand")),
        };
        match sub.as_str() {
            "HELP" => Ok(sentinel_help()),
            "MYID" => {
                if args.len() != 1 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'sentinel|myid' command",
                    ));
                }
                Ok(RespValue::BulkString(Some(Bytes::from(sentinel.my_id()))))
            }
            "MONITOR" => self.sentinel_monitor(sentinel, &args[1..]),
            "REMOVE" => self.sentinel_remove(sentinel, &args[1..]),
            "GET-MASTER-ADDR-BY-NAME" | "GET_MASTER_ADDR_BY_NAME" => {
                self.sentinel_get_master_addr(sentinel, &args[1..])
            }
            "MASTERS" => {
                if args.len() != 1 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'sentinel|masters' command",
                    ));
                }
                let n = sentinel.peers().len();
                let list = sentinel.masters();
                Ok(RespValue::Array(
                    list.iter().map(|m| master_fields(m, n)).collect(),
                ))
            }
            "MASTER" => self.sentinel_master(sentinel, &args[1..]),
            "REPLICAS" | "SLAVES" => self.sentinel_replicas(sentinel, &args[1..]),
            "SENTINELS" => self.sentinel_sentinels(sentinel, &args[1..]),
            "SET" => self.sentinel_set(sentinel, &args[1..]),
            "MEET" => {
                if args.len() != 3 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'sentinel|meet' command",
                    ));
                }
                let ip = match args[1].as_bulk_string() {
                    Some(b) => String::from_utf8_lossy(b).into_owned(),
                    None => return Ok(RespValue::error("ERR invalid ip")),
                };
                let port = match self.parse_integer(&args[2]) {
                    Ok(p) if p > 0 && p <= u16::MAX as i64 => p as u16,
                    _ => return Ok(RespValue::error("ERR Invalid port")),
                };
                match meet_sentinel(sentinel, &ip, port).await {
                    Ok(()) => Ok(RespValue::ok()),
                    Err(e) => Ok(RespValue::error(e)),
                }
            }
            "MEETPEER" => {
                // Internal handshake: SENTINEL MEETPEER <id> <ip> <port>
                if args.len() != 4 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'sentinel|meetpeer' command",
                    ));
                }
                let id = match args[1].as_bulk_string() {
                    Some(b) => String::from_utf8_lossy(b).into_owned(),
                    None => return Ok(RespValue::error("ERR invalid id")),
                };
                let ip = match args[2].as_bulk_string() {
                    Some(b) => String::from_utf8_lossy(b).into_owned(),
                    None => return Ok(RespValue::error("ERR invalid ip")),
                };
                let port = match self.parse_integer(&args[3]) {
                    Ok(p) if p > 0 && p <= u16::MAX as i64 => p as u16,
                    _ => return Ok(RespValue::error("ERR Invalid port")),
                };
                sentinel.add_peer(id, ip, port);
                Ok(RespValue::ok())
            }
            "HELLO" => {
                // Batch FA: SENTINEL HELLO <csv>  (8-field Redis-style hello)
                // Also accepts discrete fields: ip port runid epoch mname mip mport mepoch
                if args.len() == 2 {
                    let csv = match args[1].as_bulk_string() {
                        Some(b) => String::from_utf8_lossy(b).into_owned(),
                        None => return Ok(RespValue::error("ERR invalid hello payload")),
                    };
                    let Some(msg) = SentinelState::parse_hello(&csv) else {
                        return Ok(RespValue::error("ERR invalid hello payload"));
                    };
                    let _ = sentinel.apply_hello(&msg);
                    return Ok(RespValue::ok());
                }
                if args.len() == 9 {
                    let get = |i: usize| -> Option<String> {
                        args.get(i)
                            .and_then(|v| v.as_bulk_string())
                            .map(|b| String::from_utf8_lossy(b).into_owned())
                    };
                    let sip = match get(1) {
                        Some(s) => s,
                        None => return Ok(RespValue::error("ERR invalid hello fields")),
                    };
                    let sport = match self.parse_integer(&args[2]) {
                        Ok(p) if p > 0 && p <= u16::MAX as i64 => p as u16,
                        _ => return Ok(RespValue::error("ERR invalid hello port")),
                    };
                    let runid = match get(3) {
                        Some(s) => s,
                        None => return Ok(RespValue::error("ERR invalid hello runid")),
                    };
                    let epoch = match self.parse_integer(&args[4]) {
                        Ok(e) if e >= 0 => e as u64,
                        _ => 0,
                    };
                    let mname = match get(5) {
                        Some(s) => s,
                        None => return Ok(RespValue::error("ERR invalid hello master name")),
                    };
                    let mip = match get(6) {
                        Some(s) => s,
                        None => return Ok(RespValue::error("ERR invalid hello master ip")),
                    };
                    let mport = match self.parse_integer(&args[7]) {
                        Ok(p) if p > 0 && p <= u16::MAX as i64 => p as u16,
                        _ => return Ok(RespValue::error("ERR invalid hello master port")),
                    };
                    let mepoch = match self.parse_integer(&args[8]) {
                        Ok(e) if e >= 0 => e as u64,
                        _ => 0,
                    };
                    let msg = crate::sentinel::HelloMsg {
                        sentinel_ip: sip,
                        sentinel_port: sport,
                        runid,
                        current_epoch: epoch,
                        master_name: mname,
                        master_ip: mip,
                        master_port: mport,
                        master_config_epoch: mepoch,
                    };
                    let _ = sentinel.apply_hello(&msg);
                    return Ok(RespValue::ok());
                }
                Ok(RespValue::error(
                    "ERR wrong number of arguments for 'sentinel|hello' command",
                ))
            }
            "IS-MASTER-DOWN-BY-ADDR" | "IS_MASTER_DOWN_BY_ADDR" => {
                // SENTINEL IS-MASTER-DOWN-BY-ADDR <ip> <port> [epoch] [runid]
                // Batch FE: epoch/runid drive sticky leader votes when s_down.
                if args.len() < 3 || args.len() > 5 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'sentinel|is-master-down-by-addr' command",
                    ));
                }
                let ip = match args[1].as_bulk_string() {
                    Some(b) => String::from_utf8_lossy(b).into_owned(),
                    None => return Ok(RespValue::error("ERR invalid ip")),
                };
                let port = match self.parse_integer(&args[2]) {
                    Ok(p) if p > 0 && p <= u16::MAX as i64 => p as u16,
                    _ => return Ok(RespValue::error("ERR Invalid port")),
                };
                let req_epoch = if args.len() >= 4 {
                    match self.parse_integer(&args[3]) {
                        Ok(e) if e >= 0 => e as u64,
                        _ => 0,
                    }
                } else {
                    0
                };
                let req_runid = if args.len() >= 5 {
                    match args[4].as_bulk_string() {
                        Some(b) => String::from_utf8_lossy(b).into_owned(),
                        None => "*".into(),
                    }
                } else {
                    "*".into()
                };
                let (down, leader, epoch) =
                    sentinel.is_master_down_by_addr(&ip, port, req_epoch, &req_runid);
                Ok(RespValue::Array(vec![
                    RespValue::Integer(down),
                    RespValue::BulkString(Some(Bytes::from(leader))),
                    RespValue::Integer(epoch as i64),
                ]))
            }
            "FLUSHCONFIG" => {
                // SENTINEL FLUSHCONFIG — write sentinel.conf (Batch EZ).
                if args.len() != 1 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'sentinel|flushconfig' command",
                    ));
                }
                let dir = sentinel
                    .conf_dir()
                    .unwrap_or_else(|| self.config.dir.clone());
                sentinel.set_conf_dir(&dir);
                match sentinel.save_conf_to(&dir) {
                    Ok(path) => {
                        tracing::info!("sentinel: saved config to {}", path.display());
                        Ok(RespValue::ok())
                    }
                    Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
                }
            }
            "FAILOVER" => {
                if args.len() != 2 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'sentinel|failover' command",
                    ));
                }
                let name = match args[1].as_bulk_string() {
                    Some(b) => String::from_utf8_lossy(b).into_owned(),
                    None => return Ok(RespValue::error("ERR invalid master name")),
                };
                match try_failover(sentinel, &name).await {
                    Ok(()) => Ok(RespValue::ok()),
                    Err(e) => Ok(RespValue::error(e)),
                }
            }
            "CKQUORUM" => {
                // Batch FN: usable = live PING count (self + reachable peers), not table size.
                if args.len() != 2 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'sentinel|ckquorum' command",
                    ));
                }
                let name = match args[1].as_bulk_string() {
                    Some(b) => String::from_utf8_lossy(b).into_owned(),
                    None => return Ok(RespValue::error("ERR invalid master name")),
                };
                match sentinel.master(&name) {
                    Some(m) => {
                        let usable = count_reachable_sentinels(sentinel).await;
                        if usable as u32 >= m.quorum {
                            Ok(RespValue::BulkString(Some(Bytes::from(format!(
                                "OK {} usable Sentinels. Quorum and failover authorization are OK",
                                usable
                            )))))
                        } else {
                            Ok(RespValue::error(format!(
                                "NOQUORUM {} usable Sentinels. Need at least {}.",
                                usable, m.quorum
                            )))
                        }
                    }
                    None => Ok(RespValue::error(format!(
                        "ERR No such master with name '{}'",
                        name
                    ))),
                }
            }
            _ => Ok(RespValue::error(format!(
                "ERR Unknown sentinel subcommand '{}'. Try SENTINEL HELP.",
                sub
            ))),
        }
    }

    fn sentinel_monitor(
        &self,
        sentinel: &Arc<SentinelState>,
        args: &[RespValue],
    ) -> Result<RespValue> {
        if args.len() != 4 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'sentinel|monitor' command",
            ));
        }
        let name = match args[0].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR invalid master name")),
        };
        let ip = match args[1].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR invalid ip")),
        };
        let port = match self.parse_integer(&args[2]) {
            Ok(p) if p > 0 && p <= u16::MAX as i64 => p as u16,
            _ => return Ok(RespValue::error("ERR Invalid port")),
        };
        let quorum = match self.parse_integer(&args[3]) {
            Ok(q) if q > 0 && q <= u32::MAX as i64 => q as u32,
            _ => return Ok(RespValue::error("ERR Invalid quorum")),
        };
        match sentinel.monitor(name, ip, port, quorum) {
            Ok(()) => Ok(RespValue::ok()),
            Err(e) => Ok(RespValue::error(e)),
        }
    }

    fn sentinel_remove(
        &self,
        sentinel: &Arc<SentinelState>,
        args: &[RespValue],
    ) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'sentinel|remove' command",
            ));
        }
        let name = match args[0].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR invalid master name")),
        };
        match sentinel.remove(&name) {
            Ok(()) => Ok(RespValue::ok()),
            Err(e) => Ok(RespValue::error(e)),
        }
    }

    fn sentinel_get_master_addr(
        &self,
        sentinel: &Arc<SentinelState>,
        args: &[RespValue],
    ) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'sentinel|get-master-addr-by-name' command",
            ));
        }
        let name = match args[0].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR invalid master name")),
        };
        match sentinel.get_master_addr(&name) {
            Some((ip, port)) => Ok(RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from(ip))),
                RespValue::BulkString(Some(Bytes::from(port.to_string()))),
            ])),
            None => Ok(RespValue::BulkString(None)),
        }
    }

    fn sentinel_master(
        &self,
        sentinel: &Arc<SentinelState>,
        args: &[RespValue],
    ) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'sentinel|master' command",
            ));
        }
        let name = match args[0].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR invalid master name")),
        };
        match sentinel.master(&name) {
            Some(m) => Ok(master_fields(&m, sentinel.peers().len())),
            None => Ok(RespValue::error(format!(
                "ERR No such master with name '{}'",
                name
            ))),
        }
    }

    fn sentinel_sentinels(
        &self,
        sentinel: &Arc<SentinelState>,
        args: &[RespValue],
    ) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'sentinel|sentinels' command",
            ));
        }
        let name = match args[0].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR invalid master name")),
        };
        if sentinel.master(&name).is_none() {
            return Ok(RespValue::error(format!(
                "ERR No such master with name '{}'",
                name
            )));
        }
        // Lite: all peers are treated as watching the same masters.
        Ok(RespValue::Array(
            sentinel.peers().iter().map(peer_fields).collect(),
        ))
    }

    fn sentinel_replicas(
        &self,
        sentinel: &Arc<SentinelState>,
        args: &[RespValue],
    ) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'sentinel|replicas' command",
            ));
        }
        let name = match args[0].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR invalid master name")),
        };
        let Some(m) = sentinel.master(&name) else {
            return Ok(RespValue::error(format!(
                "ERR No such master with name '{}'",
                name
            )));
        };
        Ok(RespValue::Array(
            m.replicas
                .iter()
                .map(|r| replica_fields(&m, r))
                .collect(),
        ))
    }

    fn sentinel_set(
        &self,
        sentinel: &Arc<SentinelState>,
        args: &[RespValue],
    ) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'sentinel|set' command",
            ));
        }
        let name = match args[0].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR invalid master name")),
        };
        let option = match args[1].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR invalid option")),
        };
        let value = match args[2].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR invalid value")),
        };
        match sentinel.set_option(&name, &option, &value) {
            Ok(()) => Ok(RespValue::ok()),
            Err(e) => Ok(RespValue::error(e)),
        }
    }
}

fn replica_fields(m: &MasterInfo, r: &crate::sentinel::ReplicaInfo) -> RespValue {
    let pairs = [
        ("name", format!("{}:{}", r.ip, r.port)),
        ("ip", r.ip.clone()),
        ("port", r.port.to_string()),
        ("runid", String::new()),
        ("flags", "slave".into()),
        ("master-link-down-time", "0".into()),
        ("master-link-status", "ok".into()),
        ("master-host", m.ip.clone()),
        ("master-port", m.port.to_string()),
        // Batch FK: surface rank fields used by promote ranking.
        ("slave-priority", r.priority.to_string()),
        ("slave-repl-offset", r.repl_offset.to_string()),
    ];
    let mut out = Vec::new();
    for (k, v) in pairs {
        out.push(RespValue::BulkString(Some(Bytes::from(k.to_string()))));
        out.push(RespValue::BulkString(Some(Bytes::from(v))));
    }
    RespValue::Array(out)
}

fn sentinel_help() -> RespValue {
    let lines: &[&[u8]] = &[
        b"SENTINEL <subcommand> [<arg> ...]. Subcommands are:",
        b"MYID -- this Sentinel runid",
        b"MONITOR <name> <ip> <port> <quorum> -- start monitoring a master",
        b"REMOVE <name> -- stop monitoring",
        b"GET-MASTER-ADDR-BY-NAME <name> -- current master ip/port array (or null)",
        b"MASTERS -- list monitored masters",
        b"MASTER <name> -- details for one master",
        b"REPLICAS|SLAVES <name> -- known replicas of master",
        b"SENTINELS <name> -- other known Sentinels (via MEET)",
        b"MEET <ip> <port> -- introduce a peer Sentinel",
        b"HELLO <csv>|fields -- apply peer hello (discover peer + optional switch-master)",
        b"IS-MASTER-DOWN-BY-ADDR <ip> <port> [epoch] [runid] -- down vote + optional leader vote",
        b"SET <name> <option> <value> -- down-after-milliseconds | quorum | auto-failover",
        b"FAILOVER <name> -- force failover to a reachable replica",
        b"FLUSHCONFIG -- write sentinel.conf under dir (also autosaved on topology changes)",
        b"CKQUORUM <name> -- check live reachable Sentinels >= quorum (PING peers)",
        b"HELP -- this help",
    ];
    RespValue::Array(
        lines
            .iter()
            .map(|l| RespValue::BulkString(Some(Bytes::from_static(l))))
            .collect(),
    )
}
