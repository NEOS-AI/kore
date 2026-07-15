//! CLUSTER and ASKING command handlers (Redis Cluster MVP + MEET/gossip + MIGRATEKEYS).

use super::CommandHandler;
use crate::cluster::{
    key_hash_slot, meet_peer, migrate_slot_keys, ClusterState, SLOT_COUNT,
};
use crate::error::Result;
use crate::protocol::RespValue;
use bytes::Bytes;

impl CommandHandler {
    /// ASKING — one-shot flag allowing next command against IMPORTING slots.
    pub(super) fn handle_asking(&mut self, args: &[RespValue]) -> Result<RespValue> {
        if !args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'asking' command",
            ));
        }
        if self.cluster.is_none() {
            return Ok(RespValue::error(
                "ERR This instance has cluster support disabled",
            ));
        }
        self.asking = true;
        Ok(RespValue::ok())
    }

    /// CLUSTER subcommands: KEYSLOT, MYID, INFO, NODES, SLOTS, SETSLOT,
    /// MIGRATEKEYS, MEET, MEETPEER, REPLICATE.
    pub(super) async fn handle_cluster(&self, args: &[RespValue]) -> Result<RespValue> {
        let Some(cluster) = self.cluster.as_ref() else {
            return Ok(RespValue::error(
                "ERR This instance has cluster support disabled",
            ));
        };

        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'cluster' command",
            ));
        }

        let sub = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_uppercase(),
            None => return Ok(RespValue::error("ERR unknown subcommand")),
        };

        match sub.as_str() {
            "KEYSLOT" => {
                if args.len() != 2 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'cluster|keyslot' command",
                    ));
                }
                let key = match args[1].as_bulk_string() {
                    Some(k) => k,
                    None => return Ok(RespValue::error("ERR invalid key")),
                };
                Ok(RespValue::Integer(key_hash_slot(key) as i64))
            }
            "MYID" => {
                if args.len() != 1 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'cluster|myid' command",
                    ));
                }
                Ok(RespValue::BulkString(Some(Bytes::from(cluster.my_id()))))
            }
            "INFO" => {
                if args.len() != 1 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'cluster|info' command",
                    ));
                }
                Ok(RespValue::BulkString(Some(Bytes::from(
                    cluster.format_info(),
                ))))
            }
            "NODES" => {
                if args.len() != 1 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'cluster|nodes' command",
                    ));
                }
                Ok(RespValue::BulkString(Some(Bytes::from(
                    cluster.format_nodes(),
                ))))
            }
            "SLOTS" => {
                if args.len() != 1 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'cluster|slots' command",
                    ));
                }
                Ok(cluster_slots_reply(cluster))
            }
            "SETSLOT" => self.handle_cluster_setslot(cluster, &args[1..]),
            "MIGRATEKEYS" => {
                self.handle_cluster_migratekeys(cluster, &args[1..])
                    .await
            }
            "MEET" => self.handle_cluster_meet(cluster, &args[1..]).await,
            "MEETPEER" => self.handle_cluster_meetpeer(cluster, &args[1..]),
            "REPLICATE" => self.handle_cluster_replicate(cluster, &args[1..]),
            _ => Ok(RespValue::error(format!(
                "ERR unknown subcommand '{}'. Try CLUSTER HELP.",
                sub
            ))),
        }
    }

    /// CLUSTER MIGRATEKEYS <slot> <dest-ip> <dest-port>
    ///
    /// Move keys of all types in `slot` from this node to dest over RESP
    /// (ASKING + type-specific recreate + DEL). Does not change slot ownership —
    /// operator issues SETSLOT NODE afterward.
    async fn handle_cluster_migratekeys(
        &self,
        cluster: &ClusterState,
        args: &[RespValue],
    ) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'cluster|migratekeys' command",
            ));
        }
        let slot = match self.parse_integer(&args[0]) {
            Ok(s) if s >= 0 && s < SLOT_COUNT as i64 => s as u16,
            _ => return Ok(RespValue::error("ERR Invalid or out of range slot")),
        };
        let dest_ip = match args[1].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR invalid dest ip")),
        };
        let dest_port = match self.parse_integer(&args[2]) {
            Ok(p) if p > 0 && p <= u16::MAX as i64 => p as u16,
            _ => {
                return Ok(RespValue::error(
                    "ERR Invalid TCP base port specified: port must be 1-65535",
                ))
            }
        };

        // Best-effort: require we own the slot (typical operator flow is MIGRATING).
        if !cluster.owns_slot(slot) {
            return Ok(RespValue::error(
                "ERR I'm not the owner of hash slot (cannot MIGRATEKEYS)",
            ));
        }

        match migrate_slot_keys(&self.cache, slot, &dest_ip, dest_port).await {
            Ok(r) => {
                // Integer reply = number of keys migrated (all types).
                let _ = r.skipped;
                Ok(RespValue::Integer(r.migrated as i64))
            }
            Err(e) => Ok(RespValue::error(e)),
        }
    }

    /// CLUSTER MEET <ip> <port> — join peer via RESP handshake.
    async fn handle_cluster_meet(
        &self,
        cluster: &ClusterState,
        args: &[RespValue],
    ) -> Result<RespValue> {
        if args.len() != 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'cluster|meet' command",
            ));
        }
        let ip = match args[0].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR invalid ip address")),
        };
        let port = match self.parse_integer(&args[1]) {
            Ok(p) if p > 0 && p <= u16::MAX as i64 => p as u16,
            _ => {
                return Ok(RespValue::error(
                    "ERR Invalid TCP base port specified: port must be 1-65535",
                ))
            }
        };

        match meet_peer(cluster, &ip, port).await {
            Ok(()) => Ok(RespValue::ok()),
            Err(e) => Ok(RespValue::error(e)),
        }
    }

    /// CLUSTER MEETPEER <node-id> <ip> <port> — handshake: peer announces itself.
    fn handle_cluster_meetpeer(
        &self,
        cluster: &ClusterState,
        args: &[RespValue],
    ) -> Result<RespValue> {
        if args.len() != 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'cluster|meetpeer' command",
            ));
        }
        let id = match args[0].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR invalid node id")),
        };
        let ip = match args[1].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR invalid ip address")),
        };
        let port = match self.parse_integer(&args[2]) {
            Ok(p) if p > 0 && p <= u16::MAX as i64 => p as u16,
            _ => return Ok(RespValue::error("ERR Invalid port")),
        };
        if id == cluster.my_id() {
            return Ok(RespValue::ok());
        }
        cluster.add_node(&id, &ip, port);
        cluster.touch_pong(&id);
        Ok(RespValue::ok())
    }

    /// CLUSTER REPLICATE <node-id> — become cluster replica of master (topology).
    /// Also wires REPLICAOF when persistence is present (data path).
    fn handle_cluster_replicate(
        &self,
        cluster: &ClusterState,
        args: &[RespValue],
    ) -> Result<RespValue> {
        if args.len() != 1 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'cluster|replicate' command",
            ));
        }
        let master_id = match args[0].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).into_owned(),
            None => return Ok(RespValue::error("ERR invalid node id")),
        };
        match cluster.configure_as_replica_of(&master_id) {
            Ok(()) => {
                if let Some(master) = cluster.get_node(&master_id) {
                    if let Some(p) = self.persistence.as_ref() {
                        p.replication
                            .set_replicaof(Some(format!("{}:{}", master.ip, master.port)));
                    }
                }
                Ok(RespValue::ok())
            }
            Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
        }
    }

    fn handle_cluster_setslot(
        &self,
        cluster: &ClusterState,
        args: &[RespValue],
    ) -> Result<RespValue> {
        // CLUSTER SETSLOT <slot> <subcommand> [node-id]
        if args.len() < 2 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'cluster|setslot' command",
            ));
        }
        let slot = match self.parse_integer(&args[0]) {
            Ok(s) if s >= 0 && s < SLOT_COUNT as i64 => s as u16,
            _ => return Ok(RespValue::error("ERR Invalid or out of range slot")),
        };
        let sub = match args[1].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_uppercase(),
            None => return Ok(RespValue::error("ERR syntax error")),
        };

        match sub.as_str() {
            "MIGRATING" => {
                if args.len() != 3 {
                    return Ok(RespValue::error(
                        "ERR syntax error, expected CLUSTER SETSLOT <slot> MIGRATING <node-id>",
                    ));
                }
                let node_id = match args[2].as_bulk_string() {
                    Some(b) => String::from_utf8_lossy(b).into_owned(),
                    None => return Ok(RespValue::error("ERR syntax error")),
                };
                match cluster.set_migrating(slot, &node_id) {
                    Ok(()) => Ok(RespValue::ok()),
                    Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
                }
            }
            "IMPORTING" => {
                if args.len() != 3 {
                    return Ok(RespValue::error(
                        "ERR syntax error, expected CLUSTER SETSLOT <slot> IMPORTING <node-id>",
                    ));
                }
                let node_id = match args[2].as_bulk_string() {
                    Some(b) => String::from_utf8_lossy(b).into_owned(),
                    None => return Ok(RespValue::error("ERR syntax error")),
                };
                match cluster.set_importing(slot, &node_id) {
                    Ok(()) => Ok(RespValue::ok()),
                    Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
                }
            }
            "STABLE" => {
                if args.len() != 2 {
                    return Ok(RespValue::error(
                        "ERR syntax error, expected CLUSTER SETSLOT <slot> STABLE",
                    ));
                }
                match cluster.set_stable(slot) {
                    Ok(()) => Ok(RespValue::ok()),
                    Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
                }
            }
            "NODE" => {
                if args.len() != 3 {
                    return Ok(RespValue::error(
                        "ERR syntax error, expected CLUSTER SETSLOT <slot> NODE <node-id>",
                    ));
                }
                let node_id = match args[2].as_bulk_string() {
                    Some(b) => String::from_utf8_lossy(b).into_owned(),
                    None => return Ok(RespValue::error("ERR syntax error")),
                };
                match cluster.set_node(slot, &node_id) {
                    Ok(()) => Ok(RespValue::ok()),
                    Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
                }
            }
            _ => Ok(RespValue::error(format!(
                "ERR unknown subcommand '{}'. Try CLUSTER HELP.",
                sub
            ))),
        }
    }
}

fn bulk(s: impl Into<Bytes>) -> RespValue {
    RespValue::BulkString(Some(s.into()))
}

fn cluster_slots_reply(cluster: &ClusterState) -> RespValue {
    let ranges = cluster.slots_ranges();
    let mut out = Vec::with_capacity(ranges.len());
    for (start, end, node) in ranges {
        out.push(RespValue::Array(vec![
            RespValue::Integer(start as i64),
            RespValue::Integer(end as i64),
            RespValue::Array(vec![
                bulk(node.ip),
                RespValue::Integer(node.port as i64),
                bulk(node.id),
            ]),
        ]));
    }
    RespValue::Array(out)
}
