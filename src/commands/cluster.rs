//! CLUSTER and ASKING command handlers (Redis Cluster MVP + MEET/gossip + MIGRATEKEYS/RESHARD).

use super::CommandHandler;
use crate::cluster::{
    execute_reshard_plan, finish_slot_node, key_hash_slot, keys_in_slot, meet_peer,
    migrate_slot_keys, plan_reshard, reshard_slots, ClusterNode, ClusterState, ManualFailoverMode,
    SLOT_COUNT,
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
    /// MIGRATEKEYS, RESHARD, MEET, MEETPEER, REPLICATE, OWNERS, EPOCH, HELP.
    pub(super) async fn handle_cluster(&self, args: &[RespValue]) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'cluster' command",
            ));
        }

        let sub = match args[0].as_bulk_string() {
            Some(s) => String::from_utf8_lossy(s).to_uppercase(),
            None => return Ok(RespValue::error("ERR unknown subcommand")),
        };

        // HELP is available even when cluster support is disabled.
        if sub == "HELP" {
            return Ok(cluster_help());
        }

        let Some(cluster) = self.cluster.as_ref() else {
            return Ok(RespValue::error(
                "ERR This instance has cluster support disabled",
            ));
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
            "MYSHARDID" => {
                // CLUSTER MYSHARDID — shard identity (Batch EJ).
                if args.len() != 1 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'cluster|myshardid' command",
                    ));
                }
                Ok(RespValue::BulkString(Some(Bytes::from(
                    cluster.my_shard_id(),
                ))))
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
            "SHARDS" => {
                // CLUSTER SHARDS — Redis 7-style shard topology (Batch EI).
                if args.len() != 1 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'cluster|shards' command",
                    ));
                }
                Ok(cluster_shards_reply(cluster))
            }
            "LINKS" => {
                // CLUSTER LINKS — no binary bus; empty list (Batch EI honesty).
                if args.len() != 1 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'cluster|links' command",
                    ));
                }
                Ok(RespValue::Array(vec![]))
            }
            "SETSLOT" => self.handle_cluster_setslot(cluster, &args[1..]),
            "MIGRATEKEYS" => {
                self.handle_cluster_migratekeys(cluster, &args[1..])
                    .await
            }
            "RESHARD" => self.handle_cluster_reshard(cluster, &args[1..]).await,
            "MEET" => self.handle_cluster_meet(cluster, &args[1..]).await,
            "MEETPEER" => self.handle_cluster_meetpeer(cluster, &args[1..]),
            "REPLICATE" => self.handle_cluster_replicate(cluster, &args[1..]),
            "FAILOVER" => self.handle_cluster_failover(cluster, &args[1..]),
            "SLOT-STATS" | "SLOTSTATS" => {
                // CLUSTER SLOT-STATS SLOTSRANGE <start> <end>
                //   [ORDERBY key-count [LIMIT n] [ASC|DESC]]  (Batch EV)
                self.handle_cluster_slot_stats(cluster, &args[1..])
            }
            "COUNTKEYSINSLOT" => {
                // CLUSTER COUNTKEYSINSLOT <slot>
                if args.len() != 2 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'cluster|countkeysinslot' command",
                    ));
                }
                let slot = match self.parse_integer(&args[1]) {
                    Ok(s) if s >= 0 && s < SLOT_COUNT as i64 => s as u16,
                    _ => return Ok(RespValue::error("ERR Invalid or out of range slot")),
                };
                let n = keys_in_slot(&self.cache, slot).len();
                Ok(RespValue::Integer(n as i64))
            }
            "GETKEYSINSLOT" => {
                // CLUSTER GETKEYSINSLOT <slot> <count>
                if args.len() != 3 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'cluster|getkeysinslot' command",
                    ));
                }
                let slot = match self.parse_integer(&args[1]) {
                    Ok(s) if s >= 0 && s < SLOT_COUNT as i64 => s as u16,
                    _ => return Ok(RespValue::error("ERR Invalid or out of range slot")),
                };
                let count = match self.parse_integer(&args[2]) {
                    Ok(c) if c >= 0 => c as usize,
                    _ => {
                        return Ok(RespValue::error(
                            "ERR Invalid COUNT: must be a non-negative integer",
                        ))
                    }
                };
                let keys = keys_in_slot(&self.cache, slot);
                let out: Vec<RespValue> = keys
                    .into_iter()
                    .take(count)
                    .map(|k| RespValue::BulkString(Some(k)))
                    .collect();
                Ok(RespValue::Array(out))
            }
            "REPLICAS" | "SLAVES" => {
                // CLUSTER REPLICAS <node-id> (SLAVES alias)
                if args.len() != 2 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'cluster|replicas' command",
                    ));
                }
                let node_id = match args[1].as_bulk_string() {
                    Some(b) => String::from_utf8_lossy(b).into_owned(),
                    None => return Ok(RespValue::error("ERR invalid node id")),
                };
                if cluster.get_node(&node_id).is_none() {
                    return Ok(RespValue::error(format!(
                        "ERR Unknown node {}",
                        node_id
                    )));
                }
                let ids = cluster.replicas_of(&node_id);
                Ok(RespValue::Array(
                    ids.into_iter()
                        .map(|id| RespValue::BulkString(Some(Bytes::from(id))))
                        .collect(),
                ))
            }
            "BUMPEPOCH" => {
                if args.len() != 1 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'cluster|bumpepoch' command",
                    ));
                }
                let _ = cluster.bump_epoch();
                self.try_autosave_nodes_conf(cluster);
                // Redis returns 1 if the epoch was changed.
                Ok(RespValue::Integer(1))
            }
            "SET-CONFIG-EPOCH" => {
                // CLUSTER SET-CONFIG-EPOCH <epoch> (Batch EK)
                if args.len() != 2 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'cluster|set-config-epoch' command",
                    ));
                }
                let epoch = match self.parse_integer(&args[1]) {
                    Ok(e) if e >= 0 => e as u64,
                    _ => {
                        return Ok(RespValue::error(
                            "ERR Invalid or out of range config epoch",
                        ))
                    }
                };
                match cluster.set_config_epoch(epoch) {
                    Ok(()) => {
                        self.try_autosave_nodes_conf(cluster);
                        Ok(RespValue::ok())
                    }
                    Err(e) => Ok(RespValue::error(e)),
                }
            }
            "ADDSLOTS" => {
                // CLUSTER ADDSLOTS slot [slot ...]
                if args.len() < 2 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'cluster|addslots' command",
                    ));
                }
                let mut slots = Vec::with_capacity(args.len() - 1);
                for a in &args[1..] {
                    match self.parse_integer(a) {
                        Ok(s) if s >= 0 && s < SLOT_COUNT as i64 => slots.push(s as u16),
                        _ => {
                            return Ok(RespValue::error("ERR Invalid or out of range slot"))
                        }
                    }
                }
                match cluster.add_slots(&slots) {
                    Ok(()) => {
                        self.try_autosave_nodes_conf(cluster);
                        Ok(RespValue::ok())
                    }
                    Err(e) => Ok(RespValue::error(e)),
                }
            }
            "DELSLOTS" => {
                if args.len() < 2 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'cluster|delslots' command",
                    ));
                }
                let mut slots = Vec::with_capacity(args.len() - 1);
                for a in &args[1..] {
                    match self.parse_integer(a) {
                        Ok(s) if s >= 0 && s < SLOT_COUNT as i64 => slots.push(s as u16),
                        _ => {
                            return Ok(RespValue::error("ERR Invalid or out of range slot"))
                        }
                    }
                }
                match cluster.del_slots(&slots) {
                    Ok(()) => {
                        self.try_autosave_nodes_conf(cluster);
                        Ok(RespValue::ok())
                    }
                    Err(e) => Ok(RespValue::error(e)),
                }
            }
            "FLUSHSLOTS" => {
                if args.len() != 1 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'cluster|flushslots' command",
                    ));
                }
                cluster.flush_slots();
                self.try_autosave_nodes_conf(cluster);
                Ok(RespValue::ok())
            }
            "ADDSLOTSRANGE" => {
                // CLUSTER ADDSLOTSRANGE start end [start end ...]
                match parse_slot_ranges(self, &args[1..]) {
                    Ok(ranges) => match cluster.add_slot_ranges(&ranges) {
                        Ok(()) => {
                            self.try_autosave_nodes_conf(cluster);
                            Ok(RespValue::ok())
                        }
                        Err(e) => Ok(RespValue::error(e)),
                    },
                    Err(e) => Ok(RespValue::error(e)),
                }
            }
            "DELSLOTSRANGE" => {
                match parse_slot_ranges(self, &args[1..]) {
                    Ok(ranges) => match cluster.del_slot_ranges(&ranges) {
                        Ok(()) => {
                            self.try_autosave_nodes_conf(cluster);
                            Ok(RespValue::ok())
                        }
                        Err(e) => Ok(RespValue::error(e)),
                    },
                    Err(e) => Ok(RespValue::error(e)),
                }
            }
            "FORGET" => {
                // CLUSTER FORGET <node-id>
                if args.len() != 2 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'cluster|forget' command",
                    ));
                }
                let node_id = match args[1].as_bulk_string() {
                    Some(b) => String::from_utf8_lossy(b).into_owned(),
                    None => return Ok(RespValue::error("ERR invalid node id")),
                };
                match cluster.forget_node(&node_id) {
                    Ok(()) => {
                        self.try_autosave_nodes_conf(cluster);
                        Ok(RespValue::ok())
                    }
                    Err(e) => Ok(RespValue::error(e)),
                }
            }
            "SAVECONFIG" => {
                // CLUSTER SAVECONFIG — write nodes.conf under dir (Batch EM).
                if args.len() != 1 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'cluster|saveconfig' command",
                    ));
                }
                // Keep autosave dir in sync with CONFIG dir for gossip claims.
                cluster.set_nodes_conf_dir(&self.config.dir);
                match cluster.save_nodes_conf_to(&self.config.dir) {
                    Ok(path) => {
                        tracing::info!("cluster: saved config to {}", path.display());
                        Ok(RespValue::ok())
                    }
                    Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
                }
            }
            "RESET" => {
                // CLUSTER RESET [HARD|SOFT] — default SOFT (Batch EG).
                let hard = match args.len() {
                    1 => false,
                    2 => {
                        let s = match args[1].as_bulk_string() {
                            Some(b) => String::from_utf8_lossy(b).to_uppercase(),
                            None => {
                                return Ok(RespValue::error(
                                    "ERR syntax error, try CLUSTER RESET [HARD|SOFT]",
                                ))
                            }
                        };
                        match s.as_str() {
                            "HARD" => true,
                            "SOFT" => false,
                            _ => {
                                return Ok(RespValue::error(
                                    "ERR syntax error, try CLUSTER RESET [HARD|SOFT]",
                                ))
                            }
                        }
                    }
                    _ => {
                        return Ok(RespValue::error(
                            "ERR wrong number of arguments for 'cluster|reset' command",
                        ))
                    }
                };
                cluster.reset_cluster_config();
                if hard {
                    // Wipe keyspaces (all logical DBs); keep process alive.
                    // `cache` Arc still points at the selected DB; flush clears it in place.
                    self.databases.flush_all_including_search();
                }
                // If we were a data-path replica, promote so we accept writes.
                if let Some(p) = self.persistence.as_ref() {
                    if p.replication.is_replica() {
                        p.replication.promote_to_master();
                    }
                }
                self.try_autosave_nodes_conf(cluster);
                Ok(RespValue::ok())
            }
            "OWNERS" => {
                // CLUSTER OWNERS — compressed [start,end,owner_id,ip,port,epoch] ranges (Batch DU).
                if args.len() != 1 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'cluster|owners' command",
                    ));
                }
                Ok(cluster_owners_reply(cluster))
            }
            "EPOCH" => {
                if args.len() != 1 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'cluster|epoch' command",
                    ));
                }
                Ok(RespValue::Integer(cluster.current_epoch() as i64))
            }
            "FAILREPORTS" => {
                // CLUSTER FAILREPORTS — node ids we consider pfail/fail (Batch DW).
                if args.len() != 1 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'cluster|failreports' command",
                    ));
                }
                let ids = cluster.local_fail_reports();
                Ok(RespValue::Array(
                    ids.into_iter()
                        .map(|id| RespValue::BulkString(Some(Bytes::from(id))))
                        .collect(),
                ))
            }
            "COUNT-FAILURE-REPORTS" => {
                // CLUSTER COUNT-FAILURE-REPORTS <node-id> (Batch EH)
                if args.len() != 2 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'cluster|count-failure-reports' command",
                    ));
                }
                let node_id = match args[1].as_bulk_string() {
                    Some(b) => String::from_utf8_lossy(b).into_owned(),
                    None => return Ok(RespValue::error("ERR invalid node id")),
                };
                Ok(RespValue::Integer(cluster.count_failure_reports(&node_id)))
            }
            "ROLEMAP" => {
                // CLUSTER ROLEMAP — [id, role, master_id, ip, port, offset] (Batch DY/EA).
                if args.len() != 1 {
                    return Ok(RespValue::error(
                        "ERR wrong number of arguments for 'cluster|rolemap' command",
                    ));
                }
                // Refresh local offset before advertising (Batch EA).
                if let Some(p) = self.persistence.as_ref() {
                    let off = p
                        .replication
                        .replica_offset()
                        .max(p.replication.master_repl_offset());
                    cluster.set_local_repl_offset(off);
                }
                Ok(cluster_rolemap_reply(cluster))
            }
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
            // Message only: integer reply has no room for partial counts.
            // Use CLUSTER RESHARD for honest migrated/skipped on failed_keys.
            Err(e) => Ok(RespValue::error(e.message)),
        }
    }

    /// CLUSTER RESHARD <slot> <dest-node-id>
    /// CLUSTER RESHARD <start-slot> <end-slot> <dest-node-id>
    /// CLUSTER RESHARD FINISH <slot> <dest-node-id>
    /// CLUSTER RESHARD PLAN <dest-node-id> <num-slots>   (Batch DX)
    /// CLUSTER RESHARD AUTO <dest-node-id> <num-slots>   (Batch DX)
    ///
    /// Source-side orchestration of the thin reshard flow for one slot or an
    /// inclusive range. Dual-end `SETSLOT NODE` uses prepare/vote then commit
    /// (Batch FB 2PC slice) with verify+retry. `FINISH` only runs dual-end NODE
    /// (no key move) so operators can complete `partial_*_node` without
    /// re-migrating; soft-warns when the source still holds keys in the slot.
    /// Multi-slot ranges abort after the first non-`complete` status
    /// (`failed_*`, `failed_prepare`, or `partial_*`). On `failed_keys`,
    /// `migrated`/`skipped` report partial progress — retry moves leftover
    /// source keys only.
    ///
    /// **PLAN** returns a greedy donor plan (no data move). **AUTO** plans then
    /// executes (local + remote source RESP). Reply: array of per-slot field
    /// arrays (slot/migrated/skipped/source_node/dest_node/status[/warning]).
    async fn handle_cluster_reshard(
        &self,
        cluster: &ClusterState,
        args: &[RespValue],
    ) -> Result<RespValue> {
        // CLUSTER RESHARD FINISH|PLAN|AUTO …
        if !args.is_empty() {
            if let Some(b) = args[0].as_bulk_string() {
                let first = String::from_utf8_lossy(b).to_uppercase();
                if first == "FINISH" {
                    if args.len() != 3 {
                        return Ok(RespValue::error(
                            "ERR wrong number of arguments for 'cluster|reshard|finish' command",
                        ));
                    }
                    let slot = match self.parse_integer(&args[1]) {
                        Ok(s) if s >= 0 && s < SLOT_COUNT as i64 => s as u16,
                        _ => return Ok(RespValue::error("ERR Invalid or out of range slot")),
                    };
                    let dest_id = match args[2].as_bulk_string() {
                        Some(b) => String::from_utf8_lossy(b).into_owned(),
                        None => return Ok(RespValue::error("ERR invalid dest node id")),
                    };
                    return match finish_slot_node(&self.cache, cluster, slot, &dest_id).await {
                        Ok(r) => {
                            // Dual-end NODE may have mutated ownership (Batch EO).
                            self.try_autosave_nodes_conf(cluster);
                            Ok(RespValue::Array(vec![r.to_resp_array()]))
                        }
                        Err(e) => Ok(RespValue::error(e)),
                    };
                }
                if first == "PLAN" {
                    if args.len() != 3 {
                        return Ok(RespValue::error(
                            "ERR wrong number of arguments for 'cluster|reshard|plan' command",
                        ));
                    }
                    let dest_id = match args[1].as_bulk_string() {
                        Some(b) => String::from_utf8_lossy(b).into_owned(),
                        None => return Ok(RespValue::error("ERR invalid dest node id")),
                    };
                    let num = match self.parse_integer(&args[2]) {
                        Ok(n) if n > 0 => n as usize,
                        _ => {
                            return Ok(RespValue::error(
                                "ERR CLUSTER RESHARD PLAN num-slots must be a positive integer",
                            ))
                        }
                    };
                    return match plan_reshard(cluster, &dest_id, num) {
                        Ok(plan) => Ok(RespValue::Array(
                            plan.iter().map(|e| e.to_resp_array()).collect(),
                        )),
                        Err(e) => Ok(RespValue::error(e)),
                    };
                }
                if first == "AUTO" {
                    if args.len() != 3 {
                        return Ok(RespValue::error(
                            "ERR wrong number of arguments for 'cluster|reshard|auto' command",
                        ));
                    }
                    let dest_id = match args[1].as_bulk_string() {
                        Some(b) => String::from_utf8_lossy(b).into_owned(),
                        None => return Ok(RespValue::error("ERR invalid dest node id")),
                    };
                    let num = match self.parse_integer(&args[2]) {
                        Ok(n) if n > 0 => n as usize,
                        _ => {
                            return Ok(RespValue::error(
                                "ERR CLUSTER RESHARD AUTO num-slots must be a positive integer",
                            ))
                        }
                    };
                    let plan = match plan_reshard(cluster, &dest_id, num) {
                        Ok(p) => p,
                        Err(e) => return Ok(RespValue::error(e)),
                    };
                    if plan.is_empty() {
                        return Ok(RespValue::Array(vec![]));
                    }
                    return match execute_reshard_plan(&self.cache, cluster, &dest_id, &plan).await
                    {
                        Ok(results) => {
                            self.try_autosave_nodes_conf(cluster);
                            Ok(RespValue::Array(
                                results.iter().map(|r| r.to_resp_array()).collect(),
                            ))
                        }
                        Err(e) => Ok(RespValue::error(e)),
                    };
                }
            }
        }

        let (start, end, dest_id) = match args.len() {
            2 => {
                let slot = match self.parse_integer(&args[0]) {
                    Ok(s) if s >= 0 && s < SLOT_COUNT as i64 => s as u16,
                    _ => return Ok(RespValue::error("ERR Invalid or out of range slot")),
                };
                let dest_id = match args[1].as_bulk_string() {
                    Some(b) => String::from_utf8_lossy(b).into_owned(),
                    None => return Ok(RespValue::error("ERR invalid dest node id")),
                };
                (slot, slot, dest_id)
            }
            3 => {
                let start = match self.parse_integer(&args[0]) {
                    Ok(s) if s >= 0 && s < SLOT_COUNT as i64 => s as u16,
                    _ => return Ok(RespValue::error("ERR Invalid or out of range start slot")),
                };
                let end = match self.parse_integer(&args[1]) {
                    Ok(s) if s >= 0 && s < SLOT_COUNT as i64 => s as u16,
                    _ => return Ok(RespValue::error("ERR Invalid or out of range end slot")),
                };
                if start > end {
                    return Ok(RespValue::error(
                        "ERR start slot must be <= end slot",
                    ));
                }
                let dest_id = match args[2].as_bulk_string() {
                    Some(b) => String::from_utf8_lossy(b).into_owned(),
                    None => return Ok(RespValue::error("ERR invalid dest node id")),
                };
                (start, end, dest_id)
            }
            _ => {
                return Ok(RespValue::error(
                    "ERR wrong number of arguments for 'cluster|reshard' command",
                ))
            }
        };

        match reshard_slots(&self.cache, cluster, start, end, &dest_id).await {
            Ok(results) => {
                self.try_autosave_nodes_conf(cluster);
                Ok(RespValue::Array(
                    results.iter().map(|r| r.to_resp_array()).collect(),
                ))
            }
            Err(e) => Ok(RespValue::error(e)),
        }
    }

    /// `CLUSTER SLOT-STATS SLOTSRANGE <start> <end> [ORDERBY key-count [LIMIT n] [ASC|DESC]]`
    ///
    /// Batch EV: Redis-7 shaped local usage stats for slots **owned by this node**
    /// in the inclusive range. One pass over the keyspace for key-count.
    /// `cpu-usec` / network bytes are reported as 0 (not instrumented yet).
    fn handle_cluster_slot_stats(
        &self,
        cluster: &ClusterState,
        args: &[RespValue],
    ) -> Result<RespValue> {
        if args.is_empty() {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'cluster|slot-stats' command",
            ));
        }
        let sub = match args[0].as_bulk_string() {
            Some(b) => String::from_utf8_lossy(b).to_uppercase(),
            None => {
                return Ok(RespValue::error(
                    "ERR syntax error, try CLUSTER SLOT-STATS SLOTSRANGE <start> <end>",
                ))
            }
        };
        if sub != "SLOTSRANGE" {
            return Ok(RespValue::error(
                "ERR syntax error, try CLUSTER SLOT-STATS SLOTSRANGE <start> <end> [ORDERBY key-count [LIMIT n] [ASC|DESC]]",
            ));
        }
        if args.len() < 3 {
            return Ok(RespValue::error(
                "ERR wrong number of arguments for 'cluster|slot-stats|slotsrange' command",
            ));
        }
        let start = match self.parse_integer(&args[1]) {
            Ok(s) if s >= 0 && s < SLOT_COUNT as i64 => s as u16,
            _ => return Ok(RespValue::error("ERR Invalid or out of range slot")),
        };
        let end = match self.parse_integer(&args[2]) {
            Ok(s) if s >= 0 && s < SLOT_COUNT as i64 => s as u16,
            _ => return Ok(RespValue::error("ERR Invalid or out of range slot")),
        };
        if start > end {
            return Ok(RespValue::error(
                "ERR start slot must be <= end slot",
            ));
        }

        // Optional: ORDERBY key-count [LIMIT n] [ASC|DESC]
        let mut order_by_key_count = false;
        let mut limit: Option<usize> = None;
        let mut descending = true; // Redis default for ORDERBY is DESC for busiest-first
        let mut i = 3;
        while i < args.len() {
            let tok = match args[i].as_bulk_string() {
                Some(b) => String::from_utf8_lossy(b).to_uppercase(),
                None => {
                    return Ok(RespValue::error(
                        "ERR syntax error in CLUSTER SLOT-STATS options",
                    ))
                }
            };
            match tok.as_str() {
                "ORDERBY" => {
                    i += 1;
                    if i >= args.len() {
                        return Ok(RespValue::error(
                            "ERR syntax error, ORDERBY requires a metric",
                        ));
                    }
                    let metric = match args[i].as_bulk_string() {
                        Some(b) => String::from_utf8_lossy(b).to_ascii_lowercase(),
                        None => {
                            return Ok(RespValue::error("ERR syntax error, invalid ORDERBY metric"))
                        }
                    };
                    match metric.as_str() {
                        "key-count" | "key_count" | "keycount" => order_by_key_count = true,
                        "cpu-usec" | "network-bytes-in" | "network-bytes-out" => {
                            // Accepted for syntax compatibility; all zeros → order is stable by slot.
                            order_by_key_count = true;
                        }
                        _ => {
                            return Ok(RespValue::error(format!(
                                "ERR Unknown ORDERBY metric '{}'",
                                metric
                            )))
                        }
                    }
                    i += 1;
                }
                "LIMIT" => {
                    i += 1;
                    if i >= args.len() {
                        return Ok(RespValue::error(
                            "ERR syntax error, LIMIT requires a non-negative integer",
                        ));
                    }
                    match self.parse_integer(&args[i]) {
                        Ok(n) if n >= 0 => limit = Some(n as usize),
                        _ => {
                            return Ok(RespValue::error(
                                "ERR syntax error, LIMIT requires a non-negative integer",
                            ))
                        }
                    }
                    i += 1;
                }
                "ASC" => {
                    descending = false;
                    i += 1;
                }
                "DESC" => {
                    descending = true;
                    i += 1;
                }
                _ => {
                    return Ok(RespValue::error(format!(
                        "ERR syntax error, unexpected token '{}'",
                        tok
                    )))
                }
            }
        }

        // One pass over keys → per-slot counts (only owned slots in range later).
        let mut counts = std::collections::HashMap::<u16, usize>::new();
        for k in self.cache.keys(None) {
            let s = key_hash_slot(&k);
            if s >= start && s <= end && cluster.owns_slot(s) {
                *counts.entry(s).or_insert(0) += 1;
            }
        }

        let mut rows: Vec<(u16, usize)> = Vec::new();
        for slot in start..=end {
            if !cluster.owns_slot(slot) {
                continue;
            }
            let n = counts.get(&slot).copied().unwrap_or(0);
            rows.push((slot, n));
        }

        if order_by_key_count {
            rows.sort_by(|a, b| {
                let cmp = a.1.cmp(&b.1);
                if descending {
                    cmp.reverse().then_with(|| a.0.cmp(&b.0))
                } else {
                    cmp.then_with(|| a.0.cmp(&b.0))
                }
            });
        } else {
            // Natural slot order (already ascending from the range loop).
        }

        if let Some(lim) = limit {
            if rows.len() > lim {
                rows.truncate(lim);
            }
        }

        let out: Vec<RespValue> = rows
            .into_iter()
            .map(|(slot, key_count)| {
                RespValue::Array(vec![
                    bulk_static(b"slot"),
                    RespValue::Integer(slot as i64),
                    bulk_static(b"key-count"),
                    RespValue::Integer(key_count as i64),
                    // Not instrumented — zeros for Redis-7 shape compatibility.
                    bulk_static(b"cpu-usec"),
                    RespValue::Integer(0),
                    bulk_static(b"network-bytes-in"),
                    RespValue::Integer(0),
                    bulk_static(b"network-bytes-out"),
                    RespValue::Integer(0),
                ])
            })
            .collect();
        Ok(RespValue::Array(out))
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
            Ok(()) => {
                self.try_autosave_nodes_conf(cluster);
                Ok(RespValue::ok())
            }
            Err(e) => Ok(RespValue::error(e)),
        }
    }

    /// CLUSTER MEETPEER <node-id> <ip> <port> [master|slave] [master_id|-]
    ///
    /// Handshake: peer announces itself. Optional role fields (Batch DY).
    fn handle_cluster_meetpeer(
        &self,
        cluster: &ClusterState,
        args: &[RespValue],
    ) -> Result<RespValue> {
        if args.len() != 3 && args.len() != 5 {
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

        let (role_master, role_master_id) = if args.len() == 5 {
            let role = match args[3].as_bulk_string() {
                Some(b) => String::from_utf8_lossy(b).to_ascii_lowercase(),
                None => return Ok(RespValue::error("ERR invalid role")),
            };
            let mid_raw = match args[4].as_bulk_string() {
                Some(b) => String::from_utf8_lossy(b).into_owned(),
                None => return Ok(RespValue::error("ERR invalid master id")),
            };
            let is_master = role == "master" || role == "myself";
            let mid = if is_master || mid_raw == "-" || mid_raw.is_empty() {
                None
            } else {
                Some(mid_raw)
            };
            (Some(is_master), Some(mid))
        } else {
            (None, None)
        };

        cluster.add_node_with_role(&id, &ip, port, role_master, role_master_id);
        cluster.touch_pong(&id);
        self.try_autosave_nodes_conf(cluster);
        Ok(RespValue::ok())
    }

    /// CLUSTER FAILOVER [FORCE|TAKEOVER] — replica promotes and claims slots (Batch EC).
    ///
    /// - (none): master must be fail/pfail; must be election winner  
    /// - FORCE: mark master fail; still require election winner  
    /// - TAKEOVER: mark master fail; claim even if not election winner  
    fn handle_cluster_failover(
        &self,
        cluster: &ClusterState,
        args: &[RespValue],
    ) -> Result<RespValue> {
        let mode = match args.len() {
            0 => ManualFailoverMode::Safe,
            1 => {
                let s = match args[0].as_bulk_string() {
                    Some(b) => String::from_utf8_lossy(b).to_uppercase(),
                    None => {
                        return Ok(RespValue::error(
                            "ERR syntax error, try CLUSTER FAILOVER [FORCE|TAKEOVER]",
                        ))
                    }
                };
                match s.as_str() {
                    "FORCE" => ManualFailoverMode::Force,
                    "TAKEOVER" => ManualFailoverMode::Takeover,
                    _ => {
                        return Ok(RespValue::error(
                            "ERR syntax error, try CLUSTER FAILOVER [FORCE|TAKEOVER]",
                        ))
                    }
                }
            }
            _ => {
                return Ok(RespValue::error(
                    "ERR wrong number of arguments for 'cluster|failover' command",
                ))
            }
        };

        // Refresh election inputs before ranking.
        if let Some(p) = self.persistence.as_ref() {
            let off = p
                .replication
                .replica_offset()
                .max(p.replication.master_repl_offset());
            cluster.set_local_repl_offset(off);
        }

        match cluster.manual_failover(mode) {
            Ok(_n) => {
                if let Some(p) = self.persistence.as_ref() {
                    p.replication.promote_to_master();
                }
                self.try_autosave_nodes_conf(cluster);
                Ok(RespValue::ok())
            }
            Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
        }
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
                self.try_autosave_nodes_conf(cluster);
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
                    Ok(()) => {
                        self.try_autosave_nodes_conf(cluster);
                        Ok(RespValue::ok())
                    }
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
                    Ok(()) => {
                        self.try_autosave_nodes_conf(cluster);
                        Ok(RespValue::ok())
                    }
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
                    Ok(()) => {
                        self.try_autosave_nodes_conf(cluster);
                        Ok(RespValue::ok())
                    }
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
                    Ok(()) => {
                        self.try_autosave_nodes_conf(cluster);
                        Ok(RespValue::ok())
                    }
                    Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
                }
            }
            // Batch FB: prepare/vote for dual-end NODE 2PC (no ownership change).
            "PREPARE" => {
                if args.len() != 3 {
                    return Ok(RespValue::error(
                        "ERR syntax error, expected CLUSTER SETSLOT <slot> PREPARE <node-id>",
                    ));
                }
                let node_id = match args[2].as_bulk_string() {
                    Some(b) => String::from_utf8_lossy(b).into_owned(),
                    None => return Ok(RespValue::error("ERR syntax error")),
                };
                match cluster.set_prepare_node(slot, &node_id) {
                    Ok(()) => {
                        // Durable prepare autosave also runs inside set_prepare_node
                        // when dir is set; keep handler path for dir sync (Batch FO).
                        self.try_autosave_nodes_conf(cluster);
                        Ok(RespValue::ok())
                    }
                    Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
                }
            }
            "ABORTPREPARE" => {
                if args.len() != 2 {
                    return Ok(RespValue::error(
                        "ERR syntax error, expected CLUSTER SETSLOT <slot> ABORTPREPARE",
                    ));
                }
                match cluster.abort_prepare_node(slot) {
                    Ok(()) => {
                        self.try_autosave_nodes_conf(cluster);
                        Ok(RespValue::ok())
                    }
                    Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
                }
            }
            // Batch FH: commit re-check — prepare still valid (epoch/TTL/topology).
            "CHECKPREPARE" => {
                if args.len() != 3 {
                    return Ok(RespValue::error(
                        "ERR syntax error, expected CLUSTER SETSLOT <slot> CHECKPREPARE <node-id>",
                    ));
                }
                let node_id = match args[2].as_bulk_string() {
                    Some(b) => String::from_utf8_lossy(b).into_owned(),
                    None => return Ok(RespValue::error("ERR syntax error")),
                };
                match cluster.check_prepare_valid(slot, &node_id) {
                    Ok(()) => Ok(RespValue::ok()),
                    Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
                }
            }
            // Batch FO: atomic prepare re-check + NODE under one write lock.
            "COMMITPREPARE" => {
                if args.len() != 3 {
                    return Ok(RespValue::error(
                        "ERR syntax error, expected CLUSTER SETSLOT <slot> COMMITPREPARE <node-id>",
                    ));
                }
                let node_id = match args[2].as_bulk_string() {
                    Some(b) => String::from_utf8_lossy(b).into_owned(),
                    None => return Ok(RespValue::error("ERR syntax error")),
                };
                match cluster.commit_prepare_node(slot, &node_id) {
                    Ok(()) => {
                        self.try_autosave_nodes_conf(cluster);
                        Ok(RespValue::ok())
                    }
                    Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
                }
            }
            _ => Ok(RespValue::error(format!(
                "ERR unknown subcommand '{}'. Try CLUSTER HELP.",
                sub
            ))),
        }
    }

    /// Best-effort write of `nodes.conf` after topology-mutating CLUSTER ops (Batch EO)
    /// and live-flag CONFIG SET (Batch FL).
    ///
    /// Uses `config.dir` (same as explicit SAVECONFIG). Also keeps
    /// [`ClusterState::set_nodes_conf_dir`] in sync so gossip failover claims
    /// can autosave. Failures are logged only — client replies stay OK.
    pub(super) fn try_autosave_nodes_conf(&self, cluster: &ClusterState) {
        cluster.set_nodes_conf_dir(&self.config.dir);
        match cluster.save_nodes_conf_to(&self.config.dir) {
            Ok(path) => {
                tracing::debug!("cluster: autosaved config to {}", path.display());
            }
            Err(e) => {
                tracing::warn!("cluster: autosave nodes.conf failed: {}", e);
            }
        }
    }
}

/// Parse `start end [start end ...]` into range pairs for ADDSLOTSRANGE/DELSLOTSRANGE.
fn parse_slot_ranges(
    handler: &CommandHandler,
    args: &[RespValue],
) -> std::result::Result<Vec<(u16, u16)>, String> {
    if args.is_empty() || args.len() % 2 != 0 {
        return Err(
            "ERR wrong number of arguments for 'cluster|addslotsrange|delslotsrange' command"
                .into(),
        );
    }
    let mut ranges = Vec::with_capacity(args.len() / 2);
    let mut i = 0;
    while i + 1 < args.len() {
        let start = match handler.parse_integer(&args[i]) {
            Ok(s) if s >= 0 && s < SLOT_COUNT as i64 => s as u16,
            _ => return Err("ERR Invalid or out of range slot".into()),
        };
        let end = match handler.parse_integer(&args[i + 1]) {
            Ok(s) if s >= 0 && s < SLOT_COUNT as i64 => s as u16,
            _ => return Err("ERR Invalid or out of range slot".into()),
        };
        ranges.push((start, end));
        i += 2;
    }
    Ok(ranges)
}

fn bulk(s: impl Into<Bytes>) -> RespValue {
    RespValue::BulkString(Some(s.into()))
}

fn bulk_static(s: &'static [u8]) -> RespValue {
    RespValue::BulkString(Some(Bytes::from_static(s)))
}

fn cluster_help() -> RespValue {
    RespValue::Array(vec![
        bulk_static(b"CLUSTER <subcommand> [<arg> ...]. Subcommands are:"),
        bulk_static(b"KEYSLOT <key> -- return the hash slot for key"),
        bulk_static(b"MYID -- return this node's cluster id"),
        bulk_static(b"MYSHARDID -- return this node's shard id (master id of the shard)"),
        bulk_static(b"INFO -- cluster state summary"),
        bulk_static(b"NODES -- node list in CLUSTER NODES format"),
        bulk_static(
            b"SLOTS -- slot ranges: [start end [master_ip port id] [replica_ip port id]...]",
        ),
        bulk_static(
            b"SHARDS -- Redis-7 style shards (slots + master/replica nodes; RESP2 field arrays)",
        ),
        bulk_static(b"LINKS -- cluster bus links (always empty; no binary bus)"),
        bulk_static(b"SETSLOT <slot> MIGRATING|IMPORTING|STABLE|NODE|PREPARE|ABORTPREPARE|CHECKPREPARE|COMMITPREPARE [node-id]"),
        bulk_static(b"MIGRATEKEYS <slot> <ip> <port> -- move keys in slot to dest"),
        bulk_static(
            b"RESHARD <slot> <node-id> | <start> <end> <node-id> -- orchestrate migrate + dual-end NODE (verify+retry, not atomic; range aborts on non-complete)",
        ),
        bulk_static(
            b"RESHARD FINISH <slot> <node-id> -- dual-end SETSLOT NODE only (recover partial_*_node; warns if source still has keys)",
        ),
        bulk_static(
            b"RESHARD PLAN <node-id> <num-slots> -- greedy plan of slots to move to dest (no data move)",
        ),
        bulk_static(
            b"RESHARD AUTO <node-id> <num-slots> -- plan then execute (local + remote source RESP; not 2PC)",
        ),
        bulk_static(b"MEET <ip> <port> -- introduce peer into the cluster"),
        bulk_static(b"REPLICATE <node-id> -- become replica of node"),
        bulk_static(
            b"FAILOVER [FORCE|TAKEOVER] -- replica claims master slots (election / force / takeover)",
        ),
        bulk_static(b"COUNTKEYSINSLOT <slot> -- number of local keys in hash slot"),
        bulk_static(b"GETKEYSINSLOT <slot> <count> -- sample local keys in hash slot"),
        bulk_static(
            b"SLOT-STATS SLOTSRANGE <start> <end> [ORDERBY key-count [LIMIT n] [ASC|DESC]] -- local slot key counts (owned slots)",
        ),
        bulk_static(b"REPLICAS|SLAVES <node-id> -- replica node ids of the given master"),
        bulk_static(b"BUMPEPOCH -- force-bump config epoch (returns 1)"),
        bulk_static(
            b"SET-CONFIG-EPOCH <epoch> -- set config epoch if greater than current (Batch EK)",
        ),
        bulk_static(b"ADDSLOTS <slot> [slot ...] -- assign hash slots to this node"),
        bulk_static(b"DELSLOTS <slot> [slot ...] -- unbind hash slots owned by this node"),
        bulk_static(b"FLUSHSLOTS -- unbind all hash slots owned by this node"),
        bulk_static(
            b"ADDSLOTSRANGE start end [start end ...] -- assign inclusive slot ranges to this node",
        ),
        bulk_static(
            b"DELSLOTSRANGE start end [start end ...] -- unbind inclusive slot ranges owned by this node",
        ),
        bulk_static(b"FORGET <node-id> -- drop peer from nodes table (not if it owns slots)"),
        bulk_static(
            b"RESET [SOFT|HARD] -- clear cluster config (HARD also FLUSHALL-style key wipe)",
        ),
        bulk_static(
            b"SAVECONFIG -- write cluster nodes.conf under dir (topology + live flags; also autosaved on topology / CONFIG flag changes; loaded on next boot if present)",
        ),
        bulk_static(
            b"OWNERS -- compressed slot ownership ranges with config epochs (gossip merge)",
        ),
        bulk_static(b"EPOCH -- current config epoch"),
        bulk_static(
            b"FAILREPORTS -- node ids this node marks pfail/fail (gossip quorum votes)",
        ),
        bulk_static(
            b"COUNT-FAILURE-REPORTS <node-id> -- active failure report count for a node",
        ),
        bulk_static(
            b"ROLEMAP -- roles [id, master|slave, master_id, ip, port, offset, priority] (election)",
        ),
        bulk_static(b"HELP -- print this help"),
    ])
}

fn cluster_rolemap_reply(cluster: &ClusterState) -> RespValue {
    let entries = cluster.role_map_snapshot();
    let mut out = Vec::with_capacity(entries.len());
    for e in entries {
        let role = if e.master { "master" } else { "slave" };
        let mid = if e.master_id.is_empty() {
            "-"
        } else {
            e.master_id.as_str()
        };
        out.push(RespValue::Array(vec![
            bulk(e.id),
            bulk(role),
            bulk(mid.to_string()),
            bulk(e.ip),
            RespValue::Integer(e.port as i64),
            RespValue::Integer(e.repl_offset as i64),
            RespValue::Integer(e.repl_priority as i64),
        ]));
    }
    RespValue::Array(out)
}

/// `CLUSTER SLOTS` — Redis format (Batch ET includes replicas).
///
/// Each entry: `[start, end, [master_ip, port, id], [replica_ip, port, id]…]`.
/// Master is always first; known non-fail replicas of that master follow
/// (sorted by id via [`ClusterState::replicas_of`]).
fn cluster_slots_reply(cluster: &ClusterState) -> RespValue {
    let ranges = cluster.slots_ranges();
    let mut out = Vec::with_capacity(ranges.len());
    for (start, end, master) in ranges {
        // Skip empty/unbound owners (slots_ranges may omit; belt-and-suspenders).
        if master.id.is_empty() {
            continue;
        }
        let mut entry = Vec::with_capacity(4);
        entry.push(RespValue::Integer(start as i64));
        entry.push(RespValue::Integer(end as i64));
        entry.push(RespValue::Array(vec![
            bulk(master.ip.clone()),
            RespValue::Integer(master.port as i64),
            bulk(master.id.clone()),
        ]));
        // Batch ET: append replica endpoints so clients can do replica reads.
        for rid in cluster.replicas_of(&master.id) {
            if let Some(r) = cluster.get_node(&rid) {
                entry.push(RespValue::Array(vec![
                    bulk(r.ip.clone()),
                    RespValue::Integer(r.port as i64),
                    bulk(r.id.clone()),
                ]));
            }
        }
        out.push(RespValue::Array(entry));
    }
    RespValue::Array(out)
}

/// `CLUSTER SHARDS` — one entry per master that owns slots (Batch EI).
///
/// RESP2 nested arrays (field/value pairs), Redis-7 shaped:
/// `[["slots", [s,e,…]], ["nodes", [[…node fields…], …]]]`
fn cluster_shards_reply(cluster: &ClusterState) -> RespValue {
    use std::collections::BTreeMap;
    let ranges = cluster.slots_ranges();
    // Group slot ranges by master id (stable order via BTreeMap).
    let mut by_master: BTreeMap<String, (ClusterNode, Vec<(u16, u16)>)> = BTreeMap::new();
    for (start, end, node) in ranges {
        // Skip unbound / missing — slots_ranges only emits known owners.
        let e = by_master
            .entry(node.id.clone())
            .or_insert_with(|| (node.clone(), Vec::new()));
        e.1.push((start, end));
    }

    let mut shards = Vec::with_capacity(by_master.len());
    for (master_id, (master, slot_pairs)) in by_master {
        let mut slots_flat = Vec::with_capacity(slot_pairs.len() * 2);
        for (s, e) in slot_pairs {
            slots_flat.push(RespValue::Integer(s as i64));
            slots_flat.push(RespValue::Integer(e as i64));
        }

        let mut nodes_arr = Vec::new();
        // Master first.
        nodes_arr.push(shard_node_fields(
            &master,
            "master",
            cluster.election_repl_offset(&master_id),
        ));
        // Then known replicas of this master.
        for rid in cluster.replicas_of(&master_id) {
            if let Some(rnode) = cluster.get_node(&rid) {
                let role = "replica";
                let off = cluster.election_repl_offset(&rid);
                nodes_arr.push(shard_node_fields(&rnode, role, off));
            }
        }

        shards.push(RespValue::Array(vec![
            bulk_static(b"slots"),
            RespValue::Array(slots_flat),
            bulk_static(b"nodes"),
            RespValue::Array(nodes_arr),
        ]));
    }
    RespValue::Array(shards)
}

fn shard_node_fields(node: &ClusterNode, role: &str, repl_offset: u64) -> RespValue {
    let health = if node.fail {
        "fail"
    } else if node.pfail {
        "loading"
    } else {
        "online"
    };
    let endpoint = format!("{}:{}", node.ip, node.port);
    RespValue::Array(vec![
        bulk_static(b"id"),
        bulk(node.id.clone()),
        bulk_static(b"endpoint"),
        bulk(endpoint),
        bulk_static(b"ip"),
        bulk(node.ip.clone()),
        bulk_static(b"port"),
        RespValue::Integer(node.port as i64),
        bulk_static(b"role"),
        bulk(role.to_string()),
        bulk_static(b"replication-offset"),
        RespValue::Integer(repl_offset as i64),
        bulk_static(b"health"),
        bulk(health.to_string()),
    ])
}

/// `CLUSTER OWNERS` reply: array of [start, end, owner_id, ip, port, epoch].
fn cluster_owners_reply(cluster: &ClusterState) -> RespValue {
    let ranges = cluster.ownership_snapshot();
    let mut out = Vec::with_capacity(ranges.len());
    for r in ranges {
        out.push(RespValue::Array(vec![
            RespValue::Integer(r.start as i64),
            RespValue::Integer(r.end as i64),
            bulk(r.owner_id),
            bulk(r.ip),
            RespValue::Integer(r.port as i64),
            RespValue::Integer(r.epoch as i64),
        ]));
    }
    RespValue::Array(out)
}
