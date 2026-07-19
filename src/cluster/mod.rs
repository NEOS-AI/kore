//! Redis Cluster compatibility: hash slots, redirects, membership gossip (RESP).
//!
//! Internal ahash-based cache sharding is independent of CRC16 cluster slots.
//!
//! Gossip uses the client RESP port (not Redis binary cluster bus). Fail
//! detection is **single-observer** (not Redis multi-node quorum).
//!
//! Slot migration (thin MVP): `CLUSTER MIGRATEKEYS` moves all key types over RESP.
//! Orchestration: `CLUSTER RESHARD` runs the documented SETSLOT + MIGRATEKEYS flow
//! with dual-end NODE verify+retry; `CLUSTER RESHARD FINISH` completes NODE only.

mod crc16;
mod gossip;
mod migrate;
mod state;

pub use crc16::{crc16, key_hash_slot, SLOT_COUNT};
pub use gossip::{force_mark_fail, gossip_tick, meet_peer, run_cluster_gossip};
pub use migrate::{
    finish_slot_node, keys_in_slot, migrate_slot_keys, migrate_slot_string_keys, reshard_slot,
    reshard_slots, string_keys_in_slot, test_acquire_dest_node_inject,
    test_inject_dest_node_failures, DestNodeInjectGuard, MigrateSlotResult, ReshardSlotResult,
};
pub use state::{ClusterNode, ClusterState, RedirectTarget, DEFAULT_NODE_TIMEOUT_MS};
