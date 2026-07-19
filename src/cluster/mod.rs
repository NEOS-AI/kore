//! Redis Cluster compatibility: hash slots, redirects, membership gossip (RESP).
//!
//! Internal ahash-based cache sharding is independent of CRC16 cluster slots.
//!
//! Gossip uses the client RESP port (not Redis binary cluster bus). Fail
//! detection is **single-observer** (not Redis multi-node quorum).
//!
//! Slot migration (thin MVP): `CLUSTER MIGRATEKEYS` moves all key types over RESP.
//! Orchestration: `CLUSTER RESHARD` runs the documented SETSLOT + MIGRATEKEYS flow.

mod crc16;
mod gossip;
mod migrate;
mod state;

pub use crc16::{crc16, key_hash_slot, SLOT_COUNT};
pub use gossip::{force_mark_fail, gossip_tick, meet_peer, run_cluster_gossip};
pub use migrate::{
    keys_in_slot, migrate_slot_keys, migrate_slot_string_keys, reshard_slot, reshard_slots,
    string_keys_in_slot, MigrateSlotResult, ReshardSlotResult,
};
pub use state::{ClusterNode, ClusterState, RedirectTarget, DEFAULT_NODE_TIMEOUT_MS};
