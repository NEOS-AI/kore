//! Redis Cluster compatibility: hash slots, redirects, membership gossip (RESP).
//!
//! Internal ahash-based cache sharding is independent of CRC16 cluster slots.
//!
//! Gossip uses the client RESP port (not Redis binary cluster bus). Fail
//! detection is **single-observer** (not Redis multi-node quorum).
//!
//! **Ownership epochs (Batch DU):** each slot has a config epoch stamped on
//! local `SETSLOT NODE` / reassign. Heartbeats pull `CLUSTER OWNERS` from peers
//! and merge with higher-epoch-wins (skip local MIGRATING/IMPORTING).
//!
//! **Dual-end NODE (Batch DV):** dest `SETSLOT NODE` first, then source; source
//! skipped if dest fails. Stale lower-epoch gossip cannot flip ownership back.
//!
//! **Fail quorum (Batch DW):** `pfail` on timeout; `fail` after master vote
//! quorum (`masters/2+1`, or 1 when ≤2 masters). `CLUSTER FAILREPORTS` gossip.
//!
//! **Reshard planner (Batch DX):** `CLUSTER RESHARD PLAN|AUTO` — greedy donor
//! selection + optional execute (local `reshard_slot` / remote RESP RESHARD).
//!
//! **Replica election (Batch DY/EA/EB):** priority (0 never), then offset, then
//! max id; ROLEMAP carries both. **Loser reconfig (Batch DZ):** follow winner.
//! **Manual failover (Batch EC):** `CLUSTER FAILOVER [FORCE|TAKEOVER]`.
//! **Ops helpers (Batch ED):** COUNTKEYSINSLOT / GETKEYSINSLOT / REPLICAS / BUMPEPOCH.
//! **Slot bootstrap (Batch EE/EF):** ADDSLOTS / DELSLOTS / FLUSHSLOTS /
//! ADDSLOTSRANGE / DELSLOTSRANGE.
//! **Topology reset (Batch EG):** FORGET / RESET SOFT|HARD.
//! **NODE safety (Batch EH):** partial_source re-asserts MIGRATING; COUNT-FAILURE-REPORTS.
//! **Topology views (Batch EI):** CLUSTER SHARDS (Redis-7 style); LINKS empty.
//! **NODE verify (Batch EJ):** post-commit dual ownership check; MYSHARDID.
//! **NODE compensate (Batch EP):** dest ownership rollback on partial source NODE
//! (`rolled_back` status + IMPORTING restore).
//! **Epoch control (Batch EK):** CLUSTER SET-CONFIG-EPOCH.
//! **Live config (Batch EL/EQ/ES/EU/FL):** CONFIG cluster-replica-priority /
//! cluster-node-timeout / cluster-require-full-coverage /
//! cluster-allow-reads-when-down / cluster-announce-ip|port; honest `cluster_state`.
//! Live flags (except node-timeout) persist in `nodes.conf` header and restore
//! on boot; CONFIG SET autosaves when dir is set (Batch FL).
//! **Replica reads (Batch ER):** connection `READONLY` allows serving reads for
//! slots owned by this node's master (writes still MOVED).
//! **CLUSTER SLOTS (Batch ET):** each range lists master then known replicas.
//! **SLOT-STATS (Batch EV):** `CLUSTER SLOT-STATS SLOTSRANGE` local key counts.
//! **NODE preflight (Batch EY):** dual-end NODE prepare (MYID + owner check)
//! before commit; `failed_preflight` without half-apply.
//! **NODE 2PC slice (Batch FB/FH/FO):** `SETSLOT PREPARE` / `ABORTPREPARE` /
//! `CHECKPREPARE` / `COMMITPREPARE` on source+dest; prepare-epoch + wall-clock
//! TTL fence; commit re-check; atomic COMMITPREPARE (check+NODE); dest-first
//! commit; EP rollback on source commit fail; status `failed_prepare` /
//! `failed_prepare:recheck:…` without half-apply. Prepare votes persist in
//! `nodes.conf` as `# prepare <slot> <target> <epoch> <unix_ms>` (Batch FO);
//! expired/malformed lines dropped on load (fail-closed).
//! **Persistence (Batch EM/EN/EO/FL/FO):** CLUSTER SAVECONFIG → dir/nodes.conf; load on
//! boot via [`ClusterState::load_or_single_node`]; best-effort autosave after
//! topology-mutating CLUSTER ops, failover claim, live-flag CONFIG SET, and
//! prepare set/abort/commit. Header `# key value` comments carry require-full /
//! allow-reads / announce / replica-priority / prepare votes (legacy files
//! without keys keep defaults / empty prepare map).
//!
//! Slot migration (thin MVP): `CLUSTER MIGRATEKEYS` moves all key types over RESP.
//! Orchestration: `CLUSTER RESHARD` runs the documented SETSLOT + MIGRATEKEYS flow
//! with dual-end NODE verify+retry; `CLUSTER RESHARD FINISH` completes NODE only.
//! Partial key-move progress is reported on `failed_keys`; range aborts on any
//! non-`complete` status (Batch DO). Redis key-level `MIGRATE` reuses the same
//! recreate path (Batch DP).

mod crc16;
mod gossip;
mod migrate;
mod state;

pub use crc16::{crc16, key_hash_slot, SLOT_COUNT};
pub use gossip::{
    force_mark_fail, gossip_tick, meet_peer, parse_fail_reports_reply, parse_owners_reply,
    parse_rolemap_reply, run_cluster_gossip,
};
pub use migrate::{
    execute_reshard_plan, finish_slot_node, keys_in_slot, migrate_keys_to,
    migrate_one_key_on_stream, migrate_slot_keys, migrate_slot_string_keys, plan_reshard,
    reshard_slot, reshard_slots, string_keys_in_slot, test_acquire_dest_node_inject,
    test_acquire_dest_prepare_inject, test_acquire_migrate_key_inject, test_commit_recheck_inject,
    test_inject_dest_node_failures, test_source_node_inject, test_source_prepare_inject,
    CommitRecheckInjectGuard, DestNodeInjectGuard, DestPrepareInjectGuard, MigrateCommandResult,
    MigrateDestAuth, MigrateKeyInjectGuard, MigrateKeyOpts, MigrateOneOutcome, MigrateSlotError,
    MigrateSlotResult, ReshardPlanEntry, ReshardSlotResult, SourceNodeInjectGuard,
    SourcePrepareInjectGuard,
};
pub use state::{
    ClusterNode, ClusterState, ManualFailoverMode, OwnershipApplyResult, OwnershipRange,
    RedirectTarget, RoleMapEntry, DEFAULT_NODE_TIMEOUT_MS, PREPARE_VOTE_TTL,
};
