# Kore TODO

Actionable work derived from codebase analysis. Goal: a production-ready Redis/Valkey replacement, with Kore-specific differentiators (Redlock, search, fair locks) layered on a correct baseline.

Legend: `[ ]` pending · `[x]` done

---

## Priority model

Work is ordered by **two layers**:

1. **Phase order** (when to do a whole area of work)
2. **Item priority** (how urgent an item is within / across phases)

### Phase priority (do in this order)

| Rank | Phase | Priority | Why |
|------|--------|----------|-----|
| 1 | **A** — Correctness & production basics | **Critical** | Wrong answers, races, broken config — blocks everything else |
| 2 | **B** — Persistence & HA | **Critical** | Without durable storage / replicas, not production-ready |
| 3 | **C** — Core Redis types & commands | **High** | Required for real Redis client workloads |
| 4 | **D** — Scale, protocol & security | **Medium** | Cluster, RESP3, ACL/TLS for multi-tenant / large deploys |
| 5 | **E** — Differentiators | **Low** (after A–C) | Redlock, search, pub/sub polish — only after baseline is solid |
| — | Engineering quality | **Ongoing** | CI, benchmarks, fuzz — parallel to all phases |

### Item priority tags

Every checklist item is tagged:

| Tag | Name | Meaning |
|-----|------|---------|
| **`[P0]`** | Must-fix | Correctness, data loss risk, or hard production blocker |
| **`[P1]`** | Important | Compatibility, ops, performance under real load |
| **`[P2]`** | Later | Nice-to-have, niche, or dependent on earlier work |

**Rule of thumb:** Prefer **`[P0]` in earlier phases** over any **`[P1]`/`[P2]` in later phases**.  
Example: fix EXAT (`A` / `P0`) before RESP3 (`D` / `P1`) or HNSW benchmarks (`E` / `P2`).

### Recommended execution order (top of queue)

1. Phase A — all **`[P0]`** (bugs, keyspace, tests)
2. Phase A — **`[P1]`** (ops, async cleanup)
3. Phase B — all **`[P0]`** (RDB/AOF, replication)
4. Phase C — **`[P0]`** then **`[P1]`**
5. Phase D — **`[P1]`** (most items)
6. Phase E — **`[P1]`** then **`[P2]`**
7. Engineering quality — continuous, especially **`[P0]`** tests tied to the phase you are in

---

## Phase A — Correctness & production basics

**Phase priority: Critical (1st)**

### Bugs & data integrity

- [ ] **`[P0]`** **EXAT / PXAT**: treat values as absolute Unix timestamps, not relative durations; use wall-clock time (`SystemTime` / epoch ms), not `Instant`
- [ ] **`[P0]`** **Atomic INCR / DECR**: perform get-compute-store under a single shard write lock (or equivalent RMW path) to prevent lost updates
- [ ] **`[P0]`** **Atomic SET NX / XX / CAS**: hold shard lock across check-and-insert to eliminate TOCTOU races
- [ ] **`[P0]`** **Memory accounting — single source of truth**
  - [ ] **`[P0]`** Collapse dual tracking (`memory_usage` + `MemoryTracker`) or keep them strictly in sync
  - [ ] **`[P0]`** Fix order: check/allocate before commit, or roll back insert on allocate failure
  - [ ] **`[P0]`** Update global memory on expired `load` removal (include `MemoryTracker`)
  - [ ] **`[P0]`** Update global memory on background / manual `sweep_expired`
  - [ ] **`[P0]`** Reset `MemoryTracker` (all categories) on `FLUSHDB` / `FLUSHALL`
- [ ] **`[P0]`** **Eviction sampling**: make `Shard::get_random` truly random (not `HashMap::iter().next()`); avoid biased LRU
- [ ] **`[P0]`** **Enforce `maxconns`**: reject or queue accepts when active connections ≥ config limit
- [ ] **`[P0]`** **Honor `--threads`**: build Tokio runtime with configured worker thread count (today only logged)
- [ ] **`[P1]`** **Use or drop `loadfactor`**: wire into shard capacity / resize policy, or remove from CLI and validation

### Keyspace model

- [ ] **`[P0]`** **Unified keyspace**: store strings, zsets, geo (and future types) under one map keyed by name
- [ ] **`[P0]`** **Type safety**: Redis-style type errors when a key exists with a different type
- [ ] **`[P0]`** **Cross-type ops**: `DEL`, `EXISTS`, `KEYS`/`SCAN`, `DBSIZE`, `TTL`/`EXPIRE`, `TYPE` work for all types
- [ ] **`[P0]`** **Eviction / maxmemory**: account for zset, geo, search indexes, and pub/sub buffers—not only string KV

### Server / ops hygiene

- [ ] **`[P1]`** Graceful shutdown (SIGTERM/SIGINT): stop accepts, drain in-flight commands, flush persistence when present
- [ ] **`[P1]`** Implement `SCAN` (cursor-based); de-emphasize `KEYS` for production use
- [ ] **`[P1]`** Implement `TYPE`
- [ ] **`[P1]`** Wire `CONFIG SET` through to live cache atomics (e.g. changing `maxmemory` should re-evict)
- [ ] **`[P2]`** Reduce connection-lifecycle log noise at default verbosity

### Networking / async

- [ ] **`[P1]`** Remove `block_in_place` + nested `block_on` from pub/sub command path; keep handlers fully async
- [ ] **`[P1]`** Align lock types (`parking_lot` vs `std::sync` vs `tokio::sync`) with sync vs async call sites
- [ ] **`[P2]`** Audit unused deps (`dashmap`, `crossbeam`): use deliberately or remove from `Cargo.toml`

### Testing for Phase A

- [ ] **`[P0]`** Concurrency stress tests: concurrent `INCR`, `SET NX`, CAS under load
- [ ] **`[P0]`** Memory accounting tests: store / replace / expire / sweep / flush leave consistent totals
- [ ] **`[P0]`** EXAT/PXAT unit tests against wall-clock timestamps
- [ ] **`[P0]`** Real network integration tests (replace `network.rs` placeholders): PING, SET/GET, auth, maxconns

---

## Phase B — Persistence & high availability

**Phase priority: Critical (2nd)**

### Persistence

Also tracked in `docs/roadmap.md`.

- [ ] **`[P0]`** Export / snapshot to **RDB**
- [ ] **`[P0]`** **AOF** append log + rewrite
- [ ] **`[P0]`** Load data from file on startup (init from RDB and/or AOF)
- [ ] **`[P1]`** Configurable save policies (interval, change thresholds) and `BGSAVE` / `LASTSAVE`-style commands

### Replication & failover

- [ ] **`[P0]`** Async replication (replica of primary)
- [ ] **`[P1]`** Partial resync / backlog where feasible (PSYNC-style)
- [ ] **`[P1]`** Replica read path
- [ ] **`[P1]`** Failover story (external Sentinel-compatible or built-in later)

---

## Phase C — Core Redis data types & commands

**Phase priority: High (3rd)**

### Data structures

- [ ] **`[P0]`** **Hashes** (`HSET`, `HGET`, `HMGET`, `HDEL`, `HGETALL`, …)
- [ ] **`[P0]`** **Lists** (`LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `BLPOP`, …)
- [ ] **`[P0]`** **Sets** (`SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SINTER`, …)
- [ ] **`[P0]`** Transactions: `MULTI` / `EXEC` / `DISCARD` / `WATCH`
- [ ] **`[P1]`** Streams + consumer groups
- [ ] **`[P2]`** Bitmaps / bitfields, HyperLogLog

### Command coverage

- [ ] **`[P1]`** Common string ops: `APPEND`, `STRLEN`, `SETEX`, `GETSET`, `UNLINK`, `RENAME` / `RENAMENX`
- [ ] **`[P1]`** `CLIENT`, `COMMAND`, `HELLO`
- [ ] **`[P1]`** Multi-DB: `SELECT` (or explicitly document single-DB only)
- [ ] **`[P2]`** Lua scripting / functions

### Memory & expiration policy

- [ ] **`[P1]`** Eviction policies: `allkeys-lru`, `volatile-lru`, `allkeys-lfu`, `volatile-ttl`, `noeviction`
- [ ] **`[P1]`** Approximated LFU (Redis-style)
- [ ] **`[P1]`** Active expire sampling (avoid full-shard `retain` pauses on large datasets)
- [ ] **`[P2]`** More accurate memory sizing (allocator overhead, index structures)

### Sorted set / geo performance

- [ ] **`[P1]`** Shard zsets/geo like the main map (remove single global `RwLock` bottleneck)
- [ ] **`[P1]`** O(log n) rank (`ZRANK` / `ZREVRANK`) — skiplist or ranked tree instead of BTreeMap scan

---

## Phase D — Scale, protocol & security

**Phase priority: Medium (4th)**

### Cluster

Also tracked in `docs/roadmap.md`.

- [ ] **`[P1]`** Hash slots / key hashing compatible with Redis Cluster clients
- [ ] **`[P1]`** Gossip / membership and failover
- [ ] **`[P1]`** Resharding / slot migration

### Protocol & clients

- [ ] **`[P1]`** RESP3 support (`HELLO 3`, maps, bools, push)
- [ ] **`[P1]`** Zero/low-alloc command dispatch (avoid per-command `String` uppercasing; static table / perfect hash)
- [ ] **`[P2]`** Pipelining / write batching optimizations under load

### Security

- [ ] **`[P1]`** ACL (users, command/key permissions)
- [ ] **`[P1]`** TLS
- [ ] **`[P2]`** Unix domain socket option

### Observability

- [ ] **`[P1]`** Prometheus metrics endpoint and/or richer Redis-compatible `INFO` sections
- [ ] **`[P2]`** Optional structured (JSON) logging
- [ ] **`[P1]`** Health / readiness beyond bare `PING` (memory, persistence lag)

---

## Phase E — Differentiators (after baseline is solid)

**Phase priority: Low (5th)** — harden only after Phases A–C are green

### Redlock & locking

- [ ] **`[P1]`** Ensure Redlock CLI flags actually wire into the running server path
- [ ] **`[P1]`** Fair lock queueing: production hardening, metrics, docs
- [ ] **`[P2]`** Deadlock detection advanced (from roadmap)
  - [ ] **`[P2]`** Cross-process detection
  - [ ] **`[P2]`** Async support
  - [ ] **`[P2]`** Custom victim selection strategies
  - [ ] **`[P2]`** Web UI monitoring

### Search & vectors

- [ ] **`[P1]`** Document and test `FT.SEARCH` end-to-end over RESP (not only programmatic indexing)
- [ ] **`[P1]`** Memory limits and eviction interaction for indexes
- [ ] **`[P2]`** HNSW correctness/performance benchmarks vs FLAT

### Pub/Sub

- [ ] **`[P1]`** Slow-client and memory limits under fan-out load
- [ ] **`[P1]`** Pattern matcher: iterative (or bounded) matching to avoid deep recursion stack risk

---

## Engineering quality (ongoing)

**Phase priority: Ongoing** — run in parallel; raise priority when touching related code

- [ ] **`[P0]`** Tests for the phase you are implementing (always land with the feature)
- [ ] **`[P1]`** **CI**: build, unit tests, integration tests, optional redis-cli compatibility smoke
- [ ] **`[P1]`** **Benchmarks**: expand `docs/benchmarks.md` with methodology and numbers vs Redis/Valkey (`redis-benchmark`, same hardware)
- [ ] **`[P1]`** **Fuzz** RESP parser and command argument parsing
- [ ] **`[P1]`** **Concurrency / loom or stress** jobs for shard RMW paths
- [ ] **`[P2]`** Align version strings in docs/`INFO` examples with `Cargo.toml` (currently 0.6.0)
- [ ] **`[P2]`** Consistent locking and error handling guidelines in contributor docs
- [ ] **`[P2]`** Keep `docs/roadmap.md` in sync with this file (or make this the single source of truth)

---

## Quick reference: all P0 items

Highest urgency checklist (phase order preserved):

**A**

- [ ] EXAT / PXAT absolute timestamps
- [ ] Atomic INCR / DECR
- [ ] Atomic SET NX / XX / CAS
- [ ] Memory accounting (single source + all fix-ups)
- [ ] True random eviction sampling
- [ ] Enforce `maxconns`
- [ ] Honor `--threads`
- [ ] Unified keyspace + type safety + cross-type ops + maxmemory for all types
- [ ] Phase A concurrency / memory / EXAT / network tests

**B**

- [ ] RDB export
- [ ] AOF + rewrite
- [ ] Load from file on startup
- [ ] Async replication

**C**

- [ ] Hashes, Lists, Sets
- [ ] Transactions (`MULTI` / `EXEC` / `WATCH`)

When picking work: finish this list before large **`[P1]`/`[P2]`** feature work in Phases D–E.
