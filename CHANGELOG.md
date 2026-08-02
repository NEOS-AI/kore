# Changelog

All notable changes to Kore are documented here.  
Format loosely follows [Keep a Changelog](https://keepachangelog.com/). Version numbers track `Cargo.toml`.

The detailed letter-batch history lives in root [`TODO.md`](TODO.md). This file is the operator-facing summary.

## [Unreleased]

### Added

- **GI** — Redis Functions library beyond stubs: shared `FunctionLibraryStore`; `FUNCTION LOAD`/`LIST`/`DELETE`/`FLUSH`/`DUMP`/`RESTORE`/`STATS`; real `FCALL`/`FCALL_RO` via `redis.register_function` and `#!lua name=` shebang. Dump format is Kore-portable `KORF1` (not Redis binary blob). Not yet durable in RDB/AOF.
- **GJ** — Lua polish: `redis.setresp(2|3)`; `CONFIG GET|SET lua-time-limit` (default 5000 ms, `0` unlimited); hard script timeout; real `SCRIPT KILL` / `FUNCTION KILL` with write tracking (UNKILLABLE).
- **GT** — Real TF-IDF from inverted-index term frequencies + TEXT field WEIGHT; FT.SEARCH ranks by score; `WITHSCORES` / `NOCONTENT`.
- **GU** — Adaptive HNSW search `ef` for large-k; mid-N (N=800, k=50) CI recall@k vs FLAT; `set_ef_search` / `search_with_ef`.
- **GM** — Admin HTTP security: Bearer/Basic auth for metrics + deadlock UI; optional admin TLS; `--admin-bind` (non-loopback requires auth).
- **GN** — Per-slot config epochs persisted in `nodes.conf` (`# slot-epoch start end epoch`); restore on boot.
- **GO** — Dual-end reshard commit: dest prepare fence only via atomic `COMMITPREPARE` (no separate CHECKPREPARE RPC).
- **GP** — Kore peer bus lite for dual-end NODE 2PC: length-prefixed `KORB` frames (PREPARE/COMMIT/ABORT/PING) on `client_port+10000`; prefer bus when cport accepts, RESP fallback on transport failure. **Not** the Redis cluster bus (no gossip/MEET opcodes).
- **GQ** — Parallel Sentinel peer PINGs and replica `slave_priority` INFO probes.
- **GR** — HNSW bridge reconnect prune floor `max(max_m, 2)` so upper-layer `M=1` path middles keep both spanning edges (not a global insert degree change).
- **GS** — Search-doc access-touch for LRU/LFU: FT search hits update per-doc `last_access` (+ packed LFU); `allkeys-lru` sampling uses real times so hot FT docs can outrank cold ones. Still not volatile victims.

### Planned (see TODO.md)

- Optional further residuals only (full Redis cluster bus, BM25 stemmers, …)

## [0.7.0] — 2026-08-02

**Productization cut** after letter batches **GC–GH**, **GK**, **GL**. Baseline Redis/HA/cluster/search story was **0.6.0** (through **GB**); this release hardens ops, wire compat, TLS, and client smoke.

### Performance

- **GC** — pure-standalone masters skip replication stream encode/backlog when no replica has been fed; SET path cuts (type probe, static write-cmd bytes, in-place map overwrite). Indicative SET P=16 ~**+19%** vs Batch FI (~741k ops/s on M3 Pro).
- **GD** — `CommandId` enum dispatch; stack ACL lowercase (no per-command `String` lowercasing).
- **GE** — ordered deferred repl fan-out (`fanout_order`); backlog mutex not held across replica `try_send`.
- **GF** — full re-bench vs Valkey 9 (host-local; SET P=16 ~730k vs Valkey ~1.59M). See `docs/benchmarks.md`.

### Compatibility

- **GG** — `MIGRATE` via DUMP→RESTORE for core types; absolute expire with `RESTORE ABSTTL`.
- **GH** — Geo DUMP as Redis ZSET_2 (geohash scores); stream type-15 + Kore `KST1` body; MIGRATE dump path for all types; legacy KDF1 still accepted.
- **GK** — `scripts/client_smoke.sh` + CI (redis-cli + redis-py).

### Security

- **GL** — dual TLS listener (`--tls-port`), mTLS (`--tls-auth-clients` + `--tls-ca`), replica→primary TLS (`--tls-replication`).

### Docs / release

- **GC-rel** — this **0.7.0** cut; production runbook [`docs/ops.md`](docs/ops.md); changelog/README/TODO stamp.

### Known gaps

- Pipelined SET still below Valkey absolute throughput on measured host
- Redis Functions dump is Kore `KORF1` (not Redis-native blob); libraries not yet in RDB/AOF
- Stream foreign Redis listpack DUMP fixtures residual; geo DUMP restores as zset (Redis TYPE)
- No full Redis cluster bus (Kore peer bus lite covers dual-end NODE 2PC only; **GP**); Sentinel hello SUBSCRIBE fan-in not implemented
- Admin HTTP has optional auth/TLS (**GM**); no admin mTLS / rate limits

## [0.6.0] — 2026-07-31

Baseline product cut after letter batches through **GB**. Not every batch is listed; highlights:

### Added / hardened

#### Persistence & keyspace
- Unified keyspace (`KeyValue` / `KeySlot`); slot-only TTL; `Entry.expires_at` retired (**FG-4**, **FP–FQ**, **FU**, **GA**)
- RDB `KORDB` multi-DB + search sections; AOF rewrite/load including FT schema/aliases
- Redis-compatible **DUMP/RESTORE** wire for string/list/set/hash/zset; geo/stream **KDF1** dual-detect (**FY**)

#### Replication & HA
- `SYNC` / `PSYNC` + backlog; `WAIT`; min-replicas write gate
- Coordinated `FAILOVER TO`; promote ranking and election-timeout SM
- **Sentinel-lite**: MONITOR, ODOWN quorum, hello bus lite, conf persistence, CKQUORUM live probe (**EW–FN**, **FT**)

#### Cluster
- Hash slots, MOVED/ASK, gossip, fail quorum, reshard PLAN/AUTO
- Dual-end NODE prepare/commit 2PC (RESP); durable prepare in `nodes.conf` (**FB**, **FH**, **FO**)
- `nodes.conf` load/autosave + live cluster flags; require-full-coverage / allow-reads-when-down / announce (**EM–EU**, **FL**)

#### Search
- FT.CREATE / SEARCH / aliases; ACL `@search`
- **HNSW** multi-layer graph; RDB + AOF durability; ANN query path for FT.SEARCH (**FF**, **FV–FX**, **FW**)
- Proportional search-doc eviction under `allkeys-*` (**FZ**)

#### Protocol, security, ops
- RESP3 `HELLO 3`; ACL MVP + LOG; TLS server (MVP); Unix sockets
- Prometheus metrics port; `HEALTH`; JSON logging; graceful shutdown

#### Kore differentiators
- Redlock remote backends; fair lock queue; deadlock detector (async API, victim strategies, snapshot merge, optional web UI)

#### Performance
- Pipeline SET path cuts (**FI**, ~+25% P=16 on M3 Pro vs prior); AOF-off multi-DB SELECT ordering (**FI-2**)
- Repl publish: drop exclusive fullsync Mutex on hot path (**GB**; ordered backlog residual remains)

### Known gaps at 0.6.0 (many closed in 0.7.0)

- Pipelined SET below Valkey — partially addressed in **GC–GF**
- MIGRATE recreate-only — **GG/GH**
- TLS mTLS / dual / replica link — **GL**
- Redis Functions stubs — still **GI**
- No binary Redis cluster bus; Sentinel hello SUBSCRIBE residual

## Earlier history

Pre-0.6.0 work (core types, pub/sub, first persistence, Redlock MVP, etc.) is tracked as completed checklists in [`TODO.md`](TODO.md) Phases A–E and the long letter-batch series (**AA** onward).
