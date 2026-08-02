# Changelog

All notable changes to Kore are documented here.  
Format loosely follows [Keep a Changelog](https://keepachangelog.com/). Version numbers track `Cargo.toml`.

The detailed letter-batch history lives in root [`TODO.md`](TODO.md). This file is the operator-facing summary.

## [Unreleased]

### Performance

- **Batch GC** — pure-standalone masters skip replication stream encode/backlog when no replica has ever been fed; SET path drops extra type probe, static write-cmd bytes, in-place map overwrite. Indicative SET pipeline (c=50 P=16) ~**+19%** vs Batch FI on M3 Pro (~741k ops/s). See `docs/benchmarks.md`.
- **Batch GD** — `CommandId` enum dispatch (length-first match on stack-uppercased name); ACL/cluster key-spec use stack lowercase instead of allocating `String`. SET P=16 remains in the GC band (~741k); structural hygiene for further path work.
- **Batch GE** — replication publish: backlog mutex no longer held across replica `try_send`; ordered deferred fan-out via `fanout_order` (feed order still matches backlog). Multi-replica write path only.
- **Batch GF** — full FD-style re-bench vs Valkey 9 post-GC/GD (Kore SET P=16 ~730k vs Valkey ~1.59M on M3 Pro; host-local only). See `docs/benchmarks.md`.

### Compatibility

- **Batch GG** — `MIGRATE` uses DUMP→RESTORE wire for string/list/set/hash/zset (Redis RDB object from FY); geo/stream still RESP recreate. Absolute expire via `RESTORE ABSTTL`.

### Planned (see TODO.md → *Next work queue (post-GB)*)

- **GK** — multi-language client smoke CI
- **GL** — TLS depth (mTLS, dual listener, optional replica-link TLS)
- **GI / GT–GU** — Redis Functions or search scoring/ANN polish (pick one track)

### Docs

- Stamped post-GB through post-GF status; README feature matrix; this changelog; roadmap pointer to **GC+**

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
- RESP3 `HELLO 3`; ACL MVP + LOG; TLS server; Unix sockets
- Prometheus metrics port; `HEALTH`; JSON logging; graceful shutdown

#### Kore differentiators
- Redlock remote backends; fair lock queue; deadlock detector (async API, victim strategies, snapshot merge, optional web UI)

#### Performance
- Pipeline SET path cuts (**FI**, ~+25% P=16 on M3 Pro vs prior); AOF-off multi-DB SELECT ordering (**FI-2**)
- Repl publish: drop exclusive fullsync Mutex on hot path (**GB**; ordered backlog residual remains)

### Known gaps (accepted or scheduled)

- Pipelined SET still below Valkey absolute throughput on measured host — **GC–GF**
- MIGRATE still recreate-only (not DUMP wire on the wire) — **GG**
- Redis Functions / FCALL stubs only — **GI**
- TLS: no mTLS / dual listener / replica link TLS — **GL**
- No binary Redis cluster bus; Sentinel hello SUBSCRIBE fan-in not implemented (tick PUBLISH + peer HELLO)

### Benchmarks

See [`docs/benchmarks.md`](docs/benchmarks.md) (Batch FD + FI re-measure notes). Numbers are host-local; not portable marketing claims.

## Earlier history

Pre-0.6.0 work (core types, pub/sub, first persistence, Redlock MVP, etc.) is tracked as completed checklists in [`TODO.md`](TODO.md) Phases A–E and the long letter-batch series (**AA** onward). Summarize new user-visible work under `[Unreleased]` or a new version heading when cutting a release.
