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

- [x] **`[P0]`** **EXAT / PXAT**: treat values as absolute Unix timestamps, not relative durations; use wall-clock time (`SystemTime` / epoch ms), not `Instant`
- [x] **`[P0]`** **Atomic INCR / DECR**: perform get-compute-store under a single shard write lock (or equivalent RMW path) to prevent lost updates
- [x] **`[P0]`** **Atomic SET NX / XX / CAS**: hold shard lock across check-and-insert to eliminate TOCTOU races
- [x] **`[P0]`** **Memory accounting — single source of truth**
  - [x] **`[P0]`** Collapse dual tracking (`memory_usage` + `MemoryTracker`) or keep them strictly in sync
  - [x] **`[P0]`** Fix order: check/allocate before commit, or roll back insert on allocate failure
  - [x] **`[P0]`** Update global memory on expired `load` removal (include `MemoryTracker`)
  - [x] **`[P0]`** Update global memory on background / manual `sweep_expired`
  - [x] **`[P0]`** Reset `MemoryTracker` (all categories) on `FLUSHDB` / `FLUSHALL`
- [x] **`[P0]`** **Eviction sampling**: make `Shard::get_random` truly random (not `HashMap::iter().next()`); avoid biased LRU
- [x] **`[P0]`** **Enforce `maxconns`**: reject or queue accepts when active connections ≥ config limit
- [x] **`[P0]`** **Honor `--threads`**: build Tokio runtime with configured worker thread count (today only logged)
- [x] **`[P1]`** **Use or drop `loadfactor`**: wire into shard capacity / resize policy, or remove from CLI and validation
  - *Done*: `Cache::new_with_sweep_loadfactor` sets per-shard capacity to `(1024.0 / loadfactor.max(0.55)).max(16)`; CLI still validates 0.55–0.95

### Keyspace model

- [x] **`[P0]`** **Unified keyspace**: store strings, zsets, geo (and future types) under one map keyed by name
  - *Done pragmatically*: separate maps + type registry / cross-type ops
  - *Batch FG (slice A)*: `KeyValue` view enum + facade for TYPE/DEL/EXISTS/`key_type`; design + migration plan documented
  - *Batch FG-2*: **hashes** physically stored as `KeyValue::Hash` in `Cache::key_values` (`ShardedKeyMap`); legacy global hash map removed
  - *Batch FG-3*: **list / set / zset / geo / stream** in `key_values`; legacy per-type maps removed; `KeyspacePayload` is `map` + `key_values` streams
  - *Batch FG-4*: **strings** as `KeyValue::String` in `key_values`; dual `Cache::map` removed; `KeyspacePayload` is one `key_values` stream; `typed_expires` residual closed by **FP**
- [x] **`[P0]`** **Type safety**: Redis-style type errors when a key exists with a different type
- [x] **`[P0]`** **Cross-type ops**: `DEL`, `EXISTS`, `KEYS`/`SCAN`, `DBSIZE`, `TTL`/`EXPIRE`, `TYPE` work for all types
  - *Done*: `SCAN` implemented (cursor-based, sorted key index); `KEYS`/`DBSIZE`/`DEL`/`EXISTS`/`TYPE`/`FLUSH` cover all types
  - *Batch AE*: `EXPIRE`/`PEXPIRE`/`TTL`/`PTTL` on hash/list/set/zset/geo/stream (side expire map); lazy + active expire; RENAME keeps TTL
  - *Batch AF*: `PERSIST`, `EXPIREAT`/`PEXPIREAT`, `EXPIRETIME`/`PEXPIRETIME`; zero/past absolute expire deletes key; wired for AOF/replication/Lua/COMMAND
  - *Batch BA*: `EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT` optional `NX|XX|GT|LT`
- [x] **`[P0]`** **Eviction / maxmemory**: account for zset, geo, search indexes, and pub/sub buffers—not only string KV
  - *Done*: zset/geo/hash/list/set/stream/search tracked in `MemoryTracker` and count toward maxmemory; FG-3 eviction samples strings + unified `key_values` (search docs still special)

### Server / ops hygiene

- [x] **`[P1]`** Graceful shutdown (SIGTERM/SIGINT): stop accepts, drain in-flight commands, flush persistence when present
  - *Done*: `tokio::signal` → watch channel → `Server::run_with_shutdown`; SAVE on stop when persistence present
- [x] **`[P1]`** Implement `SCAN` (cursor-based); de-emphasize `KEYS` for production use
  - *Batch AJ*: `HSCAN` / `SSCAN` / `ZSCAN` (MATCH/COUNT; stable sorted cursor; missing key → empty page)
- [x] **`[P1]`** Implement `TYPE`
- [x] **`[P1]`** Wire `CONFIG SET` through to live cache atomics (e.g. changing `maxmemory` should re-evict)
  - *Done*: `max_memory` is `AtomicUsize`; `CONFIG GET/SET maxmemory` + `maxentrysize`; best-effort re-evict on lower limit
- [x] **`[P2]`** Reduce connection-lifecycle log noise at default verbosity
  - *Done*: connection open/close (and close-with-error) demoted from `info!` → `debug!` so default verbosity stays quiet

### Networking / async

- [x] **`[P1]`** Remove `block_in_place` + nested `block_on` from pub/sub command path; keep handlers fully async
- [x] **`[P1]`** Align lock types (`parking_lot` vs `std::sync` vs `tokio::sync`) with sync vs async call sites
  - *Done*: sync keyspace / type maps / search indices use `parking_lot::RwLock` (no poison unwraps); `tokio::sync` kept for async pub/sub, network, replication channels; `parking_lot::Mutex` for short sync critical sections
- [x] **`[P2]`** Audit unused deps (`dashmap`, `crossbeam`): use deliberately or remove from `Cargo.toml`
  - *Done*: removed unused `dashmap` and `crossbeam` from `Cargo.toml` / lockfile

### Testing for Phase A

- [x] **`[P0]`** Concurrency stress tests: concurrent `INCR`, `SET NX`, CAS under load
- [x] **`[P0]`** Memory accounting tests: store / replace / expire / sweep / flush leave consistent totals
- [x] **`[P0]`** EXAT/PXAT unit tests against wall-clock timestamps
- [x] **`[P0]`** Real network integration tests (replace `network.rs` placeholders): PING, SET/GET, auth, maxconns
  - *Done*: PING/SET/GET/maxconns + AUTH (NOAUTH / wrong / correct)

---

## Phase B — Persistence & high availability

**Phase priority: Critical (2nd)**

### Persistence

Also tracked in `docs/roadmap.md`.

- [x] **`[P0]`** Export / snapshot to **RDB**
  - Kore binary format (`KORDB`); commands: `SAVE`, `BGSAVE`, `LASTSAVE`
- [x] **`[P0]`** **AOF** append log + rewrite
  - RESP command log; `BGREWRITEAOF`; live append on writes when `--appendonly`
- [x] **`[P0]`** Load data from file on startup (init from RDB and/or AOF)
  - Prefer AOF if appendonly; else RDB (`--dir`, `--dbfilename`, `--appendfilename`)
- [x] **`[P1]`** Configurable save policies (interval, change thresholds) and `BGSAVE` / `LASTSAVE`-style commands
  - *Done*: Redis-style `save <sec> <changes>` via `--save` / `CONFIG GET|SET save`; dirty counter; 1s auto-BGSAVE scheduler; `BGSAVE` / `LASTSAVE` / `SAVE`

### Replication & failover

- [x] **`[P0]`** Async replication (replica of primary)
  - `SYNC` full resync (RDB bulk) + command stream; `REPLICAOF` / `--replicaof`; readonly replica
- [x] **`[P1]`** Partial resync / backlog where feasible (PSYNC-style)
  - *Done*: `PSYNC` full (`? -1` / mismatch → `+FULLRESYNC` + RDB) or partial (matching replid + offset in 1MiB circular backlog → `+CONTINUE` + backlog); `master_repl_offset` / `replid`; replica loop uses PSYNC with cached id/offset; `REPLCONF` handshake + GETACK
- [x] **`[P1]`** Replica read path
  - *Done*: readonly replica rejects writes (`READONLY …`); serves reads (GET/EXISTS/TYPE/…); `ROLE`; `INFO` `# Replication` section (role, offsets, backlog, master_host)
- [x] **`[P1]`** Failover story (external Sentinel-compatible or built-in later)
  - *Hardened (Batch AC)*: full-resync holds a gate across RDB snapshot + feed register so concurrent `propagate_raw` cannot drop writes; sibling FAILOVER test waits for both `master_link_up` + `WAIT` before asserting
  - *Done (minimal + coordinated MVP-lite)*: honest promote via `REPLICAOF NO ONE` / bare `FAILOVER` on replica — new replid, offset 0, backlog clear, drop feeds, clear replica metadata, `master_replid2` in INFO; idempotent when already master.
  - *Coordinated* `FAILOVER TO <host> <port> [TIMEOUT ms] [FORCE]` (master only, default timeout 5000ms): write pause, soft match against REPLCONF `listening-port`/`ip-address` when tracked, wait until target ack ≥ frozen `master_repl_offset` (unless **FORCE**), then TCP bare `FAILOVER`, best-effort sibling re-follow (`REPLICAOF` on feeds + client-port TCP), demote self via `set_replicaof`. Replicas honor in-stream `REPLICAOF` and reconnect on `primary_link_epoch` / primary addr change. Catch-up sources: live-link tracked ACK (`REPLCONF ACK` on feed + periodic feed GETACK probe), then client-port GETACK fallback. Replica offset uses exact wire bytes via `parse_with_consumed` and replies to master GETACK on the repl link. Catch-up timeout leaves master writable.
  - *Done (Batch EW, Sentinel-lite)*: `SENTINEL MONITOR|REMOVE|GET-MASTER-ADDR-BY-NAME|MASTERS|MASTER|REPLICAS|SET|FAILOVER|CKQUORUM|HELP`. Background PING + ROLE replica discover; **s_down** after `down-after-milliseconds` (default 30s); auto-failover (toggle via `SET auto-failover`).
  - *Done (Batch FK)*: promote ranking — highest priority (0 never), then highest ROLE offset, then greatest `ip:port` (mirrors cluster EA/EB); not discovery order.
  - *Done (Batch FM)*: live INFO `slave_priority` refresh on probe/`try_failover`; auto-failover **15s** cooldown after attempt (manual FAILOVER bypasses).
  - *Done (Batch EX, ODOWN lite)*: `SENTINEL MYID|MEET|MEETPEER|SENTINELS|IS-MASTER-DOWN-BY-ADDR`. Peer table; vote count = self s_down + peer is-master-down replies; **o_down** when votes ≥ quorum; auto-failover gated on **o_down** (not bare s_down). CKQUORUM checks known_sentinel_count ≥ quorum. No full leader election races.
  - *Done (Batch EZ)*: `SENTINEL FLUSHCONFIG` → `{dir}/sentinel.conf`; load on boot (`load_or_new`); restore myid/monitors/peers/options; **autosave** on MONITOR/REMOVE/SET/MEET/switch_master.
  - *Done (Batch FA)*: Redis-style hello CSV; `SENTINEL HELLO`; tick **PUBLISH** `__sentinel__:hello` on reachable masters; peer `SENTINEL HELLO` exchange; `apply_hello` learns peers + higher-epoch **switch-master**. No long-lived master `SUBSCRIBE` fan-in (peer HELLO is primary discovery path).
  - *Done (Batch FC)*: `promote_replica` success gate — requires `FAILOVER` OK, `REPLICAOF NO ONE` OK, or post-attempt `ROLE=master`; **never** `switch_master` on PING alone. Per-master `failover_in_progress` serializes manual FAILOVER vs tick. Promote inject hooks for tests.
  - *Done (Batch FE)*: voted-leader on `IS-MASTER-DOWN-BY-ADDR` (sticky first-seen per epoch; higher epoch re-votes); auto-failover only after `try_elect_leader` (≥ `max(quorum, floor(N/2)+1)`); ODOWN probes use runid `*`; `add_peer` autosave only on real change. Manual `SENTINEL FAILOVER` still force-bypasses election.
  - *Done (Batch FN)*: `CKQUORUM` + elect majority use **live PING** (`count_reachable_sentinels`); dead peers no longer inflate usable/N; probe `runid=*` with no prior vote returns leader `"*"` / epoch 0 (Redis-honest; sole-sentinel auto path via `is_failover_leader` / live≤1 elect).
  - *Residual (post-FN)* — see Phase D cluster + backlog table / Later:
    - **done** promote ranking (Batch FK: priority → offset → `ip:port`).
    - **done** `nodes.conf` live flags (Batch FL).
    - **done** INFO `slave_priority` refresh + auto failover cooldown (Batch FM).
    - **done** CKQUORUM / elect majority live probe + probe `*` honesty (Batch FN).
    - **P3 accepted** no long-lived master `__sentinel__:hello` SUBSCRIBE fan-in (tick PUBLISH + peer HELLO only).
    - **done** election-timeout SM (Batch FT: `ELECTION_TIMEOUT` 5s; reuse campaign epoch while live; re-campaign after expiry incl. stuck vote-for-other).
    - **P3 accepted** FM post-ship: serial INFO priority enrich; success still cools auto re-entry (optional parallel enrich stays Later).
- [x] **`[P1]`** Client durability: `WAIT` + min-replicas write gate
  - *Done*: `WAIT numreplicas timeout_ms` freezes `master_repl_offset`, probes feed GETACK, returns count of replicas with ack ≥ offset (`timeout 0` = forever). `CONFIG GET|SET min-replicas-to-write` / `min-replicas-max-lag` (aliases `min-slaves-*`); writes return `NOREPLICAS` when good replica count is below threshold. INFO exposes `min_slaves_*`.

---

## Phase C — Core Redis data types & commands

**Phase priority: High (3rd)**

### Data structures

- [x] **`[P0]`** **Hashes** (`HSET`, `HGET`, `HMGET`, `HDEL`, `HGETALL`, …)
  - *Done*: separate hash map + type registry; HSET/HGET/HMGET/HDEL/HGETALL/HLEN/HEXISTS/HKEYS/HVALS/HINCRBY
  - *Batch AO*: `HINCRBYFLOAT` (bulk float reply), `HSTRLEN`, `HMSET` (OK alias of HSET)
  - *Batch AV*: `HRANDFIELD` (count / WITHVALUES)
  - *Batch AW*: `HSETNX`
  - *Batch AX*: `HGETDEL` (get-and-delete fields; array reply)
- [x] **`[P0]`** **Lists** (`LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, …)
  - *Done*: LPUSH/RPUSH/LPOP/RPOP/LRANGE/LLEN/LINDEX/LSET; **BLPOP/BRPOP** (blocking via `ListBlockers` + `Notify`; timeout 0 = forever; multi-key left-to-right; null array on timeout)
  - *Batch AK*: `LREM` (count signed), `LTRIM`, `LINSERT` BEFORE|AFTER; empty list key deleted
  - *Batch AN*: `LPOS` (RANK/COUNT/MAXLEN), `LMOVE` / `BLMOVE` (LEFT|RIGHT sides; timeout 0 = forever; null bulk on BLMOVE timeout)
  - *Batch AW*: `RPOPLPUSH` / `BRPOPLPUSH` (legacy LMOVE RIGHT→LEFT); `LMPOP` / `BLMPOP` (`LEFT|RIGHT`, `COUNT`; multi-key left-to-right; nested `[key, [elems…]]`)
  - *Batch AY*: `LPUSHX` / `RPUSHX` (push only if list exists)
- [x] **`[P0]`** **Sets** (`SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SINTER`, …)
  - *Done*: SADD/SREM/SMEMBERS/SISMEMBER/SCARD/SINTER
  - *Batch AH*: SUNION/SDIFF + *STORE (SINTERSTORE/SUNIONSTORE/SDIFFSTORE); SMOVE; SPOP/SRANDMEMBER (optional count)
  - *Batch AV*: `SMISMEMBER`; `SINTERCARD` (`LIMIT`)
- [x] **`[P0]`** Transactions: `MULTI` / `EXEC` / `DISCARD` / `WATCH`
  - *Done*: per-connection queue; WATCH via key generation counters; UNWATCH; EXECABORT on queue errors
- [x] **`[P1]`** Streams + consumer groups
  - *Done*: XADD (auto/`*` + explicit ID, MAXLEN), XLEN, XRANGE/XREVRANGE, XDEL, XTRIM, XREAD / XREADGROUP with **BLOCK** (ms; 0=forever; `$` fixed at wait start; stream_blockers + XADD notify), XGROUP CREATE/DESTROY (+MKSTREAM), XACK, XPENDING summary; TYPE/DEL/KEYS/DBSIZE/RENAME wired; RDB v3 + AOF stream persistence (entries, groups, PEL via XCLAIM FORCE on rewrite)
  - *Batch AT*: `XCLAIM` (min-idle, FORCE/JUSTID/IDLE/TIME/RETRYCOUNT); `XAUTOCLAIM` (COUNT/JUSTID, deleted-ids); `XPENDING` range (`IDLE`, consumer); `XGROUP SETID`; `XSETID`
  - *Batch AU*: `XINFO STREAM|GROUPS|CONSUMERS`; `XGROUP CREATECONSUMER` / `DELCONSUMER`
  - *Batch BF*: `XADD` `NOMKSTREAM` / `MINID`; `XTRIM` `MINID`
  - *Batch BH*: `XREADGROUP` `NOACK` (skip PEL for `>` deliveries)
  - *Batch BI*: `XGROUP CREATE` `ENTRIESREAD` (surfaced in `XINFO GROUPS`)
  - *Batch BJ*: `XINFO STREAM FULL` [`COUNT`]; `XGROUP SETID` `ENTRIESREAD`
  - *Batch BK*: `XSETID` `ENTRIESADDED`/`MAXDELETEDID`; real `entries-added` / `max-deleted-entry-id` counters
  - *Batch BL*: `XADD`/`XTRIM` `LIMIT` (cap deletions per trim)
  - *Batch BM*: `XGROUP HELP`
- [x] **`[P2]`** Bitmaps / bitfields, HyperLogLog
  - *Done (MVP)*: `SETBIT`/`GETBIT`/`BITCOUNT`/`BITPOS`/`BITOP` (AND/OR/XOR/NOT)/`BITFIELD` (GET/SET/INCRBY + OVERFLOW WRAP|SAT|FAIL); `PFADD`/`PFCOUNT`/`PFMERGE` dense HLL (p=14, Kore `KHLL` format). String-key backed; ACL `@bitmap`/`@hyperloglog`.
  - *Batch AZ*: `BITFIELD_RO` (GET only)
  - *Batch BF*: `BITCOUNT`/`BITPOS` optional `BYTE|BIT` unit; `XADD` `NOMKSTREAM`/`MINID`; `XTRIM` `MINID`

### Command coverage

- [x] **`[P1]`** Common string ops: `APPEND`, `STRLEN`, `SETEX`, `GETSET`, `UNLINK`, `RENAME` / `RENAMENX`
  - *Done*: atomic APPEND; UNLINK = sync DEL; RENAME/RENAMENX across all key types
  - *Batch AG*: `MOVE` (cross-DB), `COPY` (`DB` / `REPLACE`), `RANDOMKEY`, `TOUCH` — multi-type dump/restore; TTL preserved
  - *Batch AL*: `GETRANGE` / `SETRANGE` (zero-pad) / `MSETNX`
  - *Batch AX*: `LCS` (+ `LEN`); `MEMORY USAGE`; `OBJECT ENCODING`
  - *Batch AY*: `PSETEX`; `INCRBYFLOAT`; `SUBSTR` (GETRANGE alias); `TIME`; `ZRANGESTORE` (BYSCORE/BYLEX/REV/LIMIT)
  - *Batch AZ*: `LCS` `IDX` / `MINMATCHLEN` / `WITHMATCHLEN`; `OBJECT IDLETIME|REFCOUNT|FREQ`; `BITFIELD_RO`; `GEORADIUS_RO` / `GEORADIUSBYMEMBER_RO`; `SWAPDB`
  - *Batch BA*: `DUMP` / `RESTORE` (Kore KDF1 multi-type; `REPLACE`/`ABSTTL`/`IDLETIME`/`FREQ`); `EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT` `NX|XX|GT|LT`; `COMMAND GETKEYS`; `ACL GENPASS`
  - *Batch FY*: `DUMP` emits **Redis RDB wire** for string/list/set/hash/zset (classic opcodes + CRC64 + RDB v9); geo/stream stay **KDF1**. `RESTORE` dual-detects KDF1 vs Redis (classic + listpack/quicklist2 fixtures). Module `src/rdb_object.rs`.
  - *Batch BB*: `SLOWLOG` GET/LEN/RESET; `MEMORY STATS|DOCTOR|PURGE`; `CLIENT REPLY` ON|OFF|SKIP; `CONFIG` `slowlog-log-slower-than` / `slowlog-max-len`
  - *Batch BC*: `SORT` (list/set/zset; `ALPHA`/`ASC`/`DESC`/`LIMIT`/`STORE`; `BY nosort` only); `LOLWUT`; `READONLY`/`READWRITE` (per-connection `cluster_readonly`)
  - *Batch BD*: `ZADD` `NX|XX|GT|LT|CH|INCR`; `SCAN TYPE`; `FLUSHDB`/`FLUSHALL` `ASYNC|SYNC`; shard pub/sub in `COMMAND` catalog
  - *Batch BE*: modern `ZRANGE` (`BYSCORE`/`BYLEX`/`REV`/`LIMIT`/`WITHSCORES`); `CLIENT NO-EVICT`/`NO-TOUCH` (flags + INFO; NO-TOUCH skips LRU on GET/MGET)
  - *Batch BF*: `BITCOUNT`/`BITPOS` `BYTE|BIT`; `XADD` `NOMKSTREAM`/`MINID`; `XTRIM` `MINID`
  - *Batch BG*: `EVAL_RO` / `EVALSHA_RO`; `CLIENT GETREDIR` / `TRACKINGINFO`
  - *Batch BH*: `XREADGROUP` `NOACK`; `CONFIG RESETSTAT`; `LATENCY` HELP/LATEST/HISTORY/RESET/DOCTOR; `MODULE LIST`
  - *Batch BI*: `INFO` section filter; `COMMAND GETKEYSANDFLAGS` / `DOCS`; `XGROUP CREATE` `ENTRIESREAD`
  - *Batch BJ*: `XINFO STREAM FULL` [`COUNT`]; `XGROUP SETID` `ENTRIESREAD`; `COMMAND HELP` / `CLIENT HELP`
  - *Batch BK*: `CLIENT TRACKING`/`CACHING`; `XSETID` `ENTRIESADDED`/`MAXDELETEDID`; stream counters in `XINFO`
  - *Batch BL*: `XADD`/`XTRIM` `LIMIT`; `SCRIPT HELP`; `CONFIG HELP`
  - *Batch BM*: `CONFIG GET` glob + multi-pattern; `COMMAND LIST FILTERBY`; `PUBSUB HELP` / `XGROUP HELP`
  - *Batch BN*: `COMMAND GETKEYS` movablekeys; `CLUSTER HELP`; `ACL DRYRUN`
  - *Batch BO*: `FUNCTION`/`FCALL` stubs; GETKEYS zset algebra; `CONFIG REWRITE`; FT.* catalog
  - *Batch BP*: `ACL LOG`; GEORADIUS GETKEYS STORE/STOREDIST; `SHUTDOWN`; `QUIT`/`CLIENT KILL ID` close
  - *Batch BQ*: `DEBUG` HELP/SLEEP/OBJECT; INFO Clients/CPU/Persistence; Lua GEO/stream allowlist
  - *Batch BR*: `CONFIG GET` ops params (`port`/`bind`/`dir`/`dbfilename`/`appendonly`/…); `MEMORY MALLOC-STATS`; `FT.TAGVALS`; Lua `COPY`/`MOVE`; TAG/NUMERIC coerce on HSET auto-index
  - *Batch BS*: `FT.ALIASADD`/`FT.ALIASDEL`/`FT.ALIASUPDATE`; alias resolution on `FT.INFO`/`FT.SEARCH`/`FT.TAGVALS`/`FT.DROPINDEX` (+ alias cleanup on drop); Lua `SELECT`/`FLUSHDB`
  - *Batch BT*: mutating `FT.*` classified as writes (`is_write_command` → AOF / replica / READONLY); alias→alias stores real index name; atomic create/alias namespace locks; Lua SELECT connection DB side-effect test
  - *Batch BU*: AOF rewrite emits `FT.CREATE` (PREFIX/SCHEMA TEXT|NUMERIC|TAG|VECTOR) then key dumps then `FT.ALIASADD`; load applies FT.* + HSET auto-index (`tests/bu_aof_ft_rewrite_test.rs`)
- [x] **`[P1]`** `CLIENT`, `COMMAND`, `HELLO`
  - *Done*: HELLO (RESP2 + RESP3; AUTH/SETNAME); CLIENT ID/SETNAME/GETNAME/SETINFO/LIST/INFO; COMMAND / COUNT / LIST / INFO catalog
  - *Batch BE*: `CLIENT NO-EVICT` / `NO-TOUCH`
  - *Batch BG*: `CLIENT GETREDIR` (-1) / `TRACKINGINFO` (flags off)
  - *Batch BI*: `COMMAND GETKEYSANDFLAGS` / `DOCS`; `INFO [section…]`
  - *Batch BJ*: `COMMAND HELP` / `CLIENT HELP`
  - *Batch BK*: `CLIENT TRACKING ON|OFF` (+ REDIRECT/PREFIX/BCAST/OPTIN/OPTOUT/NOLOOP); `CLIENT CACHING`; GETREDIR/TRACKINGINFO live
  - *Batch BM*: `COMMAND LIST FILTERBY` `PATTERN`|`MODULE`|`ACLCAT`
  - *Batch BN*: `COMMAND GETKEYS` for LMPOP/ZMPOP/SINTERCARD/XREAD/XREADGROUP/MEMORY USAGE
  - *Batch BO*: GETKEYS `ZUNION`/`ZINTER`/`ZDIFF`/`ZINTERCARD`/`Z*STORE`/`FCALL`; FT.* in catalog
  - *Batch BP*: GETKEYS `GEORADIUS`/`GEORADIUSBYMEMBER` + `STORE`/`STOREDIST`
- [x] **`[P1]`** Multi-DB: `SELECT` (or explicitly document single-DB only)
  - *Done*: `--databases` (default 16); per-connection `SELECT`; key isolation; `FLUSHDB` vs `FLUSHALL`; shared pub/sub+stats; **RDB v3 multi-DB + AOF SELECT** on save/rewrite/load/startup
  - *Batch AZ*: `SWAPDB` (content swap all types + TTL via dump/restore)
- [x] **`[P2]`** Lua scripting / functions
  - *Done (MVP)*: `EVAL` / `EVALSHA` / `SCRIPT LOAD|EXISTS|FLUSH|KILL` via mlua Lua 5.4 (vendored); shared `ScriptCache`; `redis.call` / `redis.pcall` whitelist for core string/hash/list/set/zset/bitmap/HLL ops; KEYS/ARGV; RESP↔Lua mapping (nil bulk→false, status→`{ok=…}`); ACL `@scripting`; cluster key extract from numkeys. Residual: full movablekeys COMMAND, nested scripts.
  - *Batch BG*: `EVAL_RO` / `EVALSHA_RO` (reject write `redis.call`); `CLIENT GETREDIR` / `TRACKINGINFO` (tracking off)
  - *Batch BL*: `SCRIPT HELP`; `CONFIG HELP`
  - *Batch BO*: `FUNCTION` HELP/LIST/STATS stubs; `FCALL`/`FCALL_RO` not-found; `CONFIG REWRITE` (no conf file)
  - *Batch GI*: real Redis Functions — shared `FunctionLibraryStore`; `FUNCTION LOAD`/`LIST`/`DELETE`/`FLUSH`/`DUMP`/`RESTORE`/`STATS`; `FCALL`/`FCALL_RO` with `redis.register_function` + shebang `#!lua name=`; Kore `KORF1` dump format
  - *Batch GJ*: `redis.setresp(2|3)`; RESP3 bool/map/double mapping; `CONFIG GET|SET lua-time-limit` (default 5000, 0=unlimited); hard timeout via mlua hooks; real `SCRIPT KILL` / `FUNCTION KILL` (NOTBUSY / UNKILLABLE / write tracking)
  - *Batch BP*: `ACL LOG` GET/LEN/RESET + denial recording; `SHUTDOWN` [NOSAVE|SAVE]; connection close after `QUIT`/`SHUTDOWN`/`CLIENT KILL ID … SKIPME no`; `acllog-max-len` CONFIG
  - *Batch BQ*: `DEBUG` HELP/SLEEP/OBJECT; INFO `# Clients`/`# CPU`/`# Persistence`; `redis.call` GEO* + XADD/XLEN/XRANGE/XDEL/XTRIM/XACK + TOUCH/SCAN/RANDOMKEY
  - *Batch BR*: `CONFIG GET` port/bind/dir/dbfilename/appendonly/appendfilename/unixsocket/cluster-enabled; `MEMORY MALLOC-STATS`; `FT.TAGVALS`; `redis.call` COPY/MOVE; schema coerce TAG/NUMERIC from HSET text
  - *Batch BS*: `FT.ALIASADD`/`DEL`/`UPDATE` + alias map; `redis.call` SELECT/FLUSHDB (multi-DB)
  - *Batch BT*: FT write classification; alias target resolve + locking; post-EVAL SELECT DB test
  - *Batch BU*: AOF rewrite FT schema/aliases + load-path FT apply / HSET auto-index

### Memory & expiration policy

- [x] **`[P1]`** Eviction policies: `allkeys-lru`, `volatile-lru`, `allkeys-lfu`, `volatile-ttl`, `noeviction`
  - *Done*: all 8 Redis policies via `--maxmemory-policy` / `CONFIG maxmemory-policy`; sampling victim selection
  - *Batch AE*: volatile-* includes typed keys that have a TTL (not only strings)
- [x] **`[P1]`** Approximated LFU (Redis-style)
  - *Done (Batch AB)*: Redis 24-bit LFU word (16-bit minute stamp + 8-bit log counter); probabilistic `LFULogIncr`; time decay via `lfu-decay-time` (default 1 min); `lfu-log-factor` (default 10); `CONFIG GET/SET` for both; init counter = 5. Not full redis.conf boot defaults CLI yet.
- [x] **`[P1]`** Active expire sampling (avoid full-shard `retain` pauses on large datasets)
  - *Done*: Redis-style sample cycle (20 keys/pass, continue if >25% expired, 1ms budget); background autosweep uses sampling @ 10Hz; full `SWEEP` retained for admin
- [x] **`[P2]`** More accurate memory sizing (allocator overhead, index structures)
  - *Done (Batch AA)*: central `memory::estimate_*` helpers — `Bytes`/dict entry overhead, ~12.5% allocator tax + 8-byte align; string `Entry::size`, hash/list/set/zset/geo/stream `memory_size`, search docs, keyed create/remove/rename/eviction samples; `used_memory` / `memory_usage()` = total tracked (all categories); capacity checks use total budget. HSET/HINCRBY account actual post-mutation delta (rollback on OOM; lock dropped before ensure/evict to avoid deadlock); search auto-index is best-effort under maxmemory. Not jemalloc RSS / full skiplist span accounting.

### Sorted set / geo performance

- [x] **`[P1]`** Shard zsets/geo like the main map (remove single global `RwLock` bottleneck)
  - *Done*: `ShardedKeyMap<V>` (parking_lot per-shard locks, ahash); zset + geo use same `num_shards` as string map
- [x] **`[P1]`** O(log n) rank (`ZRANK` / `ZREVRANK`) — skiplist or ranked tree instead of BTreeMap scan
  - *Done*: Redis-style span skiplist (`sorted_set.rs`); `rank`/`rev_rank`/`get_by_rank` O(log n); member HashMap for O(1) score
  - *Batch AX*: `ZRANK` / `ZREVRANK` `WITHSCORE` → `[rank, score-bulk]`
  - *Batch AI*: `ZINCRBY`, `ZRANGEBYSCORE` / `ZREVRANGEBYSCORE` (`WITHSCORES`, `LIMIT`, exclusive `(` bounds, `±inf`), `ZCOUNT`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`
  - *Batch AM*: `ZUNIONSTORE` / `ZINTERSTORE` (`numkeys`, `WEIGHTS`, `AGGREGATE` SUM|MIN|MAX); dest overwrite any type; missing source = empty; return cardinality
  - *Batch AP*: `ZPOPMIN` / `ZPOPMAX` (optional count); `BZPOPMIN` / `BZPOPMAX` (multi-key, timeout 0 = forever; null array on timeout; woken by ZADD/ZINCRBY/*STORE)
  - *Batch AQ*: `ZUNION` / `ZINTER` / `ZDIFF` (+ `WITHSCORES`; WEIGHTS/AGGREGATE for union/inter); `ZDIFFSTORE`
  - *Batch AR*: `ZMSCORE`; `ZRANDMEMBER` (count / WITHSCORES); `ZRANGEBYLEX` / `ZREVRANGEBYLEX` / `ZLEXCOUNT` / `ZREMRANGEBYLEX` (`-`/`+`/`[`/`(` bounds, LIMIT)
  - *Batch AS*: `ZINTERCARD` (`LIMIT`); `ZMPOP` / `BZMPOP` (`MIN|MAX`, `COUNT`; multi-key left-to-right; nested `[key, [[m,s]…]]` reply)
  - *Batch AY*: `ZRANGESTORE` destination source min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]
  - *Batch AV geo polish*: `GEOSEARCH` `WITHHASH`; `GEORADIUS`/`GEORADIUSBYMEMBER` `STORE`/`STOREDIST`; `GEOSEARCHSTORE` dest overwrite + memory accounting; geo commands in `COMMAND` catalog
  - *Batch AZ*: `GEORADIUS_RO` / `GEORADIUSBYMEMBER_RO` (reject STORE/STOREDIST)
  - *Batch BD*: `ZADD` `NX|XX|GT|LT|CH|INCR`
  - *Batch BE*: modern `ZRANGE` (`BYSCORE`/`BYLEX`/`REV`/`LIMIT`/`WITHSCORES`); shared path with `ZRANGESTORE`

---

## Phase D — Scale, protocol & security

**Phase priority: Medium (4th)**

### Cluster

Also tracked in `docs/roadmap.md`.

- [x] **`[P1]`** Hash slots / key hashing compatible with Redis Cluster clients
  - *Done (MVP)*: Redis CRC16-XMODEM + hash tags (`SLOT_COUNT=16384`); single-node `ClusterState` owns all slots; `--cluster-enabled`; `CLUSTER KEYSLOT/MYID/INFO/NODES/SLOTS/SETSLOT`; `ASKING` one-shot; gate after ACL (`CROSSSLOT` / `MOVED` / `ASK`); `SELECT` rejected in cluster mode; standalone path unchanged.
- [x] **`[P1]`** Gossip / membership and failover
  - *Done (thin MVP + Batch DW–EC)*: `CLUSTER MEET` over client RESP; PING heartbeat; **`pfail` → quorum `fail`**; FAILREPORTS; NODES `fail?`/`fail`. On master fail: **priority → offset → id** (EB/EA/DY); **losers re-point** (DZ). Operator **`CLUSTER FAILOVER [FORCE|TAKEOVER]`** (EC). Gaps: no binary cluster bus, no full Sentinel process.
- [x] **`[P1]`** Resharding / slot migration (thin MVP)
  - *Done*: `keys_in_slot` / `string_keys_in_slot`; `CLUSTER MIGRATEKEYS <slot> <ip> <port>` moves **all key types** via RESP (ASKING + type-specific recreate + DEL: SET/HSET/RPUSH/SADD/ZADD/GEOADD/XADD+groups); SETSLOT MIGRATING/IMPORTING/NODE/STABLE operator flow; MIGRATING miss → ASK; final NODE → MOVED.
  - *Done (Batch Y)*: multi-type MIGRATEKEYS (string/hash/list/set/zset/geo/stream)
  - *Done (Batch DM)*: `CLUSTER RESHARD <slot> <dest-id>` / `CLUSTER RESHARD <start> <end> <dest-id>` — source-side orchestration of dest IMPORTING → source MIGRATING → MIGRATEKEYS → best-effort dual-end `SETSLOT NODE`. Reply is per-slot field arrays (`migrated` / `source_node` / `dest_node` / `status`). Hard key-move failures leave MIGRATING/IMPORTING for retry; dual-end NODE is **not atomic** (`partial_dest_node` / `partial_source_node` / `failed_*` statuses). Range aborts further slots after first `failed_*`. Prep step best-effort `SETSLOT NODE <source>` on dest before IMPORTING.
  - *Done (Batch DN)*: dual-end `SETSLOT NODE` **verify + retry** (up to 3 attempts/side, short backoff; local `owner_of` + remote `CLUSTER SLOTS` check). Shared path `finish_slot_node` + operator `CLUSTER RESHARD FINISH <slot> <dest-id>` completes NODE only (no key re-migrate) after `partial_*_node`. Still **not** atomic / no 2PC — statuses stay honest. Tests: happy path, inject-1 retry→complete, inject-exhaust→partial_dest_node then FINISH recovery.
  - *Done (Batch DO)*: `MigrateSlotError` carries partial `migrated`/`skipped`; RESHARD `failed_keys` surfaces real counts; retry = leftover source keys only (docs + e2e). Range aborts on any non-`complete` (incl. `partial_*_node`). FINISH soft-checks `keys_in_slot` → optional `warning` field (does not hard-block). Source-before-dest NODE client window documented in rustdoc (order unchanged).
  - *Done (Batch DP)*: Redis key-level `MIGRATE host port key dest-db timeout [COPY] [REPLACE] [AUTH password] [AUTH2 user pass] [KEYS k…]`. Shared `migrate_one_key_on_stream` / `migrate_keys_to` reuses snapshot + RESP recreate (no DUMP/RESTORE); ASKING probed (disabled on standalone dest); `COPY` / `REPLACE` / `BUSYKEY` / `NOKEY` / connect timeout; multi-key `KEYS`; dest `SELECT` for non-zero db; AOF/repl propagates source `DEL` (not the MIGRATE form). MIGRATEKEYS loop uses the same helper. Catalog + ACL `@write`/`@keyspace`/`@dangerous` + readonly replica gate. Tests: `tests/dp_migrate_test.rs` (+ existing MIGRATEKEYS suite green).
  - *Done (Batch DQ)*: multi-key mid-batch `IOERR` includes `migrated=` / `skipped=` counts (shared inject with MIGRATEKEYS); recreate path transfers remaining TTL for all types (string `SET PX`; hash/list/set/zset/geo/stream trailing `PEXPIRE` via `Cache::ttl`). Tests: partial inject e2e + string/hash/list TTL.
  - *Gaps*: no interactive redis-cli weight UI. **MIGRATE DUMP→RESTORE** for core types (**Batch GG**); geo/stream still RESP recreate (KDF1). Dual-end NODE has RESP prepare/commit (Batch FB); not Redis binary cluster-bus 2PC. **DUMP/RESTORE Redis wire** for core types: **done Batch FY**; MIGRATE uses that wire for GG core types.
  - *Done (Batch EY)*: dual-end NODE **preflight** before any SETSLOT NODE — local owns/already-dest; dest reachable + `CLUSTER MYID` matches dest-id; dest `CLUSTER SLOTS` owner is source or dest (or unbound). Failure → `failed_preflight` (no half-apply). Idempotent complete when both already own dest.
  - *Done (Batch FB)*: dual-end NODE **wire 2PC slice 1** — `SETSLOT PREPARE`/`ABORTPREPARE` votes on source+dest (extends EY); commit only after both prepares (dest-first NODE, DV); EP rollback on source commit fail; status `failed_prepare` without half-apply; range aborts on prepare fail. Tests: happy path, dest prepare inject, source commit→rolled_back + FINISH, range abort.
  - *Done (Batch FH)*: dual-end NODE **wire 2PC slice 2** — prepare votes stamp **slot config epoch** + wall-clock **TTL** (`PREPARE_VOTE_TTL`); `SETSLOT CHECKPREPARE` + local `check_prepare_valid` before dest NODE; source re-checks again immediately before its NODE; stale epoch / cleared / expired prepare → `failed_prepare:recheck:…` without half-apply; soft clear fail-closed. Tests: unit epoch/TTL/clear/boot; e2e recheck inject + happy path; FB suite green.
  - *Done (Batch FO)*: durable prepare votes in `nodes.conf` (`# prepare <slot> <target> <epoch> <unix_ms>`); wall-clock unix-ms TTL (survives restart); load restores non-expired votes (expired/malformed dropped); autosave on PREPARE/ABORT/COMMIT when dir set; `SETSLOT COMMITPREPARE` atomic check+NODE under one write lock; dual-end commit uses COMMITPREPARE (dest RESP + source local). Tests: nodes.conf prepare round-trip / expired not restored / COMMITPREPARE; migrate **42/42**.
  - *Residual (FH / FB / FO)*:
    - **done FO** prepare durable on disk (`nodes.conf` `# prepare` lines + restore).
    - **done FO** atomic local `COMMITPREPARE` (dual-end path); bare `SETSLOT NODE` still operator bypass.
    - **P3** no Redis **binary cluster bus** 2PC (RESP-only prepare/commit).
    - **P3** dest-side `set_prepare_node(myself)` accepts unbound **or any known-peer owner** (broad vote; intentional for mid-reshard but weak fencing).
    - **P3** operator `SETSLOT NODE` still bypasses prepare (FINISH/recovery intentional; dual-end path enforces prepare + COMMITPREPARE).
    - **P3** remote dest still CHECKPREPARE then COMMITPREPARE as **two RPCs** (local source commit atomic; wire window accepted lite).
    - **P3** per-slot config epochs still not fully persisted in nodes.conf (load stamps owned slots with file epoch; prepare epoch fence fails closed when mismatch).
  - *Done (Batch EP)*: when dest NODE ok but source NODE fails: EH re-asserts MIGRATING; **compensate** dest with `SETSLOT NODE <source>` + `IMPORTING` → status `rolled_back` (both sides agree source owns; retry FINISH). Rollback failure keeps `partial_source_node` + warning. Range aborts on `rolled_back`. Source NODE inject hook for tests.
  - *Done (Batch EQ)*: Redis `cluster-require-full-coverage` (default yes) — `CLUSTER INFO cluster_state:ok|fail`; key commands get `CLUSTERDOWN The cluster is down` when any slot is unbound or fail-owned; CONFIG GET/SET + `--cluster-require-full-coverage`; ASKING+IMPORTING still allowed for reshard.
  - *Done (Batch ER)*: connection `READONLY`/`READWRITE` wired into cluster redirect gate — replicas serve **reads** for slots owned by their master; writes still `MOVED`; without READONLY all key cmds `MOVED` (Redis-compatible).
  - *Done (Batch ES)*: Redis `cluster-allow-reads-when-down` (default no) — when `cluster_state` is fail, **reads** of covered slots still served if enabled; **writes** remain `CLUSTERDOWN The cluster is down`. CONFIG GET/SET + `--cluster-allow-reads-when-down`.
  - *Done (Batch ET)*: `CLUSTER SLOTS` each range is `[start, end, [master…], [replica…]…]` (replicas via `replicas_of`, non-fail); unbound ranges omitted from `slots_ranges`.
  - *Done (Batch EU)*: Redis `cluster-announce-ip` / `cluster-announce-port` — client-facing address for myself in CLUSTER NODES/SLOTS/MEETPEER/`addr()`; bind host/port unchanged; CONFIG GET/SET + CLI; empty/0 clears.
  - *Done (Batch EV)*: `CLUSTER SLOT-STATS SLOTSRANGE <start> <end> [ORDERBY key-count [LIMIT n] [ASC|DESC]]` — key counts for **owned** slots (one keyspace pass); `cpu-usec`/network fields 0 (shape-compatible).
  - *Done (Batch EM/EN/EO)*: `CLUSTER SAVECONFIG` → `{dir}/nodes.conf`; load on boot (`load_or_single_node`); **autosave** after topology-mutating CLUSTER ops (SETSLOT/ADDSLOTS/MEET/FORGET/RESET/FAILOVER/RESHARD/…) and gossip failover claim. Best-effort (warn on I/O fail); not every gossip ownership merge.
  - *Done (Batch DU)*: per-slot config epoch on `SETSLOT NODE` / reassign / failover claim; `CLUSTER OWNERS` + `CLUSTER EPOCH`; gossip/MEET pull+merge higher-epoch-wins (skip local MIGRATING/IMPORTING); third-node learns owner after NODE.
  - *Done (Batch DV)*: dest-first dual-end NODE (source skipped if dest fails → no MOVED-to-IMPORTING window); `partial_dest_node` when source `skipped:`; stale lower-epoch gossip cannot flip post-reshard ownership.
  - *Done (Batch DX)*: `CLUSTER RESHARD PLAN <dest> <n>` greedy donors; `AUTO` plans then executes local `reshard_slot` + remote RESP `CLUSTER RESHARD` (abort-on-partial).
  - [x] **`[P1]`** **Code review (EW post-ship):** `promote_replica` succeeds on PING alone
    - *Done (Batch FC)*: `promote_replica` requires `FAILOVER` OK / `REPLICAOF NO ONE` OK / post-attempt `ROLE=master`; never PING-only. Per-master `failover_in_progress`. Tests: inject fail → no switch; inject OK + real ROLE path still switch.
  - [x] **`[P3]`** **Code review (FB post-ship nits):** prepare not re-checked at commit; dest prepare permissive; memory-only votes
    - *Done (Batch FH)*: commit re-check + prepare-epoch + TTL + soft clear.
    - *Done (Batch FO)*: durable prepare in nodes.conf; atomic COMMITPREPARE. Residual: bus 2PC; dest vote breadth; operator NODE bypass (intentional).
  - *Post-ship review (FI, 2026-07-25):* AOF-off multi-DB SELECT interleave race under concurrent writers → **fixed Batch FI-2** (`propagate_write` atomic SELECT+append).
  - [x] **`[P2]`** **Code review (EX/FA post-ship):** Sentinel failover leader election (cross-process)
    - *Done (Batch FE)*: sticky voted-leader on `IS-MASTER-DOWN-BY-ADDR`; auto path elects before `try_failover`; `voted-leader` / `voted-leader-epoch` on MASTER fields. Residual election-timeout SM → **done FT** (lite; not full Raft / lex-min runid).
  - [x] **`[P3]`** **Code review (FE post-ship nits):** election-epoch thrash; probe self-as-leader; majority uses table size
    - *Found (scheduled review after FE)*: (1) multi-sentinel o_down tick can `next_election_epoch` every second until majority; (2) probe without prior vote returns self (not `*`); (3) `leader_votes_needed` uses `known_sentinel_count` (same residual as CKQUORUM).
    - *Done (Batch FM, partial)*: post-`try_failover` auto cooldown (15s) suppresses elect+failover re-entry thrash while `last_failover_attempt` is hot.
    - *Done (Batch FN)*: probe `*` honesty; CKQUORUM + elect majority use live PING. Residual: full election-timeout SM (epoch thrash while o_down before first try_failover) → **done FT**.
  - [x] **`[P3]`** **Code review (FC post-ship nit):** Sentinel promote still first-replica-wins
    - *Done (Batch FK)*: `rank_replicas_for_promote` — highest priority (0 never), then highest ROLE offset, then greatest `ip:port`. Tests: priority / offset / skip-0 / all-zero.
    - *Done (Batch FM)*: live INFO `slave_priority` refresh on probe / `try_failover`; auto-failover **15s** cooldown after attempt (manual FAILOVER force bypasses).
    - *Done (Batch FN)*: CKQUORUM live probe + probe self-vs-`*`. Residual: full election-timeout SM → **done FT**.

  - [x] **`[P3]`** **Code review (FM post-ship):** serial INFO priority refresh; cooldown always arms
    - *Found (scheduled review after FM, 2026-07-29):* (1) `enrich_replica_priorities` issues sequential `INFO replication` per replica (N×`IO_TIMEOUT`) — acceptable for lite; parallelize only if multi-replica failover latency matters; (2) successful failover still arms 15s auto cooldown (intentional thrash guard; Redis failover-timeout ballpark lite); (3) `begin_failover` contention path returns without arming cooldown (other owner holds in-progress — intentional). No correctness bug.
    - *Closed (Batch FN)*: CKQUORUM live + probe honesty. Residual serial INFO enrich stays **accepted lite** / Later.
  - [x] **`[P3]`** **Code review (EZ/FA post-ship):** hello-path autosave thrash (+ partial CKQUORUM)
    - *Done (Batch FE, autosave)*: `add_peer` returns changed-only and autosaves only on new/updated peer.
    - *Done (Batch FN)*: `CKQUORUM` / elect majority use live PING (not peer-table size alone).
  - [x] **`[P3]`** **Code review (EN/EO/EU post-ship):** `nodes.conf` omits live cluster flags
    - *Done (Batch FL)*: header `# key value` comments persist require-full-coverage / allow-reads-when-down / announce-ip|port / replica-priority; load restores; CONFIG SET autosaves when dir set. Legacy files without keys keep defaults. Boot CLI overrides only non-default flags (plain restart keeps saved).
  - [x] **`[P2]`** **Code review (DM/DN post-ship):** `failed_keys` under-reports partial key moves
    - *Done (Batch DO)*: `migrate_slot_keys` → `Result<_, MigrateSlotError { partial, message }>`; `reshard_one_slot` maps partial into `ReshardSlotResult.migrated/skipped` under `failed_keys:*`. Retry re-runs MIGRATEKEYS/RESHARD for leftover keys only. Test: inject mid-slot fail after 1 success → `migrated: 1` + retry completes.
  - [x] **`[P2]`** **Code review (DP post-ship):** multi-key partial failure reply is coarse `IOERR` (no counts)
    - *Done (Batch DQ)*: `migrate_keys_to` IOERR after ≥1 success reports `migrated=` / `skipped=`; leftover keys stay on source for retry. Test: inject after 1 of 3 → counts + retry completes.
  - [x] **`[P3]`** **Code review (DP post-ship):** non-string TTL not transferred on recreate
    - *Done (Batch DQ)*: `KeySnapshot` carries `pttl` for all types; string uses `SET PX`; typed keys append `PEXPIRE`. Unit + e2e (string/hash/list).
  - [x] **`[P3]`** **Code review (DQ post-ship nit):** migrate TTL is remaining-ms snapshot, not absolute
    - *Found*: `Cache::ttl` → remaining ms at snapshot time; dest applies `PX`/`PEXPIRE` later so effective lifetime can shrink by RTT/processing. Absolute `PEXPIREAT` would preserve end time better under slow links.
    - *Done (Batch DT)*: snapshot via `expire_time_unix_ms`; string `SET … PXAT`; typed trailing `PEXPIREAT`. Unit freeze-under-delay + e2e `PEXPIRETIME` (string/hash/list + known absolute).
  - [x] **`[P3]`** **Code review (DN post-ship):** source NODE before dest NODE creates client window
    - *Done (docs, Batch DO)*: module + `dual_end_setslot_node` rustdoc describe MOVED-to-IMPORTING window under `partial_dest_node`. Code order left source-first (Redis client expectations); dest-first / 2PC still open under gaps.
  - [x] **`[P3]`** **Code review (DM post-ship):** range RESHARD continues after `partial_*_node`
    - *Done (Batch DO)*: abort-on-partial — range stops when `status != "complete"` (`failed_*` or `partial_*`). Test: inject dest NODE exhaust on first of two empty slots → one result, second slot still owned by source.
  - [x] **`[P3]`** **Code review (DN post-ship nit):** `RESHARD FINISH` does not check key placement
    - *Done (Batch DO)*: soft-check `keys_in_slot` on source; when non-empty, reply includes `warning` (ownership still applied — no hard-block). Docs: re-run MIGRATEKEYS after `failed_keys` before FINISH.

### Protocol & clients

- [x] **`[P1]`** RESP3 support (`HELLO 3`, maps, bools, push)
  - *Done*: `RespValue::{Map,Bool,Null,Push}` serialize+parse; `HELLO 3` map + `protocol_version`; `HGETALL`/`CONFIG GET` as maps on proto 3; pub/sub confirmations + fan-out as **push** for RESP3 clients; `CLIENT INFO` `resp=`; `RESET` → proto 2.
- [x] **`[P1]`** Zero/low-alloc command dispatch (avoid per-command `String` uppercasing; static table / perfect hash)
  - *Done (MVP)*: stack `[u8;64]` ASCII uppercase via `ascii_uppercase_cmd` (heap only if name > 64 bytes); mixed-case dispatch covered by tests.
  - *Done (Batch GD)*: `CommandId` enum + length-first `from_upper`; dispatch/`is_write`/slowlog/pubsub on enum; stack ACL lowercase.
- [x] **`[P2]`** Pipelining / write batching optimizations under load
  - *Done*: coalesce pipeline replies into fewer response-channel sends per read; write task drains/`try_recv` into up to 64KiB `write_all` batches.

### Security

- [x] **`[P1]`** ACL (users, command/key/channel permissions)
  - *Done (MVP)*: default user from `--auth`; `AUTH` password / username+password; `ACL SETUSER` (on/off, >pass/nopass, +@all/-@all, +cmd/-cmd, ~*/~prefix*, `&*`/`&pat`/allchannels/resetchannels), `GETUSER`/`LIST`/`WHOAMI`/`CAT`/`DELUSER`/`LOAD`/`SAVE`; `--aclfile` + boot load; per-connection username; command+key+channel checks; HELLO AUTH uses real user lookup.
  - *Batch BA*: `ACL GENPASS` [bits]; `COMMAND GETKEYS`; `DUMP`/`RESTORE`
  - *Batch BB*: `SLOWLOG`; `MEMORY STATS|DOCTOR|PURGE`; `CLIENT REPLY`; slowlog CONFIG params
  - *Batch BC*: `SORT`; `LOLWUT`; `READONLY`/`READWRITE`
  - *Batch BD*: `ZADD` options; `SCAN TYPE`; flush ASYNC|SYNC; shard pub/sub catalog
  - *Batch BE*: modern `ZRANGE`; `CLIENT NO-EVICT`/`NO-TOUCH`
  - *Batch BN*: `ACL DRYRUN`; `CLUSTER HELP`
  - *Batch BP*: `ACL LOG`; `CLIENT KILL ID`
- [x] **`[P1]`** TLS
  - *Done (MVP)*: `--tls` / `--tls-cert` / `--tls-key`; tokio-rustls server wrap on accept; fail-fast cert/key load; plaintext path unchanged; no mTLS / dual listener / replica link TLS
- [x] **`[P2]`** Unix domain socket option
  - *Done*: `--unixsocket <path>` binds UDS in addition to TCP; stale socket unlink on bind; remove on shutdown; no TLS on UDS.

### Observability

- [x] **`[P1]`** Prometheus metrics endpoint and/or richer Redis-compatible `INFO` sections
  - *Done (MVP)*: `--metrics-port` (default 0=off) serves hand-rolled Prometheus text on `127.0.0.1`; core series from Stats + repl/persistence; additive `# Health` INFO section. No prometheus crate.
- [x] **`[P2]`** Optional structured (JSON) logging
  - *Done (Batch CX)*: `--log-format text|json` (default `text`); `tracing-subscriber` JSON formatter when `json`; clap parse unit tests
  - *Residual (CX post-ship)*: boot-only (not `CONFIG SET`); no `RUST_LOG`/`EnvFilter`; no JSON emit smoke; default verbosity WARN hides `info!` startup lines; targets off for parity with text.
- [x] **`[P3]`** **Code review (CX post-ship):** JSON logging ops polish
  - *Done (Batch CY)*: README documents boot-only + verbosity 0–3 → ERROR/WARN/INFO/DEBUG (default 1=WARN); `EnvFilter::try_from_default_env()` falls back to verbosity; JSON `with_target(true)`; unit smoke `json_log_line_is_parseable_object`.
  - *Residual (CY post-ship)*: smoke uses string contains, not `serde_json` parse; does not exercise `with_env_filter`. `RUST_LOG` **replaces** `-v` (not a floor); empty/invalid env edge cases soft; default WARN still hides boot `info!`.
- [x] **`[P1]`** Health / readiness beyond bare `PING` (memory, persistence lag)
  - *Done (MVP)*: `HEALTH` / `HEALTH PING` → OK/PONG; `HEALTH FULL` structured status (`ready`, `role`, memory, `master_link`, `rdb_last_save`, `aof`); replica not ready when master link down.

---

## Phase E — Differentiators (after baseline is solid)

**Phase priority: Low (5th)** — harden only after Phases A–C are green

### Redlock & locking

- [x] **`[P1]`** Ensure Redlock CLI flags actually wire into the running server path
  - *Done (MVP + Batch Y)*: `Redlock::from_config` + `Server::with_redlock`; INFO fields; `LockBackend` trait with **remote RESP** backends (`SET NX PX` / GET+DEL / PEXPIRE) for `--redlock-instances`; injectable local `Cache` backends for tests.
- [x] **`[P1]`** Fair lock queueing: production hardening, metrics, docs
  - *Done*: atomic `try_acquire` under write lock; `dequeue_client` front-safe pop; retry cleanup uses `max_attempts`; Drop stops cleanup thread; CLI `--enable-fair-queue` / max-size / cleanup-ms; INFO `# FairQueue` section; docs
- [x] **`[P2]`** Deadlock detection advanced (from roadmap)
  - [x] **`[P2]`** Cross-process detection
    - *Done (Batch DC, MVP)*: `HeldLockSnapshot` / `WaitEdgeSnapshot` / `DeadlockGraphSnapshot` (serde); `export_snapshot` / `merge_snapshot` (local held wins, wait edges union+dedupe); multi-process half-cycle tests; docs “Cross-process (snapshot merge)”. No transport/Web UI.
    - *Done (Batch DD)*: merge re-links local `waiting_for` + imports `OrphanWaitSnapshot`; edge holder reconcile; remaining-TTL import (`ttl_ms - held_for_ms`, `timestamp = now`).
  - [x] **`[P2]`** Async support
    - *Done (Batch DA, standalone surface)*: `detect_deadlock_async` / `resolve_deadlock_async` / `get_stats_async` / `check_long_waits_async` (thin Tokio wrappers over short sync critical sections); `release_client_locks`; `DeadlockDetector::spawn_monitor` (abort `JoinHandle`, auto-resolve + tracing); `Redlock::check_deadlock_async` / `get_deadlock_stats_async`; unit + integration tests; docs
    - *Done (Batch DB)*: `Redlock::deadlock_detector()` / `spawn_deadlock_monitor` (backend unlock + graph); residual async is still sync-on-poll under contention.
  - [x] **`[P2]`** Custom victim selection strategies
    - *Done (Batch CZ, detector-level)*: `VictimSelectionStrategy` {`Youngest` (default), `Oldest`, `FewestLocks`}; `new_with_strategy` / `with_victim_strategy`; `Redlock::with_deadlock_detection_strategy` constructs detector; unit tests per strategy
    - *Done (Batch DB)*: Redlock lock path honors `auto_resolve` + strategy — backend unlock then atomic graph cleanup; fail-fast when auto_resolve=false.
  - [x] **`[P2]`** **Code review (CZ/DA post-ship):** wire auto-resolve + strategy through Redlock backend unlock
    - *Done (Batch DB)*: on detect + auto_resolve, `resolve_deadlock` → unlock victim resources on backends (`val` = client id token) → `release_client_locks`; E2E tests (Youngest backend release, fail-fast, Redlock monitor).
    - *Done (Batch DD)*: `record_lock_released(resource, client_id)` only clears graph when current holder matches (safe after force-unlock + re-acquire); `release_client_locks` also drops edges with `edge.holder == client_id`; E2E `test_victim_lock_drop_preserves_new_holder_graph`.
  - [x] **`[P2]`** **Code review (DA post-ship):** `release_client_locks` TOCTOU + incomplete wait cleanup
    - *Done (Batch DB)*: single write section retain-by-client; waiting_for + edge prune for victim and released resources; `cleanup_expired` drops edges/waiting for expired holds; monitor logs resolve-None; unit tests TOCTOU re-acquire / prune / expired.
  - [x] **`[P1]`** **Code review (DB post-ship):** disarm victim `Lock` after auto-resolve backend unlock
    - *Done (Batch DD)*: conditional `record_lock_released(resource, client_id)` — only remove if current holder’s `client_id` matches; Redlock `unlock`/`Drop` pass `lock.val`. Regression: resolve → re-acquire → drop victim → new holder still in `get_held_locks()`.
    - *Residual (DD post-ship → DE)*: `record_lock_released` race — fixed in Batch DE (atomic held+graph; holder-scoped prune).
  - [x] **`[P1]`** **Code review (DC post-ship):** merge re-links local waits to imported holds
    - *Done (Batch DD)*: after merging holds, scan `waiting_for` and create edges; export `OrphanWaitSnapshot` for holder-less waits; realistic half-cycle test without pre-planted peer holds.
  - [x] **`[P2]`** **Code review (DC post-ship):** reconcile wait-edge holders + remaining TTL on import
    - *Done (Batch DD)*: rewrite edges where holder ≠ held[resource]; import remaining TTL (`ttl_ms.saturating_sub(held_for_ms)`, `timestamp = now`); zero remaining skips import; `release_client_locks` drops `edge.holder == client_id`.
    - *Residual (DD post-ship → DE)*: self-wait on merge rewrite + local acquire re-link — fixed in Batch DE.
  - [x] **`[P1]`** **Code review (DD post-ship):** atomic `record_lock_released` + holder-scoped edge prune
    - *Done (Batch DE)*: single critical section (`held_locks` → `wait_graph`); prune only `edge.resource == resource && edge.holder == client_id`; regression `test_record_lock_released_holder_scoped_preserves_reacquire_edges`.
  - [x] **`[P1]`** **Code review (DD post-ship):** merge holder rewrite must not create self-waits
    - *Done (Batch DE)*: step 2 drops edge if rewrite yields `holder == waiter`; dedupe by `(waiter, resource, holder)`; unit test `test_merge_holder_rewrite_drops_self_wait`.
  - [x] **`[P2]`** **Code review (DD post-ship):** `record_lock_acquired` rewrites holders + re-links waits
    - *Done (Batch DE)*: on acquire, rewrite/drop edges for that resource and re-link `waiting_for` (mirror merge steps 2+5); tests `test_record_lock_acquired_rewrites_holders_and_relinks_waits`, `test_release_then_acquire_relinks_waiters`.
  - [x] **`[P2]`** Web UI monitoring
    - *Done (Batch DF, MVP)*: `--deadlock-ui-port` (default 0=off) serves hand-rolled HTML (`/`, `/deadlock`) + JSON (`/api/deadlock`, `/deadlock.json`) on `127.0.0.1` only; shares Redlock detector (auto-enabled when UI port set + redlock on); disabled state when no detector; unit/HTTP tests with planted cycle; docs.
  - [x] **`[P2]`** **Code review (DF post-ship):** UI snapshot not atomic; detect side-effects on poll
    - *Done (Batch DG)*: `DeadlockDetector::collect_consistent_view(cleanup)` — single critical section (held → waiting → graph); optional pure-read (`cleanup=false`); UI uses cleanup=true; docs honest about cleanup-on-poll; JSON `config.cleanup_on_collect`.
  - [x] **`[P2]`** **Code review (DF post-ship):** no CLI for deadlock params; UI hardcodes detector
    - *Done (Batch DG)*: `--enable-deadlock-detection`, `--deadlock-max-wait-ms`, `--deadlock-auto-resolve`, `--deadlock-victim-strategy`; UI port still auto-attaches for back-compat but params come from flags; JSON surfaces `config.{max_wait_time_ms,auto_resolve,victim_strategy}`.
  - [x] **`[P3]`** **Code review (DF post-ship):** no `from_config` wiring test for UI auto-enable
    - *Done (Batch DG)*: `tests/redlock_wiring_test.rs` — off-by-default, enable flag + params, UI-port auto-attach, port=0+off → no detector.
  - [x] **`[P3]`** **Code review (DF post-ship):** JS poll only updates status badge
    - *Done (Batch DH)*: 5s `fetch('/api/deadlock')` repaints badge, stats, cycle box, held/wait/orphan tables from JSON (HTML-escaped); meta-refresh kept as soft full-page fallback; unit test `html_poll_js_repaints_tables_stats_and_cycle`; docs.
  - [x] **`[P3]`** **Code review (DH post-ship):** dual refresh still full-reloads when JS works
    - *Done (Batch DI)*: wrap `<meta http-equiv="refresh" content="5">` in `<noscript>` so JSON poll is the only path when JS runs; full-page reload remains for no-JS; unit test asserts refresh sits inside noscript; docs updated.
  - [x] **`[P3]`** **Code review (DH post-ship nit):** JS numeric table cells not coerced
    - *Done (Batch DI)*: poll script `num(x)` = `Number(x)` with finite fallback `0` for `ttl_ms` / `held_for_ms` / `wait_elapsed_ms` (held/wait/orphan rows).
  - [x] **`[P3]`** **Code review (DH post-ship nit):** repaint test is string-contains only
    - *Found*: `html_poll_js_repaints_tables_stats_and_cycle` asserts embedded JS source and DOM ids; does not execute the poll or assert rendered row HTML after a fake JSON payload. Acceptable without a browser; residual if DOM fidelity regressions matter.
    - *Done (Batch DT, accept/wontfix)*: keep string-contract unit tests only — no browser/quickjs harness for localhost admin MVP.
  - [x] **`[P3]`** **Code review (DF post-ship):** HTTP MVP gaps shared with metrics server
    - *Found*: single 4KiB read (no full header parse); non-GET → 404 not 405; test binds `127.0.0.1:0` then rebinds same port (TOCTOU flake risk under load). Acceptable for localhost admin MVP. Same 4KiB pattern in `metrics::run_metrics_server`.
    - *Done (Batch DJ)*: shared `src/admin_http.rs` — request-line read until `\r\n` (8 KiB cap), method/path parse, 405+`Allow: GET` on known paths for non-GET, 404 unknown path; used by deadlock UI + metrics. Tests bind listener once (`*_on_listener` + `127.0.0.1:0`). Residual: no full header/body parse, no auth/TLS (out of scope).
  - [x] **`[P3]`** **Code review (DJ post-ship nit):** non-GET on unknown path is 404 (not 405)
    - *Found (code review after DJ)*: 405 only when path is known; `POST /nope` → 404. Correct resource-not-found semantics; document if clients expect method errors first.
    - *Done (Batch DL, document/accept)*: module rustdoc on `admin_http` + `docs/deadlock_detection.md` routing note — path membership first; unknown → 404 for any method; 405 only for non-GET on known admin routes. Existing exchange tests already cover `POST /nope` → 404; parse-level unit documents the inputs.

### Search & vectors

- [x] **`[P1]`** Document and test `FT.SEARCH` end-to-end over RESP (not only programmatic indexing)
  - *Done*: `tests/search_resp_test.rs` — FT.CREATE / HSET auto-index / FT.SEARCH / FT.DROPINDEX via `CommandHandler`; DEL/UNLINK remove from indices
- [x] **`[P1]`** Memory limits and eviction interaction for indexes
  - *Done (MVP + Batch AD)*: `MemoryCategory::Search`; `index_document` / `auto_index_key` allocate approx size; remove/drop deallocate; counts toward maxmemory. Under `allkeys-*`, sampled **search documents** are eviction victims (drop index entry + free Search bytes; underlying hash key kept). Account path may `evict_memory` before OOM. Search docs still not volatile victims (no search TTL).
- [x] **`[P0]`** **Code review (BS):** classify mutating `FT.*` as writes in `is_write_command`
  - *Done (Batch BT)*: `FT.CREATE` / `FT.DROPINDEX` / `FT.ALIASADD` / `FT.ALIASDEL` / `FT.ALIASUPDATE` in `is_write_command`; READONLY on replica + AOF contains commands (`tests/bt_ft_write_alias_resolve_test.rs`)
- [x] **`[P1]`** **Code review (BS):** resolve alias targets in `alias_add` / `alias_update` (store real index name; allow alias→alias retarget)
  - *Done (Batch BT)*: resolve `index` arg through alias map; store real name; DROPINDEX cleanup consistent
- [x] **`[P1]`** **Code review (BS):** hold a single critical section for FT create/alias namespace checks (avoid TOCTOU between index and alias maps)
  - *Done (Batch BT)*: `create_index` / `alias_add` / `alias_update` hold aliases then indices locks for full check-and-insert (matches `drop_index`)
- [x] **`[P1]`** **Code review (BT):** emit `FT.CREATE` + `FT.ALIASADD` during AOF rewrite
  - *Done (Batch BU)*: rewrite order **FT.CREATE → key dumps (HSET…) → FT.ALIASADD**; `list_definitions` / `list_aliases` on SearchIndexManager; load applies `FT.CREATE`/`FT.ALIAS*`/`FT.DROPINDEX` and HSET auto-indexes (`tests/bu_aof_ft_rewrite_test.rs`)
- [x] **`[P1]`** **Code review (BU):** AOF load must not silently drop FT mutator failures
  - *Found*: `apply_command_to_cache` FT.CREATE / FT.ALIAS* / FT.DROPINDEX use `let _ = …` and always `Ok(())` — create/alias errors (OOM, name conflict, bad def) leave hashes restored but search empty with no load error
  - *Done (Batch BV)*: propagate `create_search_index` / `drop_search_index` / `alias_*` as `Error::InvalidArgument`; non-truncated unparsable FT.CREATE → `ParseError` (truncated argv still skips like other AOF paths); `tests/bv_aof_ft_load_errors_test.rs`
- [x] **`[P2]`** Persist FT indices + aliases in RDB (AOF rewrite done in BU; RDB FT section still open)
  - *Done (Batch BY)*: RDB version 5 search section (index definitions + aliases) per DB body after typed-expires; `from_cache` / `is_empty` / `load_into` create schema first then auto-index hashes; v1–v4 still load. Tests: `tests/by_rdb_ft_section_test.rs`
- [x] **`[P1]`** **Code review (BY):** RDB load with `flush=true` keeps FT schema (BX `flush`/`flush_all`), then `create_search_index` fails on name clash
  - *Found*: `load_bytes` / `load_databases_bytes` call `cache.flush()` / `databases.flush_all()` (clear_documents only). `DbSnapshot::load_into` always `create_search_index` — second FULLRESYNC / reload of a dump that includes the same index names returns `Index '…' already exists` and aborts. Affects replica `FULLRESYNC` (`replication.rs` loads with `flush=true`) and any non-empty-target RDB load after BX.
  - *Done (Batch BZ)*: when `flush=true` on RDB load, use `flush_all_including_search()` (snapshot replace); live FLUSHDB/FLUSHALL still `clear_documents`. Tests: `tests/bz_rdb_load_wipes_ft_schema_test.rs`
- [x] **`[P2]`** **Code review (BY):** RDB load is not wiped on mid-`load_into` FT failure (AOF got BW/BX; RDB did not)
  - *Found*: `create_search_index` / `alias_add` errors abort `load_into` after partial apply (earlier indices/keys remain). Callers (`load_databases_bytes`, FULLRESYNC) do not full-wipe on `Err`.
  - *Done (Batch BZ)*: on RDB load `Err` after decode (mutate started), `flush_all_including_search` (mirror AOF BW/BX). Tests: `tests/bz_rdb_load_wipes_ft_schema_test.rs`. Scratch-load swap for non-empty targets remains open (shared with BW item).
- [x] **`[P2]`** **Code review (BZ):** RDB `flush=false` still merges into FT schema — name clash risk on merge
  - *Found*: BZ only full-wipes schema when `flush=true`. A `flush=false` load into a DB that already has the same index name still fails at `create_search_index` (now on scratch; target preserved on Err after CB). Startup / FULLRESYNC use `flush=true` and are fine.
  - *Done (Batch CF)*: on merge, `DbSnapshot::load_into` **skips** FT.CREATE / ALIASADD when the name already exists (seed definition wins). Public wrappers document snapshot-replace vs merge. Test: `tests/cf_multidb_replace_and_merge_test.rs`.
- [x] **`[P1]`** **Code review (CF post-ship):** FT merge skip is name-only — divergent RDB schema silently discarded
  - *Found*: `load_into` builds `existing_indices` by name and `continue`s with no definition check. Seed wins even when RDB has different `prefix` / fields; RDB hashes still load but `auto_index_key` follows **seed** prefixes only — silent search data loss while load returns `Ok`. Docs claiming “RDB docs still auto-index into existing index” are wrong when definitions diverge.
  - *Done (Batch CG)*: `IndexDefinition::schema_eq` (name/prefix/fields; ignore `created_at`); `load_into` schema-equal → skip, unequal → `Err(InvalidArgument)`. Tests: `tests/cg_ft_merge_schema_test.rs`; CF equal-schema success path updated.
- [x] **`[P1]`** **Code review (CF post-ship):** FT alias merge skip is name-only — retarget clash keeps seed mapping silently
  - *Found*: if seed has `blog → idx` and RDB has `blog → other_idx`, seed mapping is kept with no error; alias queries hit the wrong index after successful merge.
  - *Done (Batch CG)*: on alias name clash, compare resolved targets; equal → skip; unequal → `Err(InvalidArgument)`. Retarget + equal-target tests in `cg_ft_merge_schema_test.rs`.
- [x] **`[P2]`** **Code review (BZ nit):** `rdb_load_mid_ft_failure_wipes_partial_state` accepts almost any `InvalidArgument` message (`!msg.is_empty()`); tighten to `RDB FT.ALIASADD` / unknown index
  - *Done (Batch CF)*: assert message mentions alias / unknown index / missing (prints actual msg on fail).
  - *CF post-ship nit*: still accepts bare `"missing"`; prefer `alias` + `unknown index` or exact `RDB FT.ALIASADD` prefix.
- [x] **`[P2]`** **Code review (BZ nit):** raw `DbSnapshot::load_into` / `MultiDbSnapshot::load_into_*` still leave partial state on `Err` if called directly
  - *Found*: production paths use `load_bytes` / `load_databases_bytes` / AOF `load_into_*` wrappers (scratch-load after CB). Direct `load_into` remains non-transactional.
  - *Done (Batch CF)*: rustdoc on raw `load_into` / `load_into_*` marks them non-transactional; public wrappers documented as the supported transactional/scratch-load APIs.
- [x] **`[P2]`** **Code review (BY nit):** no HNSW `M` / `ef_construction` RDB round-trip test (FLAT covered in `by_rdb_ft_section_test`; RDB encoder already writes both HNSW params)
  - *Done (Batch CE)*: `tests/ce_acl_search_hnsw_test.rs` — FT.CREATE HNSW M+EF_CONSTRUCTION, RDB save/load preserves both
- [x] **`[P2]`** **Code review (BY nit):** RDB FT mutator errors always `Error::InvalidArgument` — align with AOF `map_ft_mutator_error` (OOM → `OutOfMemory`) if search layer can return OOM on create
  - *Done (Batch CH)*: RDB `load_into` CREATE/ALIASADD use shared `aof::map_ft_mutator_error` (OOM prefix → `OutOfMemory`).
- [x] **`[P0]`** **Code review (CH post-ship):** RDB OOM mapping broken by pre-prefixing error strings
  - *Found*: `map_ft_mutator_error(format!("RDB FT.CREATE: {}", e))` — helper only matches `starts_with("OOM:")` / `"OOM "` / exact `"OOM"`. Prefixed messages never map to `OutOfMemory` (stay `InvalidArgument`). AOF correctly passes raw search strings.
  - *Done (Batch CI)*: `map_rdb_ft_mutator_error` maps raw message first, then prefixes only `InvalidArgument`. Unit tests in `aof::ft_error_map_tests` (including pre-prefixed regression guard).
- [x] **`[P2]`** ACL `@search` category for FT.* (fine-grained users; default `+@all` unaffected)
  - *Done (Batch CE)*: `@search` in `category_names` / `category_commands`; FT read/write also under `@read`/`@write`; `+@all` expands via `all_known_commands`. Tests: `tests/ce_acl_search_hnsw_test.rs`
- [x] **`[P2]`** HNSW graph-based search (Batch CQ)
  - *Done (Batch CQ)*: `HNSWIndex::search` walks layer-0 neighbor edges (SEARCH-LAYER) with `ef_search`; `add` selects neighbors **before** inserting the vector (no self-loops); simple M-prune on reverse edges. Multi-layer insert completed in **Batch FF**.
  - *Tests*: `hnsw_top1_matches_flat_on_small_set` (kept); `hnsw_search_follows_edges_not_full_scan` (fails under full-scan); `hnsw_add_excludes_self_from_neighbors`; `hnsw_graph_has_edges_after_inserts`.
- [x] **`[P2]`** **Code review (CQ post-ship):** HNSW `remove` leaves stale graph edges
  - *Found*: `remove` deleted the vector (and may reassign `entry_point` via arbitrary `keys().next()`) but did **not** remove the node from the layer map, strip reverse edges, or repair bridges. Re-`add` of the same id used `or_insert` and could **revive stale neighbor lists**.
  - *Done (Batch CS, local hygiene)*: `remove` unlinks reverse edges + drops layer entry; `add_node` always resets neighbor list; `pick_entry_point` prefers a remaining node with edges. Tests: `hnsw_remove_middle_unlinks_graph`, `hnsw_remove_entry_reassigns`, `hnsw_remove_readd_clears_stale_neighbors`.
  - *Residual (CS post-ship)*: hard unlink did **not** reconnect former neighbors — fixed in Batch CT bridge repair.
- [x] **`[P2]`** **Code review (CQ post-ship):** M-prune can make new nodes unreachable from entry
  - *Found*: after bidirectional connect, prune rewrote only each neighbor’s **outgoing** list. With small `M`, reverse edges `neighbor → new` could all be dropped — search from entry never visited the new node.
  - *Done (Batch CS, insert-time heuristic)*: layer-0 `M_max ≈ 2M`; `prune_neighbors_keeping` force-keeps reverse edge to the **new** node at that prune. Test: `hnsw_insert_preserves_reachability_from_entry`.
  - *Residual (CS post-ship)*: force-keep is **not** a durable global invariant — later hub prunes can drop older reverse edges that were a leaf’s only path from entry. Docs softened (Batch CT); do not claim global insert reachability.
- [x] **`[P2]`** **Code review (CQ post-ship):** update-in-place does not rewire graph
  - *Found*: existing-id `add` replaced the vector only; old neighbor wiring remained — queries near the new location could miss the node.
  - *Done (Batch CS)*: existing-id `add` unlinks then re-inserts (full neighbor reselection). Test: `hnsw_update_rewires_graph` (large vector move).
  - *Residual (CS post-ship)*: update inherited hard-delete bridge partition — fixed by Batch CT reconnect on remove.
- [x] **`[P2]`** **Code review (CS post-ship):** HNSW hard-delete can partition the graph (bridge remove/update)
  - *Found*: `remove` stripped edges to the deleted id but never reconnected former neighbors. Chain `a↔b↔c` with entry `a` after `remove(b)` left `c` in `vectors` but unreachable from search. Update-in-place is remove+reinsert, so updating a cut vertex could permanently orphan the other partition.
  - *Done (Batch CT, bidirectional 2-chain)*: `remove` snapshots **outgoing** former neighbors, unlinks, then `bridge_reconnect_neighbors` — closest-peer bidirectional edges + force-keep nearest peer. Tests: `hnsw_bridge_remove_*`, `hnsw_bridge_update_*`; `hnsw_m1_hub_churn_*` is **smoke only**.
  - *Residual (CT post-ship)*: outgoing-only snapshot + multi-way force-keep — fixed in Batch CU.
- [x] **`[P2]`** **Code review (CS post-ship):** force-keep / docs over-claim global insert reachability
  - *Found*: force-keep only protects `neighbor → new` for the current insert; later `must_keep` prunes can `kept.pop()` older reverse edges. Docs saying inserts “stay reachable from entry” overstated durability.
  - *Done (Batch CT, docs honesty)*: insert-time only / not a durable global invariant. Hub-churn is soft majority smoke, not a global guarantee.
- [x] **`[P2]`** **Code review (CT post-ship):** bridge repair snapshot is outgoing-only
  - *Found*: former set is `get_neighbors(deleted)` only. Nodes with edges **into** the deleted id but not listed as out-neighbors are unlinked and never reconnected. Asymmetric prune can leave `entry→bridge`, `bridge→leaf` / `leaf→bridge` only; `remove(bridge)` then `survivors.len() < 2` and entry/leaf stay disconnected.
  - *Done (Batch CU)*: undirected former snapshot (outgoing ∪ reverse scan). Test: `hnsw_bridge_remove_asymmetric_incoming_reconnects`.
- [x] **`[P2]`** **Code review (CT post-ship):** multi-way / degree-saturated bridge reconnect incomplete
  - *Found*: each survivor only force-keeps its **single** nearest former peer. With ≥3 former neighbors or leaves already at `max_m` with closer non-former edges, secondary former links can be pruned — directed search from entry may still miss a survivor. CT tests only cover the 2-neighbor mutual-NN chain.
  - *Done (Batch CU, clique path)*: spanning reconnect (full clique when `n-1 ≤ max_m`, else nearest-neighbor path) with force-keep of spanning edges on **both** endpoints; multi must-keep prune. Test: `hnsw_bridge_remove_star_multiway_reconnects`.
  - *Done (Batch CW, path branch)*: NN-path reconnect unit test with ≥4 former neighbors + `max_m=2` (`n-1 > max_m`). Test: `hnsw_bridge_remove_path_branch_reconnects` (BFS + farthest-leaf `search`).
- [x] **`[P2]`** HNSW recall@k / throughput numbers vs FLAT
  - *Done (Batch CV, unit gate + N=300 micro)*: `hnsw_recall_at_k_vs_flat_and_throughput` — N=300 dim=16 Cosine, Q=40, fixed seed; mean recall@1 ≥ 0.90 / recall@10 ≥ 0.80 vs FLAT; indicative wall times in `docs/benchmarks.md` (FLAT still cheaper at this N; recall perfect on seed).
  - *Done (Batch DK, CV post-ship)*: tightened always-on gate (M=8/ef=32; recall@1 ≥ 0.975 / @10 ≥ 0.95); optional `#[ignore]` larger-N median-of-3 bench N=5000; docs label single-shot vs median.
  - *Done (Batch DL)*: r@10 floor 0.95→0.93 (cross-arch headroom); always-on `hnsw_recall_after_remove_update_churn` (N=120 remove+update, soft 0.90/0.85).
- [x] **`[P2]`** **Code review (CU post-ship):** NN-path bridge reconnect branch untested
  - *Found*: star multi-way test uses 3 survivors + `max_m=2` → `n-1 ≤ max_m` full clique, not the `else` nearest-neighbor path. Path construction / force-keep degree≤2 can regress green.
  - *Done (Batch CW)*: `hnsw_bridge_remove_path_branch_reconnects` — 4-leaf star, M=1/`max_m=2`, BFS all survivors + `search(self, k=1)` for farthest leaf `d`.
  - *Residual (CW post-ship)*: path test leaves survivors empty before reconnect; **bonus density alone** can pass without force-keep under degree pressure (unlike CU star with decoys).
  - *Done (Batch CY)*: path-branch test attaches `max_m` closer decoys per leaf so force-keep is load-bearing under degree pressure.
- [x] **`[P2]`** **Code review (CU post-ship):** `prune_neighbors_keeping` can drop required must-keep edges
  - *Found*: when `|must_keep| > max_m` and slots are full of required ids, fallback `kept.pop()` may drop a required edge. Layer-0 path currently keeps deg≤2 with `max_m≥2`, so latent for multi-layer `M=1` or oversize must sets.
  - *Done (Batch CW)*: cap must set to closest `max_m` by distance before force-keep; never pop a still-required id. Test: `prune_neighbors_keeping_caps_must_keep_by_distance`.
  - *Residual (CW post-ship)*: capping silently drops farther spanning edges — path middles need 2 force-keeps; multi-layer `max_m=1` would still break spanning. **Closed by Batch GR** (`prune_m = max_m.max(2)` on bridge reconnect only).
- [x] **`[P3]`** **Code review (CW post-ship):** path-branch test needs degree-saturating decoys
  - *Found*: empty neighbor lists after hub clear mean bonus closest-peer density reconnects the line without proving path force-keep under pressure.
  - *Done (Batch CY)*: `hnsw_bridge_remove_path_branch_reconnects` attaches 2 decoys/leaf closer than other survivors; BFS farthest leaf + `search` for `d` still pass.
  - *Residual (CY post-ship)*: positive connectivity only — does not assert path edges present in adjacency after prune under decoy pressure.
- [x] **`[P3]`** **Code review (CU post-ship):** undirected remove reverse-scan is O(N)
  - *Found*: every remove full-scans layer neighbors for reverse edges, then `unlink_node` scans again. Correct but costly as N grows.
  - *Done (Batch CY)*: `unlink_collecting_undirected_former` fuses undirected snapshot + strip + drop in one reverse pass (no full reverse adjacency index).
  - *Residual (CY post-ship)*: still **O(E_layer)** once (not O(N)+deg); reverse index only if delete-heavy workloads need better asymptotics.
- [x] **`[P3]`** **Code review (CV post-ship):** tighten recall gate / larger-N throughput
  - *Found*: thresholds 0.90/0.80 leave headroom on easy N/M/ef; throughput is single-shot N=300 where HNSW loses to FLAT.
  - *Done (Batch DK)*: always-on gate uses harder M=8/ef=32 + raised floors recall@1 ≥ 0.975 / @10 ≥ 0.95 (fixed seed `0xC0FFEE42`; observed ≈1.00/0.985). Optional `#[ignore]` `hnsw_recall_larger_n_median_throughput` — N=5000 M=16/ef=100, median-of-3 search timings, soft floors 0.95/0.90; run with `cargo test --release --lib hnsw_recall_larger_n_median_throughput -- --ignored --nocapture`. `docs/benchmarks.md` labels single-shot vs median-of-3; no portable large-N win claim without host re-measure.
  - *Done (Batch DL)*: r@10 floor 0.95→0.93 (~5.5pp cushion); post-delete/update churn recall micro; admin_http request-line async tests; 404-vs-405 docs.
  - *Residual*: string-only deadlock UI repaint test still open (no browser harness).
- [x] **`[P3]`** **Code review (DK post-ship):** always-on r@10 headroom is thin (~3.5pp)
  - *Found (code review after DK)*: floor 0.95 vs observed ~0.985 on seed `0xC0FFEE42` with M=8/ef=32. Deterministic `StdRng` helps, but f32 graph ops / arch differences could flake CI if r@10 dips. Larger-N ignore bench soft floors (0.95/0.90) still have room vs observed 1.0.
  - *Done (Batch DL)*: loosen always-on r@10 floor **0.95 → 0.93** with comment (still load-bearing vs CV 0.80 and observed ≈0.985); `docs/benchmarks.md` updated. Larger-N soft floors left as-is (ignore-only).
- [x] **`[P3]`** **Code review (DK post-ship):** recall suite has no post-delete / update churn
  - *Found*: unit gate and larger-N bench build once then search; no remove/update-then-recall path (graph repair covered by other HNSW unit tests, not recall@k).
  - *Done (Batch DL)*: always-on `hnsw_recall_after_remove_update_churn` — N=120 dim=16 Cosine, M=8/ef=32, seed `0xD1C40177`, remove 15 + update 15, Q=24; soft mean recall@1 ≥ 0.90 / @10 ≥ 0.85 vs FLAT.
- [x] **`[P3]`** **Code review (DJ post-ship nit):** `read_request_line` lacks async unit tests; no-CRLF fallback
  - *Found*: unit tests cover `parse_request_line` only. Without `\r\n` inside 8 KiB, reader falls back to first `\n` or the whole buffer (may yield 400). Integration tests cover GET/POST/404; no dedicated partial-read / oversized-line test.
  - *Done (Batch DL)*: tokio tests in `admin_http` — partial TCP reads assemble request line; oversized no-CRLF → unparseable (400 path); LF-only fallback; docs note 400 on unparseable.
- [x] **`[P2]`** **Code review (CP post-ship):** HNSW search does not use the graph
  - *Found*: `search` full-scanned `self.vectors`; `ef_search` unused; `add` could connect self as neighbor.
  - *Done (Batch CQ)*: graph SEARCH-LAYER + self-exclude on connect; discriminating edge-walk test.
- [x] **`[P2]`** **Code review (BT nit):** optional single critical section for `get_index` resolve+lookup; min-replicas FT test
  - *Done (Batch CH + CL)*: `get_index` dual-lock; `tests/cl_min_replicas_ft_test.rs` — FT.CREATE gated by min-replicas-to-write, FT.SEARCH not gated.
- [x] **`[P2]`** **Code review (CL post-ship):** strengthen min-replicas FT tests
  - *Found*: FT.SEARCH “not gated” asserts while a good replica is still registered (min-replicas budget still satisfied — would pass even if SEARCH were a write). Only FT.CREATE checked under NOREPLICAS; DROPINDEX/ALIAS* untested.
  - *Done (Batch CM)*: SEARCH with 0 good replicas (not NOREPLICAS); DROPINDEX/ALIASDEL/ALIASUPDATE under min-replicas=2 with one feed.
- [x] **`[P2]`** **Code review (CM post-ship):** assert FT.ALIASADD under NOREPLICAS
  - *Found*: ALIASADD only on success path after CREATE; regression dropping it from `is_write_command` would not fail current tests.
  - *Done (Batch CN)*: ALIASADD under min-replicas=1 with 0 feeds returns NOREPLICAS.
- [x] **`[P2]`** **Code review (BU):** share one FT.CREATE parser between command path and AOF load
  - *Found*: `parse_ft_create_definition` in `aof.rs` duplicates `handle_ft_create` in `commands/search.rs` — schema options can drift (already two HNSW option loops)
  - *Done (Batch CA)*: shared `IndexDefinition::from_ft_create_argv` / `from_ft_create_args` in `search_index.rs`; command path + AOF load both call it (single `ef_construction` default 200). Tests: unit in `search_index`, `tests/ca_shared_ft_create_parser_test.rs`
- [x] **`[P2]`** **Code review (BU):** HNSW `ef_construction` not round-tripped in AOF rewrite
  - *Found*: rewrite emits `HNSW M <n>` only; load hardcodes `ef_construction: 200` (command path also hardcodes 200 / unused `mut`)
  - *Done (Batch CE)*: parse `EF_CONSTRUCTION` in shared FT.CREATE parser (order-independent with `M`); AOF rewrite emits `EF_CONSTRUCTION`; default 200 when omitted. Tests: `ce_acl_search_hnsw_test` + CA rewrite asserts.
- [x] **`[P2]`** **Code review (BU nit):** VECTOR/NUMERIC AOF rewrite round-trip test; `has_search_state` can avoid double list locks
  - *Done (Batch BX, partial)*: VECTOR/NUMERIC rewrite round-trip in `tests/bu_aof_ft_rewrite_test.rs`; `has_search_state` double-list lock still open
- [x] **`[P2]`** **Code review (BV):** AOF load is not transactional on FT failure
  - *Found*: mid-file FT.CREATE/ALIAS error returns `Err` after earlier commands already applied (BV tests assert first CREATE remains); `load_at_startup` propagates `?` so process should not serve, but in-memory DBs stay partially filled if a caller ignores the error
  - *Done (Batch BW)*: on any `load_into_databases` / `load_into_cache` error, full-wipe target DBs; *Batch BX* routes that path through `flush_all_including_search()` (not live FLUSHDB). Tests: `tests/bw_aof_load_atomic_test.rs`
- [x] **`[P2]`** **Code review (BV nit):** map search OOM strings to `Error::OutOfMemory`; add DROPINDEX/ALIASDEL-missing apply tests (stricter than DEL no-op — intentional Redis-Search-style fail)
  - *Done (Batch BW)*: `map_ft_mutator_error` maps `"OOM"` substrings → `Error::OutOfMemory`; DROPINDEX/ALIASDEL missing apply tests in `tests/bv_aof_ft_load_errors_test.rs`
- [x] **`[P2]`** **Code review (BW):** FLUSHDB/FLUSHALL now drop FT indices (via `Cache::flush` → `search_index_manager.clear`)
  - *Found*: BW cleared search on flush so failed AOF load is fully empty; this also changes live `FLUSHDB`/`FLUSHALL` — RediSearch typically keeps index definitions after FLUSHDB (docs gone, schema remains)
  - *Done (Batch BX)*: decouple paths — `Cache::flush()` / `Databases::flush_all()` clear keyspace + `SearchIndexManager::clear_documents()` (schema + aliases kept); AOF load `Err` uses `flush_all_including_search()` / `SearchIndexManager::clear()` (full wipe). Tests: `tests/bx_flushdb_keeps_ft_schema_test.rs`
- [x] **`[P2]`** **Code review (BW):** failed AOF/RDB load flush wipes pre-existing data if target was non-empty
  - *Found*: `flush_all`/`flush` on load `Err` is correct for empty startup DBs, but a mid-load failure on a non-empty target would destroy prior keys/indices too
  - *Done (Batch CB)*: scratch-load + swap — AOF `load_into_*` and RDB `load_*_bytes` apply into empty (or merge-seeded) scratch; on `Ok` `replace_keyspace_from` / `replace_keyspaces_from`; on `Err` target untouched. Tests: `tests/cb_scratch_load_preserves_target_test.rs`; BW/BZ tests updated for preserve-on-Err semantics.
- [x] **`[P2]`** **Code review (BW nit):** `map_ft_mutator_error` uses `msg.contains("OOM")` (substring); prefer exact/prefix match or typed errors from search layer
  - *Done (Batch BX)*: match `starts_with("OOM:")` / `starts_with("OOM ")` / exact `"OOM"` (search layer emits `"OOM: …"`)
- [x] **`[P2]`** **Code review (BX nit):** `has_search_state` still double-lists (indices + aliases locks); optional single snapshot API
  - *Done (Batch CH)*: `SearchIndexManager::has_any_state` holds aliases+indices once; `Cache::has_search_state` uses it.
- [x] **`[P2]`** **Code review (BX nit):** `clear_documents` does not adjust `MemoryTracker` Search bytes by itself — safe only because `flush` always `memory_tracker.reset()` afterward; document or pair with deallocate if reused outside flush
  - *Done (Batch CH)*: rustdoc on `SearchIndexManager::clear_documents` documents MemoryTracker coupling (flush resets).
- [x] **`[P1]`** **Code review (CB):** wire full keyspace swap under quiesce (not just sharded maps)
  - *Done (Batch CB)*: `Cache::empty_keyspace_like` / `replace_keyspace_from` move keyspace slots (all types; typed expire on slot post-FP) + watch_gens + search take/install + tracker keyspace counts + `memory_usage`; leave pubsub/stats/blockers/maxmemory. Scratch uses `start_sweep: false`; helpers document exclusive-access / load-time quiesce. `Databases::empty_like` / `replace_keyspaces_from` wrap per-DB swap.
- [x] **`[P1]`** **Code review (CB):** keep `Cache.memory_usage` in sync with tracker on swap
  - *Done (Batch CB)*: `replace_keyspace_from` pairs tracker take/install with `memory_usage` store; no per-key `account` after `replace_all`. Drain scratch first, then drain target to discard + install (map-level mem::replace style).
- [x] **`[P0]`** **Code review (CB post-ship):** production load does not quiesce target `background_sweep` during `replace_keyspace_from`
  - *Found*: helpers document exclusive access, but `main` may start DBs with `start_sweep: true` before `load_at_startup`; FULLRESYNC load can race replica autosweep. Between map install and later `install_keyspace_counts` / `memory_usage.store`, expire can deallocate then counters get overwritten with pre-expire scratch totals → ghost `used_memory`. Multi-map torn window mid-install.
  - *Done (Batch CC)*: public AOF/RDB load commit paths wrap replace in `with_autosweep_paused` / `with_autosweep_paused_all`; `main` creates with `start_sweep: false`, applies autosweep + starts sweep tasks only after `load_at_startup`. Tests: `tests/cc_load_quiesce_and_seed_test.rs`.
- [x] **`[P0]`** **Code review (CB second-pass):** `flush=true` / FULLRESYNC peak memory ~2× (regression vs wipe-then-load)
  - *Found*: CB keeps live keyspace full while scratch fills; scratch has independent `MemoryTracker::new(max_memory)` so each side admits a full budget; process RSS not gated. During replace, discard locals + installed maps briefly hold old+new. Affects non-empty RDB load and replica `load_databases_bytes(..., true)`.
  - *Done (Batch CC)*: on successful `flush=true` (and AOF full replace), flush target including search under quiesce **before** `replace_*`; `replace_keyspace_from` drops discard locals immediately after install. Err path never flushes target.
- [x] **`[P0]`** **Code review (CB second-pass):** `flush=false` RDB seed mutates the live target before commit
  - *Found*: merge seed uses `MultiDbSnapshot::from_cache` → `cache.load(..., Default)` with `touch: true` — updates LRU/LFU, lazy-deletes expired keys, bumps shared stats. On merge `Err`, target is not “completely untouched.” `from_databases` same issue.
  - *Done (Batch CC)*: `Cache::export_strings` + `DbSnapshot::from_cache` non-mutating (skip expired, no touch/lazy-delete/stats); save + seed share the same path. Tests: failed merge keeps expired for sweep, live key, zero unexpected cmd_get/hits/evicted_expired.
- [x] **`[P1]`** **Code review (CB post-ship):** multi-DB `replace_keyspaces_from` is not atomic across DBs
  - *Found*: commits one DB at a time; concurrent readers (FULLRESYNC) can see DB0 new + DB1 old; panic mid-loop leaves partial multi-DB commit.
  - *Mitigated (CC–CK)*: staged drain; no multi-DB pre-flush; `load_generation` / `load_in_progress`; Redis-style **`-LOADING`** for data-plane commands during replace; INFO `loading:`.
  - *Done (Batch DR — Option B lock-step)*: after staging, install **all** DBs under one `keyspace_epoch_lock` write; multi-DB exporters use `with_stable_keyspace_view` (epoch read) — `MultiDbSnapshot::from_databases` wired; `load_generation` single end publish (frozen mid-install). Tests: `tests/dr_multidb_atomic_install_test.rs` (exclusion, concurrent non-torn export, raw-Arc residual, gen freeze).
  - [x] **`[P2]`** **Code review (DR post-ship):** panic mid-install still leaves partial multi-DB commit
    - *Found*: epoch write covers the install loop, but no rollback of already-installed DBs on panic. Drop guard clears `load_in_progress` + bumps gen — survivors see incomplete multi-DB dataset as “loaded.”
    - *Done (Batch DS)*: `install_keyspace_payload_retaining_discard` returns prior maps; install loop keeps discards; `InstallRollback` Drop reinstalls olds for fully-swapped DBs while epoch write still held. Tests: `panic_mid_install_rolls_back_already_installed_dbs`. Residual: panic **inside** single-DB multi-map fill (after drain, before return) is not rolled back — Option C Arc-swap for true all-or-nothing single-DB.
  - [x] **`[P2]`** **Code review (DR post-ship):** raw `Arc<Cache>` multi-DB walk still torn mid-install
    - *Found*: command path gated by LOADING; multi-DB exporters use epoch read. Residual: any internal path that holds per-DB `Arc<Cache>` and walks DBs without `with_stable_keyspace_view` can still see DB0-new + DB1-old (test `raw_per_db_access_can_see_mid_loop_tear` documents this).
    - *Done (Batch DS audit)*: AOF `rewrite_databases` now under `with_stable_keyspace_view` (was torn). RDB `from_databases` already safe. Documented: `Databases::iter` rustdoc; INFO blocked_clients / CONFIG multi-DB walks are non-keyspace (no epoch). Metrics single-cache. Residual raw mid-loop tear still true for unprivileged raw Arc walks during install (documented by existing DR test); exporters must use epoch read.
  - [x] **`[P3]`** **Code review (DR/DS residual):** single-DB mid-`install_keyspace_payload` tear
    - *Found*: even under epoch write, one DB’s install is multi-map sequential (strings then typed…). Allowlisted probes on a single selected DB during LOADING can still see partial single-DB maps if they bypass LOADING (they should not). Panic mid-fill also skips multi-DB discard rollback (discard only returned after fill completes).
    - *Done (Batch DT, document/accept)*: keep **LOADING** as the barrier; no Arc-swap unless privileged paths grow. Documented in `docs/locking.md` (Keyspace replace residuals).
  - [x] **`[P3]`** **Code review (DR post-ship nit):** production test hook `after_install_db`
    - *Found*: `Mutex<Option<Arc<dyn Fn(usize)>>>` on `Databases` for DR tests. Harmless when None; extra field on every process.
    - *Done (Batch DS docs)*: keep on type — `tests/` integration tests cannot see lib `cfg(test)`; feature flag would force CI `--features`. Rustdoc states production always `None`.
- [x] **`[P2]`** **Code review (CP post-ship):** LOADING allowlist still runs `PSYNC`/`SYNC`/`CONFIG` during replace
  - *Found*: `loading_denied` allowed `INFO`/`ROLE`/`REPLCONF`/`PSYNC`/`SYNC`/`CLIENT`/`CONFIG`/`MODULE` (and auth/admin probes). Full sync snapshots live multi-DB maps and can observe mid-`install_keyspace_payload` torn state (strings filled, typed maps empty, counters not yet installed). Data plane is correctly gated.
  - *Done (Batch CR)*: deny `SYNC`/`PSYNC` during `load_in_progress` (`-LOADING`); keep allowlist for connection/discovery/repl handshake (`AUTH`/`HELLO`/`PING`/`ECHO`/`QUIT`/`RESET`/`INFO`/`COMMAND`/`ROLE`/`REPLCONF`/`CLIENT`/`CONFIG`/`MODULE`). `CONFIG` left allowed (ops/live params; no keyspace snapshot). Docs: `docs/locking.md` Keyspace replace. Tests: `tests/ck_loading_gate_test.rs`.
- [x] **`[P1]`** **Code review (CC post-ship):** WATCH bump not atomic with keyspace install (race window)
  - *Found*: `replace_keyspace_from` installs scratch `watch_gens` (usually empty) and releases the lock, then later bumps `pre_watch_keys`. Between those steps `watch_generation` can `or_insert(0)` so a client that WATCHed at gen 0 sees clean EXEC against new/empty data. On `flush=true` the clean window spans flush (which does not touch watch_gens) through end of replace.
  - *Done (Batch CD)*: under one `watch_gens` lock, install `other_watch` and bump all `pre_watch_keys`; AOF/RDB `flush=true` commit calls `touch_all_watch_keys` before flush. Tests: `tests/cd_watch_atomic_and_typed_export_test.rs`.
- [x] **`[P1]`** **Code review (CB post-ship):** keyspace replace does not bump `watch_gens` (unlike FLUSHDB `touch_all_watch_keys`)
  - *Found*: install uses scratch’s empty `watch_gens`; clients with WATCH gen `0` can still EXEC after full dataset replace. Harmless at exclusive startup; wrong if load runs with live WATCH holders.
  - *Done (Batch CC + CD)*: sequential bump in CC; atomic install+bump + pre-flush touch in CD.
- [x] **`[P1]`** **Code review (CB second-pass):** scratch shares `Stats` Arc with target (`new_keyspace_sharing`)
  - *Found*: RDB/AOF apply on scratch increments shared `cmd_set`/`cmd_get`/OOM counters; failed loads never commit keyspace but permanently inflate INFO; success counts internal apply as client commands. PubSub category install itself is fine (KEYSPACE excludes PubSub).
  - *Done (Batch CC)*: `empty_keyspace_like` uses independent `Stats::new()`; multi-DB siblings still share stats.
- [x] **`[P1]`** **Code review (CC post-ship):** `with_autosweep_paused` does not stop an in-flight expire cycle
  - *Found*: only stores `autosweep_enabled=false`; `background_sweep` re-checks at loop top — mid-cycle `active_expire_cycle` + accounting still runs during flush/replace on live FULLRESYNC. Startup path safe (no task until after load).
  - *Done (Batch CD)*: `autosweep_cycle_lock` held for whole expire body; `with_autosweep_paused` disables flag then acquires the lock (waits for in-flight cycle) before running `f`.
- [x] **`[P2]`** **Code review (CC post-ship):** typed `export_*` can revive expired typed keys without TTL
  - *Found*: string export skips `is_expired()`; typed exports dump every key while `export_typed_expires_unix_ms` omits elapsed TTLs — `from_cache` seed/save can reify expired zset/hash/etc without expire record.
  - *Done (Batch CD)*: `typed_key_exportable` filters past TTL in zset/geo/hash/list/set/stream export; test expired hash not in `export_hashes` / `from_cache`.
- [x] **`[P2]`** **Code review (CB post-ship):** expand CB tests — post-swap memory_tracker + `string_memory_usage`; multi-DB fail/success; typed TTL after swap; PubSub category non-clobber; empty-AOF success on non-empty target; peak-memory budget; seed non-mutation on failed merge; concurrent WATCH race
  - *Done (CC–CO)*: seed non-mutation, multi-DB fail/success, post-swap string memory, empty-AOF replace, typed TTL RDB, PubSub take/install, LOADING+WATCH, load_generation.
  - *Done (Batch CP)*: dual-residency peak documented in `docs/benchmarks.md` (measure RSS if needed). Full automated peak budget test still optional.
- [x] **`[P2]`** **Code review (CB):** `drain_all` / `replace_all` not fully failure-atomic across shards
  - *Partial (Batch CB)*: pre-`reserve(self.len())` on drain_all; exclusive-access docs.
  - *Done (Batch CL)*: `replace_all` on `ShardedHashMap`/`ShardedKeyMap` uses drain-then-fill (holds discard until inserts finish). True multi-shard atomic under concurrent mutators still not claimed — exclusive load-time use only.
- [x] **`[P2]`** **Code review (CL post-ship):** `install_keyspace_payload` double-drains maps via `replace_all`
  - *Found*: payload install already `drain_all`s into outer `discard_*`, then `replace_all` drains again (empty). Extra alloc/work; CL durability claim is on the outer discards, not `replace_all`.
  - *Done (Batch CM)*: `fill_all` on `ShardedHashMap`/`ShardedKeyMap`; install uses fill after external drain.
- [x] **`[P2]`** **Code review (CM post-ship nit):** `fill_all` has no emptiness `debug_assert`
  - *Found*: misuse without prior drain silently merges old+new keys. Sole production caller drains first.
  - *Done (Batch CN)*: `debug_assert!(self.is_empty())` on `ShardedHashMap`/`ShardedKeyMap` `fill_all`.
- [x] **`[P2]`** **Code review (CB nit):** `install_keyspace_counts` not closed over `KEYSPACE_CATEGORIES`
  - *Done (Batch CB)*: install always writes fixed `KEYSPACE_CATEGORIES` slots (ignores fabricated category tags; PubSub cannot be clobbered).
- [x] **`[P2]`** **Code review (CB nit):** `SearchIndexManager::install` does not validate alias targets exist in indices (fine if only fed `take_all` output)
  - *Done (Batch CF)*: `debug_assert!` that every alias target exists in the indices map.
  - *CF post-ship nit*: validate only in debug; release still accepts dangling aliases if non-`take_all` callers mis-pair maps.
- [x] **`[P2]`** **Code review (CB second-pass nit):** `empty_keyspace_like` hardcodes `loadfactor: 0.75` (diverges from process create-time loadfactor; capacity churn only)
  - *Done (Batch CF)*: `Cache` stores create-time `loadfactor`; `empty_keyspace_like` / keyspace sharing pass it through.
- [x] **`[P2]`** **Code review (CC nit):** `new_with_sweep_loadfactor` still inlines `tokio::spawn(background_sweep)` instead of `start_background_sweep` (duplication / drift risk)
  - *Done (Batch CD)*: both create paths call `start_background_sweep`.

### Pub/Sub

- [x] **`[P1]`** Slow-client and memory limits under fan-out load
  - *Done (MVP)*: configurable client buffer capacity (default 1024); fan-out admission `message_size * max(1, N)` against maxmemory; pending PubSub memory until deliver/unregister; `RecvError::Lagged` disconnects slow clients; full broadcast buffer overwrites without panic + `messages_dropped` stat. See `docs/pubsub.md` policy notes; tests in `tests/pubsub_test.rs`
- [x] **`[P1]`** Pattern matcher: iterative (or bounded) matching to avoid deep recursion stack risk
  - *Done*: `PatternMatcher::matches` is iterative star-backtrack (supports `*` `?` `[]` `\`); pathological `*` stress test.

---

## Engineering quality (ongoing)

**Phase priority: Ongoing** — run in parallel; raise priority when touching related code

- [ ] **`[P0]`** Tests for the phase you are implementing (always land with the feature)
- [x] **`[P1]`** **CI**: build, unit tests, integration tests, optional redis-cli compatibility smoke
  - *Done*: `.github/workflows/ci.yml` — build + `cargo test --all-targets -- --test-threads=1`
- [x] **`[P1]`** **Benchmarks**: expand `docs/benchmarks.md` with methodology and numbers vs Redis/Valkey (`redis-benchmark`, same hardware)
  - *Done*: methodology runbook; **Batch FD** filled single-host median tables vs Valkey 9.0.0 (Redis non-Valkey not measured — 6379 busy)
- [x] **`[P1]`** **Fuzz** RESP parser and command argument parsing
  - *Done*: in-tree smoke fuzz unit tests (random + structured); `fuzz/` crate with `resp_parse` + `command_dispatch` targets (`cargo +nightly fuzz run …` when cargo-fuzz installed).
- [x] **`[P1]`** **Concurrency / loom or stress** jobs for shard RMW paths
  - *Done*: `tests/concurrency_stress_test.rs` — concurrent INCR, INCR/DECR net-zero, SET NX single winner, multi-key multi-shard, mixed RMW+reads, hash field RMW under `parking_lot`
- [x] **`[P2]`** Align version strings in docs/`INFO` examples with `Cargo.toml`
  - *Done (Batch CK)*: README `kore_version` examples → 0.6.0
  - *Done (Batch GC-rel)*: **0.7.0** cargo + README examples + ops runbook
- [x] **`[P2]`** Consistent locking and error handling guidelines in contributor docs
  - *Done (Batch CO)*: `docs/locking.md` (lock orders, load commit, errors); linked from README.
- [x] **`[P2]`** Keep `docs/roadmap.md` in sync with this file (or make this the single source of truth)
  - *Done (Batch CO)*: roadmap section for recent persistence/search letter batches pointing at `TODO.md`.
  - *Done (Batch GC-docs)*: post-GB pointer to **GC+** queue; README feature matrix; root `CHANGELOG.md`.
- [x] **`[P2]`** **Code review (BS nit):** assert post-`EVAL` connection DB after Lua `SELECT` (Redis-compatible side effect)
  - *Done (Batch BT)*: `bt_eval_select_persists_connection_db` — connection remains on selected DB after EVAL

### Status snapshot (2026-08-02, post-GC-rel / **v0.7.0**)

**Release cut:** **0.7.0** = productization **GC–GH + GK + GL** (ops runbook, client smoke, TLS depth, MIGRATE dump wire, SET perf). **0.6.0** remains the FW–GB baseline story in the changelog (may be untagged; cargo was already 0.6.0). Phases **A–E** green. Standing **`[P0]`** tests-with-feature remains.

**Shipped letter stream:** FW–GB baseline · GC–GH · GK · GL · **GC-rel**.

**What remains:** optional P3 residuals only (binary bus, etc.). Accepted residuals stay out of the active queue.

| Area | Residual | Priority | Planned batch |
|------|----------|----------|---------------|
| Docs / release | **0.7.0** + `docs/ops.md` | **done** | **GC-rel** |
| Perf | Pipeline SET gap vs Valkey (~0.73M vs ~1.59M GF) | P1 residual | store path later |
| Ops | Global repl backlog ordered publish residual | P3 residual | after GE |
| Compat | stream listpack foreign DUMP fixtures; geo restore as zset | P3 residual | — |
| Compat | Redis Functions library + real FCALL (**GI** done) | **P2** | — |
| Compat | Lua setresp + time limits (**GJ** done) | **P2** | — |
| Security | admin HTTP auth + TLS (**GM** done) | **P2** | — |
| Cluster | binary bus 2PC; dest prepare breadth; operator NODE bypass | P3 accepted / later | **GP** |
| Cluster | remote CHECKPREPARE→COMMITPREPARE two-RPC window | **done (GO)** | — |
| Cluster | per-slot config epochs fully in nodes.conf | **done (GN)** | — |
| Sentinel | long-lived `__sentinel__:hello` SUBSCRIBE fan-in | P3 accepted | — |
| Sentinel | Kore **higher** priority wins vs Redis lower (documented) | P3 accepted | — |
| Sentinel | serial INFO enrich / peer PING | P3 accepted lite | **GQ** optional |
| Search | HNSW `max_m=1` spanning residual | **done (GR)** | — |
| Search | search-doc access-touch for true LRU among docs | **P3** | **GS** |
| Search | inverted-index `_weight` unused (TF-IDF future) | **P2** | **GT** |
| Search | larger-N ANN recall/throughput (DK `#[ignore]` N=5000) | **P2** | **GU** |
| Keyspace / load | single-DB Arc-swap mid-fill; RENAME remove+insert TOCTOU | P3 accepted | — |
| UI | cluster reshard weight UI | P3 accepted | — |

### Next work queue (post-GB)

**Letter queue FW–GB is complete.** Next stream is **productization / perf / compat** (batches **GC+**). Standing rule: land tests with each feature (**P0 process**).

Phases A–E P0 lists are green; prefer **P1 product/perf** over hunting accepted P3s.

#### Track A — Release / docs hygiene
- [x] **`[P3]`** gitignore local `/data/` (nodes.conf / sentinel scratch)
- [x] **`[P1]`** Review + push/PR the commit stack on `main` — **closed 2026-08-02** (`main` == `origin/main`)
- [x] **`[P0]`** Standing: tests land with each feature (process rule — never closed as a checklist item; always applies)
- [x] **`[P1]`** **Batch GC-docs** — stamp post-GB status; `CHANGELOG.md`; README feature matrix; roadmap pointer
- [x] **`[P1]`** **Batch GC — Pipeline SET profile + hot-path cuts** (standalone stream skip; ~+19% SET P=16 vs FI on M3 Pro)
- [x] **`[P1]`** **Batch GD — CommandId enum dispatch + ACL lower alloc**
- [x] **`[P1]`** **Batch GC-rel — Release cut 0.7.0 + production runbook**
  - **Decision:** **0.7.0** = GC–GH + GK + GL productization; **0.6.0** = FW–GB baseline (changelog)
  - `Cargo.toml` → `0.7.0`; `CHANGELOG.md` release section; README version examples
  - Runbook: [`docs/ops.md`](docs/ops.md) (persistence, replica/Sentinel/cluster, TLS, health, checklist)
  - Tag: `v0.7.0` on this release commit

#### Recommended letter queue (start here)

Ordered execution after **0.7.0**:

1. Optional P3 residuals only when production needs them (GP binary bus, GR/GS search polish, …)

| Batch | Pri | Track | Scope |
|-------|-----|-------|--------|
| **GC** | P1 | Perf | **done** — standalone stream skip + store/argv cuts; SET P=16 ~741k vs FI ~621k |
| **GD** | P1 | Perf | **done** — `CommandId` length-first dispatch; stack ACL lowercase; enum `is_write` |
| **GE** | P2 | Perf/ops | **done** — ordered deferred fan-out (short backlog lock; `fanout_order` serializes sends) |
| **GF** | P1 | Perf | **done** — full re-bench vs Valkey 9; SET P=16 ~730k vs Valkey ~1.59M |
| **GG** | P1 | Compat | **done** — MIGRATE DUMP→RESTORE for core types; geo/stream recreate residual |
| **GH** | P2 | Compat | **done** — geo ZSET_2 geohash; stream type-15+KST1; KDF1 dual-detect kept |
| **GI** | P2 | Compat | **done** — Functions library: LOAD/LIST/DELETE/FLUSH/DUMP/RESTORE + real FCALL/FCALL_RO |
| **GJ** | P2 | Compat | **done** — `redis.setresp`, `lua-time-limit`, SCRIPT/FUNCTION KILL |
| **GK** | P1 | Compat | **done** — `scripts/client_smoke.sh` + CI (redis-cli + redis-py; ioredis optional) |
| **GL** | P1 | Security | **done** — mTLS (`--tls-auth-clients`/`--tls-ca`), dual port (`--tls-port`), replica TLS (`--tls-replication`) |
| **GM** | P2 | Security | **done** — Bearer/Basic auth, admin TLS, non-loopback requires auth |
| **GN** | P2 | Cluster | **done** — `# slot-epoch` ranges in nodes.conf round-trip |
| **GO** | P3 | Cluster | **done** — dest CHECKPREPARE folded into atomic COMMITPREPARE |
| **GP** | P3 | Cluster | Binary cluster bus 2PC (large; only if Redis cluster clients demand it) |
| **GQ** | P3 | Sentinel | **done** — parallel peer PING + replica INFO priority probes |
| **GR** | P3 | Search | **done** — reconnect prune floor 2 for upper-layer M=1 path middles |
| **GS** | P3 | Search | Search-doc access-touch for true LRU among docs (FZ residual) |
| **GT** | P2 | Search | **done** — inverted-index TF + field-weight TF-IDF; FT.SEARCH WITHSCORES |
| **GU** | P2 | Search | **done** — adaptive HNSW ef for large-k; mid-N@k=50 CI recall gate |

Checklist (active):

- [x] **`[P1]`** **Batch GC — Pipeline SET profile + hot-path cuts**
  - Standalone `needs_stream_publish` skip (encode + backlog); `arm_stream_history` for tests
  - `maybe_persist` / `on_write_command` early path; static write-cmd Bytes; SET option parse without String; drop pre-store type probe; map overwrite without key clone
  - Tests: `standalone_cold_skips_stream_until_armed_or_replica`; `standalone_set_skips_repl_stream`; repl/failover arm stream where offset required
  - Bench: SET c=50 P=16 median **~740k** ops/s (FI **~621k**, **~+19%**); residual vs Valkey still large — see `docs/benchmarks.md` Batch GC
- [x] **`[P1]`** **Batch GD — Command dispatch / alloc reduction**
  - `src/commands/cmd_id.rs`: `CommandId` (248 cmds), length-first `from_upper`, `is_write()`, `static_name()`
  - Main dispatch + write gate + slowlog skip + pubsub allowlist match on enum
  - ACL / cluster key-spec: stack lowercase (no `to_ascii_lowercase` String)
  - Bench: SET P=16 stays in GC band (~741k median); structural/hygiene win — see `docs/benchmarks.md` Batch GD
- [x] **`[P2]`** **Batch GE — Repl publish ordered deferred fan-out**
  - Under backlog lock: SELECT + append + offset only; release before `try_send`
  - `fanout_order` taken while holding backlog (append order == send order); N==0 skips fan-out
  - Partial PSYNC holds `fanout_order` across register (no CONTINUE/history double-send)
  - Fullsync barrier unchanged; FI-2 multi-DB atomicity preserved
  - Tests: `multi_writer_live_feeds_match_backlog_order`, `multi_replica_serial_set_same_ordered_payloads`, `partial_psync_no_duplicate_with_concurrent_publish`
  - Residual: backlog still serializes append; fan-out globally ordered (not parallel per-replica)
- [x] **`[P1]`** **Batch GF — Re-bench vs Valkey (+ optional Redis column / CI smoke)**
  - Full FD suite (SET/GET P=1 & P=16, INCR, d=256); warm-up + 3 passes; median ops/s + p50
  - Kore **0.6.0** git `ffefd35` vs Valkey **9.0.0** on :6378; Redis column N/A (Homebrew redis → Valkey)
  - SET P=16 median **~729,927** ops/s (GC/GD band); Valkey **~1,587,302**; non-pipeline ~93–94% of Valkey
  - CI microbench smoke skipped (absolute ops/s noisy)
  - See `docs/benchmarks.md` → Batch GF
- [x] **`[P1]`** **Batch GG — MIGRATE over DUMP/RESTORE**
  - Core types (string/list/set/hash/zset): local `dump_serialized` + dest `RESTORE` (+ `REPLACE` / `ABSTTL`)
  - Geo/stream: still RESP recreate (KDF1 DUMP is Kore-only; recreate works vs Redis dest)
  - TTL: ABSTTL absolute Unix-ms end (DT honesty on dump path)
  - Tests: unit path selection + dump/restore hash; e2e zset+TTL + geo recreate; full `dp_migrate_test` 16/16
  - Residual: geo/stream Redis DUMP wire → **GH**; MIGRATE still not binary-compatible with Redis DUMP of geo/stream
- [x] **`[P2]`** **Batch GH — DUMP Redis wire for geo/stream**
  - Geo: Redis **ZSET_2** with 52-bit geohash scores (Redis GEO shape); RESTORE → zset
  - Stream: type **15** Redis-7 metadata + Kore `KST1` entry/group body; RESTORE → stream
  - Legacy **KDF1** still dual-detected on RESTORE
  - MIGRATE uses DUMP→RESTORE for geo/stream (all types)
  - Residual: foreign Redis listpack stream DUMP fixtures; GEO* after geo DUMP restore is zset (Redis TYPE)
  - Tests: rdb_object geo/stream; fy dump wire; migrate geo/stream e2e
- [x] **`[P2]`** **Batch GI — Redis Functions library**
  - `FunctionLibraryStore` (shared server-wide) + shebang `#!lua name=` parse / strip for Lua exec
  - `FUNCTION LOAD [REPLACE]`, `LIST` [`LIBRARYNAME`/`WITHCODE`], `DELETE`, `FLUSH`, `DUMP`/`RESTORE` (Kore `KORF1`), `STATS`, `HELP`, `KILL`→NOTBUSY
  - `redis.register_function(name, cb)` and table form (`function_name`/`callback`/`flags`/`description`)
  - `FCALL` / `FCALL_RO` (RO requires `no-writes`; write `redis.call` denied under RO)
  - ACL `@scripting` includes `function`/`fcall`/`fcall_ro`
  - Tests: `tests/gi_function_library_test.rs`; BO stub expectations updated
  - Residual: Redis-native dump blob (not KORF1); library code not durable in RDB/AOF yet
- [x] **`[P2]`** **Batch GJ — Lua script limits + setresp polish**
  - `redis.setresp(2|3)`: script-local RESP for `redis.call` replies and script returns (bool/map/double wrappers)
  - `CONFIG GET|SET lua-time-limit` (ms; default 5000; `0` = unlimited); shared `ScriptRuntime`
  - Hard abort via mlua instruction hooks when past limit; write tracking for KILL
  - Real `SCRIPT KILL` / `FUNCTION KILL`: NOTBUSY / OK / UNKILLABLE
  - Tests: `tests/gj_lua_setresp_time_limit_test.rs`
  - Residual: Redis soft-BUSY-to-other-clients model (Kore multi-thread hard-aborts); no RESP3 double wire type
- [x] **`[P1]`** **Batch GK — Multi-language client smoke + CI**
  - `scripts/client_smoke.sh`: redis-cli (string/hash/list/set/zset, multi-DB `-n`); optional redis-py / ioredis
  - CI: `.github/workflows/client-smoke.yml` (redis-tools + python3-redis + release build)
- [x] **`[P1]`** **Batch GL — TLS mTLS / dual listener / replica TLS**
  - `--tls-port`: plain on `--port` + TLS on dedicated port; `tls_port==0` keeps TLS-only on `--port`
  - `--tls-auth-clients` + `--tls-ca`: mTLS via rustls `WebPkiClientVerifier`
  - `--tls-replication`: replica→primary TLS; trust `--tls-ca` or `--tls-cert`
  - Tests: dual listener, mTLS reject anonymous / accept client cert; existing TLS suite green
- [x] **`[P2]`** **Batch GM — Admin HTTP auth + TLS**
  - Shared `AdminHttpOptions`: Bearer token, Basic user/password, optional TLS acceptor, bind host
  - Header-aware request parse; `401` + `WWW-Authenticate` when credentials configured
  - Metrics + deadlock UI accept loops: plain or TLS via `serve_connection`
  - Flags: `--admin-bind`, `--admin-http-token`, `--admin-http-user`/`--admin-http-password`,
    `--admin-tls` + `--admin-tls-cert`/`--admin-tls-key` (fallback to `--tls-cert`/`--tls-key`)
  - Non-loopback bind requires auth; basic user/password must be paired
  - Tests: `tests/gm_admin_http_auth_tls_test.rs`; existing metrics/UI exchange tests updated
  - Residual: no mTLS for admin scrape; no rate limiting
- [x] **`[P2]`** **Batch GN — Per-slot epochs in nodes.conf**
  - SAVECONFIG / autosave emit `# slot-epoch <start> <end> <epoch>` (range-compressed)
  - Load restores per-slot epochs; pre-GN files stamp owned slots with file epoch
  - Tests: `nodes_conf_slot_epochs_round_trip`
- [x] **`[P3]`** **Batch GO — Single-RPC prepare commit window**
  - Dual-end commit re-check: source `check_prepare_valid` + dest MYID only
  - Dest prepare fence is atomic `COMMITPREPARE` (no separate CHECKPREPARE RPC)
- [ ] **`[P3]`** **Batch GP — Binary cluster bus 2PC**
- [x] **`[P3]`** **Batch GQ — Parallel Sentinel probes**
  - `count_reachable_sentinels`: parallel peer PINGs
  - `enrich_replica_priorities`: parallel INFO `slave_priority` fetches
- [x] **`[P3]`** **Batch GR — HNSW max_m=1 spanning**
  - `bridge_reconnect_neighbors` reconnect prune uses `prune_m = max_m.max(2)` so NN-path middles keep both path edges when upper layers have `M=1`
  - Insert-time prune still uses bare `max_edges` (not a global degree floor / insert churn change)
  - Test: `hnsw_bridge_upper_layer_m1_path_reconnects` (M=1 multi-layer, hub remove, BFS on layer 1)
- [ ] **`[P3]`** **Batch GS — Search-doc access-touch LRU**
- [x] **`[P2]`** **Batch GT — TF-IDF weight scoring**
  - `InvertedIndex` stores postings `term→(doc→tf)` + doc lengths; field WEIGHT multiplies score
  - Score: `weight * Σ (1+ln(tf)) * ln(1+N/df)`; multi-field OR sums contributions
  - Query executor ranks text hits by TF-IDF; `FT.SEARCH … WITHSCORES` / `NOCONTENT`
  - Tests: unit tf/df/weight; `tests/gt_tfidf_gu_ann_test.rs`
  - Residual: no BM25 params / stemmer; scores not yet durable metadata
- [x] **`[P2]`** **Batch GU — Larger-N ANN bench / recall**
  - `HNSWIndex::effective_ef_search(k)` scales beam for large-k (`max(ef, 2k)` when k large)
  - `set_ef_search` / `search_with_ef` for runtime tuning
  - CI gate: `hnsw_recall_mid_n_large_k_vs_flat` (N=800, k=50, recall@1≥0.95, @50≥0.88)
  - Ignored N=5000 median bench retained; see `docs/benchmarks.md`
  - Residual: at moderate N, HNSW can still be slower than FLAT (expected overhead)

#### Defer / accept (not in active queue)

Do **not** schedule these as sprint work unless a production need appears:

- Cluster binary bus 2PC; dest prepare breadth; operator NODE bypass (intentional recovery)
- Remote CHECK→COMMITPREPARE two-RPC window (accepted lite)
- Reshard weight UI
- `__sentinel__:hello` long-lived SUBSCRIBE fan-in
- Sentinel priority honesty vs Redis (documented)
- single-DB Arc-swap mid-fill (LOADING barrier accepted)
- Deadlock UI browser-driven repaint tests
- Dual SELECT trackers; `propagate_raw` skips stream-DB
- RENAME remove+insert; create ensure_type→insert TOCTOU (historical)

#### Prior letter queue (closed FW–GB)

- [x] **`[P2]`** **Batch FW — ANN query path on HNSW**
- [x] **`[P2]`** **Batch FY — DUMP/RESTORE Redis wire**
- [x] **`[P3]`** **Batch FX — HNSW AOF graph**
- [x] **`[P3]`** **Batch FZ — Search-doc eviction special**
- [x] **`[P3]`** **Batch GA — Retire `Entry.expires_at` field**
- [x] **`[P3]`** **Batch GB — Repl backlog serialize** (partial win)

#### Completed (FB–… + FU + FV + FW + FX + FY + FZ + GA + GB)

- [x] **`[P2]`** **Batch FW — ANN query path on HNSW** (`ebd88e9`)
  - `SearchIndex::get_hnsw_index`; query engine prefers HNSW ANN; FLAT exact; empty → flat
  - Tests: craft connectivity edge-walk; FLAT exact; empty HNSW fallback

- [x] **`[P3]`** **Batch FX — HNSW AOF graph** (`ebd88e9`)
  - AOF rewrite: `FT.CREATE` → keys → `FT._LOADGRAPH` → aliases; shared snapshot codec with RDB v6
  - Tests: `tests/fx_aof_hnsw_graph_test.rs`; legacy AOF rebuild path kept

- [x] **`[P2]`** **Batch FY — DUMP/RESTORE Redis wire** (`d7e59e5`)
  - DUMP Redis-compatible for string/list/set/hash/zset; geo/stream KDF1; RESTORE dual-detect
  - Module `src/rdb_object.rs`; tests `tests/fy_dump_restore_redis_wire_test.rs`
  - Residual: stream/geo Redis RDB; MIGRATE still recreate; IDLETIME/FREQ no-op

- [x] **`[P3]`** **Batch FZ — Search-doc eviction special**
  - Proportional allkeys sampling; volatile excludes search docs; dominate tests green
  - Residual: docs outside `key_values` by design; optional access-touch for true LRU among docs

- [x] **`[P3]`** **Batch GA — Retire `Entry.expires_at` field**
  - Full field removal; slot-only TTL; `string_rmw_slot_only_ttl`; typed_ttl green

- [x] **`[P3]`** **Batch GB — Repl backlog write serialization** (`dfcf7aa`)
  - Atomic fullsync barrier; hot path no Mutex gate; backlog still orders SELECT+append+fanout
  - **No Valkey parity claim**; residual further parallelization deferred

- [x] **`[P2]`** **Batch FV — HNSW durable graph in RDB** (`d2c03a4`)
  - `HnswGraphSnapshot` + `HNSWIndex::snapshot_graph` / `apply_graph_snapshot` (entry, levels, edges; neighbor lists sorted for determinism)
  - SearchIndex dual-write for VECTOR HNSW fields; Text→vector parse for hash auto-index
  - KORDB **v6** HNSW graph section after search aliases; v5- loads keep rebuild-by-readd
  - AOF residual closed by **FX** (`FT._LOADGRAPH`); query path residual closed by **FW**
  - Tests: unit snapshot round-trip; `tests/fv_rdb_hnsw_graph_test.rs` RDB SAVE/load edge-identical
  - Residual: `max_m=1` spanning closed by **GR**


- [x] **`[P1]`** **Batch FB — Cluster dual-end NODE wire 2PC (slice 1)**
- [x] **`[P1]`** **Batch FC — Sentinel promote-success gate**
- [x] **`[P1]`** **Batch FD — Benchmarks: measured numbers vs Valkey**
  - Host M3 Pro; pipeline SET is main Kore gap; no portable-win claims
- [x] **`[P2]`** **Batch FE — Sentinel leader election depth**
- [x] **`[P2]`** **Batch FF — HNSW multi-layer insert**
  - Geometric levels + upper-layer connect/search; residual closed for RDB by **FV** (AOF still rebuilds)
- [x] **`[P2]`** **Batch FG — Unified keyspace design + facade**
- [x] **`[P2]`** **Batch FG-2 — Physical hashes in `key_values`**
- [x] **`[P2]`** **Batch FG-3 — Remaining typed types + payload collapse** (`1db780e`)
  - list/set/zset/geo/stream in `key_values`; legacy typed maps removed; payload = `map` + `key_values` stream
  - *Post-ship review:* no dual-write leftover; typed RENAME is remove+insert (accepted); strings dual-map residual → FG-4
- [x] **`[P3]`** **Batch FG-4 / FJ — Strings into unified map** (`6f40cbb`)
  - `KeyValue::String` physically in `Cache::key_values`; dual `Cache::map` removed; `mutate_string` RMW under shard lock + CAS
  - `KeyspacePayload` collapsed to single `key_values` stream (+ expires/WATCH/search/memory)
  - Eviction / active expire / SCAN / KEYS / DBSIZE / RENAME sample or walk one map
  - *Post-ship review:* true single-map keyspace for all Redis types; residual closed by **FP** (`typed_expires` → slot) + **FQ** (string TTL on slot); search-doc eviction special remains
  - Tests: keyspace / string_ops / typed_ttl / phase_a / eviction / persistence green
- [x] **`[P2]`** **Batch FH — NODE 2PC slice 2** (`b078988`)
  - Prepare-epoch + TTL fence; `CHECKPREPARE` + dual-end commit re-check; soft clear fail-closed
  - Tests: stale epoch / cleared / TTL / boot; e2e recheck inject no half-apply; happy path complete (41 migrate tests)
  - Residual closed by **FO**: durable prepare + COMMITPREPARE; remaining: bus 2PC; dest prepare breadth; operator NODE bypass
- [x] **`[P2]`** **Batch FI — Pipeline SET perf investigation** (`bb97e7f`)
  - Root causes ranked in `docs/benchmarks.md` → *Pipeline SET analysis*
  - Wins: AOF-off unlock encode/propagate; direct `encode_command`; skip empty replica lock; slowlog/argv/`+OK` alloc cuts; typed-only WRONGTYPE; store value move
  - Re-measure (same host/method as FD): SET c=50 P=16 median **~621k** ops/s (FD **~498k**, **+~25%**); Valkey still ~1.59M
  - Residual **FI-2** (correctness race fixed; backlog serialize remains) → partial address **GB**
- [x] **`[P2/P3]`** **Batch FI-2 — AOF-off multi-DB SELECT ordering** (`f88e83d`)
  - Root cause: FI unlocked encode/propagate outside AOF mutex; `selected_db` update was not ordered with backlog append → concurrent multi-DB writers could land SELECT-less cmds before another thread’s SELECT
  - Fix: `ReplicationManager::propagate_write` — encode cmd outside locks; under publish section decide lazy SELECT, append SELECT+cmd (one payload) or cmd, update `ReplBacklog.selected_db`; AOF-off skips `aof` lock; AOF-on still holds AOF across disk append + `propagate_write` (disk/stream order)
  - Tests: `aof_select_concurrency_test` 8/8; unit `propagate_write_*` + `promote_resets_stream_selected_db`; multi-DB + TCP repl green
  - *Post-ship review:* correctness fix holds; residual global backlog serialize partially reduced by **GB** (dropped exclusive fullsync Mutex on hot path); dual SELECT trackers (AOF vs stream) intentional; `propagate_raw` still no stream-DB update (low-level/tests only)

- [x] **`[P3]`** **Batch FK — Sentinel promote ranking + lite SM polish** (`78730ca`)
  - Rank: highest priority (0 never) → highest ROLE offset → greatest `ip:port` (mirrors cluster EA/EB)
  - Closed open review: *FC post-ship nit first-replica-wins*
  - ROLE parse stores offset; priority defaults **100**
  - Tests: rank unit + multi-replica prefers priority/offset; priority 0 skipped
  - Residual closed by **FM**: live INFO `slave_priority` + auto-failover cooldown
- [x] **`[P3]`** **Batch FL — `nodes.conf` live cluster flags** (`3113db6`)
  - Header comments: `# require-full-coverage` / `# allow-reads-when-down` / `# announce-ip` / `# announce-port` / `# replica-priority`
  - SAVECONFIG + topology autosave write flags; load_or_single_node / from_nodes_conf restore; missing keys → defaults
  - CONFIG SET of live flags best-effort autosaves when `dir` set; boot CLI overrides only **non-default** values (plain restart keeps saved)
  - Closed open review: *EN/EO/EU post-ship nodes.conf omits live flags*
  - *Post-ship review:* migrate **42/42**; live-flags unit + legacy defaults green; flags as `#` comments (node-line parsers stay compatible)
  - Residual closed by **FO**: prepare votes in nodes.conf
- [x] **`[P3]`** **Batch FM — Sentinel residual polish (FK leftovers)** (`bbebfef`)
  - Live `INFO replication` `slave_priority` refresh on master probe + before `try_failover` rank
  - `ReplicationManager::slave_priority` + CONFIG GET/SET `replica-priority` / `slave-priority`; INFO emits `slave_priority`
  - Auto-failover cooldown **15s** (`FAILOVER_COOLDOWN`) after completed/failed `try_failover`; manual `SENTINEL FAILOVER` force-bypasses
  - Keeps FC promote-success gate + FK ranking (highest priority → offset → greatest `ip:port`; 0 never)
  - Honesty: Kore **higher** priority wins (cluster EA/EB); Redis Sentinel prefers lower numbers (module rustdoc)
  - Tests: sentinel_lite **20/20** (INFO priority 150 beats discovery-order default 100; INFO 0 skipped; cooldown arms / expires / manual bypass); lib sentinel units green
  - *Post-ship review (2026-07-29):* no correctness regressions; serial INFO enrich accepted lite; successful failover still cools auto re-entry (intentional)
  - Residual closed by **FN**: CKQUORUM live probe + probe self-vs-`*`; hello SUBSCRIBE stays accepted residual

- [x] **`[P3]`** **Batch FN — Sentinel lite SM residuals** (`db222f4`)
  - CKQUORUM + elect majority: `count_reachable_sentinels` (self + peers answering `PING`); dead peers do not inflate usable/N
  - Probe `runid=*` with no prior vote returns leader `"*"` / epoch 0 (Redis-honest); sticky vote still returned on probe; sole-sentinel auto path via `is_failover_leader` / live≤1 elect
  - Hello SUBSCRIBE fan-in **not** implemented (accepted residual; tick PUBLISH + peer HELLO remains primary discovery)
  - Tests: unit probe/`leader_votes_needed_for`; e2e CKQUORUM dead→NOQUORUM / live→OK; elect ignores dead peers; probe sticky vote
  - Residual: election-timeout SM → **FT**; optional parallel INFO enrich; ephemeral ports → **FR**

- [x] **`[P3]`** **Batch FO — NODE 2PC durable prepare** (`1ad6e6f`)
  - `nodes.conf` header `# prepare <slot> <target> <epoch> <unix_ms>`; SAVECONFIG / autosave write non-expired votes; load restores; expired/malformed skipped (fail-closed)
  - Prepare stamps wall-clock **unix-ms** (not process `Instant`) so TTL survives restart
  - Autosave on PREPARE / ABORTPREPARE / COMMITPREPARE when dir configured (dual-end local prepare too)
  - `SETSLOT COMMITPREPARE <node-id>` atomic check+NODE under one write lock; dual-end dest RESP + source local use it (NODE fallback for older peers; operator NODE still bypasses)
  - Closed FL residual (prepare not in conf) + FH residual (durable + atomic commit)
  - Tests: state prepare round-trip / expired not restored / COMMITPREPARE; migrate unit + **42/42** e2e; `cluster_test` Config compile fix
  - *Post-ship review (2026-07-29):* dual-end dest-first COMMITPREPARE + source local atomic path green; recheck inject still fail-closed; remote dest still two RPCs (CHECKPREPARE then COMMITPREPARE) — accepted lite; no correctness bug
  - Residual: binary cluster bus 2PC; dest prepare breadth; operator NODE bypass (intentional); per-slot epochs not fully persisted in nodes.conf (file-epoch stamp + prepare fence)
- [x] **`[P3]`** **Batch FP — Expire slot header** (`0fa2ed6`)
  - Folded side `typed_expires` into [`KeySlot::expires_at`] on the unified `key_values` map
  - Map type: `ShardedKeyMap<KeySlot>`; `KeyspacePayload` carries slots only (no sibling expires map)
  - EXPIRE/TTL/PERSIST/active expire/volatile sample/RENAME use slot expire; RDB/AOF still export/import via `export_typed_expires_unix_ms` / `set_typed_expire_unix_ms` (wire format unchanged)
  - LOADING / take-install / multi-DB replace: typed TTL rides with each key through epoch install
  - Tests: `typed_ttl_test` 12/12; keyspace take/install expire unit; active_expire / keyspace / multi-DB load green
  - Residual closed by **FQ**: string TTL on slot

- [x] **`[P3]`** **Batch FQ — Unify string TTL onto KeySlot** (`51bfc7e`)
  - String key-level expire stored on [`KeySlot::expires_at`] (same header as typed keys)
  - Unified EXPIRE/TTL/PERSIST/`purge_if_expired` for all types via slot
  - SET EX/PX/EXAT/PXAT/KEEPTTL + INCR/APPEND/… lift `Entry.expires_at` onto the slot (`KeySlot::string` / `mutate_string`)
  - Active expire (string sample + typed sample) and volatile eviction read slot expire
  - RENAME moves whole string slot (value + expire); take/install preserves string TTL
  - Residual closed by **FU**: drop string RMW dual-write of `Entry.expires_at`; search-doc eviction special remains

- [x] **`[P3]`** **Batch FR — Compiler nits + Sentinel ephemeral ports** (`f574294`)
  - Deleted unused `config_kv_reply` (CONFIG GET uses `config_kvs_reply`); removed dead private SkipList/`SkipNode` APIs (`len` / `is_empty` / `level`)
  - Fixed `unused_mut` in `sentinel_lite_test`; known FR nits clean under `cargo build --lib` / sentinel test build
  - Sentinel suite: `free_port` / `free_ports` (bind `127.0.0.1:0`, hold then release) replaces hard-coded `169xx` ports — parallel-safe under `cargo test`
  - Tests: `sentinel_lite_test` **23/23**; lib sorted_set **16/16**; lib sentinel **16/16**
  - Out of scope → closed by **FS**: unused imports (`bitmap` Bytes, replication Cache), `entered_replica_feed` assignment, search `weight` param
  - Residual: election-timeout SM → **FT**; optional parallel INFO enrich; other Later items unchanged

- [x] **`[P3]`** **Batch FS — Clear residual lib warnings** (`b7219ea`)
  - Removed unused `bytes::Bytes` import in `commands/bitmap.rs`
  - Moved `Cache` import into `replication` test module only (no longer top-level unused)
  - Dropped dead `entered_replica_feed` flag in `network.rs` (replica feed always `break 'conn`; pipeline flush simplified)
  - Renamed inverted-index `weight` → `_weight` (API kept; TF-IDF scoring still future)
  - Bonus: unused `search_index` type imports in `query_engine` tests
  - Verify: `cargo build --lib` **0 warnings**; lib **360/360** (+1 ignored); `bitmap_hll` / `search` / `replication` integration green
  - Residual closed by **FT**: election-timeout SM; remaining closed by **FU**: `Entry.expires_at` RMW mirror

- [x] **`[P3]`** **Batch FT — Sentinel election-timeout SM** (`4f114d9`)
  - Per-master `election_started_at` + `ELECTION_TIMEOUT` (5s; test override `test_set_election_timeout_ms`)
  - `vote_leader` stamps campaign start on higher-epoch vote
  - `try_elect_leader`: reuse self-campaign epoch while timer live (no per-tick `next_election_epoch` thrash)
  - After timeout: open higher epoch for self-campaign; clear stuck vote-for-other and re-campaign
  - `note_ok` / `switch_master` clear campaign timer with election vote
  - Closed FE/FN residual: pre-attempt campaign epoch thrash while `o_down`
  - Not full Redis SM (no lex-min runid pre-filter / random election desync delay)
  - Tests: unit stamp/expire/clear; reuse-then-bump sole campaign; re-campaign after stuck other vote; lib sentinel **19/19**
  - Residual: optional parallel INFO enrich / live PING; hello SUBSCRIBE; other Later items unchanged

- [x] **`[P3]`** **Batch FU — Expire dual-write cleanup** (`06ff29f`)
  - `KeySlot.expires_at` is the **only** key-level TTL SoT for strings in `key_values`
  - `mutate_string` passes slot expire in and takes intended new slot expire out; clears `Entry.expires_at` on write-back
  - `KeySlot::expires()` is slot-only; residual Entry expire healed on first mutate / `KeySlot::string`
  - RMW call sites (SET/INCR/APPEND/SETRANGE/SETBIT/BITFIELD/PFADD/…) use slot KEEPTTL path
  - EXPIRE/PERSIST/RENAME no longer dual-write Entry; load injects expire only on returned clones (read projection)
  - Tests: `string_rmw_clears_entry_expire_keeps_slot` (rewritten as `string_rmw_slot_only_ttl` in **GA**)
  - Residual closed by **GA** (field removal) + **FZ** (search-doc eviction)

#### Later / backlog (no letter batch)

Active product work is the **post-GB GC+** letter queue above. Remaining / accepted only:

- [ ] **`[P3]`** **Later / backlog (accepted or unscheduled)**
  - cluster reshard weight UI
  - single-DB Arc-swap mid-fill (accepted)
  - Optional parallelize `enrich_replica_priorities` / `count_reachable_sentinels` → **GQ** if scheduled
  - Cluster binary bus 2PC; dest prepare breadth; remote CHECK→COMMITPREPARE two-RPC window (accepted lite) → **GP / GO** if scheduled
  - `__sentinel__:hello` SUBSCRIBE fan-in; Sentinel priority honesty (accepted)
  - admin_http auth/TLS → promoted to **GM** when scheduled

### Code review backlog

**Batches through FU + FV + FW + FX + FY + FZ + GA + GB shipped.** Active queue is **post-GB GC+** (perf / compat / security). Standing tests-for-phase P0.

| Pri | Item | Status |
|-----|------|--------|
| P0 | `FT.*` mutators in `is_write_command` (AOF / repl / READONLY) | done (BT) |
| P0 | CB post-ship: quiesce target sweep during `replace_keyspace_from` | done (CC) |
| P0 | CB second-pass: flush=true peak memory ~2× (independent trackers) | done (CC) |
| P0 | CB second-pass: flush=false seed mutates live target (`load` touch/expire) | done (CC) |
| P0 | CH post-ship: RDB map_ft_mutator_error after prefix breaks OOM match | done (CI) |
| P1 | Alias target resolve + real-name storage | done (BT) |
| P1 | Atomic create/alias namespace critical section | done (BT) |
| P1 | AOF rewrite emits `FT.CREATE` + aliases (BT review) | done (BU) |
| P1 | AOF load surfaces FT.CREATE / alias failures (BU review) | done (BV) |
| P1 | RDB load `flush=true` must wipe FT schema (BY×BX clash) | done (BZ) |
| P1 | CB: full keyspace swap under quiesce (typed maps, expires, watch) | done (CB) |
| P1 | CB: `Cache.memory_usage` + tracker paired install (no double-account) | done (CB) |
| P1 | CB post-ship: multi-DB replace atomic / server-wide quiesce | done (DR lock-step epoch; panic/raw-Arc residual) |
| P1 | CF post-ship: FT merge compare schema on name clash (not name-only skip) | done (CG) |
| P1 | CF post-ship: FT alias merge compare targets on clash | done (CG) |
| P1 | CB post-ship: bump `watch_gens` on keyspace replace | done (CC+CD) |
| P1 | CC post-ship: atomic WATCH bump with keyspace install | done (CD) |
| P1 | CC post-ship: pause waits for in-flight expire cycle | done (CD) |
| P1 | CB second-pass: scratch independent Stats (no INFO pollution) | done (CC) |
| P2 | FT RDB section | done (BY) |
| P2 | RDB load wipe-on-FT-failure (mirror AOF BW) | done (BZ) |
| P2 | RDB `flush=false` FT merge / name-clash semantics | done (CF name-skip; CG schema/target compare) |
| P2 | ACL `@search` | done (CE) |
| P2 | Shared FT.CREATE parser (cmd + AOF load) | done (CA) |
| P2 | HNSW `ef_construction` AOF round-trip | done (CE) |
| P2 | AOF load all-or-nothing on FT failure (BV review) | done (BW) |
| P2 | FLUSHDB vs FT schema (BW: flush clears indices) | done (BX) |
| P2 | Scratch-load swap if AOF/RDB load targets non-empty DB | done (CB) |
| P2 | CB: `drain_all`/`replace_all` failure-atomic | done (CL drain-then-fill; CM fill_all install) |
| P2 | CL post-ship: strengthen min-replicas FT SEARCH/DROP/ALIAS tests | done (CM) |
| P2 | CL post-ship: fill-only after external drain (no double drain) | done (CM) |
| P2 | CM post-ship: ALIASADD under NOREPLICAS test | done (CN) |
| P2 | CM post-ship: fill_all emptiness debug_assert | done (CN) |
| P2 | CB: `install_keyspace_counts` closed over KEYSPACE_CATEGORIES | done (CB) |
| P2 | CB: optional alias-target validate on search `install` | done (CF debug_assert) |
| P2 | CB post-ship: expand tests (memory, multi-DB, TTL, pubsub, seed, peak) | done (CP) |
| P2 | HNSW graph search (ef_search; self-exclude; edge-walk tests) | done (CQ) |
| P2 | HNSW multi-layer insert (level assign + upper SEARCH-LAYER + query descent) | done (FF) |
| P2 | HNSW durable graph in RDB (KORDB v6 levels/edges/entry; AOF rebuild residual) | done (FV) |
| P2 | HNSW ANN query path (query engine prefers dual-written graph) | done (FW) |
| P3 | HNSW AOF durable graph (`FT._LOADGRAPH` rewrite/load) | done (FX) |
| P2 | CQ post-ship: HNSW remove unlinks graph + re-add clears neighbors | done (CS local hygiene) |
| P2 | CQ post-ship: insert M-prune preserves reachability from entry | done (CS insert-time heuristic) |
| P2 | CQ post-ship: update-in-place rewire or document | done (CS; CT 2-chain) |
| P2 | CS post-ship: hard-delete bridge repair / soft-delete | done (CT 2-chain heuristic) |
| P2 | CS post-ship: force-keep not global reachability (docs + hub-churn tests) | done (CT docs honesty; hub-churn smoke) |
| P2 | CT post-ship: undirected former-neighbor snapshot (incoming too) | done (CU) |
| P2 | CT post-ship: multi-way / degree-saturated bridge reconnect | done (CU clique + CW path) |
| P2 | CU post-ship: NN-path bridge reconnect unit test (n-1 > max_m) | done (CW; CY decoys) |
| P2 | CU post-ship: prune must_keep never drops required edges | done (CW; multi-layer residual → GR) |
| P3 | HNSW max_m=1 upper-layer path spanning (reconnect prune floor 2) | done (GR) |
| P3 | CW post-ship: path-branch test degree-saturating decoys | done (CY; adjacency-assert residual) |
| P3 | CU post-ship: fuse reverse-scan + unlink / reverse index | done (CY one O(E) pass; no reverse index) |
| P2 | HNSW recall@k / throughput numbers vs FLAT | done (CV unit gate + N=300 micro) |
| P3 | CV post-ship: tighter recall / larger-N throughput bench | done (DK) |
| P2 | Optional structured JSON logging | done (CX MVP) |
| P3 | CX post-ship: EnvFilter / JSON smoke / boot-only docs | done (CY; smoke/EnvFilter residual) |
| P2 | CZ victim strategies (Youngest/Oldest/FewestLocks) | done (CZ + DB Redlock wiring) |
| P2 | DA async API + spawn_monitor | done (DA + DB Redlock monitor/accessor) |
| P2 | CZ/DA post-ship: Redlock auto-resolve + strategy + backend unlock | done (DB + DD Lock Drop) |
| P2 | DA post-ship: release_client_locks TOCTOU + wait cleanup | done (DB) |
| P2 | DC cross-process snapshot merge MVP | done (DC + DD re-link/TTL) |
| P1 | DB post-ship: disarm victim Lock after auto-resolve | done (DD; release race residual) |
| P1 | DC post-ship: merge re-links local waits to imported holds | done (DD) |
| P2 | DC post-ship: reconcile edge holders + remaining TTL on import | done (DD; self-wait residual) |
| P1 | DD post-ship: atomic record_lock_released + holder-scoped edge prune | done (DE) |
| P1 | DD post-ship: merge holder rewrite must not create self-waits | done (DE) |
| P2 | DD post-ship: record_lock_acquired rewrites holders + re-links waits | done (DE) |
| P2 | DF Web UI monitoring MVP | done (DF) |
| P2 | DF post-ship: atomic UI snapshot + cleanup-on-poll docs | done (DG) |
| P2 | DF post-ship: deadlock CLI params (enable/max-wait/auto-resolve/strategy) | done (DG) |
| P3 | DF post-ship: from_config UI/detection wiring tests | done (DG) |
| P3 | DF post-ship: JS poll only updates badge | done (DH; full table/stats/cycle repaint) |
| P3 | DH post-ship: dual meta+JSON refresh when JS enabled | done (DI; meta in `<noscript>`) |
| P3 | DH post-ship nit: coerce/escape numeric JS table cells | done (DI; `num()` / Number) |
| P2 | DM/DN post-ship: failed_keys under-reports partial key moves | done (DO) |
| P3 | DN post-ship: source NODE before dest creates MOVED window | done docs (DO; order residual) |
| P3 | DM post-ship: range RESHARD continues after partial_*_node | done (DO abort-on-partial) |
| P3 | DN post-ship nit: RESHARD FINISH no key-placement check | done (DO soft warning) |
| P1 | DP: Redis MIGRATE key-level (shared recreate + COPY/REPLACE/KEYS/AUTH) | done (DP) |
| P2 | DP residual: multi-key partial failure reply coarse (IOERR only) | done (DQ) |
| P3 | DP residual: non-string TTL not transferred on recreate | done (DQ) |
| P2 | DR post-ship: panic mid-install partial multi-DB commit | done (DS; mid-fill single-DB residual) |
| P2 | DR post-ship: raw Arc multi-DB walk still torn mid-install | done (DS AOF epoch + audit; raw residual documented) |
| P3 | DR/DS residual: single-DB mid-payload map tear | done (DT document/accept; LOADING barrier) |
| P3 | DR post-ship nit: after_install_db test hook in prod type | done (DS docs; keep for tests/) |
| P3 | DQ post-ship nit: migrate TTL remaining-ms not absolute | done (DT PXAT/PEXPIREAT) |
| P3 | DP residual: no DUMP/RESTORE wire compatibility | **done FY** for DUMP/RESTORE cmds; MIGRATE still recreate-only |
| P3 | DH post-ship nit: repaint test is string-contains only | done (DT accept; string-contract only) |
| P2 | DU: slot ownership epoch + gossip after NODE | done (DU) |
| P2 | DV: dest-first dual-end NODE + epoch fence | done (DV) |
| P2 | DW: multi-master pfail/fail quorum + FAILREPORTS | done (DW) |
| P2 | DX: RESHARD PLAN/AUTO greedy planner + remote execute | done (DX) |
| P2 | DY: multi-replica election (max id) + ROLEMAP | done (DY) |
| P2 | DZ: loser reconfig to election winner after fail | done (DZ) |
| P2 | EA: failover election by repl offset (+ id tie-break) | done (EA) |
| P2 | EB: replica-priority election (0=never; CLI flag) | done (EB) |
| P2 | EC: CLUSTER FAILOVER [FORCE|TAKEOVER] manual promote | done (EC) |
| P2 | ED: COUNTKEYSINSLOT / GETKEYSINSLOT / REPLICAS / BUMPEPOCH | done (ED) |
| P2 | EE: ADDSLOTS / DELSLOTS / FLUSHSLOTS slot bootstrap | done (EE) |
| P2 | EF: ADDSLOTSRANGE / DELSLOTSRANGE | done (EF) |
| P2 | EG: CLUSTER FORGET + RESET SOFT/HARD | done (EG) |
| P2 | EH: partial_source MIGRATING restore + COUNT-FAILURE-REPORTS | done (EH) |
| P2 | EI: CLUSTER SHARDS (Redis-7) + LINKS empty | done (EI) |
| P2 | EJ: post-commit dual NODE verify + MYSHARDID | done (EJ) |
| P2 | EK: CLUSTER SET-CONFIG-EPOCH | done (EK) |
| P2 | EL: CONFIG GET/SET cluster-replica-priority + node-timeout | done (EL) |
| P2 | EM: CLUSTER SAVECONFIG → dir/nodes.conf | done (EM) |
| P2 | EN: load nodes.conf on cluster boot | done (EN) |
| P2 | EO: autosave nodes.conf on topology mutation / failover claim | done (EO) |
| P2 | EP: dual-end NODE dest compensating rollback (`rolled_back`) | done (EP) |
| P2 | EQ: cluster-require-full-coverage + honest cluster_state | done (EQ) |
| P2 | ER: READONLY serves cluster replica reads of master slots | done (ER) |
| P2 | ES: cluster-allow-reads-when-down (reads when cluster fail) | done (ES) |
| P2 | ET: CLUSTER SLOTS includes replica endpoints | done (ET) |
| P2 | EU: cluster-announce-ip / cluster-announce-port | done (EU) |
| P2 | EV: CLUSTER SLOT-STATS SLOTSRANGE key-count | done (EV) |
| P2 | EW: Sentinel-lite MONITOR/s_down/FAILOVER | done (EW) |
| P2 | EX: multi-Sentinel MEET + ODOWN vote quorum | done (EX) |
| P2 | EY: dual-end NODE preflight / failed_preflight | done (EY) |
| P2 | EZ: Sentinel FLUSHCONFIG + sentinel.conf load/autosave | done (EZ) |
| P2 | FA: Sentinel hello bus lite (HELLO + PUBLISH) | done (FA) |
| P1 | EW post-ship: promote_replica PING-only still Ok → switch_master | **done** (Batch FC) |
| P2 | EX/FA post-ship: failover leader election (cross-process); in-process gate done FC | **done** (Batch FE) |
| P3 | FE post-ship: election epoch thrash / probe self / majority table-size | **done FN** (probe/CKQUORUM); **done FT** (election-timeout SM) |
| P3 | EZ/FA post-ship: hello add_peer autosave thrash; CKQUORUM live count | **done** (FE autosave; FN live CKQUORUM) |
| P3 | EN/EO/EU post-ship: nodes.conf omits require-full/announce flags | **done** (Batch FL) |
| P1 | dual-end NODE RESP 2PC prepare/commit (slice 1) | **done** (Batch FB) |
| P3 | FB post-ship: prepare not re-checked at commit; dest prepare broad; mem-only | **done FH** epoch/TTL/recheck; **done FO** durable + COMMITPREPARE; bus residual later |
| P3 | FC post-ship: first-replica-wins promote order | **done** (Batch FK) |
| P3 | FK residual: INFO slave_priority + failover cooldown | **done** (Batch FM) |
| P3 | FM post-ship: serial INFO enrich; cooldown arms on success; begin_failover no-arm | **accepted lite** (no correctness bug) |
| P3 | FN: CKQUORUM live PING + probe `*` honesty | **done** (Batch FN; hello SUBSCRIBE residual accepted) |
| P3 | FO: durable NODE prepare + atomic COMMITPREPARE | **done** (Batch FO) |
| P3 | FO post-ship: remote CHECK→COMMITPREPARE two-RPC; dest prepare breadth; bus | **accepted lite** (local commit atomic; operator NODE intentional) |
| P3 | FN post-ship: serial peer PING; pre-attempt election epoch thrash | serial PING **accepted lite**; epoch thrash **done FT** |
| P3 | FT: Sentinel election-timeout SM (campaign reuse + re-campaign) | **done** (Batch FT) |
| P2 | dual-end NODE 2PC slice 2 (prepare-epoch / TTL / commit re-check) | **done** (Batch FH) |
| P2 | FI pipeline SET perf | **done** (Batch FI; ~+25% P=16) |
| P2/P3 | FI-2 AOF-off multi-DB SELECT ordering | **done** (Batch FI-2) |
| P3 | GB repl backlog write serialization | **done** partial (dropped fullsync Mutex on hot path; backlog ordered publish residual) |
| P3 | FL nodes.conf live flags | **done** (Batch FL) |
| P3 | FG-4 strings unified map | **done** (Batch FG-4) |
| P2 | FY: DUMP/RESTORE Redis wire (core types; KDF1 dual-detect) | **done** (Batch FY) |
| P3 | DP residual: no DUMP/RESTORE wire compatibility | **done FY** (cmds); MIGRATE recreate residual accepted |
| P3 | DF post-ship: HTTP MVP gaps shared with metrics | done (DJ; shared admin_http) |
| P3 | DK post-ship: thin r@10 headroom / cross-arch flake risk | done (DL; r@10 0.95→0.93) |
| P3 | DK post-ship: no post-delete/update churn in recall suite | done (DL; remove+update micro) |
| P3 | DJ post-ship nit: read_request_line async/oversized-line tests | done (DL) |
| P3 | DJ post-ship nit: non-GET unknown path is 404 not 405 | done (DL; document/accept) |
| P2 | CP post-ship: LOADING allowlist PSYNC/SYNC mid-install visibility | done (CR) |
| P2 | CC post-ship: typed export skip expired keys (no revive without TTL) | done (CD) |
| P2 | CC nit: unify start_background_sweep create paths | done (CD) |
| P2 | CB second-pass: empty_keyspace_like hardcodes loadfactor 0.75 | done (CF) |
| P2 | `get_index` atomic resolve; min-replicas FT test | done (CH+CL) |
| P2 | VECTOR/NUMERIC rewrite tests | done (BX) |
| P2 | HNSW RDB round-trip test | done (CE) |
| P2 | RDB FT OOM → OutOfMemory map | done (CH+CI) |
| P2 | BZ mid-fail test: tighten InvalidArgument assert | done (CF) |
| P2 | raw load_into non-transactional (wrappers are safe) | done (CF docs) |
| P2 | `has_search_state` double-list lock nit | done (CH) |
| P2 | `clear_documents` + MemoryTracker coupling (flush-only) | done (CH) |
| P2 | OOM→OutOfMemory map; DROPINDEX/ALIASDEL missing tests | done (BW) |
| P2 | `map_ft_mutator_error` OOM match hygiene | done (BX) |
| P2 | Lua SELECT DB side-effect test | done (BT) |

---

## Quick reference: all P0 items

Highest urgency checklist (phase order preserved):

**A**

- [x] EXAT / PXAT absolute timestamps
- [x] Atomic INCR / DECR
- [x] Atomic SET NX / XX / CAS
- [x] Memory accounting (single source + all fix-ups)
- [x] True random eviction sampling
- [x] Enforce `maxconns`
- [x] Honor `--threads`
- [x] Unified keyspace + type safety + cross-type ops + maxmemory for all types
  - *Done (Batch X)*: cross-type eviction samples string/hash/list/set/zset/geo/stream victims
  - *Done (Batch AD)*: search documents are allkeys eviction victims
  - *Done (Batch AE)*: typed-key TTL (`EXPIRE`/`PEXPIRE`/`TTL`/`PTTL`); lazy + active expire; volatile policies sample typed keys with TTL; RDB v4 + AOF rewrite `PEXPIREAT` (side map → **FP** slot header)
  - *Done (Batch AF)*: full expire command family (`PERSIST`, `EXPIREAT`/`PEXPIREAT`, `EXPIRETIME`/`PEXPIRETIME`)
  - *Done (Batch AG)*: `MOVE` / `COPY` / `RANDOMKEY` / `TOUCH`
  - *Follow-ups*: true single-map keyspace — **done FG-4**; typed expire slot header — **done FP**
- [x] Phase A concurrency / memory / EXAT / network tests (incl. AUTH)

**B**

- [x] RDB export
- [x] AOF + rewrite
- [x] Load from file on startup
- [x] Async replication
- [x] Timed SAVE policies (`--save` / CONFIG save)
  - *Follow-ups*: long-lived master SUBSCRIBE hello fan-in (election-timeout SM done FT; promote gate FC done)

**C**

- [x] Hashes, Lists, Sets
- [x] Transactions (`MULTI` / `EXEC` / `WATCH`)
- [x] Common string ops (`APPEND` / `STRLEN` / `SETEX` / `GETSET` / `UNLINK` / `RENAME`)
- [x] `CLIENT` / `COMMAND` / `HELLO` (RESP2)
- [x] Eviction policies (`maxmemory-policy`)
  - *Follow-ups*: Streams, bitmaps/HLL, RESP3 (done elsewhere); LFU decay done in Batch AB

**Historical note:** The original “finish this P0 list first” rule applied while A–C were incomplete. As of **Batch FA**, that list is green; **FB–FQ** letter work is committed complete; **FR+FS** hygiene; **FT–GB** closed the post-FU product letter stream. Resume from **Next work queue (post-GB)** — productization / perf / compat (**GC+**), not baseline P0s.
