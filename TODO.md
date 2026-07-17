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
  - *Done pragmatically*: separate maps + type registry / cross-type ops (not a single typed enum map yet)
- [x] **`[P0]`** **Type safety**: Redis-style type errors when a key exists with a different type
- [x] **`[P0]`** **Cross-type ops**: `DEL`, `EXISTS`, `KEYS`/`SCAN`, `DBSIZE`, `TTL`/`EXPIRE`, `TYPE` work for all types
  - *Done*: `SCAN` implemented (cursor-based, sorted key index); `KEYS`/`DBSIZE`/`DEL`/`EXISTS`/`TYPE`/`FLUSH` cover all types
  - *Batch AE*: `EXPIRE`/`PEXPIRE`/`TTL`/`PTTL` on hash/list/set/zset/geo/stream (side expire map); lazy + active expire; RENAME keeps TTL
  - *Batch AF*: `PERSIST`, `EXPIREAT`/`PEXPIREAT`, `EXPIRETIME`/`PEXPIRETIME`; zero/past absolute expire deletes key; wired for AOF/replication/Lua/COMMAND
  - *Batch BA*: `EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT` optional `NX|XX|GT|LT`
- [x] **`[P0]`** **Eviction / maxmemory**: account for zset, geo, search indexes, and pub/sub buffers—not only string KV
  - *Done*: zset/geo/hash/list/set/stream/search tracked in `MemoryTracker` and count toward maxmemory; eviction still samples string KV only

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
  - *Coordinated* `FAILOVER TO <host> <port> [TIMEOUT ms] [FORCE]` (master only, default timeout 5000ms): write pause, soft match against REPLCONF `listening-port`/`ip-address` when tracked, wait until target ack ≥ frozen `master_repl_offset` (unless **FORCE**), then TCP bare `FAILOVER`, best-effort sibling re-follow (`REPLICAOF` on feeds + client-port TCP), demote self via `set_replicaof`. Replicas honor in-stream `REPLICAOF` and reconnect on `primary_link_epoch` / primary addr change. Catch-up sources: live-link tracked ACK (`REPLCONF ACK` on feed + periodic feed GETACK probe), then client-port GETACK fallback. Replica offset uses exact wire bytes via `parse_with_consumed` and replies to master GETACK on the repl link. Catch-up timeout leaves master writable. Full Sentinel not implemented.
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
  - *Done (MVP)*: `EVAL` / `EVALSHA` / `SCRIPT LOAD|EXISTS|FLUSH|KILL` via mlua Lua 5.4 (vendored); shared `ScriptCache`; `redis.call` / `redis.pcall` whitelist for core string/hash/list/set/zset/bitmap/HLL ops; KEYS/ARGV; RESP↔Lua mapping (nil bulk→false, status→`{ok=…}`); ACL `@scripting`; cluster key extract from numkeys. Not yet: FUNCTIONS library, `redis.setresp`, full movablekeys COMMAND, nested scripts, script time limits.
  - *Batch BG*: `EVAL_RO` / `EVALSHA_RO` (reject write `redis.call`); `CLIENT GETREDIR` / `TRACKINGINFO` (tracking off)
  - *Batch BL*: `SCRIPT HELP`; `CONFIG HELP`
  - *Batch BO*: `FUNCTION` HELP/LIST/STATS (empty); `FCALL`/`FCALL_RO` not-found; `CONFIG REWRITE` (no conf file)
  - *Batch BP*: `ACL LOG` GET/LEN/RESET + denial recording; `SHUTDOWN` [NOSAVE|SAVE]; connection close after `QUIT`/`SHUTDOWN`/`CLIENT KILL ID … SKIPME no`; `acllog-max-len` CONFIG
  - *Batch BQ*: `DEBUG` HELP/SLEEP/OBJECT; INFO `# Clients`/`# CPU`/`# Persistence`; `redis.call` GEO* + XADD/XLEN/XRANGE/XDEL/XTRIM/XACK + TOUCH/SCAN/RANDOMKEY
  - *Batch BR*: `CONFIG GET` port/bind/dir/dbfilename/appendonly/appendfilename/unixsocket/cluster-enabled; `MEMORY MALLOC-STATS`; `FT.TAGVALS`; `redis.call` COPY/MOVE; schema coerce TAG/NUMERIC from HSET text

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
  - *Done (thin MVP)*: `CLUSTER MEET` over client RESP (MYID + `MEETPEER` handshake); periodic PING heartbeat; **single-observer** fail (not Redis quorum) → `fail` flag in `CLUSTER NODES`; on master fail, replica (`CLUSTER REPLICATE`) runs `promote_to_master` + claims slots. Gaps: no binary cluster bus, no multi-node quorum PFAIL/FAIL, no epoch election, no replica election among peers, no automatic reconfig of other nodes' views.
- [x] **`[P1]`** Resharding / slot migration (thin MVP)
  - *Done*: `keys_in_slot` / `string_keys_in_slot`; `CLUSTER MIGRATEKEYS <slot> <ip> <port>` moves **all key types** via RESP (ASKING + type-specific recreate + DEL: SET/HSET/RPUSH/SADD/ZADD/GEOADD/XADD+groups); SETSLOT MIGRATING/IMPORTING/NODE/STABLE operator flow; MIGRATING miss → ASK; final NODE → MOVED.
  - *Done (Batch Y)*: multi-type MIGRATEKEYS (string/hash/list/set/zset/geo/stream)
  - *Gaps*: no atomic dual-end NODE, no Redis `MIGRATE`/`CLUSTER SETSLOT` batch orchestration, no slot-stable epoch gossip of ownership.

### Protocol & clients

- [x] **`[P1]`** RESP3 support (`HELLO 3`, maps, bools, push)
  - *Done*: `RespValue::{Map,Bool,Null,Push}` serialize+parse; `HELLO 3` map + `protocol_version`; `HGETALL`/`CONFIG GET` as maps on proto 3; pub/sub confirmations + fan-out as **push** for RESP3 clients; `CLIENT INFO` `resp=`; `RESET` → proto 2.
- [x] **`[P1]`** Zero/low-alloc command dispatch (avoid per-command `String` uppercasing; static table / perfect hash)
  - *Done (MVP)*: stack `[u8;64]` ASCII uppercase via `ascii_uppercase_cmd` (heap only if name > 64 bytes); mixed-case dispatch covered by tests. Perfect-hash / enum dispatch still optional later.
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
- [ ] **`[P2]`** Optional structured (JSON) logging
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
- [ ] **`[P2]`** Deadlock detection advanced (from roadmap)
  - [ ] **`[P2]`** Cross-process detection
  - [ ] **`[P2]`** Async support
  - [ ] **`[P2]`** Custom victim selection strategies
  - [ ] **`[P2]`** Web UI monitoring

### Search & vectors

- [x] **`[P1]`** Document and test `FT.SEARCH` end-to-end over RESP (not only programmatic indexing)
  - *Done*: `tests/search_resp_test.rs` — FT.CREATE / HSET auto-index / FT.SEARCH / FT.DROPINDEX via `CommandHandler`; DEL/UNLINK remove from indices
- [x] **`[P1]`** Memory limits and eviction interaction for indexes
  - *Done (MVP + Batch AD)*: `MemoryCategory::Search`; `index_document` / `auto_index_key` allocate approx size; remove/drop deallocate; counts toward maxmemory. Under `allkeys-*`, sampled **search documents** are eviction victims (drop index entry + free Search bytes; underlying hash key kept). Account path may `evict_memory` before OOM. Search docs still not volatile victims (no search TTL).
- [ ] **`[P2]`** HNSW correctness/performance benchmarks vs FLAT

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
  - *Done*: methodology runbook; result tables TBD until measured
- [x] **`[P1]`** **Fuzz** RESP parser and command argument parsing
  - *Done*: in-tree smoke fuzz unit tests (random + structured); `fuzz/` crate with `resp_parse` + `command_dispatch` targets (`cargo +nightly fuzz run …` when cargo-fuzz installed).
- [x] **`[P1]`** **Concurrency / loom or stress** jobs for shard RMW paths
  - *Done*: `tests/concurrency_stress_test.rs` — concurrent INCR, INCR/DECR net-zero, SET NX single winner, multi-key multi-shard, mixed RMW+reads, hash field RMW under `parking_lot`
- [ ] **`[P2]`** Align version strings in docs/`INFO` examples with `Cargo.toml` (currently 0.6.0)
- [ ] **`[P2]`** Consistent locking and error handling guidelines in contributor docs
- [ ] **`[P2]`** Keep `docs/roadmap.md` in sync with this file (or make this the single source of truth)

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
  - *Done (Batch AE)*: typed-key TTL (`EXPIRE`/`PEXPIRE`/`TTL`/`PTTL`) via side `typed_expires` map; lazy + active expire; volatile policies sample typed keys with TTL; RDB v4 + AOF rewrite `PEXPIREAT`
  - *Done (Batch AF)*: full expire command family (`PERSIST`, `EXPIREAT`/`PEXPIREAT`, `EXPIRETIME`/`PEXPIRETIME`)
  - *Done (Batch AG)*: `MOVE` / `COPY` / `RANDOMKEY` / `TOUCH`
  - *Follow-ups*: true single-map keyspace
- [x] Phase A concurrency / memory / EXAT / network tests (incl. AUTH)

**B**

- [x] RDB export
- [x] AOF + rewrite
- [x] Load from file on startup
- [x] Async replication
- [x] Timed SAVE policies (`--save` / CONFIG save)
  - *Follow-ups*: Sentinel/failover

**C**

- [x] Hashes, Lists, Sets
- [x] Transactions (`MULTI` / `EXEC` / `WATCH`)
- [x] Common string ops (`APPEND` / `STRLEN` / `SETEX` / `GETSET` / `UNLINK` / `RENAME`)
- [x] `CLIENT` / `COMMAND` / `HELLO` (RESP2)
- [x] Eviction policies (`maxmemory-policy`)
  - *Follow-ups*: Streams, bitmaps/HLL, RESP3 (done elsewhere); LFU decay done in Batch AB

When picking work: finish this list before large **`[P1]`/`[P2]`** feature work in Phases D–E.
