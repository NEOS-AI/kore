# Kore

A low-latency, high-performance **Redis/Valkey-compatible** cache and data server written in Rust (**v0.7.0**).

Kore speaks the **RESP** protocol, so common clients work out of the box (`redis-cli`, redis-py, redis-rs, ioredis, …). Beyond a sharded in-memory keyspace it includes persistence, replication, cluster/Sentinel-lite, full-text and vector search, Lua scripting with Redis Functions, and Kore-specific locking (Redlock, fair queues, deadlock detection).

**Docs:** [CHANGELOG](CHANGELOG.md) · [ops runbook](docs/ops.md) · [TODO / roadmap](TODO.md) · [benchmarks](docs/benchmarks.md)

---

## Features

### Core engine
| Area | What’s implemented |
|------|--------------------|
| Keyspace | Sharded map (default **4096** shards); unified `KeyValue` / `KeySlot` for all types |
| Protocol | **RESP2 + RESP3** (`HELLO 2\|3`); maps, bools, push where applicable |
| Memory | `maxmemory` + Redis-style policies (`allkeys-lru/lfu`, `volatile-*`, `noeviction`); active expire sampling |
| Multi-DB | `SELECT` / `SWAPDB` / `MOVE` / `COPY` (`--databases`, default 16) |
| I/O | Pipelines with reply coalescing; optional **Unix socket** (`--unixsocket`) |

### Data types & commands
- **Strings, hashes, lists, sets, sorted sets, streams** (consumer groups, blocking reads, claim/autoclaim)
- **Bitmaps / HyperLogLog**, **geospatial**, transactions (`MULTI` / `EXEC` / `WATCH`)
- **Lua** — `EVAL` / `EVALSHA` / `EVAL_RO` / `EVALSHA_RO` / `SCRIPT *`
  - `redis.call` / `redis.pcall` whitelist for core ops
  - `redis.setresp(2\|3)`, `CONFIG GET|SET lua-time-limit` (default 5000 ms; `0` = unlimited)
  - Hard script timeout + real `SCRIPT KILL` / `FUNCTION KILL` (write tracking → `UNKILLABLE`)
- **Redis Functions** — `FUNCTION LOAD|LIST|DELETE|FLUSH|DUMP|RESTORE|STATS`; real `FCALL` / `FCALL_RO` via `redis.register_function` and `#!lua name=` shebang (dump format: Kore portable **KORF1**; not yet RDB/AOF-durable)
- **DUMP / RESTORE / MIGRATE** — Redis RDB wire for string/list/set/hash/zset; geo as ZSET_2 geohash; stream type-15+KST1; legacy KDF1 dual-detect kept

### Persistence & HA
- **RDB** — Kore `KORDB` multi-DB + search/HNSW graph sections (`SAVE` / `BGSAVE` / timed `--save`)
- **AOF** — append log + `BGREWRITEAOF` (incl. FT schema/aliases); load prefers AOF when `--appendonly`
- **Replication** — `REPLICAOF`, `SYNC` / `PSYNC`, backlog, `WAIT`, min-replicas write gate
- **Failover** — coordinated `FAILOVER TO`; promote ranking
- **Sentinel-lite** — `MONITOR`, ODOWN quorum, hello bus lite, conf persistence, `CKQUORUM`
- **Cluster** — hash slots, `MOVED`/`ASK`, gossip, reshard / `MIGRATE`, NODE prepare/commit **2PC over RESP** (not the binary Redis cluster bus)

### Search
- **FT.*** — `FT.CREATE` / `DROPINDEX` / `SEARCH` / `INFO` / `_LIST` / `TAGVALS` / aliases
- Field types: **TEXT** (field-weighted **TF-IDF** ranking), **NUMERIC**, **TAG**, **VECTOR**
- Vector algorithms: **FLAT** (exact) and **HNSW** (ANN) with Cosine / L2 / IP
- Adaptive HNSW search `ef` for large-k queries; graph durable in RDB/AOF
- `FT.SEARCH … WITHSCORES` / `NOCONTENT`; ACL category `@search`

### Security & ops
- **ACL** users/categories + `ACL LOG`; boot `--aclfile`
- **TLS** — server TLS (`--tls` / cert / key); dual plain+TLS (`--tls-port`); **mTLS** (`--tls-auth-clients` + `--tls-ca`); replica→primary TLS (`--tls-replication`)
- **Admin HTTP** — Prometheus metrics (`--metrics-port`) + deadlock UI (`--deadlock-ui-port`)
  - Optional **Bearer / Basic** auth, **admin TLS**, bind host (`--admin-bind`; non-loopback requires auth)
- **Slowlog**, graceful shutdown, `HEALTH` / `HEALTH FULL`, structured logs (`--log-format json`)
- Client smoke: `scripts/client_smoke.sh` (redis-cli + redis-py in CI)

### Kore differentiators
- **Redlock** multi-instance locks, **fair FIFO** queueing, **deadlock detection** + optional web UI
- Search + vector ANN on the same RESP server as the cache
- Production runbook: [docs/ops.md](docs/ops.md)

### Planning
| Doc | Role |
|-----|------|
| [CHANGELOG.md](CHANGELOG.md) | Operator-facing releases (0.7.0 productization cut) |
| [TODO.md](TODO.md) | Letter-batch backlog / next queue |
| [docs/roadmap.md](docs/roadmap.md) | High-level plan |
| [docs/benchmarks.md](docs/benchmarks.md) | redis-benchmark vs Valkey methodology |
| [docs/locking.md](docs/locking.md) | Contributor lock-order rules |

---

## Architecture

- **Sharded hashmap** — default 4096 shards to cut lock contention
- **Unified keyspace** — strings and typed containers share one map; slot-level TTL
- **Approximated LRU/LFU** — Redis-style candidate sampling
- **Background expire** — active sampling + optional full `SWEEP`
- **Atomic stats** — hits, misses, evictions via lock-free counters
- **Standalone hot path** — pure masters skip replication stream encode/backlog until a replica is fed (Batch GC)

---

## Building

```bash
cargo build --release
```

CI (GitHub Actions) runs `cargo build --all-targets` and `cargo test --all-targets -- --test-threads=1` on push/PR to `main`, plus optional client-smoke and TLS coverage.

---

## Benchmarks

See [docs/benchmarks.md](docs/benchmarks.md). Methodology: host-local `redis-benchmark` with persistence off, vs Valkey when available.

Indicative (M3 Pro, Batch GC/GF): standalone SET pipeline P=16 ~**730–740k** ops/s; Valkey on the same host ~**1.6M**. Non-pipeline paths track Valkey more closely. Absolute numbers are host-dependent.

---

## Running

```bash
./target/release/kore
```

```bash
./target/release/kore \
  --host 127.0.0.1 \
  --port 6379 \
  --shards 4096 \
  --maxmemory 1073741824 \
  --threads 4 \
  -v 2
```

With persistence and auth:

```bash
./target/release/kore \
  --dir ./data \
  --appendonly true \
  --auth mypassword \
  --maxmemory-policy allkeys-lru
```

TLS + dual port (plain on 6379, TLS on 6380):

```bash
./target/release/kore \
  --tls --tls-cert cert.pem --tls-key key.pem \
  --tls-port 6380
```

Metrics + deadlock UI with auth:

```bash
./target/release/kore \
  --enable-redlock \
  --metrics-port 9121 \
  --deadlock-ui-port 9122 \
  --admin-http-token s3cret

curl -s -H 'Authorization: Bearer s3cret' http://127.0.0.1:9121/metrics
```

Full flag list: `./target/release/kore --help` · day-2 ops: [docs/ops.md](docs/ops.md)

---

## Command-line options (summary)

### Server
| Flag | Default | Notes |
|------|---------|--------|
| `--host` | `127.0.0.1` | Bind address |
| `-p, --port` | `6379` | Client port |
| `--threads` | CPU count | Worker threads (`0` = auto) |
| `--shards` | `4096` | Hashmap shards (power of 2) |
| `--maxconns` | `1024` | Connection cap |
| `--unixsocket` | off | Extra Unix domain listener |
| `--databases` | `16` | Logical DBs for `SELECT` |
| `-v, --verbosity` | `1` (WARN) | 0=ERROR … 3=DEBUG |
| `--log-format` | `text` | `text` or `json` (boot-only) |

### Memory
| Flag | Default | Notes |
|------|---------|--------|
| `--maxmemory` | ~80% RAM | `0` = auto |
| `--maxmemory-policy` | `allkeys-lru` | Redis-compatible policy names |
| `--maxentrysize` | 500 MiB | Per-value ceiling |
| `--evict` | `true` | `false` forces noeviction behavior |
| `--autosweep` | `true` | Background expire sampling |

### Persistence & replication
| Flag | Notes |
|------|--------|
| `--dir` / `--dbfilename` | RDB path |
| `--appendonly` / `--appendfilename` | AOF |
| `--save` | Timed RDB rules (`900,1 300,10 …`) |
| `--replicaof host:port` | Start as replica |

### Cluster / Sentinel
| Flag | Notes |
|------|--------|
| `--cluster-enabled` | Hash-slot cluster mode |
| `--cluster-replica-priority` / `--cluster-require-full-coverage` / `--cluster-allow-reads-when-down` / `--cluster-announce-ip` / `--cluster-announce-port` | Topology & client announce |

### Security
| Flag | Notes |
|------|--------|
| `--auth` | Default-user password |
| `--aclfile` | ACL rules file for LOAD/SAVE |
| `--tls` / `--tls-cert` / `--tls-key` | Server TLS |
| `--tls-port` | Dual listener: plain on `--port`, TLS here (`0` = TLS-only on `--port`) |
| `--tls-ca` / `--tls-auth-clients` | mTLS |
| `--tls-replication` | Replica→primary TLS |

### Admin HTTP (metrics + deadlock UI)
| Flag | Notes |
|------|--------|
| `--metrics-port` | Prometheus text (`0` = off) |
| `--deadlock-ui-port` | HTML `/` + JSON `/api/deadlock` (`0` = off) |
| `--admin-bind` | Bind host (default `127.0.0.1`; non-loopback **requires** auth) |
| `--admin-http-token` | Bearer token |
| `--admin-http-user` / `--admin-http-password` | Basic auth (must be paired) |
| `--admin-tls` / `--admin-tls-cert` / `--admin-tls-key` | Admin TLS (cert/key fall back to `--tls-cert`/`--tls-key`) |

### Redlock / deadlock
| Flag | Notes |
|------|--------|
| `--enable-redlock` | Multi-instance Redlock |
| `--redlock-instances` | Comma-separated backends |
| `--redlock-retry-count` / `--redlock-retry-delay-ms` | Retry policy |
| `--enable-fair-queue` / `--fair-queue-max-size` / `--fair-queue-cleanup-ms` | FIFO waiters |
| `--enable-deadlock-detection` / `--deadlock-max-wait-ms` / `--deadlock-auto-resolve` / `--deadlock-victim-strategy` | Detector |

See also [docs/redlock.md](docs/redlock.md) and [docs/deadlock_detection.md](docs/deadlock_detection.md).

---

## Command surface

Kore implements a large Redis-compatible subset. Use `COMMAND` / `COMMAND LIST` / `COMMAND INFO` on a running instance for the live catalog. High-level families:

| Family | Examples |
|--------|----------|
| Connection | `PING`, `ECHO`, `AUTH`, `HELLO`, `QUIT`, `SELECT`, `SWAPDB` |
| Strings | `GET`/`SET` (+ options), `MGET`/`MSET`, `INCR*`, `APPEND`, `GETRANGE`, `GETDEL`, `GETEX`, `SETNX`, … |
| Keys | `DEL`, `EXISTS`, `TYPE`, `TTL`/`PTTL`, `EXPIRE*`, `RENAME`, `COPY`, `MOVE`, `SCAN`, `DUMP`/`RESTORE`, `MIGRATE`, … |
| Hashes / lists / sets / zsets | Full core + algebra (`ZUNION`/`ZINTER`/`ZDIFF`, `LMOVE`, `LMPOP`/`ZMPOP`, …) |
| Streams | `XADD`, `XREAD`/`XREADGROUP`, `XGROUP`, `XACK`, `XPENDING`, `XCLAIM`, `XAUTOCLAIM`, `XINFO`, … |
| Bitmap / HLL / geo | `SETBIT`/`BITOP`/`BITFIELD`, `PFADD`/`PFCOUNT`, `GEOADD`/`GEOSEARCH`, … |
| Pub/Sub | `PUBLISH`, `SUBSCRIBE`, `PSUBSCRIBE`, `PUBSUB *`, shard pub/sub where wired |
| Transactions | `MULTI`/`EXEC`/`DISCARD`/`WATCH`/`UNWATCH` |
| Scripting | `EVAL`/`EVALSHA`/`EVAL_RO`/`EVALSHA_RO`, `SCRIPT`, `FUNCTION`, `FCALL`/`FCALL_RO` |
| Search | `FT.CREATE`/`SEARCH`/`DROPINDEX`/`INFO`/`_LIST`/`TAGVALS`/`ALIAS*` |
| Admin | `INFO`, `CONFIG`, `CLIENT`, `COMMAND`, `SLOWLOG`, `ACL`, `MEMORY`, `DEBUG`, `SHUTDOWN`, `HEALTH` |
| Replication / cluster / sentinel | `REPLICAOF`, `ROLE`, `WAIT`, `FAILOVER`, `CLUSTER *`, `SENTINEL *` |

### Scripting quick start

```bash
# Classic EVAL
EVAL "return redis.call('GET', KEYS[1])" 1 mykey

# RESP3 bools from Lua
EVAL "redis.setresp(3); return true" 0

# Redis Functions library
FUNCTION LOAD "#!lua name=mylib
redis.register_function('echo', function(keys, args)
  return args[1]
end)"
FCALL echo 0 hello
```

`CONFIG GET|SET lua-time-limit` controls the hard script timeout (ms; `0` = unlimited).

### Search quick start

```bash
FT.CREATE articles PREFIX 1 article: SCHEMA title TEXT WEIGHT 2.0 body TEXT
HSET article:1 title "Rust systems" body "low-level performance"
FT.SEARCH articles "rust" WITHSCORES LIMIT 0 10

# Vector + HNSW
FT.CREATE emb SCHEMA v VECTOR HNSW 6 TYPE FLOAT32 DIM 128 DISTANCE_METRIC COSINE M 16 EF_CONSTRUCTION 200
```

Text hits are ranked with **field-weighted TF-IDF**. HNSW uses an adaptive search beam for large-k ANN.

### Pub/Sub

```bash
SUBSCRIBE notifications
PUBLISH notifications "hello"
PSUBSCRIBE news.*
```

Details: [docs/pubsub.md](docs/pubsub.md)

### Distributed locks

**Single-instance (Redis pattern):**

```bash
SET mylock <uuid> NX EX 10
GETDEL mylock
```

**Redlock (multi-instance)** — library API + CLI flags; optional fair queue and deadlock detector/UI. See [docs/redlock.md](docs/redlock.md), [docs/distributed_locks.md](docs/distributed_locks.md), [docs/deadlock_detection.md](docs/deadlock_detection.md).

---

## Example session

```bash
redis-cli -p 6379

127.0.0.1:6379> SET mykey "Hello, Kore!"
OK
127.0.0.1:6379> GET mykey
"Hello, Kore!"
127.0.0.1:6379> INCR counter
(integer) 1
127.0.0.1:6379> ZADD leaderboard 1500 p1 1800 p2
(integer) 2
127.0.0.1:6379> ZREVRANGE leaderboard 0 0 WITHSCORES
1) "p2"
2) "1800"
127.0.0.1:6379> HEALTH
ready
127.0.0.1:6379> INFO server
# Server
…
```

Client smoke (optional):

```bash
./scripts/client_smoke.sh   # redis-cli (+ redis-py when available)
```

---

## Known limitations

Honest gaps vs full Redis/Valkey (see [CHANGELOG](CHANGELOG.md) and [TODO.md](TODO.md)):

- Pipelined SET absolute throughput still below Valkey on measured hosts
- Redis Functions dump is Kore **KORF1** (not Redis-native blob); libraries not yet in RDB/AOF
- No binary Redis **cluster bus** (topology 2PC is RESP-based)
- Sentinel hello long-lived `SUBSCRIBE` fan-in not implemented
- Geo DUMP restores as zset for Redis `TYPE`; foreign Redis stream listpack fixtures residual
- Scripting: no nested `EVAL`; movablekeys catalog incomplete vs full Redis
- Search: no BM25 parameter tuning / stemmers; ANN is approximate (HNSW)

---

## Performance characteristics

- **Concurrency** — high via sharding + async I/O (Tokio)
- **Memory** — configurable `maxmemory` + eviction policies
- **Latency** — microsecond-class hits on local/hot paths
- **Throughput** — scales with cores; pipeline path improved in productization batches (GC–GE)

---

## License / contributing

Project history and batch work live in [TODO.md](TODO.md). Prefer [docs/ops.md](docs/ops.md) for production deploy notes and [docs/locking.md](docs/locking.md) before large concurrent changes.
