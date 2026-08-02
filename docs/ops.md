# Kore operations runbook

Operator guide for deploying and running Kore **0.7.0**. For command coverage and design history see [README.md](../README.md), [TODO.md](../TODO.md), and [CHANGELOG.md](../CHANGELOG.md).

## Build and install

```bash
cargo build --release
# binary: ./target/release/kore
./target/release/kore --help
```

CI builds every push/PR to `main` (`cargo build --all-targets`, `cargo test --all-targets -- --test-threads=1`) plus an optional client-smoke job (`scripts/client_smoke.sh`).

## Minimal production-ish start

```bash
mkdir -p /var/lib/kore
./target/release/kore \
  --host 0.0.0.0 \
  --port 6379 \
  --dir /var/lib/kore \
  --save "900,1 300,10 60,10000" \
  --maxmemory 0 \
  --maxmemory-policy allkeys-lru \
  --threads 0 \
  --log-format json \
  -v 1
```

| Flag | Notes |
|------|--------|
| `--dir` | Working directory for RDB/AOF, `nodes.conf`, `sentinel.conf` |
| `--save` | RDB auto-save rules (`sec,changes` pairs). Empty `""` disables timed SAVE |
| `--appendonly` | Enable AOF (flag; omit to leave off). Load prefers AOF when on |
| `--threads 0` | Use all CPUs (Tokio worker count) |
| `-v 1` | WARN default; `2`=INFO, `3`=DEBUG. `RUST_LOG` overrides |
| `--log-format json` | Structured logs for aggregators (boot-only) |

Clients: any RESP client (`redis-cli -h HOST -p 6379`, redis-py, ioredis, redis-rs).

Smoke after boot:

```bash
redis-cli -h 127.0.0.1 -p 6379 PING
redis-cli -h 127.0.0.1 -p 6379 HEALTH FULL
scripts/client_smoke.sh 6379   # if redis-cli installed
```

## Persistence

### RDB (Kore `KORDB`)

- Files: `{dir}/{dbfilename}` (default `dump.rdb`)
- Commands: `SAVE`, `BGSAVE`, `LASTSAVE`
- Boot: loads RDB when AOF is off or AOF file missing
- Includes multi-DB keyspaces, search schema, HNSW graph (v6+)

### AOF

```bash
./target/release/kore --dir /var/lib/kore --appendonly --appendfilename appendonly.aof
```

- Live append on writes; `BGREWRITEAOF` rewrites
- Boot prefers AOF when `--appendonly` is set and the file exists
- **Note:** AOF fsyncs on each append today — expect lower write throughput with AOF on

### Recommended layouts

| Role | Persistence |
|------|-------------|
| Cache / ephemeral | `--save ""` (no timed RDB); AOF off |
| Primary with durability | timed `--save` **or** `--appendonly` (or both) |
| Replica | usually no need for local save; follows primary stream |

## Replication

### Primary + replica

```bash
# primary
kore --host 0.0.0.0 --port 6379 --dir /var/lib/kore-p

# replica
kore --host 0.0.0.0 --port 6380 --dir /var/lib/kore-r \
  --replicaof 10.0.0.1:6379
```

- Handshake: `PSYNC` (partial when backlog allows) / full RDB bulk
- Replicas reject writes (`READONLY`); serve reads
- Client durability: `WAIT numreplicas timeout_ms`
- Gate writes: `CONFIG SET min-replicas-to-write N` / `min-replicas-max-lag`

### Failover

- On replica: `REPLICAOF NO ONE` or bare `FAILOVER`
- On primary: coordinated `FAILOVER TO <host> <port> [TIMEOUT ms] [FORCE]`

### TLS to primary (Batch GL)

```bash
kore --replicaof primary.example:6379 \
  --tls-replication \
  --tls-ca /etc/kore/ca.pem
# or pin with --tls-cert when CA is empty
```

## Sentinel-lite

Run Kore process(es) and issue `SENTINEL` commands (same binary; not a separate sentinel executable).

Typical ops:

| Command | Role |
|---------|------|
| `SENTINEL MONITOR name host port quorum` | Watch a master |
| `SENTINEL GET-MASTER-ADDR-BY-NAME name` | Client discovery |
| `SENTINEL FAILOVER name` | Manual failover |
| `SENTINEL CKQUORUM name` | Quorum / reachable check |
| `SENTINEL FLUSHCONFIG` | Persist `{dir}/sentinel.conf` |

Config is autosaved under `{dir}/sentinel.conf` on MONITOR/REMOVE/SET/MEET/switch.  
**Honesty:** promote ranking uses **higher** priority first (cluster-style); Redis Sentinel prefers lower numbers. No long-lived master `__sentinel__:hello` SUBSCRIBE (tick PUBLISH + peer HELLO only).

## Cluster

```bash
kore --cluster-enabled --host 0.0.0.0 --port 7000 --dir /var/lib/kore-c0
# peers via CLUSTER MEET; slots via ADDSLOTS / RESHARD / SETSLOT …
```

| Concern | Mechanism |
|---------|-----------|
| Topology file | `{dir}/nodes.conf` (load on boot, autosave on mutation) |
| Reshard | `CLUSTER RESHARD` / `MIGRATE` / MIGRATEKEYS; RESP prepare/commit 2PC (not binary bus) |
| Coverage | `--cluster-require-full-coverage` (default yes) → `CLUSTERDOWN` when slots unbound |
| Replica reads | `READONLY` on replica; `--cluster-allow-reads-when-down` |
| Announce | `--cluster-announce-ip` / `--cluster-announce-port` |

**Not Redis cluster-bus binary protocol** — clients that require the official bus may need RESP-only flows or a proxy.

## TLS (client-facing)

| Mode | Flags |
|------|--------|
| TLS-only on main port | `--tls --tls-cert cert.pem --tls-key key.pem` |
| Dual plain + TLS | `--tls --tls-port 6380 --tls-cert … --tls-key …` (plain stays on `--port`) |
| mTLS | add `--tls-auth-clients --tls-ca ca.pem` |

Unix socket (`--unixsocket`) is plaintext and has no TLS wrap.

## Health and metrics

```bash
# RESP
redis-cli HEALTH
redis-cli HEALTH FULL
redis-cli INFO server
redis-cli INFO replication

# Prometheus text (localhost only)
kore --metrics-port 9121
curl -s http://127.0.0.1:9121/metrics
```

Deadlock UI (localhost, **no auth** — Batch GM residual):

```bash
kore --enable-redlock --deadlock-ui-port 9122
# open http://127.0.0.1:9122/
```

## Auth / ACL

```bash
# single password (default user)
kore --auth secret

# ACL file
kore --aclfile /etc/kore/users.acl
```

Runtime: `ACL SETUSER` / `GETUSER` / `LIST` / `LOG` / `SAVE` / `LOAD`.

## Memory and eviction

| Flag / config | Role |
|---------------|------|
| `--maxmemory` | Cap (0 = ~80% of system RAM at boot) |
| `--maxmemory-policy` | `allkeys-lru`, `volatile-ttl`, `noeviction`, … |
| `--maxentrysize` | Per-value ceiling |
| `--evict false` | Force noeviction behavior |

`CONFIG GET|SET maxmemory` updates live atomics and best-effort re-evicts when lowered.

## Graceful shutdown

- SIGINT / SIGTERM stop accepts, drain, **SAVE** when persistence is configured
- `SHUTDOWN [NOSAVE|SAVE]` from clients

## Capacity planning notes

- Default **4096** shards — high concurrency; lower only if memory for empty maps matters
- Pipelined **SET** is the main measured gap vs Valkey (~2× on M3 Pro; see [benchmarks.md](benchmarks.md))
- AOF-on write path is slower (per-append fsync)
- Multi-replica publish is ordered (deferred fan-out after GE); pure standalone skips stream publish (GC)

## Checklist before production

1. [ ] Release binary from `cargo build --release` (or CI artifact)
2. [ ] `--dir` on durable disk; backups of RDB/AOF / `nodes.conf` / `sentinel.conf`
3. [ ] Auth or ACL file set if exposed beyond trusted network
4. [ ] TLS (and mTLS if required) for untrusted networks
5. [ ] `HEALTH FULL` / metrics scrape configured
6. [ ] Replica or Sentinel plan tested in staging
7. [ ] Client smoke: `scripts/client_smoke.sh` or app integration tests
8. [ ] Known gaps reviewed: Functions not in RDB/AOF, no cluster bus, admin UI unauthenticated

## Related docs

- [locking.md](locking.md) — contributor lock orders / load commit
- [benchmarks.md](benchmarks.md) — redis-benchmark methodology
- [redlock.md](redlock.md) / [deadlock_detection.md](deadlock_detection.md) / [pubsub.md](pubsub.md)
