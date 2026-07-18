# Benchmarks

Compare Kore throughput/latency against Redis 7.x and/or Valkey on the **same machine**, with persistence disabled so results reflect in-memory command path only.

This document is a **reproducible runbook**, not a marketing claim. Fill result tables after you measure; leave TBD until then.

## Purpose

- Baseline SET/GET (and related) ops/s vs Redis/Valkey
- Spot regressions after sharding, skiplist, AOF, or replication changes
- Document hardware/versions so numbers are comparable over time

## Build

### Kore

```bash
cargo build --release
# Disable RDB auto-save and AOF for pure memory comparison
./target/release/kore \
  --host 127.0.0.1 \
  --port 6380 \
  --save "" \
  --appendonly false
```

If your binary does not accept empty `--save`, use whatever disables timed SAVE rules (or delete save rules via `CONFIG SET save ""` after start). Prefer **port 6380** so a local Redis on 6379 can run in parallel for A/B runs.

### Redis / Valkey (apples-to-apples)

Start with AOF off and RDB save disabled, e.g.:

```bash
# Redis example
redis-server --port 6379 --save "" --appendonly no --daemonize no

# Valkey example
valkey-server --port 6379 --save "" --appendonly no --daemonize no
```

Match maxmemory policy only if you are testing eviction; for pure SET/GET leave limits high or unlimited.

## Tooling

Use `redis-benchmark` (works against Kore’s RESP server):

```bash
redis-benchmark -h 127.0.0.1 -p PORT ...
```

## Standard suite

Run against Kore (`-p 6380`) and Redis/Valkey (`-p 6379`) with **identical** flags.

```bash
# SET/GET, no pipeline
redis-benchmark -h 127.0.0.1 -p 6380 -t set,get -n 100000 -q -c 50 -P 1

# SET/GET, pipeline 16
redis-benchmark -h 127.0.0.1 -p 6380 -t set,get -n 100000 -q -c 50 -P 16

# INCR
redis-benchmark -h 127.0.0.1 -p 6380 -t incr -n 100000 -q -c 50

# Optional: larger values
redis-benchmark -h 127.0.0.1 -p 6380 -t set,get -n 100000 -q -c 50 -d 256
```

Suggested variants (report separately):

| Label | Flags |
|-------|--------|
| baseline | `-c 50 -P 1 -n 100000` |
| pipelined | `-c 50 -P 16 -n 100000` |
| high concurrency | `-c 200 -P 1 -n 200000` |

## Methodology

1. **Same host**, quiet load (close browsers/IDEs if measuring carefully).
2. **Warm-up**: one full suite discarded; then run **3** measured passes; report **median** ops/s.
3. Match client `-c` / `-P` / `-n` / `-d` across servers.
4. Prefer release builds only (`cargo build --release` for Kore; stock Redis/Valkey packages or their release builds).
5. Record environment for every table:
   - Date
   - Kore version (`Cargo.toml` / `INFO server`)
   - Redis and/or Valkey version (`redis-server --version`)
   - OS, kernel, CPU model, `nproc`, RAM
   - Whether turbo boost / power management was left default
6. Do **not** enable AOF with every-write fsync for “memory” comparisons — Kore currently fsyncs AOF on each append and will look artificially slow.

## Fairness rules

- Compare only commands Kore implements.
- Disable persistence on all sides for core throughput numbers.
- Do not compare cluster / TLS / modules Kore does not implement yet.
- If one server is CPU-bound and another is network-bound, note it; prefer loopback only.

## Result tables

Fill after measurement. Until then, leave cells as `TBD`.

### Environment

| Field | Value |
|-------|--------|
| Date | TBD |
| Host / CPU | TBD |
| OS | TBD |
| Kore | 0.6.0 (release) |
| Redis | TBD |
| Valkey | TBD |

### Throughput (ops/s, median of 3)

| Workload | Kore | Redis | Valkey | Notes |
|----------|------|-------|--------|-------|
| SET/GET c=50 P=1 | TBD | TBD | TBD | |
| SET/GET c=50 P=16 | TBD | TBD | TBD | |
| INCR c=50 | TBD | TBD | TBD | |
| SET/GET c=50 P=1 d=256 | TBD | TBD | TBD | |

### Latency (optional)

If `redis-benchmark` reports p50/p99, record them in a second table with the same workload labels.

## Vector search (HNSW vs FLAT) — methodology

Generic `redis-benchmark` does not cover RediSearch vectors. For Kore-internal
comparison:

1. Build with `cargo test --release` (correctness) or a dedicated bench binary
   when added.
2. Use the same vectors/query for FLAT (exact) and HNSW (`M`, `ef_construction`).
3. Report: dataset size (N, dim), recall@k vs FLAT ground truth, and wall time
   for build + query (median of ≥3 runs).
4. In-tree correctness gates (unit tests in `src/vector_search.rs`):
   - `hnsw_top1_matches_flat_on_small_set` (tiny N; graph search should still match FLAT)
   - `hnsw_search_follows_edges_not_full_scan` (fails if search ignores edges)
   - `hnsw_add_excludes_self_from_neighbors`

**Implementation note (Batch CQ):** `HNSWIndex::search` walks neighbor edges
(SEARCH-LAYER) with candidate list size `ef_search` (defaults to
`ef_construction`). Insert still assigns all nodes to **layer 0** only
(multi-layer assignment simplified). This is approximate ANN and may not match
full RedisSearch HNSW; use FLAT for exact recall baselines. Brute-force over
`vectors` remains only as a defensive fallback when the entry-point vector is
missing.

| Field | Value |
|-------|--------|
| Date | TBD |
| N / dim | TBD |
| HNSW M / ef | TBD |
| Recall@10 vs FLAT | TBD |
| Query p50 (ms) FLAT / HNSW | TBD |

## Load dual-residency peak (scratch-load)

AOF/RDB public load builds a full scratch keyspace then swaps. Peak process
RSS can approach ~2× used memory during stage (old multi-DB + scratch).
Mitigations already in tree: single-DB `load_bytes` pre-flush on success;
multi-DB avoids pre-flush for panic safety and uses `-LOADING` for clients.
Measure with `INFO memory` / OS RSS around `load_databases_bytes(..., true)`
if you need numbers for capacity planning.

## What not to compare

- Cluster hash slots / multi-node until Kore cluster exists
- TLS / ACL overhead until both sides use the same security features
- AOF-on durability modes without matching fsync policy
- Search / Redlock / fair-queue paths in generic `redis-benchmark` runs

## CI note

Do **not** gate CI on `redis-benchmark` by default (needs Redis tools, noisy, non-deterministic). Keep CI as `cargo test` only.

## Optional script

A future `scripts/bench.sh` may start Kore on 6380 and run the standard suite; until then, run the commands above manually.
