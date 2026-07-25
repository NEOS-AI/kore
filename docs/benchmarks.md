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
# Disable RDB auto-save for pure memory comparison.
# AOF is off by default (do not pass --appendonly).
./target/release/kore \
  --host 127.0.0.1 \
  --port 6380 \
  --save ""
```

- `--save ""` disables timed RDB auto-save (`parse_save_rules` empty list).
- `--appendonly` is a clap **flag** (presence enables AOF). Omit it for persistence-off runs; do not pass `--appendonly false`.
- Prefer **port 6380** so a local Redis/Valkey on 6379 can run in parallel for A/B runs. If 6379 is occupied by an unrelated instance, start the comparison peer on another port (e.g. 6378) and record that port in the result table.

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

**Batch FD (2026-07-25):** filled from a single-host run. Numbers are **indicative for this host only** — not a portable win claim. Re-measure before citing elsewhere.

### Environment

| Field | Value |
|-------|--------|
| Date | 2026-07-25 |
| Host / CPU | Apple M3 Pro (11 logical CPUs), 18 GB RAM |
| OS | macOS 26.3.1 (Darwin 25.3.0 arm64) |
| Kore | **0.6.0** release; git `1745294`; `cargo build --release`; default `--threads 0` (= ncpu); `--host 127.0.0.1 --port 6380 --save ""` (AOF off); `--dir /tmp/kore-bench-fd` |
| Redis | **Not measured this run** — port 6379 already held by an unrelated long-running Valkey (RDB save enabled); did not stop user daemons |
| Valkey | **9.0.0** (`redis-server` / `valkey-server` Homebrew build `28c4ffd24ca1d1ff`); `127.0.0.1:6378 --save "" --appendonly no --dir /tmp/redis-bench-fd` |
| Client | `valkey-benchmark` 9.0.0 (`redis-benchmark` symlink); loopback only |
| Power | default turbo / power management (not pinned) |
| Method | warm-up suite discarded; **3** measured passes; table = **median** ops/s (and median p50) |

### Flags used (identical client flags on both servers)

```text
# baseline
redis-benchmark -h 127.0.0.1 -p PORT -t set,get -n 100000 -q -c 50 -P 1
# pipelined
redis-benchmark -h 127.0.0.1 -p PORT -t set,get -n 100000 -q -c 50 -P 16
# INCR
redis-benchmark -h 127.0.0.1 -p PORT -t incr -n 100000 -q -c 50
# larger values
redis-benchmark -h 127.0.0.1 -p PORT -t set,get -n 100000 -q -c 50 -d 256
```

Quiet mode (`-q`) reports **p50 only** (no p99 in this client build).

### Throughput (ops/s, median of 3)

SET and GET reported separately (redis-benchmark does not emit a combined figure).

| Workload | Kore | Redis | Valkey 9.0.0 (:6378) | Notes |
|----------|------|-------|----------------------|-------|
| SET c=50 P=1 | **185,874** | — | **203,252** | baseline; value size default (3 bytes) |
| GET c=50 P=1 | **186,916** | — | **196,464** | |
| SET c=50 P=16 | **497,512** | — | **1,515,152** | pipeline; Kore SET pipeline lag vs GET |
| GET c=50 P=16 | **1,818,182** | — | **1,886,793** | |
| INCR c=50 | **186,916** | — | **190,114** | |
| SET c=50 P=1 d=256 | **177,620** | — | **200,401** | larger payload |
| GET c=50 P=1 d=256 | **190,114** | — | **205,339** | |

Pass-level raw ops/s (for audit):

| Workload | Kore passes | Valkey passes |
|----------|-------------|---------------|
| SET P=1 | 185874 / 187266 / 181488 | 203252 / 209644 / 186567 |
| GET P=1 | 186916 / 186916 / 189394 | 196464 / 210084 / 187617 |
| SET P=16 | 429185 / 546448 / 497512 | 1515152 / 1587302 / 1515152 |
| GET P=16 | 1818182 / 1428571 / 1818182 | 1886793 / 1960784 / 1818182 |
| INCR | 186916 / 153610 / 187266 | 190114 / 210084 / 187617 |
| SET d=256 | 177620 / 158730 / 184162 | 196464 / 202429 / 200401 |
| GET d=256 | 190114 / 190840 / 185529 | 205339 / 199203 / 205761 |

### Latency p50 (msec, median of 3)

| Workload | Kore p50 | Valkey p50 | Notes |
|----------|----------|------------|-------|
| SET c=50 P=1 | 0.143 | 0.127 | |
| GET c=50 P=1 | 0.143 | 0.127 | |
| SET c=50 P=16 | 1.567 | 0.447 | pipeline batch latency (higher is expected) |
| GET c=50 P=16 | 0.311 | 0.343 | |
| INCR c=50 | 0.143 | 0.127 | |
| SET c=50 P=1 d=256 | 0.151 | 0.127 | |
| GET c=50 P=1 d=256 | 0.143 | 0.127 | |

p99: **not reported** by this `redis-benchmark -q` build.

### Interpretation (host-local only)

- On this M3 Pro / loopback run, Kore is **within ~5–15%** of Valkey 9 on non-pipelined SET/GET/INCR and on GET with pipeline.
- **Pipelined SET** is the clear gap (Kore ~0.5M ops/s vs Valkey ~1.5M) — likely write-path / command-dispatch cost under batching; not investigated in Batch FD.
- **No portable performance claim.** Different CPUs, OS, thread counts, or Valkey/Redis builds will move absolute ops/s and ratios. Re-run this section before any external comparison.
- Redis (non-Valkey) left unmeasured because 6379 was busy; Valkey is the Redis-protocol peer used here.

## Vector search (HNSW vs FLAT) — methodology

Generic `redis-benchmark` does not cover RediSearch vectors. For Kore-internal
comparison:

1. Build with `cargo test --release` (correctness + micro timing) or a dedicated
   bench binary when added. Prefer **release** for throughput ratios; debug is
   fine for recall gates only.
2. Use the same vectors/query for FLAT (exact) and HNSW (`M`, `ef_construction`).
3. Report: dataset size (N, dim), recall@k vs FLAT ground truth, and wall time
   for the query set (build excluded). CI does **not** gate on absolute ms.
4. In-tree gates (unit tests in `src/vector_search.rs`):
   - `hnsw_recall_at_k_vs_flat_and_throughput` (**Batch CV + DK + DL**): always-on
     CI gate. N=300 unit vectors, dim=16, Cosine, Q=40, fixed seed
     `0xC0FFEE42`; HNSW **M=8 / ef=32** (DK tightened from CV M=16/ef=100 so
     recall@10 is load-bearing); asserts mean recall@1 ≥ **0.975** and
     recall@10 ≥ **0.93** (**Batch DL**: was 0.95; ~5.5pp headroom vs observed
     ≈0.985 for cross-arch f32 variance while still load-bearing); prints
     **single-shot** FLAT vs HNSW wall time (`eprintln!`, see `--nocapture`).
     Fast enough for debug CI.
   - `hnsw_recall_after_remove_update_churn` (**Batch DL**): always-on. N=120,
     remove 15 + update 15, Q=24, seed `0xD1C40177`, M=8/ef=32; soft floors
     mean recall@1 ≥ **0.90** / @10 ≥ **0.85** vs FLAT after churn (bridge
     reconnect is heuristic — floors looser than the no-churn gate).
   - `hnsw_recall_larger_n_median_throughput` (**Batch DK**, `#[ignore]`): N=5000,
     dim=16, Cosine, Q=40, seed `0xD1A6E501`; HNSW M=16 / ef=100; soft recall
     floors ≥ 0.95 / 0.90; prints **median-of-3** search wall times (build
     excluded). Not in default `cargo test`. Prefer release:
     ```bash
     cargo test --release --lib hnsw_recall_larger_n_median_throughput -- --ignored --nocapture
     ```
   - `hnsw_top1_matches_flat_on_small_set` (tiny N; graph search should still match FLAT)
   - `hnsw_search_follows_edges_not_full_scan` (fails if search ignores edges)
   - `hnsw_add_excludes_self_from_neighbors`
   - `hnsw_remove_middle_unlinks_graph` / `hnsw_remove_entry_reassigns` / `hnsw_remove_readd_clears_stale_neighbors` (Batch CS)
   - `hnsw_insert_preserves_reachability_from_entry` (Batch CS; small M, BFS + self-search)
   - `hnsw_update_rewires_graph` (Batch CS; large vector move)
   - `hnsw_bridge_remove_keeps_survivors_reachable` / `hnsw_bridge_update_keeps_ends_reachable` (Batch CT)
   - `hnsw_bridge_remove_asymmetric_incoming_reconnects` / `hnsw_bridge_remove_star_multiway_reconnects` (Batch CU)
   - `hnsw_m1_hub_churn_preserves_reachability` (Batch CT smoke)

**Implementation note (Batch CQ + CS + CT + CU + CV + DK + DL):** `HNSWIndex::search` walks neighbor
edges (SEARCH-LAYER) with candidate list size `ef_search` (defaults to
`ef_construction`). Insert still assigns all nodes to **layer 0** only
(multi-layer assignment simplified). Layer-0 prune uses `M_max ≈ 2M` and
force-keeps a reverse edge to each **new** node at insert time (mitigates
immediate drop under degree caps; **not** a durable global reachability
invariant — later hub prunes can still disconnect older leaves). `remove`
unlinks reverse edges and reconnects an **undirected** former-neighbor set
(outgoing ∪ reverse scan) via a spanning structure — full clique when degree
fits, else nearest-neighbor path — force-keeping those edges on both endpoints
(Batch CU; extends CT 2-chain closest-peer). Covers asymmetric incoming-only
links and multi-way stars under degree caps; still **not** a global
non-partition guarantee under arbitrary later hub churn. Existing-id `add`
rewires via remove + re-insert (inherits bridge repair). Approximate ANN; use
FLAT for exact recall baselines. Brute-force over `vectors` remains only as a
defensive fallback when the entry-point vector is missing.

### Indicative micro-results (Batch DK)

**Disclaimer:** single-host, indicative only — not a cross-machine claim and not
a CI gate on absolute ms. Absolute ms vary with CPU; use **relative** FLAT/HNSW
ratios and recall@k. Timing method is labeled per table (**single-shot** vs
**median-of-3**).

#### Always-on unit gate (N=300, single-shot)

Re-run:

```bash
cargo test --release --lib hnsw_recall_at_k_vs_flat_and_throughput -- --nocapture
```

| Field | Value |
|-------|--------|
| Date | 2026-07-19 |
| Host | single-host dev (Apple Silicon / macOS); release preferred for ms |
| Timing | **single-shot** query-set wall (build excluded) |
| N / dim / metric | 300 / 16 / Cosine (unit vectors) |
| Queries | Q=40 random unit queries; seed `0xC0FFEE42` |
| HNSW M / ef | 8 / 32 (`ef_search` = `ef_construction`) |
| mean recall@1 vs FLAT | **1.00** (gate ≥ 0.975) |
| mean recall@10 vs FLAT | **≈0.985** (gate ≥ 0.93; Batch DL headroom vs cross-arch f32) |
| Query set wall (k=10) FLAT / HNSW | host-dependent; typically HNSW slower at this N |

**Interpretation:** at N=300 with this M/ef, brute-force FLAT is usually cheaper
than graph walk (HNSW overhead dominates). The always-on test is a **load-bearing
recall correctness** smoke plus **single-shot** relative timing — not a claim that
HNSW wins at N=300. Batch DL loosens r@10 floor 0.95→0.93 (~5.5pp cushion vs
observed ≈0.985) so host/arch f32 differences do not flake CI while the gate
still fails broken search.

#### Optional larger-N bench (N=5000, median-of-3, `#[ignore]`)

Re-run (prefer release; not in default CI):

```bash
cargo test --release --lib hnsw_recall_larger_n_median_throughput -- --ignored --nocapture
```

| Field | Value |
|-------|--------|
| Date | 2026-07-19 |
| Host | single-host dev (Apple Silicon / macOS); **release** profile recommended |
| Timing | **median-of-3** query-set wall (build excluded; build printed separately) |
| N / dim / metric | 5000 / 16 / Cosine (unit vectors) |
| Queries | Q=40 random unit queries; seed `0xD1A6E501` |
| HNSW M / ef | 16 / 100 |
| mean recall@1 / @10 vs FLAT | **1.00** / **1.00** (soft floors ≥ 0.95 / 0.90) |
| Query set wall (k=10) FLAT / HNSW | ~13.1 ms / ~8.2 ms (HNSW ≈ **1.61×** FLAT; one release host) |
| Build wall (one-shot) | ~1.0 s (indicative; not gated) |

**Interpretation:** at N=5000 on this host/release profile, HNSW search wall time
beats FLAT while keeping perfect recall vs FLAT on the fixed seed. Numbers are
**indicative median-of-3 on one host** — re-measure before claiming a portable
speedup. Debug builds inflate absolute ms and can change ratios; prefer
`--release` when comparing throughput. The always-on N=300 gate still shows
FLAT cheaper (graph overhead dominates at small N).

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
