# Locking and error-handling guidelines

Short contributor notes for Kore’s concurrent keyspace. Prefer matching existing
patterns in the modules you touch over inventing new lock orders.

## Primitives

- Prefer **`parking_lot`** (`Mutex`, `RwLock`) for short critical sections on the
  command path. Avoid holding locks across `.await`.
- Shard maps (`ShardedHashMap` / `ShardedKeyMap`) use **per-shard** write locks.
  Multi-shard ops are **not** atomic unless a higher-level protocol says so
  (e.g. exclusive load commit).
- Atomically updated counters use `std::sync::atomic` with `Relaxed` for stats
  and `Acquire`/`Release` where publish/observe ordering matters
  (`Databases::load_in_progress`, `load_generation`).
- Multi-DB install vs export: `Databases::keyspace_epoch_lock` (`parking_lot::RwLock`).

## Established lock orders

Never invert these (deadlock risk):

| Area | Order |
|------|--------|
| Search indices | **aliases** then **indices** (create / drop / alias_* / `get_index` / `has_any_state` / take-install) |
| Background expire | `autosweep_cycle_lock` held for the whole expire body; `with_autosweep_paused` disables the flag then acquires the lock |
| Multi-DB load | Pause autosweep on **all** DBs before any `replace_keyspaces_from` |
| Multi-DB epoch | `keyspace_epoch_lock`: **write** = install loop; **read** = multi-DB export / `with_stable_keyspace_view` (never invert with per-shard map locks held across the epoch lock) |

## Unified keyspace facade (Batch FG)

- Cross-type **TYPE / EXISTS / `key_type`** go through `Cache::get_key_value`
  (multi-map probe today). Prefer that over ad-hoc per-map `contains_key`
  walks in new code.
- **DEL** uses `remove_key_value_raw` then clears `typed_expires` / search
  docs. Typed expire purge paths use the same remove helper without clearing
  expire (caller owns the side map).
- Physical layout is still multi-map until **FG-2+**; do not assume a single
  shard lock covers hash+string of the same name (names must not dual-reside).
  Design detail: `docs/module_architectures.md` § Unified keyspace +
  `src/cache/keyspace.rs` rustdoc.

## Keyspace replace / load commit

- Public AOF/RDB load wrappers are the supported APIs (scratch-load + swap).
- Raw `DbSnapshot::load_into` is **non-transactional** — do not call on live DBs.
- `install_keyspace_payload`: drain target into discard locals, then **`fill_all`**
  (map must already be empty; `debug_assert` in debug builds).
- Multi-DB replace stages **all** source payloads first, then installs every DB
  under one **`keyspace_epoch_lock` write** section (lock-step install).
  - Multi-DB exporters must use **`Databases::with_stable_keyspace_view`**
    (epoch **read**) — `MultiDbSnapshot::from_databases` and AOF
    `rewrite_databases` do this — so they never sample DB0-new + DB1-old
    mid-install.
  - Command path returns **`-LOADING`** while `Databases::load_in_progress()`
    is true (data plane + `SYNC`/`PSYNC`).
  - `load_generation` publishes **once at end** of replace (frozen mid-install).
  - **Panic rollback (Batch DS):** each DB install retains the discarded old
    payload; if the install loop panics after DB *i* is fully swapped, a drop
    guard reinstalls olds for `0..=i` while the epoch write is still held.
    Peak dual-residency is slightly higher during install (olds retained until
    commit). Staging-only panic still leaves all targets intact.
- **Residuals** (accepted Batch DT unless privileged paths grow)
  - Panic **inside** a single DB’s multi-map fill (after drain, before install
    returns) is not rolled back — that one DB can stay torn. Full Arc-swap of
    whole DB vector (Option C) would change that; not planned while **LOADING**
    remains the only public barrier to keyspace reads during install.
  - Raw `Arc<Cache>` access that skips the epoch lock can still observe a
    mid-loop multi-DB tear (and mid-payload single-DB map tear) **while install
    is running**. Command path is gated; do not walk all DBs’ keyspace without
    the epoch read lock. Non-keyspace multi-DB walks (blocked clients, CONFIG
    propagation) are fine without it.
  - Single-DB sequential multi-map fill is **not** all-or-nothing; LOADING
    denial of data-plane + `SYNC`/`PSYNC` is the intentional client barrier.
- **LOADING allowlist** (connection / discovery / repl handshake only — no
  keyspace snapshot): `AUTH`, `HELLO`, `PING`, `ECHO`, `QUIT`, `RESET`, `INFO`,
  `COMMAND`, `ROLE`, `REPLCONF`, `CLIENT`, `CONFIG`, `MODULE`.
  - **`SYNC` / `PSYNC` are denied** during replace so fullresync cannot snapshot
    mid-`install_keyspace_payload` torn state (e.g. strings filled, typed maps
    empty, counters not yet installed).
  - **`CONFIG` stays allowed**: ops/live params only; no data-plane keyspace
    read. Revisit if a `CONFIG GET` path ever exposes keyspace contents.

## Errors

- Prefer typed `Error` variants (`OutOfMemory`, `InvalidArgument`, …) at module
  boundaries. Search mutator strings that start with `OOM:` / `OOM ` map via
  `map_ft_mutator_error` / `map_rdb_ft_mutator_error` (map **raw** message
  first, then prefix context).
- User-visible Redis-style errors stay as RESP `Error` strings
  (`NOREPLICAS`, `LOADING`, `READONLY`, …).

## Tests

- Land letter-batch tests with the feature (`tests/<letter>_*.rs` or extend the
  matching suite). Use `start_sweep: false` unless the test is about expire.
