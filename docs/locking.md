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

## Established lock orders

Never invert these (deadlock risk):

| Area | Order |
|------|--------|
| Search indices | **aliases** then **indices** (create / drop / alias_* / `get_index` / `has_any_state` / take-install) |
| Background expire | `autosweep_cycle_lock` held for the whole expire body; `with_autosweep_paused` disables the flag then acquires the lock |
| Multi-DB load | Pause autosweep on **all** DBs before any `replace_keyspaces_from` |

## Keyspace replace / load commit

- Public AOF/RDB load wrappers are the supported APIs (scratch-load + swap).
- Raw `DbSnapshot::load_into` is **non-transactional** — do not call on live DBs.
- `install_keyspace_payload`: drain target into discard locals, then **`fill_all`**
  (map must already be empty; `debug_assert` in debug builds).
- Multi-DB replace stages **all** source payloads before any install. It is
  **not** atomic to concurrent readers; the command path returns
  **`-LOADING`** while `Databases::load_in_progress()` is true.
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
