# Module architectures

## 1. Commands module layout
```
src/commands/
├── mod.rs          - CommandHandler + execute() dispatch
├── basic.rs        - PING, ECHO, AUTH, …
├── key_value.rs    - GET, SET, DEL, EXISTS, MGET, MSET, …
├── counter.rs      - INCR, DECR, …
├── expiration.rs   - EXPIRE, PEXPIRE, TTL, PTTL, …
├── admin.rs        - DBSIZE, KEYS, SCAN, FLUSH, INFO, CONFIG, …
└── sorted_set.rs   - ZADD, ZRANGE, …
```

## 2. Cache module layout
```
src/cache/
├── mod.rs          - Cache struct + constructors
├── keyspace.rs     - KeySlot + KeyValue + unified lookup/remove facade (FG / FP)
├── storage.rs      - store, load, delete, exists, keys/scan, load install
├── operations.rs   - incr/decr, RENAME, …
├── expiration.rs   - expire, ttl, KeySlot expire, active expire
├── eviction.rs     - maxmemory policies, sample/evict, sweep
├── sorted_sets.rs / geo_sets.rs / hashes.rs / lists.rs / sets.rs / streams.rs
└── config.rs       - max_entry_size, eviction sample, …
```

## 3. Unified keyspace (Batch FG / FG-2 / FG-3 / FG-4 / FP / FQ)

### Today (FQ: single map + per-slot key-level expire for all types)

`Cache` holds:

| Field | Type |
|-------|------|
| `key_values` | `ShardedKeyMap<KeySlot>` — value + optional key-level expire |
| `list_blockers` / `stream_blockers` | blocking waiters (not key storage) |

Logical invariant: **one name → at most one type** (`ensure_type` / WRONGTYPE).  
**No dual-residence:** a name lives in exactly one place (`key_values`).  
**No side expire map:** key-level TTL is `KeySlot.expires_at` for **all** types
(Batch FP typed; Batch FQ strings; Batch FU slot-only SoT).

```text
struct KeySlot {
    expires_at: Option<Instant>,  // all types SoT (FQ/FU)
    value: KeyValue,
}

enum KeyValue {
    String(SharedEntry),  // Entry.expires_at cleared on write-back (FU)
    Hash(SharedHash),
    List(SharedList),
    Set(SharedSet),
    ZSet(SharedSortedSet),
    Geo(SharedGeoSet),    // TYPE → "zset"
    Stream(SharedStream),
}

Cache::get_key_value(key) -> Option<KeyValue>   // single map (+ lazy expire)
Cache::key_type / exists                         // via get_key_value
Cache::delete / remove_key_value_raw             // unified remove (slot drops expire)
Cache::mutate_string                             // RMW under shard lock; slot expire arg/return
```

| Op | On unified map |
|----|----------------|
| **TYPE** | `get` → `KeyValue::key_type()` → Redis TYPE string |
| **WRONGTYPE** | `ensure_type` / `ensure_string_or_absent` / `mutate_string` |
| **DEL / EXISTS** | remove / `is_some` on one map |
| **SCAN / KEYS / DBSIZE / RANDOMKEY** | iterate `key_values` only |
| **RENAME** | take whole `KeySlot` (value + expire), insert under new name |
| **TTL / EXPIRE** | all types: `KeySlot.expires_at` only (FQ/FU) |
| **Memory / eviction** | per-variant size; all victims sampled from `key_values` |

### Migration plan

1. **FG (done, slice A):** `KeyValue` + facade; storage multi-map.
2. **FG-2 (done, hashes):** Physical hashes in `ShardedKeyMap<KeyValue>`.
3. **FG-3 (done):** list/set/zset/geo/stream into `key_values`; legacy per-type
   maps removed; `KeyspacePayload` drains `map` + `key_values` streams; eviction
   samples typed keys from one map.
4. **FG-4 (done):** Merge strings into `key_values` as `KeyValue::String`;
   collapse `KeyspacePayload` to one `key_values` stream.
5. **FP (done):** Fold typed TTL into `KeySlot.expires_at`; remove side
   `typed_expires` map.
6. **FQ (done):** String key-level expire on the same slot header; EXPIRE/TTL/
   active expire/volatile sample unified.
7. **FU (done):** Slot-only string TTL; stop dual-writing `Entry.expires_at` on
   RMW write-back. Residual: `Entry.expires_at` field remains for legacy
   `ShardedHashMap` + load read-projection; search-doc eviction special.

**Load/install (`KeyspacePayload`):** `key_values: Vec<(Bytes, KeySlot)>` +
WATCH / search / memory counters (key-level expire rides on each slot). Epoch
install semantics (DR/DS) unchanged.