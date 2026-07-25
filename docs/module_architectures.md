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
├── keyspace.rs     - KeyValue enum + unified lookup/remove facade (Batch FG)
├── storage.rs      - store, load, delete, exists, keys/scan, load install
├── operations.rs   - incr/decr, RENAME, …
├── expiration.rs   - expire, ttl, typed_expires, active expire
├── eviction.rs     - maxmemory policies, sample/evict, sweep
├── sorted_sets.rs / geo_sets.rs / hashes.rs / lists.rs / sets.rs / streams.rs
└── config.rs       - max_entry_size, eviction sample, …
```

## 3. Unified keyspace (Batch FG)

### Today (pragmatic multi-map + facade)

`Cache` still holds **separate** containers per Redis type:

| Field | Type |
|-------|------|
| `map` | strings (`SharedEntry`) |
| `sorted_sets` / `geo_sets` | sharded typed maps |
| `hashes` / `lists` / `sets` / `streams` | `RwLock<HashMap<…>>` |
| `typed_expires` | absolute `Instant` for non-string keys |

Logical invariant: **one name → at most one type** (`ensure_type` / WRONGTYPE).

**FG slice A** adds a view type and facade so cross-type ops share one path:

```text
enum KeyValue {
    String(SharedEntry),  // TTL on Entry
    Hash(SharedHash),
    List(SharedList),
    Set(SharedSet),
    ZSet(SharedSortedSet),
    Geo(SharedGeoSet),    // TYPE → "zset"
    Stream(SharedStream),
}

Cache::get_key_value(key) -> Option<KeyValue>   // lazy expire + multi-map probe
Cache::key_type / exists                         // via get_key_value
Cache::delete / remove_key_value_raw             // unified remove + memory free
```

### Target (true single map)

```text
ShardedKeyMap<KeyValue>   // or KeySlot { value: KeyValue, expires_at: Option<Instant> }
```

| Op | On unified map |
|----|----------------|
| **TYPE** | `get` → `KeyValue::key_type()` → Redis TYPE string |
| **WRONGTYPE** | `ensure_type` vs existing variant |
| **DEL / EXISTS** | remove / `is_some` on one map |
| **SCAN / KEYS / DBSIZE / RANDOMKEY** | iterate one key index (type tag optional) |
| **RENAME** | take value + expire meta, insert under new name |
| **TTL / EXPIRE** | string: `Entry`; typed: side map or slot header |
| **Memory / eviction** | per-variant size; sample from unified map |

### Migration plan

1. **FG (done, slice A):** `KeyValue` + facade; storage multi-map; tests for lookup/delete/WRONGTYPE.
2. **FG-2:** Move **one** container (prefer hashes or sets) into a sharded `KeyValue` map or dual-read path; keep facade; full command + load paths green.
3. **FG-3:** Remaining types; collapse `KeyspacePayload` drain/fill to one value stream; eviction samples all types.
4. **FG-4 (optional):** Merge `typed_expires` into slot header.

**Load/install:** keep multi-field payload until FG-3 so LOADING / epoch install stays honest (no half-migrated take/install).

### Residuals (FG-2+)

- Physical single map (not just facade)
- Eviction sampling beyond string KV for allkeys-* (policy-dependent)
- `KeyspacePayload` single-stream serialization
