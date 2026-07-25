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

## 3. Unified keyspace (Batch FG / FG-2)

### Today (partial physical unify + facade)

`Cache` holds containers per Redis type; **hashes** have migrated into the
unified map:

| Field | Type |
|-------|------|
| `map` | strings (`SharedEntry`) |
| `key_values` | `ShardedKeyMap<KeyValue>` — **FG-2: only `KeyValue::Hash`** |
| `sorted_sets` / `geo_sets` | sharded typed maps (legacy) |
| `lists` / `sets` / `streams` | `RwLock<HashMap<…>>` (legacy) |
| `typed_expires` | absolute `Instant` for non-string keys |

Logical invariant: **one name → at most one type** (`ensure_type` / WRONGTYPE).

**FG slice A** added the view type and facade; **FG-2** put hash **physical**
storage into `key_values` (no dual-write leftover for hashes):

```text
enum KeyValue {
    String(SharedEntry),  // TTL on Entry
    Hash(SharedHash),     // physically in key_values (FG-2)
    List(SharedList),
    Set(SharedSet),
    ZSet(SharedSortedSet),
    Geo(SharedGeoSet),    // TYPE → "zset"
    Stream(SharedStream),
}

Cache::get_key_value(key) -> Option<KeyValue>   // string → key_values → legacy maps
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
2. **FG-2 (done, hashes):** Physical hashes in `ShardedKeyMap<KeyValue>`; facade + H* + RENAME + take/install + eviction sampling; no dual-write for hashes.
3. **FG-3:** Remaining types; collapse `KeyspacePayload` drain/fill to one value stream; eviction samples all types from one map.
4. **FG-4 (optional):** Merge `typed_expires` into slot header.

**Load/install:** still multi-field payload (`hashes: HashMap` extracted from /
re-wrapped into `KeyValue::Hash`) until FG-3 so LOADING / epoch install stays honest.

### Residuals (FG-3+)

- Migrate list / set / zset / geo / stream (and eventually strings) into `key_values`
- `KeyspacePayload` single-stream serialization
- Eviction sampling fully from one map