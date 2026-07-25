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

### Today (FG-3: typed containers unified)

`Cache` holds:

| Field | Type |
|-------|------|
| `map` | strings (`SharedEntry`) — residual separate map |
| `key_values` | `ShardedKeyMap<KeyValue>` — **Hash / List / Set / ZSet / Geo / Stream** |
| `typed_expires` | absolute `Instant` for non-string keys |
| `list_blockers` / `stream_blockers` | blocking waiters (not key storage) |

Logical invariant: **one name → at most one type** (`ensure_type` / WRONGTYPE).

```text
enum KeyValue {
    String(SharedEntry),  // view over Cache::map only (not stored in key_values)
    Hash(SharedHash),     // physically in key_values
    List(SharedList),
    Set(SharedSet),
    ZSet(SharedSortedSet),
    Geo(SharedGeoSet),    // TYPE → "zset"
    Stream(SharedStream),
}

Cache::get_key_value(key) -> Option<KeyValue>   // string map → key_values
Cache::key_type / exists                         // via get_key_value
Cache::delete / remove_key_value_raw             // unified remove + memory free
```

### Target (true single map — optional FG-4)

```text
ShardedKeyMap<KeyValue>   // include String; or KeySlot { value, expires_at }
```

| Op | On unified map |
|----|----------------|
| **TYPE** | `get` → `KeyValue::key_type()` → Redis TYPE string |
| **WRONGTYPE** | `ensure_type` vs existing variant |
| **DEL / EXISTS** | remove / `is_some` on one map |
| **SCAN / KEYS / DBSIZE / RANDOMKEY** | iterate string map + `key_values` (today) |
| **RENAME** | take value + expire meta, insert under new name |
| **TTL / EXPIRE** | string: `Entry`; typed: side map or slot header |
| **Memory / eviction** | per-variant size; typed victims sampled from `key_values` |

### Migration plan

1. **FG (done, slice A):** `KeyValue` + facade; storage multi-map.
2. **FG-2 (done, hashes):** Physical hashes in `ShardedKeyMap<KeyValue>`.
3. **FG-3 (done):** list/set/zset/geo/stream into `key_values`; legacy per-type
   maps removed; `KeyspacePayload` drains `map` + `key_values` streams; eviction
   samples typed keys from one map. **No dual-write leftover** for migrated types.
4. **FG-4 (optional):** Merge strings into `key_values`; fold `typed_expires`
   into slot header; search-doc eviction remains special.

**Load/install (`KeyspacePayload`):** `map: Vec<(Bytes, SharedEntry)>` +
`key_values: Vec<(Bytes, KeyValue)>` + expires / WATCH / search / memory counters.
Epoch install semantics (DR/DS) unchanged.