//! Unified keyspace facade + physical `KeyValue` map (Batch FG / FG-2).
//!
//! # Design target
//!
//! Redis presents a single name → typed value namespace. Kore historically stores
//! types in separate maps (`map` for strings, `sorted_sets`, `geo_sets`,
//! hashes/lists/sets/streams) plus a side `typed_expires` table. That multi-map
//! model already provides TYPE / WRONGTYPE / cross-type DEL and SCAN, but every
//! cross-type op reimplements “walk the maps.”
//!
//! The long-term shape is one sharded map of name → [`KeyValue`]:
//!
//! ```text
//! enum KeyValue {
//!     String(SharedEntry),   // TTL on Entry
//!     Hash(SharedHash),
//!     List(SharedList),
//!     Set(SharedSet),
//!     ZSet(SharedSortedSet),
//!     Geo(SharedGeoSet),     // TYPE reports "zset"
//!     Stream(SharedStream),
//! }
//! ```
//!
//! Typed (non-string) absolute expiry may stay a side map for a while, or move
//! onto a thin header wrapping `KeyValue` (`struct KeySlot { value, expires_at }`).
//!
//! # How cross-type ops work on the unified map
//!
//! | Op | Behavior |
//! |----|----------|
//! | **TYPE** | `get_key_value(k).map(|v| v.key_type())` → Redis TYPE string (`geo` → `zset`) |
//! | **WRONGTYPE** | `ensure_type(k, expected)` compares `key_type` vs expected |
//! | **DEL / UNLINK** | `remove_key_value_raw` frees memory for that variant, clears expire, search index, WATCH |
//! | **EXISTS** | `get_key_value(k).is_some()` after lazy expire |
//! | **SCAN / KEYS / DBSIZE / RANDOMKEY** | Iterate the single map (or today’s multi-map index that merges names); type tags optional for TYPE filter |
//! | **RENAME** | Atomic take of `KeyValue` + expire metadata, insert under new name (overwrite dest) |
//! | **TTL / EXPIRE** | String: `Entry.expires_at`; typed: `typed_expires` (or future slot header) |
//! | **Memory / eviction** | Per-variant size estimate; eviction samples keys from the unified map with policy filters |
//!
//! # Migration plan
//!
//! 1. **FG (slice A, done):** Introduce [`KeyValue`] + facade
//!    ([`Cache::get_key_value`], [`Cache::remove_key_value_raw`], `key_type` /
//!    `exists` / `delete` routed through it). Storage multi-map.
//! 2. **FG-2 (this batch, done for hashes):** Physical storage for **hashes**
//!    is [`Cache::key_values`] (`ShardedKeyMap<KeyValue>` holding only
//!    [`KeyValue::Hash`]). No dual-write leftover for hashes. Legacy global
//!    `hashes` map removed. Facade probes `key_values` before remaining typed
//!    maps.
//! 3. **FG-3:** Migrate remaining types into `key_values`; collapse
//!    `KeyspacePayload` to one drain/fill; eviction samples all types from
//!    one map.
//! 4. **FG-4:** Optional: merge `typed_expires` into slot header; drop type
//!    registry walks entirely.
//!
//! Load/install (`take_keyspace_payload` / `install_keyspace_payload`) still
//! multi-field: hashes are extracted/re-wrapped as `KeyValue::Hash` so
//! RDB/AOF/LOADING semantics stay honest until FG-3.
//!
//! # Invariants preserved
//!
//! - At most one type per name (enforced by `ensure_type` on creates).
//! - Lazy + active expire for typed keys; string expire on `Entry`.
//! - MemoryTracker categories unchanged (`MemoryCategory::Hashes` still used).
//! - Geo TYPE string remains `"zset"`.

use crate::entry::SharedEntry;
use crate::hash_type::SharedHash;
use crate::list_type::SharedList;
use crate::memory::MemoryCategory;
use crate::set_type::SharedSet;
use crate::sorted_set::SharedSortedSet;
use crate::stream_type::SharedStream;
use bytes::Bytes;
use std::sync::atomic::Ordering;

use super::geo_sets::SharedGeoSet;
use super::storage::KeyType;
use super::Cache;

/// Typed value for one key name in the logical Redis keyspace.
///
/// **Storage (FG-2):** hashes are **physically** stored as [`KeyValue::Hash`]
/// in [`Cache::key_values`]. Other variants are still *views* over legacy maps
/// (Arc clones); dropping a view does not remove those keys.
///
/// **Future (FG-3+):** remaining types move into `key_values` as well (or a
/// slot header wrapping this enum).
#[derive(Clone)]
pub enum KeyValue {
    String(SharedEntry),
    Hash(SharedHash),
    List(SharedList),
    Set(SharedSet),
    ZSet(SharedSortedSet),
    Geo(SharedGeoSet),
    Stream(SharedStream),
}

impl KeyValue {
    /// Redis / Kore [`KeyType`] for this value.
    #[inline]
    pub fn key_type(&self) -> KeyType {
        match self {
            KeyValue::String(_) => KeyType::String,
            KeyValue::Hash(_) => KeyType::Hash,
            KeyValue::List(_) => KeyType::List,
            KeyValue::Set(_) => KeyType::Set,
            KeyValue::ZSet(_) => KeyType::ZSet,
            KeyValue::Geo(_) => KeyType::Geo,
            KeyValue::Stream(_) => KeyType::Stream,
        }
    }

    /// Redis `TYPE` command string (`geo` reports as `"zset"`).
    #[inline]
    pub fn as_redis_type_str(&self) -> &'static str {
        self.key_type().as_redis_str()
    }

    /// Borrow the hash container when this is [`KeyValue::Hash`].
    #[inline]
    pub fn as_hash(&self) -> Option<&SharedHash> {
        match self {
            KeyValue::Hash(h) => Some(h),
            _ => None,
        }
    }
}

impl Cache {
    /// Resolve `key` to a [`KeyValue`] view, applying lazy expire.
    ///
    /// Returns `None` if the name is absent or its TTL has elapsed (and the
    /// key was purged). Used by TYPE / EXISTS / `key_type` and as the stable
    /// cross-type lookup API.
    ///
    /// Probe order: string map → typed expire purge → **`key_values` (hashes)**
    /// → remaining legacy typed maps.
    pub fn get_key_value(&self, key: &Bytes) -> Option<KeyValue> {
        // Strings: expire is on Entry (lazy delete path lives in load/mutate).
        if let Some(entry) = self.map.get(key) {
            if !entry.is_expired() {
                return Some(KeyValue::String(entry));
            }
            // Expired string: treat as absent for type resolution. Physical
            // cleanup is handled by load/sweep paths (same as historical
            // key_type).
        }

        // Typed keys: purge past-due TTL then probe type maps.
        if self.purge_typed_if_expired(key) {
            return None;
        }

        // FG-2: hashes live in the unified map.
        if let Some(kv) = self.key_values.get(key) {
            return Some(kv);
        }

        if let Some(z) = self.sorted_sets.get(key) {
            return Some(KeyValue::ZSet(z));
        }
        if let Some(g) = self.geo_sets.get(key) {
            return Some(KeyValue::Geo(g));
        }
        if let Some(l) = self.lists.read().get(key).cloned() {
            return Some(KeyValue::List(l));
        }
        if let Some(s) = self.sets.read().get(key).cloned() {
            return Some(KeyValue::Set(s));
        }
        if let Some(st) = self.streams.read().get(key).cloned() {
            return Some(KeyValue::Stream(st));
        }
        None
    }

    /// Remove any key type without clearing `typed_expires` or search indices.
    ///
    /// Memory accounting matches the historical per-type `remove_*` paths.
    /// Callers that need full DEL semantics should use [`Self::delete`].
    ///
    /// Returns `true` if a value was removed.
    pub(crate) fn remove_key_value_raw(&self, key: &Bytes) -> bool {
        if let Some(entry) = self.map.remove(key) {
            let size = entry.size();
            self.memory_usage.fetch_sub(size, Ordering::Relaxed);
            self.memory_tracker.deallocate(size, MemoryCategory::Cache);
            return true;
        }
        // FG-2 unified map (hashes only today). Prefer remove_hash so accounting
        // stays in one place; it only removes KeyValue::Hash.
        if self.remove_hash(key) {
            return true;
        }
        if self.remove_sorted_set(key) {
            return true;
        }
        if self.remove_geo_set(key) {
            return true;
        }
        if self.remove_list(key) {
            return true;
        }
        if self.remove_set(key) {
            return true;
        }
        if self.remove_stream(key) {
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::StoreOptions;
    use std::sync::Arc;
    use std::time::Duration;

    fn cache() -> Arc<Cache> {
        Cache::new_with_sweep(8, 64 * 1024 * 1024, 16 * 1024 * 1024, false)
    }

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    fn store_opts() -> StoreOptions {
        StoreOptions {
            nx: false,
            xx: false,
            get: false,
            ttl_ms: None,
            exat_ms: None,
            flags: 0,
            keepttl: false,
            cas: None,
        }
    }

    #[test]
    fn get_key_value_string_hash_set_list() {
        let c = cache();
        c.store(b("s"), Bytes::from_static(b"v"), store_opts())
            .unwrap();
        assert_eq!(
            c.get_key_value(&b("s")).map(|v| v.key_type()),
            Some(KeyType::String)
        );
        assert_eq!(c.key_type(&b("s")), KeyType::String);
        assert!(c.exists(&b("s")));

        let h = c.get_or_create_hash(&b("h")).unwrap();
        h.write().hset(b("f"), Bytes::from_static(b"1"));
        assert_eq!(
            c.get_key_value(&b("h")).map(|v| v.key_type()),
            Some(KeyType::Hash)
        );
        assert_eq!(
            c.get_key_value(&b("h")).unwrap().as_redis_type_str(),
            "hash"
        );

        let set = c.get_or_create_set(&b("set")).unwrap();
        set.write().sadd(std::iter::once(b("m")));
        assert_eq!(c.key_type(&b("set")), KeyType::Set);

        let list = c.get_or_create_list(&b("list")).unwrap();
        list.write()
            .rpush(std::iter::once(Bytes::from_static(b"x")));
        assert_eq!(c.key_type(&b("list")), KeyType::List);
    }

    #[test]
    fn get_key_value_zset_geo_stream() {
        let c = cache();
        let z = c.get_or_create_sorted_set(&b("z")).unwrap();
        z.write().add(b("m"), 1.0);
        match c.get_key_value(&b("z")) {
            Some(KeyValue::ZSet(_)) => {}
            other => panic!("expected ZSet, got {:?}", other.map(|v| v.key_type())),
        }
        assert_eq!(
            c.get_key_value(&b("z")).unwrap().as_redis_type_str(),
            "zset"
        );

        let _g = c.get_or_create_geo_set(&b("g")).unwrap();
        assert_eq!(c.key_type(&b("g")), KeyType::Geo);
        assert_eq!(
            c.get_key_value(&b("g")).unwrap().as_redis_type_str(),
            "zset"
        );

        let _st = c.get_or_create_stream(&b("st")).unwrap();
        assert_eq!(c.key_type(&b("st")), KeyType::Stream);
        assert_eq!(
            c.get_key_value(&b("st")).unwrap().as_redis_type_str(),
            "stream"
        );
    }

    #[test]
    fn missing_and_expired_typed_are_none() {
        let c = cache();
        assert!(c.get_key_value(&b("nope")).is_none());
        assert_eq!(c.key_type(&b("nope")), KeyType::None);
        assert!(!c.exists(&b("nope")));

        let _ = c.get_or_create_hash(&b("exp")).unwrap();
        c.expire(&b("exp"), 1).unwrap();
        std::thread::sleep(Duration::from_millis(5));
        // Lazy purge on lookup
        assert!(c.get_key_value(&b("exp")).is_none());
        assert_eq!(c.key_type(&b("exp")), KeyType::None);
    }

    #[test]
    fn delete_uses_unified_remove() {
        let c = cache();
        c.store(b("s"), Bytes::from_static(b"v"), store_opts())
            .unwrap();
        let _ = c.get_or_create_hash(&b("h")).unwrap();
        let _ = c.get_or_create_set(&b("t")).unwrap();
        assert_eq!(c.dbsize(), 3);

        assert!(c.delete(&b("s")).unwrap());
        assert!(c.delete(&b("h")).unwrap());
        assert!(c.delete(&b("t")).unwrap());
        assert!(!c.delete(&b("s")).unwrap());
        assert_eq!(c.dbsize(), 0);
        assert!(c.get_key_value(&b("s")).is_none());
    }

    #[test]
    fn ensure_type_wrongtype_via_facade() {
        let c = cache();
        c.store(b("s"), Bytes::from_static(b"v"), store_opts())
            .unwrap();
        assert!(c.ensure_type(&b("s"), KeyType::String).is_ok());
        assert!(matches!(
            c.ensure_type(&b("s"), KeyType::Hash),
            Err(crate::error::Error::WrongType)
        ));
        assert!(c.ensure_type(&b("missing"), KeyType::List).is_ok());
    }

    /// FG-2: hashes are physically in `key_values`, not a side HashMap.
    #[test]
    fn hash_lives_in_unified_key_values() {
        let c = cache();
        let h = c.get_or_create_hash(&b("myhash")).unwrap();
        h.write()
            .hset(Bytes::from_static(b"f"), Bytes::from_static(b"v"));

        assert_eq!(c.key_values.len(), 1);
        assert!(c.hash_exists(&b("myhash")));
        match c.key_values.get(&b("myhash")) {
            Some(KeyValue::Hash(shared)) => {
                assert_eq!(
                    shared.read().hget(&Bytes::from_static(b"f")),
                    Some(Bytes::from_static(b"v"))
                );
            }
            other => panic!("expected Hash in key_values, got {:?}", other.map(|v| v.key_type())),
        }

        // WRONGTYPE still enforced vs strings / other maps
        c.store(b("s"), Bytes::from_static(b"x"), store_opts())
            .unwrap();
        assert!(matches!(
            c.get_or_create_hash(&b("s")),
            Err(crate::error::Error::WrongType)
        ));

        assert!(c.remove_hash(&b("myhash")));
        assert_eq!(c.key_values.len(), 0);
        assert!(c.get_hash(&b("myhash")).is_none());
        assert_eq!(c.key_type(&b("myhash")), KeyType::None);
    }

    #[test]
    fn hash_rename_moves_within_key_values() {
        let c = cache();
        let h = c.get_or_create_hash(&b("src")).unwrap();
        h.write()
            .hset(Bytes::from_static(b"a"), Bytes::from_static(b"1"));
        assert!(c.rename(&b("src"), &b("dst"), false).unwrap());
        assert!(c.get_hash(&b("src")).is_none());
        assert!(c.get_hash(&b("dst")).is_some());
        assert_eq!(c.key_type(&b("dst")), KeyType::Hash);
        assert_eq!(c.key_values.len(), 1);
    }

    #[test]
    fn hash_take_install_payload_roundtrip() {
        let c = cache();
        let h = c.get_or_create_hash(&b("h1")).unwrap();
        h.write()
            .hset(Bytes::from_static(b"f"), Bytes::from_static(b"1"));
        let before_mem = c.category_memory(MemoryCategory::Hashes);
        assert!(before_mem > 0);

        let payload = c.take_keyspace_payload();
        assert_eq!(c.key_values.len(), 0);
        assert!(c.get_hash(&b("h1")).is_none());

        c.install_keyspace_payload(payload);
        assert_eq!(c.key_type(&b("h1")), KeyType::Hash);
        let restored = c.get_hash(&b("h1")).expect("hash restored");
        assert_eq!(
            restored.read().hget(&Bytes::from_static(b"f")),
            Some(Bytes::from_static(b"1"))
        );
        assert_eq!(c.category_memory(MemoryCategory::Hashes), before_mem);
    }
}
