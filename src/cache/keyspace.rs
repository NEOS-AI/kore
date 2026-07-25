//! Unified keyspace facade (Batch FG, slice A).
//!
//! # Design target
//!
//! Redis presents a single name → typed value namespace. Kore historically stores
//! types in separate maps (`map` for strings, `sorted_sets`, `geo_sets`,
//! `hashes`, `lists`, `sets`, `streams`) plus a side `typed_expires` table.
//! That multi-map model already provides TYPE / WRONGTYPE / cross-type DEL and
//! SCAN, but every cross-type op reimplements “walk the maps.”
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
//! 1. **FG (this batch, slice A):** Introduce [`KeyValue`] + facade
//!    ([`Cache::get_key_value`], [`Cache::remove_key_value_raw`], `key_type` /
//!    `exists` / `delete` routed through it). Storage stays multi-map.
//! 2. **FG-2:** Move one typed container (prefer **hashes** or **sets** — single
//!    `HashMap` today, not already sharded) into a new `ShardedKeyMap<KeyValue>`
//!    *or* co-locate with strings; dual-read facade during transition.
//! 3. **FG-3:** Migrate remaining types; collapse `KeyspacePayload` to one
//!    drain/fill; eviction samples all types from one map.
//! 4. **FG-4:** Optional: merge `typed_expires` into slot header; drop type
//!    registry walks entirely.
//!
//! Load/install (`take_keyspace_payload` / `install_keyspace_payload`) stays
//! multi-field until FG-3 so RDB/AOF/LOADING semantics stay honest.
//!
//! # Invariants preserved
//!
//! - At most one type per name (enforced by `ensure_type` on creates).
//! - Lazy + active expire for typed keys; string expire on `Entry`.
//! - MemoryTracker categories unchanged until storage migrates.
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
/// **Storage (FG slice A):** this is a *view* over the multi-map layout. Holds
/// are `Arc` clones of the live containers; dropping a `KeyValue` does not
/// remove the key.
///
/// **Future (FG-2+):** values may live directly in a single sharded map of this
/// enum (or a header wrapping it).
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
}

impl Cache {
    /// Resolve `key` to a [`KeyValue`] view, applying lazy expire.
    ///
    /// Returns `None` if the name is absent or its TTL has elapsed (and the
    /// key was purged). Used by TYPE / EXISTS / `key_type` and as the stable
    /// cross-type lookup API for future single-map storage.
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

        if let Some(z) = self.sorted_sets.get(key) {
            return Some(KeyValue::ZSet(z));
        }
        if let Some(g) = self.geo_sets.get(key) {
            return Some(KeyValue::Geo(g));
        }
        if let Some(h) = self.hashes.read().get(key).cloned() {
            return Some(KeyValue::Hash(h));
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
        if self.remove_sorted_set(key) {
            return true;
        }
        if self.remove_geo_set(key) {
            return true;
        }
        if self.remove_hash(key) {
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
}
