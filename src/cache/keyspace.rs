//! Unified keyspace facade + physical `KeySlot` map (Batch FG / FG-2 / FG-3 / FG-4 / FP).
//!
//! # Design target
//!
//! Redis presents a single name → typed value namespace. Kore stores every type
//! in one sharded map of name → [`KeySlot`]:
//!
//! ```text
//! struct KeySlot {
//!     expires_at: Option<Instant>,  // typed-key TTL (Batch FP)
//!     value: KeyValue,
//! }
//!
//! enum KeyValue {
//!     String(SharedEntry),   // TTL on Entry (not slot.expires_at)
//!     Hash(SharedHash),
//!     List(SharedList),
//!     Set(SharedSet),
//!     ZSet(SharedSortedSet),
//!     Geo(SharedGeoSet),     // TYPE reports "zset"
//!     Stream(SharedStream),
//! }
//! ```
//!
//! Batch **FP** folds the former side `typed_expires` map into
//! [`KeySlot::expires_at`] so TTL travels with the key through RENAME,
//! take/install, and LOADING replace.
//!
//! # How cross-type ops work on the unified map
//!
//! | Op | Behavior |
//! |----|----------|
//! | **TYPE** | `get_key_value(k).map(|v| v.key_type())` → Redis TYPE string (`geo` → `zset`) |
//! | **WRONGTYPE** | `ensure_type(k, expected)` compares `key_type` vs expected |
//! | **DEL / UNLINK** | `remove_key_value_raw` frees memory for that variant; expire is on the slot |
//! | **EXISTS** | `get_key_value(k).is_some()` after lazy expire |
//! | **SCAN / KEYS / DBSIZE / RANDOMKEY** | Iterate `key_values` (single map) |
//! | **RENAME** | Atomic take of `KeySlot` (value + expire), insert under new name |
//! | **TTL / EXPIRE** | String: `Entry.expires_at`; typed: `KeySlot.expires_at` |
//! | **Memory / eviction** | Per-variant size estimate; eviction samples all keys from `key_values` |
//!
//! # Migration plan
//!
//! 1. **FG (slice A, done):** Introduce [`KeyValue`] + facade.
//! 2. **FG-2 (done):** Physical **hashes** in [`Cache::key_values`].
//! 3. **FG-3 (done):** Physical **list / set / zset / geo / stream** in
//!    `key_values`; legacy per-type maps removed.
//! 4. **FG-4 (done):** Merge **strings** into `key_values` as
//!    [`KeyValue::String`]; collapse [`KeyspacePayload`] to one stream.
//! 5. **FP (this batch):** Fold typed TTL into [`KeySlot::expires_at`]; remove
//!    side `typed_expires` map. Strings still keep TTL on `Entry`.
//!
//! # Invariants preserved
//!
//! - At most one type per name (enforced by `ensure_type` on creates).
//! - Lazy + active expire for typed keys; string expire on `Entry`.
//! - MemoryTracker categories unchanged per type.
//! - Geo TYPE string remains `"zset"`.
//! - LOADING / epoch install: expire is part of each slot in the payload.

use crate::entry::SharedEntry;
use crate::hash_type::SharedHash;
use crate::list_type::SharedList;
use crate::memory::MemoryCategory;
use crate::set_type::SharedSet;
use crate::sorted_set::SharedSortedSet;
use crate::stream_type::SharedStream;
use bytes::Bytes;
use std::sync::atomic::Ordering;
use std::time::Instant;

use super::geo_sets::SharedGeoSet;
use super::storage::KeyType;
use super::Cache;

/// Per-key slot in the unified keyspace map (Batch FP).
///
/// Holds the typed value plus optional absolute Instant expiry for non-string
/// keys. Strings always have [`Self::expires_at`] = `None`; their TTL lives on
/// [`crate::entry::Entry::expires_at`].
#[derive(Clone)]
pub struct KeySlot {
    /// Absolute Instant expiry for typed (non-string) keys.
    pub expires_at: Option<Instant>,
    /// The Redis-typed value at this name.
    pub value: KeyValue,
}

impl KeySlot {
    /// New slot with no typed expire (string or freshly created typed key).
    #[inline]
    pub fn new(value: KeyValue) -> Self {
        Self {
            expires_at: None,
            value,
        }
    }

    /// Slot for a string entry (typed expire always cleared).
    #[inline]
    pub fn string(entry: SharedEntry) -> Self {
        Self {
            expires_at: None,
            value: KeyValue::String(entry),
        }
    }

    /// Slot with an explicit typed expire (ignored / cleared for strings).
    #[inline]
    pub fn with_expire(value: KeyValue, expires_at: Option<Instant>) -> Self {
        let expires_at = if matches!(value, KeyValue::String(_)) {
            None
        } else {
            expires_at
        };
        Self { expires_at, value }
    }

    #[inline]
    pub fn key_type(&self) -> KeyType {
        self.value.key_type()
    }

    #[inline]
    pub fn is_typed_container(&self) -> bool {
        self.value.is_typed_container()
    }
}

/// Typed value for one key name in the logical Redis keyspace.
///
/// **Storage (FG-4 / FP):** every type — including strings — is **physically**
/// stored in [`Cache::key_values`] as [`KeySlot`] `{ expires_at, value: KeyValue }`.
/// Dropping a cloned Arc does not remove the key; use [`Cache::delete`] /
/// type-specific `remove_*`.
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

    /// MemoryTracker category for this variant (strings use Cache).
    #[inline]
    pub fn memory_category(&self) -> MemoryCategory {
        match self {
            KeyValue::String(_) => MemoryCategory::Cache,
            KeyValue::Hash(_) => MemoryCategory::Hashes,
            KeyValue::List(_) => MemoryCategory::Lists,
            KeyValue::Set(_) => MemoryCategory::Sets,
            KeyValue::ZSet(_) => MemoryCategory::SortedSets,
            KeyValue::Geo(_) => MemoryCategory::GeoSets,
            KeyValue::Stream(_) => MemoryCategory::Streams,
        }
    }

    /// Content memory size (container only; caller adds key via `estimate_keyed_object`).
    #[inline]
    pub fn content_memory_size(&self) -> usize {
        match self {
            KeyValue::String(e) => e.size(),
            KeyValue::Hash(h) => h.read().memory_size(),
            KeyValue::List(l) => l.read().memory_size(),
            KeyValue::Set(s) => s.read().memory_size(),
            KeyValue::ZSet(z) => z.read().memory_size(),
            KeyValue::Geo(g) => g.read().memory_usage(),
            KeyValue::Stream(s) => s.read().memory_size(),
        }
    }

    /// Whether this variant is a non-string typed value that lives in `key_values`.
    #[inline]
    pub fn is_typed_container(&self) -> bool {
        !matches!(self, KeyValue::String(_))
    }
}

impl Cache {
    /// Resolve `key` to a [`KeyValue`] view, applying lazy expire.
    ///
    /// Returns `None` if the name is absent or its TTL has elapsed (and the
    /// key was purged). Used by TYPE / EXISTS / `key_type` and as the stable
    /// cross-type lookup API.
    ///
    /// Single map: purge typed expire when due, then probe [`Self::key_values`].
    /// Expired strings are treated as absent (physical cleanup by load/sweep).
    pub fn get_key_value(&self, key: &Bytes) -> Option<KeyValue> {
        // Typed TTL may be past due even when the value is still present.
        if self.purge_typed_if_expired(key) {
            return None;
        }

        match self.key_values.get(key) {
            Some(KeySlot {
                value: KeyValue::String(entry),
                ..
            }) if entry.is_expired() => None,
            Some(slot) => Some(slot.value),
            None => None,
        }
    }

    /// Borrow the string entry when `key` is a live (non-expired) string.
    pub(super) fn get_string_entry(&self, key: &Bytes) -> Option<SharedEntry> {
        match self.key_values.get(key) {
            Some(KeySlot {
                value: KeyValue::String(e),
                ..
            }) if !e.is_expired() => Some(e),
            _ => None,
        }
    }

    /// Remove any key type (slot + value). Expire metadata lives on the slot
    /// and is dropped with the remove (Batch FP).
    ///
    /// Memory accounting matches the historical per-type `remove_*` paths.
    /// Callers that need full DEL semantics should use [`Self::delete`].
    ///
    /// Returns `true` if a value was removed.
    pub(crate) fn remove_key_value_raw(&self, key: &Bytes) -> bool {
        // FG-4: all types (including strings) live in key_values.
        if let Some(slot) = self.key_values.remove(key) {
            match &slot.value {
                KeyValue::String(entry) => {
                    let size = entry.size();
                    self.memory_usage.fetch_sub(size, Ordering::Relaxed);
                    self.memory_tracker.deallocate(size, MemoryCategory::Cache);
                }
                _ => {
                    let size = crate::memory::estimate_keyed_object(
                        key.len(),
                        slot.value.content_memory_size(),
                    );
                    self.memory_tracker
                        .deallocate(size, slot.value.memory_category());
                }
            }
            return true;
        }
        false
    }

    /// Re-account key-length change when renaming a typed value in place.
    pub(crate) fn account_typed_key_rename(
        &self,
        src: &Bytes,
        dst: &Bytes,
        content: usize,
        category: MemoryCategory,
    ) {
        if src.len() == dst.len() {
            return;
        }
        let old = crate::memory::estimate_keyed_object(src.len(), content);
        let new = crate::memory::estimate_keyed_object(dst.len(), content);
        self.memory_tracker.deallocate(old, category);
        self.memory_tracker.account(new, category);
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

    /// FG-4: all types (including strings) are physically in `key_values`.
    #[test]
    fn all_types_live_in_unified_key_values() {
        let c = cache();
        let _ = c.get_or_create_hash(&b("h")).unwrap();
        let _ = c.get_or_create_list(&b("l")).unwrap();
        let _ = c.get_or_create_set(&b("s")).unwrap();
        let _ = c.get_or_create_sorted_set(&b("z")).unwrap();
        let _ = c.get_or_create_geo_set(&b("g")).unwrap();
        let _ = c.get_or_create_stream(&b("st")).unwrap();

        assert_eq!(c.key_values.len(), 6);
        assert!(matches!(
            c.key_values.get(&b("h")),
            Some(KeySlot {
                value: KeyValue::Hash(_),
                ..
            })
        ));
        assert!(matches!(
            c.key_values.get(&b("l")),
            Some(KeySlot {
                value: KeyValue::List(_),
                ..
            })
        ));
        assert!(matches!(
            c.key_values.get(&b("s")),
            Some(KeySlot {
                value: KeyValue::Set(_),
                ..
            })
        ));
        assert!(matches!(
            c.key_values.get(&b("z")),
            Some(KeySlot {
                value: KeyValue::ZSet(_),
                ..
            })
        ));
        assert!(matches!(
            c.key_values.get(&b("g")),
            Some(KeySlot {
                value: KeyValue::Geo(_),
                ..
            })
        ));
        assert!(matches!(
            c.key_values.get(&b("st")),
            Some(KeySlot {
                value: KeyValue::Stream(_),
                ..
            })
        ));

        // WRONGTYPE across types in the single map
        assert!(matches!(
            c.get_or_create_hash(&b("l")),
            Err(crate::error::Error::WrongType)
        ));
        assert!(matches!(
            c.get_or_create_set(&b("z")),
            Err(crate::error::Error::WrongType)
        ));

        // FG-4: strings live in key_values as KeyValue::String
        c.store(b("str"), Bytes::from_static(b"x"), store_opts())
            .unwrap();
        assert!(matches!(
            c.key_values.get(&b("str")),
            Some(KeySlot {
                value: KeyValue::String(_),
                ..
            })
        ));
        assert_eq!(c.dbsize(), 7);
        assert_eq!(c.key_values.len(), 7);
    }

    #[test]
    fn strings_live_in_unified_key_values() {
        let c = cache();
        c.store(b("s"), Bytes::from_static(b"v"), store_opts())
            .unwrap();
        assert_eq!(c.key_values.len(), 1);
        match c.key_values.get(&b("s")) {
            Some(KeySlot {
                value: KeyValue::String(e),
                ..
            }) => {
                assert_eq!(e.value.as_ref(), b"v");
            }
            other => panic!(
                "expected String in key_values, got {:?}",
                other.map(|s| s.key_type())
            ),
        }
        // WRONGTYPE: cannot create hash over string
        assert!(matches!(
            c.get_or_create_hash(&b("s")),
            Err(crate::error::Error::WrongType)
        ));
        // Cross-type DEL
        assert!(c.delete(&b("s")).unwrap());
        assert_eq!(c.key_values.len(), 0);
    }

    #[test]
    fn typed_rename_moves_within_key_values() {
        let c = cache();
        let h = c.get_or_create_hash(&b("src")).unwrap();
        h.write()
            .hset(Bytes::from_static(b"a"), Bytes::from_static(b"1"));
        assert!(c.rename(&b("src"), &b("dst"), false).unwrap());
        assert!(c.get_hash(&b("src")).is_none());
        assert!(c.get_hash(&b("dst")).is_some());
        assert_eq!(c.key_type(&b("dst")), KeyType::Hash);
        assert_eq!(c.key_values.len(), 1);

        let set = c.get_or_create_set(&b("s1")).unwrap();
        set.write().sadd(std::iter::once(b("m")));
        assert!(c.rename(&b("s1"), &b("s2"), false).unwrap());
        assert!(c.get_set(&b("s1")).is_none());
        assert!(c.get_set(&b("s2")).is_some());
        assert_eq!(c.key_values.len(), 2);
    }

    #[test]
    fn take_install_payload_roundtrip_all_types() {
        let c = cache();
        c.store(b("str1"), Bytes::from_static(b"hello"), store_opts())
            .unwrap();
        let h = c.get_or_create_hash(&b("h1")).unwrap();
        h.write()
            .hset(Bytes::from_static(b"f"), Bytes::from_static(b"1"));
        let l = c.get_or_create_list(&b("l1")).unwrap();
        l.write()
            .rpush(std::iter::once(Bytes::from_static(b"x")));
        let s = c.get_or_create_set(&b("set1")).unwrap();
        s.write().sadd(std::iter::once(b("m")));
        let z = c.get_or_create_sorted_set(&b("z1")).unwrap();
        z.write().add(b("zm"), 2.0);
        let g = c.get_or_create_geo_set(&b("g1")).unwrap();
        let _ = g.write().add(b("gm"), 13.0, 52.0);
        let _st = c.get_or_create_stream(&b("st1")).unwrap();

        let before_hash = c.category_memory(MemoryCategory::Hashes);
        assert!(before_hash > 0);
        assert_eq!(c.key_values.len(), 7);

        let payload = c.take_keyspace_payload();
        assert_eq!(c.key_values.len(), 0);
        assert!(c.get_string_entry(&b("str1")).is_none());
        assert!(c.get_hash(&b("h1")).is_none());
        assert!(c.get_list(&b("l1")).is_none());
        assert!(c.get_set(&b("set1")).is_none());

        c.install_keyspace_payload(payload);
        assert_eq!(c.key_type(&b("str1")), KeyType::String);
        assert_eq!(c.key_type(&b("h1")), KeyType::Hash);
        assert_eq!(c.key_type(&b("l1")), KeyType::List);
        assert_eq!(c.key_type(&b("set1")), KeyType::Set);
        assert_eq!(c.key_type(&b("z1")), KeyType::ZSet);
        assert_eq!(c.key_type(&b("g1")), KeyType::Geo);
        assert_eq!(c.key_type(&b("st1")), KeyType::Stream);
        assert_eq!(c.key_values.len(), 7);
        assert_eq!(c.category_memory(MemoryCategory::Hashes), before_hash);
        assert_eq!(
            c.get_string_entry(&b("str1")).unwrap().value.as_ref(),
            b"hello"
        );
    }

    #[test]
    fn hash_lives_in_unified_key_values() {
        let c = cache();
        let h = c.get_or_create_hash(&b("myhash")).unwrap();
        h.write()
            .hset(Bytes::from_static(b"f"), Bytes::from_static(b"v"));

        assert_eq!(c.key_values.len(), 1);
        assert!(c.hash_exists(&b("myhash")));
        match c.key_values.get(&b("myhash")) {
            Some(KeySlot {
                value: KeyValue::Hash(shared),
                ..
            }) => {
                assert_eq!(
                    shared.read().hget(&Bytes::from_static(b"f")),
                    Some(Bytes::from_static(b"v"))
                );
            }
            other => panic!(
                "expected Hash in key_values, got {:?}",
                other.map(|s| s.key_type())
            ),
        }

        assert!(c.remove_hash(&b("myhash")));
        assert_eq!(c.key_values.len(), 0);
        assert!(c.get_hash(&b("myhash")).is_none());
        assert_eq!(c.key_type(&b("myhash")), KeyType::None);
    }

    /// Batch FP: typed TTL rides on KeySlot through take/install (LOADING path).
    #[test]
    fn typed_expire_on_slot_survives_take_install() {
        let c = cache();
        let _ = c.get_or_create_hash(&b("h")).unwrap();
        c.expire(&b("h"), 60_000).unwrap();
        let ttl_before = c.ttl(&b("h"));
        assert!(ttl_before > 50_000, "ttl_before={ttl_before}");

        let payload = c.take_keyspace_payload();
        assert_eq!(c.key_values.len(), 0);
        assert_eq!(c.ttl(&b("h")), -2);

        c.install_keyspace_payload(payload);
        assert_eq!(c.key_type(&b("h")), KeyType::Hash);
        let ttl_after = c.ttl(&b("h"));
        assert!(ttl_after > 50_000, "ttl_after={ttl_after}");
        // Expire is on the slot, not a side map.
        match c.key_values.get(&b("h")) {
            Some(KeySlot {
                expires_at: Some(_),
                value: KeyValue::Hash(_),
            }) => {}
            other => panic!("expected hash slot with expire, got {:?}", other.map(|s| s.expires_at.is_some())),
        }
    }
}
