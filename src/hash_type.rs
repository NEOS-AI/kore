//! Redis Hash data type: field → value map.

use bytes::Bytes;
use std::collections::HashMap;
use parking_lot::RwLock;
use std::sync::Arc;

/// Redis-compatible Hash: `HSET`/`HGET`/… over field→value pairs.
pub struct RedisHash {
    fields: HashMap<Bytes, Bytes>,
}

impl RedisHash {
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    /// Set field → value. Returns `true` if the field was newly created.
    pub fn hset(&mut self, field: Bytes, value: Bytes) -> bool {
        self.fields.insert(field, value).is_none()
    }

    pub fn hget(&self, field: &Bytes) -> Option<Bytes> {
        self.fields.get(field).cloned()
    }

    pub fn hmget(&self, fields: &[Bytes]) -> Vec<Option<Bytes>> {
        fields.iter().map(|f| self.hget(f)).collect()
    }

    /// Delete fields. Returns number of fields removed.
    pub fn hdel(&mut self, fields: &[Bytes]) -> usize {
        fields
            .iter()
            .filter(|f| self.fields.remove(*f).is_some())
            .count()
    }

    pub fn hgetall(&self) -> Vec<(Bytes, Bytes)> {
        self.fields
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn hlen(&self) -> usize {
        self.fields.len()
    }

    pub fn hexists(&self, field: &Bytes) -> bool {
        self.fields.contains_key(field)
    }

    pub fn hkeys(&self) -> Vec<Bytes> {
        self.fields.keys().cloned().collect()
    }

    pub fn hvals(&self) -> Vec<Bytes> {
        self.fields.values().cloned().collect()
    }

    /// Increment field by `delta` (parsed as i64). Creates field at 0 if missing.
    /// Returns new value or error string if not an integer.
    pub fn hincrby(&mut self, field: Bytes, delta: i64) -> Result<i64, String> {
        let current = match self.fields.get(&field) {
            Some(v) => {
                let s = std::str::from_utf8(v).map_err(|_| "hash value is not an integer".to_string())?;
                s.parse::<i64>()
                    .map_err(|_| "hash value is not an integer".to_string())?
            }
            None => 0,
        };
        let new_val = current
            .checked_add(delta)
            .ok_or_else(|| "increment or decrement would overflow".to_string())?;
        self.fields
            .insert(field, Bytes::from(new_val.to_string()));
        Ok(new_val)
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Iterate field/value pairs (for persistence).
    pub fn iter_fields(&self) -> impl Iterator<Item = (Bytes, Bytes)> + '_ {
        self.fields.iter().map(|(k, v)| (k.clone(), v.clone()))
    }

    /// Approximate heap size of hash contents (fields only; key is charged separately).
    pub fn memory_size(&self) -> usize {
        use crate::memory::{estimate_hash_field, with_alloc_overhead};
        let mut raw = std::mem::size_of::<Self>();
        // Empty HashMap / capacity overhead
        raw += self.fields.capacity().saturating_mul(8);
        let fields: usize = self
            .fields
            .iter()
            .map(|(k, v)| estimate_hash_field(k.len(), v.len()))
            .sum();
        with_alloc_overhead(raw).saturating_add(fields)
    }
}

impl Default for RedisHash {
    fn default() -> Self {
        Self::new()
    }
}

pub type SharedHash = Arc<RwLock<RedisHash>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_basic() {
        let mut h = RedisHash::new();
        assert!(h.hset(Bytes::from("f1"), Bytes::from("v1")));
        assert!(!h.hset(Bytes::from("f1"), Bytes::from("v2")));
        assert_eq!(h.hget(&Bytes::from("f1")), Some(Bytes::from("v2")));
        assert_eq!(h.hlen(), 1);
        assert!(h.hexists(&Bytes::from("f1")));
        assert_eq!(h.hdel(&[Bytes::from("f1")]), 1);
        assert!(h.is_empty());
    }

    #[test]
    fn test_hincrby() {
        let mut h = RedisHash::new();
        assert_eq!(h.hincrby(Bytes::from("n"), 5).unwrap(), 5);
        assert_eq!(h.hincrby(Bytes::from("n"), -2).unwrap(), 3);
    }
}
