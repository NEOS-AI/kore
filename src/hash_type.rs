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

    /// Get-and-delete fields in order (HGETDEL). Missing fields yield `None`.
    pub fn hgetdel(&mut self, fields: &[Bytes]) -> Vec<Option<Bytes>> {
        fields.iter().map(|f| self.fields.remove(f)).collect()
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

    /// Increment field by float `delta`. Creates field at 0.0 if missing.
    /// Returns the new value, or an error if the stored value is not a float / NaN.
    pub fn hincrbyfloat(&mut self, field: Bytes, delta: f64) -> Result<f64, String> {
        let current = match self.fields.get(&field) {
            Some(v) => {
                let s = std::str::from_utf8(v)
                    .map_err(|_| "hash value is not a valid float".to_string())?;
                s.trim()
                    .parse::<f64>()
                    .map_err(|_| "hash value is not a valid float".to_string())?
            }
            None => 0.0,
        };
        if current.is_nan() {
            return Err("hash value is not a valid float".into());
        }
        let new_val = current + delta;
        if new_val.is_nan() {
            return Err("increment would produce NaN".into());
        }
        self.fields
            .insert(field, Bytes::from(format_hash_float(new_val)));
        Ok(new_val)
    }

    /// Byte length of the string value at `field`, or 0 if missing.
    pub fn hstrlen(&self, field: &Bytes) -> usize {
        self.fields.get(field).map(|v| v.len()).unwrap_or(0)
    }

    /// Random fields without removal (Redis HRANDFIELD count semantics).
    ///
    /// * `count > 0`: up to `count` distinct fields
    /// * `count < 0`: `|count|` fields with replacement
    /// * `count == 0`: empty
    pub fn hrandfield(&self, count: i64) -> Vec<(Bytes, Bytes)> {
        use rand::seq::{IteratorRandom, SliceRandom};
        if self.fields.is_empty() || count == 0 {
            return Vec::new();
        }
        let mut rng = rand::thread_rng();
        if count > 0 {
            let n = (count as usize).min(self.fields.len());
            self.fields
                .iter()
                .choose_multiple(&mut rng, n)
                .into_iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        } else {
            let n = (-count) as usize;
            let pool: Vec<(&Bytes, &Bytes)> = self.fields.iter().map(|(k, v)| (k, v)).collect();
            (0..n)
                .map(|_| {
                    let (k, v) = *pool.choose(&mut rng).unwrap();
                    (k.clone(), v.clone())
                })
                .collect()
        }
    }

    /// Single random field/value pair, if any.
    pub fn hrandfield_one(&self) -> Option<(Bytes, Bytes)> {
        use rand::seq::IteratorRandom;
        let mut rng = rand::thread_rng();
        self.fields
            .iter()
            .choose(&mut rng)
            .map(|(k, v)| (k.clone(), v.clone()))
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

/// Redis-ish float rendering for HINCRBYFLOAT stored values / replies.
fn format_hash_float(v: f64) -> String {
    if v.fract() == 0.0 && v.is_finite() && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{}", v)
    }
}

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

    #[test]
    fn test_hincrbyfloat_and_hstrlen() {
        let mut h = RedisHash::new();
        assert_eq!(h.hincrbyfloat(Bytes::from("f"), 1.5).unwrap(), 1.5);
        assert_eq!(h.hincrbyfloat(Bytes::from("f"), 0.5).unwrap(), 2.0);
        assert_eq!(h.hget(&Bytes::from("f")), Some(Bytes::from("2")));
        assert_eq!(h.hstrlen(&Bytes::from("f")), 1);
        assert_eq!(h.hstrlen(&Bytes::from("missing")), 0);
        h.hset(Bytes::from("s"), Bytes::from("hello"));
        assert_eq!(h.hstrlen(&Bytes::from("s")), 5);
        assert!(h.hincrbyfloat(Bytes::from("s"), 1.0).is_err());
    }

    #[test]
    fn test_hrandfield() {
        let mut h = RedisHash::new();
        h.hset(Bytes::from("a"), Bytes::from("1"));
        h.hset(Bytes::from("b"), Bytes::from("2"));
        h.hset(Bytes::from("c"), Bytes::from("3"));
        assert!(h.hrandfield_one().is_some());
        let two = h.hrandfield(2);
        assert_eq!(two.len(), 2);
        let with_dup = h.hrandfield(-5);
        assert_eq!(with_dup.len(), 5);
        assert!(h.hrandfield(0).is_empty());
        assert!(h.hrandfield(10).len() <= 3);
    }
}
