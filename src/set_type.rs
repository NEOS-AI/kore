//! Redis Set data type: unordered unique members.

use bytes::Bytes;
use std::collections::HashSet;
use parking_lot::RwLock;
use std::sync::Arc;

/// Redis-compatible Set.
pub struct RedisSet {
    members: HashSet<Bytes>,
}

impl RedisSet {
    pub fn new() -> Self {
        Self {
            members: HashSet::new(),
        }
    }

    /// Add members. Returns number of newly added members.
    pub fn sadd(&mut self, members: impl IntoIterator<Item = Bytes>) -> usize {
        let mut added = 0;
        for m in members {
            if self.members.insert(m) {
                added += 1;
            }
        }
        added
    }

    /// Remove members. Returns number removed.
    pub fn srem(&mut self, members: impl IntoIterator<Item = Bytes>) -> usize {
        let mut removed = 0;
        for m in members {
            if self.members.remove(&m) {
                removed += 1;
            }
        }
        removed
    }

    pub fn smembers(&self) -> Vec<Bytes> {
        self.members.iter().cloned().collect()
    }

    pub fn sismember(&self, member: &Bytes) -> bool {
        self.members.contains(member)
    }

    pub fn scard(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn contains(&self, member: &Bytes) -> bool {
        self.members.contains(member)
    }

    /// Intersection of this set with others (by membership snapshots).
    pub fn sinter(sets: &[&RedisSet]) -> Vec<Bytes> {
        if sets.is_empty() {
            return Vec::new();
        }
        let first = &sets[0].members;
        first
            .iter()
            .filter(|m| sets[1..].iter().all(|s| s.members.contains(*m)))
            .cloned()
            .collect()
    }

    /// Union of all provided sets.
    pub fn sunion(sets: &[&RedisSet]) -> Vec<Bytes> {
        let mut out = HashSet::new();
        for s in sets {
            for m in &s.members {
                out.insert(m.clone());
            }
        }
        out.into_iter().collect()
    }

    /// Difference: members of first set not in any of the rest.
    pub fn sdiff(sets: &[&RedisSet]) -> Vec<Bytes> {
        if sets.is_empty() {
            return Vec::new();
        }
        let first = &sets[0].members;
        first
            .iter()
            .filter(|m| !sets[1..].iter().any(|s| s.members.contains(*m)))
            .cloned()
            .collect()
    }

    pub fn iter_members(&self) -> impl Iterator<Item = Bytes> + '_ {
        self.members.iter().cloned()
    }

    pub fn memory_size(&self) -> usize {
        let mut size = std::mem::size_of::<Self>();
        for m in &self.members {
            size += std::mem::size_of::<Bytes>() + m.len();
        }
        size
    }
}

impl Default for RedisSet {
    fn default() -> Self {
        Self::new()
    }
}

pub type SharedSet = Arc<RwLock<RedisSet>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_basic() {
        let mut s = RedisSet::new();
        assert_eq!(s.sadd([Bytes::from("a"), Bytes::from("b"), Bytes::from("a")]), 2);
        assert!(s.sismember(&Bytes::from("a")));
        assert_eq!(s.scard(), 2);
        assert_eq!(s.srem([Bytes::from("a")]), 1);
        assert!(!s.sismember(&Bytes::from("a")));
    }

    #[test]
    fn test_set_ops() {
        let mut a = RedisSet::new();
        let mut b = RedisSet::new();
        a.sadd([Bytes::from("1"), Bytes::from("2"), Bytes::from("3")]);
        b.sadd([Bytes::from("2"), Bytes::from("3"), Bytes::from("4")]);
        let inter = RedisSet::sinter(&[&a, &b]);
        assert_eq!(inter.len(), 2);
        let uni = RedisSet::sunion(&[&a, &b]);
        assert_eq!(uni.len(), 4);
        let diff = RedisSet::sdiff(&[&a, &b]);
        assert_eq!(diff, vec![Bytes::from("1")]);
    }
}
