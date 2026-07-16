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

    /// Remove and return up to `count` random members.
    pub fn spop(&mut self, count: usize) -> Vec<Bytes> {
        use rand::seq::IteratorRandom;
        if count == 0 || self.members.is_empty() {
            return Vec::new();
        }
        let n = count.min(self.members.len());
        let mut rng = rand::thread_rng();
        let chosen: Vec<Bytes> = self
            .members
            .iter()
            .choose_multiple(&mut rng, n)
            .into_iter()
            .cloned()
            .collect();
        for m in &chosen {
            self.members.remove(m);
        }
        chosen
    }

    /// Random members without removal.
    ///
    /// * `count >= 0`: up to `count` distinct members (or empty if set empty).
    /// * `count < 0`: `|count|` members with replacement (duplicates allowed).
    pub fn srandmember(&self, count: i64) -> Vec<Bytes> {
        use rand::seq::{IteratorRandom, SliceRandom};
        if self.members.is_empty() || count == 0 {
            return Vec::new();
        }
        let mut rng = rand::thread_rng();
        if count > 0 {
            let n = (count as usize).min(self.members.len());
            self.members
                .iter()
                .choose_multiple(&mut rng, n)
                .into_iter()
                .cloned()
                .collect()
        } else {
            let n = (-count) as usize;
            let pool: Vec<&Bytes> = self.members.iter().collect();
            (0..n)
                .map(|_| (*pool.choose(&mut rng).unwrap()).clone())
                .collect()
        }
    }

    /// Single random member, if any.
    pub fn srandmember_one(&self) -> Option<Bytes> {
        use rand::seq::IteratorRandom;
        let mut rng = rand::thread_rng();
        self.members.iter().choose(&mut rng).cloned()
    }

    pub fn iter_members(&self) -> impl Iterator<Item = Bytes> + '_ {
        self.members.iter().cloned()
    }

    /// Approximate heap size of set contents (members only; key charged separately).
    pub fn memory_size(&self) -> usize {
        use crate::memory::{estimate_set_member, with_alloc_overhead};
        let mut raw = std::mem::size_of::<Self>();
        raw += self.members.capacity().saturating_mul(8);
        let members: usize = self
            .members
            .iter()
            .map(|m| estimate_set_member(m.len()))
            .sum();
        with_alloc_overhead(raw).saturating_add(members)
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

    #[test]
    fn test_spop_and_srandmember() {
        let mut s = RedisSet::new();
        s.sadd([Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]);
        let one = s.srandmember_one();
        assert!(one.is_some());
        assert!(s.sismember(one.as_ref().unwrap()));
        let multi = s.srandmember(2);
        assert_eq!(multi.len(), 2);
        let with_dup = s.srandmember(-5);
        assert_eq!(with_dup.len(), 5);
        let popped = s.spop(2);
        assert_eq!(popped.len(), 2);
        assert_eq!(s.scard(), 1);
    }
}
