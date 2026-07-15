//! Sorted set: score-ordered members with O(log n) rank via a span skiplist
//! (Redis zskiplist-style), plus HashMap for O(1) member → score lookup.

use bytes::Bytes;
use rand::Rng;
use std::cmp::Ordering as CmpOrdering;
use std::collections::HashMap;
use parking_lot::RwLock;
use std::sync::Arc;

/// Maximum skiplist height (Redis uses 64; 32 is plenty for 2^32 elements).
const SKIPLIST_MAXLEVEL: usize = 32;
/// P = 1/4 probability of promoting a level (Redis default).
const SKIPLIST_P: f64 = 0.25;

/// Member with its score
#[derive(Debug, Clone, PartialEq)]
pub struct ScoredMember {
    pub member: Bytes,
    pub score: f64,
}

impl ScoredMember {
    pub fn new(member: Bytes, score: f64) -> Self {
        Self { member, score }
    }
}

/// Key that orders by score first, then by member (lexicographically).
#[derive(Debug, Clone, PartialEq)]
struct ScoreKey {
    score: OrderedFloat,
    member: Bytes,
}

impl ScoreKey {
    fn new(score: f64, member: Bytes) -> Self {
        Self {
            score: OrderedFloat(score),
            member,
        }
    }
}

impl Eq for ScoreKey {}

impl PartialOrd for ScoreKey {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoreKey {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        match self.score.cmp(&other.score) {
            CmpOrdering::Equal => self.member.cmp(&other.member),
            ord => ord,
        }
    }
}

/// Wrapper for f64 to implement Ord/Eq (NaN-safe, Redis-like: all NaNs equal and sort last).
#[derive(Debug, Clone, Copy)]
struct OrderedFloat(f64);

impl PartialEq for OrderedFloat {
    fn eq(&self, other: &Self) -> bool {
        match (self.0.is_nan(), other.0.is_nan()) {
            (true, true) => true,
            (false, false) => self.0 == other.0,
            _ => false,
        }
    }
}

impl Eq for OrderedFloat {}

impl PartialOrd for OrderedFloat {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        match (self.0.is_nan(), other.0.is_nan()) {
            (true, true) => CmpOrdering::Equal,
            (true, false) => CmpOrdering::Greater,
            (false, true) => CmpOrdering::Less,
            (false, false) => self.0.partial_cmp(&other.0).unwrap_or(CmpOrdering::Equal),
        }
    }
}

/// Skiplist node. Head uses a dummy key; real nodes hold ScoreKey.
struct SkipNode {
    key: Option<ScoreKey>,
    /// forward[level] → node index (None = end)
    forward: Vec<Option<usize>>,
    /// span[level] = # of nodes between this and forward[level] (including the
    /// landed node, excluding self) — same as Redis zskiplist.
    span: Vec<usize>,
}

impl SkipNode {
    fn head(level: usize) -> Self {
        Self {
            key: None,
            forward: vec![None; level],
            span: vec![0; level],
        }
    }

    fn new(key: ScoreKey, level: usize) -> Self {
        Self {
            key: Some(key),
            forward: vec![None; level],
            span: vec![0; level],
        }
    }

    fn level(&self) -> usize {
        self.forward.len()
    }
}

/// Span skiplist for ordered members.
struct SkipList {
    /// nodes[0] is always the head sentinel.
    nodes: Vec<SkipNode>,
    free: Vec<usize>,
    /// Current top level index in use (0-based).
    level: usize,
    length: usize,
}

impl SkipList {
    fn new() -> Self {
        let mut nodes = Vec::with_capacity(16);
        nodes.push(SkipNode::head(SKIPLIST_MAXLEVEL));
        Self {
            nodes,
            free: Vec::new(),
            level: 0,
            length: 0,
        }
    }

    fn len(&self) -> usize {
        self.length
    }

    fn is_empty(&self) -> bool {
        self.length == 0
    }

    fn random_level() -> usize {
        let mut lvl = 1;
        let mut rng = rand::thread_rng();
        while rng.gen::<f64>() < SKIPLIST_P && lvl < SKIPLIST_MAXLEVEL {
            lvl += 1;
        }
        lvl
    }

    fn alloc_node(&mut self, key: ScoreKey, level: usize) -> usize {
        if let Some(idx) = self.free.pop() {
            self.nodes[idx] = SkipNode::new(key, level);
            idx
        } else {
            let idx = self.nodes.len();
            self.nodes.push(SkipNode::new(key, level));
            idx
        }
    }

    fn free_node(&mut self, idx: usize) {
        debug_assert!(idx != 0);
        self.nodes[idx] = SkipNode::head(1); // drop key / links
        self.free.push(idx);
    }

    /// Compare node key with target. Head is always less than any real key.
    fn node_less(&self, idx: usize, key: &ScoreKey) -> bool {
        match &self.nodes[idx].key {
            None => true, // head
            Some(k) => k < key,
        }
    }

    /// Insert key. Caller must ensure key is not already present.
    fn insert(&mut self, key: ScoreKey) {
        let mut update = [0usize; SKIPLIST_MAXLEVEL];
        let mut rank = [0usize; SKIPLIST_MAXLEVEL];
        let mut x = 0usize; // head

        for i in (0..=self.level).rev() {
            rank[i] = if i == self.level { 0 } else { rank[i + 1] };
            while let Some(next) = self.nodes[x].forward[i] {
                if self.node_less(next, &key) {
                    rank[i] += self.nodes[x].span[i];
                    x = next;
                } else {
                    break;
                }
            }
            update[i] = x;
        }

        let lvl = Self::random_level();
        if lvl - 1 > self.level {
            for i in (self.level + 1)..lvl {
                rank[i] = 0;
                update[i] = 0; // head
                self.nodes[0].span[i] = self.length;
            }
            self.level = lvl - 1;
        }

        let idx = self.alloc_node(key, lvl);
        for i in 0..lvl {
            self.nodes[idx].forward[i] = self.nodes[update[i]].forward[i];
            self.nodes[update[i]].forward[i] = Some(idx);
            // span: how many nodes we jump over to reach forward
            self.nodes[idx].span[i] = self.nodes[update[i]].span[i] - (rank[0] - rank[i]);
            self.nodes[update[i]].span[i] = (rank[0] - rank[i]) + 1;
        }

        // Levels higher than new node: increment span of update nodes
        for i in lvl..=self.level {
            self.nodes[update[i]].span[i] += 1;
        }

        self.length += 1;
    }

    /// Remove key if present. Returns true if removed.
    fn remove(&mut self, key: &ScoreKey) -> bool {
        let mut update = [0usize; SKIPLIST_MAXLEVEL];
        let mut x = 0usize;

        for i in (0..=self.level).rev() {
            while let Some(next) = self.nodes[x].forward[i] {
                if self.node_less(next, key) {
                    x = next;
                } else {
                    break;
                }
            }
            update[i] = x;
        }

        let Some(target) = self.nodes[x].forward[0] else {
            return false;
        };
        match &self.nodes[target].key {
            Some(k) if k == key => {}
            _ => return false,
        }

        for i in 0..=self.level {
            if self.nodes[update[i]].forward[i] == Some(target) {
                self.nodes[update[i]].span[i] += self.nodes[target].span[i].saturating_sub(1);
                self.nodes[update[i]].forward[i] = self.nodes[target].forward[i];
            } else {
                self.nodes[update[i]].span[i] = self.nodes[update[i]].span[i].saturating_sub(1);
            }
        }

        self.free_node(target);
        while self.level > 0 && self.nodes[0].forward[self.level].is_none() {
            self.level -= 1;
        }
        self.length = self.length.saturating_sub(1);
        true
    }

    /// 0-based rank of key, or None if absent. O(log n).
    fn rank_of(&self, key: &ScoreKey) -> Option<usize> {
        let mut x = 0usize;
        let mut rank = 0usize;

        for i in (0..=self.level).rev() {
            while let Some(next) = self.nodes[x].forward[i] {
                if self.node_less(next, key) {
                    rank += self.nodes[x].span[i];
                    x = next;
                } else {
                    break;
                }
            }
        }

        let next = self.nodes[x].forward[0]?;
        match &self.nodes[next].key {
            Some(k) if k == key => Some(rank),
            _ => None,
        }
    }

    /// Element at 0-based rank, O(log n).
    ///
    /// Spans count nodes in the jump (including the landed node), so the walk
    /// uses a 1-based target rank — same as Redis `zslGetElementByRank`.
    fn get_by_rank(&self, rank: usize) -> Option<&ScoreKey> {
        if rank >= self.length {
            return None;
        }
        let target = rank + 1; // 1-based for span arithmetic
        let mut x = 0usize; // head
        let mut traversed = 0usize;

        for i in (0..=self.level).rev() {
            while let Some(next) = self.nodes[x].forward[i] {
                if traversed + self.nodes[x].span[i] <= target {
                    traversed += self.nodes[x].span[i];
                    x = next;
                } else {
                    break;
                }
            }
        }
        self.nodes[x].key.as_ref()
    }

    /// Iterate all keys in ascending order.
    fn iter_keys(&self) -> SkipListIter<'_> {
        SkipListIter {
            list: self,
            cur: self.nodes[0].forward[0],
        }
    }
}

struct SkipListIter<'a> {
    list: &'a SkipList,
    cur: Option<usize>,
}

impl<'a> Iterator for SkipListIter<'a> {
    type Item = &'a ScoreKey;

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.cur?;
        let key = self.list.nodes[idx].key.as_ref()?;
        self.cur = self.list.nodes[idx].forward[0];
        Some(key)
    }
}

/// Sorted Set with O(log n) add/remove/rank and O(log n + k) range-by-rank.
pub struct SortedSet {
    /// Span skiplist ordered by (score, member).
    list: SkipList,
    /// member → score for O(1) lookup / existence.
    member_map: HashMap<Bytes, f64>,
}

impl SortedSet {
    pub fn new() -> Self {
        Self {
            list: SkipList::new(),
            member_map: HashMap::new(),
        }
    }

    /// Add or update a member with its score.
    /// Returns true if newly added, false if updated (or score unchanged).
    /// NaN scores compare equal to each other (Redis-like total order).
    pub fn add(&mut self, member: Bytes, score: f64) -> bool {
        if let Some(&old_score) = self.member_map.get(&member) {
            // Treat NaN == NaN so re-adding the same NaN score is a no-op.
            if OrderedFloat(old_score) == OrderedFloat(score) {
                return false;
            }
            let old_key = ScoreKey::new(old_score, member.clone());
            self.list.remove(&old_key);
            let new_key = ScoreKey::new(score, member.clone());
            self.list.insert(new_key);
            self.member_map.insert(member, score);
            return false;
        }

        let key = ScoreKey::new(score, member.clone());
        self.list.insert(key);
        self.member_map.insert(member, score);
        true
    }

    pub fn remove(&mut self, member: &Bytes) -> bool {
        if let Some(score) = self.member_map.remove(member) {
            let key = ScoreKey::new(score, member.clone());
            self.list.remove(&key);
            true
        } else {
            false
        }
    }

    pub fn score(&self, member: &Bytes) -> Option<f64> {
        self.member_map.get(member).copied()
    }

    pub fn len(&self) -> usize {
        self.member_map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.member_map.is_empty()
    }

    pub fn iter_members(&self) -> impl Iterator<Item = (Bytes, f64)> + '_ {
        self.member_map
            .iter()
            .map(|(m, s)| (m.clone(), *s))
    }

    /// Members in index range [start, stop] (Redis-style negative indices).
    pub fn range(&self, start: isize, stop: isize, reverse: bool) -> Vec<ScoredMember> {
        let len = self.len() as isize;
        if len == 0 {
            return Vec::new();
        }

        let start_idx = if start < 0 {
            (len + start).max(0)
        } else {
            start
        };
        let stop_idx = if stop < 0 {
            (len + stop).max(0)
        } else {
            stop
        };

        if start_idx > stop_idx || start_idx >= len {
            return Vec::new();
        }
        let stop_idx = stop_idx.min(len - 1);

        if reverse {
            self.range_reverse(start_idx as usize, stop_idx as usize)
        } else {
            self.range_forward(start_idx as usize, stop_idx as usize)
        }
    }

    fn range_forward(&self, start: usize, stop: usize) -> Vec<ScoredMember> {
        let mut out = Vec::with_capacity(stop - start + 1);
        for rank in start..=stop {
            if let Some(key) = self.list.get_by_rank(rank) {
                out.push(ScoredMember::new(key.member.clone(), key.score.0));
            }
        }
        out
    }

    fn range_reverse(&self, start: usize, stop: usize) -> Vec<ScoredMember> {
        let total = self.len();
        // Reverse view: rank 0 = highest score = ascending index (total-1)
        let mut out = Vec::with_capacity(stop - start + 1);
        for rev_rank in start..=stop {
            let asc = total - 1 - rev_rank;
            if let Some(key) = self.list.get_by_rank(asc) {
                out.push(ScoredMember::new(key.member.clone(), key.score.0));
            }
        }
        out
    }

    /// 0-based rank in ascending score order. O(log n).
    pub fn rank(&self, member: &Bytes) -> Option<usize> {
        let score = *self.member_map.get(member)?;
        let key = ScoreKey::new(score, member.clone());
        self.list.rank_of(&key)
    }

    /// 0-based rank in descending score order. O(log n).
    pub fn rev_rank(&self, member: &Bytes) -> Option<usize> {
        let rank = self.rank(member)?;
        Some(self.len() - 1 - rank)
    }

    pub fn remove_range_by_rank(&mut self, start: isize, stop: isize) -> usize {
        let members_to_remove: Vec<Bytes> = self
            .range(start, stop, false)
            .into_iter()
            .map(|sm| sm.member)
            .collect();

        let mut count = 0;
        for member in members_to_remove {
            if self.remove(&member) {
                count += 1;
            }
        }
        count
    }

    pub fn memory_size(&self) -> usize {
        let mut size = std::mem::size_of::<Self>();
        size += self.list.nodes.capacity() * std::mem::size_of::<SkipNode>();
        size += self.member_map.len() * (std::mem::size_of::<Bytes>() + std::mem::size_of::<f64>());
        for member in self.member_map.keys() {
            size += member.len();
        }
        size
    }
}

impl Default for SortedSet {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-safe wrapper for SortedSet
pub type SharedSortedSet = Arc<RwLock<SortedSet>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sorted_set_basic_operations() {
        let mut zset = SortedSet::new();

        assert!(zset.add(Bytes::from("alice"), 100.0));
        assert!(zset.add(Bytes::from("bob"), 200.0));
        assert!(zset.add(Bytes::from("charlie"), 150.0));

        assert_eq!(zset.len(), 3);
        assert_eq!(zset.score(&Bytes::from("alice")), Some(100.0));
        assert_eq!(zset.score(&Bytes::from("bob")), Some(200.0));
        assert_eq!(zset.score(&Bytes::from("charlie")), Some(150.0));
    }

    #[test]
    fn test_sorted_set_update() {
        let mut zset = SortedSet::new();

        assert!(zset.add(Bytes::from("alice"), 100.0));
        assert!(!zset.add(Bytes::from("alice"), 150.0));
        assert_eq!(zset.len(), 1);
        assert_eq!(zset.score(&Bytes::from("alice")), Some(150.0));
    }

    #[test]
    fn test_sorted_set_range() {
        let mut zset = SortedSet::new();

        zset.add(Bytes::from("alice"), 100.0);
        zset.add(Bytes::from("bob"), 200.0);
        zset.add(Bytes::from("charlie"), 150.0);
        zset.add(Bytes::from("david"), 175.0);

        let range = zset.range(0, 2, false);
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].member, Bytes::from("alice"));
        assert_eq!(range[1].member, Bytes::from("charlie"));
        assert_eq!(range[2].member, Bytes::from("david"));

        let range = zset.range(0, 2, true);
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].member, Bytes::from("bob"));
        assert_eq!(range[1].member, Bytes::from("david"));
        assert_eq!(range[2].member, Bytes::from("charlie"));
    }

    #[test]
    fn test_sorted_set_negative_indices() {
        let mut zset = SortedSet::new();

        zset.add(Bytes::from("a"), 1.0);
        zset.add(Bytes::from("b"), 2.0);
        zset.add(Bytes::from("c"), 3.0);
        zset.add(Bytes::from("d"), 4.0);

        let range = zset.range(-2, -1, false);
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].member, Bytes::from("c"));
        assert_eq!(range[1].member, Bytes::from("d"));
    }

    #[test]
    fn test_sorted_set_rank() {
        let mut zset = SortedSet::new();
        zset.add(Bytes::from("a"), 1.0);
        zset.add(Bytes::from("b"), 2.0);
        zset.add(Bytes::from("c"), 3.0);

        assert_eq!(zset.rank(&Bytes::from("a")), Some(0));
        assert_eq!(zset.rank(&Bytes::from("b")), Some(1));
        assert_eq!(zset.rank(&Bytes::from("c")), Some(2));
        assert_eq!(zset.rev_rank(&Bytes::from("c")), Some(0));
        assert_eq!(zset.rev_rank(&Bytes::from("a")), Some(2));
        assert_eq!(zset.rank(&Bytes::from("missing")), None);
    }

    #[test]
    fn test_sorted_set_remove() {
        let mut zset = SortedSet::new();
        zset.add(Bytes::from("a"), 1.0);
        zset.add(Bytes::from("b"), 2.0);
        assert!(zset.remove(&Bytes::from("a")));
        assert_eq!(zset.len(), 1);
        assert_eq!(zset.rank(&Bytes::from("b")), Some(0));
        assert!(!zset.remove(&Bytes::from("a")));
    }

    #[test]
    fn test_rank_after_many_ops() {
        let mut zset = SortedSet::new();
        for i in 0..200 {
            zset.add(Bytes::from(format!("m{i}")), i as f64);
        }
        // Delete half
        for i in (0..200).step_by(2) {
            zset.remove(&Bytes::from(format!("m{i}")));
        }
        // Remaining: 1,3,5,... ranks 0,1,2,...
        for (rank, i) in (1..200).step_by(2).enumerate() {
            assert_eq!(
                zset.rank(&Bytes::from(format!("m{i}"))),
                Some(rank),
                "member m{i}"
            );
        }
        // Update score of m199 to lowest
        zset.add(Bytes::from("m199"), -1.0);
        assert_eq!(zset.rank(&Bytes::from("m199")), Some(0));
    }

    #[test]
    fn test_get_by_rank_matches_rank_of() {
        let mut zset = SortedSet::new();
        for i in 0..50 {
            zset.add(Bytes::from(format!("x{i}")), (50 - i) as f64);
        }
        // Ascending order by score: x49 (1.0), x48 (2.0), ..., x0 (50.0)
        for i in 0..50 {
            let key = zset.list.get_by_rank(i).expect("rank present");
            assert_eq!(zset.list.rank_of(key), Some(i));
            assert_eq!(key.score.0, (i + 1) as f64);
        }
        // Full range equals successive get_by_rank
        let all = zset.range(0, -1, false);
        assert_eq!(all.len(), 50);
        assert_eq!(all[0].member, Bytes::from("x49"));
        assert_eq!(all[49].member, Bytes::from("x0"));
        for (i, sm) in all.iter().enumerate() {
            assert_eq!(zset.rank(&sm.member), Some(i));
        }
    }

    #[test]
    fn test_same_score_lex_order_and_rank() {
        let mut zset = SortedSet::new();
        zset.add(Bytes::from("c"), 1.0);
        zset.add(Bytes::from("a"), 1.0);
        zset.add(Bytes::from("b"), 1.0);
        let range = zset.range(0, -1, false);
        assert_eq!(
            range.iter().map(|s| s.member.clone()).collect::<Vec<_>>(),
            vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]
        );
        assert_eq!(zset.rank(&Bytes::from("a")), Some(0));
        assert_eq!(zset.rank(&Bytes::from("b")), Some(1));
        assert_eq!(zset.rank(&Bytes::from("c")), Some(2));
        assert_eq!(zset.rev_rank(&Bytes::from("a")), Some(2));
    }

    #[test]
    fn test_remove_range_by_rank() {
        let mut zset = SortedSet::new();
        for i in 0..10 {
            zset.add(Bytes::from(format!("m{i}")), i as f64);
        }
        let n = zset.remove_range_by_rank(2, 5);
        assert_eq!(n, 4);
        assert_eq!(zset.len(), 6);
        assert!(zset.score(&Bytes::from("m2")).is_none());
        assert!(zset.score(&Bytes::from("m5")).is_none());
        assert_eq!(zset.rank(&Bytes::from("m6")), Some(2));
    }

    #[test]
    fn test_nan_score_orders_last() {
        let mut zset = SortedSet::new();
        zset.add(Bytes::from("n"), f64::NAN);
        zset.add(Bytes::from("z"), 0.0);
        zset.add(Bytes::from("p"), 1.0);
        let range = zset.range(0, -1, false);
        assert_eq!(range[0].member, Bytes::from("z"));
        assert_eq!(range[1].member, Bytes::from("p"));
        assert_eq!(range[2].member, Bytes::from("n"));
        assert_eq!(zset.rank(&Bytes::from("n")), Some(2));
        assert_eq!(zset.rev_rank(&Bytes::from("n")), Some(0));
        // Re-add same NaN score is a no-op (NaN == NaN for ordering)
        assert!(!zset.add(Bytes::from("n"), f64::NAN));
        assert_eq!(zset.len(), 3);
        // Remove + re-add NaN works
        assert!(zset.remove(&Bytes::from("n")));
        assert_eq!(zset.rank(&Bytes::from("n")), None);
        assert!(zset.add(Bytes::from("n"), f64::NAN));
        assert_eq!(zset.rank(&Bytes::from("n")), Some(2));
        // Update from finite → NaN and back
        assert!(!zset.add(Bytes::from("z"), f64::NAN));
        assert_eq!(zset.rank(&Bytes::from("z")), Some(2));
        assert!(!zset.add(Bytes::from("z"), -5.0));
        assert_eq!(zset.rank(&Bytes::from("z")), Some(0));
    }

    #[test]
    fn test_inf_scores_order() {
        let mut zset = SortedSet::new();
        zset.add(Bytes::from("pos"), f64::INFINITY);
        zset.add(Bytes::from("neg"), f64::NEG_INFINITY);
        zset.add(Bytes::from("mid"), 0.0);
        let range = zset.range(0, -1, false);
        assert_eq!(range[0].member, Bytes::from("neg"));
        assert_eq!(range[1].member, Bytes::from("mid"));
        assert_eq!(range[2].member, Bytes::from("pos"));
        assert_eq!(zset.rank(&Bytes::from("neg")), Some(0));
        assert_eq!(zset.rank(&Bytes::from("pos")), Some(2));
    }

    #[test]
    fn test_score_update_preserves_ranks() {
        let mut zset = SortedSet::new();
        for i in 0..20 {
            zset.add(Bytes::from(format!("m{i}")), i as f64);
        }
        // Bump m0 to the top
        zset.add(Bytes::from("m0"), 100.0);
        assert_eq!(zset.rank(&Bytes::from("m0")), Some(19));
        assert_eq!(zset.rank(&Bytes::from("m1")), Some(0));
        // Drop m19 to the bottom
        zset.add(Bytes::from("m19"), -1.0);
        assert_eq!(zset.rank(&Bytes::from("m19")), Some(0));
        assert_eq!(zset.len(), 20);
    }

    #[test]
    fn test_large_set_rank_consistency() {
        let mut zset = SortedSet::new();
        const N: usize = 1000;
        for i in 0..N {
            // Insert in shuffled-ish order
            let score = ((i * 7) % N) as f64;
            zset.add(Bytes::from(format!("m{i:04}")), score);
        }
        assert_eq!(zset.len(), N);
        let all = zset.range(0, -1, false);
        assert_eq!(all.len(), N);
        for (i, sm) in all.iter().enumerate() {
            assert_eq!(zset.rank(&sm.member), Some(i));
            assert_eq!(zset.rev_rank(&sm.member), Some(N - 1 - i));
            let by = zset.list.get_by_rank(i).unwrap();
            assert_eq!(by.member, sm.member);
        }
        // Out of range
        assert!(zset.list.get_by_rank(N).is_none());
    }
}
