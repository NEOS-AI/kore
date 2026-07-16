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

/// Inclusive/exclusive score bound for ZRANGEBYSCORE / ZCOUNT / ZREMRANGEBYSCORE.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoreBound {
    pub value: f64,
    pub exclusive: bool,
}

/// Lexicographical bound for ZRANGEBYLEX / ZLEXCOUNT / ZREMRANGEBYLEX.
///
/// Redis specs: `-` / `+` (open ends), `[member` (inclusive), `(member` (exclusive).
#[derive(Debug, Clone, PartialEq)]
pub enum LexBound {
    NegInf,
    PosInf,
    Inclusive(Bytes),
    Exclusive(Bytes),
}

impl LexBound {
    /// Parse a Redis lex range item (`-`, `+`, `[…`, `(…`).
    pub fn parse(s: &[u8]) -> Result<Self, ()> {
        if s == b"-" {
            return Ok(LexBound::NegInf);
        }
        if s == b"+" {
            return Ok(LexBound::PosInf);
        }
        if s.is_empty() {
            return Err(());
        }
        match s[0] {
            b'[' => Ok(LexBound::Inclusive(Bytes::copy_from_slice(&s[1..]))),
            b'(' => Ok(LexBound::Exclusive(Bytes::copy_from_slice(&s[1..]))),
            _ => Err(()),
        }
    }

    fn accepts_as_min(&self, member: &[u8]) -> bool {
        match self {
            LexBound::NegInf => true,
            LexBound::PosInf => false,
            LexBound::Inclusive(b) => member >= b.as_ref(),
            LexBound::Exclusive(b) => member > b.as_ref(),
        }
    }

    fn accepts_as_max(&self, member: &[u8]) -> bool {
        match self {
            LexBound::PosInf => true,
            LexBound::NegInf => false,
            LexBound::Inclusive(b) => member <= b.as_ref(),
            LexBound::Exclusive(b) => member < b.as_ref(),
        }
    }
}

fn member_in_lex_range(member: &[u8], min: &LexBound, max: &LexBound) -> bool {
    min.accepts_as_min(member) && max.accepts_as_max(member)
}

impl ScoreBound {
    pub fn inclusive(value: f64) -> Self {
        Self {
            value,
            exclusive: false,
        }
    }

    pub fn exclusive(value: f64) -> Self {
        Self {
            value,
            exclusive: true,
        }
    }

    /// Parse Redis score spec: `1.5`, `(1.5`, `-inf`, `+inf`, `inf`.
    pub fn parse(s: &str) -> Result<Self, ()> {
        let s = s.trim();
        if s.eq_ignore_ascii_case("-inf") {
            return Ok(Self::inclusive(f64::NEG_INFINITY));
        }
        if s.eq_ignore_ascii_case("+inf") || s.eq_ignore_ascii_case("inf") {
            return Ok(Self::inclusive(f64::INFINITY));
        }
        let (exclusive, rest) = if let Some(r) = s.strip_prefix('(') {
            (true, r)
        } else {
            (false, s)
        };
        let value: f64 = rest.parse().map_err(|_| ())?;
        if value.is_nan() {
            return Err(());
        }
        Ok(Self { value, exclusive })
    }

    fn accepts_as_min(self, score: f64) -> bool {
        let s = OrderedFloat(score);
        let b = OrderedFloat(self.value);
        if self.exclusive {
            s > b
        } else {
            s >= b
        }
    }

    fn accepts_as_max(self, score: f64) -> bool {
        let s = OrderedFloat(score);
        let b = OrderedFloat(self.value);
        if self.exclusive {
            s < b
        } else {
            s <= b
        }
    }
}

fn score_in_range(score: f64, min: ScoreBound, max: ScoreBound) -> bool {
    min.accepts_as_min(score) && max.accepts_as_max(score)
}

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

    /// Pop up to `count` members with the lowest scores (ascending order).
    pub fn pop_min(&mut self, count: usize) -> Vec<ScoredMember> {
        let n = count.min(self.len());
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            let Some(key) = self.list.get_by_rank(0) else {
                break;
            };
            let member = key.member.clone();
            let score = key.score.0;
            let _ = self.remove(&member);
            out.push(ScoredMember::new(member, score));
        }
        out
    }

    /// Pop up to `count` members with the highest scores (descending order).
    pub fn pop_max(&mut self, count: usize) -> Vec<ScoredMember> {
        let n = count.min(self.len());
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            if self.is_empty() {
                break;
            }
            let last = self.len() - 1;
            let Some(key) = self.list.get_by_rank(last) else {
                break;
            };
            let member = key.member.clone();
            let score = key.score.0;
            let _ = self.remove(&member);
            out.push(ScoredMember::new(member, score));
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

    /// Increment member score by `incr` (create with score `incr` if absent).
    /// Returns the new score.
    pub fn incr_by(&mut self, member: Bytes, incr: f64) -> f64 {
        let new_score = self.member_map.get(&member).copied().unwrap_or(0.0) + incr;
        // `add` returns false on update; we always want the new score stored.
        let _ = self.add(member, new_score);
        new_score
    }

    /// Members with scores in `[min, max]` (respecting exclusive bounds).
    ///
    /// When `reverse` is true, results are highest-score first.
    /// `offset` / `count` apply after ordering (Redis LIMIT semantics).
    pub fn range_by_score(
        &self,
        min: ScoreBound,
        max: ScoreBound,
        reverse: bool,
        offset: usize,
        count: Option<usize>,
    ) -> Vec<ScoredMember> {
        let mut matched: Vec<ScoredMember> = self
            .list
            .iter_keys()
            .filter(|k| score_in_range(k.score.0, min, max))
            .map(|k| ScoredMember::new(k.member.clone(), k.score.0))
            .collect();
        if reverse {
            matched.reverse();
        }
        if offset >= matched.len() {
            return Vec::new();
        }
        let end = match count {
            Some(c) => (offset + c).min(matched.len()),
            None => matched.len(),
        };
        matched[offset..end].to_vec()
    }

    /// Count members with scores in range.
    pub fn count_by_score(&self, min: ScoreBound, max: ScoreBound) -> usize {
        self.list
            .iter_keys()
            .filter(|k| score_in_range(k.score.0, min, max))
            .count()
    }

    /// Remove members with scores in range. Returns number removed.
    pub fn remove_range_by_score(&mut self, min: ScoreBound, max: ScoreBound) -> usize {
        let members: Vec<Bytes> = self
            .list
            .iter_keys()
            .filter(|k| score_in_range(k.score.0, min, max))
            .map(|k| k.member.clone())
            .collect();
        let mut count = 0;
        for m in members {
            if self.remove(&m) {
                count += 1;
            }
        }
        count
    }

    /// Members whose names fall in the lex range `[min, max]` (Redis ZRANGEBYLEX).
    ///
    /// Intended for sets where all scores are equal; with mixed scores the walk
    /// order follows the skiplist `(score, member)` order.
    pub fn range_by_lex(
        &self,
        min: &LexBound,
        max: &LexBound,
        reverse: bool,
        offset: usize,
        count: Option<usize>,
    ) -> Vec<ScoredMember> {
        let mut matched: Vec<ScoredMember> = self
            .list
            .iter_keys()
            .filter(|k| member_in_lex_range(k.member.as_ref(), min, max))
            .map(|k| ScoredMember::new(k.member.clone(), k.score.0))
            .collect();
        // Lex order is member order; with equal scores skiplist order is already
        // member-sorted. Sort by member for stable pure-lex results.
        matched.sort_by(|a, b| a.member.cmp(&b.member));
        if reverse {
            matched.reverse();
        }
        if offset >= matched.len() {
            return Vec::new();
        }
        let end = match count {
            Some(c) => (offset + c).min(matched.len()),
            None => matched.len(),
        };
        matched[offset..end].to_vec()
    }

    /// Count members in a lex range.
    pub fn count_by_lex(&self, min: &LexBound, max: &LexBound) -> usize {
        self.member_map
            .keys()
            .filter(|m| member_in_lex_range(m.as_ref(), min, max))
            .count()
    }

    /// Remove members in a lex range. Returns number removed.
    pub fn remove_range_by_lex(&mut self, min: &LexBound, max: &LexBound) -> usize {
        let members: Vec<Bytes> = self
            .member_map
            .keys()
            .filter(|m| member_in_lex_range(m.as_ref(), min, max))
            .cloned()
            .collect();
        let mut count = 0;
        for m in members {
            if self.remove(&m) {
                count += 1;
            }
        }
        count
    }

    /// Random members without removal (Redis ZRANDMEMBER count semantics).
    ///
    /// * `count > 0`: up to `count` distinct members
    /// * `count < 0`: `|count|` members with replacement
    /// * `count == 0`: empty
    pub fn randmember(&self, count: i64) -> Vec<ScoredMember> {
        use rand::seq::{IteratorRandom, SliceRandom};
        if self.is_empty() || count == 0 {
            return Vec::new();
        }
        let mut rng = rand::thread_rng();
        if count > 0 {
            let n = (count as usize).min(self.len());
            self.member_map
                .iter()
                .choose_multiple(&mut rng, n)
                .into_iter()
                .map(|(m, s)| ScoredMember::new(m.clone(), *s))
                .collect()
        } else {
            let n = (-count) as usize;
            let pool: Vec<(&Bytes, f64)> = self
                .member_map
                .iter()
                .map(|(m, s)| (m, *s))
                .collect();
            (0..n)
                .map(|_| {
                    let (m, s) = *pool.choose(&mut rng).unwrap();
                    ScoredMember::new(m.clone(), s)
                })
                .collect()
        }
    }

    /// Single random member, if any.
    pub fn randmember_one(&self) -> Option<ScoredMember> {
        use rand::seq::IteratorRandom;
        let mut rng = rand::thread_rng();
        self.member_map
            .iter()
            .choose(&mut rng)
            .map(|(m, s)| ScoredMember::new(m.clone(), *s))
    }

    /// Approximate heap size of zset contents (members only; key charged separately).
    pub fn memory_size(&self) -> usize {
        use crate::memory::{estimate_zset_member, with_alloc_overhead};
        let skip_node = std::mem::size_of::<SkipNode>();
        let mut raw = std::mem::size_of::<Self>();
        // Skiplist node vector capacity + member map capacity
        raw += self.list.nodes.capacity().saturating_mul(skip_node);
        raw += self
            .member_map
            .capacity()
            .saturating_mul(std::mem::size_of::<Bytes>() + std::mem::size_of::<f64>());
        let members: usize = self
            .member_map
            .keys()
            .map(|m| estimate_zset_member(m.len(), skip_node))
            .sum();
        with_alloc_overhead(raw).saturating_add(members)
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
    fn test_score_bound_parse() {
        assert_eq!(
            ScoreBound::parse("1.5").unwrap(),
            ScoreBound::inclusive(1.5)
        );
        assert_eq!(
            ScoreBound::parse("(2").unwrap(),
            ScoreBound::exclusive(2.0)
        );
        assert!(ScoreBound::parse("-inf").unwrap().value.is_infinite());
        assert!(ScoreBound::parse("+inf").unwrap().value.is_infinite());
        assert!(ScoreBound::parse("nan").is_err());
    }

    #[test]
    fn test_incr_by_and_range_by_score() {
        let mut zset = SortedSet::new();
        zset.add(Bytes::from("a"), 1.0);
        zset.add(Bytes::from("b"), 2.0);
        zset.add(Bytes::from("c"), 3.0);
        zset.add(Bytes::from("d"), 4.0);

        assert_eq!(zset.incr_by(Bytes::from("b"), 1.5), 3.5);
        assert_eq!(zset.score(&Bytes::from("b")), Some(3.5));
        // New member
        assert_eq!(zset.incr_by(Bytes::from("e"), 0.5), 0.5);

        let mid = zset.range_by_score(
            ScoreBound::inclusive(2.0),
            ScoreBound::inclusive(3.5),
            false,
            0,
            None,
        );
        let names: Vec<_> = mid.iter().map(|m| m.member.as_ref()).collect();
        assert_eq!(names, [b"c".as_ref(), b"b".as_ref()]);

        // After incr: a=1, e=0.5, c=3, b=3.5, d=4 → exclusive (1,4) keeps c and b.
        let excl = zset.range_by_score(
            ScoreBound::exclusive(1.0),
            ScoreBound::exclusive(4.0),
            false,
            0,
            None,
        );
        assert_eq!(excl.len(), 2);

        assert_eq!(
            zset.count_by_score(ScoreBound::inclusive(3.0), ScoreBound::inclusive(4.0)),
            3 // c, b, d
        );

        let n = zset.remove_range_by_score(
            ScoreBound::inclusive(3.0),
            ScoreBound::inclusive(3.5),
        );
        assert_eq!(n, 2); // c and b
        assert_eq!(zset.len(), 3); // a, d, e
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
