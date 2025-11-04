use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::cmp::Ordering as CmpOrdering;
use std::sync::{Arc, RwLock};

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

/// Key for BTreeMap that orders by score first, then by member (lexicographically)
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
        // First compare by score
        match self.score.cmp(&other.score) {
            CmpOrdering::Equal => {
                // If scores are equal, compare by member lexicographically
                self.member.cmp(&other.member)
            }
            ord => ord,
        }
    }
}

/// Wrapper for f64 to implement Ord
#[derive(Debug, Clone, Copy, PartialEq)]
struct OrderedFloat(f64);

impl Eq for OrderedFloat {}

impl PartialOrd for OrderedFloat {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        // Handle NaN: treat NaN as less than any number
        match (self.0.is_nan(), other.0.is_nan()) {
            (true, true) => CmpOrdering::Equal,
            (true, false) => CmpOrdering::Less,
            (false, true) => CmpOrdering::Greater,
            (false, false) => self.0.partial_cmp(&other.0).unwrap(),
        }
    }
}

/// Sorted Set implementation using Strategy pattern for range queries
/// and Iterator pattern for traversal
pub struct SortedSet {
    /// BTreeMap for maintaining score order: (score, member) -> ()
    score_map: BTreeMap<ScoreKey, ()>,
    /// HashMap for quick member lookup: member -> score
    member_map: HashMap<Bytes, f64>,
}

impl SortedSet {
    /// Create a new empty sorted set
    pub fn new() -> Self {
        Self {
            score_map: BTreeMap::new(),
            member_map: HashMap::new(),
        }
    }

    /// Add or update a member with its score
    /// Returns true if the member was newly added, false if updated
    pub fn add(&mut self, member: Bytes, score: f64) -> bool {
        let is_new = if let Some(&old_score) = self.member_map.get(&member) {
            // Remove old entry from score_map if score changed
            if old_score != score {
                let old_key = ScoreKey::new(old_score, member.clone());
                self.score_map.remove(&old_key);
                false
            } else {
                // Score hasn't changed, no need to update
                return false;
            }
        } else {
            true
        };

        // Insert new entry
        let key = ScoreKey::new(score, member.clone());
        self.score_map.insert(key, ());
        self.member_map.insert(member, score);

        is_new
    }

    /// Remove a member from the sorted set
    /// Returns true if the member was present
    pub fn remove(&mut self, member: &Bytes) -> bool {
        if let Some(score) = self.member_map.remove(member) {
            let key = ScoreKey::new(score, member.clone());
            self.score_map.remove(&key);
            true
        } else {
            false
        }
    }

    /// Get the score of a member
    pub fn score(&self, member: &Bytes) -> Option<f64> {
        self.member_map.get(member).copied()
    }

    /// Get the number of members in the sorted set
    pub fn len(&self) -> usize {
        self.member_map.len()
    }

    /// Check if the sorted set is empty
    pub fn is_empty(&self) -> bool {
        self.member_map.is_empty()
    }

    /// Get members in a range by index (0-based)
    /// Returns members in ascending order of score
    /// If reverse is true, returns in descending order
    pub fn range(&self, start: isize, stop: isize, reverse: bool) -> Vec<ScoredMember> {
        let len = self.len() as isize;
        if len == 0 {
            return Vec::new();
        }

        // Normalize negative indices
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

        // Check if indices are valid
        if start_idx > stop_idx || start_idx >= len {
            return Vec::new();
        }

        // Clamp stop_idx to valid range
        let stop_idx = stop_idx.min(len - 1);

        // Use Strategy pattern: different iteration strategies based on reverse flag
        if reverse {
            self.range_reverse_strategy(start_idx as usize, stop_idx as usize)
        } else {
            self.range_forward_strategy(start_idx as usize, stop_idx as usize)
        }
    }

    /// Forward iteration strategy (ascending order)
    fn range_forward_strategy(&self, start: usize, stop: usize) -> Vec<ScoredMember> {
        self.score_map
            .iter()
            .skip(start)
            .take(stop - start + 1)
            .map(|(key, _)| ScoredMember::new(key.member.clone(), key.score.0))
            .collect()
    }

    /// Reverse iteration strategy (descending order)
    fn range_reverse_strategy(&self, start: usize, stop: usize) -> Vec<ScoredMember> {
        let total = self.len();
        // For reverse, we need to reverse the indices
        let rev_start = total - 1 - stop;
        let rev_stop = total - 1 - start;

        self.score_map
            .iter()
            .skip(rev_start)
            .take(rev_stop - rev_start + 1)
            .rev()
            .map(|(key, _)| ScoredMember::new(key.member.clone(), key.score.0))
            .collect()
    }

    /// Get rank of a member (0-based index in ascending order)
    pub fn rank(&self, member: &Bytes) -> Option<usize> {
        let score = self.member_map.get(member)?;
        let key = ScoreKey::new(*score, member.clone());

        // Count how many elements are before this one
        let rank = self.score_map.range(..key).count();
        Some(rank)
    }

    /// Get reverse rank of a member (0-based index in descending order)
    pub fn rev_rank(&self, member: &Bytes) -> Option<usize> {
        let rank = self.rank(member)?;
        Some(self.len() - 1 - rank)
    }

    /// Remove members in a range by rank
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

    /// Get memory size estimate
    pub fn memory_size(&self) -> usize {
        let mut size = std::mem::size_of::<Self>();
        
        // Estimate BTreeMap size
        size += self.score_map.len() * (std::mem::size_of::<ScoreKey>() + std::mem::size_of::<()>());
        
        // Estimate HashMap size
        size += self.member_map.len() * (std::mem::size_of::<Bytes>() + std::mem::size_of::<f64>());
        
        // Add member bytes size
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
        assert!(!zset.add(Bytes::from("alice"), 150.0)); // Update
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

        // Forward range: should be alice(100), charlie(150), david(175), bob(200)
        let range = zset.range(0, 2, false);
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].member, Bytes::from("alice"));
        assert_eq!(range[1].member, Bytes::from("charlie"));
        assert_eq!(range[2].member, Bytes::from("david"));

        // Reverse range
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

        // Last two elements
        let range = zset.range(-2, -1, false);
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].member, Bytes::from("c"));
        assert_eq!(range[1].member, Bytes::from("d"));
    }

    #[test]
    fn test_sorted_set_rank() {
        let mut zset = SortedSet::new();

        zset.add(Bytes::from("alice"), 100.0);
        zset.add(Bytes::from("bob"), 200.0);
        zset.add(Bytes::from("charlie"), 150.0);

        assert_eq!(zset.rank(&Bytes::from("alice")), Some(0));
        assert_eq!(zset.rank(&Bytes::from("charlie")), Some(1));
        assert_eq!(zset.rank(&Bytes::from("bob")), Some(2));

        assert_eq!(zset.rev_rank(&Bytes::from("bob")), Some(0));
        assert_eq!(zset.rev_rank(&Bytes::from("charlie")), Some(1));
        assert_eq!(zset.rev_rank(&Bytes::from("alice")), Some(2));
    }

    #[test]
    fn test_sorted_set_remove() {
        let mut zset = SortedSet::new();

        zset.add(Bytes::from("alice"), 100.0);
        zset.add(Bytes::from("bob"), 200.0);

        assert!(zset.remove(&Bytes::from("alice")));
        assert!(!zset.remove(&Bytes::from("alice"))); // Already removed
        assert_eq!(zset.len(), 1);
        assert_eq!(zset.score(&Bytes::from("alice")), None);
    }
}
