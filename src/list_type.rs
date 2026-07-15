//! Redis List data type: double-ended queue of bulk strings.

use bytes::Bytes;
use std::collections::VecDeque;
use parking_lot::RwLock;
use std::sync::Arc;

/// Redis-compatible List backed by `VecDeque`.
pub struct RedisList {
    items: VecDeque<Bytes>,
}

impl RedisList {
    pub fn new() -> Self {
        Self {
            items: VecDeque::new(),
        }
    }

    /// Push values to the head (left). Returns new length.
    pub fn lpush(&mut self, values: impl IntoIterator<Item = Bytes>) -> usize {
        for v in values {
            self.items.push_front(v);
        }
        self.items.len()
    }

    /// Push values to the tail (right). Returns new length.
    pub fn rpush(&mut self, values: impl IntoIterator<Item = Bytes>) -> usize {
        for v in values {
            self.items.push_back(v);
        }
        self.items.len()
    }

    pub fn lpop(&mut self) -> Option<Bytes> {
        self.items.pop_front()
    }

    pub fn rpop(&mut self) -> Option<Bytes> {
        self.items.pop_back()
    }

    /// Pop up to `count` elements from the left.
    pub fn lpop_count(&mut self, count: usize) -> Vec<Bytes> {
        let n = count.min(self.items.len());
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            if let Some(v) = self.items.pop_front() {
                out.push(v);
            }
        }
        out
    }

    /// Pop up to `count` elements from the right.
    pub fn rpop_count(&mut self, count: usize) -> Vec<Bytes> {
        let n = count.min(self.items.len());
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            if let Some(v) = self.items.pop_back() {
                out.push(v);
            }
        }
        out
    }

    /// Inclusive range with Redis-style negative indices.
    pub fn lrange(&self, start: isize, stop: isize) -> Vec<Bytes> {
        let len = self.items.len() as isize;
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
        self.items
            .iter()
            .skip(start_idx as usize)
            .take((stop_idx - start_idx + 1) as usize)
            .cloned()
            .collect()
    }

    pub fn llen(&self) -> usize {
        self.items.len()
    }

    pub fn lindex(&self, index: isize) -> Option<Bytes> {
        let len = self.items.len() as isize;
        if len == 0 {
            return None;
        }
        let idx = if index < 0 { len + index } else { index };
        if idx < 0 || idx >= len {
            return None;
        }
        self.items.get(idx as usize).cloned()
    }

    /// Set element at index. Returns error if out of range.
    pub fn lset(&mut self, index: isize, value: Bytes) -> Result<(), String> {
        let len = self.items.len() as isize;
        if len == 0 {
            return Err("index out of range".into());
        }
        let idx = if index < 0 { len + index } else { index };
        if idx < 0 || idx >= len {
            return Err("index out of range".into());
        }
        self.items[idx as usize] = value;
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Left-to-right iteration (for persistence / LPUSH rewrite).
    pub fn iter_items(&self) -> impl Iterator<Item = Bytes> + '_ {
        self.items.iter().cloned()
    }

    pub fn memory_size(&self) -> usize {
        let mut size = std::mem::size_of::<Self>();
        for item in &self.items {
            size += std::mem::size_of::<Bytes>() + item.len();
        }
        size
    }
}

impl Default for RedisList {
    fn default() -> Self {
        Self::new()
    }
}

pub type SharedList = Arc<RwLock<RedisList>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_push_pop() {
        let mut list = RedisList::new();
        assert_eq!(list.lpush([Bytes::from("a"), Bytes::from("b")]), 2);
        // LPUSH a then b → head is b
        assert_eq!(list.lrange(0, -1), vec![Bytes::from("b"), Bytes::from("a")]);
        assert_eq!(list.rpush([Bytes::from("c")]), 3);
        assert_eq!(list.lpop(), Some(Bytes::from("b")));
        assert_eq!(list.rpop(), Some(Bytes::from("c")));
        assert_eq!(list.llen(), 1);
    }

    #[test]
    fn test_lindex_lset() {
        let mut list = RedisList::new();
        list.rpush([Bytes::from("x"), Bytes::from("y"), Bytes::from("z")]);
        assert_eq!(list.lindex(1), Some(Bytes::from("y")));
        assert_eq!(list.lindex(-1), Some(Bytes::from("z")));
        list.lset(1, Bytes::from("Y")).unwrap();
        assert_eq!(list.lindex(1), Some(Bytes::from("Y")));
    }
}
