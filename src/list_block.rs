//! Blocking list waiters for BLPOP / BRPOP.
//!
//! Clients waiting on empty lists register here; LPUSH/RPUSH notify waiters
//! so they can re-attempt a pop (FIFO fairness is best-effort via registration order).

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

struct Waiter {
    id: u64,
    notify: Arc<Notify>,
    active: AtomicBool,
}

/// Per-keyspace registry of clients blocked on list keys.
pub struct ListBlockers {
    next_id: AtomicU64,
    /// key → FIFO of waiters registered on that key
    by_key: Mutex<HashMap<Bytes, VecDeque<Arc<Waiter>>>>,
}

impl ListBlockers {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            by_key: Mutex::new(HashMap::new()),
        }
    }

    /// Register a waiter on every key. Returns (waiter_id, notify).
    pub fn register(&self, keys: &[Bytes]) -> (u64, Arc<Notify>) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let notify = Arc::new(Notify::new());
        let waiter = Arc::new(Waiter {
            id,
            notify: Arc::clone(&notify),
            active: AtomicBool::new(true),
        });
        let mut map = self.by_key.lock();
        for key in keys {
            map.entry(key.clone())
                .or_default()
                .push_back(Arc::clone(&waiter));
        }
        (id, notify)
    }

    /// Drop registrations for this waiter (timeout, success, or disconnect).
    pub fn unregister(&self, id: u64, keys: &[Bytes]) {
        let mut map = self.by_key.lock();
        for key in keys {
            if let Some(q) = map.get_mut(key) {
                // Mark inactive so concurrent notify can skip
                for w in q.iter() {
                    if w.id == id {
                        w.active.store(false, Ordering::Release);
                    }
                }
                q.retain(|w| w.id != id);
                if q.is_empty() {
                    map.remove(key);
                }
            }
        }
    }

    /// Wake waiters blocked on `key` (after a successful push).
    pub fn notify_key(&self, key: &Bytes) {
        let map = self.by_key.lock();
        if let Some(q) = map.get(key) {
            for w in q.iter() {
                if w.active.load(Ordering::Acquire) {
                    w.notify.notify_one();
                }
            }
        }
    }

    /// Number of waiter slots across all keys (for tests / INFO).
    pub fn waiter_slots(&self) -> usize {
        self.by_key.lock().values().map(|q| q.len()).sum()
    }

    /// Approximate count of unique blocked clients (distinct waiter ids).
    pub fn blocked_clients(&self) -> usize {
        let map = self.by_key.lock();
        let mut ids = std::collections::HashSet::new();
        for q in map.values() {
            for w in q {
                if w.active.load(Ordering::Acquire) {
                    ids.insert(w.id);
                }
            }
        }
        ids.len()
    }
}

impl Default for ListBlockers {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_unregister_clears_slots() {
        let b = ListBlockers::new();
        let keys = [Bytes::from("a"), Bytes::from("b")];
        let (id, _n) = b.register(&keys);
        assert_eq!(b.waiter_slots(), 2); // one slot per key
        b.unregister(id, &keys);
        assert_eq!(b.waiter_slots(), 0);
    }

    #[test]
    fn notify_wakes_registered_waiter() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let b = ListBlockers::new();
            let key = Bytes::from("q");
            let (id, notify) = b.register(&[key.clone()]);
            let wake = tokio::spawn(async move {
                notify.notified().await;
            });
            // Give the waiter a chance to park
            tokio::task::yield_now().await;
            b.notify_key(&key);
            wake.await.unwrap();
            b.unregister(id, &[key]);
            assert_eq!(b.waiter_slots(), 0);
        });
    }

    #[test]
    fn notify_unknown_key_is_noop() {
        let b = ListBlockers::new();
        b.notify_key(&Bytes::from("missing"));
        assert_eq!(b.waiter_slots(), 0);
    }

    #[test]
    fn multiple_waiters_same_key_all_notified() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let b = ListBlockers::new();
            let key = Bytes::from("q");
            let (id1, n1) = b.register(&[key.clone()]);
            let (id2, n2) = b.register(&[key.clone()]);
            assert_eq!(b.waiter_slots(), 2);

            let w1 = tokio::spawn(async move {
                n1.notified().await;
            });
            let w2 = tokio::spawn(async move {
                n2.notified().await;
            });
            tokio::task::yield_now().await;
            b.notify_key(&key);
            w1.await.unwrap();
            w2.await.unwrap();

            b.unregister(id1, &[key.clone()]);
            b.unregister(id2, &[key]);
            assert_eq!(b.waiter_slots(), 0);
        });
    }

    #[test]
    fn unregister_one_waiter_leaves_other() {
        let b = ListBlockers::new();
        let keys = [Bytes::from("k")];
        let (id1, _) = b.register(&keys);
        let (id2, _) = b.register(&keys);
        assert_eq!(b.waiter_slots(), 2);
        b.unregister(id1, &keys);
        assert_eq!(b.waiter_slots(), 1);
        b.unregister(id2, &keys);
        assert_eq!(b.waiter_slots(), 0);
    }

    #[test]
    fn register_same_waiter_on_many_keys() {
        let b = ListBlockers::new();
        let keys = [
            Bytes::from("a"),
            Bytes::from("b"),
            Bytes::from("c"),
        ];
        let (id, _) = b.register(&keys);
        assert_eq!(b.waiter_slots(), 3);
        // Partial unregister of one key only
        b.unregister(id, &[Bytes::from("b")]);
        assert_eq!(b.waiter_slots(), 2);
        b.unregister(id, &keys);
        assert_eq!(b.waiter_slots(), 0);
    }
}
