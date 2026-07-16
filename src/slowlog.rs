//! Redis-compatible slow log (SLOWLOG GET/LEN/RESET).
//!
//! Entries are recorded when a command takes longer than
//! `slowlog-log-slower-than` microseconds (default 10_000). Set to a
//! negative value to disable. Ring size is `slowlog-max-len` (default 128).

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Default: log commands slower than 10ms.
pub const DEFAULT_SLOWER_THAN_US: i64 = 10_000;
/// Default ring capacity.
pub const DEFAULT_MAX_LEN: usize = 128;

#[derive(Clone, Debug)]
pub struct SlowLogEntry {
    pub id: u64,
    /// Unix time (seconds) when the command was processed.
    pub timestamp: i64,
    /// Execution time in microseconds.
    pub duration_us: i64,
    pub argv: Vec<Bytes>,
}

pub struct SlowLog {
    slower_than_us: AtomicI64,
    max_len: AtomicUsize,
    next_id: AtomicU64,
    entries: Mutex<VecDeque<SlowLogEntry>>,
}

impl Default for SlowLog {
    fn default() -> Self {
        Self::new()
    }
}

impl SlowLog {
    pub fn new() -> Self {
        Self {
            slower_than_us: AtomicI64::new(DEFAULT_SLOWER_THAN_US),
            max_len: AtomicUsize::new(DEFAULT_MAX_LEN),
            next_id: AtomicU64::new(0),
            entries: Mutex::new(VecDeque::new()),
        }
    }

    pub fn slower_than_us(&self) -> i64 {
        self.slower_than_us.load(Ordering::Relaxed)
    }

    pub fn set_slower_than_us(&self, us: i64) {
        self.slower_than_us.store(us, Ordering::Relaxed);
    }

    pub fn max_len(&self) -> usize {
        self.max_len.load(Ordering::Relaxed)
    }

    pub fn set_max_len(&self, n: usize) {
        self.max_len.store(n, Ordering::Relaxed);
        let mut g = self.entries.lock();
        while g.len() > n {
            g.pop_back();
        }
    }

    /// Record if duration exceeds threshold. `argv` is the full command array.
    pub fn maybe_push(&self, duration_us: i64, argv: Vec<Bytes>) {
        let threshold = self.slower_than_us();
        // Negative threshold disables logging (Redis).
        if threshold < 0 || duration_us < threshold {
            return;
        }
        let max_len = self.max_len();
        if max_len == 0 {
            return;
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let entry = SlowLogEntry {
            id,
            timestamp,
            duration_us,
            argv,
        };
        let mut g = self.entries.lock();
        g.push_front(entry);
        while g.len() > max_len {
            g.pop_back();
        }
    }

    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    pub fn reset(&self) {
        self.entries.lock().clear();
    }

    /// Newest-first, up to `count` entries (default Redis: 10).
    pub fn get(&self, count: usize) -> Vec<SlowLogEntry> {
        let g = self.entries.lock();
        g.iter().take(count).cloned().collect()
    }
}
