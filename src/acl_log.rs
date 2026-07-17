//! Redis-compatible ACL security log (ACL LOG GET/LEN/RESET).
//!
//! Records denied commands (NOPERM) for operators. Ring size is
//! `acllog-max-len` (default 128), configurable via CONFIG SET.

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Default ring capacity (Redis default).
pub const DEFAULT_MAX_LEN: usize = 128;

#[derive(Clone, Debug)]
pub struct AclLogEntry {
    pub id: u64,
    /// How many times this entry was counted (aggregation; we always use 1).
    pub count: u64,
    /// Reason: "command", "key", "channel", or "auth".
    pub reason: String,
    /// Context: "toplevel", "multi", "lua", "module", "idle-callback" — we use toplevel.
    pub context: String,
    /// Command name or key/channel that caused the denial.
    pub object: String,
    pub username: String,
    /// Unix timestamp (seconds) when created.
    pub timestamp_created: i64,
    /// Client id if known.
    pub client_id: usize,
}

pub struct AclLog {
    max_len: AtomicUsize,
    next_id: AtomicU64,
    entries: Mutex<VecDeque<AclLogEntry>>,
}

impl Default for AclLog {
    fn default() -> Self {
        Self::new()
    }
}

impl AclLog {
    pub fn new() -> Self {
        Self {
            max_len: AtomicUsize::new(DEFAULT_MAX_LEN),
            next_id: AtomicU64::new(0),
            entries: Mutex::new(VecDeque::new()),
        }
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

    pub fn push(
        &self,
        reason: &str,
        object: &str,
        username: &str,
        client_id: usize,
    ) {
        let max_len = self.max_len();
        if max_len == 0 {
            return;
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let entry = AclLogEntry {
            id,
            count: 1,
            reason: reason.to_string(),
            context: "toplevel".to_string(),
            object: object.to_string(),
            username: username.to_string(),
            timestamp_created: timestamp,
            client_id,
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
    pub fn get(&self, count: usize) -> Vec<AclLogEntry> {
        let g = self.entries.lock();
        g.iter().take(count).cloned().collect()
    }
}

/// Build a RESP2 flat field array for one ACL LOG entry (Redis-compatible shape).
pub fn entry_to_resp(entry: &AclLogEntry) -> crate::protocol::RespValue {
    use crate::protocol::RespValue;
    let bulk = |s: String| RespValue::BulkString(Some(Bytes::from(s)));
    let bulk_static = |s: &'static str| RespValue::BulkString(Some(Bytes::from_static(s.as_bytes())));
    let age = {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        (now - entry.timestamp_created).max(0) as f64
    };
    let client_info = format!("id={}", entry.client_id);
    RespValue::Array(vec![
        bulk_static("count"),
        RespValue::Integer(entry.count as i64),
        bulk_static("reason"),
        bulk(entry.reason.clone()),
        bulk_static("context"),
        bulk(entry.context.clone()),
        bulk_static("object"),
        bulk(entry.object.clone()),
        bulk_static("username"),
        bulk(entry.username.clone()),
        bulk_static("age-seconds"),
        bulk(format!("{:.3}", age)),
        bulk_static("client-info"),
        bulk(client_info),
        bulk_static("entry-id"),
        RespValue::Integer(entry.id as i64),
        bulk_static("timestamp-created"),
        RespValue::Integer(entry.timestamp_created),
        bulk_static("timestamp-last-updated"),
        RespValue::Integer(entry.timestamp_created),
    ])
}
