//! Lua script cache (SCRIPT LOAD / EVALSHA) and SHA1 helpers.
//!
//! Shared server-wide so SCRIPT LOAD on one connection is visible to others.

use parking_lot::Mutex;
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::sync::Arc;

/// SHA1 hex digest of a Lua script body (lowercase, Redis-compatible).
pub fn script_sha1(script: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(script.as_bytes());
    hex::encode(hasher.finalize())
}

/// In-memory SCRIPT LOAD cache keyed by lowercase SHA1 hex.
#[derive(Debug, Default)]
pub struct ScriptCache {
    scripts: Mutex<HashMap<String, String>>,
}

impl ScriptCache {
    pub fn new() -> Self {
        Self {
            scripts: Mutex::new(HashMap::new()),
        }
    }

    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Insert (or re-insert) a script; returns its SHA1 hex.
    pub fn load(&self, script: &str) -> String {
        let sha = script_sha1(script);
        self.scripts.lock().insert(sha.clone(), script.to_string());
        sha
    }

    /// Look up script source by SHA1 hex (case-insensitive).
    pub fn get(&self, sha: &str) -> Option<String> {
        let key = sha.to_ascii_lowercase();
        self.scripts.lock().get(&key).cloned()
    }

    /// SCRIPT EXISTS: 1 if present, 0 otherwise (order preserved).
    pub fn exists(&self, shas: &[String]) -> Vec<i64> {
        let map = self.scripts.lock();
        shas.iter()
            .map(|s| {
                if map.contains_key(&s.to_ascii_lowercase()) {
                    1
                } else {
                    0
                }
            })
            .collect()
    }

    /// SCRIPT FLUSH — drop all cached scripts.
    pub fn flush(&self) {
        self.scripts.lock().clear();
    }

    pub fn len(&self) -> usize {
        self.scripts.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.scripts.lock().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha1_stable() {
        // echo -n 'return 1' | sha1sum
        let sha = script_sha1("return 1");
        assert_eq!(sha.len(), 40);
        assert_eq!(sha, script_sha1("return 1"));
        assert_ne!(sha, script_sha1("return 2"));
    }

    #[test]
    fn load_get_exists_flush() {
        let c = ScriptCache::new();
        let body = "return redis.call('GET', KEYS[1])";
        let sha = c.load(body);
        assert_eq!(c.get(&sha).as_deref(), Some(body));
        assert!(c.get(&sha.to_uppercase()).is_some());
        assert_eq!(c.exists(&[sha.clone(), "deadbeef".into()]), vec![1, 0]);
        c.flush();
        assert!(c.is_empty());
        assert_eq!(c.exists(&[sha]), vec![0]);
    }
}
