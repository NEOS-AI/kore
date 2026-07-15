//! Pluggable Redlock backends: in-process Cache or remote RESP (Kore/Redis).

use crate::cache::Cache;
use crate::entry::{LoadOptions, StoreOptions};
use crate::protocol::{RespParser, RespValue};
use bytes::Bytes;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

/// Operations required by the Redlock algorithm on one independent keyspace.
pub trait LockBackend: Send + Sync {
    /// SET key val NX PX ttl_ms. Returns true if the lock was acquired.
    fn try_acquire(&self, key: &Bytes, val: &Bytes, ttl_ms: u64) -> bool;

    /// Release only if the stored value equals `expected` (GET + DEL race window).
    fn release_if_equal(&self, key: &Bytes, expected: &Bytes);

    /// Extend TTL only if ownership matches.
    fn extend_if_equal(&self, key: &Bytes, expected: &Bytes, ttl_ms: u64) -> bool;

    /// Human-readable backend identity for logs.
    fn label(&self) -> &str;
}

/// In-process Kore `Cache` backend (tests + single-process wiring).
pub struct LocalCacheBackend {
    cache: Arc<Cache>,
    label: String,
}

impl LocalCacheBackend {
    pub fn new(cache: Arc<Cache>) -> Self {
        Self {
            cache,
            label: "local-cache".into(),
        }
    }

    pub fn wrap_all(caches: Vec<Arc<Cache>>) -> Vec<Arc<dyn LockBackend>> {
        caches
            .into_iter()
            .map(|c| Arc::new(LocalCacheBackend::new(c)) as Arc<dyn LockBackend>)
            .collect()
    }
}

impl LockBackend for LocalCacheBackend {
    fn try_acquire(&self, key: &Bytes, val: &Bytes, ttl_ms: u64) -> bool {
        let opts = StoreOptions {
            nx: true,
            ttl_ms: Some(ttl_ms),
            ..Default::default()
        };
        match self.cache.store(key.clone(), val.clone(), opts) {
            Ok(old) => old.is_none(),
            Err(_) => false,
        }
    }

    fn release_if_equal(&self, key: &Bytes, expected: &Bytes) {
        if let Ok(Some(entry)) = self.cache.load(key, LoadOptions::default()) {
            if entry.value == *expected {
                let _ = self.cache.delete(key);
            }
        }
    }

    fn extend_if_equal(&self, key: &Bytes, expected: &Bytes, ttl_ms: u64) -> bool {
        if let Ok(Some(entry)) = self.cache.load(key, LoadOptions::default()) {
            if entry.value == *expected {
                return self.cache.expire(key, ttl_ms).is_ok();
            }
        }
        false
    }

    fn label(&self) -> &str {
        &self.label
    }
}

/// Remote Redis/Kore instance reached over blocking RESP TCP.
///
/// Connects per operation (MVP). Soft-fails on network errors so the Redlock
/// algorithm can treat the instance as unavailable (no quorum).
pub struct RespBackend {
    addr: String,
    io_timeout: Duration,
}

impl RespBackend {
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            io_timeout: Duration::from_millis(500),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.io_timeout = timeout;
        self
    }

    pub fn from_addrs(addrs: &[String]) -> Vec<Arc<dyn LockBackend>> {
        addrs
            .iter()
            .map(|a| Arc::new(RespBackend::new(a.clone())) as Arc<dyn LockBackend>)
            .collect()
    }

    fn connect(&self) -> Option<TcpStream> {
        let sock = self
            .addr
            .to_socket_addrs()
            .ok()?
            .next()?;
        let stream = TcpStream::connect_timeout(&sock, self.io_timeout).ok()?;
        let _ = stream.set_read_timeout(Some(self.io_timeout));
        let _ = stream.set_write_timeout(Some(self.io_timeout));
        let _ = stream.set_nodelay(true);
        Some(stream)
    }

    fn call(&self, parts: &[RespValue]) -> Option<RespValue> {
        let mut stream = self.connect()?;
        let payload = RespValue::Array(parts.to_vec()).serialize();
        stream.write_all(&payload).ok()?;
        stream.flush().ok()?;

        let mut parser = RespParser::new();
        let mut buf = [0u8; 16 * 1024];
        loop {
            if let Some(val) = parser.parse().ok()? {
                return Some(val);
            }
            let n = stream.read(&mut buf).ok()?;
            if n == 0 {
                return None;
            }
            parser.feed(&buf[..n]);
        }
    }
}

impl LockBackend for RespBackend {
    fn try_acquire(&self, key: &Bytes, val: &Bytes, ttl_ms: u64) -> bool {
        let parts = [
            bulk_static(b"SET"),
            RespValue::BulkString(Some(key.clone())),
            RespValue::BulkString(Some(val.clone())),
            bulk_static(b"NX"),
            bulk_static(b"PX"),
            bulk_owned(ttl_ms.to_string()),
        ];
        match self.call(&parts) {
            Some(RespValue::SimpleString(s)) if s.as_ref() == b"OK" => true,
            // null = key exists (NX failed)
            Some(RespValue::BulkString(None)) | Some(RespValue::Null) => false,
            _ => false,
        }
    }

    fn release_if_equal(&self, key: &Bytes, expected: &Bytes) {
        let get = [
            bulk_static(b"GET"),
            RespValue::BulkString(Some(key.clone())),
        ];
        let val = match self.call(&get) {
            Some(RespValue::BulkString(Some(b))) => b,
            _ => return,
        };
        if val != *expected {
            return;
        }
        let del = [
            bulk_static(b"DEL"),
            RespValue::BulkString(Some(key.clone())),
        ];
        let _ = self.call(&del);
    }

    fn extend_if_equal(&self, key: &Bytes, expected: &Bytes, ttl_ms: u64) -> bool {
        let get = [
            bulk_static(b"GET"),
            RespValue::BulkString(Some(key.clone())),
        ];
        let val = match self.call(&get) {
            Some(RespValue::BulkString(Some(b))) => b,
            _ => return false,
        };
        if val != *expected {
            return false;
        }
        let pexpire = [
            bulk_static(b"PEXPIRE"),
            RespValue::BulkString(Some(key.clone())),
            bulk_owned(ttl_ms.to_string()),
        ];
        matches!(self.call(&pexpire), Some(RespValue::Integer(n)) if n == 1)
    }

    fn label(&self) -> &str {
        &self.addr
    }
}

fn bulk_static(s: &'static [u8]) -> RespValue {
    RespValue::BulkString(Some(Bytes::from_static(s)))
}

fn bulk_owned(s: String) -> RespValue {
    RespValue::BulkString(Some(Bytes::from(s)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_backend_acquire_release() {
        let cache = Cache::new_with_sweep(16, 8 * 1024 * 1024, 1024 * 1024, false);
        let b = LocalCacheBackend::new(cache);
        let key = Bytes::from_static(b"lock:t");
        let val = Bytes::from_static(b"v1");
        assert!(b.try_acquire(&key, &val, 5000));
        assert!(!b.try_acquire(&key, &Bytes::from_static(b"v2"), 5000));
        b.release_if_equal(&key, &val);
        assert!(b.try_acquire(&key, &Bytes::from_static(b"v2"), 5000));
    }
}
