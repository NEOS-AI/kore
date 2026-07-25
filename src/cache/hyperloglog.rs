//! HyperLogLog (PFADD / PFCOUNT / PFMERGE) on string keys.
//!
//! Dense format (Kore-native, not Redis RDB wire-compatible):
//!   magic "KHLL" (4) + version u8 (1) + 16384 × u8 registers
//!
//! Algorithm: classic HLL with p=14 (m=16384), harmonic mean estimate.

use crate::entry::Entry;
use crate::error::{Error, Result};
use crate::hashmap::EntryAction;
use bytes::Bytes;
use std::hash::{Hash, Hasher};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use ahash::AHasher;

use super::storage::KeyType;
use super::Cache;

const HLL_P: u32 = 14;
const HLL_M: usize = 1 << HLL_P; // 16384
const HLL_HEADER: usize = 5; // magic(4) + version(1)
const HLL_SIZE: usize = HLL_HEADER + HLL_M;
const HLL_MAGIC: &[u8; 4] = b"KHLL";
const HLL_VERSION: u8 = 1;

/// Alpha_m * m^2 constant for p=14.
fn alpha_m_m2() -> f64 {
    // α_16k ≈ 0.7213 / (1 + 1.079/m)
    let m = HLL_M as f64;
    let alpha = 0.7213 / (1.0 + 1.079 / m);
    alpha * m * m
}

fn hash64(element: &[u8]) -> u64 {
    let mut h = AHasher::default();
    element.hash(&mut h);
    h.finish()
}

/// rho = 1 + leading zeros of the remaining bits after index (plus 1 for the stop bit).
fn rho(hash: u64) -> u8 {
    // Use lower p bits for index; remaining for leading-zero count.
    let w = hash >> HLL_P;
    // Number of leading zeros in the (64-p)-bit value, plus 1.
    let width = 64 - HLL_P;
    if w == 0 {
        return (width + 1) as u8;
    }
    // Count leading zeros within the lower `width` bits of w.
    let shift = 64 - width;
    let aligned = w << shift;
    (aligned.leading_zeros() + 1) as u8
}

fn index(hash: u64) -> usize {
    (hash & ((HLL_M as u64) - 1)) as usize
}

fn empty_hll() -> Vec<u8> {
    let mut v = vec![0u8; HLL_SIZE];
    v[0..4].copy_from_slice(HLL_MAGIC);
    v[4] = HLL_VERSION;
    v
}

fn is_hll(bytes: &[u8]) -> bool {
    bytes.len() >= HLL_HEADER && &bytes[0..4] == HLL_MAGIC && bytes[4] == HLL_VERSION
}

fn registers_mut(bytes: &mut [u8]) -> &mut [u8] {
    &mut bytes[HLL_HEADER..HLL_HEADER + HLL_M]
}

fn registers(bytes: &[u8]) -> &[u8] {
    &bytes[HLL_HEADER..HLL_HEADER + HLL_M]
}

fn estimate(regs: &[u8]) -> u64 {
    let m = HLL_M as f64;
    let mut sum = 0.0f64;
    let mut zeros = 0u64;
    for &r in regs {
        sum += 2f64.powi(-(r as i32));
        if r == 0 {
            zeros += 1;
        }
    }
    let mut e = alpha_m_m2() / sum;
    // Small range correction
    if e <= 2.5 * m {
        if zeros > 0 {
            e = m * (m / zeros as f64).ln();
        }
    }
    // Large range correction (optional for 64-bit hash space)
    let two64 = 2f64.powi(64);
    if e > two64 / 30.0 {
        e = -two64 * (1.0 - e / two64).ln();
    }
    e.round().max(0.0) as u64
}

fn merge_regs(dest: &mut [u8], src: &[u8]) -> bool {
    let mut changed = false;
    for i in 0..HLL_M {
        if src[i] > dest[i] {
            dest[i] = src[i];
            changed = true;
        }
    }
    changed
}

impl Cache {
    /// Load HLL registers from a key; None if missing. Err on wrong type / corrupt non-HLL string.
    fn hll_load(&self, key: &Bytes) -> Result<Option<Vec<u8>>> {
        match self.key_type(key) {
            KeyType::None => Ok(None),
            KeyType::String => {
                let Some(entry) = self.get_string_entry(key) else {
                    return Ok(None);
                };
                if entry.is_expired() {
                    return Ok(None);
                }
                if !is_hll(&entry.value) {
                    return Err(Error::InvalidArgument(
                        "WRONGTYPE Key is not a valid HyperLogLog string value.".into(),
                    ));
                }
                // Ensure full size
                let mut v = entry.value.to_vec();
                if v.len() < HLL_SIZE {
                    v.resize(HLL_SIZE, 0);
                    v[0..4].copy_from_slice(HLL_MAGIC);
                    v[4] = HLL_VERSION;
                }
                Ok(Some(v))
            }
            _ => Err(Error::WrongType),
        }
    }

    /// PFADD key element [element ...] → 1 if any register changed, else 0.
    pub fn pfadd(&self, key: &Bytes, elements: &[Bytes]) -> Result<i64> {
        self.ensure_string_or_absent(key)?;
        // Existing non-HLL string is an error
        if let Some(entry) = self.get_string_entry(key) {
            if !entry.is_expired() && !entry.value.is_empty() && !is_hll(&entry.value) {
                return Err(Error::InvalidArgument(
                    "WRONGTYPE Key is not a valid HyperLogLog string value.".into(),
                ));
            }
        }

        let max_entry_size = self.max_entry_size.load(Ordering::Relaxed);
        let projected = key.len() + HLL_SIZE + std::mem::size_of::<Entry>();
        if projected > max_entry_size {
            return Err(Error::EntryTooLarge);
        }
        let old = self.get_string_entry(key);
        let old_size = old
            .as_ref()
            .filter(|e| !e.is_expired())
            .map(|e| e.size())
            .unwrap_or(0);
        let net = projected.saturating_sub(old_size);
        self.ensure_capacity(net)?;

        let elems: Vec<Bytes> = elements.to_vec();

        enum Out {
            Ok { changed: i64, old_size: usize, new_size: usize },
            BadType,
            TooLarge,
        }

        let outcome = match self.mutate_string(key, |current, next_cas| {
            let (mut bytes, expires_at, flags, old_size) = match current {
                Some(entry) if !entry.is_expired() => {
                    if !entry.value.is_empty() && !is_hll(&entry.value) {
                        return (EntryAction::Keep, Out::BadType);
                    }
                    let v = if is_hll(&entry.value) {
                        let mut v = entry.value.to_vec();
                        if v.len() < HLL_SIZE {
                            v.resize(HLL_SIZE, 0);
                        }
                        v
                    } else {
                        empty_hll()
                    };
                    (v, entry.expires_at, entry.flags, entry.size())
                }
                Some(entry) => (empty_hll(), None, 0u32, entry.size()),
                None => (empty_hll(), None, 0u32, 0usize),
            };

            let regs = registers_mut(&mut bytes);
            let mut changed = 0i64;
            for el in &elems {
                let h = hash64(el);
                let idx = index(h);
                let r = rho(h);
                if r > regs[idx] {
                    regs[idx] = r;
                    changed = 1;
                }
            }

            if changed == 0 && old_size > 0 {
                // No structural change — keep
                return (
                    EntryAction::Keep,
                    Out::Ok {
                        changed: 0,
                        old_size: 0,
                        new_size: 0,
                    },
                );
            }

            let entry_size = key.len() + bytes.len() + std::mem::size_of::<Entry>();
            if entry_size > max_entry_size {
                return (EntryAction::Keep, Out::TooLarge);
            }

            let mut entry = Entry::new(key.clone(), Bytes::from(bytes));
            entry.expires_at = expires_at;
            entry = entry.with_flags(flags).with_cas(next_cas);
            let new_size = entry.size();
            (
                EntryAction::Set(Arc::new(entry)),
                Out::Ok {
                    changed,
                    old_size,
                    new_size,
                },
            )
        }) {
            Ok(o) => o,
            Err(e) => return Err(e),
        };

        match outcome {
            Out::BadType => Err(Error::InvalidArgument(
                "WRONGTYPE Key is not a valid HyperLogLog string value.".into(),
            )),
            Out::TooLarge => Err(Error::EntryTooLarge),
            Out::Ok {
                changed,
                old_size,
                new_size,
            } => {
                if new_size > 0 || old_size > 0 {
                    // only account when we wrote
                    if changed == 1 || (old_size == 0 && new_size > 0) {
                        self.account_replace(old_size, new_size);
                    } else if new_size > 0 {
                        self.account_replace(old_size, new_size);
                    }
                }
                // Always create key on PFADD even with no elements? Redis creates empty HLL.
                // With elements and changed=0 but key was missing we still write empty then update.
                Ok(changed)
            }
        }
    }

    /// PFCOUNT key [key ...] → estimated cardinality (union if multiple).
    pub fn pfcount(&self, keys: &[Bytes]) -> Result<i64> {
        if keys.is_empty() {
            return Err(Error::InvalidArgument(
                "wrong number of arguments for 'pfcount'".into(),
            ));
        }
        if keys.len() == 1 {
            return match self.hll_load(&keys[0])? {
                None => Ok(0),
                Some(v) => Ok(estimate(registers(&v)) as i64),
            };
        }
        // Union estimate
        let mut acc = empty_hll();
        let acc_regs = registers_mut(&mut acc);
        let mut any = false;
        for k in keys {
            if let Some(v) = self.hll_load(k)? {
                merge_regs(acc_regs, registers(&v));
                any = true;
            }
        }
        if !any {
            return Ok(0);
        }
        Ok(estimate(acc_regs) as i64)
    }

    /// PFMERGE dest source [source ...] → OK semantics; returns dest cardinality estimate length not needed — Redis returns OK.
    /// We return the dest register storage length for consistency with internal callers; command layer returns OK.
    pub fn pfmerge(&self, dest: &Bytes, sources: &[Bytes]) -> Result<()> {
        if sources.is_empty() {
            return Err(Error::InvalidArgument(
                "wrong number of arguments for 'pfmerge'".into(),
            ));
        }
        self.ensure_string_or_absent(dest)?;
        if let Some(entry) = self.get_string_entry(dest) {
            if !entry.is_expired() && !entry.value.is_empty() && !is_hll(&entry.value) {
                return Err(Error::InvalidArgument(
                    "WRONGTYPE Key is not a valid HyperLogLog string value.".into(),
                ));
            }
        }

        let mut merged = empty_hll();
        {
            let regs = registers_mut(&mut merged);
            // Start from dest if present
            if let Some(v) = self.hll_load(dest)? {
                merge_regs(regs, registers(&v));
            }
            for s in sources {
                if let Some(v) = self.hll_load(s)? {
                    merge_regs(regs, registers(&v));
                }
            }
        }

        let max_entry_size = self.max_entry_size.load(Ordering::Relaxed);
        let projected = dest.len() + HLL_SIZE + std::mem::size_of::<Entry>();
        if projected > max_entry_size {
            return Err(Error::EntryTooLarge);
        }
        let old = self.get_string_entry(dest);
        let old_size = old
            .as_ref()
            .filter(|e| !e.is_expired())
            .map(|e| e.size())
            .unwrap_or(0);
        let net = projected.saturating_sub(old_size);
        self.ensure_capacity(net)?;

        let outcome = match self.mutate_string(dest, |current, next_cas| {
            let old_size = match current {
                Some(e) => e.size(),
                None => 0,
            };
            let entry = Entry::new(dest.clone(), Bytes::from(merged.clone())).with_cas(next_cas);
            let new_size = entry.size();
            (EntryAction::Set(Arc::new(entry)), (old_size, new_size))
        }) {
            Ok(o) => o,
            Err(e) => return Err(e),
        };
        self.account_replace(outcome.0, outcome.1);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::Cache;

    #[test]
    fn pfadd_pfcount_basic() {
        let c = Cache::new_with_sweep(4, 50 * 1024 * 1024, 10 * 1024 * 1024, false);
        let k = Bytes::from_static(b"h");
        assert_eq!(c.pfadd(&k, &[Bytes::from_static(b"a")]).unwrap(), 1);
        assert_eq!(c.pfadd(&k, &[Bytes::from_static(b"a")]).unwrap(), 0);
        let n = c.pfcount(&[k.clone()]).unwrap();
        assert!(n >= 1 && n <= 3, "estimate={n}");
    }
}
