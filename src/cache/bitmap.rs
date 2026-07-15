//! Bitmap operations on string keys (Redis SETBIT/GETBIT/BITCOUNT/BITOP/BITPOS/BITFIELD).

use crate::entry::Entry;
use crate::error::{Error, Result};
use crate::hashmap::EntryAction;
use crate::memory::MemoryCategory;
use bytes::Bytes;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::storage::KeyType;
use super::Cache;

/// Max bit offset Redis allows (512MB string).
const MAX_BIT_OFFSET: u64 = (512 * 1024 * 1024 * 8) - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitOpKind {
    And,
    Or,
    Xor,
    Not,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitfieldOverflow {
    Wrap,
    Sat,
    Fail,
}

#[derive(Debug, Clone, Copy)]
pub enum BitfieldOp {
    Get {
        signed: bool,
        bits: u8,
        offset: u64,
    },
    Set {
        signed: bool,
        bits: u8,
        offset: u64,
        value: i64,
    },
    IncrBy {
        signed: bool,
        bits: u8,
        offset: u64,
        increment: i64,
    },
}

impl Cache {
    /// SETBIT key offset value → previous bit (0/1).
    pub fn setbit(&self, key: &Bytes, offset: u64, bit: u8) -> Result<i64> {
        if bit > 1 {
            return Err(Error::InvalidArgument(
                "bit is not an integer or out of range".into(),
            ));
        }
        if offset > MAX_BIT_OFFSET {
            return Err(Error::InvalidArgument("bit offset is not an integer or out of range".into()));
        }
        self.ensure_string_or_absent(key)?;

        let byte_index = (offset / 8) as usize;
        let bit_index = (offset % 8) as u8;
        let needed_len = byte_index + 1;
        let max_entry_size = self.max_entry_size.load(Ordering::Relaxed);

        // Capacity pre-check for growth
        let existing = self.map.get(key);
        let old_val_len = existing
            .as_ref()
            .filter(|e| !e.is_expired())
            .map(|e| e.value.len())
            .unwrap_or(0);
        let old_size = existing
            .as_ref()
            .filter(|e| !e.is_expired())
            .map(|e| e.size())
            .unwrap_or(0);
        let new_len = needed_len.max(old_val_len);
        let projected = key.len() + new_len + std::mem::size_of::<Entry>();
        if projected > max_entry_size {
            return Err(Error::EntryTooLarge);
        }
        let net = projected.saturating_sub(old_size);
        self.ensure_capacity(net)?;

        enum Out {
            Ok { prev: i64, old_size: usize, new_size: usize },
            TooLarge,
        }

        let outcome = self.map.mutate(key, |current, next_cas| {
            let (mut bytes, expires_at, flags, old_size) = match current {
                Some(entry) if !entry.is_expired() => {
                    let mut v = entry.value.to_vec();
                    if v.len() < needed_len {
                        v.resize(needed_len, 0);
                    }
                    (v, entry.expires_at, entry.flags, entry.size())
                }
                Some(entry) => {
                    let v = vec![0u8; needed_len];
                    (v, None, 0u32, entry.size())
                }
                None => (vec![0u8; needed_len], None, 0u32, 0usize),
            };

            let mask = 1u8 << (7 - bit_index);
            let prev = if bytes[byte_index] & mask != 0 { 1i64 } else { 0i64 };
            if bit == 1 {
                bytes[byte_index] |= mask;
            } else {
                bytes[byte_index] &= !mask;
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
                    prev,
                    old_size,
                    new_size,
                },
            )
        });

        match outcome {
            Out::TooLarge => Err(Error::EntryTooLarge),
            Out::Ok {
                prev,
                old_size,
                new_size,
            } => {
                self.account_replace(old_size, new_size);
                Ok(prev)
            }
        }
    }

    /// GETBIT key offset → 0 or 1.
    pub fn getbit(&self, key: &Bytes, offset: u64) -> Result<i64> {
        if offset > MAX_BIT_OFFSET {
            return Err(Error::InvalidArgument("bit offset is not an integer or out of range".into()));
        }
        match self.key_type(key) {
            KeyType::None => return Ok(0),
            KeyType::String => {}
            _ => return Err(Error::WrongType),
        }
        let Some(entry) = self.map.get(key) else {
            return Ok(0);
        };
        if entry.is_expired() {
            return Ok(0);
        }
        let byte_index = (offset / 8) as usize;
        let bit_index = (offset % 8) as u8;
        if byte_index >= entry.value.len() {
            return Ok(0);
        }
        let mask = 1u8 << (7 - bit_index);
        Ok(if entry.value[byte_index] & mask != 0 {
            1
        } else {
            0
        })
    }

    /// BITCOUNT key [start end] — byte-oriented range (Redis default).
    pub fn bitcount(&self, key: &Bytes, start: Option<i64>, end: Option<i64>) -> Result<i64> {
        match self.key_type(key) {
            KeyType::None => return Ok(0),
            KeyType::String => {}
            _ => return Err(Error::WrongType),
        }
        let Some(entry) = self.map.get(key) else {
            return Ok(0);
        };
        if entry.is_expired() {
            return Ok(0);
        }
        let bytes = &entry.value;
        let len = bytes.len() as i64;
        if len == 0 {
            return Ok(0);
        }
        let (s, e) = match (start, end) {
            (None, None) => (0i64, len - 1),
            (Some(s), None) => {
                let s = normalize_index(s, len);
                (s, len - 1)
            }
            (Some(s), Some(e)) => {
                let s = normalize_index(s, len);
                let e = normalize_index(e, len);
                (s, e)
            }
            (None, Some(_)) => (0, len - 1),
        };
        if s > e || s >= len {
            return Ok(0);
        }
        let e = e.min(len - 1);
        let slice = &bytes[s as usize..=e as usize];
        let mut count = 0i64;
        for &b in slice {
            count += b.count_ones() as i64;
        }
        Ok(count)
    }

    /// BITPOS key bit [start [end]] — first bit set to `bit` (0 or 1). Byte range.
    /// Returns -1 when not found (for bit=1, or bit=0 with explicit end).
    pub fn bitpos(
        &self,
        key: &Bytes,
        bit: u8,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Result<i64> {
        if bit > 1 {
            return Err(Error::InvalidArgument(
                "bit is not an integer or out of range".into(),
            ));
        }
        match self.key_type(key) {
            KeyType::None => {
                // Empty key: bit 0 is at position 0; bit 1 not found
                return Ok(if bit == 0 { 0 } else { -1 });
            }
            KeyType::String => {}
            _ => return Err(Error::WrongType),
        }
        let Some(entry) = self.map.get(key) else {
            return Ok(if bit == 0 { 0 } else { -1 });
        };
        if entry.is_expired() {
            return Ok(if bit == 0 { 0 } else { -1 });
        }
        let bytes = &entry.value;
        let len = bytes.len() as i64;
        if len == 0 {
            return Ok(if bit == 0 { 0 } else { -1 });
        }
        let end_given = end.is_some();
        let (s, e) = match (start, end) {
            (None, None) => (0i64, len - 1),
            (Some(s), None) => {
                let s = normalize_index(s, len);
                (s, len - 1)
            }
            (Some(s), Some(e)) => {
                let s = normalize_index(s, len);
                let e = normalize_index(e, len);
                (s, e)
            }
            (None, Some(e)) => {
                let e = normalize_index(e, len);
                (0, e)
            }
        };
        if s > e || s >= len {
            return Ok(-1);
        }
        let e = e.min(len - 1);
        for bi in s as usize..=e as usize {
            let b = bytes[bi];
            let target = if bit == 1 { b } else { !b };
            if target == 0 {
                continue;
            }
            for bit_i in 0..8u8 {
                let mask = 1u8 << (7 - bit_i);
                let is_set = b & mask != 0;
                if (bit == 1 && is_set) || (bit == 0 && !is_set) {
                    return Ok((bi as u64 * 8 + bit_i as u64) as i64);
                }
            }
        }
        // Redis: searching for 0 without end → bits past string are 0
        if bit == 0 && !end_given {
            return Ok(len * 8);
        }
        Ok(-1)
    }

    /// BITOP op dest key [key ...] → length of dest string.
    pub fn bitop(&self, op: BitOpKind, dest: &Bytes, keys: &[Bytes]) -> Result<i64> {
        if keys.is_empty() {
            return Err(Error::InvalidArgument(
                "wrong number of arguments for 'bitop'".into(),
            ));
        }
        if op == BitOpKind::Not && keys.len() != 1 {
            return Err(Error::InvalidArgument(
                "BITOP NOT must be called with a single source key".into(),
            ));
        }

        // Load all sources (type-check)
        let mut sources: Vec<Vec<u8>> = Vec::with_capacity(keys.len());
        let mut max_len = 0usize;
        for k in keys {
            match self.key_type(k) {
                KeyType::None => {
                    sources.push(Vec::new());
                }
                KeyType::String => {
                    if let Some(e) = self.map.get(k) {
                        if e.is_expired() {
                            sources.push(Vec::new());
                        } else {
                            max_len = max_len.max(e.value.len());
                            sources.push(e.value.to_vec());
                        }
                    } else {
                        sources.push(Vec::new());
                    }
                }
                _ => return Err(Error::WrongType),
            }
        }

        // Dest must be string or absent (may overwrite)
        self.ensure_string_or_absent(dest)?;

        let mut result = vec![0u8; max_len];
        match op {
            BitOpKind::Not => {
                let src = &sources[0];
                // NOT of missing key → empty dest
                result.resize(src.len(), 0);
                for (i, b) in src.iter().enumerate() {
                    result[i] = !*b;
                }
            }
            BitOpKind::And | BitOpKind::Or | BitOpKind::Xor => {
                if max_len == 0 {
                    result.clear();
                } else {
                    // Init from first key (pad with 0)
                    let s0 = &sources[0];
                    for i in 0..max_len {
                        result[i] = if i < s0.len() { s0[i] } else { 0 };
                    }
                    for src in sources.iter().skip(1) {
                        for i in 0..max_len {
                            let b = if i < src.len() { src[i] } else { 0 };
                            match op {
                                BitOpKind::And => result[i] &= b,
                                BitOpKind::Or => result[i] |= b,
                                BitOpKind::Xor => result[i] ^= b,
                                BitOpKind::Not => unreachable!(),
                            }
                        }
                    }
                }
            }
        }

        let new_len = result.len() as i64;
        let max_entry_size = self.max_entry_size.load(Ordering::Relaxed);
        let projected = dest.len() + result.len() + std::mem::size_of::<Entry>();
        if projected > max_entry_size {
            return Err(Error::EntryTooLarge);
        }

        let old = self.map.get(dest);
        let old_size = old
            .as_ref()
            .filter(|e| !e.is_expired())
            .map(|e| e.size())
            .unwrap_or(0);
        // If empty result, delete dest (Redis BITOP empty → delete)
        if result.is_empty() {
            if old_size > 0 {
                let _ = self.map.mutate(dest, |_c, _cas| (EntryAction::Remove, ()));
                self.account_replace(old_size, 0);
            }
            return Ok(0);
        }

        let net = projected.saturating_sub(old_size);
        self.ensure_capacity(net)?;

        let outcome = self.map.mutate(dest, |current, next_cas| {
            let old_size = match current {
                Some(e) if !e.is_expired() => e.size(),
                Some(e) => e.size(),
                None => 0,
            };
            let expires_at = current
                .filter(|e| !e.is_expired())
                .and_then(|e| e.expires_at);
            // BITOP does not preserve TTL in Redis — clears expire
            let _ = expires_at;
            let entry = Entry::new(dest.clone(), Bytes::from(result.clone())).with_cas(next_cas);
            let new_size = entry.size();
            (
                EntryAction::Set(Arc::new(entry)),
                (old_size, new_size),
            )
        });
        self.account_replace(outcome.0, outcome.1);
        Ok(new_len)
    }

    /// BITFIELD — GET/SET/INCRBY with OVERFLOW WRAP|SAT|FAIL.
    /// Returns one integer (or null on FAIL) per GET/SET/INCRBY op.
    pub fn bitfield(
        &self,
        key: &Bytes,
        ops: &[BitfieldOp],
        mut overflow: BitfieldOverflow,
    ) -> Result<Vec<Option<i64>>> {
        if ops.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_string_or_absent(key)?;

        // Compute max bit extent needed for writes
        let mut max_bit_end = 0u64;
        let mut any_write = false;
        for op in ops {
            let (bits, offset, is_write) = match *op {
                BitfieldOp::Get { bits, offset, .. } => (bits, offset, false),
                BitfieldOp::Set {
                    bits, offset, ..
                } => (bits, offset, true),
                BitfieldOp::IncrBy {
                    bits, offset, ..
                } => (bits, offset, true),
            };
            if bits == 0 || bits > 64 || (!matches!(op, BitfieldOp::Get { signed: false, .. }) && bits == 64 && matches!(op, BitfieldOp::Get { signed: false, bits: 64, .. })) {
                // validated by caller typically
            }
            if bits == 0 || bits > 64 {
                return Err(Error::InvalidArgument("invalid bitfield type".into()));
            }
            // u64 not supported
            if let BitfieldOp::Get { signed: false, bits: b, .. }
            | BitfieldOp::Set { signed: false, bits: b, .. }
            | BitfieldOp::IncrBy { signed: false, bits: b, .. } = op
            {
                if *b > 63 {
                    return Err(Error::InvalidArgument("invalid bitfield type".into()));
                }
            }
            let end = offset.saturating_add(bits as u64);
            if end > max_bit_end {
                max_bit_end = end;
            }
            if is_write {
                any_write = true;
            }
            let _ = overflow; // per-op overflow may change; handled below
        }

        let needed_len = ((max_bit_end + 7) / 8) as usize;
        let max_entry_size = self.max_entry_size.load(Ordering::Relaxed);

        if any_write {
            let existing = self.map.get(key);
            let old_val_len = existing
                .as_ref()
                .filter(|e| !e.is_expired())
                .map(|e| e.value.len())
                .unwrap_or(0);
            let old_size = existing
                .as_ref()
                .filter(|e| !e.is_expired())
                .map(|e| e.size())
                .unwrap_or(0);
            let new_len = needed_len.max(old_val_len);
            let projected = key.len() + new_len + std::mem::size_of::<Entry>();
            if projected > max_entry_size {
                return Err(Error::EntryTooLarge);
            }
            let net = projected.saturating_sub(old_size);
            self.ensure_capacity(net)?;
        }

        // Re-parse overflow as stateful across ops: Redis OVERFLOW applies to subsequent INCRBY/SET
        // We'll accept overflow baked into a parallel slice via mutating a local when decoding ops.
        // Here ops are already split; caller passes current overflow per op by expanding.
        // For simplicity, we track overflow changes only if ops include overflow markers —
        // our command layer expands OVERFLOW into the starting overflow and can split.
        // Actually command layer will pass ops with a single starting overflow and may call
        // multiple times, OR we embed overflow in IncrBy/Set. Let's handle via external
        // `overflow` starting value and allow command layer to call with sequential groups.
        // Simpler: command layer flattens and we accept `ops` only as GET/SET/INCRBY;
        // overflow is a parameter that applies to all SET/INCRBY in this call.
        let _ = &mut overflow;

        enum Out {
            Ok {
                replies: Vec<Option<i64>>,
                old_size: usize,
                new_size: usize,
                wrote: bool,
            },
            TooLarge,
        }

        let ops = ops.to_vec();
        let overflow_mode = overflow;

        let outcome = self.map.mutate(key, |current, next_cas| {
            let (mut bytes, expires_at, flags, old_size) = match current {
                Some(entry) if !entry.is_expired() => {
                    let mut v = entry.value.to_vec();
                    if any_write && v.len() < needed_len {
                        v.resize(needed_len, 0);
                    }
                    (v, entry.expires_at, entry.flags, entry.size())
                }
                Some(entry) => {
                    let v = if any_write {
                        vec![0u8; needed_len]
                    } else {
                        Vec::new()
                    };
                    (v, None, 0u32, entry.size())
                }
                None => {
                    let v = if any_write {
                        vec![0u8; needed_len]
                    } else {
                        Vec::new()
                    };
                    (v, None, 0u32, 0usize)
                }
            };

            let mut replies = Vec::with_capacity(ops.len());
            let mut wrote = false;
            let mut ovf = overflow_mode;

            for op in &ops {
                match *op {
                    BitfieldOp::Get {
                        signed,
                        bits,
                        offset,
                    } => {
                        let v = get_bits(&bytes, offset, bits, signed);
                        replies.push(Some(v));
                    }
                    BitfieldOp::Set {
                        signed,
                        bits,
                        offset,
                        value,
                    } => {
                        let old = get_bits(&bytes, offset, bits, signed);
                        let (stored, ok) = apply_overflow(value, bits, signed, ovf, true);
                        if !ok {
                            replies.push(None);
                        } else {
                            ensure_len(&mut bytes, offset, bits);
                            set_bits(&mut bytes, offset, bits, stored as u64, bits);
                            wrote = true;
                            replies.push(Some(old));
                        }
                    }
                    BitfieldOp::IncrBy {
                        signed,
                        bits,
                        offset,
                        increment,
                    } => {
                        let cur = get_bits(&bytes, offset, bits, signed);
                        // checked add in i128 then overflow handling
                        let sum = (cur as i128) + (increment as i128);
                        let (stored, ok) = apply_overflow_i128(sum, bits, signed, ovf);
                        if !ok {
                            replies.push(None);
                        } else {
                            ensure_len(&mut bytes, offset, bits);
                            set_bits(&mut bytes, offset, bits, stored as u64, bits);
                            wrote = true;
                            replies.push(Some(stored));
                        }
                    }
                }
                let _ = &mut ovf;
            }

            if !wrote {
                return (
                    EntryAction::Keep,
                    Out::Ok {
                        replies,
                        old_size: 0,
                        new_size: 0,
                        wrote: false,
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
                    replies,
                    old_size,
                    new_size,
                    wrote: true,
                },
            )
        });

        match outcome {
            Out::TooLarge => Err(Error::EntryTooLarge),
            Out::Ok {
                replies,
                old_size,
                new_size,
                wrote,
            } => {
                if wrote {
                    self.account_replace(old_size, new_size);
                }
                Ok(replies)
            }
        }
    }
}

fn normalize_index(idx: i64, len: i64) -> i64 {
    if idx < 0 {
        (len + idx).max(0)
    } else {
        idx
    }
}

fn ensure_len(bytes: &mut Vec<u8>, offset: u64, bits: u8) {
    let need = ((offset + bits as u64 + 7) / 8) as usize;
    if bytes.len() < need {
        bytes.resize(need, 0);
    }
}

/// Read up to 64 bits big-endian from bit offset.
fn get_bits(bytes: &[u8], offset: u64, bits: u8, signed: bool) -> i64 {
    let mut value: u64 = 0;
    for i in 0..bits {
        let bit_pos = offset + i as u64;
        let bi = (bit_pos / 8) as usize;
        let bit_i = (bit_pos % 8) as u8;
        let b = if bi < bytes.len() { bytes[bi] } else { 0 };
        let bit = if b & (1u8 << (7 - bit_i)) != 0 { 1u64 } else { 0u64 };
        value = (value << 1) | bit;
    }
    if signed && bits < 64 {
        let sign_bit = 1u64 << (bits - 1);
        if value & sign_bit != 0 {
            // sign extend
            let mask = (1u64 << bits) - 1;
            let neg = value | !mask;
            return neg as i64;
        }
    } else if signed && bits == 64 {
        return value as i64;
    }
    value as i64
}

fn set_bits(bytes: &mut [u8], offset: u64, bits: u8, value: u64, width: u8) {
    let mask = if width >= 64 {
        u64::MAX
    } else {
        (1u64 << width) - 1
    };
    let value = value & mask;
    for i in 0..bits {
        let bit_pos = offset + i as u64;
        let bi = (bit_pos / 8) as usize;
        let bit_i = (bit_pos % 8) as u8;
        let shift = bits - 1 - i;
        let bit = (value >> shift) & 1;
        let m = 1u8 << (7 - bit_i);
        if bit == 1 {
            bytes[bi] |= m;
        } else {
            bytes[bi] &= !m;
        }
    }
}

fn apply_overflow(
    value: i64,
    bits: u8,
    signed: bool,
    mode: BitfieldOverflow,
    _is_set: bool,
) -> (i64, bool) {
    apply_overflow_i128(value as i128, bits, signed, mode)
}

fn apply_overflow_i128(
    value: i128,
    bits: u8,
    signed: bool,
    mode: BitfieldOverflow,
) -> (i64, bool) {
    let (min, max) = if signed {
        let max = (1i128 << (bits - 1)) - 1;
        let min = -(1i128 << (bits - 1));
        (min, max)
    } else {
        let max = if bits >= 64 {
            u64::MAX as i128
        } else {
            (1i128 << bits) - 1
        };
        (0i128, max)
    };

    if value >= min && value <= max {
        return (value as i64, true);
    }

    match mode {
        BitfieldOverflow::Fail => (0, false),
        BitfieldOverflow::Sat => {
            let v = if value > max { max } else { min };
            (v as i64, true)
        }
        BitfieldOverflow::Wrap => {
            if signed {
                let range = 1i128 << bits;
                let mut v = value % range;
                if v < 0 {
                    v += range;
                }
                // convert unsigned residue to signed
                if v >= (1i128 << (bits - 1)) {
                    v -= range;
                }
                (v as i64, true)
            } else {
                let range = if bits >= 64 {
                    // u64 wrap via cast
                    return ((value as u64) as i64, true);
                } else {
                    1i128 << bits
                };
                let mut v = value % range;
                if v < 0 {
                    v += range;
                }
                (v as i64, true)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::Cache;

    #[test]
    fn setbit_getbit_roundtrip() {
        let c = Cache::new_with_sweep(4, 10 * 1024 * 1024, 1024 * 1024, false);
        let k = Bytes::from_static(b"b");
        assert_eq!(c.setbit(&k, 0, 1).unwrap(), 0);
        assert_eq!(c.getbit(&k, 0).unwrap(), 1);
        assert_eq!(c.setbit(&k, 0, 1).unwrap(), 1);
        assert_eq!(c.bitcount(&k, None, None).unwrap(), 1);
    }
}
