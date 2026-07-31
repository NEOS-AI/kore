//! Redis RDB object codec for DUMP / RESTORE wire compatibility (Batch FY).
//!
//! # DUMP format choice
//!
//! Kore `DUMP` emits **Redis-compatible** payloads for core types:
//! string, list, set, hash, zset (classic RDB type opcodes 0/1/2/4/5).
//! Geo and stream stay on Kore **KDF1** (see `cache::key_xfer`).
//!
//! Payload layout (Redis classic DUMP):
//! ```text
//!   type_opcode:u8 | type-specific encoding | rdb_version:u16_le | crc64:u64_le
//! ```
//!
//! `RESTORE` dual-detects: magic `KDF1` → Kore path; otherwise Redis RDB object
//! decode (classic encodings + common listpack / quicklist2 forms from real Redis).
//!
//! RDB version written: **9** (widely accepted by Redis/Valkey RESTORE).
//! CRC64: Redis Jones poly (reflected `0x95ac9329ac4bc9b5`), init 0, no final xor.

use bytes::Bytes;

/// RDB version embedded in DUMP payloads we produce.
pub const RDB_VERSION: u16 = 9;

// RDB type opcodes (subset).
const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_LIST: u8 = 1;
const RDB_TYPE_SET: u8 = 2;
const RDB_TYPE_ZSET: u8 = 3;
const RDB_TYPE_HASH: u8 = 4;
const RDB_TYPE_ZSET_2: u8 = 5;
const RDB_TYPE_HASH_ZIPMAP: u8 = 9;
const RDB_TYPE_LIST_ZIPLIST: u8 = 10;
const RDB_TYPE_SET_INTSET: u8 = 11;
const RDB_TYPE_ZSET_ZIPLIST: u8 = 12;
const RDB_TYPE_HASH_ZIPLIST: u8 = 13;
const RDB_TYPE_LIST_QUICKLIST: u8 = 14;
const RDB_TYPE_HASH_LISTPACK: u8 = 16;
const RDB_TYPE_ZSET_LISTPACK: u8 = 17;
const RDB_TYPE_LIST_QUICKLIST_2: u8 = 18;
const RDB_TYPE_STREAM_LISTPACKS: u8 = 15;
const RDB_TYPE_SET_LISTPACK: u8 = 20;

// Special string encodings (high 2 bits of length byte == 11).
const RDB_ENC_INT8: u8 = 0;
const RDB_ENC_INT16: u8 = 1;
const RDB_ENC_INT32: u8 = 2;
const RDB_ENC_LZF: u8 = 3;

// Quicklist node container types (quicklist2).
const QUICKLIST_NODE_CONTAINER_PLAIN: u64 = 1;
const QUICKLIST_NODE_CONTAINER_PACKED: u64 = 2;

const ERR: &str = "DUMP payload version or checksum are wrong";

// ─── CRC64 (Redis) ──────────────────────────────────────────────────────────

/// Redis CRC64 polynomial (reflected form of Jones poly).
const POLY64REV: u64 = 0x95ac_9329_ac4b_c9b5;

fn crc64_table() -> [u64; 256] {
    let mut table = [0u64; 256];
    for i in 0..256 {
        let mut crc = i as u64;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ POLY64REV;
            } else {
                crc >>= 1;
            }
        }
        table[i] = crc;
    }
    table
}

/// Redis CRC64 over `data` (init 0, no invert).
pub fn redis_crc64(data: &[u8]) -> u64 {
    // OnceLock would be nicer on newer MSRV; compute table once per process via lazy.
    use std::sync::OnceLock;
    static TABLE: OnceLock<[u64; 256]> = OnceLock::new();
    let table = TABLE.get_or_init(crc64_table);
    let mut crc = 0u64;
    for &b in data {
        crc = table[((crc as u8) ^ b) as usize] ^ (crc >> 8);
    }
    crc
}

// ─── Reader ─────────────────────────────────────────────────────────────────

struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn need(&self, n: usize) -> Result<(), String> {
        if self.pos + n > self.data.len() {
            Err(ERR.into())
        } else {
            Ok(())
        }
    }

    fn u8(&mut self) -> Result<u8, String> {
        self.need(1)?;
        let v = self.data[self.pos];
        self.pos += 1;
        Ok(v)
    }

    fn raw(&mut self, n: usize) -> Result<&'a [u8], String> {
        self.need(n)?;
        let s = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn f64_le(&mut self) -> Result<f64, String> {
        let b = self.raw(8)?;
        Ok(f64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
}

// ─── Length / string encoding ───────────────────────────────────────────────

/// Encode a Redis RDB length.
pub fn encode_len(out: &mut Vec<u8>, n: usize) {
    if n < (1 << 6) {
        out.push(n as u8);
    } else if n < (1 << 14) {
        out.push(((n >> 8) as u8) | 0x40);
        out.push((n & 0xff) as u8);
    } else {
        out.push(0x80);
        out.extend_from_slice(&(n as u32).to_be_bytes());
    }
}

/// Decode a Redis RDB length. Returns `(value, is_encoded)` where `is_encoded`
/// means the value is an RDB_ENC_* special string type tag (not a byte length).
fn decode_len(c: &mut Cursor<'_>) -> Result<(u64, bool), String> {
    let byte = c.u8()?;
    match (byte & 0xC0) >> 6 {
        0 => Ok(((byte & 0x3F) as u64, false)),
        1 => {
            let next = c.u8()?;
            let n = (((byte & 0x3F) as u64) << 8) | (next as u64);
            Ok((n, false))
        }
        2 => {
            let b = c.raw(4)?;
            let n = u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as u64;
            Ok((n, false))
        }
        3 => Ok(((byte & 0x3F) as u64, true)),
        _ => unreachable!(),
    }
}

fn encode_raw_string(out: &mut Vec<u8>, s: &[u8]) {
    encode_len(out, s.len());
    out.extend_from_slice(s);
}

/// Load an RDB string object (length-prefixed, integer-encoded, or LZF).
fn decode_string(c: &mut Cursor<'_>) -> Result<Bytes, String> {
    let (len, encoded) = decode_len(c)?;
    if !encoded {
        if len > (512 * 1024 * 1024) {
            return Err(ERR.into());
        }
        let raw = c.raw(len as usize)?;
        return Ok(Bytes::copy_from_slice(raw));
    }
    match len as u8 {
        RDB_ENC_INT8 => {
            let v = c.u8()? as i8;
            Ok(Bytes::from(v.to_string()))
        }
        RDB_ENC_INT16 => {
            let b = c.raw(2)?;
            let v = i16::from_le_bytes([b[0], b[1]]);
            Ok(Bytes::from(v.to_string()))
        }
        RDB_ENC_INT32 => {
            let b = c.raw(4)?;
            let v = i32::from_le_bytes([b[0], b[1], b[2], b[3]]);
            Ok(Bytes::from(v.to_string()))
        }
        RDB_ENC_LZF => {
            let (clen, enc) = decode_len(c)?;
            if enc {
                return Err(ERR.into());
            }
            let (ulen, enc) = decode_len(c)?;
            if enc || ulen > (512 * 1024 * 1024) || clen > (512 * 1024 * 1024) {
                return Err(ERR.into());
            }
            let compressed = c.raw(clen as usize)?;
            let plain = lzf_decompress(compressed, ulen as usize)?;
            Ok(Bytes::from(plain))
        }
        _ => Err(ERR.into()),
    }
}

/// Minimal LZF decompress (Redis RDB_ENC_LZF).
fn lzf_decompress(input: &[u8], out_len: usize) -> Result<Vec<u8>, String> {
    let mut out = vec![0u8; out_len];
    let mut i = 0usize;
    let mut o = 0usize;
    while i < input.len() {
        let ctrl = input[i] as usize;
        i += 1;
        if ctrl < 32 {
            // literal run: ctrl+1 bytes
            let lit = ctrl + 1;
            if i + lit > input.len() || o + lit > out_len {
                return Err(ERR.into());
            }
            out[o..o + lit].copy_from_slice(&input[i..i + lit]);
            i += lit;
            o += lit;
        } else {
            // back reference
            let mut len = ctrl >> 5;
            let mut ref_off = ((ctrl & 0x1f) << 8) as isize;
            if len == 7 {
                if i >= input.len() {
                    return Err(ERR.into());
                }
                len += input[i] as usize;
                i += 1;
            }
            len += 2;
            if i >= input.len() {
                return Err(ERR.into());
            }
            ref_off |= input[i] as isize;
            i += 1;
            let start = o as isize - ref_off - 1;
            if start < 0 || (start as usize) >= o || o + len > out_len {
                return Err(ERR.into());
            }
            for k in 0..len {
                out[o + k] = out[start as usize + k];
            }
            o += len;
        }
    }
    if o != out_len {
        return Err(ERR.into());
    }
    Ok(out)
}

// ─── Listpack ───────────────────────────────────────────────────────────────

/// Decode a Redis listpack blob into entry values (as bytes).
fn decode_listpack(data: &[u8]) -> Result<Vec<Bytes>, String> {
    if data.len() < 7 {
        return Err(ERR.into());
    }
    let total = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if total != data.len() {
        // Some producers may pass only the payload; still try if EOF present.
        if data.last() != Some(&0xFF) {
            return Err(ERR.into());
        }
    }
    let n_elems = u16::from_le_bytes([data[4], data[5]]) as usize;
    let mut pos = 6usize;
    let mut out = Vec::with_capacity(n_elems);
    while pos < data.len() {
        let b = data[pos];
        if b == 0xFF {
            break;
        }
        let (val, entry_len) = lp_decode_entry(&data[pos..])?;
        // backlen is at end of entry
        let backlen_len = lp_backlen_size(entry_len);
        if pos + entry_len + backlen_len > data.len() {
            return Err(ERR.into());
        }
        out.push(val);
        pos += entry_len + backlen_len;
    }
    Ok(out)
}

/// Size of the backlen trailer for an entry of `entry_len` content bytes.
fn lp_backlen_size(entry_len: usize) -> usize {
    // listpack stores backlen as variable 1-5 bytes encoding entry_len.
    if entry_len <= 127 {
        1
    } else if entry_len < (1 << 14) {
        2
    } else if entry_len < (1 << 21) {
        3
    } else if entry_len < (1 << 28) {
        4
    } else {
        5
    }
}

/// Decode one listpack entry at start of `data` (without backlen).
/// Returns (value, encoding+data length excluding backlen).
fn lp_decode_entry(data: &[u8]) -> Result<(Bytes, usize), String> {
    if data.is_empty() {
        return Err(ERR.into());
    }
    let b0 = data[0];
    // 0xxxxxxx — 7-bit unsigned int
    if b0 & 0x80 == 0 {
        let v = (b0 & 0x7F) as i64;
        return Ok((Bytes::from(v.to_string()), 1));
    }
    // 10xxxxxx — 6-bit string length
    if b0 & 0xC0 == 0x80 {
        let len = (b0 & 0x3F) as usize;
        if data.len() < 1 + len {
            return Err(ERR.into());
        }
        return Ok((Bytes::copy_from_slice(&data[1..1 + len]), 1 + len));
    }
    // 110xxxxx yyyyyyyy — 13-bit signed int
    if b0 & 0xE0 == 0xC0 {
        if data.len() < 2 {
            return Err(ERR.into());
        }
        let mut v = (((b0 as i64) & 0x1F) << 8) | (data[1] as i64);
        if v >= (1 << 12) {
            v -= 1 << 13; // sign extend 13-bit
        }
        return Ok((Bytes::from(v.to_string()), 2));
    }
    // 1110xxxx yyyyyyyy — 12-bit string length
    if b0 & 0xF0 == 0xE0 {
        if data.len() < 2 {
            return Err(ERR.into());
        }
        let len = ((((b0 as usize) & 0x0F) << 8) | (data[1] as usize)) as usize;
        if data.len() < 2 + len {
            return Err(ERR.into());
        }
        return Ok((Bytes::copy_from_slice(&data[2..2 + len]), 2 + len));
    }
    // 1111 xxxx — larger encodings
    match b0 {
        0xF0 => {
            // 32-bit string length
            if data.len() < 5 {
                return Err(ERR.into());
            }
            let len = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
            if data.len() < 5 + len {
                return Err(ERR.into());
            }
            Ok((Bytes::copy_from_slice(&data[5..5 + len]), 5 + len))
        }
        0xF1 => {
            // 16-bit signed int
            if data.len() < 3 {
                return Err(ERR.into());
            }
            let v = i16::from_le_bytes([data[1], data[2]]) as i64;
            Ok((Bytes::from(v.to_string()), 3))
        }
        0xF2 => {
            // 24-bit signed int
            if data.len() < 4 {
                return Err(ERR.into());
            }
            let mut v =
                (data[1] as i64) | ((data[2] as i64) << 8) | ((data[3] as i64) << 16);
            if v >= (1 << 23) {
                v -= 1 << 24;
            }
            Ok((Bytes::from(v.to_string()), 4))
        }
        0xF3 => {
            if data.len() < 5 {
                return Err(ERR.into());
            }
            let v = i32::from_le_bytes([data[1], data[2], data[3], data[4]]) as i64;
            Ok((Bytes::from(v.to_string()), 5))
        }
        0xF4 => {
            if data.len() < 9 {
                return Err(ERR.into());
            }
            let v = i64::from_le_bytes([
                data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
            ]);
            Ok((Bytes::from(v.to_string()), 9))
        }
        _ => Err(ERR.into()),
    }
}

// ─── Ziplist (legacy, best-effort) ──────────────────────────────────────────

/// Best-effort ziplist decode for older Redis DUMP types.
fn decode_ziplist(data: &[u8]) -> Result<Vec<Bytes>, String> {
    if data.len() < 11 {
        return Err(ERR.into());
    }
    // zlbytes(4) zltail(4) zllen(2) entries… zlend(0xFF)
    let n = u16::from_le_bytes([data[8], data[9]]) as usize;
    let mut pos = 10usize;
    let mut out = Vec::with_capacity(n.min(1024));
    while pos < data.len() {
        if data[pos] == 0xFF {
            break;
        }
        // prevlen
        let prevlen_len = if data[pos] == 0xFE {
            if pos + 5 > data.len() {
                return Err(ERR.into());
            }
            5
        } else {
            1
        };
        pos += prevlen_len;
        if pos >= data.len() {
            return Err(ERR.into());
        }
        let enc = data[pos];
        // string encodings
        if enc >> 6 == 0 {
            // 00pppppp — 6 bit len
            let len = (enc & 0x3F) as usize;
            pos += 1;
            if pos + len > data.len() {
                return Err(ERR.into());
            }
            out.push(Bytes::copy_from_slice(&data[pos..pos + len]));
            pos += len;
        } else if enc >> 6 == 1 {
            // 01 — 14 bit len
            if pos + 2 > data.len() {
                return Err(ERR.into());
            }
            let len = (((enc & 0x3F) as usize) << 8) | (data[pos + 1] as usize);
            pos += 2;
            if pos + len > data.len() {
                return Err(ERR.into());
            }
            out.push(Bytes::copy_from_slice(&data[pos..pos + len]));
            pos += len;
        } else if enc >> 6 == 2 {
            // 10 — 32 bit len
            if pos + 5 > data.len() {
                return Err(ERR.into());
            }
            let len = u32::from_be_bytes([data[pos + 1], data[pos + 2], data[pos + 3], data[pos + 4]])
                as usize;
            pos += 5;
            if pos + len > data.len() {
                return Err(ERR.into());
            }
            out.push(Bytes::copy_from_slice(&data[pos..pos + len]));
            pos += len;
        } else {
            // integer encodings 0xC0..
            let (v, step) = match enc {
                0xC0 => {
                    if pos + 3 > data.len() {
                        return Err(ERR.into());
                    }
                    (i16::from_le_bytes([data[pos + 1], data[pos + 2]]) as i64, 3)
                }
                0xD0 => {
                    if pos + 4 > data.len() {
                        return Err(ERR.into());
                    }
                    let v = (data[pos + 1] as i64)
                        | ((data[pos + 2] as i64) << 8)
                        | ((data[pos + 3] as i64) << 16);
                    let v = if v >= (1 << 23) { v - (1 << 24) } else { v };
                    (v, 4)
                }
                0xE0 => {
                    if pos + 5 > data.len() {
                        return Err(ERR.into());
                    }
                    (
                        i32::from_le_bytes([
                            data[pos + 1],
                            data[pos + 2],
                            data[pos + 3],
                            data[pos + 4],
                        ]) as i64,
                        5,
                    )
                }
                0xF0 => {
                    if pos + 9 > data.len() {
                        return Err(ERR.into());
                    }
                    (
                        i64::from_le_bytes([
                            data[pos + 1],
                            data[pos + 2],
                            data[pos + 3],
                            data[pos + 4],
                            data[pos + 5],
                            data[pos + 6],
                            data[pos + 7],
                            data[pos + 8],
                        ]),
                        9,
                    )
                }
                0xFE => {
                    if pos + 2 > data.len() {
                        return Err(ERR.into());
                    }
                    (data[pos + 1] as i8 as i64, 2)
                }
                e if (0xF1..=0xFD).contains(&e) => {
                    // 4-bit immediate: 1..13 → value 0..12
                    ((e - 0xF1) as i64, 1)
                }
                _ => return Err(ERR.into()),
            };
            out.push(Bytes::from(v.to_string()));
            pos += step;
        }
    }
    Ok(out)
}

// ─── Decoded value ─────────────────────────────────────────────────────────

/// Decoded Redis DUMP object (no TTL — RESTORE supplies it).
#[derive(Debug, Clone)]
pub enum RdbObject {
    String(Bytes),
    List(Vec<Bytes>),
    Set(Vec<Bytes>),
    Hash(Vec<(Bytes, Bytes)>),
    ZSet(Vec<(Bytes, f64)>),
}

// ─── Encode (DUMP) ──────────────────────────────────────────────────────────

fn finish_dump(mut body: Vec<u8>) -> Vec<u8> {
    body.extend_from_slice(&RDB_VERSION.to_le_bytes());
    let crc = redis_crc64(&body);
    body.extend_from_slice(&crc.to_le_bytes());
    body
}

/// Encode a string value as Redis DUMP payload.
pub fn encode_string_dump(value: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(value.len() + 16);
    body.push(RDB_TYPE_STRING);
    encode_raw_string(&mut body, value);
    finish_dump(body)
}

/// Encode a list (head→tail order) as Redis DUMP (classic LIST type 1).
pub fn encode_list_dump(elements: &[Bytes]) -> Vec<u8> {
    let mut body = Vec::with_capacity(64);
    body.push(RDB_TYPE_LIST);
    encode_len(&mut body, elements.len());
    for e in elements {
        encode_raw_string(&mut body, e);
    }
    finish_dump(body)
}

/// Encode a set as Redis DUMP (classic SET type 2).
pub fn encode_set_dump(members: &[Bytes]) -> Vec<u8> {
    let mut body = Vec::with_capacity(64);
    body.push(RDB_TYPE_SET);
    encode_len(&mut body, members.len());
    for m in members {
        encode_raw_string(&mut body, m);
    }
    finish_dump(body)
}

/// Encode a hash as Redis DUMP (classic HASH type 4).
pub fn encode_hash_dump(fields: &[(Bytes, Bytes)]) -> Vec<u8> {
    let mut body = Vec::with_capacity(64);
    body.push(RDB_TYPE_HASH);
    encode_len(&mut body, fields.len());
    for (k, v) in fields {
        encode_raw_string(&mut body, k);
        encode_raw_string(&mut body, v);
    }
    finish_dump(body)
}

/// Encode a zset as Redis DUMP (ZSET_2 type 5 — binary f64 scores).
pub fn encode_zset_dump(members: &[(Bytes, f64)]) -> Vec<u8> {
    let mut body = Vec::with_capacity(64);
    body.push(RDB_TYPE_ZSET_2);
    encode_len(&mut body, members.len());
    for (m, score) in members {
        encode_raw_string(&mut body, m);
        body.extend_from_slice(&score.to_le_bytes());
    }
    finish_dump(body)
}

// ─── Decode (RESTORE) ───────────────────────────────────────────────────────

/// Decode a full Redis DUMP blob (type + object + version + crc64).
pub fn decode_redis_dump(data: &[u8]) -> Result<RdbObject, String> {
    // Minimum: 1 type + 2 version + 8 crc
    if data.len() < 11 {
        return Err(ERR.into());
    }
    let crc_stored = u64::from_le_bytes(data[data.len() - 8..].try_into().unwrap());
    let body = &data[..data.len() - 8];
    if redis_crc64(body) != crc_stored {
        return Err(ERR.into());
    }
    if body.len() < 3 {
        return Err(ERR.into());
    }
    let _version = u16::from_le_bytes([body[body.len() - 2], body[body.len() - 1]]);
    // Version is not strictly validated: Redis accepts a range; we accept any
    // after CRC check so older/newer dumps restore.
    let obj_bytes = &body[..body.len() - 2];
    decode_rdb_object(obj_bytes)
}

fn decode_rdb_object(data: &[u8]) -> Result<RdbObject, String> {
    let mut c = Cursor::new(data);
    let ty = c.u8()?;
    let obj = match ty {
        RDB_TYPE_STRING => RdbObject::String(decode_string(&mut c)?),
        RDB_TYPE_LIST => {
            let (n, enc) = decode_len(&mut c)?;
            if enc {
                return Err(ERR.into());
            }
            let mut elements = Vec::with_capacity(n as usize);
            for _ in 0..n {
                elements.push(decode_string(&mut c)?);
            }
            RdbObject::List(elements)
        }
        RDB_TYPE_SET => {
            let (n, enc) = decode_len(&mut c)?;
            if enc {
                return Err(ERR.into());
            }
            let mut members = Vec::with_capacity(n as usize);
            for _ in 0..n {
                members.push(decode_string(&mut c)?);
            }
            RdbObject::Set(members)
        }
        RDB_TYPE_ZSET => {
            let (n, enc) = decode_len(&mut c)?;
            if enc {
                return Err(ERR.into());
            }
            let mut members = Vec::with_capacity(n as usize);
            for _ in 0..n {
                let m = decode_string(&mut c)?;
                let score_s = decode_string(&mut c)?;
                let score: f64 = std::str::from_utf8(&score_s)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| ERR.to_string())?;
                members.push((m, score));
            }
            RdbObject::ZSet(members)
        }
        RDB_TYPE_ZSET_2 => {
            let (n, enc) = decode_len(&mut c)?;
            if enc {
                return Err(ERR.into());
            }
            let mut members = Vec::with_capacity(n as usize);
            for _ in 0..n {
                let m = decode_string(&mut c)?;
                let score = c.f64_le()?;
                members.push((m, score));
            }
            RdbObject::ZSet(members)
        }
        RDB_TYPE_HASH => {
            let (n, enc) = decode_len(&mut c)?;
            if enc {
                return Err(ERR.into());
            }
            let mut fields = Vec::with_capacity(n as usize);
            for _ in 0..n {
                let k = decode_string(&mut c)?;
                let v = decode_string(&mut c)?;
                fields.push((k, v));
            }
            RdbObject::Hash(fields)
        }
        RDB_TYPE_HASH_LISTPACK | RDB_TYPE_HASH_ZIPLIST => {
            let blob = decode_string(&mut c)?;
            let entries = if ty == RDB_TYPE_HASH_LISTPACK {
                decode_listpack(&blob)?
            } else {
                decode_ziplist(&blob)?
            };
            if entries.len() % 2 != 0 {
                return Err(ERR.into());
            }
            let mut fields = Vec::with_capacity(entries.len() / 2);
            let mut it = entries.into_iter();
            while let Some(k) = it.next() {
                let v = it.next().ok_or_else(|| ERR.to_string())?;
                fields.push((k, v));
            }
            RdbObject::Hash(fields)
        }
        RDB_TYPE_ZSET_LISTPACK | RDB_TYPE_ZSET_ZIPLIST => {
            let blob = decode_string(&mut c)?;
            let entries = if ty == RDB_TYPE_ZSET_LISTPACK {
                decode_listpack(&blob)?
            } else {
                decode_ziplist(&blob)?
            };
            if entries.len() % 2 != 0 {
                return Err(ERR.into());
            }
            let mut members = Vec::with_capacity(entries.len() / 2);
            let mut it = entries.into_iter();
            while let Some(m) = it.next() {
                let score_s = it.next().ok_or_else(|| ERR.to_string())?;
                let score: f64 = std::str::from_utf8(&score_s)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| ERR.to_string())?;
                members.push((m, score));
            }
            RdbObject::ZSet(members)
        }
        RDB_TYPE_SET_LISTPACK => {
            let blob = decode_string(&mut c)?;
            let members = decode_listpack(&blob)?;
            RdbObject::Set(members)
        }
        RDB_TYPE_LIST_ZIPLIST => {
            let blob = decode_string(&mut c)?;
            RdbObject::List(decode_ziplist(&blob)?)
        }
        RDB_TYPE_LIST_QUICKLIST | RDB_TYPE_LIST_QUICKLIST_2 => {
            let (n, enc) = decode_len(&mut c)?;
            if enc {
                return Err(ERR.into());
            }
            let mut elements = Vec::new();
            for _ in 0..n {
                if ty == RDB_TYPE_LIST_QUICKLIST_2 {
                    let (container, enc) = decode_len(&mut c)?;
                    if enc {
                        return Err(ERR.into());
                    }
                    let blob = decode_string(&mut c)?;
                    match container {
                        QUICKLIST_NODE_CONTAINER_PLAIN => {
                            elements.push(blob);
                        }
                        QUICKLIST_NODE_CONTAINER_PACKED => {
                            // listpack (or ziplist in older nodes)
                            if let Ok(lp) = decode_listpack(&blob) {
                                elements.extend(lp);
                            } else {
                                elements.extend(decode_ziplist(&blob)?);
                            }
                        }
                        _ => return Err(ERR.into()),
                    }
                } else {
                    // quicklist v1: each node is ziplist/listpack string
                    let blob = decode_string(&mut c)?;
                    if let Ok(lp) = decode_listpack(&blob) {
                        elements.extend(lp);
                    } else {
                        elements.extend(decode_ziplist(&blob)?);
                    }
                }
            }
            RdbObject::List(elements)
        }
        RDB_TYPE_SET_INTSET => {
            // intset: encoding u32 LE, length u32 LE, then integers
            let blob = decode_string(&mut c)?;
            RdbObject::Set(decode_intset(&blob)?)
        }
        // zipmap / stream RDB object: residual (use KDF1 for streams)
        RDB_TYPE_HASH_ZIPMAP | RDB_TYPE_STREAM_LISTPACKS => return Err(ERR.into()),
        _ => return Err(ERR.into()),
    };
    if c.pos != c.data.len() {
        return Err(ERR.into());
    }
    Ok(obj)
}

fn decode_intset(data: &[u8]) -> Result<Vec<Bytes>, String> {
    if data.len() < 8 {
        return Err(ERR.into());
    }
    let encoding = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let length = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let mut out = Vec::with_capacity(length);
    let mut pos = 8usize;
    for _ in 0..length {
        match encoding {
            2 => {
                if pos + 2 > data.len() {
                    return Err(ERR.into());
                }
                let v = i16::from_le_bytes([data[pos], data[pos + 1]]) as i64;
                out.push(Bytes::from(v.to_string()));
                pos += 2;
            }
            4 => {
                if pos + 4 > data.len() {
                    return Err(ERR.into());
                }
                let v = i32::from_le_bytes([
                    data[pos],
                    data[pos + 1],
                    data[pos + 2],
                    data[pos + 3],
                ]) as i64;
                out.push(Bytes::from(v.to_string()));
                pos += 4;
            }
            8 => {
                if pos + 8 > data.len() {
                    return Err(ERR.into());
                }
                let v = i64::from_le_bytes([
                    data[pos],
                    data[pos + 1],
                    data[pos + 2],
                    data[pos + 3],
                    data[pos + 4],
                    data[pos + 5],
                    data[pos + 6],
                    data[pos + 7],
                ]);
                out.push(Bytes::from(v.to_string()));
                pos += 8;
            }
            _ => return Err(ERR.into()),
        }
    }
    Ok(out)
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc64_known_vector() {
        // Redis Jones CRC64("123456789") = 0xe9c6d914c4b8d9ca
        assert_eq!(redis_crc64(b"123456789"), 0xe9c6d914c4b8d9ca);
    }

    #[test]
    fn string_dump_matches_redis_hello_fixture() {
        // Real Valkey/Redis DUMP of SET s hello (RDB v80). We emit v9, so only
        // check that decode of the real fixture works.
        let fixture = hex_to_bytes("000568656c6c6f5000ac5816e7fb6647fe");
        let obj = decode_redis_dump(&fixture).expect("fixture");
        match obj {
            RdbObject::String(s) => assert_eq!(&s[..], b"hello"),
            _ => panic!("expected string"),
        }
    }

    #[test]
    fn string_roundtrip_and_empty() {
        let d = encode_string_dump(b"hello");
        match decode_redis_dump(&d).unwrap() {
            RdbObject::String(s) => assert_eq!(&s[..], b"hello"),
            _ => panic!(),
        }
        let d = encode_string_dump(b"");
        match decode_redis_dump(&d).unwrap() {
            RdbObject::String(s) => assert!(s.is_empty()),
            _ => panic!(),
        }
    }

    #[test]
    fn redis_int_encoded_string_fixture() {
        // DUMP of SET num 12345 (INT16 encoding)
        let fixture = hex_to_bytes("00c13930500052be23b60dae6f4d");
        match decode_redis_dump(&fixture).unwrap() {
            RdbObject::String(s) => assert_eq!(&s[..], b"12345"),
            _ => panic!(),
        }
    }

    #[test]
    fn list_set_hash_zset_roundtrip() {
        let list = encode_list_dump(&[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]);
        match decode_redis_dump(&list).unwrap() {
            RdbObject::List(e) => {
                assert_eq!(e.len(), 3);
                assert_eq!(&e[0][..], b"a");
                assert_eq!(&e[2][..], b"c");
            }
            _ => panic!(),
        }

        let set = encode_set_dump(&[Bytes::from("x"), Bytes::from("y")]);
        match decode_redis_dump(&set).unwrap() {
            RdbObject::Set(m) => {
                assert_eq!(m.len(), 2);
            }
            _ => panic!(),
        }

        let hash = encode_hash_dump(&[
            (Bytes::from("f"), Bytes::from("v")),
            (Bytes::from("g"), Bytes::from("w")),
        ]);
        match decode_redis_dump(&hash).unwrap() {
            RdbObject::Hash(f) => {
                assert_eq!(f.len(), 2);
                assert_eq!(&f[0].0[..], b"f");
                assert_eq!(&f[0].1[..], b"v");
            }
            _ => panic!(),
        }

        let zset = encode_zset_dump(&[(Bytes::from("m"), 1.5)]);
        match decode_redis_dump(&zset).unwrap() {
            RdbObject::ZSet(m) => {
                assert_eq!(m.len(), 1);
                assert_eq!(&m[0].0[..], b"m");
                assert!((m[0].1 - 1.5).abs() < 1e-9);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn real_redis_listpack_fixtures() {
        // LIST quicklist2
        let list = hex_to_bytes(
            "12010210100000000300816102816202816302ff50000732709d0b61356a",
        );
        match decode_redis_dump(&list).unwrap() {
            RdbObject::List(e) => {
                assert_eq!(e.iter().map(|b| b.as_ref()).collect::<Vec<_>>(), vec![
                    b"a".as_ref(),
                    b"b".as_ref(),
                    b"c".as_ref()
                ]);
            }
            _ => panic!("list"),
        }

        // SET listpack
        let set = hex_to_bytes("140d0d0000000200817802817902ff5000ba652f89b43519f7");
        match decode_redis_dump(&set).unwrap() {
            RdbObject::Set(m) => {
                let mut v: Vec<_> = m.iter().map(|b| b.to_vec()).collect();
                v.sort();
                assert_eq!(v, vec![b"x".to_vec(), b"y".to_vec()]);
            }
            _ => panic!("set"),
        }

        // HASH listpack
        let hash =
            hex_to_bytes("1013130000000400816602817602816702817702ff500016f40c58885dd5c1");
        match decode_redis_dump(&hash).unwrap() {
            RdbObject::Hash(f) => {
                let map: std::collections::HashMap<_, _> = f
                    .into_iter()
                    .map(|(k, v)| (k.to_vec(), v.to_vec()))
                    .collect();
                assert_eq!(map.get(b"f".as_ref()).map(|v| v.as_slice()), Some(b"v".as_ref()));
                assert_eq!(map.get(b"g".as_ref()).map(|v| v.as_slice()), Some(b"w".as_ref()));
            }
            _ => panic!("hash"),
        }

        // ZSET listpack
        let zset =
            hex_to_bytes("110f0f0000000200816d0283312e3504ff500086f34ef1e677297e");
        match decode_redis_dump(&zset).unwrap() {
            RdbObject::ZSet(m) => {
                assert_eq!(m.len(), 1);
                assert_eq!(&m[0].0[..], b"m");
                assert!((m[0].1 - 1.5).abs() < 1e-9);
            }
            _ => panic!("zset"),
        }
    }

    #[test]
    fn bad_crc_rejected() {
        let mut d = encode_string_dump(b"hello");
        let last = d.len() - 1;
        d[last] ^= 0xff;
        assert!(decode_redis_dump(&d).is_err());
    }

    fn hex_to_bytes(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }
}
