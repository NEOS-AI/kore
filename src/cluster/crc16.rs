//! Redis Cluster CRC16 (XMODEM / CCITT-FALSE variant used by Redis).
//!
//! Polynomial 0x1021, initial value 0x0000, no final XOR.
//! See Redis `crc16.c` / `CLUSTER KEYSLOT`.

/// Number of hash slots in a Redis Cluster (2^14).
pub const SLOT_COUNT: u16 = 16384;

/// CRC16-XMODEM over `data` (Redis-compatible).
pub fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

/// Redis `keyHashSlot`: CRC16 of key (or hash-tag body) modulo 16384.
///
/// Hash tags: if the key contains `{...}` with non-empty content between braces,
/// only the bytes between the first `{` and the next `}` are hashed.
pub fn key_hash_slot(key: &[u8]) -> u16 {
    let hash_input = hash_tag_bytes(key);
    crc16(hash_input) & (SLOT_COUNT - 1)
}

/// Select the byte slice that participates in slot hashing (hash-tag aware).
fn hash_tag_bytes(key: &[u8]) -> &[u8] {
    // Find first '{'
    let Some(s) = key.iter().position(|&b| b == b'{') else {
        return key;
    };
    // Find first '}' after '{'
    let Some(rel_e) = key[s + 1..].iter().position(|&b| b == b'}') else {
        return key;
    };
    let e = s + 1 + rel_e;
    // Empty tag `{}` → hash whole key
    if e == s + 1 {
        return key;
    }
    &key[s + 1..e]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc16_redis_vector_123456789() {
        // Redis documents CRC16("123456789") == 0x31C3
        assert_eq!(crc16(b"123456789"), 0x31C3);
    }

    #[test]
    fn slot_foo() {
        assert_eq!(key_hash_slot(b"foo"), 12182);
    }
}
