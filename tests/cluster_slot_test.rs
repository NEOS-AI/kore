//! Lane D: Redis Cluster CRC16 / hash-slot golden vectors.

use kore::{crc16, key_hash_slot, SLOT_COUNT};

#[test]
fn crc16_redis_documented_vector() {
    // Redis crc16.c / docs: CRC16 of "123456789" is 0x31C3
    assert_eq!(crc16(b"123456789"), 0x31C3);
}

#[test]
fn key_hash_slot_foo() {
    // Well-known Redis Cluster KEYSLOT
    assert_eq!(key_hash_slot(b"foo"), 12182);
}

#[test]
fn key_hash_slot_bar() {
    assert_eq!(key_hash_slot(b"bar"), 5061);
}

#[test]
fn hash_tags_share_slot() {
    // Content between first `{` and next `}` is hashed
    let a = key_hash_slot(b"{user1000}.following");
    let b = key_hash_slot(b"{user1000}.followers");
    assert_eq!(a, b);
    assert_eq!(a, key_hash_slot(b"user1000"));
    assert_eq!(a, 3443);
}

#[test]
fn hash_tag_only_inner_hashed() {
    // this{foo} and {foo}this both hash "foo"
    assert_eq!(key_hash_slot(b"this{foo}"), 12182);
    assert_eq!(key_hash_slot(b"{foo}this"), 12182);
    assert_eq!(key_hash_slot(b"foo"), 12182);
}

#[test]
fn empty_hash_tag_hashes_whole_key() {
    // `{}xxx` → empty tag → hash entire key (Redis behavior)
    let whole = key_hash_slot(b"{}bar");
    assert_eq!(whole, crc16(b"{}bar") & (SLOT_COUNT - 1));
    assert_ne!(whole, key_hash_slot(b"bar"));
}

#[test]
fn unmatched_braces_hash_whole_key() {
    assert_eq!(key_hash_slot(b"{bar"), crc16(b"{bar") & (SLOT_COUNT - 1));
    assert_eq!(key_hash_slot(b"bar}"), crc16(b"bar}") & (SLOT_COUNT - 1));
}

#[test]
fn slot_count_is_16384() {
    assert_eq!(SLOT_COUNT, 16384);
    // All slots are in range
    for key in [b"a".as_slice(), b"foo", b"xyzzy", b"{tag}key"] {
        let s = key_hash_slot(key);
        assert!(s < SLOT_COUNT, "slot {s} out of range for {key:?}");
    }
}
