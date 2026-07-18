//! Batch CJ: multi-DB load generation + post-swap memory accounting.

use bytes::Bytes;
use kore::databases::Databases;
use kore::entry::StoreOptions;
use kore::persistence::rdb::{self, DbSnapshot, MultiDbSnapshot, StringRecord};
use kore::Cache;
use std::sync::Arc;

fn make_databases() -> Arc<Databases> {
    Databases::create(4, 8, 1024 * 1024 * 50, 500 * 1024 * 1024, false, 0.75)
}

#[test]
fn replace_keyspaces_bumps_load_generation() {
    let dbs = make_databases();
    let g0 = dbs.load_generation();
    assert!(!dbs.load_in_progress());

    let scratch = dbs.empty_like();
    scratch
        .get(0)
        .unwrap()
        .store(
            Bytes::from_static(b"k"),
            Bytes::from_static(b"v"),
            StoreOptions::default(),
        )
        .unwrap();

    assert!(!dbs.load_in_progress());
    dbs.replace_keyspaces_from(&scratch);
    assert!(!dbs.load_in_progress());
    let g1 = dbs.load_generation();
    // start + end each bump once
    assert_eq!(g1, g0 + 2, "expected start+end bumps, g0={g0} g1={g1}");
    assert_eq!(
        dbs.get(0)
            .unwrap()
            .load(&Bytes::from_static(b"k"), Default::default())
            .unwrap()
            .map(|e| e.value.to_vec()),
        Some(b"v".to_vec())
    );
}

#[test]
fn post_swap_memory_matches_string_payload() {
    let cache = Cache::new_with_sweep(4, 1024 * 1024, 1024 * 1024, false);
    cache
        .store(
            Bytes::from_static(b"a"),
            Bytes::from_static(b"hello"),
            StoreOptions::default(),
        )
        .unwrap();
    let before = cache.string_memory_usage();
    assert!(before > 0);

    let scratch = cache.empty_keyspace_like();
    scratch
        .store(
            Bytes::from_static(b"b"),
            Bytes::from_static(b"world!!"),
            StoreOptions::default(),
        )
        .unwrap();
    let scratch_mem = scratch.string_memory_usage();
    assert!(scratch_mem > 0);

    cache.replace_keyspace_from(&scratch);
    assert_eq!(
        cache.string_memory_usage(),
        scratch_mem,
        "after swap, target string memory must match staged scratch total"
    );
    // Old key gone, new key present
    assert!(cache
        .load(&Bytes::from_static(b"a"), Default::default())
        .unwrap()
        .is_none());
    assert!(cache
        .load(&Bytes::from_static(b"b"), Default::default())
        .unwrap()
        .is_some());
}

#[test]
fn multi_db_flush_false_merge_preserves_other_db() {
    let dbs = make_databases();
    dbs.get(0)
        .unwrap()
        .store(
            Bytes::from_static(b"db0"),
            Bytes::from_static(b"0"),
            StoreOptions::default(),
        )
        .unwrap();
    dbs.get(1)
        .unwrap()
        .store(
            Bytes::from_static(b"db1"),
            Bytes::from_static(b"1"),
            StoreOptions::default(),
        )
        .unwrap();

    // RDB only has DB0 key "new"
    let snap = MultiDbSnapshot {
        databases: vec![(
            0,
            DbSnapshot {
                strings: vec![StringRecord {
                    key: Bytes::from_static(b"new"),
                    value: Bytes::from_static(b"x"),
                    flags: 0,
                    expire_unix_ms: -1,
                }],
                zsets: Vec::new(),
                geos: Vec::new(),
                hashes: Vec::new(),
                lists: Vec::new(),
                sets: Vec::new(),
                streams: Vec::new(),
                typed_expires: Vec::new(),
                search_indices: Vec::new(),
                search_aliases: Vec::new(),
            },
        )],
    };
    let bytes = snap.encode().unwrap();
    rdb::load_databases_bytes(&dbs, &bytes, false).unwrap();

    // Merge keeps pre-existing db0 key and adds new; db1 untouched
    let c0 = dbs.get(0).unwrap();
    assert!(c0
        .load(&Bytes::from_static(b"db0"), Default::default())
        .unwrap()
        .is_some());
    assert!(c0
        .load(&Bytes::from_static(b"new"), Default::default())
        .unwrap()
        .is_some());
    assert!(dbs
        .get(1)
        .unwrap()
        .load(&Bytes::from_static(b"db1"), Default::default())
        .unwrap()
        .is_some());
}

#[test]
fn empty_aof_success_replaces_nonempty_target() {
    use kore::persistence::aof;
        use std::time::{SystemTime, UNIX_EPOCH};

    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "kore-cj-empty-aof-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("appendonly.aof");
    // Empty file is a valid empty AOF
    std::fs::File::create(&path).unwrap();

    let cache = Cache::new_with_sweep(4, 1024 * 1024, 1024 * 1024, false);
    cache
        .store(
            Bytes::from_static(b"old"),
            Bytes::from_static(b"v"),
            StoreOptions::default(),
        )
        .unwrap();
    let n = aof::load_into_cache(&cache, &path).unwrap();
    assert_eq!(n, 0);
    assert!(
        cache
            .load(&Bytes::from_static(b"old"), Default::default())
            .unwrap()
            .is_none(),
        "empty AOF success is full keyspace replace"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
