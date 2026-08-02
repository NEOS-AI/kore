//! Batch GY: Redis Functions durable in RDB (KORDB v7) and AOF rewrite/load.

use bytes::Bytes;
use kore::persistence::aof;
use kore::persistence::rdb::{self, MultiDbSnapshot};
use kore::scripting::FunctionLibraryStore;
use kore::{Cache, Databases};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const LIB_CODE: &str = r#"#!lua name=mylib
redis.register_function('echo_fn', function(keys, args)
  return args[1]
end)
"#;

fn tmp_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("kore-gy-{}-{}", name, nanos));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn load_sample(store: &FunctionLibraryStore) {
    store
        .load_from_source(LIB_CODE, false)
        .expect("load sample library");
    assert_eq!(store.library_count(), 1);
    assert!(store.find_function("echo_fn").is_some());
}

#[test]
fn rdb_v7_roundtrip_functions() {
    let dir = tmp_path("rdb");
    let path = dir.join("dump.rdb");

    let dbs = Databases::create(2, 4, 1024 * 1024, 1024 * 1024, false, 0.75);
    dbs.db0()
        .store(
            Bytes::from_static(b"k"),
            Bytes::from_static(b"v"),
            Default::default(),
        )
        .unwrap();

    let libs = FunctionLibraryStore::shared();
    load_sample(&libs);

    rdb::save_databases_with_functions(&dbs, &path, Some(&libs)).unwrap();

    let libs2 = FunctionLibraryStore::shared();
    assert!(libs2.is_empty());
    let dbs2 = Databases::create(2, 4, 1024 * 1024, 1024 * 1024, false, 0.75);
    let n = rdb::load_databases_with_functions(&dbs2, &path, true, Some(&libs2)).unwrap();
    assert!(n >= 1);
    assert_eq!(libs2.library_count(), 1);
    let lib = libs2.find_function("echo_fn").expect("echo_fn restored");
    assert_eq!(lib.name, "mylib");
    assert!(lib.code.contains("register_function"));
}

#[test]
fn rdb_v7_empty_functions_section_decodes() {
    let cache = Cache::new_with_sweep(4, 1024 * 1024, 1024 * 1024, false);
    cache
        .store(
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
            Default::default(),
        )
        .unwrap();
    let snap = MultiDbSnapshot::from_cache(&cache).unwrap();
    assert!(snap.functions_dump.is_empty());
    let bytes = snap.encode().unwrap();
    let version = u32::from_le_bytes(bytes[6..10].try_into().unwrap());
    assert_eq!(version, 7);
    let decoded = MultiDbSnapshot::decode(&bytes).unwrap();
    assert!(decoded.functions_dump.is_empty());
    assert_eq!(decoded.databases.len(), 1);
}

#[test]
fn aof_rewrite_load_functions() {
    let dir = tmp_path("aof");
    let path = dir.join("appendonly.aof");

    let dbs = Databases::create(2, 4, 1024 * 1024, 1024 * 1024, false, 0.75);
    dbs.db0()
        .store(
            Bytes::from_static(b"x"),
            Bytes::from_static(b"1"),
            Default::default(),
        )
        .unwrap();
    let libs = FunctionLibraryStore::shared();
    load_sample(&libs);

    aof::rewrite_databases_with_functions(&dbs, &path, Some(&libs)).unwrap();
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(
        text.contains("FUNCTION") && text.contains("FLUSH"),
        "rewrite should emit FUNCTION FLUSH: {}",
        text
    );
    assert!(text.contains("mylib") || text.contains("register_function"));

    let libs2 = FunctionLibraryStore::shared();
    let dbs2 = Databases::create(2, 4, 1024 * 1024, 1024 * 1024, false, 0.75);
    let n = aof::load_into_databases_with_functions(&dbs2, &path, Some(&libs2)).unwrap();
    assert!(n >= 1);
    assert_eq!(libs2.library_count(), 1);
    assert!(libs2.find_function("echo_fn").is_some());
    // Keyspace also restored
    let v = dbs2
        .db0()
        .load(&Bytes::from_static(b"x"), Default::default())
        .unwrap()
        .expect("key x");
    assert_eq!(v.value.as_ref(), b"1");
}

#[test]
fn aof_functions_only_rewrite() {
    // Libraries alone should still produce a non-empty rewrite.
    let dir = tmp_path("aof-only");
    let path = dir.join("appendonly.aof");
    let dbs = Databases::create(1, 4, 1024 * 1024, 1024 * 1024, false, 0.75);
    let libs = FunctionLibraryStore::shared();
    load_sample(&libs);
    aof::rewrite_databases_with_functions(&dbs, &path, Some(&libs)).unwrap();
    assert!(path.metadata().unwrap().len() > 0);

    let libs2 = FunctionLibraryStore::shared();
    aof::load_into_databases_with_functions(&dbs, &path, Some(&libs2)).unwrap();
    assert_eq!(libs2.library_count(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}
