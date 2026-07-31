//! Batch CF: multi-DB replace success/fail preserve; RDB flush=false FT name-clash merge.

use bytes::Bytes;
use kore::databases::Databases;
use kore::entry::LoadOptions;
use kore::persistence::rdb::{
    self, DbSnapshot, HashRecord, MultiDbSnapshot, StringRecord,
};
use kore::search_index::{FieldDefinition, FieldType, IndexDefinition};

fn make_databases() -> std::sync::Arc<Databases> {
    Databases::create(16, 8, 1024 * 1024 * 50, 500 * 1024 * 1024, false, 0.75)
}

fn empty_snap() -> DbSnapshot {
    DbSnapshot {
        strings: Vec::new(),
        zsets: Vec::new(),
        geos: Vec::new(),
        hashes: Vec::new(),
        lists: Vec::new(),
        sets: Vec::new(),
        streams: Vec::new(),
        typed_expires: Vec::new(),
        search_indices: Vec::new(),
        search_aliases: Vec::new(),
        hnsw_graphs: Vec::new(),
    }
}

fn string_snap(key: &str, value: &str) -> DbSnapshot {
    let mut s = empty_snap();
    s.strings.push(StringRecord {
        key: Bytes::from(key.to_string()),
        value: Bytes::from(value.to_string()),
        flags: 0,
        expire_unix_ms: -1,
    });
    s
}

fn load_opts() -> LoadOptions {
    LoadOptions {
        touch: false,
        with_cas: false,
    }
}

fn get_string(db: &Databases, db_idx: usize, key: &str) -> Option<Bytes> {
    db.get(db_idx)
        .unwrap()
        .load(&Bytes::from(key.to_string()), load_opts())
        .unwrap()
        .map(|e| e.value.clone())
}

/// Failed multi-DB RDB load (bad alias on DB0) must preserve keys on DB0 and DB1.
#[test]
fn multi_db_rdb_load_failure_preserves_both_dbs() {
    let loaded = make_databases();
    loaded
        .get(0)
        .unwrap()
        .store(
            Bytes::from("db0-pre"),
            Bytes::from("v0"),
            Default::default(),
        )
        .unwrap();
    loaded
        .get(1)
        .unwrap()
        .store(
            Bytes::from("db1-pre"),
            Bytes::from("v1"),
            Default::default(),
        )
        .unwrap();

    let def = IndexDefinition::new(
        "idx".to_string(),
        vec!["doc:".to_string()],
        vec![FieldDefinition {
            name: "title".to_string(),
            field_type: FieldType::Text {
                weight: 1.0,
                sortable: false,
            },
        }],
    );
    // DB0 has a bad alias; DB1 has a valid key. Mid-apply fails on DB0 aliases
    // after schema/keys — whole multi-DB load must not commit either DB.
    let mut db0 = empty_snap();
    db0.hashes.push(HashRecord {
        key: Bytes::from("doc:1"),
        fields: vec![(Bytes::from("title"), Bytes::from("hello"))],
    });
    db0.search_indices.push(def);
    db0.search_aliases
        .push(("blog".to_string(), "missing".to_string()));

    let snap = MultiDbSnapshot {
        databases: vec![(0, db0), (1, string_snap("from-rdb-db1", "x"))],
    };
    let bytes = Bytes::from(snap.encode().unwrap());

    rdb::load_databases_bytes(&loaded, &bytes, true)
        .expect_err("bad alias must fail multi-DB RDB load");

    assert_eq!(
        get_string(&loaded, 0, "db0-pre").as_deref(),
        Some(b"v0".as_ref()),
        "DB0 pre-existing must survive failed load"
    );
    assert_eq!(
        get_string(&loaded, 1, "db1-pre").as_deref(),
        Some(b"v1".as_ref()),
        "DB1 pre-existing must survive failed load"
    );
    assert!(
        loaded
            .get(0)
            .unwrap()
            .get_hash(&Bytes::from_static(b"doc:1"))
            .is_none(),
        "partial DB0 hash must not commit"
    );
    assert!(
        get_string(&loaded, 1, "from-rdb-db1").is_none(),
        "DB1 RDB key must not commit on multi-DB failure"
    );
    assert!(
        !loaded
            .get(0)
            .unwrap()
            .list_search_indices()
            .iter()
            .any(|n| n == "idx"),
        "partial FT index must not commit"
    );
}

/// Successful multi-DB RDB load with flush=true replaces keys on DB0 and DB1.
#[test]
fn multi_db_rdb_flush_success_updates_both_dbs() {
    let source = make_databases();
    source
        .get(0)
        .unwrap()
        .store(
            Bytes::from("db0-new"),
            Bytes::from("n0"),
            Default::default(),
        )
        .unwrap();
    source
        .get(1)
        .unwrap()
        .store(
            Bytes::from("db1-new"),
            Bytes::from("n1"),
            Default::default(),
        )
        .unwrap();
    let bytes = rdb::save_databases_to_bytes(&source).unwrap();

    let loaded = make_databases();
    loaded
        .get(0)
        .unwrap()
        .store(
            Bytes::from("db0-old"),
            Bytes::from("o0"),
            Default::default(),
        )
        .unwrap();
    loaded
        .get(1)
        .unwrap()
        .store(
            Bytes::from("db1-old"),
            Bytes::from("o1"),
            Default::default(),
        )
        .unwrap();

    let n = rdb::load_databases_bytes(&loaded, &bytes, true).expect("multi-DB flush load");
    assert!(n >= 2, "expected keys from both DBs, got {n}");

    assert_eq!(
        get_string(&loaded, 0, "db0-new").as_deref(),
        Some(b"n0".as_ref())
    );
    assert_eq!(
        get_string(&loaded, 1, "db1-new").as_deref(),
        Some(b"n1".as_ref())
    );
    assert!(
        get_string(&loaded, 0, "db0-old").is_none(),
        "flush=true must drop DB0 old key"
    );
    assert!(
        get_string(&loaded, 1, "db1-old").is_none(),
        "flush=true must drop DB1 old key"
    );
}

/// flush=false merge: same FT index name with **equal schema** skips create;
/// seed key kept, RDB keys added; existing definition retained. Same-target
/// alias is skipped; new names from RDB are added.
///
/// Divergent schema / alias retarget clash cases live in
/// `tests/cg_ft_merge_schema_test.rs` (Batch CG).
#[test]
fn rdb_flush_false_ft_name_clash_merges() {
    let loaded = make_databases();
    let cache = loaded.get(0).unwrap();
    cache
        .store(
            Bytes::from("seed-key"),
            Bytes::from("keep"),
            Default::default(),
        )
        .unwrap();
    let idx_fields = vec![FieldDefinition {
        name: "title".to_string(),
        field_type: FieldType::Text {
            weight: 1.0,
            sortable: false,
        },
    }];
    cache
        .create_search_index(IndexDefinition::new(
            "idx".to_string(),
            vec!["doc:".to_string()],
            idx_fields.clone(),
        ))
        .unwrap();
    cache.alias_add("blog", "idx").unwrap();

    // RDB carries same index name with **identical** schema + a new key + a
    // new index name. Equal schema → skip create (idempotent merge).
    let mut body = empty_snap();
    body.strings.push(StringRecord {
        key: Bytes::from("from-rdb"),
        value: Bytes::from("v"),
        flags: 0,
        expire_unix_ms: -1,
    });
    body.search_indices.push(IndexDefinition::new(
        "idx".to_string(),
        vec!["doc:".to_string()], // same prefix as seed — schema-equal skip
        idx_fields,
    ));
    body.search_indices.push(IndexDefinition::new(
        "idx2".to_string(),
        vec!["x:".to_string()],
        vec![FieldDefinition {
            name: "t".to_string(),
            field_type: FieldType::Text {
                weight: 1.0,
                sortable: false,
            },
        }],
    ));
    // Same alias → same target: skip; new alias should apply.
    body.search_aliases
        .push(("blog".to_string(), "idx".to_string()));
    body.search_aliases
        .push(("news".to_string(), "idx2".to_string()));

    let snap = MultiDbSnapshot {
        databases: vec![(0, body)],
    };
    let bytes = Bytes::from(snap.encode().unwrap());

    let n = rdb::load_databases_bytes(&loaded, &bytes, false)
        .expect("merge with schema-equal FT name clash must succeed");
    assert!(n >= 1);

    assert_eq!(
        get_string(&loaded, 0, "seed-key").as_deref(),
        Some(b"keep".as_ref()),
        "seed key must remain after merge"
    );
    assert_eq!(
        get_string(&loaded, 0, "from-rdb").as_deref(),
        Some(b"v".as_ref()),
        "RDB key must be merged in"
    );

    let indices = cache.list_search_indices();
    assert!(
        indices.iter().any(|n| n == "idx"),
        "seed index retained; got {indices:?}"
    );
    assert!(
        indices.iter().any(|n| n == "idx2"),
        "new RDB index added; got {indices:?}"
    );
    // Seed definition kept (prefix doc:).
    let defs = cache.list_search_index_definitions();
    let idx_def = defs.iter().find(|d| d.name == "idx").expect("idx def");
    assert_eq!(
        idx_def.prefix,
        vec!["doc:".to_string()],
        "seed index definition kept on schema-equal name clash"
    );

    let aliases = cache.list_search_aliases();
    assert!(
        aliases.iter().any(|(a, t)| a == "blog" && t == "idx"),
        "seed alias retained; got {aliases:?}"
    );
    assert!(
        aliases.iter().any(|(a, t)| a == "news" && t == "idx2"),
        "new RDB alias added; got {aliases:?}"
    );
}

/// Failed multi-DB AOF load must preserve pre-existing keys on DB0 and DB1.
#[test]
fn multi_db_aof_load_failure_preserves_both_dbs() {
    use kore::persistence::aof;
    use std::time::{SystemTime, UNIX_EPOCH};

    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "kore-cf-aof-fail-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("appendonly.aof");

    let mut writer = aof::AofWriter::open(&path).unwrap();
    let argv = |parts: &[&str]| -> Vec<Bytes> {
        parts.iter().map(|p| Bytes::from(p.to_string())).collect()
    };
    writer
        .append_command(&argv(&[
            "FT.CREATE",
            "idx",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
        ]))
        .unwrap();
    // Duplicate CREATE → load fails after partial apply on scratch.
    writer
        .append_command(&argv(&[
            "FT.CREATE",
            "idx",
            "SCHEMA",
            "title",
            "TEXT",
        ]))
        .unwrap();
    drop(writer);

    let loaded = make_databases();
    loaded
        .get(0)
        .unwrap()
        .store(
            Bytes::from("db0-pre"),
            Bytes::from("v0"),
            Default::default(),
        )
        .unwrap();
    loaded
        .get(1)
        .unwrap()
        .store(
            Bytes::from("db1-pre"),
            Bytes::from("v1"),
            Default::default(),
        )
        .unwrap();

    aof::load_into_databases(&loaded, &path).expect_err("duplicate CREATE must fail");

    assert_eq!(
        get_string(&loaded, 0, "db0-pre").as_deref(),
        Some(b"v0".as_ref())
    );
    assert_eq!(
        get_string(&loaded, 1, "db1-pre").as_deref(),
        Some(b"v1".as_ref())
    );
    assert!(
        !loaded
            .get(0)
            .unwrap()
            .list_search_indices()
            .iter()
            .any(|n| n == "idx")
    );

    let _ = std::fs::remove_dir_all(&dir);
}
