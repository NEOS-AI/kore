//! Batch CG: RDB flush=false FT merge compares schema/alias targets (not name-only).

use bytes::Bytes;
use kore::databases::Databases;
use kore::error::Error;
use kore::persistence::rdb::{self, DbSnapshot, HashRecord, MultiDbSnapshot, StringRecord};
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
    }
}

fn text_field(name: &str) -> FieldDefinition {
    FieldDefinition {
        name: name.to_string(),
        field_type: FieldType::Text {
            weight: 1.0,
            sortable: false,
        },
    }
}

fn idx_def(name: &str, prefix: &str, field: &str) -> IndexDefinition {
    IndexDefinition::new(
        name.to_string(),
        vec![prefix.to_string()],
        vec![text_field(field)],
    )
}

/// Equal-schema name clash → load Ok; seed schema kept; hash under shared
/// prefix still auto-indexes into the seed index.
#[test]
fn equal_schema_name_clash_skips_create() {
    let loaded = make_databases();
    let cache = loaded.get(0).unwrap();
    cache
        .create_search_index(idx_def("idx", "doc:", "title"))
        .unwrap();

    let mut body = empty_snap();
    // Independently constructed definition with same logical schema.
    body.search_indices
        .push(idx_def("idx", "doc:", "title"));
    body.hashes.push(HashRecord {
        key: Bytes::from("doc:1"),
        fields: vec![(Bytes::from("title"), Bytes::from("hello"))],
    });

    let snap = MultiDbSnapshot {
        databases: vec![(0, body)],
    };
    let bytes = Bytes::from(snap.encode().unwrap());

    rdb::load_databases_bytes(&loaded, &bytes, false)
        .expect("schema-equal name clash must succeed");

    let defs = cache.list_search_index_definitions();
    let idx = defs.iter().find(|d| d.name == "idx").expect("idx");
    assert_eq!(idx.prefix, vec!["doc:".to_string()]);
    assert_eq!(idx.fields.len(), 1);
    assert_eq!(idx.fields[0].name, "title");

    // RDB hash under shared prefix should be searchable via seed index.
    let results = cache.search("idx", "hello", 10, 0).expect("search");
    assert_eq!(
        results.total, 1,
        "hash under shared prefix must auto-index into seed schema"
    );
    assert_eq!(results.documents[0].id.as_ref(), b"doc:1");
}

/// Divergent schema (different PREFIX) same name → load Err; seed preserved.
#[test]
fn divergent_schema_name_clash_fails_preserves_seed() {
    let loaded = make_databases();
    let cache = loaded.get(0).unwrap();
    cache
        .store(
            Bytes::from("seed-key"),
            Bytes::from("keep"),
            Default::default(),
        )
        .unwrap();
    cache
        .create_search_index(idx_def("idx", "doc:", "title"))
        .unwrap();

    let mut body = empty_snap();
    body.search_indices
        .push(idx_def("idx", "other:", "title")); // different PREFIX
    body.strings.push(StringRecord {
        key: Bytes::from("from-rdb"),
        value: Bytes::from("v"),
        flags: 0,
        expire_unix_ms: -1,
    });
    body.hashes.push(HashRecord {
        key: Bytes::from("other:1"),
        fields: vec![(Bytes::from("title"), Bytes::from("lost"))],
    });

    let snap = MultiDbSnapshot {
        databases: vec![(0, body)],
    };
    let bytes = Bytes::from(snap.encode().unwrap());

    let err = rdb::load_databases_bytes(&loaded, &bytes, false)
        .expect_err("divergent schema must fail merge");
    match err {
        Error::InvalidArgument(msg) => {
            assert!(
                msg.contains("idx") && (msg.contains("schema") || msg.contains("prefix")),
                "expected schema clash message, got: {msg}"
            );
        }
        other => panic!("expected InvalidArgument, got {other:?}"),
    }

    // Target/pre-existing preserved via public wrapper.
    assert_eq!(
        cache
            .load(
                &Bytes::from("seed-key"),
                kore::entry::LoadOptions {
                    touch: false,
                    with_cas: false,
                },
            )
            .unwrap()
            .map(|e| e.value.clone())
            .as_deref(),
        Some(b"keep".as_ref()),
        "seed key must survive failed merge"
    );
    assert!(
        cache
            .load(
                &Bytes::from("from-rdb"),
                kore::entry::LoadOptions {
                    touch: false,
                    with_cas: false,
                },
            )
            .unwrap()
            .is_none(),
        "RDB key must not commit on schema clash"
    );
    let defs = cache.list_search_index_definitions();
    let idx = defs.iter().find(|d| d.name == "idx").expect("idx");
    assert_eq!(
        idx.prefix,
        vec!["doc:".to_string()],
        "seed schema preserved after failed merge"
    );
    assert!(
        cache.get_hash(&Bytes::from_static(b"other:1")).is_none(),
        "RDB hash under divergent prefix must not commit"
    );
}

/// Equal alias target → Ok skip (idempotent).
#[test]
fn equal_alias_target_skips() {
    let loaded = make_databases();
    let cache = loaded.get(0).unwrap();
    cache
        .create_search_index(idx_def("idx", "doc:", "title"))
        .unwrap();
    cache.alias_add("blog", "idx").unwrap();

    let mut body = empty_snap();
    body.search_indices
        .push(idx_def("idx", "doc:", "title"));
    body.search_aliases
        .push(("blog".to_string(), "idx".to_string()));
    body.strings.push(StringRecord {
        key: Bytes::from("from-rdb"),
        value: Bytes::from("v"),
        flags: 0,
        expire_unix_ms: -1,
    });

    let snap = MultiDbSnapshot {
        databases: vec![(0, body)],
    };
    let bytes = Bytes::from(snap.encode().unwrap());

    rdb::load_databases_bytes(&loaded, &bytes, false)
        .expect("equal alias target must succeed");

    let aliases = cache.list_search_aliases();
    assert!(
        aliases.iter().any(|(a, t)| a == "blog" && t == "idx"),
        "seed alias retained; got {aliases:?}"
    );
    assert_eq!(
        cache
            .load(
                &Bytes::from("from-rdb"),
                kore::entry::LoadOptions {
                    touch: false,
                    with_cas: false,
                },
            )
            .unwrap()
            .map(|e| e.value.clone())
            .as_deref(),
        Some(b"v".as_ref())
    );
}

/// Alias retarget clash (seed blog→idx, RDB blog→other) → Err; seed unchanged.
#[test]
fn alias_retarget_clash_fails_preserves_seed() {
    let loaded = make_databases();
    let cache = loaded.get(0).unwrap();
    cache
        .create_search_index(idx_def("idx", "doc:", "title"))
        .unwrap();
    cache
        .create_search_index(idx_def("other", "x:", "t"))
        .unwrap();
    cache.alias_add("blog", "idx").unwrap();

    let mut body = empty_snap();
    // Both indices present so alias target is valid — clash is retarget only.
    body.search_indices
        .push(idx_def("idx", "doc:", "title"));
    body.search_indices
        .push(idx_def("other", "x:", "t"));
    body.search_aliases
        .push(("blog".to_string(), "other".to_string()));
    body.strings.push(StringRecord {
        key: Bytes::from("from-rdb"),
        value: Bytes::from("v"),
        flags: 0,
        expire_unix_ms: -1,
    });

    let snap = MultiDbSnapshot {
        databases: vec![(0, body)],
    };
    let bytes = Bytes::from(snap.encode().unwrap());

    let err = rdb::load_databases_bytes(&loaded, &bytes, false)
        .expect_err("alias retarget clash must fail");
    match err {
        Error::InvalidArgument(msg) => {
            assert!(
                msg.contains("blog")
                    && (msg.contains("ALIAS") || msg.contains("alias") || msg.contains("points")),
                "expected retarget clash message, got: {msg}"
            );
        }
        other => panic!("expected InvalidArgument, got {other:?}"),
    }

    let aliases = cache.list_search_aliases();
    assert_eq!(
        aliases,
        vec![("blog".to_string(), "idx".to_string())],
        "seed alias must be unchanged after failed wrapper load"
    );
    assert!(
        cache
            .load(
                &Bytes::from("from-rdb"),
                kore::entry::LoadOptions {
                    touch: false,
                    with_cas: false,
                },
            )
            .unwrap()
            .is_none(),
        "RDB key must not commit on alias retarget clash"
    );
}

/// Single-DB `load_bytes` also fails divergent schema and preserves seed.
#[test]
fn load_bytes_divergent_schema_preserves_seed() {
    let dbs = make_databases();
    let cache = dbs.get(0).unwrap();
    cache
        .create_search_index(idx_def("idx", "doc:", "title"))
        .unwrap();
    cache
        .store(
            Bytes::from("seed"),
            Bytes::from("yes"),
            Default::default(),
        )
        .unwrap();

    let mut body = empty_snap();
    body.search_indices
        .push(idx_def("idx", "other:", "title"));
    let snap = MultiDbSnapshot {
        databases: vec![(0, body)],
    };
    let bytes = Bytes::from(snap.encode().unwrap());

    rdb::load_bytes(&cache, &bytes, false).expect_err("divergent must fail");

    let defs = cache.list_search_index_definitions();
    assert_eq!(
        defs.iter().find(|d| d.name == "idx").unwrap().prefix,
        vec!["doc:".to_string()]
    );
}
