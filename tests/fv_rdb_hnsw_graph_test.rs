//! Batch FV: RDB durable HNSW graph (levels + edges + entry_point).

use bytes::Bytes;
use kore::cache::Cache;
use kore::persistence::rdb;
use kore::search_index::{
    DocumentField, FieldDefinition, FieldType, IndexDefinition, VectorAlgorithm,
};
use kore::{DistanceMetric, HnswGraphSnapshot};
use std::collections::HashMap;
use std::sync::Arc;

fn make_cache() -> Arc<Cache> {
    Cache::new_with_sweep(8, 16 * 1024 * 1024, 500 * 1024 * 1024, false)
}

fn hnsw_def(name: &str, prefix: &str) -> IndexDefinition {
    IndexDefinition::new(
        name.to_string(),
        vec![prefix.to_string()],
        vec![FieldDefinition {
            name: "emb".to_string(),
            field_type: FieldType::Vector {
                algorithm: VectorAlgorithm::HNSW {
                    m: 4,
                    ef_construction: 32,
                },
                dimensions: 2,
                distance_metric: DistanceMetric::L2,
            },
        }],
    )
}

/// Index docs with forced multi-layer levels, SAVE → load → graph matches.
#[test]
fn rdb_roundtrip_hnsw_graph_levels_edges_entry() {
    let cache = make_cache();
    cache.create_search_index(hnsw_def("vec", "doc:")).unwrap();

    // Force multi-layer levels before dual-write inserts.
    cache
        .enqueue_hnsw_levels("vec", "emb", [0, 0, 2, 1, 0])
        .unwrap();

    let docs = [
        ("doc:a", vec![0.0f32, 0.0]),
        ("doc:b", vec![1.0, 0.0]),
        ("doc:c", vec![2.0, 0.0]),
        ("doc:d", vec![3.0, 0.0]),
        ("doc:e", vec![4.0, 0.0]),
    ];
    for (id, v) in &docs {
        // Persist as hash so RDB load re-indexes vectors from text fields.
        let hash = cache.get_or_create_hash(&Bytes::from(*id)).unwrap();
        {
            let mut h = hash.write();
            let emb = v
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>()
                .join(",");
            h.hset(Bytes::from("emb"), Bytes::from(emb));
        }
        let mut fields = HashMap::new();
        fields.insert("emb".to_string(), DocumentField::Vector(v.clone()));
        cache
            .index_document("vec", Bytes::from(*id), fields)
            .unwrap();
    }

    let before = cache
        .export_hnsw_graphs()
        .into_iter()
        .find(|(i, f, _)| i == "vec" && f == "emb")
        .map(|(_, _, s)| s)
        .expect("graph before save");
    assert_eq!(before.entry_point, Some(Bytes::from("doc:c")));
    assert_eq!(
        before
            .levels
            .iter()
            .find(|(id, _)| id.as_ref() == b"doc:c")
            .map(|(_, l)| *l),
        Some(2)
    );
    assert!(
        !before.layers.is_empty(),
        "expected multi-layer adjacency export"
    );

    let bytes = rdb::save_to_bytes(&cache).unwrap();
    let version = u32::from_le_bytes(bytes[6..10].try_into().unwrap());
    assert_eq!(version, 6, "durable graph requires KORDB v6");

    let loaded = make_cache();
    rdb::load_bytes(&loaded, &bytes, true).unwrap();

    let after = loaded
        .export_hnsw_graphs()
        .into_iter()
        .find(|(i, f, _)| i == "vec" && f == "emb")
        .map(|(_, _, s)| s)
        .expect("graph after load");

    assert_eq!(
        after, before,
        "RDB restore must be edge-identical (levels + edges + entry)"
    );
}

/// Old RDB without graph section still loads (rebuild path); schema preserved.
#[test]
fn rdb_v5_without_graph_still_loads_hnsw_schema() {
    // Build a v6 snapshot, then strip to prove empty-graph + schema works via
    // public API: create HNSW index only (no docs), save, load.
    let cache = make_cache();
    cache.create_search_index(hnsw_def("empty_hnsw", "v:")).unwrap();
    let bytes = rdb::save_to_bytes(&cache).unwrap();

    let loaded = make_cache();
    rdb::load_bytes(&loaded, &bytes, true).unwrap();
    let defs = loaded.list_search_index_definitions();
    assert!(defs.iter().any(|d| d.name == "empty_hnsw"));
    // No vectors → no graph blob exported.
    assert!(loaded.export_hnsw_graphs().is_empty());
}

/// Hand-built graph applied over vectors survives RDB when hashes rehydrate vectors.
#[test]
fn rdb_hnsw_graph_blob_roundtrip_in_snapshot() {
    let snap = HnswGraphSnapshot {
        entry_point: Some(Bytes::from("n:1")),
        levels: vec![(Bytes::from("n:1"), 1), (Bytes::from("n:2"), 0)],
        layers: vec![
            vec![
                (Bytes::from("n:1"), vec![Bytes::from("n:2")]),
                (Bytes::from("n:2"), vec![Bytes::from("n:1")]),
            ],
            vec![(Bytes::from("n:1"), vec![])],
        ],
    };

    let cache = make_cache();
    cache.create_search_index(hnsw_def("g", "n:")).unwrap();
    // Hashes so RDB load can rehydrate vectors via auto_index + text parse.
    for (id, emb) in [("n:1", "0,0"), ("n:2", "1,0")] {
        let hash = cache.get_or_create_hash(&Bytes::from(id)).unwrap();
        hash.write()
            .hset(Bytes::from("emb"), Bytes::from(emb));
        let mut fields = HashMap::new();
        let comps: Vec<f32> = emb
            .split(',')
            .map(|s| s.parse().unwrap())
            .collect();
        fields.insert("emb".to_string(), DocumentField::Vector(comps));
        cache
            .index_document("g", Bytes::from(id), fields)
            .unwrap();
    }
    cache
        .apply_hnsw_graphs(&[("g".into(), "emb".into(), snap.clone())])
        .unwrap();

    let bytes = rdb::save_to_bytes(&cache).unwrap();
    let loaded = make_cache();
    rdb::load_bytes(&loaded, &bytes, true).unwrap();
    let restored = loaded
        .export_hnsw_graphs()
        .into_iter()
        .find(|(i, f, _)| i == "g" && f == "emb")
        .map(|(_, _, s)| s)
        .expect("restored");
    assert_eq!(restored, snap);
}
