//! Batch FX: AOF rewrite emits FT._LOADGRAPH; reload preserves HNSW levels/edges.

use bytes::Bytes;
use kore::cache::Cache;
use kore::persistence::aof;
use kore::search_index::{
    DocumentField, FieldDefinition, FieldType, IndexDefinition, VectorAlgorithm,
};
use kore::DistanceMetric;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn tmp_aof(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "kore-fx-{}-{}.aof",
        name,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    p
}

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

/// Rewrite AOF with forced multi-layer levels → load → graph edge-identical.
#[test]
fn aof_rewrite_load_preserves_hnsw_graph() {
    let cache = make_cache();
    cache.create_search_index(hnsw_def("vec", "doc:")).unwrap();

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
        .expect("graph before rewrite");
    assert_eq!(before.entry_point, Some(Bytes::from("doc:c")));
    assert_eq!(
        before
            .levels
            .iter()
            .find(|(id, _)| id.as_ref() == b"doc:c")
            .map(|(_, l)| *l),
        Some(2)
    );

    let path = tmp_aof("roundtrip");
    aof::rewrite(&cache, &path).unwrap();

    // Rewritten AOF must contain FT._LOADGRAPH.
    let raw = std::fs::read(&path).unwrap();
    let text = String::from_utf8_lossy(&raw);
    assert!(
        text.contains("FT._LOADGRAPH") || raw.windows(b"FT._LOADGRAPH".len()).any(|w| w == b"FT._LOADGRAPH"),
        "AOF rewrite must emit FT._LOADGRAPH"
    );

    let loaded = make_cache();
    aof::load_into_cache(&loaded, &path).unwrap();

    let after = loaded
        .export_hnsw_graphs()
        .into_iter()
        .find(|(i, f, _)| i == "vec" && f == "emb")
        .map(|(_, _, s)| s)
        .expect("graph after AOF load");

    assert_eq!(
        after, before,
        "AOF restore must be edge-identical (levels + edges + entry)"
    );

    let _ = std::fs::remove_file(&path);
}

/// Old AOF without FT._LOADGRAPH still loads (rebuild path).
#[test]
fn aof_without_loadgraph_still_rebuilds_hnsw() {
    // Manually craft AOF with only FT.CREATE + HSET (no FT._LOADGRAPH).
    let path = tmp_aof("legacy");
    {
        let mut buf = Vec::new();
        let create = vec![
            Bytes::from_static(b"FT.CREATE"),
            Bytes::from_static(b"v"),
            Bytes::from_static(b"PREFIX"),
            Bytes::from_static(b"1"),
            Bytes::from_static(b"k:"),
            Bytes::from_static(b"SCHEMA"),
            Bytes::from_static(b"emb"),
            Bytes::from_static(b"VECTOR"),
            Bytes::from_static(b"HNSW"),
            Bytes::from_static(b"M"),
            Bytes::from_static(b"4"),
            Bytes::from_static(b"EF_CONSTRUCTION"),
            Bytes::from_static(b"32"),
            Bytes::from_static(b"TYPE"),
            Bytes::from_static(b"FLOAT32"),
            Bytes::from_static(b"DIM"),
            Bytes::from_static(b"2"),
            Bytes::from_static(b"DISTANCE_METRIC"),
            Bytes::from_static(b"L2"),
        ];
        buf.extend_from_slice(&aof::encode_command(&create));
        let hset = vec![
            Bytes::from_static(b"HSET"),
            Bytes::from_static(b"k:1"),
            Bytes::from_static(b"emb"),
            Bytes::from_static(b"0,0"),
        ];
        buf.extend_from_slice(&aof::encode_command(&hset));
        std::fs::write(&path, &buf).unwrap();
    }

    let loaded = make_cache();
    aof::load_into_cache(&loaded, &path).unwrap();
    let graphs = loaded.export_hnsw_graphs();
    // Rebuild-by-readd still produces a non-empty graph for the one vector.
    assert!(
        graphs
            .iter()
            .any(|(i, f, s)| i == "v" && f == "emb" && !s.is_empty()),
        "legacy AOF without LOADGRAPH should rebuild HNSW via re-add"
    );

    let _ = std::fs::remove_file(&path);
}
