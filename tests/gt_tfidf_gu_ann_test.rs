//! Batch GT: TF-IDF field-weight scoring + FT.SEARCH WITHSCORES.
//! Batch GU: adaptive HNSW ef smoke via search ranking (unit gate lives in lib).

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::protocol::RespValue;
use kore::vector_search::HNSWIndex;
use kore::{
    Cache, DistanceMetric, DocumentField, FieldDefinition, FieldType, IndexDefinition,
    VectorAlgorithm,
};
use std::collections::HashMap;
use std::sync::Arc;

fn make_cache() -> Arc<Cache> {
    Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false)
}

fn make_handler(cache: Arc<Cache>) -> CommandHandler {
    let config = Config {
        host: "127.0.0.1".to_string(),
        port: 6379,
        threads: 1,
        shards: 16,
        maxmemory: 1024 * 1024 * 100,
        evict: true,
        autosweep: false,
        loadfactor: 0.75,
        maxconns: 100,
        auth: String::new(),
        maxentrysize: 500 * 1024 * 1024,
        verbosity: 0,
        enable_redlock: false,
        redlock_instances: String::new(),
        redlock_retry_count: 3,
        redlock_retry_delay_ms: 200,
        enable_fair_queue: false,
        fair_queue_max_size: 1024,
        fair_queue_cleanup_ms: 500,
        dir: "./data".to_string(),
        dbfilename: "dump.rdb".to_string(),
        appendonly: false,
        appendfilename: "appendonly.aof".to_string(),
        replicaof: String::new(),
        save: "900,1 300,10 60,10000".to_string(),
        maxmemory_policy: "allkeys-lru".to_string(),
        databases: 16,
        metrics_port: 0,
        deadlock_ui_port: 0,
        enable_deadlock_detection: false,
        deadlock_max_wait_ms: 30_000,
        deadlock_auto_resolve: false,
        deadlock_victim_strategy: "youngest".to_string(),
        tls: false,
        tls_cert: String::new(),
        tls_key: String::new(),
        tls_port: 0,
        tls_ca: String::new(),
        tls_auth_clients: false,
        tls_replication: false,
        aclfile: String::new(),
        cluster_enabled: false,
        cluster_replica_priority: 100,
        cluster_require_full_coverage: true,
        cluster_allow_reads_when_down: false,
        cluster_announce_ip: String::new(),
        cluster_announce_port: 0,
        unixsocket: String::new(),
        log_format: "text".to_string(),
    };
    CommandHandler::new(cache, Arc::new(config))
}

fn bulk(s: &str) -> RespValue {
    RespValue::BulkString(Some(Bytes::from(s.to_string())))
}

fn cmd(parts: &[&str]) -> RespValue {
    RespValue::Array(parts.iter().map(|p| bulk(p)).collect())
}

fn handle(handler: &mut CommandHandler, value: RespValue) -> RespValue {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async { handler.handle(value).await.unwrap() })
}

fn as_bulk_str(v: &RespValue) -> Option<String> {
    match v {
        RespValue::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    }
}

#[test]
fn gt_tf_idf_ranks_by_term_frequency_and_weight() {
    let cache = make_cache();
    let definition = IndexDefinition::new(
        "docs".to_string(),
        vec![],
        vec![FieldDefinition {
            name: "body".to_string(),
            field_type: FieldType::Text {
                weight: 1.0,
                sortable: false,
            },
        }],
    );
    cache.create_search_index(definition).unwrap();

    // doc_hi: "apple" thrice; doc_lo: once; fillers dilute IDF.
    let mut hi = HashMap::new();
    hi.insert(
        "body".to_string(),
        DocumentField::Text("apple apple apple banana".into()),
    );
    cache
        .index_document("docs", Bytes::from("doc_hi"), hi)
        .unwrap();

    let mut lo = HashMap::new();
    lo.insert(
        "body".to_string(),
        DocumentField::Text("apple orange".into()),
    );
    cache
        .index_document("docs", Bytes::from("doc_lo"), lo)
        .unwrap();

    for i in 0..8 {
        let mut f = HashMap::new();
        f.insert(
            "body".to_string(),
            DocumentField::Text("banana orange kiwi".into()),
        );
        cache
            .index_document("docs", Bytes::from(format!("fill{i}")), f)
            .unwrap();
    }

    let results = cache.search("docs", "apple", 10, 0).unwrap();
    assert!(results.total >= 2);
    // TF-IDF should rank doc_hi first.
    assert_eq!(
        results.documents[0].id,
        Bytes::from("doc_hi"),
        "expected higher TF first, got {:?}",
        results.documents.iter().map(|d| &d.id).collect::<Vec<_>>()
    );
    let s0 = results.documents[0].score.expect("score on hit");
    let s1 = results.documents[1].score.expect("score on hit");
    assert!(s0 > s1, "score hi {} should exceed lo {}", s0, s1);
}

#[test]
fn gt_field_weight_boosts_title_over_body() {
    let cache = make_cache();
    let definition = IndexDefinition::new(
        "articles".to_string(),
        vec![],
        vec![
            FieldDefinition {
                name: "title".to_string(),
                field_type: FieldType::Text {
                    weight: 5.0,
                    sortable: false,
                },
            },
            FieldDefinition {
                name: "body".to_string(),
                field_type: FieldType::Text {
                    weight: 1.0,
                    sortable: false,
                },
            },
        ],
    );
    cache.create_search_index(definition).unwrap();

    // Only title matches "quantum"
    let mut a = HashMap::new();
    a.insert(
        "title".to_string(),
        DocumentField::Text("quantum computing".into()),
    );
    a.insert(
        "body".to_string(),
        DocumentField::Text("general intro text".into()),
    );
    cache
        .index_document("articles", Bytes::from("title_hit"), a)
        .unwrap();

    // Only body matches "quantum" (same TF=1)
    let mut b = HashMap::new();
    b.insert(
        "title".to_string(),
        DocumentField::Text("other topic".into()),
    );
    b.insert(
        "body".to_string(),
        DocumentField::Text("quantum notes here".into()),
    );
    cache
        .index_document("articles", Bytes::from("body_hit"), b)
        .unwrap();

    let results = cache.search("articles", "quantum", 10, 0).unwrap();
    assert_eq!(results.total, 2);
    assert_eq!(results.documents[0].id, Bytes::from("title_hit"));
    let st = results.documents[0].score.unwrap();
    let sb = results.documents[1].score.unwrap();
    assert!(st > sb, "title weight 5 should beat body weight 1: {st} vs {sb}");
}

#[test]
fn gt_ft_search_withscores_wire() {
    let cache = make_cache();
    let mut h = make_handler(cache.clone());

    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "FT.CREATE",
                "idx",
                "SCHEMA",
                "t",
                "TEXT",
                "WEIGHT",
                "1.0",
            ])
        ),
        RespValue::ok()
    );

    // Index via HSET auto-index needs PREFIX — use direct cache index path then search via handler.
    let mut f1 = HashMap::new();
    f1.insert("t".to_string(), DocumentField::Text("alpha alpha beta".into()));
    cache
        .index_document("idx", Bytes::from("d1"), f1)
        .unwrap();
    let mut f2 = HashMap::new();
    f2.insert("t".to_string(), DocumentField::Text("alpha gamma".into()));
    cache
        .index_document("idx", Bytes::from("d2"), f2)
        .unwrap();

    match handle(
        &mut h,
        cmd(&["FT.SEARCH", "idx", "alpha", "WITHSCORES", "NOCONTENT", "LIMIT", "0", "10"]),
    ) {
        RespValue::Array(parts) => {
            // [total, id, score, id, score, ...]
            assert!(parts.len() >= 5, "got {:?}", parts);
            match &parts[0] {
                RespValue::Integer(n) => assert!(*n >= 2),
                other => panic!("{:?}", other),
            }
            let id0 = as_bulk_str(&parts[1]).unwrap();
            let sc0: f32 = as_bulk_str(&parts[2])
                .unwrap()
                .parse()
                .expect("score float");
            assert_eq!(id0, "d1");
            assert!(sc0 > 0.0, "score {}", sc0);
        }
        other => panic!("{:?}", other),
    }
}

#[test]
fn gu_effective_ef_scales_for_large_k() {
    let mut hnsw = HNSWIndex::new(8, 32, DistanceMetric::Cosine);
    for i in 0..200 {
        let mut v = vec![0.0f32; 8];
        v[i % 8] = 1.0;
        hnsw.add(Bytes::from(format!("n{i}")), v);
    }
    assert_eq!(hnsw.ef_search(), 32);
    // k=10 stays at base ef
    assert_eq!(hnsw.effective_ef_search(10), 32);
    // k=50 expands (≥ 2k or corpus)
    let ef50 = hnsw.effective_ef_search(50);
    assert!(ef50 >= 100, "expected scaled ef, got {ef50}");

    hnsw.set_ef_search(64);
    assert_eq!(hnsw.ef_search(), 64);
    assert!(hnsw.effective_ef_search(10) >= 64);
}

#[test]
fn gu_hnsw_index_definition_still_builds() {
    let cache = make_cache();
    let definition = IndexDefinition::new(
        "vecs".to_string(),
        vec![],
        vec![FieldDefinition {
            name: "emb".to_string(),
            field_type: FieldType::Vector {
                algorithm: VectorAlgorithm::HNSW {
                    m: 8,
                    ef_construction: 32,
                },
                dimensions: 4,
                distance_metric: DistanceMetric::L2,
            },
        }],
    );
    cache.create_search_index(definition).unwrap();
    for i in 0..20 {
        let mut f = HashMap::new();
        f.insert(
            "emb".to_string(),
            DocumentField::Vector(vec![i as f32, 0.0, 0.0, 0.0]),
        );
        cache
            .index_document("vecs", Bytes::from(format!("v{i}")), f)
            .unwrap();
    }
    let info = cache.get_search_index_info("vecs").unwrap();
    assert_eq!(info.num_docs, 20);
}
