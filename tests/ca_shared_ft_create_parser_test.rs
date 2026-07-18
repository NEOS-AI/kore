//! Batch CA: shared FT.CREATE parser used by command path and AOF load.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::databases::Databases;
use kore::persistence::aof;
use kore::protocol::RespValue;
use kore::{
    DistanceMetric, FieldType, IndexDefinition, VectorAlgorithm,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn tmp_dir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "kore-ca-{}-{}",
        name,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn make_config(dir: &PathBuf) -> Arc<Config> {
    Arc::new(Config {
        host: "127.0.0.1".to_string(),
        port: 6379,
        threads: 1,
        shards: 8,
        maxmemory: 1024 * 1024 * 50,
        evict: true,
        autosweep: false,
        loadfactor: 0.75,
        maxconns: 10,
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
        dir: dir.to_string_lossy().to_string(),
        dbfilename: "dump.rdb".to_string(),
        appendonly: false,
        appendfilename: "appendonly.aof".to_string(),
        replicaof: String::new(),
        save: "".to_string(),
        maxmemory_policy: "allkeys-lru".to_string(),
        databases: 16,
        metrics_port: 0,
        deadlock_ui_port: 0,
        tls: false,
        tls_cert: String::new(),
        tls_key: String::new(),
        aclfile: String::new(),
        cluster_enabled: false,
        unixsocket: String::new(),
            log_format: "text".to_string(),
    })
}

fn make_databases() -> Arc<Databases> {
    Databases::create(16, 8, 1024 * 1024 * 50, 500 * 1024 * 1024, false, 0.75)
}

fn make_handler(databases: Arc<Databases>, dir: &PathBuf) -> CommandHandler {
    CommandHandler::with_databases(databases, make_config(dir), None)
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

fn b(s: &str) -> Bytes {
    Bytes::from(s.to_string())
}

/// HNSW VECTOR schema accepted identically by shared parser (AOF argv shape)
/// and by live FT.CREATE command path.
#[test]
fn shared_parser_hnsw_matches_command_and_aof_load() {
    let hnsw_parts = [
        "FT.CREATE",
        "vec_idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "v:",
        "SCHEMA",
        "emb",
        "VECTOR",
        "HNSW",
        "M",
        "24",
        "TYPE",
        "FLOAT32",
        "DIM",
        "8",
        "DISTANCE_METRIC",
        "COSINE",
        "meta",
        "TEXT",
        "SORTABLE",
    ];

    // Shared parser (AOF-style full argv)
    let argv: Vec<Bytes> = hnsw_parts.iter().map(|p| b(p)).collect();
    let parsed = IndexDefinition::from_ft_create_argv(&argv).expect("shared parse");
    assert_eq!(parsed.name, "vec_idx");
    assert_eq!(parsed.prefix, vec!["v:".to_string()]);
    assert_eq!(parsed.fields.len(), 2);
    match &parsed.fields[0].field_type {
        FieldType::Vector {
            algorithm,
            dimensions,
            distance_metric,
        } => {
            assert_eq!(*dimensions, 8);
            assert!(matches!(distance_metric, DistanceMetric::Cosine));
            match algorithm {
                VectorAlgorithm::HNSW { m, ef_construction } => {
                    assert_eq!(*m, 24);
                    assert_eq!(*ef_construction, 200, "default ef_construction only in shared parser");
                }
                other => panic!("expected HNSW, got {:?}", other),
            }
        }
        other => panic!("expected Vector, got {:?}", other),
    }
    assert!(matches!(
        parsed.fields[1].field_type,
        FieldType::Text {
            weight: _,
            sortable: true
        }
    ));

    // Command path creates the same schema shape
    let dir = tmp_dir("hnsw-cmd");
    let databases = make_databases();
    let mut h = make_handler(databases.clone(), &dir);
    assert_eq!(handle(&mut h, cmd(&hnsw_parts)), RespValue::ok());

    let cmd_def = databases
        .get(0)
        .unwrap()
        .list_search_index_definitions()
        .into_iter()
        .find(|d| d.name == "vec_idx")
        .expect("index from command path");
    assert_eq!(cmd_def.fields, parsed.fields);
    assert_eq!(cmd_def.prefix, parsed.prefix);

    // AOF rewrite + load yields the same schema
    let path = dir.join("appendonly.aof");
    aof::rewrite_databases(&databases, &path).unwrap();
    let text = String::from_utf8_lossy(&std::fs::read(&path).unwrap()).into_owned();
    assert!(
        text.contains("HNSW")
            && text.contains("M")
            && text.contains("24")
            && text.contains("EF_CONSTRUCTION")
            && text.contains("200"),
        "rewrite must emit HNSW M + EF_CONSTRUCTION; got:\n{text}"
    );

    let loaded = make_databases();
    aof::load_into_databases(&loaded, &path).expect("AOF load HNSW schema");
    let aof_def = loaded
        .get(0)
        .unwrap()
        .list_search_index_definitions()
        .into_iter()
        .find(|d| d.name == "vec_idx")
        .expect("index from AOF load");
    assert_eq!(aof_def.fields, parsed.fields);
    assert_eq!(aof_def.prefix, parsed.prefix);

    let _ = std::fs::remove_dir_all(&dir);
}

/// FT.CREATE via command path still works end-to-end after parser extraction.
#[test]
fn command_ft_create_still_works() {
    let dir = tmp_dir("cmd-create");
    let databases = make_databases();
    let mut h = make_handler(databases.clone(), &dir);

    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "FT.CREATE",
                "articles",
                "PREFIX",
                "1",
                "doc:",
                "SCHEMA",
                "title",
                "TEXT",
                "WEIGHT",
                "1.5",
                "tags",
                "TAG",
                "SEPARATOR",
                ",",
            ]),
        ),
        RespValue::ok()
    );

    // Duplicate index name fails
    let dup = handle(
        &mut h,
        cmd(&[
            "FT.CREATE",
            "articles",
            "SCHEMA",
            "title",
            "TEXT",
        ]),
    );
    match dup {
        RespValue::Error(e) => {
            let msg = String::from_utf8_lossy(&e);
            assert!(!msg.is_empty(), "duplicate create should error");
        }
        other => panic!("expected error on duplicate, got {:?}", other),
    }

    assert_eq!(
        handle(
            &mut h,
            cmd(&["HSET", "doc:1", "title", "hello shared parser", "tags", "a,b"]),
        ),
        RespValue::Integer(2)
    );

    let search = handle(&mut h, cmd(&["FT.SEARCH", "articles", "hello"]));
    match search {
        RespValue::Array(arr) => {
            assert!(matches!(arr.first(), Some(RespValue::Integer(n)) if *n >= 1));
        }
        RespValue::Error(e) => panic!("search failed: {}", String::from_utf8_lossy(&e)),
        other => panic!("unexpected search reply: {:?}", other),
    }

    let _ = std::fs::remove_dir_all(&dir);
}
