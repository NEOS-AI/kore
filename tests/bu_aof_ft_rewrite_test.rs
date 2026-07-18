//! Batch BU: AOF rewrite emits FT.CREATE + FT.ALIASADD; reload restores search.

use bytes::Bytes;
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::databases::Databases;
use kore::persistence::aof;
use kore::protocol::RespValue;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn tmp_dir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "kore-bu-{}-{}",
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
        enable_deadlock_detection: false,
        deadlock_max_wait_ms: 30_000,
        deadlock_auto_resolve: false,
        deadlock_victim_strategy: "youngest".to_string(),
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

fn search_total(resp: &RespValue) -> i64 {
    match resp {
        RespValue::Array(arr) => match arr.first() {
            Some(RespValue::Integer(n)) => *n,
            _ => panic!("FT.SEARCH reply missing total integer: {:?}", resp),
        },
        RespValue::Error(e) => panic!("FT.SEARCH error: {}", String::from_utf8_lossy(e)),
        other => panic!("expected FT.SEARCH array, got {:?}", other),
    }
}

fn search_ids(resp: &RespValue) -> Vec<String> {
    match resp {
        RespValue::Array(arr) => {
            let mut ids = Vec::new();
            let mut i = 1;
            while i < arr.len() {
                if let RespValue::BulkString(Some(id)) = &arr[i] {
                    ids.push(String::from_utf8_lossy(id).into_owned());
                }
                i += 2;
            }
            ids
        }
        other => panic!("expected FT.SEARCH array, got {:?}", other),
    }
}

/// Rewrite order: FT.CREATE → HSET dumps → FT.ALIASADD, then load restores search.
#[test]
fn aof_rewrite_emits_ft_create_and_aliases() {
    let dir = tmp_dir("ft-rewrite");
    let path = dir.join("appendonly.aof");
    let databases = make_databases();
    let mut h = make_handler(databases.clone(), &dir);

    // Index with PREFIX + SCHEMA (TEXT + TAG with custom SEPARATOR)
    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "FT.CREATE",
                "articles",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "doc:",
                "SCHEMA",
                "title",
                "TEXT",
                "tags",
                "TAG",
                "SEPARATOR",
                "|",
            ]),
        ),
        RespValue::ok()
    );

    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "HSET",
                "doc:1",
                "title",
                "hello world",
                "tags",
                "rust|redis",
            ]),
        ),
        RespValue::Integer(2)
    );
    assert_eq!(
        handle(
            &mut h,
            cmd(&["HSET", "doc:2", "title", "other note", "tags", "go"]),
        ),
        RespValue::Integer(2)
    );

    assert_eq!(
        handle(&mut h, cmd(&["FT.ALIASADD", "blog", "articles"])),
        RespValue::ok()
    );

    // Sanity: search works before rewrite
    let pre = handle(&mut h, cmd(&["FT.SEARCH", "blog", "hello"]));
    assert_eq!(search_total(&pre), 1);
    assert_eq!(search_ids(&pre), vec!["doc:1".to_string()]);

    aof::rewrite_databases(&databases, &path).unwrap();
    assert!(path.exists());

    let text = String::from_utf8_lossy(&std::fs::read(&path).unwrap()).into_owned();
    // Case-sensitive RESP bulk content uses uppercase command names from rewrite.
    assert!(
        text.contains("FT.CREATE"),
        "rewrite must emit FT.CREATE; got:\n{text}"
    );
    assert!(
        text.contains("articles"),
        "rewrite must include index name; got:\n{text}"
    );
    assert!(
        text.contains("PREFIX") || text.contains("doc:"),
        "rewrite must include PREFIX; got:\n{text}"
    );
    assert!(
        text.contains("SCHEMA") && text.contains("TEXT") && text.contains("TAG"),
        "rewrite must include SCHEMA field types; got:\n{text}"
    );
    assert!(
        text.contains("SEPARATOR"),
        "rewrite must preserve non-default TAG SEPARATOR; got:\n{text}"
    );
    assert!(
        text.contains("HSET") || text.contains("hset"),
        "rewrite must dump hash keys; got:\n{text}"
    );
    assert!(
        text.contains("FT.ALIASADD"),
        "rewrite must emit FT.ALIASADD; got:\n{text}"
    );
    assert!(
        text.contains("blog"),
        "rewrite must include alias name; got:\n{text}"
    );

    // CREATE must appear before HSET; ALIASADD after CREATE (prefer after HSET).
    let create_pos = text.find("FT.CREATE").expect("FT.CREATE position");
    let hset_pos = text.find("HSET").or_else(|| text.find("hset")).expect("HSET");
    let alias_pos = text.find("FT.ALIASADD").expect("FT.ALIASADD");
    assert!(
        create_pos < hset_pos,
        "FT.CREATE must precede HSET (create_pos={create_pos}, hset_pos={hset_pos})"
    );
    assert!(
        hset_pos < alias_pos,
        "FT.ALIASADD must follow HSET (hset_pos={hset_pos}, alias_pos={alias_pos})"
    );

    // Load into fresh databases
    let loaded = make_databases();
    let n = aof::load_into_databases(&loaded, &path).unwrap();
    assert!(n >= 3, "expected FT.CREATE + HSETs + ALIASADD, got {n} cmds");

    let mut h2 = make_handler(loaded, &dir);

    // Index name search
    let by_name = handle(&mut h2, cmd(&["FT.SEARCH", "articles", "hello"]));
    assert_eq!(search_total(&by_name), 1);
    assert_eq!(search_ids(&by_name), vec!["doc:1".to_string()]);

    // Alias resolution
    let by_alias = handle(&mut h2, cmd(&["FT.SEARCH", "blog", "hello"]));
    assert_eq!(search_total(&by_alias), 1);
    assert_eq!(search_ids(&by_alias), vec!["doc:1".to_string()]);

    // FT.INFO via alias
    let info = handle(&mut h2, cmd(&["FT.INFO", "blog"]));
    match info {
        RespValue::Array(parts) => assert!(!parts.is_empty(), "FT.INFO should return fields"),
        RespValue::Error(e) => panic!("FT.INFO via alias failed: {}", String::from_utf8_lossy(&e)),
        other => panic!("unexpected FT.INFO: {other:?}"),
    }

    // TAG SEPARATOR '|' should have split "rust|redis" into two tag values
    match handle(&mut h2, cmd(&["FT.TAGVALS", "articles", "tags"])) {
        RespValue::Array(a) => {
            let mut vals: Vec<String> = a
                .iter()
                .filter_map(|v| match v {
                    RespValue::BulkString(Some(b)) => {
                        Some(String::from_utf8_lossy(b).into_owned())
                    }
                    _ => None,
                })
                .collect();
            vals.sort();
            assert_eq!(
                vals,
                vec!["go".to_string(), "redis".to_string(), "rust".to_string()],
                "TAG SEPARATOR must survive rewrite so values split correctly"
            );
        }
        RespValue::Error(e) => {
            panic!("FT.TAGVALS failed: {}", String::from_utf8_lossy(&e))
        }
        other => panic!("expected TAGVALS array, got {:?}", other),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Index-only DB (schema without keys yet) still rewrites FT.CREATE.
#[test]
fn aof_rewrite_index_only_db() {
    let dir = tmp_dir("ft-only");
    let path = dir.join("appendonly.aof");
    let databases = make_databases();
    let mut h = make_handler(databases.clone(), &dir);

    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "FT.CREATE",
                "empty_idx",
                "PREFIX",
                "1",
                "x:",
                "SCHEMA",
                "body",
                "TEXT",
            ]),
        ),
        RespValue::ok()
    );

    aof::rewrite_databases(&databases, &path).unwrap();
    let text = String::from_utf8_lossy(&std::fs::read(&path).unwrap()).into_owned();
    assert!(
        text.contains("FT.CREATE") && text.contains("empty_idx"),
        "index-only DB must still rewrite schema; got:\n{text}"
    );

    let loaded = make_databases();
    aof::load_into_databases(&loaded, &path).unwrap();
    let mut h2 = make_handler(loaded, &dir);

    // Create a doc after load — auto-index against restored schema
    assert_eq!(
        handle(
            &mut h2,
            cmd(&["HSET", "x:1", "body", "post rewrite doc"]),
        ),
        RespValue::Integer(1)
    );
    let resp = handle(&mut h2, cmd(&["FT.SEARCH", "empty_idx", "rewrite"]));
    assert_eq!(search_total(&resp), 1);

    let _ = std::fs::remove_dir_all(&dir);
}

/// NUMERIC + VECTOR field types round-trip through AOF rewrite/load.
#[test]
fn aof_rewrite_numeric_and_vector_fields() {
    let dir = tmp_dir("ft-num-vec");
    let path = dir.join("appendonly.aof");
    let databases = make_databases();
    let mut h = make_handler(databases.clone(), &dir);

    assert_eq!(
        handle(
            &mut h,
            cmd(&[
                "FT.CREATE",
                "mixed",
                "PREFIX",
                "1",
                "item:",
                "SCHEMA",
                "price",
                "NUMERIC",
                "SORTABLE",
                "emb",
                "VECTOR",
                "FLAT",
                "TYPE",
                "FLOAT32",
                "DIM",
                "3",
                "DISTANCE_METRIC",
                "COSINE",
            ]),
        ),
        RespValue::ok()
    );

    aof::rewrite_databases(&databases, &path).unwrap();
    let text = String::from_utf8_lossy(&std::fs::read(&path).unwrap()).into_owned();
    assert!(
        text.contains("NUMERIC") && text.contains("VECTOR") && text.contains("FLAT"),
        "rewrite must emit NUMERIC + VECTOR; got:\n{text}"
    );
    assert!(
        text.contains("DIM") && text.contains("COSINE"),
        "rewrite must emit vector DIM + metric; got:\n{text}"
    );

    let loaded = make_databases();
    aof::load_into_databases(&loaded, &path).expect("load numeric/vector schema");
    let cache = loaded.get(0).unwrap();
    let def = cache
        .list_search_index_definitions()
        .into_iter()
        .find(|d| d.name == "mixed")
        .expect("mixed index restored");
    assert_eq!(def.fields.len(), 2);
    assert!(matches!(
        def.fields[0].field_type,
        kore::FieldType::Numeric { sortable: true }
    ));
    match &def.fields[1].field_type {
        kore::FieldType::Vector {
            algorithm,
            dimensions,
            distance_metric,
        } => {
            assert_eq!(*dimensions, 3);
            assert!(matches!(algorithm, kore::VectorAlgorithm::Flat));
            assert!(matches!(distance_metric, kore::DistanceMetric::Cosine));
        }
        other => panic!("expected Vector field, got {:?}", other),
    }

    let _ = std::fs::remove_dir_all(&dir);
}
