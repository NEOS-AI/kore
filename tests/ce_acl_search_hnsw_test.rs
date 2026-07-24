//! Batch CE: ACL @search category; HNSW EF_CONSTRUCTION AOF/RDB round-trip.

use bytes::Bytes;
use kore::acl::{category_commands, category_names};
use kore::commands::CommandHandler;
use kore::config::Config;
use kore::databases::Databases;
use kore::persistence::{aof, rdb};
use kore::protocol::RespValue;
use kore::{DistanceMetric, FieldType, VectorAlgorithm};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn tmp_dir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "kore-ce-{}-{}",
        name,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn make_config(dir: &PathBuf, auth: &str) -> Arc<Config> {
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
        auth: auth.to_string(),
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
            cluster_replica_priority: 100,
            cluster_require_full_coverage: true,
            cluster_allow_reads_when_down: false,
            cluster_announce_ip: String::new(),
            cluster_announce_port: 0,
        unixsocket: String::new(),
            log_format: "text".to_string(),
    })
}

fn make_databases() -> Arc<Databases> {
    Databases::create(16, 8, 1024 * 1024 * 50, 500 * 1024 * 1024, false, 0.75)
}

fn make_handler(databases: Arc<Databases>, dir: &PathBuf, auth: &str) -> CommandHandler {
    CommandHandler::with_databases(databases, make_config(dir, auth), None)
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

fn is_ok(resp: RespValue) -> bool {
    matches!(resp, RespValue::SimpleString(s) if s.as_ref() == b"OK")
}

fn is_noperm(resp: &RespValue) -> bool {
    match resp {
        RespValue::Error(e) => String::from_utf8_lossy(e).contains("NOPERM"),
        _ => false,
    }
}

#[test]
fn acl_cat_lists_search_and_commands() {
    assert!(
        category_names().iter().any(|c| *c == "search"),
        "ACL CAT must list search"
    );
    let cmds = category_commands("search").expect("search category");
    for need in [
        "ft.create",
        "ft.search",
        "ft.dropindex",
        "ft.aliasadd",
        "ft.info",
        "ft._list",
        "ft.tagvals",
    ] {
        assert!(
            cmds.iter().any(|c| c == need),
            "missing {need} in @search: {cmds:?}"
        );
    }
}

#[test]
fn acl_search_category_allows_ft_search_denies_without() {
    let dir = tmp_dir("acl-search");
    // nopass default with +@all so we can ACL SETUSER
    let databases = make_databases();
    let mut h = make_handler(databases.clone(), &dir, "");

    assert!(is_ok(handle(
        &mut h,
        cmd(&[
            "ACL",
            "SETUSER",
            "searcher",
            "on",
            ">spass",
            "+@search",
            "+@connection",
            "~*",
            "&*"
        ])
    )));
    // Restrict default so AUTH is meaningful: reset default to require auth with all
    assert!(is_ok(handle(
        &mut h,
        cmd(&["ACL", "SETUSER", "default", "on", ">admin", "+@all", "~*", "&*"])
    )));

    // New connection simulation: re-auth as searcher
    // CommandHandler keeps username — AUTH switches user
    assert!(is_ok(handle(
        &mut h,
        cmd(&["AUTH", "searcher", "spass"])
    )));

    // FT.CREATE allowed via @search
    assert!(
        is_ok(handle(
            &mut h,
            cmd(&[
                "FT.CREATE",
                "idx",
                "PREFIX",
                "1",
                "doc:",
                "SCHEMA",
                "t",
                "TEXT"
            ])
        )),
        "FT.CREATE should be allowed for +@search"
    );
    let search1 = handle(&mut h, cmd(&["FT.SEARCH", "idx", "*"]));
    assert!(
        !is_noperm(&search1),
        "FT.SEARCH should not be NOPERM for +@search, got {:?}",
        search1
    );

    // SET denied (not in @search / @connection)
    assert!(
        is_noperm(&handle(&mut h, cmd(&["SET", "k", "v"]))),
        "SET should be denied without @write"
    );

    // User with only @read cannot FT.CREATE
    assert!(is_ok(handle(
        &mut h,
        cmd(&["AUTH", "default", "admin"])
    )));
    assert!(is_ok(handle(
        &mut h,
        cmd(&[
            "ACL",
            "SETUSER",
            "reader",
            "on",
            ">rpass",
            "+@read",
            "+@connection",
            "~*",
            "&*"
        ])
    )));
    assert!(is_ok(handle(&mut h, cmd(&["AUTH", "reader", "rpass"]))));
    assert!(
        is_noperm(&handle(
            &mut h,
            cmd(&[
                "FT.CREATE",
                "idx2",
                "PREFIX",
                "1",
                "x:",
                "SCHEMA",
                "t",
                "TEXT"
            ])
        )),
        "FT.CREATE not in @read"
    );
    // FT.SEARCH is in @read
    let search = handle(&mut h, cmd(&["FT.SEARCH", "idx", "*"]));
    assert!(
        !is_noperm(&search),
        "FT.SEARCH should be allowed for +@read, got {:?}",
        search
    );

    let _ = std::fs::remove_dir_all(&dir);
}


#[test]
fn hnsw_ef_construction_aof_and_rdb_roundtrip() {
    let dir = tmp_dir("hnsw-ef");
    let databases = make_databases();
    let mut h = make_handler(databases.clone(), &dir, "");

    let create = [
        "FT.CREATE",
        "vec_hnsw",
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
        "32",
        "EF_CONSTRUCTION",
        "400",
        "TYPE",
        "FLOAT32",
        "DIM",
        "4",
        "DISTANCE_METRIC",
        "L2",
    ];
    assert_eq!(handle(&mut h, cmd(&create)), RespValue::ok());

    let def = databases
        .get(0)
        .unwrap()
        .list_search_index_definitions()
        .into_iter()
        .find(|d| d.name == "vec_hnsw")
        .expect("index");
    match &def.fields[0].field_type {
        FieldType::Vector {
            algorithm,
            dimensions,
            distance_metric,
        } => {
            assert_eq!(*dimensions, 4);
            assert!(matches!(distance_metric, DistanceMetric::L2));
            match algorithm {
                VectorAlgorithm::HNSW { m, ef_construction } => {
                    assert_eq!(*m, 32);
                    assert_eq!(*ef_construction, 400);
                }
                other => panic!("expected HNSW, got {:?}", other),
            }
        }
        other => panic!("expected Vector, got {:?}", other),
    }

    // AOF rewrite emits EF_CONSTRUCTION and reloads it.
    let aof_path = dir.join("appendonly.aof");
    aof::rewrite_databases(&databases, &aof_path).unwrap();
    let text = String::from_utf8_lossy(&std::fs::read(&aof_path).unwrap()).into_owned();
    assert!(
        text.contains("EF_CONSTRUCTION") && text.contains("400") && text.contains("32"),
        "AOF rewrite must emit HNSW M + EF_CONSTRUCTION; got:\n{text}"
    );
    let loaded_aof = make_databases();
    aof::load_into_databases(&loaded_aof, &aof_path).unwrap();
    let aof_def = loaded_aof
        .get(0)
        .unwrap()
        .list_search_index_definitions()
        .into_iter()
        .find(|d| d.name == "vec_hnsw")
        .expect("aof index");
    match &aof_def.fields[0].field_type {
        FieldType::Vector {
            algorithm: VectorAlgorithm::HNSW { m, ef_construction },
            ..
        } => {
            assert_eq!(*m, 32);
            assert_eq!(*ef_construction, 400);
        }
        other => panic!("AOF load expected HNSW, got {:?}", other),
    }

    // RDB round-trip preserves M + ef_construction.
    let rdb_path = dir.join("dump.rdb");
    rdb::save_databases(&databases, &rdb_path).unwrap();
    let loaded_rdb = make_databases();
    rdb::load_databases(&loaded_rdb, &rdb_path, true).unwrap();
    let rdb_def = loaded_rdb
        .get(0)
        .unwrap()
        .list_search_index_definitions()
        .into_iter()
        .find(|d| d.name == "vec_hnsw")
        .expect("rdb index");
    match &rdb_def.fields[0].field_type {
        FieldType::Vector {
            algorithm: VectorAlgorithm::HNSW { m, ef_construction },
            dimensions,
            distance_metric,
        } => {
            assert_eq!(*m, 32);
            assert_eq!(*ef_construction, 400);
            assert_eq!(*dimensions, 4);
            assert!(matches!(distance_metric, DistanceMetric::L2));
        }
        other => panic!("RDB load expected HNSW, got {:?}", other),
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hnsw_ef_construction_order_independent_parse() {
    use kore::search_index::IndexDefinition;
    // EF_CONSTRUCTION before M
    let argv: Vec<Bytes> = [
        "FT.CREATE",
        "idx",
        "SCHEMA",
        "emb",
        "VECTOR",
        "HNSW",
        "EF_CONSTRUCTION",
        "111",
        "M",
        "7",
        "TYPE",
        "FLOAT32",
        "DIM",
        "2",
        "DISTANCE_METRIC",
        "COSINE",
    ]
    .iter()
    .map(|s| Bytes::from(*s))
    .collect();
    let def = IndexDefinition::from_ft_create_argv(&argv).expect("parse");
    match &def.fields[0].field_type {
        FieldType::Vector {
            algorithm: VectorAlgorithm::HNSW { m, ef_construction },
            ..
        } => {
            assert_eq!(*m, 7);
            assert_eq!(*ef_construction, 111);
        }
        other => panic!("{:?}", other),
    }
}
