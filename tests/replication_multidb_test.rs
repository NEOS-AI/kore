//! Multi-DB parity for full SYNC/PSYNC RDB and replica command apply (SELECT / FLUSHALL).

use bytes::Bytes;
use kore::databases::Databases;
use kore::entry::{LoadOptions, StoreOptions};
use kore::persistence::rdb;
use kore::persistence::replication::{self, SyncStart};
use kore::persistence::{PersistenceConfig, PersistenceManager, SaveRule};
use kore::protocol::{RespParser, RespValue};
use kore::Cache;
use kore::Server;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout, Duration};

fn unique_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("kore-repl-mdb-{}-{}", label, nanos));
    let _ = std::fs::create_dir_all(&dir);
    dir
}

fn make_persistence(dir: &PathBuf) -> Arc<PersistenceManager> {
    let pconfig = PersistenceConfig {
        dir: dir.clone(),
        dbfilename: "dump.rdb".to_string(),
        appendonly: false,
        appendfilename: "appendonly.aof".to_string(),
        save_rules: vec![SaveRule::new(900, 1)],
    };
    PersistenceManager::new(pconfig).unwrap()
}

fn make_databases(n: usize) -> Arc<Databases> {
    Databases::create(n, 8, 1024 * 1024 * 20, 500 * 1024 * 1024, false, 0.75)
}

fn store_str(cache: &Cache, key: &str, val: &str) {
    let _ = cache.store(
        Bytes::from(key.to_string()),
        Bytes::from(val.to_string()),
        StoreOptions::default(),
    );
}

fn load_str(cache: &Cache, key: &str) -> Option<String> {
    cache
        .load(&Bytes::from(key.to_string()), LoadOptions::default())
        .ok()
        .flatten()
        .map(|e| String::from_utf8_lossy(&e.value).into_owned())
}

/// Skip `+FULLRESYNC ...\r\n` and parse the following bulk RDB.
fn extract_rdb_after_fullresync(raw: &[u8]) -> Bytes {
    let line_end = raw
        .windows(2)
        .position(|w| w == b"\r\n")
        .expect("FULLRESYNC line terminator")
        + 2;
    let mut parser = RespParser::new();
    parser.feed(&raw[line_end..]);
    match parser.parse().expect("parse").expect("complete bulk") {
        RespValue::BulkString(Some(data)) => data,
        other => panic!("expected bulk RDB after FULLRESYNC, got {:?}", other),
    }
}

/// Parse legacy SYNC bulk RDB response.
fn extract_rdb_from_sync(raw: &[u8]) -> Bytes {
    let mut parser = RespParser::new();
    parser.feed(raw);
    match parser.parse().expect("parse").expect("complete bulk") {
        RespValue::BulkString(Some(data)) => data,
        other => panic!("expected bulk RDB from SYNC, got {:?}", other),
    }
}

fn argv(parts: &[&str]) -> Vec<Bytes> {
    parts
        .iter()
        .map(|p| Bytes::from(p.to_string()))
        .collect()
}

#[test]
fn psync_full_resync_rdb_contains_all_databases() {
    let dir = unique_dir("psync-all-dbs");
    let mgr = make_persistence(&dir);
    let databases = make_databases(16);

    store_str(&databases.get(0).unwrap(), "db0key", "v0");
    store_str(&databases.get(1).unwrap(), "db1key", "v1");

    let start = mgr
        .replication
        .start_psync(&databases, "?", -1)
        .expect("psync");
    let raw = match start {
        SyncStart::Full { raw_response, feed: _ } => raw_response,
        SyncStart::Partial { .. } => panic!("expected full resync"),
    };
    let s = String::from_utf8_lossy(&raw);
    assert!(
        s.starts_with("+FULLRESYNC "),
        "expected FULLRESYNC, got {}",
        &s[..s.len().min(80)]
    );

    let rdb_bytes = extract_rdb_after_fullresync(&raw);
    let loaded = make_databases(16);
    rdb::load_databases_bytes(&loaded, &rdb_bytes, true).expect("load rdb");

    assert_eq!(load_str(&loaded.get(0).unwrap(), "db0key").as_deref(), Some("v0"));
    assert_eq!(load_str(&loaded.get(1).unwrap(), "db1key").as_deref(), Some("v1"));
    // Isolation: key from DB1 must not appear on DB0
    assert!(load_str(&loaded.get(0).unwrap(), "db1key").is_none());
    assert!(load_str(&loaded.get(1).unwrap(), "db0key").is_none());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn sync_legacy_rdb_contains_all_databases() {
    let dir = unique_dir("sync-all-dbs");
    let mgr = make_persistence(&dir);
    let databases = make_databases(16);

    store_str(&databases.get(0).unwrap(), "zero", "a");
    store_str(&databases.get(1).unwrap(), "one", "b");

    let (raw, _feed) = mgr
        .replication
        .start_full_sync(&databases)
        .expect("full sync");
    assert!(raw.starts_with(b"$"), "SYNC should be bulk RDB");

    let rdb_bytes = extract_rdb_from_sync(&raw);
    let loaded = make_databases(16);
    rdb::load_databases_bytes(&loaded, &rdb_bytes, true).expect("load rdb");

    assert_eq!(load_str(&loaded.get(0).unwrap(), "zero").as_deref(), Some("a"));
    assert_eq!(load_str(&loaded.get(1).unwrap(), "one").as_deref(), Some("b"));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn apply_replicated_select_then_set_targets_correct_db() {
    let databases = make_databases(16);
    let mut current_db = 0usize;

    replication::apply_argv(&databases, &mut current_db, argv(&["SELECT", "1"])).unwrap();
    assert_eq!(current_db, 1);
    replication::apply_argv(
        &databases,
        &mut current_db,
        argv(&["SET", "only_db1", "yes"]),
    )
    .unwrap();

    assert!(load_str(&databases.get(0).unwrap(), "only_db1").is_none());
    assert_eq!(
        load_str(&databases.get(1).unwrap(), "only_db1").as_deref(),
        Some("yes")
    );
}

#[test]
fn apply_replicated_select_switch_back_to_db0() {
    let databases = make_databases(16);
    let mut current_db = 0usize;

    replication::apply_argv(
        &databases,
        &mut current_db,
        argv(&["SET", "on0", "a"]),
    )
    .unwrap();
    replication::apply_argv(&databases, &mut current_db, argv(&["SELECT", "1"])).unwrap();
    replication::apply_argv(
        &databases,
        &mut current_db,
        argv(&["SET", "on1", "b"]),
    )
    .unwrap();
    replication::apply_argv(&databases, &mut current_db, argv(&["SELECT", "0"])).unwrap();
    assert_eq!(current_db, 0);
    replication::apply_argv(
        &databases,
        &mut current_db,
        argv(&["SET", "on0b", "c"]),
    )
    .unwrap();

    assert_eq!(load_str(&databases.get(0).unwrap(), "on0").as_deref(), Some("a"));
    assert_eq!(load_str(&databases.get(0).unwrap(), "on0b").as_deref(), Some("c"));
    assert!(load_str(&databases.get(0).unwrap(), "on1").is_none());
    assert_eq!(load_str(&databases.get(1).unwrap(), "on1").as_deref(), Some("b"));
    assert!(load_str(&databases.get(1).unwrap(), "on0").is_none());
}

#[test]
fn apply_replicated_flushall_clears_all_dbs() {
    let databases = make_databases(16);
    let mut current_db = 0usize;

    store_str(&databases.get(0).unwrap(), "k0", "v0");
    store_str(&databases.get(1).unwrap(), "k1", "v1");
    store_str(&databases.get(2).unwrap(), "k2", "v2");

    replication::apply_argv(&databases, &mut current_db, argv(&["FLUSHALL"])).unwrap();

    assert_eq!(databases.get(0).unwrap().dbsize(), 0);
    assert_eq!(databases.get(1).unwrap().dbsize(), 0);
    assert_eq!(databases.get(2).unwrap().dbsize(), 0);
}

/// TCP: PSYNC full resync RDB includes non-zero logical DBs written on the primary.
#[tokio::test(flavor = "multi_thread")]
async fn tcp_psync_full_resync_transfers_non_zero_dbs() {
    use kore::config::Config;

    let dir = unique_dir("tcp-mdb");
    // Distinct from network_integration (16490+) and replication_test (16500+).
    let port = 16510u16;
    let config = Arc::new(Config {
        host: "127.0.0.1".to_string(),
        port,
        threads: 1,
        shards: 8,
        maxmemory: 1024 * 1024 * 20,
        evict: true,
        autosweep: false,
        loadfactor: 0.75,
        maxconns: 50,
        auth: String::new(),
        maxentrysize: 500 * 1024 * 1024,
        verbosity: 0,
        enable_redlock: false,
        redlock_instances: String::new(),
        redlock_retry_count: 3,
        redlock_retry_delay_ms: 200,
        dir: dir.to_string_lossy().to_string(),
        dbfilename: "dump.rdb".to_string(),
        appendonly: false,
        appendfilename: "appendonly.aof".to_string(),
        replicaof: String::new(),
        save: "900,1".to_string(),
        maxmemory_policy: "allkeys-lru".to_string(),
        databases: 16,
        metrics_port: 0,
        tls: false,
        tls_cert: String::new(),
        tls_key: String::new(),
        cluster_enabled: false,
});
    let mgr = make_persistence(&dir);
    let databases = make_databases(16);

    // Seed both logical DBs before serving PSYNC (full resync snapshot source).
    store_str(&databases.get(0).unwrap(), "db0k", "from0");
    store_str(&databases.get(1).unwrap(), "db1k", "from1");

    let server = Server::with_databases_and_persistence(
        databases.clone(),
        Arc::clone(&config),
        Arc::clone(&mgr),
    );
    let handle = tokio::spawn(async move {
        let _ = server.run().await;
    });
    sleep(Duration::from_millis(250)).await;

    let mut stream = timeout(
        Duration::from_secs(2),
        TcpStream::connect(format!("127.0.0.1:{}", port)),
    )
    .await
    .expect("connect timeout")
    .expect("connect failed");

    let psync = b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    stream.write_all(psync).await.unwrap();

    // Accumulate until we have FULLRESYNC + complete bulk RDB
    let mut acc = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let rdb_bytes = loop {
        if tokio::time::Instant::now() > deadline {
            panic!(
                "timeout waiting for full RDB; got {} bytes: {}",
                acc.len(),
                String::from_utf8_lossy(&acc[..acc.len().min(120)])
            );
        }
        let mut buf = vec![0u8; 256 * 1024];
        let n = timeout(Duration::from_secs(2), stream.read(&mut buf))
            .await
            .expect("read timeout")
            .expect("read err");
        assert!(n > 0, "primary closed before RDB complete");
        acc.extend_from_slice(&buf[..n]);

        if !acc.starts_with(b"+FULLRESYNC ") {
            continue;
        }
        let Some(line_end) = acc.windows(2).position(|w| w == b"\r\n") else {
            continue;
        };
        let after = &acc[line_end + 2..];
        let mut parser = RespParser::new();
        parser.feed(after);
        if let Ok(Some(RespValue::BulkString(Some(data)))) = parser.parse() {
            break data;
        }
    };

    let loaded = make_databases(16);
    rdb::load_databases_bytes(&loaded, &rdb_bytes, true).expect("load transferred rdb");
    assert_eq!(
        load_str(&loaded.get(0).unwrap(), "db0k").as_deref(),
        Some("from0")
    );
    assert_eq!(
        load_str(&loaded.get(1).unwrap(), "db1k").as_deref(),
        Some("from1")
    );

    drop(stream);
    handle.abort();
    sleep(Duration::from_millis(50)).await;
    let _ = std::fs::remove_dir_all(&dir);
}
