//! AOF SELECT concurrency: concurrent writers on different DBs must not
//! interleave SELECT and write commands incorrectly in the AOF stream.

use bytes::Bytes;
use kore::databases::Databases;
use kore::persistence::{aof, PersistenceConfig, PersistenceManager};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

fn tmp_dir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "kore-aof-select-{}-{}",
        name,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn make_pm(dir: &PathBuf) -> Arc<PersistenceManager> {
    let pconfig = PersistenceConfig {
        dir: dir.clone(),
        dbfilename: "dump.rdb".to_string(),
        appendonly: true,
        appendfilename: "appendonly.aof".to_string(),
        save_rules: vec![],
    };
    let mgr = PersistenceManager::new(pconfig).unwrap();
    mgr.ensure_dir().unwrap();
    mgr
}

fn make_databases() -> Arc<Databases> {
    Databases::create(16, 8, 1024 * 1024 * 50, 500 * 1024 * 1024, false, 0.75)
}

fn set_args(key: &str, val: &str) -> Vec<Bytes> {
    vec![
        Bytes::from_static(b"SET"),
        Bytes::from(key.to_string()),
        Bytes::from(val.to_string()),
    ]
}

fn cmd_name(argv: &[Bytes]) -> String {
    String::from_utf8_lossy(&argv[0]).to_uppercase()
}

/// Concurrent writers on different DBs must replay every key onto its intended DB.
#[test]
fn aof_select_concurrent_writers_different_dbs_replay_correctly() {
    let dir = tmp_dir("concurrent");
    let aof_path = dir.join("appendonly.aof");
    let mgr = make_pm(&dir);

    const N_THREADS: usize = 8;
    const M_WRITES: usize = 120;

    let mut handles = Vec::with_capacity(N_THREADS);
    for t in 0..N_THREADS {
        let mgr = Arc::clone(&mgr);
        handles.push(thread::spawn(move || {
            for i in 0..M_WRITES {
                let db = t % 3;
                let key = format!("t{t}:k{i}");
                let val = format!("v-db{db}-t{t}-{i}");
                mgr.on_write_command(db, &set_args(&key, &val));
            }
        }));
    }
    for h in handles {
        h.join().expect("writer thread panicked");
    }

    assert!(aof_path.exists(), "AOF file should exist after writes");

    let loaded = make_databases();
    let n = aof::load_into_databases(&loaded, &aof_path).unwrap();
    assert!(n >= N_THREADS * M_WRITES, "expected at least all SET commands, got {n}");

    for t in 0..N_THREADS {
        let db = t % 3;
        let cache = loaded.get(db).expect("db exists");
        for i in 0..M_WRITES {
            let key = Bytes::from(format!("t{t}:k{i}"));
            let val = format!("v-db{db}-t{t}-{i}");
            let entry = cache
                .load(&key, Default::default())
                .unwrap()
                .unwrap_or_else(|| panic!("missing key {key:?} on db {db}"));
            assert_eq!(
                entry.value,
                Bytes::from(val.clone()),
                "wrong value for key {:?} on db {}",
                key,
                db
            );

            // Key must not appear on the other two DBs.
            for other in 0..3usize {
                if other == db {
                    continue;
                }
                let other_cache = loaded.get(other).unwrap();
                let found = other_cache.load(&key, Default::default()).unwrap();
                assert!(
                    found.is_none(),
                    "key {:?} for db {db} also found on db {other}",
                    key
                );
            }
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Lazy SELECT: DB0 writes need no SELECT; first non-zero DB write emits SELECT.
#[test]
fn aof_select_serial_still_lazy() {
    let dir = tmp_dir("lazy");
    let aof_path = dir.join("appendonly.aof");
    let mgr = make_pm(&dir);

    // Two writes on DB 0 — should not emit SELECT (lazy, default DB).
    mgr.on_write_command(0, &set_args("a", "1"));
    mgr.on_write_command(0, &set_args("b", "2"));

    let raw = std::fs::read(&aof_path).unwrap();
    let text = String::from_utf8_lossy(&raw).into_owned();
    assert!(
        !text.to_uppercase().contains("SELECT"),
        "DB0-only writes must not emit SELECT; got:\n{text}"
    );

    // Switch to DB 1 — SELECT then SET.
    mgr.on_write_command(1, &set_args("c", "3"));

    let mut cmds: Vec<Vec<Bytes>> = Vec::new();
    aof::load_file_with(&aof_path, |argv| {
        cmds.push(argv);
        Ok(())
    })
    .unwrap();

    // Expect: SET a, SET b, SELECT 1, SET c
    assert_eq!(cmds.len(), 4, "expected 4 AOF commands, got {:?}", cmds.len());
    assert_eq!(cmd_name(&cmds[0]), "SET");
    assert_eq!(cmds[0][1], Bytes::from_static(b"a"));
    assert_eq!(cmd_name(&cmds[1]), "SET");
    assert_eq!(cmds[1][1], Bytes::from_static(b"b"));
    assert_eq!(cmd_name(&cmds[2]), "SELECT");
    assert_eq!(cmds[2][1], Bytes::from_static(b"1"));
    assert_eq!(cmd_name(&cmds[3]), "SET");
    assert_eq!(cmds[3][1], Bytes::from_static(b"c"));

    // Back to DB 0 — SELECT 0 then SET.
    mgr.on_write_command(0, &set_args("d", "4"));
    cmds.clear();
    aof::load_file_with(&aof_path, |argv| {
        cmds.push(argv);
        Ok(())
    })
    .unwrap();
    assert_eq!(cmd_name(&cmds[4]), "SELECT");
    assert_eq!(cmds[4][1], Bytes::from_static(b"0"));
    assert_eq!(cmd_name(&cmds[5]), "SET");
    assert_eq!(cmds[5][1], Bytes::from_static(b"d"));

    // Replay correctness
    let loaded = make_databases();
    aof::load_into_databases(&loaded, &aof_path).unwrap();
    let e = loaded
        .get(0)
        .unwrap()
        .load(&Bytes::from_static(b"a"), Default::default())
        .unwrap()
        .unwrap();
    assert_eq!(e.value, Bytes::from_static(b"1"));
    let e = loaded
        .get(0)
        .unwrap()
        .load(&Bytes::from_static(b"d"), Default::default())
        .unwrap()
        .unwrap();
    assert_eq!(e.value, Bytes::from_static(b"4"));
    let e = loaded
        .get(1)
        .unwrap()
        .load(&Bytes::from_static(b"c"), Default::default())
        .unwrap()
        .unwrap();
    assert_eq!(e.value, Bytes::from_static(b"3"));

    let _ = std::fs::remove_dir_all(&dir);
}

/// In the AOF stream, whenever the logical DB changes between writes, SELECT
/// appears immediately before the write command.
#[test]
fn aof_select_precedes_write_on_db_change() {
    let dir = tmp_dir("stream-order");
    let aof_path = dir.join("appendonly.aof");
    let mgr = make_pm(&dir);

    // Sequence of DBs for successive SETs: 0, 0, 2, 2, 1, 0
    let sequence = [0usize, 0, 2, 2, 1, 0];
    for (i, &db) in sequence.iter().enumerate() {
        let key = format!("k{i}");
        let val = format!("v{i}");
        mgr.on_write_command(db, &set_args(&key, &val));
    }

    let mut cmds: Vec<Vec<Bytes>> = Vec::new();
    aof::load_file_with(&aof_path, |argv| {
        cmds.push(argv);
        Ok(())
    })
    .unwrap();

    // Walk the stream: track logical DB; every SET must match expected DB;
    // SELECT must immediately precede a write when DB changes.
    let mut logical_db: Option<usize> = None; // None = unknown / default 0
    let mut set_idx = 0usize;
    let mut i = 0usize;
    while i < cmds.len() {
        let name = cmd_name(&cmds[i]);
        if name == "SELECT" {
            assert!(
                i + 1 < cmds.len(),
                "SELECT must be followed by a write command"
            );
            let next = cmd_name(&cmds[i + 1]);
            assert_ne!(next, "SELECT", "consecutive SELECTs without write");
            let db: usize = std::str::from_utf8(&cmds[i][1]).unwrap().parse().unwrap();
            // After SELECT, next write should target this db
            logical_db = Some(db);
            i += 1;
            continue;
        }

        // Write command
        let expected_db = sequence[set_idx];
        let effective = logical_db.unwrap_or(0);
        assert_eq!(
            effective, expected_db,
            "SET at stream index {i} (set #{set_idx}) effective db {effective} != expected {expected_db}"
        );

        // If DB changed from previous write, we must have just processed SELECT
        // (i.e. previous command was SELECT) — enforced by effective matching.
        // Additionally: when expected_db differs from prior sequence entry,
        // the command immediately before this SET must be SELECT.
        if set_idx > 0 && sequence[set_idx] != sequence[set_idx - 1] {
            assert!(
                i > 0 && cmd_name(&cmds[i - 1]) == "SELECT",
                "db change to {} at set #{set_idx}: expected SELECT immediately before write, got {:?}",
                expected_db,
                cmds.get(i.wrapping_sub(1)).map(|c| cmd_name(c))
            );
        } else if set_idx == 0 && expected_db != 0 {
            assert!(
                i > 0 && cmd_name(&cmds[i - 1]) == "SELECT",
                "first write on non-zero db must be preceded by SELECT"
            );
        } else if set_idx == 0 && expected_db == 0 {
            assert!(
                i == 0 || cmd_name(&cmds[i - 1]) != "SELECT",
                "first DB0 write must not be preceded by SELECT"
            );
        }

        set_idx += 1;
        i += 1;
    }
    assert_eq!(set_idx, sequence.len(), "not all SETs found in AOF");

    let _ = std::fs::remove_dir_all(&dir);
}

/// When SELECT is emitted with a write, replication must see them as one
/// contiguous payload (single `propagate_raw`) so a concurrent PSYNC cannot
/// register a feed between SELECT and the write command.
#[test]
fn replication_select_and_write_are_atomic_in_backlog() {
    let dir = tmp_dir("repl-atomic");
    // AOF optional for this check; enable so AOF still gets separate appends.
    let mgr = make_pm(&dir);

    // Intercept live feed before the write.
    let mut feed = mgr.replication.register_replica();

    // DB 1 write → SELECT 1 + SET must be emitted.
    mgr.on_write_command(1, &set_args("k", "v"));

    let msg = feed
        .try_recv()
        .expect("expected one propagated replication message");
    assert!(
        feed.try_recv().is_err(),
        "SELECT+cmd must be a single atomic propagate, not two messages"
    );

    let select_raw = aof::encode_command(&[
        Bytes::from_static(b"SELECT"),
        Bytes::from_static(b"1"),
    ]);
    let set_raw = aof::encode_command(&set_args("k", "v"));
    let mut expected = select_raw.to_vec();
    expected.extend_from_slice(&set_raw);

    assert_eq!(
        msg.as_ref(),
        expected.as_slice(),
        "SELECT+SET must appear as one contiguous backlog/feed payload"
    );
    assert_eq!(
        mgr.replication.master_repl_offset(),
        expected.len() as u64,
        "backlog offset must equal combined SELECT+SET length"
    );

    // AOF still records SELECT and SET as separate RESP commands.
    let aof_path = dir.join("appendonly.aof");
    let mut cmds: Vec<Vec<Bytes>> = Vec::new();
    aof::load_file_with(&aof_path, |argv| {
        cmds.push(argv);
        Ok(())
    })
    .unwrap();
    assert_eq!(cmds.len(), 2);
    assert_eq!(cmd_name(&cmds[0]), "SELECT");
    assert_eq!(cmds[0][1], Bytes::from_static(b"1"));
    assert_eq!(cmd_name(&cmds[1]), "SET");
    assert_eq!(cmds[1][1], Bytes::from_static(b"k"));

    let _ = std::fs::remove_dir_all(&dir);
}

/// Stress the race many times so a non-atomic SELECT path fails reliably.
#[test]
fn aof_select_concurrent_stress_no_cross_db_pollution() {
    // Run several independent concurrent batches; any cross-DB key is a failure.
    for batch in 0..4 {
        let dir = tmp_dir(&format!("stress-{batch}"));
        let aof_path = dir.join("appendonly.aof");
        let mgr = make_pm(&dir);

        const N_THREADS: usize = 12;
        const M_WRITES: usize = 80;

        let mut handles = Vec::with_capacity(N_THREADS);
        for t in 0..N_THREADS {
            let mgr = Arc::clone(&mgr);
            handles.push(thread::spawn(move || {
                for i in 0..M_WRITES {
                    let db = (t + i) % 4; // rotate DBs within a thread too
                    let key = format!("b{batch}:t{t}:i{i}");
                    let val = format!("db{db}");
                    mgr.on_write_command(db, &set_args(&key, &val));
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let loaded = make_databases();
        aof::load_into_databases(&loaded, &aof_path).unwrap();

        for t in 0..N_THREADS {
            for i in 0..M_WRITES {
                let db = (t + i) % 4;
                let key = Bytes::from(format!("b{batch}:t{t}:i{i}"));
                let entry = loaded
                    .get(db)
                    .unwrap()
                    .load(&key, Default::default())
                    .unwrap()
                    .unwrap_or_else(|| panic!("batch {batch}: missing {key:?} on db {db}"));
                assert_eq!(
                    entry.value,
                    Bytes::from(format!("db{db}")),
                    "batch {batch}: wrong value for {key:?}"
                );
                for other in 0..4usize {
                    if other == db {
                        continue;
                    }
                    assert!(
                        loaded
                            .get(other)
                            .unwrap()
                            .load(&key, Default::default())
                            .unwrap()
                            .is_none(),
                        "batch {batch}: key {key:?} leaked to db {other}"
                    );
                }
            }
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
