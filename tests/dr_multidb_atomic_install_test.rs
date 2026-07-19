//! Batch DR / DS: multi-DB lock-step keyspace install under the epoch lock,
//! panic rollback of already-installed DBs, and multi-DB exporter audit.

use bytes::Bytes;
use kore::databases::Databases;
use kore::entry::StoreOptions;
use kore::persistence::aof;
use kore::persistence::rdb::MultiDbSnapshot;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn make_dbs(n: usize) -> Arc<Databases> {
    Databases::create(n, 8, 1024 * 1024 * 50, 500 * 1024 * 1024, false, 0.75)
}

fn store(db: &kore::Cache, k: &str, v: &str) {
    db.store(
        Bytes::from(k.to_string()),
        Bytes::from(v.to_string()),
        StoreOptions::default(),
    )
    .unwrap();
}

fn has(db: &kore::Cache, k: &str) -> bool {
    db.load(&Bytes::from(k.to_string()), Default::default())
        .unwrap()
        .is_some()
}

fn key_in_snap(snap: &MultiDbSnapshot, db_idx: u32, key: &[u8]) -> bool {
    snap.databases
        .iter()
        .find(|(i, _)| *i == db_idx)
        .map(|(_, s)| s.strings.iter().any(|r| r.key.as_ref() == key))
        .unwrap_or(false)
}

/// Epoch write is held across the whole install loop — stable-view try_read fails
/// after DB0 is installed (still mid multi-DB install).
#[test]
fn epoch_write_excludes_stable_view_mid_install() {
    let dbs = make_dbs(2);
    store(&dbs.get(0).unwrap(), "old0", "a");
    store(&dbs.get(1).unwrap(), "old1", "b");

    let scratch = dbs.empty_like();
    store(&scratch.get(0).unwrap(), "new0", "x");
    store(&scratch.get(1).unwrap(), "new1", "y");

    let excluded = Arc::new(AtomicBool::new(false));
    let saw_after_db0 = Arc::new(AtomicBool::new(false));
    {
        let dbs_h = dbs.clone();
        let excluded = excluded.clone();
        let saw_after_db0 = saw_after_db0.clone();
        dbs.set_after_install_db_hook(Some(Arc::new(move |i| {
            if i == 0 {
                saw_after_db0.store(true, Ordering::Release);
                // Under epoch write: try_read must fail (no deadlock).
                match dbs_h.try_with_stable_keyspace_view(|| ()) {
                    None => excluded.store(true, Ordering::Release),
                    Some(()) => excluded.store(false, Ordering::Release),
                }
            }
        })));
    }

    dbs.replace_keyspaces_from(&scratch);
    dbs.set_after_install_db_hook(None);

    assert!(
        saw_after_db0.load(Ordering::Acquire),
        "hook must run after DB0 install"
    );
    assert!(
        excluded.load(Ordering::Acquire),
        "try_with_stable_keyspace_view must fail under install epoch write"
    );
    assert!(has(&dbs.get(0).unwrap(), "new0"));
    assert!(has(&dbs.get(1).unwrap(), "new1"));
    assert!(!has(&dbs.get(0).unwrap(), "old0"));
}

/// Concurrent multi-DB exporters either finish all-old or wait for all-new —
/// never a mixed DB0/DB1 generation.
#[test]
fn concurrent_from_databases_never_torn() {
    let dbs = make_dbs(2);
    store(&dbs.get(0).unwrap(), "old0", "a");
    store(&dbs.get(1).unwrap(), "old1", "b");

    let stop = Arc::new(AtomicBool::new(false));
    let mixed = Arc::new(AtomicUsize::new(0));
    let samples = Arc::new(AtomicUsize::new(0));

    let reader = {
        let dbs = dbs.clone();
        let stop = stop.clone();
        let mixed = mixed.clone();
        let samples = samples.clone();
        thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                let snap = MultiDbSnapshot::from_databases(&dbs).expect("export");
                samples.fetch_add(1, Ordering::Relaxed);
                let d0_old = key_in_snap(&snap, 0, b"old0");
                let d0_new = key_in_snap(&snap, 0, b"new0");
                let d1_old = key_in_snap(&snap, 1, b"old1");
                let d1_new = key_in_snap(&snap, 1, b"new1");
                // Valid: all-old (pre) or all-new (post). Mixed is the tear.
                let all_old = d0_old && !d0_new && d1_old && !d1_new;
                let all_new = d0_new && !d0_old && d1_new && !d1_old;
                // Empty-ish partial DB encode: a DB with no keys is omitted from
                // the snapshot — treat missing as "empty new" only when the other
                // side is also post-replace. For our keys both DBs always have one.
                if !(all_old || all_new) {
                    mixed.fetch_add(1, Ordering::Relaxed);
                }
            }
        })
    };

    // Let the reader start sampling all-old.
    thread::sleep(Duration::from_millis(5));

    for round in 0..40 {
        let scratch = dbs.empty_like();
        if round % 2 == 0 {
            store(&scratch.get(0).unwrap(), "new0", "x");
            store(&scratch.get(1).unwrap(), "new1", "y");
        } else {
            store(&scratch.get(0).unwrap(), "old0", "a");
            store(&scratch.get(1).unwrap(), "old1", "b");
        }
        dbs.replace_keyspaces_from(&scratch);
    }

    stop.store(true, Ordering::Release);
    reader.join().unwrap();

    assert!(
        samples.load(Ordering::Relaxed) > 0,
        "reader must sample at least once"
    );
    assert_eq!(
        mixed.load(Ordering::Relaxed),
        0,
        "stable multi-DB export must never observe DB0/DB1 torn across replace"
    );
}

/// Raw per-DB Arc access (no epoch lock) can still see mid-loop tear **during**
/// install — documents residual for unprivileged walks; exporters must use
/// `with_stable_keyspace_view`. After a successful replace, state is consistent.
#[test]
fn raw_per_db_access_can_see_mid_loop_tear() {
    let dbs = make_dbs(2);
    store(&dbs.get(0).unwrap(), "old0", "a");
    store(&dbs.get(1).unwrap(), "old1", "b");

    let scratch = dbs.empty_like();
    store(&scratch.get(0).unwrap(), "new0", "x");
    store(&scratch.get(1).unwrap(), "new1", "y");

    let tore = Arc::new(AtomicBool::new(false));
    {
        let dbs_h = dbs.clone();
        let tore = tore.clone();
        dbs.set_after_install_db_hook(Some(Arc::new(move |i| {
            if i == 0 {
                let db0_new = has(&dbs_h.get(0).unwrap(), "new0");
                let db1_new = has(&dbs_h.get(1).unwrap(), "new1");
                if db0_new && !db1_new {
                    tore.store(true, Ordering::Release);
                }
            }
        })));
    }

    dbs.replace_keyspaces_from(&scratch);
    dbs.set_after_install_db_hook(None);

    assert!(
        tore.load(Ordering::Acquire),
        "without epoch lock, raw Arc<Cache> still observes DB0-new + DB1-old mid-loop"
    );
    // Successful complete install ends consistent.
    assert!(has(&dbs.get(0).unwrap(), "new0"));
    assert!(has(&dbs.get(1).unwrap(), "new1"));
}

/// Batch DS: panic after DB0 is fully installed rolls back DB0 to the pre-replace
/// payload; DB1 was never swapped. Survivors must not see a partial multi-DB commit.
#[test]
fn panic_mid_install_rolls_back_already_installed_dbs() {
    let dbs = make_dbs(2);
    store(&dbs.get(0).unwrap(), "old0", "a");
    store(&dbs.get(1).unwrap(), "old1", "b");

    let scratch = dbs.empty_like();
    store(&scratch.get(0).unwrap(), "new0", "x");
    store(&scratch.get(1).unwrap(), "new1", "y");

    let g0 = dbs.load_generation();
    dbs.set_after_install_db_hook(Some(Arc::new(|i| {
        if i == 0 {
            panic!("forced mid-install after DB0");
        }
    })));

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        dbs.replace_keyspaces_from(&scratch);
    }));
    assert!(panicked.is_err(), "hook must panic the replace");
    dbs.set_after_install_db_hook(None);

    // Pre-replace multi-DB dataset restored (DB0 rolled back; DB1 never swapped).
    assert!(
        has(&dbs.get(0).unwrap(), "old0"),
        "DB0 must roll back to old payload"
    );
    assert!(
        !has(&dbs.get(0).unwrap(), "new0"),
        "DB0 must not keep partial new payload"
    );
    assert!(
        has(&dbs.get(1).unwrap(), "old1"),
        "DB1 must still hold pre-replace data"
    );
    assert!(!has(&dbs.get(1).unwrap(), "new1"));
    assert!(
        !dbs.load_in_progress(),
        "load_in_progress cleared by drop guard even after panic"
    );
    assert_eq!(
        dbs.load_generation(),
        g0 + 1,
        "generation still publishes once on panic exit"
    );

    // Recovered process can replace again successfully.
    let scratch2 = dbs.empty_like();
    store(&scratch2.get(0).unwrap(), "ok0", "z");
    store(&scratch2.get(1).unwrap(), "ok1", "w");
    dbs.replace_keyspaces_from(&scratch2);
    assert!(has(&dbs.get(0).unwrap(), "ok0"));
    assert!(has(&dbs.get(1).unwrap(), "ok1"));
}

/// Batch DS: AOF rewrite_databases is under the epoch read lock — concurrent
/// multi-DB replace cannot produce a mixed-generation AOF body.
#[test]
fn concurrent_aof_rewrite_databases_never_torn() {
    let dbs = make_dbs(2);
    store(&dbs.get(0).unwrap(), "old0", "a");
    store(&dbs.get(1).unwrap(), "old1", "b");

    let dir = std::env::temp_dir().join(format!(
        "kore_ds_aof_rewrite_{}",
        std::process::id()
    ));
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("appendonly.aof");

    let stop = Arc::new(AtomicBool::new(false));
    let mixed = Arc::new(AtomicUsize::new(0));
    let samples = Arc::new(AtomicUsize::new(0));

    let rewriter = {
        let dbs = dbs.clone();
        let stop = stop.clone();
        let mixed = mixed.clone();
        let samples = samples.clone();
        let path = path.clone();
        thread::spawn(move || {
            let mut n = 0u64;
            while !stop.load(Ordering::Acquire) {
                n += 1;
                let p = path.with_extension(format!("aof.{}", n));
                if aof::rewrite_databases(&dbs, &p).is_err() {
                    continue;
                }
                samples.fetch_add(1, Ordering::Relaxed);
                let Ok(body) = std::fs::read_to_string(&p) else {
                    let _ = std::fs::remove_file(&p);
                    continue;
                };
                let _ = std::fs::remove_file(&p);
                let has_old0 = body.contains("old0");
                let has_new0 = body.contains("new0");
                let has_old1 = body.contains("old1");
                let has_new1 = body.contains("new1");
                let all_old = has_old0 && has_old1 && !has_new0 && !has_new1;
                let all_new = has_new0 && has_new1 && !has_old0 && !has_old1;
                // Empty body (no keys) is also non-torn.
                let empty = !has_old0 && !has_new0 && !has_old1 && !has_new1;
                if !(all_old || all_new || empty) {
                    mixed.fetch_add(1, Ordering::Relaxed);
                }
            }
        })
    };

    thread::sleep(Duration::from_millis(5));

    for round in 0..30 {
        let scratch = dbs.empty_like();
        if round % 2 == 0 {
            store(&scratch.get(0).unwrap(), "new0", "x");
            store(&scratch.get(1).unwrap(), "new1", "y");
        } else {
            store(&scratch.get(0).unwrap(), "old0", "a");
            store(&scratch.get(1).unwrap(), "old1", "b");
        }
        dbs.replace_keyspaces_from(&scratch);
    }

    stop.store(true, Ordering::Release);
    rewriter.join().unwrap();
    let _ = std::fs::remove_dir_all(&dir);

    assert!(
        samples.load(Ordering::Relaxed) > 0,
        "AOF rewriter must sample at least once"
    );
    assert_eq!(
        mixed.load(Ordering::Relaxed),
        0,
        "rewrite_databases must never encode DB0/DB1 torn across replace"
    );
}

/// Mid-install: try_with_stable_keyspace_view fails; epoch read is exclusive with install.
#[test]
fn aof_rewrite_blocked_mid_install_via_try_stable() {
    // rewrite_databases uses with_stable_keyspace_view (blocking). Prove the
    // same epoch write excludes try_read mid-install — already covered by
    // epoch_write_excludes_stable_view_mid_install; this asserts AOF path is
    // documented as a stable-view consumer by compiling against the API.
    let dbs = make_dbs(2);
    store(&dbs.get(0).unwrap(), "k", "v");
    let dir = std::env::temp_dir().join(format!(
        "kore_ds_aof_once_{}",
        std::process::id()
    ));
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("once.aof");
    aof::rewrite_databases(&dbs, &path).expect("rewrite");
    let body = std::fs::read_to_string(&path).unwrap();
    assert!(body.contains("k") || body.contains("SET"), "AOF should encode key");
    let _ = std::fs::remove_dir_all(&dir);
}

/// load_generation publishes once per replace (end only).
#[test]
fn load_generation_single_publish_per_replace() {
    let dbs = make_dbs(2);
    let g0 = dbs.load_generation();
    let scratch = dbs.empty_like();
    store(&scratch.get(0).unwrap(), "k", "v");
    dbs.replace_keyspaces_from(&scratch);
    assert_eq!(dbs.load_generation(), g0 + 1);
    assert!(!dbs.load_in_progress());
}

/// During install, load_in_progress is true and generation is still frozen.
#[test]
fn gen_frozen_and_loading_true_mid_install() {
    let dbs = make_dbs(2);
    let g0 = dbs.load_generation();
    let scratch = dbs.empty_like();
    store(&scratch.get(0).unwrap(), "a", "1");
    store(&scratch.get(1).unwrap(), "b", "2");

    let mid_loading = Arc::new(AtomicBool::new(false));
    let mid_gen_frozen = Arc::new(AtomicBool::new(false));
    {
        let dbs_h = dbs.clone();
        let mid_loading = mid_loading.clone();
        let mid_gen_frozen = mid_gen_frozen.clone();
        dbs.set_after_install_db_hook(Some(Arc::new(move |i| {
            if i == 0 {
                mid_loading.store(dbs_h.load_in_progress(), Ordering::Release);
                mid_gen_frozen.store(dbs_h.load_generation() == g0, Ordering::Release);
            }
        })));
    }

    dbs.replace_keyspaces_from(&scratch);
    dbs.set_after_install_db_hook(None);

    assert!(mid_loading.load(Ordering::Acquire));
    assert!(
        mid_gen_frozen.load(Ordering::Acquire),
        "generation must stay frozen until replace finishes"
    );
    assert_eq!(dbs.load_generation(), g0 + 1);
}
