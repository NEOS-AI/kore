//! Batch DR: multi-DB lock-step keyspace install under the epoch lock.

use bytes::Bytes;
use kore::databases::Databases;
use kore::entry::StoreOptions;
use kore::persistence::rdb::MultiDbSnapshot;
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

/// Raw per-DB Arc access (no epoch lock) can still see mid-loop tear — documents
/// residual; the hook proves DB0 is already new while DB1 is still old.
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
