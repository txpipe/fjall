use fjall::{AbstractTree, Database, KeyspaceCreateOptions};
use std::{sync::mpsc, thread, time::Duration};

/// A writer blocked at the L0 write-halt threshold must not stay blocked once
/// the queued compactions are spent: with idle workers, the halt has to
/// request compaction itself and clear.
#[test_log::test]
fn write_halt_clears_with_idle_workers() -> fjall::Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Database::builder(&dir).worker_threads(4).open()?;
    let ks = db.keyspace("items", KeyspaceCreateOptions::default)?;

    for i in 0..30u32 {
        // Keys at both ends make every table overlap, so each flush is its own run.
        ks.insert("a", i.to_be_bytes())?;
        ks.insert("z", i.to_be_bytes())?;
        ks.tree.flush_active_memtable(0)?; // adds a run without queueing a compaction
    }
    assert_eq!(30, ks.tree.l0_run_count());

    let (tx, rx) = mpsc::channel();
    let writer = ks.clone();
    thread::spawn(move || tx.send(writer.insert("final", "value")).unwrap());

    rx.recv_timeout(Duration::from_secs(10))
        .expect("write still halted at 30 L0 runs with idle workers")?;

    Ok(())
}
