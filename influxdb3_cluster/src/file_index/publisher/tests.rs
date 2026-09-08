use std::sync::Arc;

use influxdb3_catalog::catalog::CatalogSequenceNumber;
use influxdb3_id::{DbId, ParquetFileId, TableId};
use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};
use influxdb3_write::persister::Persister;
use influxdb3_write::{
    ChunkFilter, DatabaseTables, ParquetFile, PersistedSnapshot, PersistedSnapshotVersion,
};
use iox_time::{MockProvider, Time, TimeProvider};
use object_store::ObjectStore;
use object_store::memory::InMemory;

use super::*;
use crate::file_index::FileIndex;
use crate::file_index::log::FileIndexLog;

const NODE: &str = "host01";
const DB: DbId = DbId::new(1);
const TABLE: TableId = TableId::new(0);

fn file(id: u64, path: &str) -> ParquetFile {
    ParquetFile {
        id: ParquetFileId::from(id),
        path: path.into(),
        size_bytes: 100,
        row_count: 10,
        chunk_time: 0,
        min_time: 0,
        max_time: 10,
    }
}

/// A manifest at `seq` that adds and/or removes files for one table.
fn manifest(
    seq: u64,
    added: Vec<ParquetFile>,
    removed: Vec<ParquetFile>,
) -> PersistedSnapshotVersion {
    let mut snapshot = PersistedSnapshot::new(
        NODE,
        SnapshotSequenceNumber::new(seq),
        WalFileSequenceNumber::new(seq),
        CatalogSequenceNumber::new(seq),
    );
    if !added.is_empty() {
        let mut tables = DatabaseTables::default();
        tables.tables.insert(TABLE, added);
        snapshot.databases.insert(DB, tables);
    }
    if !removed.is_empty() {
        let mut tables = DatabaseTables::default();
        tables.tables.insert(TABLE, removed);
        snapshot.removed_files.insert(DB, tables);
    }
    PersistedSnapshotVersion::V1(snapshot)
}

struct Harness {
    persister: Arc<Persister>,
    log: Arc<FileIndexLog>,
    index: Arc<FileIndex>,
}

fn harness() -> Harness {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let time: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    Harness {
        persister: Arc::new(Persister::new(
            Arc::clone(&store),
            NODE,
            Arc::clone(&time),
            None,
        )),
        log: Arc::new(FileIndexLog::new(Arc::clone(&store), "mycluster".into())),
        index: Arc::new(FileIndex::new()),
    }
}

impl Harness {
    async fn persist(&self, m: &PersistedSnapshotVersion) {
        self.persister.persist_snapshot(m).await.unwrap();
    }

    async fn publish(&self) {
        publish_pending(&NODE.into(), &self.persister, &self.log, &self.index)
            .await
            .unwrap();
    }

    fn paths(&self) -> Vec<String> {
        self.index
            .get_files_filtered(DB, TABLE, &ChunkFilter::default(), None)
            .into_iter()
            .map(|f| f.path.to_string())
            .collect()
    }
}

#[tokio::test]
async fn publishes_a_single_manifest() {
    let h = harness();
    h.persist(&manifest(1, vec![file(1, "host01/a.parquet")], vec![]))
        .await;
    h.publish().await;

    assert_eq!(h.paths(), vec!["host01/a.parquet"]);
    assert_eq!(
        h.index.published_watermark(NODE),
        Some(SnapshotSequenceNumber::new(1))
    );
}

#[tokio::test]
async fn snapshots_that_coalesced_on_the_watch_are_all_published() {
    // The reason the publisher reads manifests instead of the watch payload. `tokio::sync::watch`
    // keeps only the latest value, so two snapshots completing between polls deliver ONE
    // notification. Reading from the persister means both are still found.
    let h = harness();
    h.persist(&manifest(1, vec![file(1, "host01/a.parquet")], vec![]))
        .await;
    h.persist(&manifest(2, vec![file(2, "host01/b.parquet")], vec![]))
        .await;
    h.persist(&manifest(3, vec![file(3, "host01/c.parquet")], vec![]))
        .await;

    // One pass, standing in for the single notification that survived coalescing.
    h.publish().await;

    let mut got = h.paths();
    got.sort();
    assert_eq!(
        got,
        vec!["host01/a.parquet", "host01/b.parquet", "host01/c.parquet"],
        "every manifest must be published, not just the one the watch retained"
    );
    assert_eq!(
        h.index.published_watermark(NODE),
        Some(SnapshotSequenceNumber::new(3))
    );
}

#[tokio::test]
async fn republishing_is_idempotent() {
    let h = harness();
    h.persist(&manifest(1, vec![file(1, "host01/a.parquet")], vec![]))
        .await;

    h.publish().await;
    let sequence_after_first = h.index.sequence();
    h.publish().await;

    assert_eq!(h.index.file_count(), 1);
    assert_eq!(
        h.index.sequence(),
        sequence_after_first,
        "a pass with nothing new must not burn a log sequence"
    );
}

#[tokio::test]
async fn a_restarted_publisher_resumes_from_its_watermark() {
    let h = harness();
    h.persist(&manifest(1, vec![file(1, "host01/a.parquet")], vec![]))
        .await;
    h.publish().await;

    // Simulate a restart: fresh in-memory index, same object store. The watermark has to come
    // back from the log, or every manifest gets published a second time.
    let restarted = Harness {
        persister: Arc::clone(&h.persister),
        log: Arc::clone(&h.log),
        index: Arc::new(h.log.load().await.unwrap()),
    };
    assert_eq!(
        restarted.index.published_watermark(NODE),
        Some(SnapshotSequenceNumber::new(1)),
        "watermark must survive a restart"
    );

    h.persist(&manifest(2, vec![file(2, "host01/b.parquet")], vec![]))
        .await;
    restarted.publish().await;

    assert_eq!(restarted.index.file_count(), 2);
    assert_eq!(
        restarted.paths().len(),
        2,
        "the already-published manifest must not be indexed twice"
    );
}

#[tokio::test]
async fn a_manifest_removing_and_adding_applies_the_removal_first() {
    // What a compaction looks like: inputs retired, merged output added, in one manifest.
    let h = harness();
    h.persist(&manifest(
        1,
        vec![file(1, "host01/a.parquet"), file(2, "host01/b.parquet")],
        vec![],
    ))
    .await;
    h.publish().await;
    assert_eq!(h.index.file_count(), 2);

    h.persist(&manifest(
        2,
        vec![file(3, "host01/merged.parquet")],
        vec![file(1, "host01/a.parquet"), file(2, "host01/b.parquet")],
    ))
    .await;
    h.publish().await;

    assert_eq!(h.paths(), vec!["host01/merged.parquet"]);
}

#[tokio::test]
async fn manifests_are_published_oldest_first() {
    // `load_snapshots*` returns newest-first, because snapshot paths encode an inverted sequence.
    // Publishing in that order would let manifest 1's addition undo manifest 2's removal of it.
    let h = harness();
    h.persist(&manifest(1, vec![file(1, "host01/a.parquet")], vec![]))
        .await;
    h.persist(&manifest(2, vec![], vec![file(1, "host01/a.parquet")]))
        .await;

    h.publish().await;

    assert!(
        h.paths().is_empty(),
        "the later removal must win, so ordering has to be oldest-first"
    );
}

#[tokio::test]
async fn nothing_to_publish_is_not_an_error() {
    let h = harness();
    h.publish().await;

    assert_eq!(h.index.file_count(), 0);
    assert_eq!(h.index.published_watermark(NODE), None);
}

#[tokio::test]
async fn a_manifest_that_never_fired_the_watch_is_still_published() {
    // The load-test bug. A compaction manifest is written by the peer-RPC handler calling
    // `persist_snapshot` directly, and the watch channel's only sender lives in the regular
    // snapshot task — so no notification is ever emitted for it.
    //
    // Waiting solely on the watch left the removal unpublished while the compactor, on its own
    // grace timer, deleted the input files anyway. Queries then resolved paths that no longer
    // existed and failed with NotFound. The scheduled pass is what makes that impossible: it
    // reads manifests regardless of whether anything announced them.
    let h = harness();
    h.persist(&manifest(
        1,
        vec![file(1, "host01/a.parquet"), file(2, "host01/b.parquet")],
        vec![],
    ))
    .await;
    h.publish().await;
    assert_eq!(h.index.file_count(), 2);

    // A compaction lands: inputs retired, merged output added. No watch notification accompanies
    // it — which is exactly why the publisher must not depend on one.
    h.persist(&manifest(
        2,
        vec![file(3, "host01/merged.parquet")],
        vec![file(1, "host01/a.parquet"), file(2, "host01/b.parquet")],
    ))
    .await;

    // The scheduled pass, standing in for the tick rather than a notification.
    h.publish().await;

    assert_eq!(
        h.paths(),
        vec!["host01/merged.parquet"],
        "the removal must reach the index without any watch notification, or queries will \
         resolve files the compactor has already deleted"
    );
}
