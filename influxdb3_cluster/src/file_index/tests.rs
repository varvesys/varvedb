use std::sync::Arc;

use influxdb3_id::{DbId, ParquetFileId, TableId};
use influxdb3_write::{ChunkFilter, ParquetFile};
use object_store::ObjectStore;
use object_store::memory::InMemory;

use super::log::FileIndexLog;
use super::*;

const DB: DbId = DbId::new(1);
const TABLE: TableId = TableId::new(0);

fn file(id: u64, path: &str, min_time: i64, max_time: i64) -> ParquetFile {
    ParquetFile {
        id: ParquetFileId::from(id),
        path: path.into(),
        size_bytes: 100,
        row_count: 10,
        chunk_time: min_time,
        min_time,
        max_time,
    }
}

fn delta(node: &str, added: Vec<ParquetFile>, removed: Vec<&str>) -> FileIndexDelta {
    FileIndexDelta {
        node_id: node.into(),
        db_id: DB,
        table_id: TABLE,
        snapshot_sequence: SnapshotSequenceNumber::new(1),
        added,
        removed: removed.into_iter().map(Arc::from).collect(),
    }
}

fn entry(seq: u64, deltas: Vec<FileIndexDelta>) -> FileIndexLogEntry {
    FileIndexLogEntry {
        sequence: FileIndexSequence::new(seq),
        deltas,
    }
}

fn paths(index: &FileIndex) -> Vec<String> {
    index
        .get_files_filtered(DB, TABLE, &ChunkFilter::default())
        .into_iter()
        .map(|f| f.path.to_string())
        .collect()
}

fn log() -> (FileIndexLog, Arc<dyn ObjectStore>) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    (
        FileIndexLog::new(Arc::clone(&store), "mycluster".into()),
        store,
    )
}

// ---- index semantics ----

#[test]
fn applies_adds_and_removes_in_order() {
    let index = FileIndex::new();
    index.apply(&entry(
        1,
        vec![delta(
            "host01",
            vec![
                file(1, "host01/a.parquet", 0, 10),
                file(2, "host01/b.parquet", 10, 20),
            ],
            vec![],
        )],
    ));
    assert_eq!(index.file_count(), 2);

    index.apply(&entry(
        2,
        vec![delta(
            "host01",
            vec![file(3, "host01/merged.parquet", 0, 20)],
            vec!["host01/a.parquet", "host01/b.parquet"],
        )],
    ));

    assert_eq!(paths(&index), vec!["host01/merged.parquet"]);
    assert_eq!(index.sequence(), FileIndexSequence::new(2));
}

#[test]
fn dedups_additions_by_path_not_by_record() {
    // The bug this guards against: `ParquetFile` derives `PartialEq` over every field including
    // `id`, so a redelivered entry whose id was minted again compares unequal. Matching on the
    // whole record would insert the same file twice — double-scanned, double-counted against the
    // query file limit.
    let index = FileIndex::new();
    index.apply(&entry(
        1,
        vec![delta(
            "host01",
            vec![file(1, "host01/a.parquet", 0, 10)],
            vec![],
        )],
    ));
    index.apply(&entry(
        2,
        vec![delta(
            "host01",
            vec![file(999, "host01/a.parquet", 0, 10)],
            vec![],
        )],
    ));

    assert_eq!(index.file_count(), 1, "same path must not be indexed twice");
}

#[test]
fn replaying_the_same_log_twice_is_a_no_op() {
    let index = FileIndex::new();
    let e = entry(
        1,
        vec![delta(
            "host01",
            vec![file(1, "host01/a.parquet", 0, 10)],
            vec![],
        )],
    );

    index.apply(&e);
    index.apply(&e);
    index.apply(&entry(
        1,
        vec![delta(
            "host01",
            vec![file(2, "host01/b.parquet", 0, 10)],
            vec![],
        )],
    ));

    assert_eq!(
        index.file_count(),
        1,
        "entries at or below the current position are ignored"
    );
}

#[test]
fn removing_an_absent_file_is_a_no_op() {
    let index = FileIndex::new();
    index.apply(&entry(
        1,
        vec![delta(
            "host01",
            vec![file(1, "host01/a.parquet", 0, 10)],
            vec![],
        )],
    ));
    index.apply(&entry(
        2,
        vec![delta(
            "host01",
            vec![],
            vec!["host01/never-existed.parquet"],
        )],
    ));

    assert_eq!(paths(&index), vec!["host01/a.parquet"]);
}

#[test]
fn one_node_cannot_remove_another_nodes_file() {
    // Deltas are scoped to their owning node's prefix, so a removal cannot reach across.
    let index = FileIndex::new();
    index.apply(&entry(
        1,
        vec![delta(
            "host01",
            vec![file(1, "host01/a.parquet", 0, 10)],
            vec![],
        )],
    ));
    index.apply(&entry(
        2,
        vec![delta("host02", vec![], vec!["host01/a.parquet"])],
    ));

    assert_eq!(paths(&index), vec!["host01/a.parquet"]);
}

#[test]
fn ordering_puts_the_newest_publication_last() {
    // The direction that matters, and the one that was wrong. `ChunkOrder` is assigned by zipping
    // an ASCENDING counter over this slice, and HIGHER order wins deduplication — so the winner is
    // the file at the END. Ordering `added_at` descending would hand the newest file the lowest
    // order and silently resolve every cross-node conflict to the oldest copy.
    let index = FileIndex::new();
    index.apply(&entry(
        1,
        vec![delta(
            "host01",
            vec![file(1, "host01/old.parquet", 0, 10)],
            vec![],
        )],
    ));
    index.apply(&entry(
        2,
        vec![delta(
            "host02",
            vec![file(1, "host02/new.parquet", 0, 10)],
            vec![],
        )],
    ));

    assert_eq!(
        paths(&index),
        vec!["host01/old.parquet", "host02/new.parquet"],
        "newest publication must sort LAST so it receives the highest ChunkOrder"
    );
}

#[test]
fn assigned_chunk_order_favours_the_later_publication() {
    // Asserts on the order actually assigned, not on slice position. Position is exactly what
    // misled the original doc comment: the slice looked right while the resulting orders were
    // inverted.
    let index = FileIndex::new();
    index.apply(&entry(
        1,
        vec![delta(
            "host01",
            vec![file(1, "host01/old.parquet", 0, 10)],
            vec![],
        )],
    ));
    index.apply(&entry(
        2,
        vec![delta(
            "host02",
            vec![file(1, "host02/new.parquet", 0, 10)],
            vec![],
        )],
    ));

    // Mirrors `ClusterWriteBuffer::get_table_chunks`: ascending counter zipped over the slice.
    let orders: Vec<(String, i64)> = (0i64..)
        .zip(index.get_files_filtered(DB, TABLE, &ChunkFilter::default()))
        .map(|(order, f)| (f.path.to_string(), order))
        .collect();

    let newer = orders.iter().find(|(p, _)| p.contains("new")).unwrap().1;
    let older = orders.iter().find(|(p, _)| p.contains("old")).unwrap().1;
    assert!(
        newer > older,
        "later publication must get the higher ChunkOrder (higher wins dedup); got new={newer} old={older}"
    );
}

#[test]
fn colliding_file_ids_across_nodes_stay_distinct() {
    // Ids are per-allocator, so two nodes routinely mint the same one. Paths are what separate
    // them, because the node prefix leads.
    let index = FileIndex::new();
    index.apply(&entry(
        1,
        vec![
            delta("host01", vec![file(7, "host01/a.parquet", 0, 10)], vec![]),
            delta("host02", vec![file(7, "host02/a.parquet", 0, 10)], vec![]),
        ],
    ));

    assert_eq!(index.file_count(), 2);
}

#[test]
fn time_filter_prunes_non_overlapping_files() {
    let index = FileIndex::new();
    index.apply(&entry(
        1,
        vec![delta(
            "host01",
            vec![
                file(1, "host01/early.parquet", 0, 10),
                file(2, "host01/late.parquet", 1_000, 2_000),
            ],
            vec![],
        )],
    ));

    let mut filter = ChunkFilter::default();
    filter.time_lower_bound_ns = Some(500);
    let got = index.get_files_filtered(DB, TABLE, &filter);
    assert_eq!(got.len(), 1);
    assert_eq!(&*got[0].path, "host01/late.parquet");
}

#[test]
fn to_snapshot_round_trips_through_an_empty_index() {
    // What makes the log discardable behind a snapshot: the flattened form must replay to the
    // same state.
    let index = FileIndex::new();
    index.apply(&entry(
        1,
        vec![
            delta("host01", vec![file(1, "host01/a.parquet", 0, 10)], vec![]),
            delta("host02", vec![file(2, "host02/b.parquet", 10, 20)], vec![]),
        ],
    ));
    index.apply(&entry(
        2,
        vec![delta("host01", vec![], vec!["host01/a.parquet"])],
    ));

    let rebuilt = FileIndex::new();
    rebuilt.restore(&index.to_snapshot());

    assert_eq!(paths(&rebuilt), paths(&index));
    assert_eq!(rebuilt.sequence(), index.sequence());
}

// ---- log semantics ----

#[tokio::test]
async fn load_on_an_empty_store_yields_an_empty_index() {
    let (log, _) = log();
    let index = log.load().await.unwrap();

    assert_eq!(index.file_count(), 0);
    assert_eq!(index.sequence(), FileIndexSequence::new(0));
}

#[tokio::test]
async fn append_then_load_elsewhere_reproduces_the_index() {
    let (log, store) = log();
    let index = FileIndex::new();

    log.append(
        &index,
        vec![delta(
            "host01",
            vec![file(1, "host01/a.parquet", 0, 10)],
            vec![],
        )],
    )
    .await
    .unwrap();
    log.append(
        &index,
        vec![delta(
            "host02",
            vec![file(2, "host02/b.parquet", 10, 20)],
            vec![],
        )],
    )
    .await
    .unwrap();

    // A different node reading the same log must arrive at the same place.
    let other = FileIndexLog::new(Arc::clone(&store), "mycluster".into());
    let loaded = other.load().await.unwrap();

    assert_eq!(paths(&loaded), paths(&index));
    assert_eq!(loaded.sequence(), index.sequence());
}

#[tokio::test]
async fn concurrent_appenders_both_land_at_distinct_sequences() {
    // The CAS property. Two writers sharing a store must not overwrite one another, and the loser
    // must pick up the winner's entry rather than clobbering it.
    let (log_a, store) = log();
    let log_b = FileIndexLog::new(Arc::clone(&store), "mycluster".into());

    let index_a = FileIndex::new();
    let index_b = FileIndex::new();

    let seq_a = log_a
        .append(
            &index_a,
            vec![delta(
                "host01",
                vec![file(1, "host01/a.parquet", 0, 10)],
                vec![],
            )],
        )
        .await
        .unwrap();
    let seq_b = log_b
        .append(
            &index_b,
            vec![delta(
                "host02",
                vec![file(2, "host02/b.parquet", 0, 10)],
                vec![],
            )],
        )
        .await
        .unwrap();

    assert_ne!(seq_a, seq_b, "appends must occupy distinct sequences");

    // B caught up on A's entry while racing for its own slot.
    assert_eq!(index_b.file_count(), 2);

    log_a.sync(&index_a).await.unwrap();
    assert_eq!(paths(&index_a), paths(&index_b));
}

#[tokio::test]
async fn sync_applies_only_what_is_new() {
    let (log, store) = log();
    let writer = FileIndex::new();
    log.append(
        &writer,
        vec![delta(
            "host01",
            vec![file(1, "host01/a.parquet", 0, 10)],
            vec![],
        )],
    )
    .await
    .unwrap();

    let reader_log = FileIndexLog::new(Arc::clone(&store), "mycluster".into());
    let reader = reader_log.load().await.unwrap();
    assert_eq!(reader.file_count(), 1);

    assert_eq!(
        reader_log.sync(&reader).await.unwrap(),
        0,
        "nothing new to apply"
    );

    log.append(
        &writer,
        vec![delta(
            "host01",
            vec![file(2, "host01/b.parquet", 0, 10)],
            vec![],
        )],
    )
    .await
    .unwrap();
    assert_eq!(reader_log.sync(&reader).await.unwrap(), 1);
    assert_eq!(reader.file_count(), 2);
}

#[tokio::test]
async fn snapshot_lets_the_log_be_replayed_from_a_later_point() {
    let (log, store) = log();
    let index = FileIndex::new();

    for i in 0..5u64 {
        log.append(
            &index,
            vec![delta(
                "host01",
                vec![file(i, &format!("host01/{i}.parquet"), 0, 10)],
                vec![],
            )],
        )
        .await
        .unwrap();
    }
    log.append(
        &index,
        vec![delta("host01", vec![], vec!["host01/0.parquet"])],
    )
    .await
    .unwrap();

    log.write_snapshot(&index).await.unwrap();

    // Delete every log object: the snapshot alone must carry the full live set. This is the
    // property that makes compaction traffic affordable here and impossible in the catalog.
    let mut listing = store.list(Some(&object_store::path::Path::from(
        "mycluster/file-index/logs",
    )));
    let mut locations = Vec::new();
    {
        use futures::StreamExt;
        while let Some(item) = listing.next().await {
            locations.push(item.unwrap().location);
        }
    }
    assert_eq!(locations.len(), 6);
    for location in locations {
        store.delete(&location).await.unwrap();
    }

    let reloaded = FileIndexLog::new(Arc::clone(&store), "mycluster".into())
        .load()
        .await
        .unwrap();

    assert_eq!(
        reloaded.file_count(),
        4,
        "the removed file must not come back"
    );
    assert_eq!(paths(&reloaded), paths(&index));
    assert_eq!(reloaded.sequence(), index.sequence());
}

#[tokio::test]
async fn appending_nothing_does_not_burn_a_sequence() {
    let (log, _) = log();
    let index = FileIndex::new();

    let before = index.sequence();
    let after = log.append(&index, vec![]).await.unwrap();

    assert_eq!(before, after);
}

// ---- time watermark: the peer-buffer RPC skip ----

#[test]
fn persisted_max_time_tracks_the_highest_file_time_per_node() {
    let index = FileIndex::new();
    index.apply(&entry(
        1,
        vec![
            delta("host01", vec![file(1, "host01/a.parquet", 0, 100)], vec![]),
            delta("host02", vec![file(2, "host02/a.parquet", 0, 500)], vec![]),
        ],
    ));

    assert_eq!(index.persisted_max_time("host01"), Some(100));
    assert_eq!(index.persisted_max_time("host02"), Some(500));
}

#[test]
fn an_unseen_node_has_no_watermark() {
    // `None` means "cannot prove anything", not "empty". A caller that read absence as zero would
    // skip a peer it knows nothing about and silently drop its rows.
    let index = FileIndex::new();
    assert_eq!(index.persisted_max_time("never-seen"), None);
}

#[test]
fn removals_do_not_lower_the_watermark() {
    // Dropping a file does not un-persist the rows it held. Lowering the watermark would start
    // skipping peers whose buffers may hold newer data.
    let index = FileIndex::new();
    index.apply(&entry(
        1,
        vec![delta(
            "host01",
            vec![file(1, "host01/a.parquet", 0, 100)],
            vec![],
        )],
    ));
    index.apply(&entry(
        2,
        vec![delta("host01", vec![], vec!["host01/a.parquet"])],
    ));

    assert!(paths(&index).is_empty(), "file is gone");
    assert_eq!(
        index.persisted_max_time("host01"),
        Some(100),
        "but the watermark it established is not"
    );
}

#[test]
fn watermarks_survive_a_rollup_snapshot() {
    // A rollup can collapse away the very files the watermark was derived from, so recomputing it
    // from the surviving set would silently lower it.
    let index = FileIndex::new();
    index.apply(&entry(
        1,
        vec![delta(
            "host01",
            vec![file(1, "host01/a.parquet", 0, 100)],
            vec![],
        )],
    ));
    index.apply(&entry(
        2,
        vec![delta("host01", vec![], vec!["host01/a.parquet"])],
    ));

    let restored = FileIndex::new();
    restored.restore(&index.to_snapshot());

    assert_eq!(restored.persisted_max_time("host01"), Some(100));
    assert_eq!(
        restored.published_watermark("host01"),
        index.published_watermark("host01")
    );
}
