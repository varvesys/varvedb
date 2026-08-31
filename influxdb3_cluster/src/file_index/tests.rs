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
        .get_files_filtered(DB, TABLE, &ChunkFilter::default(), None)
        .into_iter()
        .map(|f| f.path.to_string())
        .collect()
}

/// A node running `--mode ingest,query` publishes its own Parquet into the shared index and also
/// reads those files locally from `PersistedFiles`. The query path must therefore exclude itself,
/// or it plans every one of its own files twice — which DataFusion rejects outright ("should not
/// be rescanning the same file"), failing every query over persisted data on a combined-mode node.
#[test]
fn excluded_node_is_dropped_from_the_result() {
    let index = FileIndex::default();
    index.apply(&entry(
        1,
        vec![
            delta("self01", vec![file(1, "self01/a.parquet", 0, 10)], vec![]),
            delta("peer02", vec![file(2, "peer02/b.parquet", 0, 10)], vec![]),
        ],
    ));

    let all = index.get_files_filtered(DB, TABLE, &ChunkFilter::default(), None);
    assert_eq!(all.len(), 2, "without an exclusion both nodes' files appear");

    let without_self = index.get_files_filtered(DB, TABLE, &ChunkFilter::default(), Some("self01"));
    assert_eq!(
        without_self
            .iter()
            .map(|f| f.path.to_string())
            .collect::<Vec<_>>(),
        vec!["peer02/b.parquet"],
        "the excluded node's files must not be planned a second time"
    );

    // Excluding a node that holds nothing is a no-op, not an error: a querier-only node has no
    // files of its own in the index and must still see every peer's.
    assert_eq!(
        index
            .get_files_filtered(DB, TABLE, &ChunkFilter::default(), Some("querier03"))
            .len(),
        2
    );
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
        .zip(index.get_files_filtered(DB, TABLE, &ChunkFilter::default(), None))
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
    let got = index.get_files_filtered(DB, TABLE, &filter, None);
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

    // The rollup prunes the entries it covers, so the snapshot is now the *only* record of these
    // files. That is the property making compaction traffic affordable here and impossible in the
    // catalog, which can never discard a record it has applied.
    assert_eq!(
        log_count(&store).await,
        0,
        "the rollup should have reclaimed every entry it covers"
    );

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

// ---- rollup pruning ----

/// Count log objects remaining under the logs prefix.
async fn log_count(store: &Arc<dyn ObjectStore>) -> usize {
    use futures::StreamExt;
    let mut listing = store.list(Some(&object_store::path::Path::from(
        "mycluster/file-index/logs",
    )));
    let mut n = 0;
    while let Some(item) = listing.next().await {
        item.unwrap();
        n += 1;
    }
    n
}

#[tokio::test]
async fn a_rollup_prunes_the_entries_it_covers() {
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
    assert_eq!(log_count(&store).await, 5);

    log.write_snapshot(&index).await.unwrap();

    assert_eq!(
        log_count(&store).await,
        0,
        "every entry the snapshot covers must be reclaimed"
    );
}

#[tokio::test]
async fn pruning_leaves_entries_the_snapshot_does_not_cover() {
    // The off-by-one that matters. An entry above the snapshot's sequence holds changes the
    // snapshot never saw; deleting it would lose them outright.
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

    // Snapshot at sequence 1, then append more.
    log.write_snapshot(&index).await.unwrap();
    assert_eq!(log_count(&store).await, 0);

    log.append(
        &index,
        vec![delta(
            "host01",
            vec![file(2, "host01/b.parquet", 0, 10)],
            vec![],
        )],
    )
    .await
    .unwrap();
    assert_eq!(log_count(&store).await, 1, "the newer entry must survive");

    // A fresh reader must still see both files: one from the snapshot, one from the tail.
    let reloaded = FileIndexLog::new(Arc::clone(&store), "mycluster".into())
        .load()
        .await
        .unwrap();
    assert_eq!(reloaded.file_count(), 2);
    assert_eq!(paths(&reloaded), paths(&index));
}

#[tokio::test]
async fn rollup_then_prune_still_replays_to_the_same_index() {
    let (log, store) = log();
    let index = FileIndex::new();

    for i in 0..4u64 {
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

    let reloaded = FileIndexLog::new(Arc::clone(&store), "mycluster".into())
        .load()
        .await
        .unwrap();

    assert_eq!(paths(&reloaded), paths(&index));
    assert_eq!(reloaded.sequence(), index.sequence());
    assert_eq!(
        reloaded.published_watermark("host01"),
        index.published_watermark("host01"),
        "watermarks must survive a rollup that discarded the log they came from"
    );
    assert_eq!(
        reloaded.persisted_max_time("host01"),
        index.persisted_max_time("host01")
    );
}

#[tokio::test]
async fn pruning_is_safe_to_repeat() {
    // Two nodes rolling up around the same sequence both try to delete the same entries. The
    // loser must treat NotFound as success, not as a failure worth retrying forever.
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

    log.write_snapshot(&index).await.unwrap();
    log.write_snapshot(&index).await.unwrap();

    assert_eq!(log_count(&store).await, 0);
    let reloaded = FileIndexLog::new(Arc::clone(&store), "mycluster".into())
        .load()
        .await
        .unwrap();
    assert_eq!(reloaded.file_count(), 1);
}

#[tokio::test]
async fn a_reader_pruned_past_recovers_from_the_snapshot() {
    // The bug the load test surfaced, and the one that makes pruning dangerous.
    //
    // A rollup deletes the entries it covers. A reader that had not caught up finds them gone,
    // and reading forward from its own position skips them entirely — every removal they carried
    // is lost, permanently, and the reader goes on serving files that were deleted. That is
    // exactly what happened: queries failed with NotFound on paths the compactor had reclaimed.
    let (log, store) = log();
    let writer = FileIndex::new();

    // A reader that saw only the first entry, then fell behind.
    log.append(
        &writer,
        vec![delta(
            "host01",
            vec![
                file(1, "host01/a.parquet", 0, 10),
                file(2, "host01/b.parquet", 0, 10),
            ],
            vec![],
        )],
    )
    .await
    .unwrap();

    let reader_log = FileIndexLog::new(Arc::clone(&store), "mycluster".into());
    let reader = reader_log.load().await.unwrap();
    assert_eq!(reader.file_count(), 2);
    let stalled_at = reader.sequence();

    // The cluster moves on: a compaction retires both files for a merged one.
    log.append(
        &writer,
        vec![delta(
            "host01",
            vec![file(3, "host01/merged.parquet", 0, 10)],
            vec!["host01/a.parquet", "host01/b.parquet"],
        )],
    )
    .await
    .unwrap();

    // A rollup prunes everything up to here — including the entry the reader never saw.
    log.write_snapshot(&writer).await.unwrap();
    assert_eq!(log_count(&store).await, 0);

    // Something new lands, so the reader has a reason to sync.
    log.append(
        &writer,
        vec![delta(
            "host01",
            vec![file(4, "host01/c.parquet", 0, 10)],
            vec![],
        )],
    )
    .await
    .unwrap();

    assert_eq!(reader.sequence(), stalled_at, "reader has not moved yet");
    reader_log.sync(&reader).await.unwrap();

    assert_eq!(
        paths(&reader),
        paths(&writer),
        "a reader pruned past must recover the removals it missed, not skip over them"
    );
    assert!(
        !paths(&reader).iter().any(|p| p.contains("a.parquet")),
        "the compacted-away file must be gone; serving it would be a NotFound at query time"
    );
}

#[tokio::test]
async fn a_caught_up_reader_does_not_reload_the_snapshot() {
    // Gap recovery must be the exception. A reader in step with the log should apply entries
    // forward and never pay for a snapshot fetch.
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
    assert_eq!(paths(&reader), paths(&writer));
}

// ---- reader watermark for the handoff gap ----

#[test]
fn max_file_id_reports_the_highest_id_for_that_table() {
    let index = FileIndex::new();
    index.apply(&entry(
        1,
        vec![delta(
            "host01",
            vec![
                file(7, "host01/a.parquet", 0, 10),
                file(19, "host01/b.parquet", 0, 10),
                file(11, "host01/c.parquet", 0, 10),
            ],
            vec![],
        )],
    ));

    assert_eq!(
        index.max_file_id("host01", DB, TABLE),
        Some(ParquetFileId::from(19)),
        "the max is taken from the id field, not from insertion order"
    );
}

#[test]
fn max_file_id_is_scoped_per_table() {
    // ParquetFileId comes from ONE counter per node, spanning every database and table. A high id
    // in a busy table must not mask a lower one here, or the peer would skip exactly the files
    // this is meant to recover.
    const OTHER_TABLE: TableId = TableId::new(1);

    let index = FileIndex::new();
    index.apply(&FileIndexLogEntry {
        sequence: FileIndexSequence::new(1),
        deltas: vec![
            FileIndexDelta {
                node_id: "host01".into(),
                db_id: DB,
                table_id: TABLE,
                snapshot_sequence: SnapshotSequenceNumber::new(1),
                added: vec![file(5, "host01/low.parquet", 0, 10)],
                removed: vec![],
            },
            FileIndexDelta {
                node_id: "host01".into(),
                db_id: DB,
                table_id: OTHER_TABLE,
                snapshot_sequence: SnapshotSequenceNumber::new(1),
                added: vec![file(900, "host01/high.parquet", 0, 10)],
                removed: vec![],
            },
        ],
    });

    assert_eq!(
        index.max_file_id("host01", DB, TABLE),
        Some(ParquetFileId::from(5)),
        "the busy table's id 900 must not raise this table's watermark"
    );
}

#[test]
fn max_file_id_is_none_for_an_unseen_peer_or_table() {
    // None means "no claim". The peer must read it as "send the buffer only" — reading it as zero
    // would ask it to ship every file it holds.
    let index = FileIndex::new();
    index.apply(&entry(
        1,
        vec![delta(
            "host01",
            vec![file(1, "host01/a.parquet", 0, 10)],
            vec![],
        )],
    ));

    assert_eq!(index.max_file_id("host02", DB, TABLE), None);
    assert_eq!(index.max_file_id("host01", DbId::new(99), TABLE), None);
    assert_eq!(index.max_file_id("host01", DB, TableId::new(99)), None);
}

#[test]
fn the_watermark_excludes_exactly_what_the_reader_already_has() {
    // The disjointness the design rests on: the reader plans files <= N from its own index, the
    // peer sends rows from files > N. Overlap would double-count; a gap would lose rows.
    let index = FileIndex::new();
    index.apply(&entry(
        1,
        vec![delta(
            "host01",
            vec![
                file(10, "host01/a.parquet", 0, 10),
                file(11, "host01/b.parquet", 0, 10),
            ],
            vec![],
        )],
    ));

    let watermark = index.max_file_id("host01", DB, TABLE).unwrap();

    // What the peer would hold, including two files not yet published to the log.
    let peer_files = [
        file(10, "host01/a.parquet", 0, 10),
        file(11, "host01/b.parquet", 0, 10),
        file(12, "host01/c.parquet", 0, 10),
        file(13, "host01/d.parquet", 0, 10),
    ];
    let would_send: Vec<&str> = peer_files
        .iter()
        .filter(|f| f.id > watermark)
        .map(|f| &*f.path)
        .collect();

    assert_eq!(
        would_send,
        vec!["host01/c.parquet", "host01/d.parquet"],
        "only the files above the reader's watermark"
    );
}

#[test]
fn no_watermark_means_the_peer_sends_everything_it_has() {
    // The first persist of a (peer, db, table) pair. The reader has no file for it, so it derives
    // no watermark AND plans no Parquet for that peer — nothing else in the query covers these
    // rows. The peer must send all of them, not none.
    let index = FileIndex::new();
    index.apply(&entry(
        1,
        vec![delta(
            "host01",
            vec![file(1, "host01/a.parquet", 0, 10)],
            vec![],
        )],
    ));

    // A different peer this reader has never seen a file from.
    let watermark = index.max_file_id("host02", DB, TABLE);
    assert_eq!(watermark, None);

    let peer_files = [
        file(3, "host02/x.parquet", 0, 10),
        file(4, "host02/y.parquet", 0, 10),
    ];
    let would_send: Vec<&str> = match watermark {
        Some(since) => peer_files
            .iter()
            .filter(|f| f.id > since)
            .map(|f| &*f.path)
            .collect(),
        None => peer_files.iter().map(|f| &*f.path).collect(),
    };

    assert_eq!(
        would_send,
        vec!["host02/x.parquet", "host02/y.parquet"],
        "with no watermark the peer sends its whole set for the table"
    );
}

#[test]
fn one_seen_file_switches_the_peer_to_sending_only_newer() {
    // As soon as the reader holds a single file for the pair, the watermark takes over and the
    // peer stops resending history. This is the transition out of the cold case above.
    let index = FileIndex::new();
    index.apply(&entry(
        1,
        vec![delta(
            "host02",
            vec![file(3, "host02/x.parquet", 0, 10)],
            vec![],
        )],
    ));

    let watermark = index.max_file_id("host02", DB, TABLE);
    assert_eq!(watermark, Some(ParquetFileId::from(3)));

    let peer_files = [
        file(3, "host02/x.parquet", 0, 10),
        file(4, "host02/y.parquet", 0, 10),
    ];
    let would_send: Vec<&str> = peer_files
        .iter()
        .filter(|f| f.id > watermark.unwrap())
        .map(|f| &*f.path)
        .collect();

    assert_eq!(
        would_send,
        vec!["host02/y.parquet"],
        "the already-seen file must not be resent; the reader plans it from its own index"
    );
}
