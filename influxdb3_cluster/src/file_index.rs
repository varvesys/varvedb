//! A cluster-wide index of every persisted Parquet file, replayed from a shared log.
//!
//! # Why this replaces per-peer polling
//!
//! [`PeerFiles`](crate::peer_files::PeerFiles) is built by polling each peer's own
//! `snapshots/` prefix, which gives every node a *separate* view assembled from *per-node*
//! sequence numbers. Three problems follow from that, and all three are structural:
//!
//! * **Staleness is undetectable.** A node has no way to know its view is behind, so it serves
//!   fewer rows and reports success.
//! * **Cross-node ordering is arbitrary.** `ChunkOrder` decides which row wins deduplication, but
//!   `ParquetFile` carries no cluster-wide ordering field — snapshot and WAL sequences are minted
//!   per node. Two nodes that wrote the same primary key produce a winner that is *stable* only
//!   because ties are broken by path.
//! * **Cold start is silent.** An empty index answers queries as though the cluster held no
//!   persisted data.
//!
//! One shared log fixes all three at once: a single monotonic sequence orders every file addition
//! cluster-wide, and the sequence a node has replayed to is a number it can compare against
//! anyone else's.
//!
//! # Node ids are opaque prefixes
//!
//! A node is identified here by the object-store prefix its files live under, never by "the first
//! path segment". Today that prefix is just the node id; nesting node data under the cluster
//! prefix later would make it `{cluster}/{node}`, and nothing in this module would need to change.

pub mod log;
pub mod publisher;

use std::collections::HashMap;
use std::sync::Arc;

use influxdb3_id::{DbId, TableId};
use influxdb3_wal::SnapshotSequenceNumber;
use influxdb3_write::{ChunkFilter, ParquetFile};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Position in the shared log.
///
/// Unlike `SnapshotSequenceNumber` this is **cluster-wide**: every node reads and appends to one
/// log, so comparing two of these is meaningful across nodes.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct FileIndexSequence(u64);

impl FileIndexSequence {
    pub const fn new(v: u64) -> Self {
        Self(v)
    }

    pub const fn as_u64(&self) -> u64 {
        self.0
    }

    pub const fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

impl std::fmt::Display for FileIndexSequence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// One node's change to one table: files it added, files it retired.
///
/// Removals travel as **paths**, not [`ParquetFileId`](influxdb3_id::ParquetFileId)s. Ids are
/// unique only within the allocator that minted them, while a path is unique cluster-wide because
/// the node prefix is its leading component. Matching on ids across nodes would let one node's
/// removal delete another node's file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileIndexDelta {
    /// Object-store prefix of the node that owns these files.
    pub node_id: Arc<str>,
    pub db_id: DbId,
    pub table_id: TableId,
    /// The owning node's snapshot that produced this delta.
    ///
    /// Recorded so a publisher restarting after a crash can resume from
    /// [`FileIndex::published_watermark`] instead of re-reading every manifest it ever wrote —
    /// and so republishing an already-published manifest is recognisable rather than silently
    /// duplicating its files at a second log position.
    pub snapshot_sequence: SnapshotSequenceNumber,
    #[serde(default)]
    pub added: Vec<ParquetFile>,
    #[serde(default)]
    pub removed: Vec<Arc<str>>,
}

/// One appended log object: a batch of deltas sharing a sequence.
///
/// Batched because a single snapshot commonly touches many tables, and one object per table would
/// multiply both the append cost and the replay cost for no benefit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileIndexLogEntry {
    pub sequence: FileIndexSequence,
    pub deltas: Vec<FileIndexDelta>,
}

/// A file plus the log position that introduced it.
///
/// The sequence is the whole point: it is the cluster-wide ordering key that `ParquetFile` lacks,
/// and it is what makes deduplication between two nodes' copies of the same primary key
/// *recency-correct* rather than merely deterministic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedFile {
    pub file: ParquetFile,
    pub added_at: FileIndexSequence,
}

type TableFiles = HashMap<TableId, Vec<IndexedFile>>;
type DbFiles = HashMap<DbId, TableFiles>;

#[derive(Debug, Default)]
struct Inner {
    /// Keyed by owning node's object-store prefix.
    nodes: HashMap<Arc<str>, DbFiles>,
    /// Highest log sequence replayed into this index.
    sequence: FileIndexSequence,
    /// Highest snapshot sequence published *per node*, from the deltas seen.
    ///
    /// Distinct from `sequence`, which counts positions in the shared log. This tracks how far
    /// each node's own manifests have been drained, which is what a restarting publisher needs.
    watermarks: HashMap<Arc<str>, SnapshotSequenceNumber>,
    /// Highest `max_time` each node has ever persisted, across every database and table.
    ///
    /// Deliberately coarse — one number per node, not per table — because it is used to *prove* a
    /// peer's buffer cannot match a query, and a coarser bound can only ever under-skip. Monotone:
    /// only `max` is applied, so a removal never lowers it. A node we have seen no files from is
    /// absent, and absent must never be read as zero.
    persisted_max_times: HashMap<Arc<str>, i64>,
}

/// The in-memory projection of the shared log.
#[derive(Debug, Default)]
pub struct FileIndex {
    inner: RwLock<Inner>,
}

impl FileIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// The log position this index has been replayed to.
    ///
    /// Comparing this against a position obtained from elsewhere — a peer's reported high-water,
    /// say — is how a node detects that it is behind instead of silently answering with less data.
    pub fn sequence(&self) -> FileIndexSequence {
        self.inner.read().sequence
    }

    /// Apply one log entry.
    ///
    /// Idempotent by construction: entries at or below the current position are ignored, additions
    /// dedup by path, and a removal naming an already-absent file is a no-op. Replaying the same
    /// log twice therefore produces the same index, which is what lets a node recover by simply
    /// re-reading from any earlier position.
    pub fn apply(&self, entry: &FileIndexLogEntry) {
        let mut inner = self.inner.write();
        if entry.sequence <= inner.sequence {
            return;
        }

        for delta in &entry.deltas {
            let table = inner
                .nodes
                .entry(Arc::clone(&delta.node_id))
                .or_default()
                .entry(delta.db_id)
                .or_default()
                .entry(delta.table_id)
                .or_default();

            // Removals first, then additions. A compaction names its inputs as removed and its
            // output as added; if the two ever share a path, the addition must win, because that
            // path is what the merged file now lives at.
            if !delta.removed.is_empty() {
                table.retain(|indexed| !delta.removed.contains(&indexed.file.path));
            }

            for file in &delta.added {
                // Dedup on **path**, never on the whole record. `ParquetFile` derives `PartialEq`
                // across every field including `id`, so a redelivered entry whose id was minted
                // again would compare unequal and be inserted a second time — the same file
                // counted twice, scanned twice, and charged twice against the query file limit.
                if table.iter().any(|indexed| indexed.file.path == file.path) {
                    continue;
                }
                table.push(IndexedFile {
                    file: file.clone(),
                    added_at: entry.sequence,
                });
            }

            let watermark = inner
                .watermarks
                .entry(Arc::clone(&delta.node_id))
                .or_default();
            *watermark = (*watermark).max(delta.snapshot_sequence);

            // Track the time watermark from the files themselves. Removals are ignored on
            // purpose: dropping a file does not un-persist the rows it held, and lowering the
            // watermark would start skipping peers whose buffers may hold newer rows.
            if let Some(highest) = delta.added.iter().map(|f| f.max_time).max() {
                inner
                    .persisted_max_times
                    .entry(Arc::clone(&delta.node_id))
                    .and_modify(|seen| *seen = (*seen).max(highest))
                    .or_insert(highest);
            }
        }

        inner.sequence = entry.sequence;
    }

    /// Highest timestamp a node has ever persisted, or `None` if we have seen nothing from it.
    ///
    /// Used to prove a peer's buffer cannot satisfy a query: a buffer holds only rows not yet
    /// persisted, so nothing in it predates this. `None` means *unprovable*, never *empty* — a
    /// caller must not skip a peer it knows nothing about.
    pub fn persisted_max_time(&self, node_id: &str) -> Option<i64> {
        self.inner.read().persisted_max_times.get(node_id).copied()
    }

    /// How far a node's own snapshot manifests have been drained into the log.
    ///
    /// A publisher resumes from here, so this is what keeps a manifest from being published twice
    /// across a restart.
    pub fn published_watermark(&self, node_id: &str) -> Option<SnapshotSequenceNumber> {
        self.inner.read().watermarks.get(node_id).copied()
    }

    /// Every file for a table, across all nodes, overlapping the filter's time bounds.
    ///
    /// Ordered by `added_at` **ascending**, so a caller assigning `ChunkOrder` positionally from
    /// an increasing counter gives the most recently published file the **highest** order — and
    /// higher order wins deduplication (`ChunkOrder`'s own doc: *"chunks with higher order
    /// overwrite data in chunks with lower order"*; the plan sorts ascending by order and the
    /// deduplicator keeps the last value).
    ///
    /// The direction is easy to get backwards, and being wrong is silent: results still look
    /// plausible, but every cross-node conflict resolves to the **oldest** copy. Read it as
    /// "last in the slice wins", not "first".
    ///
    /// Path breaks ties for determinism when two files share an append.
    pub fn get_files_filtered(
        &self,
        db_id: DbId,
        table_id: TableId,
        filter: &ChunkFilter<'_>,
    ) -> Vec<ParquetFile> {
        let inner = self.inner.read();
        let mut out: Vec<&IndexedFile> = inner
            .nodes
            .values()
            .filter_map(|dbs| dbs.get(&db_id))
            .filter_map(|tables| tables.get(&table_id))
            .flatten()
            .filter(|indexed| {
                filter.test_time_stamp_min_max(indexed.file.min_time, indexed.file.max_time)
            })
            .collect();

        out.sort_by(|a, b| {
            a.added_at
                .cmp(&b.added_at)
                .then_with(|| a.file.path.cmp(&b.file.path))
        });

        out.into_iter()
            .map(|indexed| indexed.file.clone())
            .collect()
    }

    /// Every `(db, table)` a node holds files for. Used by the compactor to enumerate candidates.
    pub fn tables_for_node(&self, node_id: &str) -> Vec<(DbId, TableId)> {
        let inner = self.inner.read();
        let Some(dbs) = inner.nodes.get(node_id) else {
            return Vec::new();
        };
        dbs.iter()
            .flat_map(|(db_id, tables)| tables.keys().map(move |table_id| (*db_id, *table_id)))
            .collect()
    }

    /// Every file one node holds for one table, unfiltered.
    pub fn files_for_table(
        &self,
        node_id: &str,
        db_id: DbId,
        table_id: TableId,
    ) -> Vec<ParquetFile> {
        let inner = self.inner.read();
        inner
            .nodes
            .get(node_id)
            .and_then(|dbs| dbs.get(&db_id))
            .and_then(|tables| tables.get(&table_id))
            .map(|files| files.iter().map(|i| i.file.clone()).collect())
            .unwrap_or_default()
    }

    /// Total files held, for logging and metrics.
    pub fn file_count(&self) -> usize {
        self.inner
            .read()
            .nodes
            .values()
            .flat_map(|dbs| dbs.values())
            .flat_map(|tables| tables.values())
            .map(|files| files.len())
            .sum()
    }

    /// Collapse the index into a rollup snapshot.
    ///
    /// Restoring this reproduces the index exactly, which is what allows the log behind it to be
    /// discarded — the piece the catalog's own record log cannot do, and the reason this log can
    /// absorb compaction traffic that the catalog could not.
    ///
    /// Each entry carries its **own** `added_at` rather than inheriting the snapshot's sequence.
    /// Flattening them to one position would silently destroy the cluster-wide ordering this whole
    /// mechanism exists to provide: a node restoring from the snapshot would order files by path
    /// while a node replaying the log ordered them by sequence, and the two would disagree about
    /// which row wins deduplication.
    pub fn to_snapshot(&self) -> FileIndexSnapshot {
        let inner = self.inner.read();
        let mut files = Vec::new();
        for (node_id, dbs) in &inner.nodes {
            for (db_id, tables) in dbs {
                for (table_id, indexed) in tables {
                    for i in indexed {
                        files.push(SnapshotEntry {
                            node_id: Arc::clone(node_id),
                            db_id: *db_id,
                            table_id: *table_id,
                            file: i.file.clone(),
                            added_at: i.added_at,
                        });
                    }
                }
            }
        }
        FileIndexSnapshot {
            sequence: inner.sequence,
            files,
            watermarks: inner
                .watermarks
                .iter()
                .map(|(node, seq)| (Arc::clone(node), *seq))
                .collect(),
            persisted_max_times: inner
                .persisted_max_times
                .iter()
                .map(|(node, t)| (Arc::clone(node), *t))
                .collect(),
        }
    }

    /// Fold another index into this one, preserving log positions and watermarks.
    ///
    /// Used to move the result of a fresh [`FileIndexLog::load`](log::FileIndexLog::load) into the
    /// long-lived index the query path already holds, without swapping the `Arc` out from under it.
    pub fn merge_from(&self, other: &FileIndex) {
        self.restore(&other.to_snapshot());
    }

    /// Replace this index's contents with a rollup snapshot.
    ///
    /// Unlike [`restore`](Self::restore), which merges, this **discards** what the index held
    /// first. That distinction is load-bearing: a reader recovering from a pruned log is holding
    /// files the snapshot no longer lists, and merging would keep them — leaving it serving paths
    /// that have since been deleted, which is the failure being recovered from.
    ///
    /// Only correct for a reader that is strictly *behind* the snapshot. A reader ahead of it
    /// would lose entries it had legitimately applied.
    pub fn reset_from(&self, snapshot: &FileIndexSnapshot) {
        {
            let mut inner = self.inner.write();
            inner.nodes.clear();
            inner.watermarks.clear();
            inner.persisted_max_times.clear();
            inner.sequence = FileIndexSequence::default();
        }
        self.restore(snapshot);
    }

    /// Rebuild from a rollup snapshot, preserving every file's original log position.
    ///
    /// Additive: entries already present are kept. Use [`reset_from`](Self::reset_from) when the
    /// index may hold state the snapshot has superseded.
    pub fn restore(&self, snapshot: &FileIndexSnapshot) {
        let mut inner = self.inner.write();
        for entry in &snapshot.files {
            let table = inner
                .nodes
                .entry(Arc::clone(&entry.node_id))
                .or_default()
                .entry(entry.db_id)
                .or_default()
                .entry(entry.table_id)
                .or_default();

            if table.iter().any(|i| i.file.path == entry.file.path) {
                continue;
            }
            table.push(IndexedFile {
                file: entry.file.clone(),
                added_at: entry.added_at,
            });
        }

        // Watermarks must survive the rollup. Without them a publisher that restarted after the
        // log behind a snapshot was discarded would see no watermark, resume from zero, and
        // republish every manifest it had ever written.
        for (node_id, seq) in &snapshot.watermarks {
            let watermark = inner.watermarks.entry(Arc::clone(node_id)).or_default();
            *watermark = (*watermark).max(*seq);
        }

        // Time watermarks must survive too. They are derived from files that a rollup may have
        // since collapsed away, so recomputing them from the surviving set would silently lower
        // them and start skipping peers that should be asked.
        for (node_id, t) in &snapshot.persisted_max_times {
            inner
                .persisted_max_times
                .entry(Arc::clone(node_id))
                .and_modify(|seen| *seen = (*seen).max(*t))
                .or_insert(*t);
        }

        inner.sequence = inner.sequence.max(snapshot.sequence);
    }
}

/// One file in a rollup snapshot, with the log position that introduced it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotEntry {
    pub node_id: Arc<str>,
    pub db_id: DbId,
    pub table_id: TableId,
    pub file: ParquetFile,
    pub added_at: FileIndexSequence,
}

/// The whole index at one log position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileIndexSnapshot {
    pub sequence: FileIndexSequence,
    pub files: Vec<SnapshotEntry>,
    /// Per-node publish watermarks, carried so they outlive the log they were derived from.
    #[serde(default)]
    pub watermarks: Vec<(Arc<str>, SnapshotSequenceNumber)>,
    /// Per-node time watermarks, carried for the same reason.
    #[serde(default)]
    pub persisted_max_times: Vec<(Arc<str>, i64)>,
}

#[cfg(test)]
mod tests;

impl FileIndex {
    /// Highest `ParquetFileId` this index holds for one node's table.
    ///
    /// Sent to that peer so it can answer with the rows from any file it has *above* this — the
    /// files it has persisted but which have not yet reached this reader through the log.
    ///
    /// Scoped per table on purpose. `ParquetFileId` is allocated from one counter per node, across
    /// every database and table, so a high id in a busy table would otherwise mask a lower one
    /// here and hide exactly the rows this is meant to recover.
    ///
    /// Reads the `id` **field** of each entry rather than relying on position: the stored order is
    /// insertion order, which concurrent persist jobs, compaction appends and restart rebuilds all
    /// disturb. The ids themselves stay monotonic regardless.
    pub fn max_file_id(
        &self,
        node_id: &str,
        db_id: DbId,
        table_id: TableId,
    ) -> Option<influxdb3_id::ParquetFileId> {
        let inner = self.inner.read();
        inner
            .nodes
            .get(node_id)?
            .get(&db_id)?
            .get(&table_id)?
            .iter()
            .map(|indexed| indexed.file.id)
            .max()
    }
}
