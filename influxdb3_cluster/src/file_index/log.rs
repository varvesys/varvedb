//! The shared, append-only log behind [`FileIndex`].
//!
//! ```text
//! {cluster_id}/file-index/snapshot.json          rolled-up index
//! {cluster_id}/file-index/logs/{seq:020}.json    appended deltas
//! ```
//!
//! Under the **cluster** prefix, not a node prefix: one log for the whole cluster is what makes
//! the sequence globally meaningful, and it is what lets peer polling go away.
//!
//! # Appends are compare-and-swap
//!
//! Each append creates `logs/{next_sequence}` with `PutMode::Create`. Losing that race is the
//! normal way a node discovers someone else appended: it replays what it missed and retries at a
//! higher sequence. That is the same shape `Catalog::update_committed` uses, and it is why the
//! sequence can be trusted as a total order rather than a per-node guess.
//!
//! Pass a store wrapped in [`SelfVerifyingCreateStore`] to get the stronger guarantee. Every
//! append is nonce-tagged, so a retry that collides with its *own* earlier success is recognised
//! as such instead of being mistaken for a competing writer — which would otherwise cause the same
//! delta to be appended twice at two sequences.
//!
//! [`SelfVerifyingCreateStore`]: object_store_utils::SelfVerifyingCreateStore
//! [`FileIndex`]: super::FileIndex

use std::sync::Arc;

use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, PutPayload};
use object_store_utils::{PutNonce, SelfVerifyingCreate};
use observability_deps::tracing::{debug, info, warn};

use super::{FileIndex, FileIndexDelta, FileIndexLogEntry, FileIndexSequence, FileIndexSnapshot};

/// How many collisions to absorb before giving up on one append.
///
/// A collision means real progress by someone else, not a transient fault, so the retry is
/// productive rather than a backoff. The bound only exists to turn a pathological livelock into an
/// error the caller can log.
const MAX_APPEND_ATTEMPTS: usize = 16;

/// Sequences between rollup snapshots.
///
/// Bounds replay work for a starting node: it reads one snapshot plus at most this many log
/// objects, no matter how long the cluster has been running.
pub const DEFAULT_SNAPSHOT_INTERVAL: u64 = 500;

#[derive(Debug, thiserror::Error)]
pub enum FileIndexLogError {
    #[error("object store: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("malformed file index object at {path}: {source}")]
    Malformed {
        path: String,
        #[source]
        source: serde_json::Error,
    },

    #[error("serializing file index: {0}")]
    Serialize(#[source] serde_json::Error),

    #[error("gave up appending after {0} sequence collisions")]
    TooManyCollisions(usize),
}

type Result<T, E = FileIndexLogError> = std::result::Result<T, E>;

/// Reads and writes the shared log.
#[derive(Debug)]
pub struct FileIndexLog {
    object_store: Arc<dyn ObjectStore>,
    /// Cluster prefix. Opaque: this type never parses structure out of it.
    prefix: Arc<str>,
    snapshot_interval: u64,
}

impl FileIndexLog {
    pub fn new(object_store: Arc<dyn ObjectStore>, cluster_id: Arc<str>) -> Self {
        Self {
            object_store,
            prefix: cluster_id,
            snapshot_interval: DEFAULT_SNAPSHOT_INTERVAL,
        }
    }

    pub fn with_snapshot_interval(mut self, interval: u64) -> Self {
        self.snapshot_interval = interval;
        self
    }

    fn snapshot_path(&self) -> ObjPath {
        ObjPath::from(format!("{}/file-index/snapshot.json", self.prefix))
    }

    fn logs_dir(&self) -> ObjPath {
        ObjPath::from(format!("{}/file-index/logs", self.prefix))
    }

    /// Zero-padded to 20 digits so lexicographic listing order equals numeric sequence order —
    /// the same convention the catalog log uses, and what makes `list_with_offset` usable as a
    /// "everything after sequence N" query.
    fn log_path(&self, sequence: FileIndexSequence) -> ObjPath {
        ObjPath::from(format!(
            "{}/file-index/logs/{:020}.json",
            self.prefix,
            sequence.as_u64()
        ))
    }

    /// Build an index from scratch: the rollup snapshot, then every log after it.
    pub async fn load(&self) -> Result<FileIndex> {
        let index = FileIndex::new();

        match self.object_store.get(&self.snapshot_path()).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                let snapshot: FileIndexSnapshot =
                    serde_json::from_slice(&bytes).map_err(|source| {
                        FileIndexLogError::Malformed {
                            path: self.snapshot_path().to_string(),
                            source,
                        }
                    })?;
                debug!(sequence = %snapshot.sequence, "loaded file index snapshot");
                index.restore(&snapshot);
            }
            Err(object_store::Error::NotFound { .. }) => {
                debug!("no file index snapshot yet; replaying from the start of the log");
            }
            Err(e) => return Err(e.into()),
        }

        let applied = self.sync(&index).await?;
        info!(
            sequence = %index.sequence(),
            logs_applied = applied,
            files = index.file_count(),
            "file index loaded"
        );
        Ok(index)
    }

    /// Apply every log entry after the index's current position. Returns how many were applied.
    ///
    /// This is the whole of "staying current" — there is no second mechanism, and no per-peer
    /// bookkeeping, because one sequence covers the cluster.
    pub async fn sync(&self, index: &FileIndex) -> Result<usize> {
        let mut entries = self.load_after(index.sequence()).await?;
        entries.sort_by_key(|e| e.sequence);
        let count = entries.len();
        for entry in entries {
            index.apply(&entry);
        }
        Ok(count)
    }

    /// Fetch every log object with a sequence strictly greater than `after`.
    async fn load_after(&self, after: FileIndexSequence) -> Result<Vec<FileIndexLogEntry>> {
        use futures::StreamExt;

        // `list_with_offset` is exclusive of the offset key, and the zero-padded names make that
        // exactly "sequences above `after`" without fetching and discarding earlier objects.
        let offset = self.log_path(after);
        let mut listing = self
            .object_store
            .list_with_offset(Some(&self.logs_dir()), &offset);

        let mut locations = Vec::new();
        while let Some(item) = listing.next().await {
            locations.push(item?.location);
        }

        let mut entries = Vec::with_capacity(locations.len());
        for location in locations {
            let bytes = self.object_store.get(&location).await?.bytes().await?;
            let entry: FileIndexLogEntry =
                serde_json::from_slice(&bytes).map_err(|source| FileIndexLogError::Malformed {
                    path: location.to_string(),
                    source,
                })?;
            entries.push(entry);
        }
        Ok(entries)
    }

    /// Append deltas at the next free sequence, replaying anything missed on collision.
    ///
    /// `index` is brought up to date as a side effect, so on return it reflects both the caller's
    /// own deltas and whatever else landed while they were racing for a slot.
    pub async fn append(
        &self,
        index: &FileIndex,
        deltas: Vec<FileIndexDelta>,
    ) -> Result<FileIndexSequence> {
        if deltas.is_empty() {
            return Ok(index.sequence());
        }

        for attempt in 0..MAX_APPEND_ATTEMPTS {
            // Catch up first: appending at a sequence below what already exists would create an
            // object nobody replays, silently losing the deltas.
            self.sync(index).await?;
            let sequence = index.sequence().next();

            let entry = FileIndexLogEntry {
                sequence,
                deltas: deltas.clone(),
            };
            let body = serde_json::to_vec(&entry).map_err(FileIndexLogError::Serialize)?;

            let nonce = PutNonce::generate();
            match self
                .object_store
                .put_opts(
                    &self.log_path(sequence),
                    PutPayload::from_bytes(body.into()),
                    SelfVerifyingCreate::with_nonce(nonce).put_options(),
                )
                .await
            {
                Ok(_) => {
                    index.apply(&entry);
                    debug!(%sequence, deltas = entry.deltas.len(), "appended to file index log");
                    self.maybe_snapshot(index).await;
                    return Ok(sequence);
                }
                Err(object_store::Error::AlreadyExists { .. }) => {
                    // Someone else took this slot. That is progress, not a fault — loop, replay
                    // their entry, and try the next sequence.
                    debug!(%sequence, attempt, "file index sequence taken; retrying");
                }
                Err(e) => return Err(e.into()),
            }
        }

        Err(FileIndexLogError::TooManyCollisions(MAX_APPEND_ATTEMPTS))
    }

    /// Write a rollup snapshot if enough sequences have passed since the last one.
    ///
    /// Best effort: a failure here costs a longer replay next time, never correctness, so it is
    /// logged rather than propagated into the caller's append.
    async fn maybe_snapshot(&self, index: &FileIndex) {
        if self.snapshot_interval == 0
            || !index
                .sequence()
                .as_u64()
                .is_multiple_of(self.snapshot_interval)
        {
            return;
        }
        if let Err(error) = self.write_snapshot(index).await {
            warn!(%error, "failed to write file index snapshot; replay will be longer");
        }
    }

    /// Collapse the current index into a single object.
    ///
    /// This is what the catalog's own record log cannot do — it retains every record ever applied
    /// and rewrites the full history into each snapshot, so it cannot absorb data-plane churn.
    /// Here a merge that adds one file and removes a hundred collapses to one file, and the log
    /// behind the snapshot becomes discardable.
    pub async fn write_snapshot(&self, index: &FileIndex) -> Result<()> {
        let snapshot = index.to_snapshot();
        let body = serde_json::to_vec(&snapshot).map_err(FileIndexLogError::Serialize)?;

        // Overwrite, not create: the snapshot is derived state. A concurrent writer produces the
        // same bytes for the same sequence, and a stale writer is corrected by the log objects
        // that still sit above whatever sequence it recorded.
        self.object_store
            .put(&self.snapshot_path(), PutPayload::from_bytes(body.into()))
            .await?;

        info!(
            sequence = %snapshot.sequence,
            files = snapshot.files.len(),
            "wrote file index snapshot"
        );
        Ok(())
    }
}

/// Keep an index current by polling the shared log.
///
/// Runs on every node, including ingesters: the publisher only syncs when *this* node snapshots,
/// which on a quiet node may be rarely, and a node still needs to see what its peers published.
///
/// This is the one poller the design keeps. It replaces per-peer polling with a single listing
/// scoped to one prefix, and — unlike peer sync — what it returns is a position that can be
/// compared against anyone else's.
pub fn spawn_file_index_sync(
    log: Arc<FileIndexLog>,
    index: Arc<FileIndex>,
    interval: std::time::Duration,
    time_provider: Arc<dyn iox_time::TimeProvider>,
    shutdown: influxdb3_shutdown::ShutdownToken,
) {
    tokio::spawn(async move {
        // Load once before entering the loop so a node does not spend its first interval
        // answering queries from an empty index — the cold-start hole peer sync leaves open.
        match log.load().await {
            Ok(loaded) => index.merge_from(&loaded),
            Err(error) => warn!(%error, "initial file index load failed; starting empty"),
        }

        let mut next = time_provider.now() + interval;
        loop {
            tokio::select! {
                _ = time_provider.sleep_until(next) => {
                    if shutdown.is_cancelled() {
                        break;
                    }
                    if let Err(error) = log.sync(&index).await {
                        warn!(%error, "file index sync failed; retrying next interval");
                    }
                    next = time_provider.now() + interval;
                }
                _ = shutdown.wait_for_shutdown() => break,
            }
        }
    });
}
