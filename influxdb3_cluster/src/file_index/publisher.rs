//! Publishes this node's persisted files into the shared [`FileIndexLog`].
//!
//! Runs on ingesting nodes only. Everything a peer needs to know about this node's Parquet reaches
//! them through here, which is what allows peer polling to go away.
//!
//! # The watch channel is a hint, not the data
//!
//! `Bufferer::watch_persisted_snapshots` is a [`tokio::sync::watch`], which keeps only the most
//! recent value. Two snapshots completing between two polls collapse into one and the earlier is
//! never observed. Translating the watch payload directly would therefore drop those files
//! permanently — nothing else indexes them once peer sync is gone.
//!
//! So the channel is used only to learn that *something* happened. The data comes from this node's
//! own snapshot manifests, read back through [`Persister::load_snapshots_after`] — the same call
//! `sync.rs` makes against peers, pointed at ourselves. Coalescing then costs nothing, and a
//! publisher that was down for an hour catches up through the identical path on restart.
//!
//! Two properties of the send site make this safe: the notification fires only *after*
//! `persist_snapshot()` has durably written the manifest, so anything we are told about is
//! readable; and nothing is sent when a snapshot had no persist jobs and no removals, so a wake-up
//! never implies there is work.
//!
//! # Not every manifest fires the watch
//!
//! The channel's only sender lives in `QueryableBuffer`'s snapshot task. A **compaction manifest**
//! is written by the peer-RPC handler calling `persist_snapshot` directly, so it never rings the
//! bell at all. A publisher that waited solely on the watch would not learn that a compaction had
//! replaced files until some *unrelated* snapshot happened to fire it — while the compactor, on
//! its own grace timer, deletes the inputs regardless. Queries then resolve paths that no longer
//! exist and fail outright.
//!
//! That is not hypothetical: it is what a load test produced, as `NotFound` on every query
//! touching the affected table. So the loop also ticks on an interval. The watch makes publishing
//! *prompt*; the tick makes it *reliable*, and reliability is what correctness depends on here.

use std::sync::Arc;

use influxdb3_shutdown::ShutdownToken;
use influxdb3_write::persister::Persister;
use influxdb3_write::{PersistedSnapshot, PersistedSnapshotVersion};
use observability_deps::tracing::{debug, error, info, warn};
use tokio::sync::watch;

use super::log::FileIndexLog;
use super::{FileIndex, FileIndexDelta};

/// How many manifests to drain in one pass.
///
/// Bounds the work of a single catch-up after a long outage, so the publisher makes visible
/// progress and stays responsive to shutdown instead of disappearing into one enormous batch.
const MAX_MANIFESTS_PER_PASS: usize = 256;

/// Spawn the publisher. Only meaningful on a node that ingests.
#[allow(clippy::too_many_arguments)]
pub fn spawn_file_index_publisher(
    node_id: Arc<str>,
    persister: Arc<Persister>,
    log: Arc<FileIndexLog>,
    index: Arc<FileIndex>,
    snapshot_rx: watch::Receiver<Option<PersistedSnapshotVersion>>,
    tick: std::time::Duration,
    time_provider: Arc<dyn iox_time::TimeProvider>,
    shutdown: ShutdownToken,
) {
    tokio::spawn(async move {
        run_publisher(
            node_id,
            persister,
            log,
            index,
            snapshot_rx,
            tick,
            time_provider,
            shutdown,
        )
        .await;
    });
}

#[allow(clippy::too_many_arguments)]
async fn run_publisher(
    node_id: Arc<str>,
    persister: Arc<Persister>,
    log: Arc<FileIndexLog>,
    index: Arc<FileIndex>,
    mut snapshot_rx: watch::Receiver<Option<PersistedSnapshotVersion>>,
    tick: std::time::Duration,
    time_provider: Arc<dyn iox_time::TimeProvider>,
    shutdown: ShutdownToken,
) {
    // Publish anything this node persisted while it was down, before waiting on the channel at
    // all. A restart otherwise leaves those manifests unpublished until the *next* snapshot
    // happens to fire the watch, which on an idle table could be never.
    if let Err(error) = publish_pending(&node_id, &persister, &log, &index).await {
        warn!(%error, "initial file index catch-up failed; will retry on next snapshot");
    }

    let mut next_tick = time_provider.now() + tick;
    loop {
        tokio::select! {
            // The safety net. Catches manifests no watch announced — compaction being the one
            // that matters — and costs a listing of this node's own snapshot prefix per tick.
            _ = time_provider.sleep_until(next_tick) => {
                if shutdown.is_cancelled() {
                    break;
                }
                if let Err(error) = publish_pending(&node_id, &persister, &log, &index).await {
                    warn!(%error, "scheduled file index publish failed; will retry");
                }
                next_tick = time_provider.now() + tick;
            }
            changed = snapshot_rx.changed() => {
                if changed.is_err() {
                    // The only sender lives on the queryable buffer, so this means the buffer is
                    // gone and there is nothing left to publish.
                    debug!("snapshot notification channel closed; file index publisher stopping");
                    break;
                }
                if shutdown.is_cancelled() {
                    break;
                }
                if let Err(error) = publish_pending(&node_id, &persister, &log, &index).await {
                    // Deliberately not fatal. The watermark is unchanged, so the next pass
                    // retries exactly the manifests that did not land.
                    error!(%error, "failed to publish to file index; will retry");
                }
                next_tick = time_provider.now() + tick;
            }
            _ = shutdown.wait_for_shutdown() => break,
        }
    }
}

/// Drain every manifest above the published watermark into the log.
async fn publish_pending(
    node_id: &Arc<str>,
    persister: &Persister,
    log: &FileIndexLog,
    index: &FileIndex,
) -> Result<(), super::log::FileIndexLogError> {
    // Re-read the log first: another process writing under this node id would be a
    // misconfiguration, but resuming from a stale in-memory watermark would republish files
    // rather than fail loudly, and duplicated files are harder to notice than a duplicate node.
    log.sync(index).await?;

    let watermark = index.published_watermark(node_id);
    let mut manifests = match watermark {
        Some(seq) => {
            persister
                .load_snapshots_after(seq, MAX_MANIFESTS_PER_PASS)
                .await
        }
        None => persister.load_snapshots(MAX_MANIFESTS_PER_PASS).await,
    }
    .map_err(|e| {
        super::log::FileIndexLogError::ObjectStore(object_store::Error::Generic {
            store: "file index publisher",
            source: Box::new(e),
        })
    })?;

    if manifests.is_empty() {
        return Ok(());
    }

    // Oldest first. A manifest's removals must be applied before a later manifest's additions,
    // and `load_snapshots*` returns newest-first because the paths are inverted-sequence encoded.
    manifests.sort_by_key(|m| snapshot_of(m).snapshot_sequence_number);

    let mut deltas = Vec::new();
    let mut highest = watermark.unwrap_or_default();
    for versioned in &manifests {
        let snapshot = snapshot_of(versioned);
        if Some(snapshot.snapshot_sequence_number) <= watermark {
            continue;
        }
        deltas.extend(deltas_from_snapshot(snapshot));
        highest = highest.max(snapshot.snapshot_sequence_number);
    }

    if deltas.is_empty() {
        return Ok(());
    }

    let sequence = log.append(index, deltas).await?;
    info!(
        %sequence,
        manifests = manifests.len(),
        watermark = highest.as_u64(),
        "published to file index"
    );
    Ok(())
}

fn snapshot_of(versioned: &PersistedSnapshotVersion) -> &PersistedSnapshot {
    match versioned {
        PersistedSnapshotVersion::V1(snapshot) => snapshot,
    }
}

/// Translate one manifest into per-table deltas.
///
/// A table appears once even when the manifest both adds and removes files for it, so the
/// index's "removals before additions" rule inside a single delta does the right thing for a
/// compaction that replaces its inputs in one step.
fn deltas_from_snapshot(snapshot: &PersistedSnapshot) -> Vec<FileIndexDelta> {
    use std::collections::HashMap;

    use influxdb3_id::{DbId, TableId};

    // Keyed by (db, table) so adds and removes for the same table merge into one delta.
    let mut by_table: HashMap<(DbId, TableId), FileIndexDelta> = HashMap::new();

    // The manifest's own `node_id` is the owning node's object-store prefix, which is exactly the
    // opaque key the index wants — never re-derived from a path.
    let blank = |db_id, table_id| FileIndexDelta {
        node_id: Arc::clone(&snapshot.node_id),
        db_id,
        table_id,
        snapshot_sequence: snapshot.snapshot_sequence_number,
        added: Vec::new(),
        removed: Vec::new(),
    };

    for (db_id, tables) in snapshot.removed_files.iter() {
        for (table_id, files) in tables.tables.iter() {
            by_table
                .entry((*db_id, *table_id))
                .or_insert_with(|| blank(*db_id, *table_id))
                .removed
                .extend(files.iter().map(|f| Arc::clone(&f.path)));
        }
    }

    for (db_id, tables) in snapshot.databases.iter() {
        for (table_id, files) in tables.tables.iter() {
            by_table
                .entry((*db_id, *table_id))
                .or_insert_with(|| blank(*db_id, *table_id))
                .added
                .extend(files.iter().cloned());
        }
    }

    by_table.into_values().collect()
}

#[cfg(test)]
mod tests;
