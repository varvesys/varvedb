//! Merging of small, cold Parquet files belonging to ingesting peers.
//!
//! # Why this exists
//!
//! Every node keeps an in-memory [`PersistedFiles`] listing every Parquet file it has persisted,
//! and `get_files_filtered` clones that whole per-table list on each query before filtering it. A
//! node that has been ingesting for months holds hundreds of thousands of entries, and
//! `--query-file-limit` defaults to 432. Merging ~100 cold files into one keeps both the resident
//! list and the per-query clone inside the range they were built for.
//!
//! # Why a dedicated role
//!
//! A `--mode compact` node accepts no writes and serves no queries — [`resolve_modes`] rejects
//! `compact` combined with anything else. Nothing refreshes a node's `PersistedFiles` from object
//! store while it runs, so a `--mode ingest,compact` node would need a second path keeping that list
//! coherent. Keeping those roles apart means the compactor only ever touches prefixes it does not
//! serve from, and the owner finds out through exactly one mechanism: the [`CompactionNotice`].
//!
//! `--mode all` is the deliberate exception. It compacts its **own** prefix in addition to any
//! ingesting peers', and applies each self-merge in-process through
//! [`crate::rpc::server::apply_compaction_locally`] — the very same function the RPC handler runs
//! for a remote owner — so there is still exactly one path that mutates `PersistedFiles`, not two.
//! The "exactly one compactor per cluster" invariant below is unchanged: an `all` node is that one
//! compactor.
//!
//! # What one compaction does
//!
//! 1. Pick cold, small files for one `(peer, db, table)` — see [`select_merges`].
//! 2. Merge them with the same `ReorgPlanner::compact_plan` the persist path already uses to sort
//!    and deduplicate, and write the result into the peer's prefix.
//! 3. Send the peer a [`CompactionNotice`] describing the merge, and wait for it to be recorded.
//! 4. Only then schedule the inputs for deletion, after `--compact-input-grace`.
//!
//! # The compactor writes bytes; the owner writes the record
//!
//! Step 3 is not a courtesy notification — it is where the compaction becomes durable. The owner
//! allocates the snapshot sequence and the `ParquetFileId` from its own allocators and persists the
//! manifest that every downstream view derives from.
//!
//! It has to be that way round, because **no node may allocate in another node's sequence space**.
//! A compactor that tried would either take a number the peer was about to use — and
//! `persist_snapshot` is an unconditional PUT, so one of the two manifests is destroyed — or escape
//! into a reserved band so high that it strands the single watermark `sync.rs` keeps per peer, after
//! which every file that peer persists is filtered out of every querier's view, permanently.
//!
//! Both were tried. Neither is recoverable from without the owner minting.
//!
//! # Exactly one compactor may run per cluster
//!
//! This is an **operational invariant, enforced nowhere.** [`resolve_modes`] takes no catalog and
//! so cannot check it; node registration does not look; `init::wrap_write_buffer` spawns
//! unconditionally. Two `--mode compact` nodes start cleanly, and so do two processes sharing one
//! `--node-id`, because the second reuses the first's `instance_id` from the catalog.
//!
//! Upstream assumes the same thing — `RemoveNodeOp` calls compactor mode "the single-writer
//! primary-lease holder" — but the lease it refers to lives in Enterprise and is not in this tree.
//!
//! ## Why it looks safe when it is not
//!
//! Two compactors that pick the *same* input set are nearly harmless, which is exactly what makes
//! this easy to get wrong. [`output_path_discriminator`] hashes the sorted input paths, so an
//! identical set yields an identical output path; the second [`CompactionNotice`] finds nothing to
//! add or remove and returns without writing a snapshot; a double delete swallows `NotFound`. A
//! short test with two compactors will therefore pass.
//!
//! ## What actually breaks
//!
//! They cannot be relied on to pick the same set. `cold_before` is wall-clock, so two loops
//! ticking at different instants disagree about which files are cold; and each reads its own
//! [`FileIndex`] replica, which lags the log by up to a sync interval. [`select_merges`] packs
//! greedily, so a single file's difference at the head shifts every later group boundary — the two
//! partitions diverge completely rather than locally.
//!
//! One compactor merges `{f1,f2,f3}` into `M_A`; the other merges `{f2,f3,f4}` into `M_B`. Both
//! notices do real work, both outputs stay live, and each holds the overlap. Query-time
//! deduplication keeps *results* correct, which is why this never surfaces as an error — it shows
//! up only as permanently duplicated storage, extra dedup work, and inflated counts against
//! `--query-file-limit`. Nothing detects or repairs it, and every subsequent pass can overlap
//! again.
//!
//! A smaller hazard rides along: the output PUT is unconditional, so concurrent identical merges
//! race on one object while the index keeps only the first notice's `size_bytes` — which is handed
//! to the reader as `ObjectMeta.size`. The writer properties are fixed, so the bytes are almost
//! certainly identical; nothing enforces or checks that they are.
//!
//! ## Replacing a dead compactor
//!
//! There is no heartbeat, TTL or lease anywhere in this tree, and the only `Running -> Stopped`
//! transition is the graceful-shutdown hook. A compactor killed abruptly stays `Running` forever.
//! It is also unremovable: `RemoveNodeOp`'s compactor check runs *before* its state check, so a
//! `Compact` node cannot be removed in **any** state, `Stopped` included.
//!
//! So replace a dead compactor by starting the new process under the **same `--node-id`** — which
//! works, because the reused `instance_id` satisfies re-registration. Do not expect to remove it
//! and re-add it under a new name.
//!
//! [`resolve_modes`]: crate::config
//! [`FileIndex`]: crate::file_index::FileIndex
//!
//! # Failure is cheap
//!
//! Until the owner acknowledges, nothing has been given up: the inputs are intact and still
//! referenced, and the merged file is an orphan the next pass overwrites — the output path is a hash
//! of the input paths, so a retry lands on the same object. Deletion happens only after the
//! acknowledgement proves the replacement is durable.
//!
//! [`PersistedFiles`]: influxdb3_write::write_buffer::persisted_files::PersistedFiles
//! [`CompactionNotice`]: crate::rpc::notice::CompactionNotice
//! [`resolve_modes`]: crate::config

use std::sync::Arc;
use std::time::Duration;

use influxdb3_catalog::catalog::{Catalog, NodeMode};
use influxdb3_id::{DbId, ParquetFileId, TableId};
use influxdb3_shutdown::ShutdownToken;
use influxdb3_wal::WalFileSequenceNumber;
use influxdb3_write::ParquetFile;
use influxdb3_write::paths::ParquetFilePath;
use influxdb3_write::persister::Persister;
use influxdb3_write::write_buffer::{WriteBufferImpl, parquet_chunk_from_file};
use iox_query::QueryChunk;
use iox_query::exec::Executor;
use iox_query::frontend::reorg::ReorgPlanner;
use iox_time::TimeProvider;
use observability_deps::tracing::{debug, error, info, warn};

use crate::config::CompactionConfig;
use crate::file_index::FileIndex;
use crate::rpc::client::PeerClients;

#[derive(Debug, thiserror::Error)]
pub enum CompactorError {
    #[error("planning the merge failed: {0}")]
    Plan(String),

    #[error("executing the merge failed: {0}")]
    Execute(#[from] datafusion::error::DataFusionError),

    #[error("writing the merged Parquet file failed: {0}")]
    Write(String),

    #[error("the owning node did not record the compaction: {0}")]
    Unreachable(String),

    #[error("recording the compaction on this node failed: {0}")]
    RecordLocal(String),
}

/// A merge that has been chosen but not yet performed.
#[derive(Debug, Clone)]
pub struct MergeGroup {
    pub db_id: DbId,
    pub table_id: TableId,
    pub inputs: Vec<ParquetFile>,
}

impl MergeGroup {
    fn total_size(&self) -> u64 {
        self.inputs.iter().map(|f| f.size_bytes).sum()
    }

    fn total_rows(&self) -> u64 {
        self.inputs.iter().map(|f| f.row_count).sum()
    }

    /// The chunk time the merged file is filed under: the earliest of its inputs.
    fn chunk_time(&self) -> i64 {
        self.inputs
            .iter()
            .map(|f| f.chunk_time)
            .min()
            .unwrap_or_default()
    }

    fn min_time(&self) -> i64 {
        self.inputs
            .iter()
            .map(|f| f.min_time)
            .min()
            .unwrap_or_default()
    }

    fn max_time(&self) -> i64 {
        self.inputs
            .iter()
            .map(|f| f.max_time)
            .max()
            .unwrap_or_default()
    }
}

/// Choose which files to merge for one table.
///
/// Two predicates decide eligibility, and both matter:
///
/// * **Cold.** A file is only considered once its `chunk_time` is older than `min_age`. Gen1 files
///   are immutable once written, but a `chunk_time` bucket keeps receiving *new* files until the
///   buffer advances past it. Waiting makes the input set genuinely closed, so a merge cannot race
///   the arrival of another file for the same bucket.
/// * **Small.** Files at or above `max_input_size_bytes` are already large enough to be worth
///   reading directly, and rewriting them would cost far more than it saves.
///
/// Eligible files are packed in `chunk_time` order so a merged file covers a contiguous span rather
/// than a scattered one, which keeps its min/max time tight and lets time-range filters skip it.
/// Groups of one are dropped: rewriting a single file changes nothing but its name.
pub fn select_merges(
    db_id: DbId,
    table_id: TableId,
    files: &[ParquetFile],
    cold_before: i64,
    config: &CompactionConfig,
) -> Vec<MergeGroup> {
    let mut eligible: Vec<ParquetFile> = files
        .iter()
        .filter(|f| f.chunk_time < cold_before && f.size_bytes < config.max_input_size_bytes)
        .cloned()
        .collect();

    if eligible.len() < 2 {
        return Vec::new();
    }

    eligible.sort_by_key(|f| (f.chunk_time, f.min_time));

    let mut groups = Vec::new();
    let mut current: Vec<ParquetFile> = Vec::new();
    let mut current_size = 0u64;

    for file in eligible {
        let would_exceed_size = current_size + file.size_bytes > config.target_size_bytes;
        let would_exceed_count = current.len() >= config.max_inputs;

        if !current.is_empty() && (would_exceed_size || would_exceed_count) {
            groups.push(std::mem::take(&mut current));
            current_size = 0;
        }

        current_size += file.size_bytes;
        current.push(file);
    }
    if !current.is_empty() {
        groups.push(current);
    }

    groups
        .into_iter()
        .filter(|inputs| inputs.len() >= 2)
        .map(|inputs| MergeGroup {
            db_id,
            table_id,
            inputs,
        })
        .collect()
}

/// Whether this peer's files may be rewritten by a remote compactor.
///
/// Any node that ingests qualifies, including one that also serves queries. What keeps a target's
/// inputs safe is not its role but the order the merge commits in:
///
/// * **The acknowledgement gates deletion.** [`compact_group`] calls `notify_owner(...).await?`, so
///   a notice that does not land returns early and the inputs are never scheduled for reclamation.
/// * **The acknowledgement implies durability.** The handler applies the swap and then persists a
///   snapshot, returning an error if that write fails — precisely so a compactor never deletes
///   inputs the owner could lose on restart.
/// * **`--compact-input-grace` covers the rest.** A query that resolved the old paths before the
///   swap keeps reading them for the grace period, which must exceed the longest query the cluster
///   runs. That hazard belongs to every node, not to query-serving ones.
///
/// An earlier version excluded query-serving peers, reasoning that a lost notice would leave such a
/// node naming inputs until the grace period deleted them under it. The first bullet above makes
/// that unreachable: no acknowledgement, no deletion. Notice loss is equally likely against an
/// ingest-only node in any case — only the imagined consequence differed.
///
/// A peer already carrying `Compact` is still skipped, so compactors never rewrite each other's
/// files. That says nothing about two compactors converging on a *third* node's prefix — see the
/// module header on why exactly one compactor may run.
///
/// A node running `--mode all` carries `NodeMode::All`, not `Ingest`, so it is never selected by
/// this predicate — its own files are compacted through a separate, explicit self path in
/// [`compact_once`] instead, never through here. That keeps this predicate answering only "may a
/// *remote* compactor rewrite this peer", which is what its every caller assumes.
fn is_compactable(modes: &[NodeMode]) -> bool {
    let ingests = modes.iter().any(|m| matches!(m, NodeMode::Ingest));
    let compacts = modes.iter().any(|m| matches!(m, NodeMode::Compact));
    ingests && !compacts
}

/// The prefixes one compaction pass will work on.
///
/// Every registered node other than this one whose files a remote compactor may rewrite
/// ([`is_compactable`]), plus this node itself when `compact_self` is set (`--mode all`). Self is
/// appended explicitly and last: `is_compactable` deliberately rejects `NodeMode::All` so no *other*
/// compactor touches an `all` node's files, but the node still compacts its own — and it does so
/// whether or not its own catalog registration has landed yet.
fn select_targets(
    nodes: &[(Arc<str>, Vec<NodeMode>)],
    self_id: &Arc<str>,
    compact_self: bool,
) -> Vec<Arc<str>> {
    let mut targets: Vec<Arc<str>> = nodes
        .iter()
        .filter(|(id, modes)| id != self_id && is_compactable(modes))
        .map(|(id, _)| Arc::clone(id))
        .collect();

    if compact_self {
        targets.push(Arc::clone(self_id));
    }

    targets
}

/// Everything the compaction loop needs.
pub struct CompactorArgs {
    pub node_id: Arc<str>,
    /// Cluster prefix, so the pending-deletion queue outlives any one compactor process.
    pub cluster_id: Arc<str>,
    pub config: CompactionConfig,
    pub catalog: Arc<Catalog>,
    /// The same index the queriers read, so the compactor can never merge a file set the readers
    /// do not believe in.
    pub file_index: Arc<FileIndex>,
    pub persister: Arc<Persister>,
    pub executor: Arc<Executor>,
    pub time_provider: Arc<dyn TimeProvider>,
    pub peer_clients: Arc<PeerClients>,
    /// This node's own write buffer, for applying a self-compaction in-process. Only used when
    /// `compact_self` is set.
    pub inner: Arc<WriteBufferImpl>,
    /// Whether this node also compacts its **own** Parquet, applying each merge in-process via
    /// [`crate::rpc::server::apply_compaction_locally`] rather than over RPC. Set for `--mode all`.
    pub compact_self: bool,
}

impl std::fmt::Debug for CompactorArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactorArgs")
            .field("node_id", &self.node_id)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// Spawn the compaction loop. Returns immediately.
pub fn spawn_compactor(args: CompactorArgs, shutdown: ShutdownToken) {
    let interval: Duration = args.config.interval.into();
    tokio::spawn(async move {
        info!(
            node_id = %args.node_id,
            interval_secs = interval.as_secs(),
            min_age_secs = Duration::from(args.config.min_age).as_secs(),
            target_size_bytes = args.config.target_size_bytes,
            max_inputs = args.config.max_inputs,
            "starting compactor"
        );

        let deleter = InputDeleter::new(
            Arc::clone(&args.persister),
            Arc::clone(&args.time_provider),
            args.config.input_grace.into(),
            Arc::clone(&args.cluster_id),
        );
        // Pick up what a previous compactor process owed. Without this the queue starts empty and
        // every file it had scheduled leaks, since the owner dropped them from its `PersistedFiles`
        // when it acknowledged the merge.
        deleter.load().await;

        loop {
            let next = args.time_provider.now() + interval;
            tokio::select! {
                _ = args.time_provider.sleep_until(next) => {
                    compact_once(&args, &deleter).await;
                    deleter.reclaim_due().await;
                }
                _ = shutdown.wait_for_shutdown() => {
                    info!("compactor shutting down");
                    break;
                }
            }
        }
    });
}

/// One full pass over every compactable target.
///
/// A target is normally a peer; a `--mode all` node also adds itself, and merges applied to its own
/// prefix go through [`compact_group`]'s in-process path rather than an RPC to the owner.
async fn compact_once(args: &CompactorArgs, deleter: &InputDeleter) {
    let cold_before = args.time_provider.now().timestamp_nanos()
        - Duration::from(args.config.min_age).as_nanos() as i64;

    let nodes: Vec<(Arc<str>, Vec<NodeMode>)> = args
        .catalog
        .list_nodes()
        .into_iter()
        .map(|n| (n.node_id(), n.modes().clone()))
        .collect();
    let targets = select_targets(&nodes, &args.node_id, args.compact_self);

    if targets.is_empty() {
        // Worth saying out loud: a compact-only node whose peers all serve queries has nothing it
        // may safely touch, and would otherwise look identical to one that is simply idle.
        // Warn rather than debug: a node started in `compact` mode with nothing to work on is
        // almost always a misconfiguration, and at default log levels a `debug!` here is
        // indistinguishable from a healthy idle compactor. A `--mode all` node always has at least
        // itself, so this only fires for a genuine compact-only misconfiguration.
        warn!("no compactable peers; no registered peer ingests without also compacting");
        return;
    }

    for target in targets {
        // Candidates come from whichever index the queries are served from, so the compactor can
        // never merge a file set the readers do not believe in.
        for (db_id, table_id) in args.file_index.tables_for_node(&target) {
            let files = args.file_index.files_for_table(&target, db_id, table_id);
            let groups = select_merges(db_id, table_id, &files, cold_before, &args.config);

            for group in groups {
                match compact_group(args, &target, &group).await {
                    Ok(Some(outcome)) => {
                        info!(
                            %target,
                            ?db_id,
                            ?table_id,
                            inputs = group.inputs.len(),
                            input_bytes = group.total_size(),
                            input_rows = group.total_rows(),
                            output_bytes = outcome.merged.size_bytes,
                            output_rows = outcome.merged.row_count,
                            "compacted files"
                        );
                        deleter.schedule(&target, &group.inputs).await;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        // One failed merge must not stall the others. The inputs are untouched, so
                        // the next pass simply retries them.
                        warn!(%target, ?db_id, ?table_id, %error, "compaction failed; inputs left in place");
                    }
                }
            }
        }
    }
}

struct CompactionOutcome {
    merged: ParquetFile,
}

/// Merge one group, publish it, and record it against the owner.
///
/// `target` is the prefix that owns the files — a peer, or this node itself for `--mode all`. The
/// merge is written under `{target}/` either way; only the final recording step differs (RPC to a
/// peer, in-process call for self).
async fn compact_group(
    args: &CompactorArgs,
    target: &Arc<str>,
    group: &MergeGroup,
) -> Result<Option<CompactionOutcome>, CompactorError> {
    let Some(db_schema) = args.catalog.db_schema_by_id(&group.db_id) else {
        debug!(?group.db_id, "database vanished from catalog; skipping");
        return Ok(None);
    };
    let Some(table_def) = db_schema.table_definition_by_id(&group.table_id) else {
        debug!(?group.table_id, "table vanished from catalog; skipping");
        return Ok(None);
    };

    // A Persister scoped to the target's prefix. Paths it builds land under `{target}/`, which is
    // what makes the merged file and its snapshot show up as that node's, exactly like the files
    // being replaced.
    let target_persister = Persister::new(
        args.persister.object_store(),
        Arc::clone(target),
        Arc::clone(&args.time_provider),
        None,
    );

    // Reuse the query path's chunk construction so the merge reads inputs exactly as a query would.
    let chunks: Vec<Arc<dyn QueryChunk>> = group
        .inputs
        .iter()
        .enumerate()
        .map(|(order, file)| {
            Arc::new(parquet_chunk_from_file(
                file,
                &table_def.schema,
                args.persister.object_store_url().clone(),
                args.persister.object_store(),
                order as i64,
            )) as Arc<dyn QueryChunk>
        })
        .collect();

    // The same planner the persist path uses to sort and deduplicate a buffer chunk. Feeding it
    // ParquetChunks instead is the whole of the merge.
    let logical_plan = ReorgPlanner::new()
        .compact_plan(
            data_types::TableId::new(0),
            Arc::clone(&table_def.table_name),
            &table_def.schema,
            chunks,
            table_def.sort_key.clone(),
        )
        .map_err(|e| CompactorError::Plan(e.to_string()))?;

    let ctx = args.executor.new_context();
    let physical_plan = ctx.create_physical_plan(&logical_plan).await?;
    let batches = ctx.collect(physical_plan).await?;
    let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

    if row_count == 0 {
        warn!(?group.db_id, ?group.table_id, "merge produced no rows; leaving inputs in place");
        return Ok(None);
    }

    let path = ParquetFilePath::new_with_chunk_ordinal(
        target,
        group.db_id.get(),
        group.table_id.get(),
        group.chunk_time(),
        // The WAL sequence component carries no meaning for a file no WAL produced; the ordinal
        // below is what makes the path unique.
        WalFileSequenceNumber::new(0),
        output_path_discriminator(group),
    );

    let stream = futures::stream::iter(batches.into_iter().map(Ok));
    let batch_stream = Box::pin(
        datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            table_def.schema.as_arrow(),
            stream,
        ),
    );

    let (size_bytes, _meta, _to_cache) = target_persister
        .persist_parquet_file(path.clone(), batch_stream)
        .await
        .map_err(|e| CompactorError::Write(e.to_string()))?;

    // Id `0` is a placeholder: the owner assigns the real one from its own allocator when it records
    // the merge. It is never written anywhere in this form.
    let merged = ParquetFile {
        id: ParquetFileId::from(0),
        path: path.to_string().into(),
        size_bytes,
        row_count,
        chunk_time: group.chunk_time(),
        min_time: group.min_time(),
        max_time: group.max_time(),
    };

    // The merge only counts as done once the owner has recorded it — that is what proves the
    // replacement is durable and makes deleting the inputs safe. For a peer that means an RPC; when
    // the target is this node (`--mode all`), the same recording runs in-process against our own
    // `PersistedFiles` and allocators — no RPC, no `conn_info` needed.
    if target.as_ref() == args.node_id.as_ref() {
        let notice = crate::rpc::notice::CompactionNotice::new(
            target.to_string(),
            group.db_id,
            group.table_id,
            &merged,
            group.inputs.iter().map(|f| f.path.to_string()).collect(),
        );
        crate::rpc::server::apply_compaction_locally(
            &args.inner,
            &args.catalog,
            &args.persister,
            &args.node_id,
            &notice,
        )
        .await
        .map_err(|e| CompactorError::RecordLocal(e.to_string()))?;
    } else {
        notify_owner(args, target, group, &merged).await?;
    }

    Ok(Some(CompactionOutcome { merged }))
}

/// Distinguish a compacted file's path from any gen1 file sharing its chunk time.
///
/// `chunk_ordinal` is the only free component of a Parquet path, and gen1 uses it solely as a count
/// of Arrow-varchar splits — small values. Setting the high bit puts compaction output somewhere
/// gen1 cannot reach.
///
/// Deriving it from the inputs rather than from a counter makes the output path a pure function of
/// what went into it, so a retry after a failed or unacknowledged merge rewrites the *same* object
/// instead of stranding another orphan next to it.
fn output_path_discriminator(group: &MergeGroup) -> u32 {
    use std::hash::{Hash, Hasher};

    let mut paths: Vec<&str> = group.inputs.iter().map(|f| &*f.path).collect();
    paths.sort_unstable();

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for path in paths {
        path.hash(&mut hasher);
    }
    0x8000_0000 | (hasher.finish() as u32 & 0x7FFF_FFFF)
}

/// Hand the merge to the node that owns the files, and wait for it to be recorded.
///
/// This is where the compaction becomes real. The compactor has written bytes; the owner allocates
/// the snapshot sequence and the `ParquetFileId` from its own allocators and persists the manifest
/// that every downstream view derives from — its `PersistedFiles`, its table index, its monthly
/// checkpoint, and every querier's `PeerFiles`.
///
/// The owner has to be the one to do it. Both allocators are per-node, and a compactor writing into
/// a peer's sequence space either collides with a number the peer is about to use — whose
/// unconditional PUT then destroys a real manifest — or escapes into a band so high it breaks the
/// single watermark `sync.rs` keeps per peer, silently hiding every file that peer writes afterwards.
///
/// Failure here is not fatal to anything: the inputs are untouched and still referenced, and the
/// merged file is simply an orphan until the next pass rewrites it at the same path. But it *does*
/// mean the caller must not delete the inputs, which is why this returns an error rather than
/// logging one.
async fn notify_owner(
    args: &CompactorArgs,
    peer: &Arc<str>,
    group: &MergeGroup,
    merged: &ParquetFile,
) -> Result<(), CompactorError> {
    let addr = args
        .catalog
        .list_nodes()
        .into_iter()
        .find(|n| n.node_id() == *peer)
        .and_then(|n| n.conn_info())
        .ok_or_else(|| {
            CompactorError::Unreachable(format!("peer {peer} publishes no connection address"))
        })?;

    let removed_paths: Vec<String> = group.inputs.iter().map(|f| f.path.to_string()).collect();
    let notice = crate::rpc::notice::CompactionNotice::new(
        peer.to_string(),
        group.db_id,
        group.table_id,
        merged,
        removed_paths,
    );

    crate::rpc::client::notify_compaction(&args.peer_clients, &addr, &notice)
        .await
        .map_err(|e| CompactorError::Unreachable(e.to_string()))
}

/// Deletes merged-away inputs once no query can still be reading them.
///
/// The snapshot removes an input from every index the moment it lands, but a query that resolved
/// the index just before that still holds the input's path and will read it. Deleting immediately —
/// which is what `purge_expired` does for expired data — would fail those reads on a path they had
/// every reason to trust. So deletion waits out `--compact-input-grace`.
#[derive(Debug)]
struct InputDeleter {
    persister: Arc<Persister>,
    time_provider: Arc<dyn TimeProvider>,
    grace: Duration,
    pending: parking_lot::Mutex<Vec<PendingDeletion>>,
    /// Cluster prefix for the queue's object-store path.
    cluster_id: Arc<str>,
}

/// One input file awaiting reclamation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PendingDeletion {
    /// Wall-clock nanos after which the file may be deleted.
    due_ns: i64,
    /// Object-store prefix of the node that owns the file.
    peer: Arc<str>,
    path: String,
}

impl InputDeleter {
    fn new(
        persister: Arc<Persister>,
        time_provider: Arc<dyn TimeProvider>,
        grace: Duration,
        cluster_id: Arc<str>,
    ) -> Self {
        Self {
            persister,
            time_provider,
            grace,
            pending: parking_lot::Mutex::new(Vec::new()),
            cluster_id,
        }
    }

    /// Where the queue lives.
    ///
    /// Under the **cluster** prefix rather than this compactor's own node prefix, so a replacement
    /// started under a different `--node-id` still finds what its predecessor owed.
    fn queue_path(&self) -> object_store::path::Path {
        object_store::path::Path::from(format!(
            "{}/compactor/pending-deletions.json",
            self.cluster_id
        ))
    }

    /// Read a previous process's queue, if there is one.
    ///
    /// Every failure here starts empty rather than guessing. That leaks the objects the lost
    /// entries named, which is the same outcome as before this queue was durable at all — and it
    /// stays strictly preferable to acting on a half-read list and deleting a file some query
    /// still needs.
    async fn load(&self) {
        let path = self.queue_path();
        let bytes = match self.persister.object_store().get(&path).await {
            Ok(result) => match result.bytes().await {
                Ok(bytes) => bytes,
                Err(error) => {
                    error!(%error, %path, "could not read pending-deletion queue; starting empty");
                    return;
                }
            },
            Err(object_store::Error::NotFound { .. }) => {
                debug!("no pending-deletion queue; starting empty");
                return;
            }
            Err(error) => {
                error!(%error, %path, "could not open pending-deletion queue; starting empty");
                return;
            }
        };

        match serde_json::from_slice::<Vec<PendingDeletion>>(&bytes) {
            Ok(loaded) => {
                // Entries already past due are deleted on the next pass. Safe: they were retired at
                // least a full grace period ago, so nothing references them any more.
                info!(
                    recovered = loaded.len(),
                    "recovered pending deletions from a previous compactor"
                );
                self.pending.lock().extend(loaded);
            }
            Err(error) => {
                error!(%error, %path, "pending-deletion queue is unreadable; starting empty");
            }
        }
    }

    /// Overwrite the queue.
    ///
    /// A plain PUT, not a compare-and-swap: exactly one compactor may run per cluster, and this is
    /// that compactor's own state. Two would clobber one another here — last writer wins over a set
    /// of deletions, which could both resurrect and drop entries — but that is already forbidden by
    /// the invariant in the module header.
    ///
    /// A failed write leaves the in-memory queue intact, so the next pass rewrites it. The cost of
    /// losing this object is a leak, never a premature delete.
    async fn save(&self) {
        let snapshot: Vec<PendingDeletion> = self.pending.lock().clone();
        let body = match serde_json::to_vec(&snapshot) {
            Ok(body) => body,
            Err(error) => {
                error!(%error, "could not serialise pending deletions");
                return;
            }
        };
        if let Err(error) = self
            .persister
            .object_store()
            .put(
                &self.queue_path(),
                object_store::PutPayload::from_bytes(body.into()),
            )
            .await
        {
            error!(%error, "could not persist pending deletions; they survive only in memory");
        }
    }

    async fn schedule(&self, peer: &Arc<str>, inputs: &[ParquetFile]) {
        let due_ns = self.time_provider.now().timestamp_nanos() + self.grace.as_nanos() as i64;
        {
            let mut pending = self.pending.lock();
            for file in inputs {
                pending.push(PendingDeletion {
                    due_ns,
                    peer: Arc::clone(peer),
                    path: file.path.to_string(),
                });
            }
        }
        self.save().await;
    }

    /// Delete everything whose grace period has elapsed.
    ///
    /// The queue is persisted, so a compactor that restarts still owes what it owed. It was
    /// in-memory once, and a restart inside the grace window then leaked every scheduled file —
    /// permanently, because the owner drops them from its `PersistedFiles` the moment it
    /// acknowledges the merge, so nothing else tracks them.
    ///
    /// Durability does not change the safety bias. Every failure path here and in [`Self::load`]
    /// prefers leaking an object to deleting one early: the leaked file is already absent from
    /// every index, so nothing will ever read it, whereas an early delete breaks a live query.
    async fn reclaim_due(&self) {
        let now = self.time_provider.now().timestamp_nanos();
        let due: Vec<(Arc<str>, String)> = {
            let mut pending = self.pending.lock();
            let (ready, waiting): (Vec<_>, Vec<_>) =
                pending.drain(..).partition(|entry| entry.due_ns <= now);
            *pending = waiting;
            ready
                .into_iter()
                .map(|entry| (entry.peer, entry.path))
                .collect()
        };

        if due.is_empty() {
            return;
        }

        let object_store = self.persister.object_store();
        let mut deleted = 0usize;
        for (peer, path) in due {
            let obj_path = object_store::path::Path::from(path.as_str());
            match object_store.delete(&obj_path).await {
                Ok(()) => deleted += 1,
                // Already gone — the owner's own retention may have reached it first.
                Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => {
                    error!(%peer, %path, %error, "failed to delete compacted-away input");
                }
            }
        }
        if deleted > 0 {
            debug!(deleted, "reclaimed compacted-away input files");
        }
        // Record the shorter queue, so a restart does not retry what is already gone.
        self.save().await;
    }
}

#[cfg(test)]
mod tests;
