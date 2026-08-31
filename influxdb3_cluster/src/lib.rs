//! Cluster support for InfluxDB 3 Core.
//!
//! When several nodes share a catalog (via `--cluster-id`) each still owns its own object-store
//! prefix for data: WAL, Parquet, snapshots and table indices all live under `{node_id}/`. A query
//! served by one node therefore sees only that node's rows, even though every node agrees on the
//! schema.
//!
//! This crate closes that gap **without modifying the core write path**. [`ClusterWriteBuffer`] is
//! a decorator over [`WriteBufferImpl`] that implements the existing [`WriteBuffer`] trait: every
//! method delegates to the inner buffer except [`ChunkContainer::get_table_chunks`], which unions
//! the local chunks with Parquet published by peers.
//!
//! The decorator shape is deliberate. Peers' files are held in a separate [`FileIndex`] rather than
//! merged into the node's own `PersistedFiles`, which keeps them out of every path that could
//! delete an object or advance the local `ParquetFileId` allocator.
//!
//! That index is replayed from a single cluster-shared log rather than polled per peer. One log
//! means one monotonic sequence across all nodes, which is what makes cross-node deduplication
//! resolve to the most recently published file instead of an arbitrary-but-stable winner — per-node
//! snapshot sequences are not comparable to each other. It also gives a reader a number it can
//! check against a peer's own claim, so being behind is detectable rather than silent. See
//! [`file_index`].

use std::sync::Arc;

use crate::peer_chunk::PeerBufferChunk;
use crate::rpc::client::PeerClients;
use crate::rpc::ticket::PeerChunkTicket;
use async_trait::async_trait;
use data_types::{PartitionHashId, PartitionKey};
use datafusion::catalog::Session;
use datafusion::error::DataFusionError;
use influxdb3_cache::distinct_cache::DistinctCacheProvider;
use influxdb3_cache::last_cache::LastCacheProvider;
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema, TableDefinition};
use influxdb3_id::{DbId, TableId};
use influxdb3_types::DatabaseName;
use influxdb3_types::write::Precision;
use influxdb3_wal::Wal;
use influxdb3_write::persister::Persister;
use influxdb3_write::write_buffer::{self, WriteBufferImpl, parquet_chunk_from_file};
use influxdb3_write::{
    BufferedWriteRequest, Bufferer, ChunkContainer, ChunkFilter, DistinctCacheManager,
    LastCacheManager, ParquetFile, PersistedSnapshotVersion, WriteBuffer,
};
use iox_query::QueryChunk;
use iox_time::Time;
use observability_deps::tracing::debug;

pub mod compactor;
pub mod config;
pub mod file_index;

pub mod init;
pub mod peer_chunk;
pub mod rpc;

pub use compactor::{CompactorArgs, spawn_compactor};
pub use config::{
    ClusterConfig, ClusterError, ClusterIdentity, CompactionConfig, DEFAULT_QUERY_FILE_LIMIT,
};
pub use file_index::FileIndex;
pub use init::{init_catalog, wrap_write_buffer};

/// A [`WriteBuffer`] that answers queries from this node's data **and** peers' persisted Parquet.
///
/// Writes, WAL, snapshotting, retention and the last/distinct caches are untouched — they delegate
/// straight through to the inner buffer and continue to operate on local data only.
#[derive(Debug)]
pub struct ClusterWriteBuffer {
    inner: Arc<WriteBufferImpl>,
    persister: Arc<Persister>,
    /// This node's id, so it can exclude itself when enumerating peers.
    node_id: Arc<str>,
    /// Cached gRPC channels, one per peer.
    peer_clients: Arc<PeerClients>,
    /// Maximum number of Parquet files a single query may scan, mirroring the inner buffer's own
    /// limit. Applied to the combined local + peer file set.
    query_file_limit: usize,
    /// Whether this node accepts writes. False for `--mode query`, where `write_lp` is refused.
    ingests: bool,
    /// Every peer's persisted Parquet, replayed from the shared log.
    file_index: Arc<FileIndex>,
    /// Detects that this node's index is behind a peer it is reading buffered rows from.
    coverage: Arc<crate::rpc::coverage::CoverageTracker>,
}

impl ClusterWriteBuffer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        inner: Arc<WriteBufferImpl>,
        persister: Arc<Persister>,
        node_id: Arc<str>,
        query_file_limit: usize,
        ingests: bool,
        file_index: Arc<FileIndex>,
    ) -> Self {
        Self {
            inner,
            persister,
            node_id,
            peer_clients: Arc::new(PeerClients::new()),
            query_file_limit,
            ingests,
            coverage: crate::rpc::coverage::CoverageTracker::new(Arc::clone(&file_index)),
            file_index,
        }
    }

    /// Times a query was served while this node was behind a peer's published files.
    ///
    /// Zero in steady state. Non-zero means results may have been short, which is precisely the
    /// condition that used to pass unnoticed.
    pub fn coverage_gaps(&self) -> u64 {
        self.coverage.behind_count()
    }

    /// Highest timestamp a peer has persisted, from whichever index is authoritative.
    ///
    /// `None` means we cannot prove anything about that peer, and the caller must therefore ask
    /// it. Only reachable via the authoritative index: mixing sources here could let a stale one
    /// authorise a skip the other would not.
    fn persisted_max_time(&self, peer_id: &str) -> Option<i64> {
        self.file_index.persisted_max_time(peer_id)
    }

    /// Build one lazy chunk per peer that could hold un-persisted rows for this table.
    ///
    /// Two skips are applied, and both are *provable* — a wrong skip means silently missing rows,
    /// so heuristics are not acceptable here:
    ///
    /// 1. **Role.** Only nodes running in an ingest role buffer writes at all.
    /// 2. **Time range.** A peer's buffer holds only rows it has not yet persisted, so nothing in it
    ///    predates that peer's highest persisted timestamp. If the query's upper bound is below
    ///    that watermark, the peer cannot match. A peer we have never seen a snapshot from is
    ///    *never* skipped, because we can prove nothing about it.
    fn peer_buffer_chunks(
        &self,
        db_schema: &Arc<DatabaseSchema>,
        table_def: &Arc<TableDefinition>,
        filter: &ChunkFilter<'_>,
    ) -> Vec<Arc<dyn QueryChunk>> {
        let mut chunks: Vec<Arc<dyn QueryChunk>> = Vec::new();

        for node in self.inner.catalog().list_nodes() {
            let peer_id = node.node_id();
            // With `--mode ingest` a peer registers `Ingest` and `is_ingest()` answers on its own;
            // a `--mode query` peer is correctly skipped here. The `Core` arm is the compatibility
            // path: `Core` is its own catalog variant satisfying neither predicate, yet such a node
            // buffers writes exactly like an ingester. Without it, every peer in a cluster running
            // the default mode is skipped and no buffered rows are ever fetched.
            let ingests = node.is_ingest()
                || node
                    .modes()
                    .iter()
                    .any(|m| matches!(m, influxdb3_catalog::catalog::NodeMode::Core));
            if peer_id == self.node_id || !ingests {
                continue;
            }
            let Some(addr) = node.conn_info() else {
                // A peer that has not published a connection address cannot be reached. Log rather
                // than fail: the rest of the cluster is still answerable.
                debug!(%peer_id, "peer has no conn_info; skipping its buffer");
                continue;
            };

            if let (Some(upper), Some(watermark)) = (
                filter.time_upper_bound_ns,
                self.persisted_max_time(&peer_id),
            ) && upper <= watermark
            {
                debug!(%peer_id, upper, watermark, "query predates peer watermark; skipping RPC");
                continue;
            }

            // Tell the peer what we already have, so it can send the rows from anything newer.
            // Those files exist on the peer but cannot have reached us yet: a file enters the
            // shared log only after its manifest is written, which is after the buffer chunk
            // covering it was dropped. Without this the rows are in neither source.
            let since_file_id =
                self.file_index
                    .max_file_id(&peer_id, db_schema.id, table_def.table_id);

            let ticket = PeerChunkTicket::new(
                db_schema.id,
                table_def.table_id,
                filter.time_lower_bound_ns,
                filter.time_upper_bound_ns,
                since_file_id,
            );
            // Peer buffer rows span whatever gen1 windows the peer holds, so they get their own
            // partition key rather than being attributed to one window.
            let partition_id = PartitionHashId::new(
                data_types::TableId::new(0),
                &PartitionKey::from(format!("peer-buffer-{peer_id}")),
            );
            chunks.push(Arc::new(PeerBufferChunk::new(
                addr,
                Arc::clone(&self.peer_clients),
                ticket,
                table_def.influx_schema().clone(),
                partition_id,
                Arc::clone(&peer_id),
                Arc::clone(&self.coverage),
            )));
        }

        chunks
    }
}

impl ChunkContainer for ClusterWriteBuffer {
    fn get_table_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter<'_>,
        projection: Option<&Vec<usize>>,
        ctx: &dyn Session,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        // Local chunks (buffer + this node's Parquet) come from the inner buffer unchanged, so
        // buffered rows keep their `i64::MAX` chunk order and continue to win deduplication.
        let mut chunks = self.inner.get_table_chunks(
            Arc::clone(&db_schema),
            Arc::clone(&table_def),
            filter,
            projection,
            ctx,
        )?;

        // Lazy peer-buffer chunks: no RPC happens here, only when DataFusion polls them.
        chunks.extend(self.peer_buffer_chunks(&db_schema, &table_def, filter));

        let peer_parquet =
            self.file_index
                .get_files_filtered(db_schema.id, table_def.table_id, filter);

        if peer_parquet.is_empty() {
            return Ok(chunks);
        }

        let total_files = chunks.len() + peer_parquet.len();
        if total_files > self.query_file_limit {
            return Err(DataFusionError::External(
                format!(
                    "Query would scan {total_files} Parquet files across the cluster, exceeding \
                     the file limit of {}. Use a narrower time range, or increase the limit with \
                     --query-file-limit.",
                    self.query_file_limit
                )
                .into(),
            ));
        }

        debug!(
            local_chunks = chunks.len(),
            peer_files = peer_parquet.len(),
            "cluster query chunks breakdown"
        );

        // Peer chunks are ordered after the local ones, continuing from `chunks.len()` so the
        // whole sequence stays strictly increasing above every local Parquet order.
        //
        // The counter ascends while the slice is ordered oldest-first, so the **last** element
        // takes the **highest** order — and higher order wins deduplication. Reversing either
        // side silently resolves every conflict to the oldest copy.
        for (chunk_order, parquet_file) in (chunks.len() as i64..).zip(peer_parquet.iter()) {
            chunks.push(Arc::new(parquet_chunk_from_file(
                parquet_file,
                &table_def.schema,
                self.persister.object_store_url().clone(),
                self.persister.object_store(),
                chunk_order,
            )));
        }

        Ok(chunks)
    }
}

#[async_trait]
impl Bufferer for ClusterWriteBuffer {
    async fn write_lp(
        &self,
        database: DatabaseName,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
        no_sync: bool,
    ) -> write_buffer::Result<BufferedWriteRequest> {
        // Every user write reaches the buffer through this trait method — all four HTTP write
        // endpoints funnel through `HttpApi::write_lp_inner`, and plugin writes arrive via
        // `InProcessWriteEndpoint`. Refusing here therefore covers them all without touching
        // core's write path.
        //
        // Accepting the write instead would be worse than merely surprising: this node would
        // buffer rows, snapshot them, and start owning Parquet that no operator expects it to own.
        if !self.ingests {
            return Err(write_buffer::Error::NodeIsQueryOnly);
        }

        self.inner
            .write_lp(
                database,
                lp,
                ingest_time,
                accept_partial,
                precision,
                no_sync,
            )
            .await
    }

    async fn write_internal_lp(
        &self,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
        no_sync: bool,
    ) -> write_buffer::Result<BufferedWriteRequest> {
        self.inner
            .write_internal_lp(lp, ingest_time, accept_partial, precision, no_sync)
            .await
    }

    fn catalog(&self) -> Arc<Catalog> {
        self.inner.catalog()
    }

    fn wal(&self) -> Arc<dyn Wal> {
        self.inner.wal()
    }

    /// Local files only.
    ///
    /// This backs `system.parquet_files`, which is intentionally a per-node view — mixing peers in
    /// would make it impossible to see which files this node actually owns.
    fn parquet_files_filtered(
        &self,
        db_id: DbId,
        table_id: TableId,
        filter: &ChunkFilter<'_>,
    ) -> Vec<ParquetFile> {
        self.inner.parquet_files_filtered(db_id, table_id, filter)
    }

    fn watch_persisted_snapshots(
        &self,
    ) -> tokio::sync::watch::Receiver<Option<PersistedSnapshotVersion>> {
        self.inner.watch_persisted_snapshots()
    }
}

impl DistinctCacheManager for ClusterWriteBuffer {
    fn distinct_cache_provider(&self) -> Arc<DistinctCacheProvider> {
        self.inner.distinct_cache_provider()
    }
}

impl LastCacheManager for ClusterWriteBuffer {
    fn last_cache_provider(&self) -> Arc<LastCacheProvider> {
        self.inner.last_cache_provider()
    }
}

impl WriteBuffer for ClusterWriteBuffer {}
