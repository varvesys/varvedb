//! A `QueryChunk` backed by a peer's in-memory buffer, fetched lazily over RPC.
//!
//! # Why this is lazy
//!
//! `ChunkContainer::get_table_chunks` is **synchronous**, but fetching from a peer is async. The
//! tempting bridge — `block_in_place` + `block_on` — would block a DataFusion worker thread for a
//! whole network round trip *during query planning*, before the execution permit is even taken.
//! There is no precedent for blocking in any query path in this codebase.
//!
//! No bridge is needed. `QueryChunkData::RecordBatches` carries a `SendableRecordBatchStream`:
//! constructing a stream is synchronous, and all I/O happens when it is **polled**, which
//! DataFusion does asynchronously at execution time.
//!
//! Two invariants this file must uphold:
//!
//! 1. **`data()` must not perform I/O.** DataFusion calls it once during planning purely to
//!    discriminate the `QueryChunkData` variant, and throws the result away
//!    (`iox_query::provider::physical::chunks_to_physical_nodes`). `stream::once(async { … })`
//!    creates the future without running it.
//! 2. **Everything else must be answerable synchronously** — schema, stats, ordering — because
//!    those are consulted while building the plan. Row counts are unknown before the fetch, so
//!    statistics are `new_unknown`.

use std::any::Any;
use std::sync::Arc;

use data_types::{ChunkId, ChunkOrder, PartitionHashId};
use datafusion::common::Statistics;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use iox_query::{QueryChunk, QueryChunkData};
use schema::Schema;
use schema::sort::SortKey;

use futures::TryStreamExt;

use crate::rpc::client::{PeerClients, fetch_peer_batches};
use crate::rpc::ticket::PeerChunkTicket;

/// Chunk ordering for peer buffer data.
///
/// The local buffer uses `i64::MAX` (`influxdb3_write`'s `QueryableBuffer`). Peer buffer data is
/// newer than any Parquet but should lose ties to this node's own buffer, so it sits one below.
pub const PEER_BUFFER_CHUNK_ORDER: i64 = i64::MAX - 1;

/// A peer's un-persisted rows for one table, fetched on first poll.
#[derive(Debug)]
pub struct PeerBufferChunk {
    peer_addr: Arc<str>,
    clients: Arc<PeerClients>,
    ticket: PeerChunkTicket,
    schema: Schema,
    stats: Arc<Statistics>,
    partition_id: PartitionHashId,
    id: ChunkId,
    chunk_order: ChunkOrder,
    /// Owning peer's id, for coverage reporting.
    peer_id: Arc<str>,
    /// Detects that this node is behind the peer it is reading from.
    coverage: Arc<crate::rpc::coverage::CoverageTracker>,
}

impl PeerBufferChunk {
    pub fn new(
        peer_addr: Arc<str>,
        clients: Arc<PeerClients>,
        ticket: PeerChunkTicket,
        schema: Schema,
        partition_id: PartitionHashId,
        peer_id: Arc<str>,
        coverage: Arc<crate::rpc::coverage::CoverageTracker>,
    ) -> Self {
        // Row counts and value ranges are unknown until the peer answers, and the plan is built
        // before that happens. Unknown statistics are correct here: they make the optimiser
        // conservative rather than wrong.
        let stats = Arc::new(Statistics::new_unknown(&schema.as_arrow()));
        Self {
            peer_addr,
            clients,
            ticket,
            schema,
            stats,
            partition_id,
            id: ChunkId::new(),
            chunk_order: ChunkOrder::new(PEER_BUFFER_CHUNK_ORDER),
            peer_id,
            coverage,
        }
    }
}

impl QueryChunk for PeerBufferChunk {
    fn stats(&self) -> Arc<Statistics> {
        Arc::clone(&self.stats)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn partition_id(&self) -> &PartitionHashId {
        &self.partition_id
    }

    fn sort_key(&self) -> Option<&SortKey> {
        // Peer buffer rows arrive unsorted, exactly like the local buffer's chunks.
        None
    }

    fn id(&self) -> ChunkId {
        self.id
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        // Raw buffered rows, not deduplicated — same as the local buffer.
        true
    }

    fn data(&self) -> QueryChunkData {
        let peer_addr = Arc::clone(&self.peer_addr);
        let clients = Arc::clone(&self.clients);
        let ticket = self.ticket;
        let arrow_schema = self.schema.as_arrow();
        let stream_schema = Arc::clone(&arrow_schema);

        // `stream::once` builds the future without polling it, so this call performs no I/O. The
        // RPC runs when DataFusion polls the stream during `RecordBatchesExec::execute`.
        let coverage = Arc::clone(&self.coverage);
        let peer_id = Arc::clone(&self.peer_id);

        let stream = futures::stream::once(async move {
            // Push the network call onto the IO runtime rather than the DataFusion pool. The
            // DedicatedExecutor registers that handle on every worker thread for exactly this.
            let fetched = executor::spawn_io({
                let peer_addr = Arc::clone(&peer_addr);
                async move { fetch_peer_batches(&clients, &peer_addr, ticket).await }
            })
            .await?;

            // The peer told us how far it has published. If we have replayed less than that, rows
            // it has already dropped from its buffer may live in files we do not know about — so
            // these batches, on their own, may be an incomplete answer. Say so rather than let the
            // query report success over a silently short result.
            coverage.observe(&peer_id, fetched.peer_published);

            Ok::<_, datafusion::error::DataFusionError>(futures::stream::iter(
                fetched.batches.into_iter().map(Ok),
            ))
        })
        .try_flatten();

        QueryChunkData::RecordBatches(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
    }

    fn chunk_type(&self) -> &str {
        "PeerBufferChunk"
    }

    fn order(&self) -> ChunkOrder {
        self.chunk_order
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests;
