//! The ingester side: serve this node's **un-persisted** rows to peers.
//!
//! Runs on its own port rather than the main gRPC port, because `UnifiedService` in
//! `influxdb3_server` accepts exactly one gRPC service and offers no extension point. Binding
//! separately keeps `influxdb3_server` unmodified.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_flight::encode::{DictionaryHandling, FlightDataEncoderBuilder};
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::TryStreamExt;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_id::ParquetFileId;
use influxdb3_shutdown::ShutdownToken;
use influxdb3_write::persister::Persister;
use influxdb3_write::write_buffer::WriteBufferImpl;
use influxdb3_write::{
    ChunkFilter, DatabaseTables, ParquetFile, PersistedSnapshot, PersistedSnapshotVersion,
};
use observability_deps::tracing::{debug, error, info, warn};
use tonic::{Request, Response, Status, Streaming};

use crate::rpc::notice::{COMPACTION_NOTICE_ACTION, CompactionNotice};
use crate::rpc::ticket::PeerChunkTicket;

type TonicStream<T> = Pin<Box<dyn futures::Stream<Item = Result<T, Status>> + Send + 'static>>;

/// Serves buffer-only chunks for peers, and applies compaction notices.
#[derive(Debug)]
pub struct PeerChunkService {
    write_buffer: Arc<WriteBufferImpl>,
    catalog: Arc<Catalog>,
    /// This node's id, used to reject a compaction notice naming someone else's prefix.
    node_id: Arc<str>,
    /// Scoped to this node's own prefix, for loading a snapshot a compactor wrote there.
    persister: Arc<Persister>,
    /// Number of `do_get` calls served. Used by tests to assert that queriers skip peers they can
    /// prove hold nothing relevant.
    requests: Arc<AtomicU64>,
    /// Reads this node's own Parquet when a reader asks for files it cannot know about.
    executor: Option<Arc<iox_query::exec::Executor>>,
    /// The shared index, used only to report how far *this* node has published.
    ///
    /// Reporting our own watermark is what lets a caller tell an empty buffer from rows that have
    /// moved to Parquet the caller has not heard about yet.
    file_index: Option<Arc<crate::file_index::FileIndex>>,
}

/// Why [`apply_compaction_locally`] refused or failed to record a merge.
///
/// Kept separate from `tonic::Status` so the in-process caller (`--mode all` compacting its own
/// files) does not depend on the gRPC layer. The RPC handler maps it back to a `Status`.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CompactionApplyError {
    #[error("compaction notice names prefix {got:?}, but this node is {expected:?}")]
    WrongNode { expected: String, got: String },

    #[error("persisting compaction snapshot: {0}")]
    Persist(String),
}

impl From<CompactionApplyError> for Status {
    fn from(err: CompactionApplyError) -> Self {
        match err {
            CompactionApplyError::WrongNode { .. } => Status::invalid_argument(err.to_string()),
            CompactionApplyError::Persist(_) => Status::internal(err.to_string()),
        }
    }
}

/// Record a compaction against `node_id`'s own Parquet: swap `PersistedFiles` in memory, then
/// persist one `PersistedSnapshot` so the swap survives a restart and reaches the table index and
/// every querier's `PeerFiles`.
///
/// This node — not whoever planned the merge — allocates every identifier. Both the snapshot
/// sequence and the `ParquetFileId` come from per-node allocators, so allocating them here is what
/// keeps the compaction inside the one namespace that may legitimately advance them. It is called
/// two ways: from the RPC handler when a remote compactor merged this node's files, and directly
/// by the in-process compactor of a `--mode all` node merging its own.
///
/// Returning `Ok` means the inputs are safe to delete after the grace period, so every failure
/// below propagates rather than being swallowed. An idempotent redelivery (nothing changed) also
/// returns `Ok`, without writing a second snapshot.
pub(crate) async fn apply_compaction_locally(
    write_buffer: &WriteBufferImpl,
    catalog: &Catalog,
    persister: &Persister,
    node_id: &str,
    notice: &CompactionNotice,
) -> Result<(), CompactionApplyError> {
    // A notice names the prefix it was written to. Acting on one that names someone else would let
    // any peer able to reach this port drive files out of this node's index.
    if notice.node_id.as_str() != node_id {
        return Err(CompactionApplyError::WrongNode {
            expected: node_id.to_string(),
            got: notice.node_id.clone(),
        });
    }

    let db_id = notice.db_id();
    let table_id = notice.table_id();

    // Mint from this node's allocator, then swap under one lock acquisition. `apply_compaction`
    // adds before it removes, so the rows are advertised by one file or the other at every instant.
    let merged = notice.merged_file(ParquetFileId::new());
    let persisted_files = write_buffer.persisted_files();
    let (added, removed_files) = persisted_files.apply_compaction(
        db_id,
        table_id,
        std::slice::from_ref(&merged),
        &notice.removed_paths,
    );

    // Nothing changed, so this is a redelivery — or a second compactor arriving late with a merge
    // whose inputs are already gone. Either way the durable record exists, and writing another
    // would burn a sequence number to say nothing.
    if added == 0 && removed_files.is_empty() {
        debug!(
            %notice.merged_path,
            "compaction notice already applied; no snapshot written"
        );
        return Ok(());
    }

    // Reserving is not the same as reading the last sequence and adding one: that reserves nothing,
    // and the WAL flush path would hand the same number to a real snapshot whose unconditional PUT
    // then overwrites whichever manifest lost the race.
    let seq = write_buffer.wal().reserve_snapshot_sequence_number().await;
    // A compaction persists no buffered rows — it only swaps already-persisted Parquet — so this
    // snapshot must carry the WAL watermark a *real* snapshot last established, not the newest WAL
    // file that exists. `last_wal_sequence_number()` is the latter: stamping it here makes the next
    // restart treat every WAL file up to "now" as snapshotted and skip the un-persisted ones,
    // silently dropping their writes.
    let wal_seq = write_buffer.wal().last_persisted_wal_sequence_number().await;

    let mut snapshot =
        PersistedSnapshot::new(node_id.to_string(), seq, wal_seq, catalog.sequence_number());

    // Set the aggregates by hand: `add_parquet_file` is private, and it also re-reads
    // `ParquetFileId::next_id()`, which would be redundant here since the id is already minted.
    snapshot.next_file_id = ParquetFileId::next_id();
    snapshot.parquet_size_bytes = merged.size_bytes;
    snapshot.row_count = merged.row_count;
    snapshot.min_time = merged.min_time;
    snapshot.max_time = merged.max_time;

    let mut added_tables = DatabaseTables::default();
    added_tables.tables.insert(table_id, vec![merged.clone()]);
    snapshot.databases.insert(db_id, added_tables);

    // These carry this node's own ids, which matters because the two downstream consumers match
    // differently: the table index prunes by `ParquetFileId`, while `PeerFiles` removes by path.
    let removed_count = removed_files.len();
    if removed_count > 0 {
        let mut removed_tables = DatabaseTables::default();
        removed_tables.tables.insert(table_id, removed_files);
        snapshot.removed_files.insert(db_id, removed_tables);
    }

    persister
        .persist_snapshot(&PersistedSnapshotVersion::V1(snapshot))
        .await
        .map_err(|e| {
            // The in-memory swap already happened. Report the failure so the compactor does not
            // delete the inputs; this node recovers its old view on restart.
            CompactionApplyError::Persist(e.to_string())
        })?;

    info!(
        sequence = seq.as_u64(),
        added,
        removed = removed_count,
        merged_path = %notice.merged_path,
        "recorded compaction"
    );
    Ok(())
}

impl PeerChunkService {
    pub fn new(
        write_buffer: Arc<WriteBufferImpl>,
        catalog: Arc<Catalog>,
        node_id: Arc<str>,
        persister: Arc<Persister>,
    ) -> Self {
        Self {
            write_buffer,
            catalog,
            node_id,
            persister,
            requests: Arc::new(AtomicU64::new(0)),
            executor: None,
            file_index: None,
        }
    }

    /// Attach an executor so this node can materialise its own Parquet for a lagging reader.
    ///
    /// Without one, the peer answers with buffered rows only — correct, but it cannot close the
    /// handoff window.
    pub fn with_executor(mut self, executor: Arc<iox_query::exec::Executor>) -> Self {
        self.executor = Some(executor);
        self
    }

    /// Attach the shared index so this node can report its own publish watermark.
    pub fn with_file_index(mut self, index: Option<Arc<crate::file_index::FileIndex>>) -> Self {
        self.file_index = index;
        self
    }

    /// How far this node's own manifests have reached the shared log.
    ///
    /// `None` means no claim — either nothing published yet, or no index wired up. A caller must
    /// read that as "unknown", never as "caught up at zero".
    fn published_watermark(&self) -> Option<influxdb3_wal::SnapshotSequenceNumber> {
        self.file_index.as_ref()?.published_watermark(&self.node_id)
    }

    /// Record a compaction a compactor performed on this node's Parquet.
    ///
    /// Thin wrapper over [`apply_compaction_locally`], which holds the actual logic so a `--mode all`
    /// node can call it in-process without going through gRPC. Returning `Ok` tells the compactor
    /// the inputs are safe to delete after its grace period.
    async fn apply_compaction_notice(&self, notice: &CompactionNotice) -> Result<(), Status> {
        apply_compaction_locally(
            &self.write_buffer,
            &self.catalog,
            &self.persister,
            &self.node_id,
            notice,
        )
        .await
        .map_err(Status::from)
    }

    /// Counter of served requests, for tests and metrics.
    pub fn request_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.requests)
    }

    /// Collect the local buffer's record batches for one table.
    ///
    /// Deliberately reads only `WriteBufferImpl::buffer()`, never `get_table_chunks` — peers index
    /// each other's Parquet separately, so returning persisted data here would double-count.
    fn buffer_batches(&self, ticket: &PeerChunkTicket) -> Result<BufferAnswer, Status> {
        let db_id = ticket.db_id();
        let table_id = ticket.table_id();

        let db_schema = self
            .catalog
            .db_schema_by_id(&db_id)
            .ok_or_else(|| Status::not_found(format!("unknown database id {}", ticket.db_id)))?;
        let table_def = db_schema
            .table_definition_by_id(&table_id)
            .ok_or_else(|| Status::not_found(format!("unknown table id {}", ticket.table_id)))?;

        let mut filter = ChunkFilter::default();
        filter.time_lower_bound_ns = ticket.time_lower_bound_ns;
        filter.time_upper_bound_ns = ticket.time_upper_bound_ns;

        let arrow_schema = table_def.schema.as_arrow();

        // The buffer-only accessor is `pub(crate)` to `influxdb3_write`, so this goes through the
        // combined one and drops the Parquet half. Discarding it is not a shortcut — it is the
        // required behaviour: peers index each other's Parquet from published snapshots, so
        // returning persisted files here would double-count every one of them.
        //
        // Reading both under the single buffer guard this method holds is also the safer shape.
        // Its own doc notes that a persist job swaps chunks and files under one write guard, so
        // reading them separately can return the snapshotted rows twice.
        //
        // Both halves are used now. The buffer answers for rows not yet persisted; `persisted_files`
        // answers for the narrow set the reader cannot know about yet. Reading them under the same
        // guard is what makes the pair consistent — a persist job moving a chunk to a file between
        // the two reads would otherwise be counted twice or not at all.
        let (chunks, persisted_files) = self
            .write_buffer
            .buffer()
            .get_table_chunks_and_parquet_files(
                Arc::clone(&db_schema),
                Arc::clone(&table_def),
                &filter,
                None,
                // The buffer path ignores the session; it is only used for tracing in the parquet
                // path. There is no session on the serving side.
                &NoopSession,
            )
            .map_err(|e| Status::internal(format!("failed to read local buffer: {e}")))?;

        // Files this reader cannot have heard about: it learns of our files through the shared log,
        // and a file reaches the log only after its manifest is written — which happens after the
        // buffer chunk covering it was already dropped. In that window the rows are in neither the
        // reader's index nor our buffer, so nobody would return them.
        //
        // Filtering on the `id` field, never on position: the stored order is insertion order,
        // which concurrent persist jobs, compaction appends and restart rebuilds all disturb. The
        // ids stay monotonic regardless.
        let unseen: Vec<ParquetFile> = match ticket.since_file_id() {
            Some(since) => persisted_files
                .into_iter()
                .filter(|f| f.id > since)
                .collect(),

            // No watermark means the reader holds no file of ours for this table, so it also
            // planned no Parquet for us — nothing else in the query will cover these rows. Send
            // them all rather than leave them in no source at all. This is the first persist of a
            // (peer, db, table) pair, where there is no earlier file to derive a watermark from.
            //
            // Note what this does to ordering. Everything returned here reaches the reader as
            // `RecordBatchesExec` output carrying `PEER_BUFFER_CHUNK_ORDER`, which outranks every
            // Parquet chunk. For the intended case — a few files persisted moments ago — that is
            // right: those rows were in this buffer until just now and would have carried that
            // order anyway.
            //
            // A cold-started reader also arrives with no watermark, and then this returns our whole
            // history for the table at that same order, so our older rows can beat another node's
            // newer Parquet on a primary-key conflict. That only bites where one series is written
            // to several nodes, and it ends as soon as the reader's first sync gives it a real
            // watermark.
            None => persisted_files,
        };

        // Buffer chunks are always in-memory record batches.
        let mut batches = Vec::new();
        for chunk in chunks {
            match chunk.data() {
                iox_query::QueryChunkData::RecordBatches(stream) => {
                    let collected: Vec<_> =
                        futures::executor::block_on(stream.try_collect::<Vec<_>>()).map_err(
                            |e| Status::internal(format!("failed to collect buffer batch: {e}")),
                        )?;
                    batches.extend(collected);
                }
                iox_query::QueryChunkData::Parquet(_) => {
                    // Unreachable: QueryableBuffer only ever produces in-memory chunks. Skip rather
                    // than panic, so a future change upstream degrades instead of crashing a node.
                    warn!("unexpected parquet chunk from the in-memory buffer; skipping");
                }
            }
        }

        Ok(BufferAnswer {
            schema: arrow_schema,
            batches,
            unseen,
            table_def,
        })
    }

    /// Materialise this node's own Parquet files into record batches.
    ///
    /// Only ever called for the handful of files a reader cannot yet know about, so the common
    /// case is an empty list and no work at all. Mirrors the compactor's read path
    /// (`compactor.rs:411-441`): build chunks with the query path's own constructor, then let
    /// `ReorgPlanner` sort and deduplicate them.
    async fn parquet_batches(
        &self,
        table_def: &Arc<influxdb3_catalog::catalog::TableDefinition>,
        files: &[ParquetFile],
    ) -> Result<Vec<arrow::array::RecordBatch>, Status> {
        if files.is_empty() {
            return Ok(Vec::new());
        }
        let Some(executor) = self.executor.as_ref() else {
            // Without an executor this node cannot read its own Parquet. The buffer half is still
            // correct, so degrade rather than fail the reader's query.
            warn!(
                files = files.len(),
                "no executor; cannot serve files the reader has not seen"
            );
            return Ok(Vec::new());
        };

        let chunks: Vec<Arc<dyn iox_query::QueryChunk>> = files
            .iter()
            .enumerate()
            .map(|(order, file)| {
                Arc::new(influxdb3_write::write_buffer::parquet_chunk_from_file(
                    file,
                    &table_def.schema,
                    self.persister.object_store_url().clone(),
                    self.persister.object_store(),
                    order as i64,
                )) as Arc<dyn iox_query::QueryChunk>
            })
            .collect();

        let logical_plan = iox_query::frontend::reorg::ReorgPlanner::new()
            .compact_plan(
                data_types::TableId::new(0),
                Arc::clone(&table_def.table_name),
                &table_def.schema,
                chunks,
                table_def.sort_key.clone(),
            )
            .map_err(|e| Status::internal(format!("failed to plan unseen-file read: {e}")))?;

        let ctx = executor.new_context();
        let physical_plan = ctx
            .create_physical_plan(&logical_plan)
            .await
            .map_err(|e| Status::internal(format!("failed to plan unseen-file read: {e}")))?;
        ctx.collect(physical_plan)
            .await
            .map_err(|e| Status::internal(format!("failed to read unseen files: {e}")))
    }
}

/// What a peer has for one table: its buffered rows, plus the files the asking reader cannot yet
/// know about.
struct BufferAnswer {
    schema: arrow::datatypes::SchemaRef,
    batches: Vec<arrow::array::RecordBatch>,
    /// Files with an id above the reader's watermark. Empty whenever it is caught up.
    unseen: Vec<ParquetFile>,
    table_def: Arc<influxdb3_catalog::catalog::TableDefinition>,
}

/// The buffer's `get_table_chunks` takes a `&dyn Session` but only uses it for tracing spans in the
/// parquet path, which this call never reaches.
#[derive(Debug)]
struct NoopSession;

impl datafusion::catalog::Session for NoopSession {
    fn session_id(&self) -> &str {
        "influxdb3-cluster-peer"
    }
    fn config(&self) -> &datafusion::execution::config::SessionConfig {
        unimplemented!("peer chunk service does not plan queries")
    }
    fn create_physical_plan<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _logical_plan: &'life1 datafusion::logical_expr::LogicalPlan,
    ) -> Pin<
        Box<
            dyn std::future::Future<
                    Output = datafusion::error::Result<
                        Arc<dyn datafusion::physical_plan::ExecutionPlan>,
                    >,
                > + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        unimplemented!("peer chunk service does not plan queries")
    }
    fn create_physical_expr(
        &self,
        _expr: datafusion::logical_expr::Expr,
        _df_schema: &datafusion::common::DFSchema,
    ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::PhysicalExpr>> {
        unimplemented!("peer chunk service does not plan queries")
    }
    fn scalar_functions(
        &self,
    ) -> &std::collections::HashMap<String, Arc<datafusion::logical_expr::ScalarUDF>> {
        unimplemented!("peer chunk service does not plan queries")
    }
    fn aggregate_functions(
        &self,
    ) -> &std::collections::HashMap<String, Arc<datafusion::logical_expr::AggregateUDF>> {
        unimplemented!("peer chunk service does not plan queries")
    }
    fn window_functions(
        &self,
    ) -> &std::collections::HashMap<String, Arc<datafusion::logical_expr::WindowUDF>> {
        unimplemented!("peer chunk service does not plan queries")
    }
    fn runtime_env(&self) -> &Arc<datafusion::execution::runtime_env::RuntimeEnv> {
        unimplemented!("peer chunk service does not plan queries")
    }
    fn execution_props(&self) -> &datafusion::execution::context::ExecutionProps {
        unimplemented!("peer chunk service does not plan queries")
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn table_options(&self) -> &datafusion::config::TableOptions {
        unimplemented!("peer chunk service does not plan queries")
    }
    fn table_options_mut(&mut self) -> &mut datafusion::config::TableOptions {
        unimplemented!("peer chunk service does not plan queries")
    }
    fn task_ctx(&self) -> Arc<datafusion::execution::TaskContext> {
        unimplemented!("peer chunk service does not plan queries")
    }
}

#[tonic::async_trait]
impl FlightService for PeerChunkService {
    type HandshakeStream = TonicStream<HandshakeResponse>;
    type ListFlightsStream = TonicStream<FlightInfo>;
    type DoGetStream = TonicStream<FlightData>;
    type DoPutStream = TonicStream<PutResult>;
    type DoActionStream = TonicStream<arrow_flight::Result>;
    type ListActionsStream = TonicStream<ActionType>;
    type DoExchangeStream = TonicStream<FlightData>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        self.requests.fetch_add(1, Ordering::SeqCst);

        let ticket = PeerChunkTicket::decode(&request.into_inner().ticket)
            .map_err(|e| Status::invalid_argument(format!("malformed ticket: {e}")))?;

        debug!(?ticket, "serving peer chunk request");

        let BufferAnswer {
            schema,
            mut batches,
            unseen,
            table_def,
        } = self.buffer_batches(&ticket)?;

        // Rows from files this reader cannot know about yet. Empty whenever it is caught up, which
        // is the normal case, so this costs nothing in steady state.
        //
        // They ride back as record batches alongside the buffered rows, and so arrive at the
        // reader carrying `PEER_BUFFER_CHUNK_ORDER`. That is not a new ordering claim: these rows
        // were in this node's buffer moments ago and would have been served at exactly that order
        // had the query arrived slightly sooner. This preserves the order they already had across
        // the handoff rather than inventing one.
        let unseen_count = unseen.len();
        let unseen_batches = self.parquet_batches(&table_def, &unseen).await?;
        if unseen_count > 0 {
            debug!(
                files = unseen_count,
                batches = unseen_batches.len(),
                since = ?ticket.since_file_id(),
                "served files the reader had not seen"
            );
        }
        batches.extend(unseen_batches);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        debug!(
            batches = batches.len(),
            rows, "serving buffer batches to peer"
        );

        let stream = futures::stream::iter(batches.into_iter().map(Ok));
        let encoded = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            // Tags are `Dictionary(Int32, Utf8)`. Flight's default `Hydrate` mode rewrites
            // dictionary columns to their plain values, which changes the schema the querier
            // receives and fails dedup with a type mismatch. `Resend` preserves them.
            .with_dictionary_handling(DictionaryHandling::Resend)
            .build(stream)
            .map_err(|e| Status::internal(format!("flight encode error: {e}")));

        let mut response = Response::new(Box::pin(encoded) as Self::DoGetStream);

        // Tell the caller how far this node has published its own manifests. It costs one header
        // on a call the querier is already making, and it is the only way a reader can tell
        // "this peer's buffer is genuinely empty" from "this peer moved rows to Parquet I have not
        // heard about yet" — the two look identical in the batches alone.
        crate::rpc::coverage::attach_watermark(response.metadata_mut(), self.published_watermark());

        Ok(response)
    }

    async fn handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake"))
    }
    async fn list_flights(
        &self,
        _: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }
    async fn get_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }
    async fn poll_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }
    async fn get_schema(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }
    async fn do_put(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        if action.r#type != COMPACTION_NOTICE_ACTION {
            return Err(Status::unimplemented(format!(
                "unknown action {:?}",
                action.r#type
            )));
        }

        let notice = CompactionNotice::decode(&action.body)
            .map_err(|e| Status::invalid_argument(format!("malformed compaction notice: {e}")))?;

        self.apply_compaction_notice(&notice).await?;

        let stream = futures::stream::empty();
        Ok(Response::new(Box::pin(stream) as Self::DoActionStream))
    }
    async fn list_actions(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![Ok(ActionType {
            r#type: COMPACTION_NOTICE_ACTION.to_string(),
            description: "Apply a compaction a compactor wrote into this node's prefix".to_string(),
        })];
        let stream = futures::stream::iter(actions);
        Ok(Response::new(Box::pin(stream) as Self::ListActionsStream))
    }
    async fn do_exchange(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }
}

/// Bind and serve the peer chunk service until shutdown.
pub fn spawn_peer_chunk_server(
    addr: SocketAddr,
    service: PeerChunkService,
    shutdown: ShutdownToken,
) {
    tokio::spawn(async move {
        info!(%addr, "starting cluster peer chunk RPC server");
        let server = tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_shutdown(addr, async move {
                shutdown.wait_for_shutdown().await;
            });
        if let Err(error) = server.await {
            error!(%error, %addr, "cluster peer chunk RPC server failed");
        }
        info!("cluster peer chunk RPC server stopped");
    });
}
