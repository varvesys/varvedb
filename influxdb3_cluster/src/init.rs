//! Wiring helpers, so the server binary needs only a call site rather than the assembly.

use std::sync::Arc;

use influxdb3_catalog::CatalogError;
use influxdb3_catalog::catalog::{Catalog, CatalogArgs, CatalogLimiter};
use influxdb3_process::ProcessUuidGetter;
use influxdb3_shutdown::{ShutdownManager, ShutdownToken};
use influxdb3_wal::NoopCatalogSnapshotObserver;
use influxdb3_write::persister::Persister;
use influxdb3_write::write_buffer::WriteBufferImpl;
use influxdb3_write::{Bufferer, WriteBuffer};
use iox_query::exec::Executor;
use iox_time::TimeProvider;
use metric::Registry;
use object_store::ObjectStore;
use observability_deps::tracing::info;

use crate::ClusterWriteBuffer;
use crate::config::ClusterIdentity;
use crate::file_index::FileIndex;
use crate::file_index::log::{FileIndexLog, spawn_file_index_sync};
use crate::file_index::publisher::spawn_file_index_publisher;

/// Build the catalog for a node participating in a cluster, and keep it in sync with peers.
///
/// The catalog is prefixed by the **cluster** id so that every node shares one schema, while data
/// (WAL, Parquet, snapshots, indices) stays under the node prefix. An existing node-prefixed
/// catalog is promoted to the cluster prefix on first start by
/// `promote_core_catalog_to_cluster_prefix`, which the constructor calls internally.
///
/// Also spawns the background catalog poller. Without it a node only catches up when its *own*
/// commit loses the compare-and-swap race, so a node that never writes would never observe a peer's
/// new table.
#[allow(clippy::too_many_arguments)]
pub async fn init_catalog(
    identity: &ClusterIdentity,
    object_store: Arc<dyn ObjectStore>,
    time_provider: Arc<dyn TimeProvider>,
    metrics: Arc<Registry>,
    shutdown_manager: &ShutdownManager,
    process_uuid_getter: Arc<dyn ProcessUuidGetter>,
    limits: Arc<dyn CatalogLimiter>,
) -> Result<Arc<Catalog>, CatalogError> {
    let catalog = Catalog::new_enterprise_with_shutdown(
        Arc::clone(identity.node_id()),
        Arc::clone(identity.cluster_id()),
        object_store,
        time_provider,
        metrics,
        shutdown_manager.register("catalog"),
        limits,
        CatalogArgs::default(),
        process_uuid_getter,
        Arc::new(NoopCatalogSnapshotObserver),
    )
    .await?;

    spawn_catalog_sync(
        Arc::clone(&catalog),
        identity,
        shutdown_manager.register("catalog_background_update"),
    );

    Ok(catalog)
}

fn spawn_catalog_sync(catalog: Arc<Catalog>, identity: &ClusterIdentity, token: ShutdownToken) {
    let interval = identity.catalog_sync_interval();
    info!(
        ?interval,
        "starting background catalog sync for shared catalog"
    );
    tokio::spawn(async move {
        catalog.background_update(interval, token).await;
    });
}

/// Wrap the write buffer so queries also see peers' persisted Parquet, and start the poller that
/// discovers it.
///
/// Writes, WAL, snapshotting, retention and the caches are untouched — the decorator delegates all
/// of them to the inner buffer and only overrides chunk provisioning.
#[allow(clippy::too_many_arguments)]
pub fn wrap_write_buffer(
    identity: &ClusterIdentity,
    inner: Arc<WriteBufferImpl>,
    catalog: Arc<Catalog>,
    persister: Arc<Persister>,
    time_provider: Arc<dyn TimeProvider>,
    executor: Arc<Executor>,
    shutdown_manager: &ShutdownManager,
    query_file_limit: usize,
) -> Arc<dyn WriteBuffer> {
    // The shared file index: one log, replayed by every node, carrying every peer's Parquet.
    //
    // This replaced per-peer snapshot polling. The difference that matters is not the request
    // count but the ordering — one CAS-assigned sequence spans the whole cluster, where per-node
    // snapshot sequences were never comparable to each other.
    let file_index = Arc::new(FileIndex::new());
    let file_index_log = Arc::new(
        FileIndexLog::new(persister.object_store(), Arc::clone(identity.cluster_id()))
            .with_snapshot_interval(identity.file_index_snapshot_interval()),
    );

    // Serve this node's un-persisted rows to peers. Bound to its own port because
    // `influxdb3_server`'s UnifiedService accepts exactly one gRPC service.
    //
    // A query-only node skips this entirely: it refuses writes, so its buffer is always empty and
    // the port would exist only to return nothing. It also publishes no `conn_info`, so no peer
    // has an address to dial in the first place.
    if identity.ingests() {
        crate::rpc::server::spawn_peer_chunk_server(
            identity.cluster_rpc_bind(),
            crate::rpc::server::PeerChunkService::new(
                Arc::clone(&inner),
                Arc::clone(&catalog),
                Arc::clone(identity.node_id()),
                Arc::clone(&persister),
            )
            .with_file_index(Some(Arc::clone(&file_index))),
            shutdown_manager.register("cluster_peer_rpc"),
        );
    } else {
        info!("node does not ingest; not starting the peer chunk RPC server");
    }

    spawn_file_index_sync(
        Arc::clone(&file_index_log),
        Arc::clone(&file_index),
        identity.peer_sync_interval(),
        Arc::clone(&time_provider),
        shutdown_manager.register("file_index_sync"),
    );

    if identity.ingests() {
        spawn_file_index_publisher(
            Arc::clone(identity.node_id()),
            Arc::clone(&persister),
            Arc::clone(&file_index_log),
            Arc::clone(&file_index),
            inner.watch_persisted_snapshots(),
            identity.peer_sync_interval(),
            Arc::clone(&time_provider),
            shutdown_manager.register("file_index_publisher"),
        );
    }

    // The compactor reads the same index the queriers read, so it can never merge a file set the
    // readers do not believe in. Removals arrive through the same log, so files this node has
    // already merged drop out of its own candidate list with no extra bookkeeping.
    if identity.compacts() {
        crate::compactor::spawn_compactor(
            crate::compactor::CompactorArgs {
                node_id: Arc::clone(identity.node_id()),
                config: *identity.compaction(),
                catalog: Arc::clone(&catalog),
                file_index: Arc::clone(&file_index),
                persister: Arc::clone(&persister),
                executor,
                time_provider,
                peer_clients: Arc::new(crate::rpc::client::PeerClients::new()),
            },
            shutdown_manager.register("compactor"),
        );
    }

    Arc::new(ClusterWriteBuffer::new(
        inner,
        persister,
        Arc::clone(identity.node_id()),
        query_file_limit,
        identity.ingests(),
        file_index,
    ))
}
