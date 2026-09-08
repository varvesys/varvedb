//! The querier side: fetch a peer's un-persisted rows over Arrow Flight.
//!
//! Everything here is invoked from inside a chunk's stream, i.e. at **execution** time, never
//! during planning. See [`crate::peer_chunk`] for why that matters.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, Ticket};
use bytes::Bytes;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use futures::TryStreamExt;
use observability_deps::tracing::{debug, warn};
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::rpc::notice::{COMPACTION_NOTICE_ACTION, CompactionNotice};
use crate::rpc::ticket::PeerChunkTicket;

/// How long to wait to establish a connection to a peer.
///
/// A peer that is slow or gone must fail the query with a clear error rather than hanging it — the
/// CPSDB read path's missing timeouts are the cautionary example.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Overall deadline for fetching one peer's chunks.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Cache of gRPC channels, one per peer.
///
/// `tonic::transport::Channel` is cheap to clone and multiplexes concurrent requests over a single
/// HTTP/2 connection, so caching one per peer removes a TCP handshake from every chunk fetch.
/// Without this, a cluster of Q queriers and I ingesters pays Q×I connections *per query* — the
/// per-query overhead that motivated choosing a pull RPC over eager WAL tailing in the first place.
#[derive(Debug, Default)]
pub struct PeerClients {
    channels: Mutex<HashMap<Arc<str>, Channel>>,
}

impl PeerClients {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get a channel for `addr`, connecting if this is the first use.
    async fn channel(&self, addr: &str) -> Result<Channel, DataFusionError> {
        if let Some(channel) = self.channels.lock().await.get(addr) {
            return Ok(channel.clone());
        }

        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .map_err(|e| {
                DataFusionError::External(format!("invalid peer address {addr}: {e}").into())
            })?
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT);

        let channel = endpoint.connect().await.map_err(|e| {
            // Name the peer: a bare transport error is useless when several are in play.
            DataFusionError::External(format!("failed to connect to peer {addr}: {e}").into())
        })?;

        self.channels
            .lock()
            .await
            .insert(Arc::from(addr), channel.clone());
        Ok(channel)
    }

    /// Forget the cached channel for `addr`.
    ///
    /// Called after a transport failure. A cached channel whose peer has restarted is worse than no
    /// cache at all — it would fail every subsequent query until the process restarted.
    async fn invalidate(&self, addr: &str) {
        if self.channels.lock().await.remove(addr).is_some() {
            debug!(%addr, "dropped cached channel for peer");
        }
    }
}

/// Tell a peer that a compactor rewrote some of its Parquet.
///
/// Reuses the same channel cache as the buffered-row fetch, so a compactor pays one connection per
/// peer rather than one per notice.
///
/// The caller treats failure as advisory: the compaction is already durable in object store, and an
/// undelivered notice only means the peer keeps stale entries in memory until it restarts.
pub async fn notify_compaction(
    clients: &PeerClients,
    addr: &str,
    notice: &CompactionNotice,
) -> Result<(), DataFusionError> {
    match notify_inner(clients, addr, notice).await {
        Ok(()) => Ok(()),
        Err(e) => {
            clients.invalidate(addr).await;
            Err(e)
        }
    }
}

async fn notify_inner(
    clients: &PeerClients,
    addr: &str,
    notice: &CompactionNotice,
) -> Result<(), DataFusionError> {
    let channel = clients.channel(addr).await?;
    let mut client = FlightServiceClient::new(channel);

    let body = notice.encode().map_err(|e| {
        DataFusionError::External(format!("encoding compaction notice: {e}").into())
    })?;

    let action = Action {
        r#type: COMPACTION_NOTICE_ACTION.to_string(),
        body: Bytes::from(body),
    };

    let mut stream = client
        .do_action(action)
        .await
        .map_err(|e| {
            DataFusionError::External(format!("compaction notice to {addr} failed: {e}").into())
        })?
        .into_inner();

    // Drain the response so the peer sees a completed call rather than a cancelled one.
    while stream
        .try_next()
        .await
        .map_err(|e| {
            DataFusionError::External(
                format!("compaction notice response from {addr} failed: {e}").into(),
            )
        })?
        .is_some()
    {}

    Ok(())
}

/// A peer's buffered rows, plus what it claims to have published.
#[derive(Debug)]
pub struct PeerFetch {
    pub batches: Vec<RecordBatch>,
    /// How far the peer says its own manifests have reached the shared log.
    ///
    /// `None` is "no claim" — an older peer, or one that has published nothing. Never read it as
    /// zero: that would turn silence into a false assurance of completeness.
    pub peer_published: Option<influxdb3_wal::SnapshotSequenceNumber>,
}

/// Fetch a peer's buffered rows for one table.
///
/// `addr` is the peer's `conn_info` from the catalog, e.g. `10.0.0.4:8383`.
pub async fn fetch_peer_batches(
    clients: &PeerClients,
    addr: &str,
    ticket: PeerChunkTicket,
) -> Result<PeerFetch, DataFusionError> {
    match fetch_inner(clients, addr, ticket).await {
        Ok(fetched) => Ok(fetched),
        Err(e) => {
            // Any failure may have been caused by a stale channel (peer restarted, connection
            // reset), so drop it. The next attempt reconnects.
            clients.invalidate(addr).await;
            Err(e)
        }
    }
}

async fn fetch_inner(
    clients: &PeerClients,
    addr: &str,
    ticket: PeerChunkTicket,
) -> Result<PeerFetch, DataFusionError> {
    let channel = clients.channel(addr).await?;
    let mut client = FlightServiceClient::new(channel);

    let encoded = ticket
        .encode()
        .map_err(|e| DataFusionError::External(format!("failed to encode ticket: {e}").into()))?;

    let response = client
        .do_get(Ticket {
            ticket: Bytes::from(encoded),
        })
        .await
        .map_err(|e| {
            DataFusionError::External(format!("peer {addr} rejected request: {e}").into())
        })?;

    // Read the peer's publish watermark before consuming the stream — `into_inner` discards the
    // metadata, and this header is the only thing that distinguishes "the buffer really is empty"
    // from "these rows moved to Parquet you have not replayed yet".
    let peer_published = crate::rpc::coverage::read_watermark(response.metadata());
    let stream = response.into_inner();

    let mut record_batch_stream =
        arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
            stream.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
        );

    let mut batches = Vec::new();
    while let Some(batch) = record_batch_stream
        .try_next()
        .await
        .map_err(|e| DataFusionError::External(format!("peer {addr} stream error: {e}").into()))?
    {
        batches.push(batch);
    }

    debug!(
        %addr,
        batches = batches.len(),
        rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        ?peer_published,
        "fetched peer buffer batches"
    );

    Ok(PeerFetch {
        batches,
        peer_published,
    })
}

/// Log a peer failure at a consistent level, so operators can find them.
pub fn warn_peer_failure(addr: &str, error: &DataFusionError) {
    warn!(%addr, %error, "failed to fetch peer buffer chunks");
}
