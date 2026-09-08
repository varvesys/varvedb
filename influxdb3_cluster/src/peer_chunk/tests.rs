use std::sync::Arc;

use data_types::{PartitionHashId, PartitionKey};
use influxdb3_id::{DbId, TableId};
use iox_query::{QueryChunk, QueryChunkData};
use schema::{InfluxColumnType, InfluxFieldType, SchemaBuilder};

use super::{PEER_BUFFER_CHUNK_ORDER, PeerBufferChunk};
use crate::rpc::client::PeerClients;
use crate::rpc::ticket::PeerChunkTicket;

fn test_schema() -> schema::Schema {
    let mut builder = SchemaBuilder::new();
    builder.measurement("m");
    builder.influx_column("host", InfluxColumnType::Tag);
    builder.influx_column("v", InfluxColumnType::Field(InfluxFieldType::Float));
    builder.influx_column("time", InfluxColumnType::Timestamp);
    builder.build().unwrap()
}

fn chunk(addr: &str) -> PeerBufferChunk {
    PeerBufferChunk::new(
        Arc::from(addr),
        Arc::new(PeerClients::new()),
        PeerChunkTicket::new(DbId::new(1), TableId::new(0), None, None, None),
        test_schema(),
        PartitionHashId::new(
            data_types::TableId::new(0),
            &PartitionKey::from("peer-buffer-x"),
        ),
        Arc::from("peer-under-test"),
        crate::rpc::coverage::CoverageTracker::new(Arc::new(crate::FileIndex::new())),
    )
}

/// **The invariant the whole design rests on.**
///
/// `ChunkContainer::get_table_chunks` is synchronous, so the only way to fetch from a peer without
/// blocking a DataFusion worker is to defer the RPC into the chunk's stream. DataFusion calls
/// `data()` once during planning purely to discriminate the variant and discards the result — if
/// that call ever performed I/O, we would be doing a network round trip inside sync planning code.
///
/// The address here is unroutable. If `data()` were eager this test would hang or error; instead it
/// returns immediately, because nothing has been polled.
#[test]
fn data_performs_no_io_until_polled() {
    let chunk = chunk("240.0.0.1:1");

    let data = chunk.data();

    assert!(
        matches!(data, QueryChunkData::RecordBatches(_)),
        "peer chunks must present as record batches, not parquet"
    );
    // Dropping the stream unpolled must also be harmless — this is exactly what
    // `chunks_to_physical_nodes` does at plan time.
    drop(data);
}

/// Calling `data()` repeatedly must be safe: the plan-time probe is followed by a real call at
/// execution time.
#[test]
fn data_can_be_called_more_than_once() {
    let chunk = chunk("240.0.0.1:1");
    drop(chunk.data());
    drop(chunk.data());
}

/// Everything the planner needs must be answerable without contacting the peer.
#[test]
fn plan_time_metadata_needs_no_rpc() {
    let chunk = chunk("240.0.0.1:1");

    assert_eq!(chunk.schema().len(), 3);
    // Unknown row count is the honest answer before the fetch; it makes the optimiser conservative
    // rather than wrong.
    assert_eq!(
        chunk.stats().num_rows,
        datafusion::common::stats::Precision::Absent
    );
    assert_eq!(chunk.chunk_type(), "PeerBufferChunk");
}

/// Ordering must place peer buffer data above all Parquet but below this node's own buffer, which
/// uses `i64::MAX`.
#[test]
fn peer_buffer_ranks_below_local_buffer_and_above_parquet() {
    let chunk = chunk("240.0.0.1:1");
    let order = chunk.order().get();

    assert_eq!(order, PEER_BUFFER_CHUNK_ORDER);
    assert!(order < i64::MAX, "local buffer must win ties against peers");
    assert!(
        order > 1_000_000,
        "peer buffer must outrank parquet, which is numbered from 0"
    );
}

/// Raw buffered rows are not deduplicated, so the chunk must declare that — otherwise the planner
/// may skip the dedup it needs.
#[test]
fn peer_buffer_declares_possible_duplicates() {
    let chunk = chunk("240.0.0.1:1");
    assert!(chunk.may_contain_pk_duplicates());
    assert!(chunk.sort_key().is_none());
}
