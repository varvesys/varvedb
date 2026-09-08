//! Node-to-node RPC.
//!
//! Two messages share one Arrow Flight service and one channel cache:
//!
//! * A querier fetches a peer's un-persisted rows over `do_get`, sending a JSON
//!   [`ticket::PeerChunkTicket`] and receiving `RecordBatch`es via `FlightDataEncoderBuilder`.
//! * A compactor tells a node it has rewritten some of that node's Parquet, over `do_action` with a
//!   JSON [`notice::CompactionNotice`].
//!
//! The two flow in opposite directions — queriers pull from ingesters, compactors push to them —
//! but a compact-only node publishes no `conn_info`, so nothing ever dials a compactor.

pub mod client;
pub mod coverage;
pub mod notice;
pub mod server;
pub mod ticket;
