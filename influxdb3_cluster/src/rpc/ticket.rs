//! The request a querier sends to a peer ingester.
//!
//! This rides on the standard Arrow Flight `do_get` RPC as the `Ticket` payload — the same design
//! IOx uses in `core/ingester_query_grpc` (whose `.proto` declares no service of its own for
//! exactly this reason). Encoding it as JSON rather than protobuf avoids pulling `protoc` codegen
//! into this crate for what is a four-field message.

use influxdb3_id::{DbId, TableId};
use serde::{Deserialize, Serialize};

/// A scoped request for a peer's **un-persisted** rows.
///
/// Scoping matters: this is what makes the RPC proportional to the query rather than to the
/// cluster's ingest rate. The peer answers only for one table, and only within the time bounds the
/// query actually needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerChunkTicket {
    pub db_id: u32,
    pub table_id: u32,
    /// Inclusive lower bound on `time`, if the query had one.
    pub time_lower_bound_ns: Option<i64>,
    /// Inclusive upper bound on `time`, if the query had one.
    pub time_upper_bound_ns: Option<i64>,
}

impl PeerChunkTicket {
    pub fn new(
        db_id: DbId,
        table_id: TableId,
        time_lower_bound_ns: Option<i64>,
        time_upper_bound_ns: Option<i64>,
    ) -> Self {
        Self {
            db_id: db_id.get(),
            table_id: table_id.get(),
            time_lower_bound_ns,
            time_upper_bound_ns,
        }
    }

    pub fn db_id(&self) -> DbId {
        DbId::from(self.db_id)
    }

    pub fn table_id(&self) -> TableId {
        TableId::from(self.table_id)
    }

    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticket_round_trips() {
        let ticket = PeerChunkTicket::new(DbId::new(3), TableId::new(7), Some(100), Some(200));
        let encoded = ticket.encode().unwrap();
        let decoded = PeerChunkTicket::decode(&encoded).unwrap();
        assert_eq!(ticket, decoded);
        assert_eq!(decoded.db_id(), DbId::new(3));
        assert_eq!(decoded.table_id(), TableId::new(7));
    }

    #[test]
    fn unbounded_time_range_round_trips() {
        let ticket = PeerChunkTicket::new(DbId::new(1), TableId::new(0), None, None);
        let decoded = PeerChunkTicket::decode(&ticket.encode().unwrap()).unwrap();
        assert_eq!(decoded.time_lower_bound_ns, None);
        assert_eq!(decoded.time_upper_bound_ns, None);
    }
}
