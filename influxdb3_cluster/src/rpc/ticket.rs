//! The request a querier sends to a peer ingester.
//!
//! This rides on the standard Arrow Flight `do_get` RPC as the `Ticket` payload — the same design
//! IOx uses in `core/ingester_query_grpc` (whose `.proto` declares no service of its own for
//! exactly this reason). Encoding it as JSON rather than protobuf avoids pulling `protoc` codegen
//! into this crate for what is a four-field message.

use influxdb3_id::{DbId, ParquetFileId, TableId};
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
    /// Highest `ParquetFileId` this reader already knows about for this peer and table.
    ///
    /// The peer answers with its buffered rows **plus** the rows from any of its own files above
    /// this id. Those are exactly the files the reader cannot know about: it learns of a peer's
    /// files through the shared log, and a file is published to that log only after its manifest
    /// is written — which happens after the buffer chunk covering it was already dropped.
    ///
    /// Without this, rows are in neither source for that window: gone from the peer's buffer, not
    /// yet in the reader's index. Visibility becomes non-monotonic, which is worse than merely
    /// stale — a row a query returned can vanish from the next one.
    ///
    /// `None` means the reader made no claim (an older build, or it has seen nothing from this
    /// peer). Treated as "send only the buffer", never as zero — a reader that has seen nothing
    /// still gets a correct answer, just without the extra files.
    #[serde(default)]
    pub since_file_id: Option<u64>,
}

impl PeerChunkTicket {
    pub fn new(
        db_id: DbId,
        table_id: TableId,
        time_lower_bound_ns: Option<i64>,
        time_upper_bound_ns: Option<i64>,
        since_file_id: Option<ParquetFileId>,
    ) -> Self {
        Self {
            db_id: db_id.get(),
            table_id: table_id.get(),
            time_lower_bound_ns,
            time_upper_bound_ns,
            since_file_id: since_file_id.map(|id| id.as_u64()),
        }
    }

    /// The reader's file watermark, if it declared one.
    pub fn since_file_id(&self) -> Option<ParquetFileId> {
        self.since_file_id.map(ParquetFileId::from)
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
        let ticket = PeerChunkTicket::new(
            DbId::new(3),
            TableId::new(7),
            Some(100),
            Some(200),
            Some(ParquetFileId::from(42)),
        );
        let encoded = ticket.encode().unwrap();
        let decoded = PeerChunkTicket::decode(&encoded).unwrap();
        assert_eq!(ticket, decoded);
        assert_eq!(decoded.db_id(), DbId::new(3));
        assert_eq!(decoded.table_id(), TableId::new(7));
    }

    #[test]
    fn unbounded_time_range_round_trips() {
        let ticket = PeerChunkTicket::new(DbId::new(1), TableId::new(0), None, None, None);
        let decoded = PeerChunkTicket::decode(&ticket.encode().unwrap()).unwrap();
        assert_eq!(decoded.time_lower_bound_ns, None);
        assert_eq!(decoded.time_upper_bound_ns, None);
    }
}

#[cfg(test)]
mod watermark_tests {
    use super::*;

    #[test]
    fn file_watermark_round_trips() {
        let ticket = PeerChunkTicket::new(
            DbId::new(1),
            TableId::new(0),
            None,
            None,
            Some(ParquetFileId::from(9_001)),
        );
        let decoded = PeerChunkTicket::decode(&ticket.encode().unwrap()).unwrap();
        assert_eq!(decoded.since_file_id(), Some(ParquetFileId::from(9_001)));
    }

    #[test]
    fn a_ticket_from_an_older_reader_decodes_with_no_claim() {
        // Compatibility in the direction that matters: a reader on an older build sends a payload
        // without the field. Serde resolves a missing `Option` to `None`, which the peer must read
        // as "no claim" — send the buffer only — never as zero, which would ask it to ship every
        // file it has.
        let older =
            br#"{"db_id":1,"table_id":0,"time_lower_bound_ns":null,"time_upper_bound_ns":null}"#;
        let decoded = PeerChunkTicket::decode(older).unwrap();

        assert_eq!(decoded.since_file_id(), None);
        assert_eq!(decoded.db_id(), DbId::new(1));
    }

    #[test]
    fn an_unknown_field_is_ignored() {
        // The other direction: a newer reader sends a field this peer does not know. Decoding must
        // tolerate it rather than fail the query.
        let newer = br#"{"db_id":1,"table_id":0,"time_lower_bound_ns":null,
                         "time_upper_bound_ns":null,"since_file_id":5,"future_field":"x"}"#;
        let decoded = PeerChunkTicket::decode(newer).unwrap();

        assert_eq!(decoded.since_file_id(), Some(ParquetFileId::from(5)));
    }
}
