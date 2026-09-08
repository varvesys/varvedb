//! The notice a compactor sends to the node whose Parquet it merged.
//!
//! This rides on Arrow Flight `do_action`, reusing the channel cache the buffered-row `do_get`
//! already keeps, and is encoded as JSON for the same reason [`super::ticket`] is: it does not
//! justify pulling `protoc` codegen into this crate.
//!
//! # Why the receiver mints the identifiers
//!
//! The notice describes the merge but allocates nothing. Snapshot sequence numbers and
//! `ParquetFileId`s both come from per-node allocators, and **no node may allocate in another
//! node's space** — a compactor that tried would either collide with a sequence the owner was about
//! to use (whose unconditional PUT then destroys a real manifest) or escape into a reserved band so
//! high that it breaks the single watermark `sync.rs` keeps per peer, silently hiding every file
//! that peer persists afterwards.
//!
//! So the compactor writes bytes and the owner writes the record: on receipt the owner reserves a
//! sequence from its own WAL, mints its own `ParquetFileId`, and persists the manifest. That is why
//! no id appears on the wire.
//!
//! # Delivery
//!
//! The merged Parquet is already durable when the notice is sent, and the input files are still
//! intact and still referenced. A notice that never arrives therefore costs an orphaned object, not
//! correctness, and the compactor retries on its next pass — writing to the same path, because the
//! output path is derived from the inputs.
//!
//! The compactor deletes the inputs only after this call succeeds. Acknowledgement is what proves
//! the replacement is durable somewhere other than the compactor's memory.

use influxdb3_id::{DbId, TableId};
use influxdb3_write::ParquetFile;
use serde::{Deserialize, Serialize};

/// The `Action::r#type` value identifying a compaction notice.
pub const COMPACTION_NOTICE_ACTION: &str = "compaction-notice";

/// Describes one completed merge to the node that owns the files.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionNotice {
    /// The prefix the merged file was written to.
    ///
    /// The receiver checks this against its own node id before acting. Without it, any peer able to
    /// reach the RPC port could drive files out of another node's index.
    pub node_id: String,
    pub db_id: u32,
    pub table_id: u32,
    /// Object store path of the merged file the compactor wrote.
    pub merged_path: String,
    pub merged_size_bytes: u64,
    pub merged_row_count: u64,
    pub merged_chunk_time: i64,
    pub merged_min_time: i64,
    pub merged_max_time: i64,
    /// Paths of the files the merged file replaces.
    ///
    /// Paths rather than `ParquetFileId`s: ids are unique only per allocator, while paths are
    /// unique cluster-wide because the node prefix is their first component.
    pub removed_paths: Vec<String>,
}

impl CompactionNotice {
    pub fn new(
        node_id: impl Into<String>,
        db_id: DbId,
        table_id: TableId,
        merged: &ParquetFile,
        removed_paths: Vec<String>,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            db_id: db_id.get(),
            table_id: table_id.get(),
            merged_path: merged.path.to_string(),
            merged_size_bytes: merged.size_bytes,
            merged_row_count: merged.row_count,
            merged_chunk_time: merged.chunk_time,
            merged_min_time: merged.min_time,
            merged_max_time: merged.max_time,
            removed_paths,
        }
    }

    pub fn db_id(&self) -> DbId {
        DbId::from(self.db_id)
    }

    pub fn table_id(&self) -> TableId {
        TableId::from(self.table_id)
    }

    /// Rebuild the merged file's record, assigning it an id from the **receiver's** allocator.
    pub fn merged_file(&self, id: influxdb3_id::ParquetFileId) -> ParquetFile {
        ParquetFile {
            id,
            path: self.merged_path.as_str().into(),
            size_bytes: self.merged_size_bytes,
            row_count: self.merged_row_count,
            chunk_time: self.merged_chunk_time,
            min_time: self.merged_min_time,
            max_time: self.merged_max_time,
        }
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
    use influxdb3_id::ParquetFileId;

    fn merged() -> ParquetFile {
        ParquetFile {
            id: ParquetFileId::from(7),
            path: "ingest-01/dbs/0/1/2026-08-19/14-00/0000000000-2147483649.parquet".into(),
            size_bytes: 1024,
            row_count: 500,
            chunk_time: 100,
            min_time: 100,
            max_time: 900,
        }
    }

    #[test]
    fn notice_round_trips() {
        let notice = CompactionNotice::new(
            "ingest-01",
            DbId::new(3),
            TableId::new(7),
            &merged(),
            vec!["a.parquet".to_string(), "b.parquet".to_string()],
        );

        let decoded = CompactionNotice::decode(&notice.encode().unwrap()).unwrap();
        assert_eq!(notice, decoded);
        assert_eq!(decoded.db_id(), DbId::new(3));
        assert_eq!(decoded.table_id(), TableId::new(7));
        assert_eq!(decoded.removed_paths.len(), 2);
    }

    #[test]
    fn merged_file_takes_the_receivers_id() {
        // The compactor's own id for the file never reaches the wire; the owner assigns one from
        // its own allocator so nothing allocates in a namespace it does not own.
        let notice = CompactionNotice::new(
            "ingest-01",
            DbId::new(0),
            TableId::new(0),
            &merged(),
            vec![],
        );
        let rebuilt = notice.merged_file(ParquetFileId::from(99));

        assert_eq!(rebuilt.id, ParquetFileId::from(99));
        assert_eq!(rebuilt.path, merged().path);
        assert_eq!(rebuilt.row_count, 500);
        assert_eq!(rebuilt.min_time, 100);
        assert_eq!(rebuilt.max_time, 900);
    }
}
