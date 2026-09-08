//! Detecting that a querier's file index is behind the peer it is reading from.
//!
//! # The gap this closes
//!
//! A row moves from an ingester's buffer to its Parquet in two steps that no reader observes
//! atomically: the manifest is written, then the buffer entry is dropped. A querier learns about
//! the Parquet only once the manifest has been published to the shared log and the querier has
//! replayed that far. Between the ingester dropping the rows and the querier catching up, the rows
//! are in **neither** source — the buffer no longer serves them, and the index does not yet know
//! the file that holds them.
//!
//! Shortening the sync interval narrows that window but never closes it, because the two events
//! are not coordinated. What *can* be closed is the reader's ignorance of it: an ingester knows
//! exactly how far it has published, so it can say so, and a querier that is behind can find out
//! instead of quietly returning fewer rows.
//!
//! # How it rides along
//!
//! The querier already calls `do_get` on every peer whose buffer might match. The peer answers with
//! its published watermark in a gRPC response header — no extra round trip, no new endpoint, and
//! nothing to keep in sync when peers come and go.
//!
//! A peer that does not send the header is simply older than this feature; its absence is treated
//! as "no claim", never as zero.

use influxdb3_wal::SnapshotSequenceNumber;
use tonic::metadata::{MetadataMap, MetadataValue};

/// gRPC response header carrying the peer's published snapshot watermark.
pub const PUBLISHED_WATERMARK_HEADER: &str = "x-influxdb3-published-snapshot";

/// Attach a peer's published watermark to a response.
pub fn attach_watermark(metadata: &mut MetadataMap, watermark: Option<SnapshotSequenceNumber>) {
    let Some(watermark) = watermark else {
        // Nothing published yet. Saying "0" would be a claim we cannot support — a reader could
        // read it as "fully caught up at zero" — so say nothing at all.
        return;
    };
    if let Ok(value) = MetadataValue::try_from(watermark.as_u64().to_string()) {
        metadata.insert(PUBLISHED_WATERMARK_HEADER, value);
    }
}

/// Read a peer's published watermark from a response, if it sent one.
pub fn read_watermark(metadata: &MetadataMap) -> Option<SnapshotSequenceNumber> {
    metadata
        .get(PUBLISHED_WATERMARK_HEADER)?
        .to_str()
        .ok()?
        .parse::<u64>()
        .ok()
        .map(SnapshotSequenceNumber::new)
}

/// What the comparison established.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Coverage {
    /// The reader has replayed everything the peer says it published.
    Complete,
    /// The peer has published manifests this reader has not seen.
    ///
    /// Rows the peer has already dropped from its buffer may live in those files, so the answer
    /// this query is about to produce can be missing rows.
    Behind {
        peer_published: SnapshotSequenceNumber,
        reader_has: Option<SnapshotSequenceNumber>,
    },
    /// The peer made no claim, so nothing was checked.
    Unknown,
}

/// Compare what a peer says it published against what this reader has replayed.
pub fn assess(
    peer_published: Option<SnapshotSequenceNumber>,
    reader_has: Option<SnapshotSequenceNumber>,
) -> Coverage {
    match (peer_published, reader_has) {
        (None, _) => Coverage::Unknown,
        (Some(published), Some(has)) if has >= published => Coverage::Complete,
        // A peer that has published anything at all while this reader has replayed nothing from it
        // is behind, not merely unseen — the distinction matters because "never seen" is otherwise
        // indistinguishable from "seen and empty".
        (Some(published), reader_has) => Coverage::Behind {
            peer_published: published,
            reader_has,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seq(n: u64) -> SnapshotSequenceNumber {
        SnapshotSequenceNumber::new(n)
    }

    #[test]
    fn caught_up_is_complete() {
        assert_eq!(assess(Some(seq(5)), Some(seq(5))), Coverage::Complete);
        assert_eq!(assess(Some(seq(5)), Some(seq(9))), Coverage::Complete);
    }

    #[test]
    fn lagging_reader_is_detected() {
        assert!(matches!(
            assess(Some(seq(7)), Some(seq(3))),
            Coverage::Behind { .. }
        ));
    }

    #[test]
    fn a_reader_that_has_seen_nothing_from_a_publishing_peer_is_behind() {
        // The case the whole mechanism exists for: cold start. An empty index is otherwise
        // indistinguishable from a peer that has genuinely persisted nothing.
        assert!(matches!(
            assess(Some(seq(1)), None),
            Coverage::Behind {
                reader_has: None,
                ..
            }
        ));
    }

    #[test]
    fn a_silent_peer_yields_no_claim() {
        // An older peer, or one that has published nothing. Either way we learn nothing, and
        // must not infer completeness from silence.
        assert_eq!(assess(None, Some(seq(4))), Coverage::Unknown);
        assert_eq!(assess(None, None), Coverage::Unknown);
    }

    #[test]
    fn header_round_trips() {
        let mut md = MetadataMap::new();
        attach_watermark(&mut md, Some(seq(42)));
        assert_eq!(read_watermark(&md), Some(seq(42)));
    }

    #[test]
    fn nothing_published_attaches_no_header() {
        let mut md = MetadataMap::new();
        attach_watermark(&mut md, None);
        assert_eq!(read_watermark(&md), None, "absence must not read as zero");
    }
}

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use observability_deps::tracing::warn;

use crate::file_index::FileIndex;

/// Records how often a query was served while this node's index was behind a peer.
///
/// Deliberately observational: it does not fail the query. A short answer is bad, but failing
/// every query during a routine catch-up would be worse, and the window is normally milliseconds.
/// What matters is that the condition stops being invisible — before this, a querier that was
/// behind returned fewer rows and reported success, with nothing anywhere recording that it had.
#[derive(Debug)]
pub struct CoverageTracker {
    /// This node's replay state, for comparison against what a peer claims.
    index: Arc<FileIndex>,
    behind: AtomicU64,
    checked: AtomicU64,
}

impl CoverageTracker {
    pub fn new(index: Arc<FileIndex>) -> Arc<Self> {
        Arc::new(Self {
            index,
            behind: AtomicU64::new(0),
            checked: AtomicU64::new(0),
        })
    }

    /// Compare a peer's claim against what this node has replayed for it.
    pub fn observe(&self, peer_id: &str, peer_published: Option<SnapshotSequenceNumber>) {
        self.checked.fetch_add(1, Ordering::Relaxed);
        let reader_has = self.index.published_watermark(peer_id);

        if let Coverage::Behind {
            peer_published,
            reader_has,
        } = assess(peer_published, reader_has)
        {
            let count = self.behind.fetch_add(1, Ordering::Relaxed) + 1;
            warn!(
                %peer_id,
                peer_published = peer_published.as_u64(),
                reader_has = ?reader_has.map(|s| s.as_u64()),
                occurrences = count,
                "serving a query while behind a peer\'s published files; \
                 rows it has already buffered-out may be missing from this result"
            );
        }
    }

    /// Queries served while behind a peer. Zero in steady state.
    pub fn behind_count(&self) -> u64 {
        self.behind.load(Ordering::Relaxed)
    }

    /// Peer responses whose coverage was checked at all.
    pub fn checked_count(&self) -> u64 {
        self.checked.load(Ordering::Relaxed)
    }
}
