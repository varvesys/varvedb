//! CLI configuration and identity resolution for clustering.
//!
//! These flags live here rather than in `influxdb3::commands::serve` so that the binary only has to
//! flatten one struct — the same pattern `ObjectStoreConfig`, `TracingConfig` and
//! `ProcessingEngineConfig` already use.

use std::sync::Arc;
use std::time::Duration;

use influxdb3_catalog::catalog::NodeMode;

/// Maximum length of a `--node-id` or `--cluster-id`.
const MAX_IDENTIFIER_LEN: usize = 64;

/// Mirrors `WriteBufferImpl`'s own default when `--query-file-limit` is unset.
pub const DEFAULT_QUERY_FILE_LIMIT: usize = 432;

#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error("invalid --{kind} {value:?}: {reason}")]
    InvalidIdentifier {
        kind: &'static str,
        value: String,
        reason: String,
    },

    #[error("invalid --mode: {reason}")]
    InvalidMode { reason: String },
}

/// The node roles selectable on the command line.
///
/// This mirrors a subset of [`NodeMode`] rather than reusing it directly: `NodeMode` has no
/// `FromStr` or `clap::ValueEnum` impl, and deriving one on the catalog type would widen that
/// crate's API. `Process` registers the processing-engine role; the engine itself is built
/// unconditionally today, so naming the role changes nothing yet but keeps `--mode` honest about
/// what a node is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum CliNodeMode {
    /// Ingests writes and serves queries — the historical single-node behaviour, and the default.
    Core,
    /// Accepts writes, and serves its un-persisted rows to querying peers.
    Ingest,
    /// Serves queries only; refuses writes and publishes no peer address.
    Query,
    /// Merges small, cold Parquet files belonging to ingest-only peers.
    ///
    /// Accepts no writes, serves no queries, and publishes no peer address — the traffic it
    /// generates is one-directional, from this node out to the peers whose files it rewrites.
    Compact,
    /// Registers the processing-engine role. The engine runs regardless of mode in this build, so
    /// this currently only records the role in the catalog.
    Process,
    /// Runs every role in one process: ingest, query, compact, and process. Like any non-`core`
    /// mode it requires `--cluster-id`; a lone all-in-one node names its own cluster of one. A
    /// node in this mode compacts its own Parquet in-process rather than over RPC.
    All,
}

impl From<CliNodeMode> for NodeMode {
    fn from(mode: CliNodeMode) -> Self {
        match mode {
            CliNodeMode::Core => NodeMode::Core,
            CliNodeMode::Ingest => NodeMode::Ingest,
            CliNodeMode::Query => NodeMode::Query,
            CliNodeMode::Compact => NodeMode::Compact,
            CliNodeMode::Process => NodeMode::Process,
            CliNodeMode::All => NodeMode::All,
        }
    }
}

pub type Result<T, E = ClusterError> = std::result::Result<T, E>;

/// Cluster-related server configuration.
///
/// Flattened into the `serve` command's `Config`.
#[derive(Debug, Clone, clap::Parser)]
pub struct ClusterConfig {
    /// Identifies the cluster that this node belongs to. All nodes sharing a cluster must use the
    /// same value, and it is used as the object store prefix for the shared catalog.
    ///
    /// When omitted this defaults to the `--node-id`, which reproduces the historical single-node
    /// layout exactly. Setting it to something other than the node id promotes an existing
    /// node-prefixed catalog to the cluster prefix on first start; that migration is one-way.
    #[clap(long = "cluster-id", env = "INFLUXDB3_CLUSTER_ID", action)]
    pub cluster_id: Option<String>,

    /// How often to poll the object store for catalog changes made by other nodes in the cluster.
    ///
    /// Only used when `--cluster-id` differs from `--node-id`; a node that owns its catalog
    /// exclusively has nothing to poll for. Lower values reduce how long a peer's new table or
    /// column stays invisible, at the cost of more object store LIST/GET requests.
    #[clap(
        long = "catalog-sync-interval",
        env = "INFLUXDB3_CATALOG_SYNC_INTERVAL",
        default_value = "1s",
        action
    )]
    pub catalog_sync_interval: humantime::Duration,

    /// How often to replay the shared cluster file index log.
    ///
    /// Only used when `--cluster-id` differs from `--node-id`. Lower values reduce how long a
    /// peer's newly persisted data stays invisible to this node, at the cost of more object store
    /// requests — one LIST scoped to a single prefix, regardless of how many peers there are.
    #[clap(
        long = "file-index-sync-interval",
        visible_alias = "peer-sync-interval",
        env = "INFLUXDB3_FILE_INDEX_SYNC_INTERVAL",
        default_value = "5s",
        action
    )]
    pub peer_sync_interval: humantime::Duration,

    /// How many log appends between rollup snapshots of the cluster file index.
    ///
    /// A rollup collapses the log into a single object holding the live file set, after which the
    /// entries behind it are deleted. This is what keeps the index affordable under compaction —
    /// a merge that replaces a hundred files with one leaves a hundred removals in the log, and
    /// the rollup is what discards them.
    ///
    /// Lower values bound replay work for a starting node and reclaim storage sooner, at the cost
    /// of rewriting the full live set more often. Tune to the deployment's churn rate: a cluster
    /// merging constantly wants a lower value than one that mostly appends.
    #[clap(
        long = "file-index-snapshot-interval",
        env = "INFLUXDB3_FILE_INDEX_SNAPSHOT_INTERVAL",
        default_value = "500",
        action
    )]
    pub file_index_snapshot_interval: u64,

    /// Address this node binds for serving its un-persisted rows to peers.
    ///
    /// Only used when `--cluster-id` differs from `--node-id`. The address is published to the
    /// shared catalog as this node's `conn_info` so peers can reach it, so it must be reachable
    /// from other nodes — not `127.0.0.1` in a multi-host deployment.
    #[clap(
        long = "cluster-rpc-bind",
        env = "INFLUXDB3_CLUSTER_RPC_BIND",
        default_value = "0.0.0.0:8383",
        action
    )]
    pub cluster_rpc_bind: std::net::SocketAddr,

    /// The roles this node performs in the cluster.
    ///
    /// Accepts a comma-separated list. `core` (the default) both ingests and serves queries, which
    /// is the historical single-node behaviour. `ingest` accepts writes and serves its buffered
    /// rows to querying peers. `query` serves queries only: it refuses writes and publishes no
    /// peer address, so no other node will ask it for buffered rows.
    ///
    /// `ingest,query` is equivalent in capability to `core` but registers both roles explicitly.
    /// `compact` merges small, cold Parquet files belonging to ingest-only peers; it neither
    /// accepts writes nor serves queries. `process` names the processing-engine role, which today
    /// runs regardless of mode. `all` runs every role in one process and compacts its own files
    /// in-process. Anything other than `core` requires `--cluster-id`, and `all`, like `core`,
    /// cannot be combined with other modes.
    #[clap(
        long = "mode",
        env = "INFLUXDB3_MODE",
        value_delimiter = ',',
        default_value = "core",
        value_enum
    )]
    pub mode: Vec<CliNodeMode>,

    #[clap(flatten)]
    pub compaction: CompactionConfig,
}

/// Tunables for the compactor, used only by a node running with `--mode compact`.
///
/// The defaults target the problem compaction exists to solve: `PersistedFiles` holds every file a
/// node has persisted, and [`get_files_filtered`] clones that whole per-table list on every query.
/// Merging ~100 cold files into one keeps both inside the range they were built for.
///
/// [`get_files_filtered`]: influxdb3_write::write_buffer::persisted_files::PersistedFiles::get_files_filtered
#[derive(Debug, Clone, Copy, clap::Parser)]
pub struct CompactionConfig {
    /// How often the compactor scans peers for merge candidates.
    #[clap(
        long = "compact-interval",
        env = "INFLUXDB3_COMPACT_INTERVAL",
        default_value = "5m",
        action
    )]
    pub interval: humantime::Duration,

    /// How old a file's `chunk_time` must be before it is eligible to be merged.
    ///
    /// Gen1 files are immutable once persisted, but a `chunk_time` bucket keeps receiving new files
    /// until the buffer advances past it. Waiting well beyond that point makes the input set for a
    /// merge genuinely closed, so no file is compacted while its bucket is still being filled.
    #[clap(
        long = "compact-min-age",
        env = "INFLUXDB3_COMPACT_MIN_AGE",
        default_value = "20m",
        action
    )]
    pub min_age: humantime::Duration,

    /// Files at or above this size are already large enough and are never used as merge inputs.
    #[clap(
        long = "compact-max-input-size",
        env = "INFLUXDB3_COMPACT_MAX_INPUT_SIZE",
        default_value = "104857600",
        action
    )]
    pub max_input_size_bytes: u64,

    /// Stop adding inputs to a merge once their combined size reaches this much.
    #[clap(
        long = "compact-target-size",
        env = "INFLUXDB3_COMPACT_TARGET_SIZE",
        default_value = "536870912",
        action
    )]
    pub target_size_bytes: u64,

    /// Hard cap on how many files a single merge may consume.
    ///
    /// Bounds both the peak memory of one merge and how much work is lost if it fails partway.
    #[clap(
        long = "compact-max-inputs",
        env = "INFLUXDB3_COMPACT_MAX_INPUTS",
        default_value = "100",
        action
    )]
    pub max_inputs: usize,

    /// How long a merged-away input file survives in object store before deletion.
    ///
    /// A query that resolved the index before the compaction landed still holds paths to the
    /// inputs. This must comfortably exceed the longest query the cluster will run, or such a query
    /// fails on a path it has every reason to believe is valid.
    #[clap(
        long = "compact-input-grace",
        env = "INFLUXDB3_COMPACT_INPUT_GRACE",
        default_value = "1h",
        action
    )]
    pub input_grace: humantime::Duration,
}

impl ClusterConfig {
    /// Validate the node id, resolve the cluster id, and produce the identity the rest of the
    /// crate works from.
    ///
    /// `cluster_id` defaults to `node_id`, so omitting the flag reproduces the single-node layout
    /// and makes the catalog promotion a no-op.
    pub fn resolve(&self, node_id: &str) -> Result<ClusterIdentity> {
        validate_identifier("node-id", node_id)?;
        let cluster_id = match self.cluster_id.as_deref() {
            Some(cluster_id) => {
                validate_identifier("cluster-id", cluster_id)?;
                cluster_id
            }
            None => node_id,
        };

        let modes = resolve_modes(&self.mode, cluster_id != node_id)?;

        Ok(ClusterIdentity {
            node_id: Arc::from(node_id),
            cluster_id: Arc::from(cluster_id),
            catalog_sync_interval: self.catalog_sync_interval.into(),
            peer_sync_interval: self.peer_sync_interval.into(),
            file_index_snapshot_interval: self.file_index_snapshot_interval,
            cluster_rpc_bind: self.cluster_rpc_bind,
            modes,
            compaction: self.compaction,
        })
    }
}

/// Validate the requested roles and map them onto catalog [`NodeMode`]s.
///
/// The catalog accepts any combination without complaint — `From<Vec<NodeMode>> for NodeModes` just
/// collects into a set — so every meaningful check has to happen here, before a node registers
/// itself into a state nothing downstream will question.
fn resolve_modes(requested: &[CliNodeMode], is_clustered: bool) -> Result<Vec<NodeMode>> {
    // An empty list registers a node that is neither an ingester nor a querier. The catalog permits
    // it and it starts up cleanly, then silently does nothing useful.
    if requested.is_empty() {
        return Err(ClusterError::InvalidMode {
            reason: "at least one mode is required".to_string(),
        });
    }

    let mut modes: Vec<CliNodeMode> = Vec::with_capacity(requested.len());
    for mode in requested {
        if !modes.contains(mode) {
            modes.push(*mode);
        }
    }

    // `Core` is its own catalog variant, not a union: it satisfies none of `is_ingester()` /
    // `is_querier()`. Combining it with a real role would produce a node whose capabilities depend
    // on which predicate a given call site happens to use.
    if modes.contains(&CliNodeMode::Core) && modes.len() > 1 {
        return Err(ClusterError::InvalidMode {
            reason: "'core' already covers both ingest and query, so it cannot be combined with \
                     other modes; use 'ingest,query' to name both explicitly"
                .to_string(),
        });
    }

    // `All` is a union of every role, so combining it with any of them is redundant at best and
    // contradictory at worst. Checked before `Compact` below so `all,compact` reports this message.
    if modes.contains(&CliNodeMode::All) && modes.len() > 1 {
        return Err(ClusterError::InvalidMode {
            reason: "'all' already covers every role, so it cannot be combined with other modes"
                .to_string(),
        });
    }

    // Compaction is deliberately a dedicated role rather than something folded into an ingester.
    //
    // The reason is not just resource contention. A node's own `PersistedFiles` is an in-memory
    // list of the Parquet it has persisted, and nothing refreshes it from object store while the
    // process runs. A `--mode ingest,compact` node would have to keep that list coherent with its
    // own rewrites through a second path; keeping those roles apart means the compactor only ever
    // touches prefixes it does not serve from. `all` is the deliberate exception: it both ingests
    // and compacts, but applies its own merges through the very same `PersistedFiles::apply_compaction`
    // path a remote owner uses (`rpc::server::apply_compaction_locally`), so there is exactly one
    // path, not two.
    if modes.contains(&CliNodeMode::Compact) && modes.len() > 1 {
        return Err(ClusterError::InvalidMode {
            reason:
                "'compact' is a dedicated role: a compacting node accepts no writes and serves \
                     no queries, so it cannot be combined with other modes"
                    .to_string(),
        });
    }

    if !is_clustered && modes != [CliNodeMode::Core] {
        return Err(ClusterError::InvalidMode {
            reason: "roles other than 'core' require --cluster-id, since a node with no peers \
                     cannot delegate the roles it gives up"
                .to_string(),
        });
    }

    Ok(modes.into_iter().map(NodeMode::from).collect())
}

/// This node's resolved place in the cluster.
#[derive(Debug, Clone)]
pub struct ClusterIdentity {
    node_id: Arc<str>,
    cluster_id: Arc<str>,
    catalog_sync_interval: Duration,
    peer_sync_interval: Duration,
    file_index_snapshot_interval: u64,
    cluster_rpc_bind: std::net::SocketAddr,
    modes: Vec<NodeMode>,
    compaction: CompactionConfig,
}

impl ClusterIdentity {
    pub fn node_id(&self) -> &Arc<str> {
        &self.node_id
    }

    pub fn cluster_id(&self) -> &Arc<str> {
        &self.cluster_id
    }

    pub fn catalog_sync_interval(&self) -> Duration {
        self.catalog_sync_interval
    }

    /// Log appends between rollup snapshots of the file index.
    pub fn file_index_snapshot_interval(&self) -> u64 {
        self.file_index_snapshot_interval
    }

    pub fn peer_sync_interval(&self) -> Duration {
        self.peer_sync_interval
    }

    pub fn cluster_rpc_bind(&self) -> std::net::SocketAddr {
        self.cluster_rpc_bind
    }

    /// The roles this node registers in the shared catalog.
    pub fn modes(&self) -> Vec<NodeMode> {
        self.modes.clone()
    }

    /// Whether this node accepts writes and serves its buffer to peers.
    ///
    /// `Core` counts: it has no separate role, but it buffers writes exactly like an ingester.
    pub fn ingests(&self) -> bool {
        self.modes
            .iter()
            .any(|m| matches!(m, NodeMode::Core | NodeMode::Ingest | NodeMode::All))
    }

    /// Whether this node serves queries.
    pub fn queries(&self) -> bool {
        self.modes
            .iter()
            .any(|m| matches!(m, NodeMode::Core | NodeMode::Query | NodeMode::All))
    }

    /// Whether this node merges cold Parquet files belonging to its peers.
    pub fn compacts(&self) -> bool {
        self.modes
            .iter()
            .any(|m| matches!(m, NodeMode::Compact | NodeMode::All))
    }

    /// Whether this node runs the processing engine as a named role.
    ///
    /// Nothing branches on this yet — the engine is constructed regardless of mode — but it keeps
    /// the predicate set complete alongside [`ingests`](Self::ingests) and friends.
    pub fn processes(&self) -> bool {
        self.modes
            .iter()
            .any(|m| matches!(m, NodeMode::Process | NodeMode::All))
    }

    /// Whether this node runs every role in one process (`--mode all`).
    ///
    /// The distinguishing effect is that it compacts its **own** Parquet in-process rather than
    /// handing merges to a peer over RPC.
    pub fn is_all(&self) -> bool {
        self.modes.iter().any(|m| matches!(m, NodeMode::All))
    }

    /// Tunables for the compaction loop. Meaningful only when [`compacts`](Self::compacts).
    pub fn compaction(&self) -> &CompactionConfig {
        &self.compaction
    }

    /// The address peers should use to reach this node, published as `conn_info`.
    ///
    /// Only meaningful for a node that [`ingests`](Self::ingests) — a query-only node has no
    /// buffered rows to serve, so it publishes nothing and peers never dial it.
    pub fn conn_info(&self) -> String {
        self.cluster_rpc_bind.to_string()
    }

    /// Whether this node shares its catalog with others.
    ///
    /// False when `--cluster-id` was omitted or set equal to the node id, in which case the server
    /// takes the original single-node code paths and none of the cluster machinery is constructed.
    pub fn is_clustered(&self) -> bool {
        self.cluster_id != self.node_id
    }
}

/// Validate an identifier that is used as an object store path prefix.
///
/// Nothing in core validates `--node-id` today, so an arbitrary string flows straight into object
/// store paths. Both ids are path components, and a value containing `/` would silently create
/// nested prefixes and break the path parsing in `influxdb3_write::paths` that recovers the node id
/// from the first path segment.
fn validate_identifier(kind: &'static str, value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(ClusterError::InvalidIdentifier {
            kind,
            value: value.to_string(),
            reason: "must not be empty".to_string(),
        });
    }
    if value.len() > MAX_IDENTIFIER_LEN {
        return Err(ClusterError::InvalidIdentifier {
            kind,
            value: value.to_string(),
            reason: format!("must be at most {MAX_IDENTIFIER_LEN} characters"),
        });
    }
    if let Some(c) = value
        .chars()
        .find(|c| !c.is_ascii_alphanumeric() && *c != '-' && *c != '_')
    {
        return Err(ClusterError::InvalidIdentifier {
            kind,
            value: value.to_string(),
            reason: format!(
                "contains invalid character {c:?}; only ASCII alphanumerics, '-' and '_' are allowed"
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    /// Build a `ClusterConfig` through clap so the flag definitions are exercised too.
    fn config(args: &[&str]) -> ClusterConfig {
        #[derive(Debug, Parser)]
        struct Wrapper {
            #[clap(flatten)]
            cluster: ClusterConfig,
        }
        let mut argv = vec!["test"];
        argv.extend_from_slice(args);
        Wrapper::parse_from(argv).cluster
    }

    #[test]
    fn accepts_reasonable_identifiers() {
        let cfg = config(&[]);
        for id in ["host01", "node-1", "my_cluster", "A1", &"a".repeat(64)] {
            assert!(cfg.resolve(id).is_ok(), "expected {id:?} to be accepted");
        }
    }

    #[test]
    fn rejects_path_separators() {
        // A `/` would silently create a nested object store prefix and break the path parsing that
        // recovers the node id from the first path segment.
        let err = config(&[]).resolve("a/b").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("invalid --node-id"), "got: {msg}");
        assert!(msg.contains("invalid character"), "got: {msg}");
    }

    #[test]
    fn rejects_empty_and_overlong() {
        let err = config(&[]).resolve("").unwrap_err();
        assert!(err.to_string().contains("must not be empty"), "got: {err}");

        let too_long = "a".repeat(65);
        let err = config(&[]).resolve(&too_long).unwrap_err();
        assert!(err.to_string().contains("at most 64"), "got: {err}");
    }

    #[test]
    fn cluster_id_defaults_to_node_id_and_is_not_clustered() {
        // Omitting --cluster-id must reproduce the historical single-node layout so that the
        // catalog promotion is a no-op and existing deployments are untouched.
        let identity = config(&[]).resolve("host01").unwrap();
        assert_eq!(identity.cluster_id().as_ref(), "host01");
        assert!(
            !identity.is_clustered(),
            "single-node must not enable cluster machinery"
        );
    }

    #[test]
    fn explicit_cluster_id_is_used_and_validated() {
        let identity = config(&["--cluster-id", "mycluster"])
            .resolve("host01")
            .unwrap();
        assert_eq!(identity.cluster_id().as_ref(), "mycluster");
        assert!(identity.is_clustered());

        assert!(
            config(&["--cluster-id", "bad/id"])
                .resolve("host01")
                .is_err(),
            "cluster-id must be validated too"
        );
    }

    #[test]
    fn cluster_id_equal_to_node_id_is_not_clustered() {
        // Setting them equal explicitly is the same as omitting the flag.
        let identity = config(&["--cluster-id", "host01"])
            .resolve("host01")
            .unwrap();
        assert!(!identity.is_clustered());
    }

    #[test]
    fn mode_defaults_to_core_and_does_both() {
        // Omitting --mode must reproduce the historical behaviour exactly: a node that both
        // ingests and answers queries.
        let identity = config(&[]).resolve("host01").unwrap();
        assert_eq!(identity.modes(), vec![NodeMode::Core]);
        assert!(identity.ingests());
        assert!(identity.queries());
    }

    #[test]
    fn single_roles_parse_and_set_only_their_own_capability() {
        let ingest = config(&["--cluster-id", "c", "--mode", "ingest"])
            .resolve("host01")
            .unwrap();
        assert_eq!(ingest.modes(), vec![NodeMode::Ingest]);
        assert!(ingest.ingests());
        assert!(
            !ingest.queries(),
            "an ingest-only node must not claim query"
        );

        let query = config(&["--cluster-id", "c", "--mode", "query"])
            .resolve("host01")
            .unwrap();
        assert_eq!(query.modes(), vec![NodeMode::Query]);
        assert!(query.queries());
        assert!(
            !query.ingests(),
            "a query-only node must not claim ingest, or peers will RPC it for rows it never has"
        );
    }

    #[test]
    fn both_roles_can_be_named_explicitly() {
        let identity = config(&["--cluster-id", "c", "--mode", "ingest,query"])
            .resolve("host01")
            .unwrap();
        assert_eq!(identity.modes(), vec![NodeMode::Ingest, NodeMode::Query]);
        assert!(identity.ingests() && identity.queries());
    }

    #[test]
    fn duplicate_modes_are_deduped() {
        let identity = config(&["--cluster-id", "c", "--mode", "ingest,ingest"])
            .resolve("host01")
            .unwrap();
        assert_eq!(identity.modes(), vec![NodeMode::Ingest]);
    }

    #[test]
    fn core_cannot_be_combined_with_other_roles() {
        // `Core` satisfies neither is_ingester() nor is_querier() in the catalog, so mixing it with
        // a real role yields a node whose capability depends on which predicate is consulted.
        let err = config(&["--cluster-id", "c", "--mode", "core,query"])
            .resolve("host01")
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("invalid --mode"), "got: {msg}");
        assert!(msg.contains("cannot be combined"), "got: {msg}");
    }

    #[test]
    fn compact_is_a_dedicated_role() {
        let identity = config(&["--cluster-id", "c", "--mode", "compact"])
            .resolve("host01")
            .unwrap();
        assert_eq!(identity.modes(), vec![NodeMode::Compact]);
        assert!(identity.compacts());

        // A compactor accepts no writes and serves no queries, so it publishes no peer address —
        // nothing in the cluster ever dials it.
        assert!(!identity.ingests(), "a compactor must not accept writes");
        assert!(!identity.queries(), "a compactor must not serve queries");
    }

    #[test]
    fn compact_cannot_be_combined_with_other_roles() {
        // Folding compaction into a node that serves queries would leave that node answering from
        // an in-memory file list its own rewrites had invalidated.
        for modes in ["compact,ingest", "compact,query", "core,compact"] {
            let err = config(&["--cluster-id", "c", "--mode", modes])
                .resolve("host01")
                .unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("invalid --mode"), "{modes}: {msg}");
            assert!(msg.contains("cannot be combined"), "{modes}: {msg}");
        }
    }

    #[test]
    fn compaction_tunables_have_defaults() {
        let identity = config(&["--cluster-id", "c", "--mode", "compact"])
            .resolve("host01")
            .unwrap();
        let compaction = identity.compaction();
        assert_eq!(compaction.max_inputs, 100);
        assert_eq!(compaction.target_size_bytes, 512 * 1024 * 1024);
        assert_eq!(compaction.max_input_size_bytes, 100 * 1024 * 1024);
        assert_eq!(
            Duration::from(compaction.interval),
            Duration::from_secs(300)
        );
        assert_eq!(
            Duration::from(compaction.input_grace),
            Duration::from_secs(3600)
        );
    }

    #[test]
    fn a_lone_compactor_needs_a_cluster() {
        // With no peers there is nothing whose files it may merge, and it never merges its own.
        let err = config(&["--mode", "compact"])
            .resolve("host01")
            .unwrap_err();
        assert!(
            err.to_string().contains("require --cluster-id"),
            "got: {err}"
        );
    }

    #[test]
    fn empty_mode_list_is_rejected() {
        // The catalog accepts an empty mode vec and produces a node that is neither an ingester nor
        // a querier — it starts cleanly and then does nothing.
        assert!(resolve_modes(&[], true).is_err());
    }

    #[test]
    fn roles_require_a_cluster() {
        // A standalone --mode query node could never be written to and has no peers to read from.
        let err = config(&["--mode", "query"]).resolve("host01").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("require --cluster-id"), "got: {msg}");

        // ...but plain --mode core without a cluster stays valid, since it is the default.
        assert!(config(&["--mode", "core"]).resolve("host01").is_ok());
    }

    #[test]
    fn sync_intervals_have_defaults() {
        let identity = config(&[]).resolve("host01").unwrap();
        assert_eq!(identity.catalog_sync_interval(), Duration::from_secs(1));
        assert_eq!(identity.peer_sync_interval(), Duration::from_secs(5));
    }

    #[test]
    fn all_mode_carries_every_capability() {
        let identity = config(&["--cluster-id", "c", "--mode", "all"])
            .resolve("host01")
            .unwrap();
        assert_eq!(identity.modes(), vec![NodeMode::All]);
        assert!(identity.ingests());
        assert!(identity.queries());
        assert!(identity.compacts());
        assert!(identity.processes());
        assert!(identity.is_all());
        assert!(identity.is_clustered());
    }

    #[test]
    fn all_mode_needs_a_cluster() {
        // `all` is a non-`core` role like any other: a lone all-in-one node still names its own
        // cluster of one.
        let err = config(&["--mode", "all"]).resolve("host01").unwrap_err();
        assert!(
            err.to_string().contains("require --cluster-id"),
            "got: {err}"
        );
    }

    #[test]
    fn all_cannot_be_combined_with_other_roles() {
        for modes in ["all,ingest", "all,query", "all,compact", "core,all"] {
            let err = config(&["--cluster-id", "c", "--mode", modes])
                .resolve("host01")
                .unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("invalid --mode"), "{modes}: {msg}");
            assert!(msg.contains("cannot be combined"), "{modes}: {msg}");
            // `all,compact` must report the `all` message, not the `compact` one — the `all` check
            // runs first.
            if modes == "all,compact" {
                assert!(msg.contains("'all' already covers every role"), "{modes}: {msg}");
            }
        }
    }

    #[test]
    fn process_is_a_named_role_requiring_a_cluster() {
        let identity = config(&["--cluster-id", "c", "--mode", "process"])
            .resolve("host01")
            .unwrap();
        assert_eq!(identity.modes(), vec![NodeMode::Process]);
        assert!(identity.processes());
        assert!(!identity.ingests(), "process alone is not an ingester");
        assert!(!identity.queries(), "process alone is not a querier");
        assert!(!identity.compacts(), "process alone is not a compactor");

        let err = config(&["--mode", "process"]).resolve("host01").unwrap_err();
        assert!(
            err.to_string().contains("require --cluster-id"),
            "got: {err}"
        );
    }
}
