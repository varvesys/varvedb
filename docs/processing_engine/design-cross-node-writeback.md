# Design: cross-node write-back from a plugin (D)

**Status:** design only — not implemented. Targeted at a later ("v3") milestone.

## Problem

`influxdb3_local.write()` / `.write_to_db()` / `.write_sync*()` inside a plugin resolve to
`InProcessWriteEndpoint` ([influxdb3_processing_engine/src/write.rs](../../influxdb3_processing_engine/src/write.rs)),
which calls `Bufferer::write_lp` on **the local node only**. In a cluster a processing node
that is not itself an ingester (`--mode process` alone, or `compact,process`) therefore
cannot persist a rollup — today the operator must pin such triggers to an ingest-capable
node or have the plugin `POST /api/v3/write_lp` to an ingester over HTTP itself
(see `README_processing_engine.md`, "Footguns → Write-back is local").

D removes that constraint: a plugin write from a non-ingest node is routed to a peer
ingester over the existing cluster Flight connection, addressed by the peer's catalog
`conn_info`.

## Shape — a `WriteEndpoint` seam, mirroring `TriggerPlacement`

`WriteEndpoint` ([influxdb3_py_api/src/write.rs](../../influxdb3_py_api/src/write.rs)) is
already a narrow async trait:

```rust
pub trait WriteEndpoint: Debug + Send + Sync + 'static {
    async fn write_lp(&self, target: WriteTarget, lp: String, /* … */) -> Result<_, WriteError>;
}
```

`serve.rs` constructs `InProcessWriteEndpoint` and hands it to
`ProcessingEngineManagerImpl::new_with_options`. The cluster build would instead inject a
`ClusterWriteEndpoint` (in `influxdb3_cluster`) exactly the way it already injects
`ClusterPlacement` via `with_placement` — one guarded line in `serve.rs`, no upstream diff
beyond a `with_write_endpoint` builder if one does not already exist.

```
ClusterWriteEndpoint::write_lp
  ├─ local node ingests?  → delegate to InProcessWriteEndpoint (unchanged fast path)
  └─ otherwise            → pick a target ingester, Flight do_put to its conn_info
```

## Server side — implement `do_put` on `PeerChunkService`

The cluster RPC server ([influxdb3_cluster/src/rpc/server.rs](../../influxdb3_cluster/src/rpc/server.rs))
already exposes `FlightService` on the `conn_info` address but returns
`Status::unimplemented("do_put")`. D implements it:

- accept a stream of `FlightData`; the first message's `flight_descriptor` (or an app-metadata
  header) carries the target database and write options (`no_sync`, precision);
- decode record batches (or raw line protocol — see open questions) and call the node's local
  `Bufferer::write_lp`;
- return `PutResult` once buffered/persisted per `no_sync`, mapping `WriteBufferError` to
  `Status` codes the client can act on (retryable vs not).

This is symmetric with the existing `do_get` (peer buffer fetch) and `do_action` (compaction
notice) handlers on the same service.

## Client side — reuse `PeerClients`

`influxdb3_cluster::rpc::client` already has `PeerClients`: a `conn_info`-keyed Flight channel
pool with invalidate-on-error. `ClusterWriteEndpoint` calls a new `put_writes(clients, addr,
db, lp, opts)` alongside `fetch_peer_batches` / `notify_compaction`, with the same
"invalidate the channel on any failure, next attempt reconnects" handling.

## Target-ingester selection

`ClusterWriteEndpoint` needs "some ingester that can accept this write":

- `Catalog::list_nodes()` filtered to `is_ingest() && is_running() && conn_info.is_some()`;
- prefer a stable pick (e.g. lowest `node_catalog_id`) so retries and successive writes hit
  the same node and its WAL, rather than smearing one logical write across ingesters;
- re-resolve on failure (peer down) and try the next;
- if none qualifies, return a `WriteError` whose message names the cause — this is the same
  class of misconfiguration as a WAL trigger on a non-ingest node.

Optional later: let the plugin name a target (`write_to_db(db, lb, node="host01")`, or a
`node_spec`-style value) for affinity; default stays automatic.

## Auth

Peer Flight calls today are unauthenticated within the cluster boundary. A cluster write is
strictly more sensitive than a buffer read, so D should carry the node's identity/token in
Flight metadata and have `do_put` verify it — ideally the same mechanism compaction RPC
adopts, not a bespoke one. Tracked as a blocker for enabling this outside a trusted network.

## Testing

Unlike E (component-level, in-process), D is a real network path and needs a multi-process
test: a `process`-only node + an `ingest` node sharing an object store, a schedule trigger
whose plugin calls `influxdb3_local.write()`, then assert the rows are queryable from the
ingester. This is the first cluster Processing Engine test that needs the multi-node
`TestServer` harness that does not yet exist — building that harness is part of D's cost.

## Phasing

1. `do_put` on `PeerChunkService` + `put_writes` client fn + round-trip unit coverage of the
   encode/decode and `Status` mapping.
2. `ClusterWriteEndpoint` (local-ingest fast path + single-target remote path) and the
   `serve.rs` injection line.
3. Target selection with failover; the multi-process test.
4. Auth on the write path.
5. Plugin-facing target affinity (optional).
6. README: replace the "Write-back is local" footgun with the new behaviour and its limits.

## Open questions

- **Wire format:** raw line protocol in `FlightData.data_body` (simple, matches the HTTP write
  path, lets the ingester own parsing/partitioning) vs. Arrow batches (consistent with the
  read path, but the plugin already produced line protocol). Leaning line protocol.
- **`no_sync` semantics across the hop:** does `write_sync` wait for the *remote* WAL fsync, or
  just for the remote buffer accept? Must be explicit and documented.
- **Backpressure / size limits:** a plugin can emit a large batch; the `do_put` handler needs
  the same guardrails as the HTTP writer.
- **Ordering vs. the local read path:** a plugin that writes then immediately queries
  (`influxdb3_local.query`) will not see its own cross-node write until the target ingester
  publishes and this node replays. Document as a known non-guarantee.
