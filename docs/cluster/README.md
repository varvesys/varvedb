# Cluster docs

`feat/cluster-mode` runs the ingest / query / compact / process roles as
separate node modes over one shared object store and catalog.

- [cluster-playground.md](cluster-playground.md) — start a 5-node cluster
  (2 ingest + 2 query + 1 compact) and drive it yourself over the HTTP API;
  includes [`scripts/cluster-playground.sh`](scripts/cluster-playground.sh) and
  [`scripts/cluster-shutdown.sh`](scripts/cluster-shutdown.sh), with worked
  write/query examples.
- [live-test-node-modes.md](live-test-node-modes.md) — hands-on three-process
  walkthrough (write routing, cluster-wide reads, cross-node compaction), with a
  runnable script under [`scripts/`](scripts/).

Related:

- [`influxdb3_cluster/tests/downsampler_e2e`](../../influxdb3_cluster/tests/downsampler_e2e/README.md)
  — automated multi-process e2e for cluster-wide Processing Engine placement.
- [`docs/processing_engine/architecture.md`](../processing_engine/architecture.md)
  — the `TriggerPlacement` seam and per-flow sequence diagrams.
