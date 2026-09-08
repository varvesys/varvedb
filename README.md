<p align="center">
  <img src="assets/varvedb-mark-512.png" alt="VarveDB" width="128">
</p>

# VarveDB

VarveDB is a fork of [InfluxDB 3 Core](https://github.com/influxdata/influxdb)
that adds multi-node clustering.

A varve is a pair of sediment layers deposited over a single year — a
naturally occurring time series, readable by counting strata. The name
fits how the database stores data: immutable columnar layers accumulating
in object storage, compacted over time, with a shared log recording which
layers are live.

## What's added

- **Cluster file index** — a single CAS-appended log giving every node a
  globally ordered view of the cluster's Parquet files, replacing per-node
  manifest polling.
- **Cluster-aware Processing Engine** — trigger placement across nodes, so
  scheduled and request plugins run where you pin them instead of everywhere.
- **Node modes** — run ingest, query, and compaction as separate tiers, or
  combined on a single node.

Nodes share nothing but object storage and a catalog. There is no
coordinator, no gossip, and no node-to-node dependency for durability.

## Status

Experimental. Not affiliated with or endorsed by InfluxData.

## License

MIT or Apache-2.0, at your option — the same terms as upstream InfluxDB 3 Core.
See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).
"InfluxDB" is a trademark of InfluxData, Inc. VarveDB is an independent fork.
