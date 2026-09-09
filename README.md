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

## Try it

Build the binary, then stand up a local cluster:

```sh
cargo build --bin varvedb
```

**A cluster you drive yourself** — 2 ingest + 2 query + 1 compact node, one
shared local object store, no auth:

```sh
docs/cluster/scripts/cluster-playground.sh          # Ctrl-C to stop all five
```

| role    | endpoints                                          |
|---------|---------------------------------------------------|
| ingest  | `http://127.0.0.1:8181`, `http://127.0.0.1:8182`  |
| query   | `http://127.0.0.1:8183`, `http://127.0.0.1:8184`  |
| compact | background only — merges the ingesters' cold Parquet |

Write line protocol to an **ingest** node:

```sh
curl -sS "http://127.0.0.1:8181/api/v3/write_lp?db=sensors&precision=second" \
  --data-binary "cpu,host=a,region=west usage=0.62,temp=48 $(date +%s)"
# -> HTTP 204
```

Query from a **query** node — it sees data from every ingester, cluster-wide:

```sh
curl -sS --get "http://127.0.0.1:8183/api/v3/query_sql" \
  --data-urlencode "db=sensors" \
  --data-urlencode "q=SELECT host, count(*) n, avg(usage) avg_usage FROM cpu GROUP BY host" \
  --data-urlencode "format=jsonl"
# {"host":"a","n":1,"avg_usage":0.62}
# {"host":"b","n":2,"avg_usage":0.63}
```

A write sent to a query node is refused:

```sh
curl -sS -i "http://127.0.0.1:8183/api/v3/write_lp?db=sensors" --data-binary "cpu x=1"
# HTTP/1.1 421 Misdirected Request
# {"error":"this node runs in query-only mode and does not accept writes; ..."}
```

Stop it from another shell with
[`docs/cluster/scripts/cluster-shutdown.sh`](docs/cluster/scripts/cluster-shutdown.sh).

More, including InfluxQL and how to watch the compactor:
[docs/cluster/cluster-playground.md](docs/cluster/cluster-playground.md).

**A scripted, self-checking walkthrough** of the same roles — write routing,
cluster-wide reads, and cross-node compaction with assertions —
[docs/cluster/live-test-node-modes.md](docs/cluster/live-test-node-modes.md):

```sh
docs/cluster/scripts/node-modes-live-test.sh
```

## Status

Experimental. Not affiliated with or endorsed by InfluxData.

## License

MIT or Apache-2.0, at your option — the same terms as upstream InfluxDB 3 Core.
See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).
"InfluxDB" is a trademark of InfluxData, Inc. VarveDB is an independent fork.
