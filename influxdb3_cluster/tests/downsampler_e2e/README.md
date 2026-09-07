# `downsampler_e2e` — process-level cluster Processing Engine test

The multi-process counterpart to the in-crate `pe_placement` unit suite (roadmap
"gap E"). Driven by [`../downsampler_e2e.rs`](../downsampler_e2e.rs), marked
`#[ignore]` because it spawns three servers and needs a built binary + Python.

## What it proves

A real 3-node `feat/cluster-mode` cluster on localhost, one shared local object store:

| node | mode | role |
|------|------|------|
| node-a | `ingest` | raw writes (6h slots 00:00 / 12:00) |
| node-b | `ingest,query` + `--plugin-dir` | runs the plugins |
| node-c | `query` | independent verifier — runs no triggers |

Raw `home` data (2023–2025, every 6h) is split so **every daily bucket has 2 points
from node-a and 2 from node-b**. Then:

1. **Cluster-wide read** — the stock `downsampler` (vendored, unmodified), pinned
   `--trigger-arguments node_spec=nodes:node-b`, produces `home_daily` where *every*
   bucket has `record_count == 4` and `SUM(record_count) == 8768` (all raw points).
   `record_count == 2` would mean the plugin only read node-b's own WAL.
2. **Calendar rollups** — `calendar_rollup.py` cascades `home_daily → home_monthly →
   home_yearly` with `date_trunc`; Feb `record_count` is `112 / 116 / 112` for
   2023 / 2024(leap) / 2025, and `SUM(record_count)` is conserved at every level.
3. **Placement** — `POST /api/v3/engine/downsample` returns 200 on node-b, 404 on
   node-a and node-c, and both non-target nodes log `not placed on this node`.

All assertions query **node-c**, so a pass also shows the rollups are in the shared
cluster and visible from a node that never ran a plugin.

## Running it

```sh
cargo build --bin influxdb3
cargo test -p influxdb3_cluster --test downsampler_e2e -- --ignored --nocapture
```

Requirements:

* the `influxdb3` binary — found via `$INFLUXDB3_BIN`, else
  `<target>/{debug,release}/influxdb3`;
* a `python3` >= 3.11 on `PATH` (the downsampler imports `tomllib`); the test creates
  an offline `venv` under a temp dir — no `pip`, no network.

Everything runs in a `tempfile::TempDir` that is removed on exit; the three child
processes are killed by a `Drop` guard even if an assertion panics.

## Files

| file | notes |
|------|-------|
| `vendor/downsampler.py` | verbatim copy of the official plugin — see `vendor/PROVENANCE` |
| `vendor/LICENSE` | Apache-2.0, from the `influxdb3_plugins` repo |
| `calendar_rollup.py` | ours: `date_trunc` month/year rollups carrying `temp_sum` + `record_count` |
| `seed.py` | raw-data generator; node URLs from argv or `$NODE_A_URL` / `$NODE_B_URL` |

## Refreshing the vendored downsampler

```sh
SHA=<new upstream commit>
curl -fsSL "https://raw.githubusercontent.com/influxdata/influxdb3_plugins/$SHA/influxdata/downsampler/downsampler.py" \
  -o influxdb3_cluster/tests/downsampler_e2e/vendor/downsampler.py
shasum -a 256 influxdb3_cluster/tests/downsampler_e2e/vendor/downsampler.py
```

Then update `DOWNSAMPLER_SHA256` in `downsampler_e2e.rs` and the `commit` / `sha256` /
`retrieved` lines in `vendor/PROVENANCE`, and re-run the test — the HTTP `calculations`
argument shape (`[["temp","sum"]]`) or the aggregate column names may have changed.
