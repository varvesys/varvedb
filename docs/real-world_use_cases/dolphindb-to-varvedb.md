# Migrating from DolphinDB to VarveDB — `tick` / `bar` / `tickoverview` / `baroverview`

Maps the 4-table DolphinDB `dfs://vnpy` schema (vn.py-style market data) onto
VarveDB's measurement / tag / field / time model, with verified `create table`
commands and complete `write_lp` examples for each table.

## The core mapping

| DolphinDB | VarveDB |
|---|---|
| table | measurement |
| `SYMBOL`-typed column you filter/group by | **tag** — dictionary-encoded string, always present, no nulls |
| `DOUBLE`/`INT` payload column | **field** — typed value column. Only `float64`, `int64`, `uint64`, `utf8`, `bool` exist; there is no native timestamp field type |
| one `NANOTIMESTAMP` column, the partition/order-by key | the native `time` column — one per point, nanosecond precision, matches `NANOTIMESTAMP` exactly |
| `HASH(symbol,32) + RANGE(datetime, daily)` partitioning | not something you declare. VarveDB buckets Parquet files by `--gen1-duration` (default 10m) automatically and prunes on tag values + time range — there's no manual symbol-hashing step, and nowhere to put one |

VarveDB's storage engine (Arrow/Parquet/DataFusion) doesn't build a
file-per-tag-series the way legacy TSM-based InfluxDB 1.x did, so a few
hundred/thousand distinct `symbol` values (every contract month across every
product) is a non-issue for tag cardinality here.

### Two timestamp collisions that need a decision

A VarveDB point has exactly **one** `time`. Two of these four tables have more
than one `NANOTIMESTAMP` column, so one has to become a plain field:

- **`tick`** has `datetime` (exchange time) and `localtime` (local receipt
  time). Every query filters/orders by `datetime`, so that's the native
  `time`; `localtime` becomes an integer epoch-nanoseconds field
  (`localtime_ns`). Keep the raw value rather than converting it to a latency
  figure at write time — you can always compute `localtime_ns - time` later,
  but you can't get the raw value back once it's discarded.
- **`tickoverview` / `baroverview`** have `datetime`, `start`, and `end`. These
  look like append-only snapshots ("what data do we have so far"), and the
  queries don't filter by time at all — so `datetime` (when the snapshot was
  taken) is the native `time`; `start`/`end` become `start_ns`/`end_ns`
  integer fields. To read "the current overview," add
  `ORDER BY time DESC LIMIT 1` to the query.

## Schemas

### `tick`
| | columns |
|---|---|
| tags | `symbol`, `exchange` (`name` too, if you ever filter/group on it — otherwise a `utf8` field is fine) |
| fields (`float64`) | `volume`, `turnover`, `open_interest`, `last_price`, `last_volume`, `limit_up`, `limit_down`, `open_price`, `high_price`, `low_price`, `pre_close`, `bid_price_1..5`, `ask_price_1..5`, `bid_volume_1..5`, `ask_volume_1..5` |
| fields (`int64`) | `localtime_ns` |
| time | `datetime` |

### `bar`
| | columns |
|---|---|
| tags | `symbol`, `exchange`, `interval` |
| fields (`float64`) | `volume`, `turnover`, `open_interest`, `open_price`, `high_price`, `low_price`, `close_price` |
| time | `datetime` |

`interval` (`1m`, `1h`, …) is a small, bounded, filtered-on value — exactly
what a tag is for. Keeping it as a tag column (rather than splitting into
`bar_1m`/`bar_1h` measurements) is the direct translation of the one shared
`bar` table you already have.

### `tickoverview`
| | columns |
|---|---|
| tags | `symbol`, `exchange` |
| fields (`int64`) | `count`, `start_ns`, `end_ns` |
| time | `datetime` |

### `baroverview`
| | columns |
|---|---|
| tags | `symbol`, `exchange`, `interval` |
| fields (`int64`) | `count`, `start_ns`, `end_ns` |
| time | `datetime` |

## Creating the tables

Schema is normally inferred from the first write, but pre-creating these
avoids a type-inference foot-gun: a field that lands as `float64` on the first
write can never later become `int64`. Verified against `varvedb create table
--help`:

`--tags` takes space-separated names; `--fields` takes one comma-separated
`name:type` list (no spaces) — mixing the two styles up is the most likely
transcription error, so this is copy-pasted from a verified run:

```sh
varvedb create table --database vnpy tick \
  --tags symbol exchange \
  --fields volume:float64,turnover:float64,open_interest:float64,last_price:float64,last_volume:float64,limit_up:float64,limit_down:float64,open_price:float64,high_price:float64,low_price:float64,pre_close:float64,bid_price_1:float64,bid_price_2:float64,bid_price_3:float64,bid_price_4:float64,bid_price_5:float64,ask_price_1:float64,ask_price_2:float64,ask_price_3:float64,ask_price_4:float64,ask_price_5:float64,bid_volume_1:float64,bid_volume_2:float64,bid_volume_3:float64,bid_volume_4:float64,bid_volume_5:float64,ask_volume_1:float64,ask_volume_2:float64,ask_volume_3:float64,ask_volume_4:float64,ask_volume_5:float64,localtime_ns:int64

varvedb create table --database vnpy bar \
  --tags symbol exchange interval \
  --fields volume:float64,turnover:float64,open_interest:float64,open_price:float64,high_price:float64,low_price:float64,close_price:float64

varvedb create table --database vnpy tickoverview \
  --tags symbol exchange \
  --fields count:int64,start_ns:int64,end_ns:int64

varvedb create table --database vnpy baroverview \
  --tags symbol exchange interval \
  --fields count:int64,start_ns:int64,end_ns:int64
```

Each prints `Table "vnpy"."<name>" created successfully`. (`--database vnpy`
also auto-creates the database if it doesn't exist yet; a write to a
nonexistent database does too, so this step is optional but avoids the
type-inference foot-gun above.)

## Writing — complete `curl` command per table

Each is a full, self-contained `POST /api/v3/write_lp` call: host, database,
precision, and a complete line-protocol body for one point. Replace
`localhost:8181` with your ingest node's address; `precision=ns` is explicit
rather than relying on magnitude-based auto-detection, since every timestamp
here is a full nanosecond epoch value (matching `NANOTIMESTAMP`).

### `tick`

```sh
curl -sS "http://localhost:8181/api/v3/write_lp?db=vnpy&precision=ns" \
  --data-binary 'tick,symbol=sn2604,exchange=SHFE last_price=6925.0,volume=60,turnover=83100000,open_interest=63162,last_volume=60,limit_up=7200,limit_down=6600,open_price=6920.0,high_price=6930.0,low_price=6915.0,pre_close=6918.0,bid_price_1=6924.0,bid_price_2=6923.0,bid_price_3=6922.0,bid_price_4=6921.0,bid_price_5=6920.0,ask_price_1=6926.0,ask_price_2=6927.0,ask_price_3=6928.0,ask_price_4=6929.0,ask_price_5=6930.0,bid_volume_1=5,bid_volume_2=8,bid_volume_3=3,bid_volume_4=10,bid_volume_5=2,ask_volume_1=3,ask_volume_2=6,ask_volume_3=4,ask_volume_4=1,ask_volume_5=9,localtime_ns=1755000000123456789i 1755000000100000000'
```
`-> HTTP 204`

### `bar`

```sh
curl -sS "http://localhost:8181/api/v3/write_lp?db=vnpy&precision=ns" \
  --data-binary 'bar,symbol=sn2606,exchange=SHFE,interval=1m volume=22,turnover=8150730,open_interest=19591,open_price=370540,high_price=370620,low_price=370400,close_price=370620 1743465600000000000'
```
`-> HTTP 204`

### `tickoverview`

```sh
curl -sS "http://localhost:8181/api/v3/write_lp?db=vnpy&precision=ns" \
  --data-binary 'tickoverview,symbol=sn2610,exchange=SHFE count=1234567i,start_ns=1700000000000000000i,end_ns=1757000000000000000i 1757000000000000000'
```
`-> HTTP 204`

### `baroverview`

```sh
curl -sS "http://localhost:8181/api/v3/write_lp?db=vnpy&precision=ns" \
  --data-binary 'baroverview,symbol=SN99,exchange=CFFEX,interval=1m count=525600i,start_ns=1700000000000000000i,end_ns=1757000000000000000i 1757000000000000000'
```
`-> HTTP 204`

Note the trailing `i` on every integer field (`count=1234567i`,
`localtime_ns=...i`, `start_ns=...i`, `end_ns=...i`) — an unsuffixed number is
a `float64` in line protocol. Omit it once and that field's type is locked in;
a later write with the `i` suffix for the same field then conflicts with the
schema instead of just being redundant.

For batch loads, put many lines (one point per line, same measurement or
mixed) in one request body — `write_lp` accepts multi-line payloads, so a
migration script can batch thousands of ticks/bars per HTTP call instead of
one `curl` per row.

## Your queries, translated

VarveDB's SQL engine (DataFusion) via `POST`/`GET /api/v3/query_sql`:

```sql
-- select count(datetime) from tb_tick where datetime>2026.08.12 and symbol=`SN99 group by symbol,date(datetime)
SELECT symbol, date_trunc('day', time) AS day, count(*) AS n
FROM tick WHERE time > '2026-08-12' AND symbol = 'SN99'
GROUP BY symbol, day ORDER BY day;

-- data_SN = select * from tb_tick where symbol=`sn2604 order by datetime
SELECT * FROM tick WHERE symbol = 'sn2604' ORDER BY time;

-- pt_sn = select * from tb_bar where symbol=`sn2606 and interval=`1m and datetime >= 2025.01.01 and datetime <= 2025.12.31 order by datetime
SELECT * FROM bar WHERE symbol = 'sn2606' AND "interval" = '1m'
  AND time >= '2025-01-01' AND time <= '2025-12-31' ORDER BY time;

-- select * from tb_tick_over where symbol=`sn2610 order by datetime
SELECT * FROM tickoverview WHERE symbol = 'sn2610' ORDER BY time;
-- (latest snapshot only: add ORDER BY time DESC LIMIT 1)

-- select * from tb_bar_over where symbol=`SN99
SELECT * FROM baroverview WHERE symbol = 'SN99' ORDER BY time DESC LIMIT 1;
```

`interval` is a reserved SQL keyword (it's also the name of a literal type,
`INTERVAL '1' DAY`), so as a column name it needs double quotes —
`"interval" = '1m'` — or the parser errors on it as if it were malformed
syntax rather than an unknown-column error. Verified: `WHERE interval = '1m'`
fails with `sql parser error: Expected: an expression, found: =`;
`WHERE "interval" = '1m'` works.

Example, as a full `curl` call:

```sh
curl -sS --get "http://localhost:8181/api/v3/query_sql" \
  --data-urlencode "db=vnpy" \
  --data-urlencode "q=SELECT * FROM tick WHERE symbol = 'sn2604' ORDER BY time" \
  --data-urlencode "format=jsonl"
```

## Python — reading and writing DataFrames (`influxdb3-python`)

DolphinDB's Python API moves data as pandas DataFrames via `s.table()` /
`.toDF()`. VarveDB has the same thing already, as InfluxData's own
**`influxdb3-python`** client — no VarveDB-specific driver needed, because the
client only talks the same two things every example in this doc already
uses: `write_lp` over HTTP for writes, and SQL over **Arrow Flight SQL** (a
real gRPC + Arrow-IPC service, multiplexed on the same `--http-bind` port) for
reads. Every example below was run against a live VarveDB server.

```sh
pip install influxdb3-python
```

```python
from influxdb_client_3 import InfluxDBClient3

client = InfluxDBClient3(
    host="http://localhost:8181",
    database="vnpy",
    token="unused",   # any non-empty string works with --without-auth; use a real token otherwise
)
```

### Writing — one `write_dataframe()` call per table

`write_dataframe(df, measurement, timestamp_column, tags=[...])` maps
column-for-column onto the schemas above: `tags` names the tag columns,
`timestamp_column` names the one column that becomes `time`, and every other
column becomes a field, typed from the DataFrame's own dtype — a `float64`
column becomes a `float64` field, an `int64` column becomes an `int64` field.
This is the one thing to get right for `localtime_ns` / `count` / `start_ns`
/ `end_ns`: give them pandas `int64` dtype (e.g. `pd.array([...],
dtype="int64")`), not plain Python ints in an `object` column and not
`float64` — verified below, they round-trip as `int64` exactly, matching the
`create table ...:int64` schema instead of conflicting with it the way an
un-suffixed `curl` field would.

```python
import pandas as pd

tick_df = pd.DataFrame({
    "time": pd.to_datetime(["2026-09-13 09:30:00.100", "2026-09-13 09:30:00.600"]),
    "symbol": ["sn2604", "sn2604"],
    "exchange": ["SHFE", "SHFE"],
    "last_price": [6925.0, 6926.5],
    "volume": [60.0, 61.0],
    "turnover": [83100000.0, 83200000.0],
    "open_interest": [63162.0, 63163.0],
    "last_volume": [60.0, 5.0],
    "limit_up": [7200.0, 7200.0],
    "limit_down": [6600.0, 6600.0],
    "open_price": [6920.0, 6920.0],
    "high_price": [6930.0, 6930.0],
    "low_price": [6915.0, 6915.0],
    "pre_close": [6918.0, 6918.0],
    "bid_price_1": [6924.0, 6925.0], "bid_price_2": [6923.0, 6924.0],
    "bid_price_3": [6922.0, 6923.0], "bid_price_4": [6921.0, 6922.0], "bid_price_5": [6920.0, 6921.0],
    "ask_price_1": [6926.0, 6927.0], "ask_price_2": [6927.0, 6928.0],
    "ask_price_3": [6928.0, 6929.0], "ask_price_4": [6929.0, 6930.0], "ask_price_5": [6930.0, 6931.0],
    "bid_volume_1": [5.0, 6.0], "bid_volume_2": [8.0, 7.0], "bid_volume_3": [3.0, 4.0],
    "bid_volume_4": [10.0, 9.0], "bid_volume_5": [2.0, 3.0],
    "ask_volume_1": [3.0, 4.0], "ask_volume_2": [6.0, 5.0], "ask_volume_3": [4.0, 3.0],
    "ask_volume_4": [1.0, 2.0], "ask_volume_5": [9.0, 8.0],
    "localtime_ns": pd.array([1755000000123456789, 1755000000623456789], dtype="int64"),
})
client.write_dataframe(tick_df, measurement="tick", timestamp_column="time", tags=["symbol", "exchange"])

bar_df = pd.DataFrame({
    "time": pd.to_datetime(["2026-04-01 00:00:00"]),
    "symbol": ["sn2606"], "exchange": ["SHFE"], "interval": ["1m"],
    "volume": [22.0], "turnover": [8150730.0], "open_interest": [19591.0],
    "open_price": [370540.0], "high_price": [370620.0],
    "low_price": [370400.0], "close_price": [370620.0],
})
client.write_dataframe(bar_df, measurement="bar", timestamp_column="time",
                        tags=["symbol", "exchange", "interval"])

tickoverview_df = pd.DataFrame({
    "time": pd.to_datetime(["2026-09-13 10:00:00"]),
    "symbol": ["sn2610"], "exchange": ["SHFE"],
    "count": pd.array([1234567], dtype="int64"),
    "start_ns": pd.array([1700000000000000000], dtype="int64"),
    "end_ns": pd.array([1757000000000000000], dtype="int64"),
})
client.write_dataframe(tickoverview_df, measurement="tickoverview", timestamp_column="time",
                        tags=["symbol", "exchange"])

baroverview_df = pd.DataFrame({
    "time": pd.to_datetime(["2026-09-13 10:00:00"]),
    "symbol": ["SN99"], "exchange": ["CFFEX"], "interval": ["1m"],
    "count": pd.array([525600], dtype="int64"),
    "start_ns": pd.array([1700000000000000000], dtype="int64"),
    "end_ns": pd.array([1757000000000000000], dtype="int64"),
})
client.write_dataframe(baroverview_df, measurement="baroverview", timestamp_column="time",
                        tags=["symbol", "exchange", "interval"])
```

### Reading — `.query(..., mode="pandas")`

Runs the SQL over Arrow Flight SQL and returns a DataFrame directly — no
manual JSON/CSV parsing, and dtypes come back as the field's real type, not
strings:

```python
tick_df = client.query("SELECT * FROM tick ORDER BY time", mode="pandas")

bar_df = client.query(
    "SELECT * FROM bar WHERE symbol = 'sn2606' AND \"interval\" = '1m' "
    "AND time >= '2025-01-01' AND time <= '2025-12-31' ORDER BY time",
    mode="pandas",
)

tickoverview_df = client.query(
    "SELECT * FROM tickoverview WHERE symbol = 'sn2610' ORDER BY time DESC LIMIT 1",
    mode="pandas",
)

baroverview_df = client.query(
    "SELECT * FROM baroverview WHERE symbol = 'SN99' ORDER BY time DESC LIMIT 1",
    mode="pandas",
)
```

Verified output for `tickoverview` — note `count`/`start_ns`/`end_ns` come
back as genuine `int64`, and `time` as `datetime64[ns]`, exactly matching the
schema, not stringified or coerced to `float64`:

```
count                int64
end_ns               int64
exchange            object
start_ns             int64
symbol              object
time        datetime64[ns]
dtype: object

     count               end_ns exchange               start_ns  symbol                time
0  1234567  1757000000000000000     SHFE  1700000000000000000  sn2610 2026-09-13 10:00:00
```

`mode="polars"` returns a Polars DataFrame instead, via the same Flight SQL
path, if that's your dataframe library of choice; `mode="all"` (the default)
returns the plain row/column result without a DataFrame dependency at all.

## Bulk-loading a historical Parquet export

For migrating an actual DolphinDB export rather than a handful of test rows —
this was verified against the real
[`tickoverview.parquet`](tickoverview.parquet) (291,620 rows, 4.9 MB) and
[`baroverview.parquet`](baroverview.parquet) (379,478 rows, 3.9 MB) sitting
alongside this doc.

**The one fact that makes chunking mandatory, not optional:** the server
rejects any `write_lp` body over 10 MiB (`10,485,760` bytes exactly) with
`HTTP 413 max request size (10485760 bytes) exceeded` — confirmed by actually
sending an oversized payload. A `baroverview`-shaped row is ~120 bytes of
line protocol, so these two files alone would produce ~35–45 MB of line
protocol unchunked — 3–4× over the cap. There's no config flag involved in
avoiding this; the fix is simply to write in batches.

**The rest is exactly the tools already shown above** — `pyarrow` to read the
Parquet, one vectorized `.astype('int64')` per timestamp/count column (not a
per-row loop), and `write_dataframe()` once per batch:

```python
import time
import pyarrow.parquet as pq
from influxdb_client_3 import InfluxDBClient3

client = InfluxDBClient3(host="http://localhost:8181", database="vnpy", token="unused")
BATCH = 20_000   # ~2.4 MB of line protocol per batch for these tables — ~4x headroom under the 10 MiB cap

def migrate(parquet_path, measurement, tags):
    df = pq.read_table(parquet_path).to_pandas()
    df["count"] = df["count"].astype("int64")        # source is int32; VarveDB fields are int64
    df["start_ns"] = df.pop("start").astype("int64")  # timestamp[ns] -> epoch-ns int, same trick as before
    df["end_ns"] = df.pop("end").astype("int64")
    n = len(df)
    t0 = time.time()
    for i in range(0, n, BATCH):
        chunk = df.iloc[i:i + BATCH]
        client.write_dataframe(chunk, measurement=measurement, timestamp_column="datetime", tags=tags)
    print(f"{measurement}: {n} rows in {time.time() - t0:.1f}s")

migrate("docs/migration/tickoverview.parquet", "tickoverview", ["symbol", "exchange"])
migrate("docs/migration/baroverview.parquet", "baroverview", ["symbol", "exchange", "interval"])
```

**Real measured results**, running the above against a live VarveDB instance
with these two actual files:

```
tickoverview: 291620 rows in 15.2s  (~19,200 rows/s), 15 batches
baroverview:  379478 rows in 19.1s  (~19,900 rows/s), 19 batches
```

Verified afterward with `SELECT count(*) FROM tickoverview` /
`SELECT count(*) FROM baroverview` — `291620` and `379478` exactly, matching
the source Parquet row counts with no loss and no duplication.

A couple of things worth knowing before scaling this up:

- **20,000 rows/batch isn't a hard rule** — it's sized off the measured
  ~120 bytes/row for these two tables (2.4 MB/batch, comfortably under the
  10 MiB cap). `tick` has far more fields per row, so its safe batch size is
  smaller; measure your own row width the same way (`len(line_protocol_for_one_batch)
  / batch_size`) rather than reusing 20,000 blindly for a wider table.
- **No need to read Parquet in a streaming fashion** at this size —
  `pq.read_table(...).to_pandas()` loading the whole file (a few MB, a few
  hundred thousand rows) into memory before chunking is simpler and plenty
  fast. For an export dramatically larger than this (tens of millions of
  rows / multiple GB), switch to `ParquetFile.iter_batches()` so you're never
  holding more than one batch in memory at a time — the write loop is
  otherwise identical.
- **This parallelizes trivially for bigger jobs**: `write_lp` has no
  cross-request state, so multiple files (or shards of one file) can be
  migrated concurrently from separate processes/threads against the same
  ingest node, or spread across multiple ingest nodes in a cluster — see the
  [README's Architecture section](../../README.md#architecture).
- **The `bulk_ingest` gRPC service isn't a shortcut here** — its proto
  (`influxdata.iox.bulk_ingest.v1`) exists in this codebase (inherited from
  upstream IOx), but there is no server-side implementation of it, so it
  isn't a usable path today. `write_lp` in batches, as above, is the actual
  efficient method.

For how to stand up the server these calls hit, see
[docs/cluster/](../cluster/) — a single node is enough to start
(`docs/cluster/deploy-single-node-ubuntu.md`), and the same `tick`/`bar`
tables work unchanged if you later split into the ingest/query/compact
cluster roles described in the [README's Architecture section](../../README.md#architecture).
