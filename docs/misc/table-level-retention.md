# Design: table-level retention

**Status:** design only — not implemented.

## Problem

`create database --retention-period 30d mydb` works. The analogous
`create table --database mydb --tags room --fields temp:float64
--retention-period 7d cpu` does not exist — verified, it fails with
`error: unexpected argument '--retention-period' found`. There is no CLI
command, HTTP endpoint, or read-back path that lets one table in a database
carry a retention period different from the rest of that database.

## What's already there — the catalog needs no changes

This is the load-bearing fact for the whole design: every layer below the
CLI/HTTP surface is already built, correct, and wired into the enforcement
loop. Verified by reading the source, not assumed:

- **Catalog mutations exist.**
  `Catalog::set_retention_period_for_table` / `clear_retention_period_for_table`
  ([influxdb3_catalog/src/catalog/versions/v3/catalog.rs:2172-2210](../../influxdb3_catalog/src/catalog/versions/v3/catalog.rs))
  are real, committed catalog log ops (`SetTableRetentionPeriodOp` /
  `ClearTableRetentionPeriodOp`) — the same commit machinery as
  `set_retention_period_for_database` / `clear_retention_period_for_database`
  right above them in the same file.
- **Precedence is already correct.**
  `DatabaseSchema::retention_period_cutoff` /
  `get_retention_cutoff_and_period`
  ([influxdb3_catalog/src/catalog/versions/v3/schema/database.rs:175-224](../../influxdb3_catalog/src/catalog/versions/v3/schema/database.rs))
  already resolve `(_, Some(table)) => table, (Some(db), None) => db, (None, None) => indefinite`
  — a table override wins, otherwise the database's own retention applies,
  otherwise there is none.
- **Enforcement already reads it per-table.**
  `Catalog::get_retention_period_cutoff_map` /
  `get_retention_cutoff_and_period_map`
  ([catalog.rs:1531-1565](../../influxdb3_catalog/src/catalog/versions/v3/catalog.rs))
  are keyed by `(DbId, TableId)` and call straight into the function above.
  This is exactly what `RetentionPeriodHandler`
  ([influxdb3_write/src/retention_period_handler.rs](../../influxdb3_write/src/retention_period_handler.rs))
  consumes every `--retention-check-interval` (default `30m`) to decide which
  gen1 Parquet to delete.
- **Creation-time plumbing exists too.**
  `CreateTableOptions { retention_period: Option<Duration>, field_family_mode }`
  (`catalog.rs:284`) is already accepted by `create_table_opts`.

Nothing above needs to change. The gap is entirely that nothing calls these
functions from outside the catalog crate.

## The gap — three additive pieces, all at the API/CLI edges

### 1. Read-back: `system.tables` + `show retention`

`influxdb3_system_tables/src/tables.rs`'s `tables_schema()` has 8 columns
today — `database_name, table_name, column_count, series_key_columns,
last_cache_count, distinct_cache_count, deleted, hard_deletion_time` — no
retention column.

- Add `retention_period_ns: UInt64, nullable`, populated from
  `table.retention_period`. This is a copy of a pattern that already exists
  one file over: `influxdb3_system_tables/src/databases.rs` builds exactly
  this column (`retention_period_ns`, nullable UInt64) for
  `system.databases` today (schema at line 30, builder at 55, match at
  65-69, array push at 83).
- `varvedb show retention`'s own help text already claims *"Show retention
  policies with effective retention for each table"*
  ([influxdb3/src/commands/show.rs:31](../../influxdb3/src/commands/show.rs)),
  but `show_retention_policies` (`show.rs:229-283`) only ever queries
  `system.databases` — the implementation hasn't caught up to the command's
  own description yet. Once the column above exists, extend that query to
  join `system.databases` + `system.tables` and `COALESCE` table-then-database,
  matching `retention_period_cutoff`'s precedence exactly so the two can never
  disagree.

### 2. Set/clear on an existing table — mirror `update database`

Every piece here is a direct copy of an existing database-level counterpart:

| new (table) | copy of (database) |
|---|---|
| `UpdateTableRequest { db, table, retention_period: Option<Duration> }` in `influxdb3_types/src/http.rs` | `UpdateDatabaseRequest { db, retention_period }` |
| `update_table()` handler in `influxdb3_server/src/http.rs` | `update_database()` (`http.rs:1920-1948`) |
| `(Method::PUT, API_V3_CONFIGURE_TABLE) => update_table` route | `(Method::PUT, API_V3_CONFIGURE_DATABASE) => update_database` |
| `api_v3_configure_table_update(db, table, retention_period)` in `influxdb3_client/src/lib.rs` | `api_v3_configure_db_update` (`lib.rs:506-524`) |
| `Table(UpdateTable)` variant + struct in `influxdb3/src/commands/update.rs` | `Database(UpdateDatabase)` |

The route addition is confirmed conflict-free: `/api/v3/configure/table`
currently only handles `POST` (create) and `DELETE` (delete)
([all_paths.rs:24](../../influxdb3_server/src/all_paths.rs),
[http.rs:2892-2893](../../influxdb3_server/src/http.rs)) — no existing `PUT`.

End state: `varvedb update table --database mydb --table cpu
--retention-period 7d`, and `--retention-period none` to clear — same
grammar as `varvedb update database --database mydb --retention-period
<dur>|none`.

### 3. Set at creation time — parity with `create database --retention-period`

- `CreateTableRequest` (`influxdb3_types/src/http.rs:326`): add
  `retention_period: Option<Duration>`.
- `create_table()` handler (`http.rs:1978-1999`): swap
  `catalog.create_table(...)` for
  `catalog.create_table_opts(.., CreateTableOptions { retention_period, ..Default::default() })`
  — a one-line change, since `CreateTableOptions` already exists and already
  has this field.
- `api_v3_configure_table_create` (`influxdb3_client/src/lib.rs:610`) + the
  CLI's `create/table.rs`: add `--retention-period`, same flag definition
  `create/database`'s already uses.

## Phasing

1. **system.tables column + `show retention` join** — pure read-back, zero
   behavioral risk, and needed as the verification tool for steps 2 and 3.
2. **`update table`** (types → handler → route → client → CLI) — covers the
   primary use case (set, change, or clear retention on a table that already
   exists) on its own.
3. **`create table --retention-period`** — convenience/parity only; strictly
   optional, since step 2 reaches the identical end state via
   create-then-update.

## Testing

- No new catalog-level tests needed — that layer is unmodified and already
  covered.
- New HTTP/CLI integration tests in `influxdb3/tests/cli/`, mirroring the
  existing [`db_retention.rs`](../../influxdb3/tests/cli/db_retention.rs)
  suite one-for-one as `table_retention.rs`: it already has exactly the
  six cases this feature needs —
  `test_create_db_with_retention_period`,
  `test_create_db_without_retention_period`,
  `test_create_db_with_invalid_retention_period`,
  `test_update_db_retention_period`,
  `test_clear_db_retention_period`,
  `test_update_db_retention_after_delete` — each has a direct table-level
  analogue.
- End-to-end (the same style used to verify this design's premise): create a
  table, `update table --retention-period 7d`, confirm `system.tables` shows
  `retention_period_ns` set and `show retention` displays it, then set a
  short retention (e.g. `1m`) on a throwaway table, write backdated data,
  wait past `--retention-check-interval`, and confirm the gen1 Parquet is
  actually deleted and `count(*)` drops — proving the existing enforcement
  loop picks up the table-level value with zero changes to itself.

## Open questions

- ~~**Interaction with `field_family_mode`.**~~ **Resolved: no dependency,
  by construction.** `create_table_with_opts`
  ([transaction.rs:194](../../influxdb3_catalog/src/catalog/versions/v3/transaction.rs))
  pushes exactly one `CreateTable` catalog record carrying both
  `retention_period` and `field_family_mode` — there is no second op, so no
  partial-apply state and no sequencing between the two. Neither field is
  validated inside that function either (only "table already exists" and
  "table count limit" can fail there); an invalid `--retention-period` string
  is already rejected by `humantime::Duration::from_str` at CLI-parse time,
  before a request is even built. Confirmed empirically too: POSTing a
  `retention_period` + `field_family_mode` body in both key orders lands
  identically (`retention_period_ns` set the same either way). Separately,
  `field_family_mode` turned out **not** to be gated behind any flag — it's
  an always-on field defaulting to `Aware`
  ([schema/column.rs:419-423](../../influxdb3_catalog/src/catalog/versions/v3/schema/column.rs)),
  unrelated to `StorageMode::PachaTree` (a different, separate field this fork
  doesn't expose via any CLI/HTTP path either). And moot in practice today
  regardless: `CreateTableRequest` has no `field_family_mode` field at all, so
  every table created via the public API gets `Aware` unconditionally.
- ~~**Token scope.**~~ **Resolved: same gate, and it's actually authentication,
  not a permission check.** `perform_routing`
  ([http.rs:2697](../../influxdb3_server/src/http.rs)) calls
  `authenticate_request` once, before the shared route-dispatch `match` that
  both `update_database` and `update_table` live in; that function passes an
  **empty permissions list** to the authorizer (own code comment: *"in future
  we may be able to derive the permissions based on the incoming request"*) —
  there is no per-resource permission model to be inconsistent with. The only
  endpoints with an extra explicit check are the 3 plugin-file ones
  (`authorize_admin`, `AccessRequest::Admin`), which are *more* strictly
  gated, not a tier `update_table` needs to match. Confirmed live, auth
  enabled: no token → `401` for both `create_database` and `update_table`;
  admin token → `200` for both. Core only ever issues admin tokens
  (`create token --help` has no scoped-token subcommand), so "same admin
  gate" holds in practice.
- **Does `delete table` need a retention-clearing counterpart**, mirroring
  `delete database`'s pattern, or is `update table --retention-period none`
  sufficient? Leaning toward "sufficient, no new `delete`-side path needed,"
  but worth a consistency pass against how the database side settled this.
