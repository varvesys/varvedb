#!/usr/bin/env bash
# Live test: a 3-node feat/cluster-mode cluster with the ingest / query / compact roles
# split across separate processes, sharing one local object store.
#
# Proves, end to end:
#   1. role registration      — system.nodes shows each node with only its own mode
#   2. write routing          — --mode query and --mode compact refuse writes (HTTP 421);
#                               only --mode ingest accepts them (HTTP 204)
#   3. cluster-wide reads     — rows written to node-ingest are visible from node-query,
#                               which stores nothing itself (shared cluster file index)
#   4. cross-node compaction  — node-compact merges node-ingest's small cold gen1 Parquet
#                               into one file; the cluster-wide row count is conserved
#
# Usage: node-modes-live-test.sh [path-to-influxdb3-binary]
set -euo pipefail

BIN="${1:-$(pwd)/target/debug/influxdb3}"
[ -x "$BIN" ] || { echo "influxdb3 binary not found/executable: $BIN" >&2; exit 1; }

ROOT="$(mktemp -d "${TMPDIR:-/tmp}/varvedb-nodemodes.XXXXXX")"
DATA="$ROOT/data"; LOGS="$ROOT/logs"
mkdir -p "$DATA" "$LOGS"
echo "workdir: $ROOT"
echo "binary : $BIN ($($BIN --version))"

CLUSTER=testcluster
HTTP_INGEST=18181; HTTP_QUERY=18182; HTTP_COMPACT=18183
RPC_INGEST=18281;  RPC_QUERY=18282;  RPC_COMPACT=18283

PIDS=()
cleanup() {
  echo "--- shutting down ---"
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  echo "logs kept under $LOGS"
}
trap cleanup EXIT

# small gen1 windows + snapshot-per-flush so a handful of writes make several tiny Parquet files
common=(serve --object-store file --data-dir "$DATA" --cluster-id "$CLUSTER"
        --without-auth
        --catalog-sync-interval 1s --file-index-sync-interval 1s
        --gen1-duration 1m --wal-flush-interval 1s --wal-files-per-snapshot 1)

start() { # start <name> <mode> <http> <rpc> [extra args...]
  local name=$1 mode=$2 http=$3 rpc=$4; shift 4
  echo "--- starting $name (--mode $mode) ---"
  "$BIN" "${common[@]}" --node-id "$name" --mode "$mode" \
    --http-bind 127.0.0.1:"$http" --cluster-rpc-bind 127.0.0.1:"$rpc" "$@" \
    >"$LOGS/$name.log" 2>&1 & PIDS+=($!)
}
wait_health() { # wait_health <http>
  for i in $(seq 1 60); do
    curl -fsS "http://127.0.0.1:$1/health" >/dev/null 2>&1 && { echo "  :$1 healthy"; return; }
    sleep 1
  done
  echo "  :$1 never became healthy" >&2; exit 1
}
q() { # q <http> <db> <sql>
  curl -fsS --get "http://127.0.0.1:$1/api/v3/query_sql" \
    --data-urlencode "db=$2" --data-urlencode "q=$3" --data-urlencode "format=jsonl"
}
w() { # w <http> <lp>  ->  prints the HTTP status code
  curl -s -o /dev/null -w '%{http_code}' \
    "http://127.0.0.1:$1/api/v3/write_lp?db=sensors&precision=second" --data-binary "$2"
}
pq() { # pq <node>   ->  list that node's on-disk Parquet, newest layout
  find "$DATA/$1" -name '*.parquet' 2>/dev/null | sed "s#$DATA/##" | sort \
    | while read -r f; do echo "  $f ($(wc -c <"$DATA/$f" | tr -d ' ') bytes)"; done
  echo "  count: $(find "$DATA/$1" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')"
}

##############################################################################
echo; echo "=== start the ingest + query nodes (compact node joins later) ==="
start node-ingest ingest $HTTP_INGEST $RPC_INGEST
sleep 1
start node-query  query  $HTTP_QUERY  $RPC_QUERY
wait_health $HTTP_INGEST
wait_health $HTTP_QUERY

echo; echo "=== 1. role registration (system.nodes, read from node-query) ==="
sleep 2
q $HTTP_QUERY _internal "SELECT node_id, mode, state FROM system.nodes ORDER BY node_id"

echo; echo "=== 2. create database (on node-ingest) ==="
"$BIN" create database sensors --host "http://127.0.0.1:$HTTP_INGEST"

echo; echo "=== 3. write routing — only the ingest node accepts writes ==="
NOW=$(date +%s)
printf '  write -> node-query  : HTTP %s  (expect 421 Misdirected Request)\n' "$(w $HTTP_QUERY  "probe,k=x v=1 $NOW")"
printf '  write -> node-ingest : HTTP %s  (expect 204 No Content)\n'          "$(w $HTTP_INGEST "probe,k=x v=1 $NOW")"

echo; echo "=== 4. seed 8 backdated batches into node-ingest (25 rows each = 200 rows) ==="
for i in $(seq 0 7); do
  TS=$((NOW - 3600 + i * 60))              # 8 distinct 1-minute buckets, ~1h old => already cold
  LP=""
  for r in $(seq 1 25); do
    LP+="room,site=hq,rack=r$r temp=$((20 + (i * 7 + r) % 15)),hum=$((40 + r % 20))i $TS"$'\n'
  done
  echo "  batch $i (ts=$TS): HTTP $(w $HTTP_INGEST "$LP")"
  sleep 2
done
echo "  ...letting the buffer advance so every bucket persists as gen1"
sleep 12

echo; echo "=== 5. cluster-wide read from node-query (which persists nothing itself) ==="
q $HTTP_QUERY sensors \
  "SELECT count(*) rows, count(DISTINCT rack) racks, min(temp) tmin, max(temp) tmax FROM room"
ROWS_BEFORE=$(q $HTTP_QUERY sensors "SELECT count(*) c FROM room" | sed 's/.*"c":\([0-9]*\).*/\1/')
echo "  node-query stores no Parquet of its own:"; pq node-query

echo; echo "=== 6. node-ingest's gen1 Parquet BEFORE the compactor joins ==="
pq node-ingest
BEFORE=$(find "$DATA/node-ingest" -name '*.parquet' | wc -l | tr -d ' ')

echo; echo "=== 7. now start node-compact (--mode compact) with aggressive tunables ==="
start node-compact compact $HTTP_COMPACT $RPC_COMPACT \
  --compact-interval 5s --compact-min-age 1s --compact-input-grace 8s
wait_health $HTTP_COMPACT
printf '  write -> node-compact: HTTP %s  (expect 421 — a compactor takes no writes)\n' \
  "$(w $HTTP_COMPACT "probe,k=x v=1 $NOW")"

echo; echo "=== 8. wait for the merge, then show node-compact's log ==="
for i in $(seq 1 40); do grep -q "compacted files" "$LOGS/node-compact.log" && break; sleep 1; done
grep -E "compacted files|reclaimed compacted-away input files|no compactable peers" \
  "$LOGS/node-compact.log" || true
sleep 10   # let --compact-input-grace elapse so merged-away inputs are reclaimed

echo; echo "=== 9. node-ingest's Parquet AFTER compaction ==="
pq node-ingest
AFTER=$(find "$DATA/node-ingest" -name '*.parquet' | wc -l | tr -d ' ')

echo; echo "=== 10. re-read from node-query — same rows, fewer files ==="
q $HTTP_QUERY sensors "SELECT count(*) rows FROM room"
ROWS_AFTER=$(q $HTTP_QUERY sensors "SELECT count(*) c FROM room" | sed 's/.*"c":\([0-9]*\).*/\1/')
echo
echo "  parquet files under node-ingest/ : $BEFORE  ->  $AFTER"
echo "  cluster-wide row count (node-query): before=$ROWS_BEFORE  after=$ROWS_AFTER"
[ "$ROWS_BEFORE" = "$ROWS_AFTER" ] && [ "$AFTER" -lt "$BEFORE" ] \
  && echo "  PASS: compaction reduced the file count with no row loss" \
  || { echo "  FAIL"; exit 1; }

echo; echo "=== done ==="
