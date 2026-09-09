#!/usr/bin/env bash
# Start a 5-node feat/cluster-mode cluster on localhost and leave it running so you can
# drive it yourself over the HTTP API.
#
#   2x --mode ingest    accept writes, serve buffered rows to query peers
#   2x --mode query     serve queries only (refuse writes), read cluster-wide
#   1x --mode compact   merge the ingesters' small cold gen1 Parquet
#
# All five share one local object store (one --data-dir) and one --cluster-id.
#
# Usage:
#   docs/cluster/scripts/cluster-playground.sh [DATA_DIR]
#   RESET=1 docs/cluster/scripts/cluster-playground.sh      # wipe DATA_DIR first
#   BIN=/path/to/varvedb docs/cluster/scripts/cluster-playground.sh
#
# Ctrl-C shuts every node down cleanly. Data and logs persist under DATA_DIR
# between runs unless RESET=1.
set -euo pipefail

BIN="${BIN:-$PWD/target/debug/varvedb}"
DATA_DIR="${1:-$PWD/.cluster-playground}"
CLUSTER_ID="${CLUSTER_ID:-playground}"

[ -x "$BIN" ] || { echo "varvedb binary not found/executable: $BIN" >&2
                   echo "build it with: cargo build --bin varvedb" >&2; exit 1; }

if [ "${RESET:-0}" = "1" ]; then rm -rf "$DATA_DIR"; fi
LOGS="$DATA_DIR/logs"
mkdir -p "$LOGS"

# name  mode     http  rpc
NODES=(
  "ingest-1  ingest   8181  8281"
  "ingest-2  ingest   8182  8282"
  "query-1   query    8183  8283"
  "query-2   query    8184  8284"
  "compact-1 compact  8185  8285"
)

common=(serve
        --object-store file --data-dir "$DATA_DIR"
        --cluster-id "$CLUSTER_ID"
        --without-auth
        --catalog-sync-interval 1s
        --file-index-sync-interval 2s
        --gen1-duration 1m
        --wal-flush-interval 1s
        --wal-files-per-snapshot 1)

PIDS=()
cleanup() {
  echo
  echo "shutting down..."
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -f "$DATA_DIR/cluster.pids"
  echo "stopped. data + logs kept under: $DATA_DIR"
}
trap cleanup EXIT INT TERM

echo "binary : $BIN"
echo "data   : $DATA_DIR   (cluster-id: $CLUSTER_ID)"
echo

for spec in "${NODES[@]}"; do
  # shellcheck disable=SC2086
  set -- $spec
  name=$1 mode=$2 http=$3 rpc=$4
  args=("${common[@]}" --node-id "$name" --mode "$mode"
        --http-bind "127.0.0.1:$http" --cluster-rpc-bind "127.0.0.1:$rpc")
  if [ "$mode" = "compact" ]; then
    args+=(--compact-interval 30s --compact-min-age 2m --compact-input-grace 5m)
  fi
  echo "starting $name  (--mode $mode)  http :$http  rpc :$rpc"
  "$BIN" "${args[@]}" >"$LOGS/$name.log" 2>&1 &
  PIDS+=($!)
  sleep 1
done

# record pids so cluster-shutdown.sh (or a later shell) can stop this cluster
printf '%s\n' "${PIDS[@]}" > "$DATA_DIR/cluster.pids"

echo
echo "waiting for /health ..."
for spec in "${NODES[@]}"; do
  # shellcheck disable=SC2086
  set -- $spec
  http=$3
  for i in $(seq 1 60); do
    curl -fsS "http://127.0.0.1:$http/health" >/dev/null 2>&1 && { echo "  :$http up"; break; }
    sleep 1
    [ "$i" = 60 ] && { echo "  :$http never came up — see $LOGS/$1.log" >&2; exit 1; }
  done
done

# a database to write into (a write would auto-create it too)
"$BIN" create database sensors --host "http://127.0.0.1:8181" >/dev/null 2>&1 || true

echo
echo "cluster membership (from query-1):"
curl -fsS --get "http://127.0.0.1:8183/api/v3/query_sql" \
  --data-urlencode "db=_internal" \
  --data-urlencode "q=SELECT node_id, mode, state FROM system.nodes ORDER BY node_id" \
  --data-urlencode "format=jsonl" || true

cat <<'EOF'

────────────────────────────────────────────────────────────────────────────
cluster is up. endpoints:

  ingest  ->  http://127.0.0.1:8181   http://127.0.0.1:8182   (writes)
  query   ->  http://127.0.0.1:8183   http://127.0.0.1:8184   (reads)
  compact ->  http://127.0.0.1:8185   (no client API; merges in the background)

── write line protocol to an ingest node ──────────────────────────────────
  curl -sS "http://127.0.0.1:8181/api/v3/write_lp?db=sensors&precision=second" \
    --data-binary "cpu,host=a,region=west usage=0.62,temp=48 $(date +%s)"

  # spread load across both ingesters
  curl -sS "http://127.0.0.1:8182/api/v3/write_lp?db=sensors&precision=second" \
    --data-binary "cpu,host=b,region=east usage=0.55,temp=51 $(date +%s)"

── query from a query node (SQL) ──────────────────────────────────────────
  curl -sS --get "http://127.0.0.1:8183/api/v3/query_sql" \
    --data-urlencode "db=sensors" \
    --data-urlencode "q=SELECT host, count(*) n, avg(usage) FROM cpu GROUP BY host" \
    --data-urlencode "format=jsonl"

  # the other query node sees the same data
  curl -sS --get "http://127.0.0.1:8184/api/v3/query_sql" \
    --data-urlencode "db=sensors" \
    --data-urlencode "q=SELECT * FROM cpu ORDER BY time DESC LIMIT 5" \
    --data-urlencode "format=jsonl"

── query with InfluxQL ───────────────────────────────────────────────────
  curl -sS --get "http://127.0.0.1:8183/api/v3/query_influxql" \
    --data-urlencode "db=sensors" \
    --data-urlencode "q=SELECT mean(usage) FROM cpu WHERE time > now() - 1h GROUP BY time(1m)" \
    --data-urlencode "format=jsonl"

── a write to a query node is refused (HTTP 421) ─────────────────────────
  curl -i -sS "http://127.0.0.1:8183/api/v3/write_lp?db=sensors" --data-binary "cpu x=1"

── watch compaction ─────────────────────────────────────────────────────
  tail -f DATA_DIR/logs/compact-1.log        # look for "compacted files"
  find DATA_DIR -name '*.parquet'            # file count drops as merges land

Ctrl-C to stop all five nodes. From another shell:
  docs/cluster/scripts/cluster-shutdown.sh DATA_DIR
────────────────────────────────────────────────────────────────────────────
EOF
echo "(replace DATA_DIR above with: $DATA_DIR)"
echo
echo "logs: tail -f $LOGS/*.log"
echo "running. Ctrl-C to stop (or run cluster-shutdown.sh '$DATA_DIR' elsewhere)."
wait
