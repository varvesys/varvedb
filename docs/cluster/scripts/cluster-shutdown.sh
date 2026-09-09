#!/usr/bin/env bash
# Stop a cluster started by cluster-playground.sh.
#
# Useful when the playground was backgrounded or its terminal is gone (a
# foreground playground already stops every node on Ctrl-C).
#
# Usage:
#   docs/cluster/scripts/cluster-shutdown.sh [DATA_DIR]
#   CLUSTER_ID=playground docs/cluster/scripts/cluster-shutdown.sh
#
# Stops nodes recorded in DATA_DIR/cluster.pids, then sweeps for any leftover
# `varvedb serve` process for this --data-dir / --cluster-id. Data and logs
# under DATA_DIR are left untouched.
set -euo pipefail

DATA_DIR="${1:-$PWD/.cluster-playground}"
CLUSTER_ID="${CLUSTER_ID:-playground}"
PIDFILE="$DATA_DIR/cluster.pids"

stopped=0
term_wait_kill() { # term_wait_kill <pid...>
  local pids=("$@") p
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null && stopped=$((stopped + 1)) || true; done
  for _ in $(seq 1 20); do
    local alive=0
    for p in "${pids[@]}"; do kill -0 "$p" 2>/dev/null && alive=1; done
    [ "$alive" = 0 ] && return
    sleep 0.5
  done
  for p in "${pids[@]}"; do kill -9 "$p" 2>/dev/null || true; done
}

if [ -f "$PIDFILE" ]; then
  echo "stopping pids from $PIDFILE ..."
  PIDS=()
  while read -r p; do [ -n "$p" ] && PIDS+=("$p"); done < "$PIDFILE"
  [ "${#PIDS[@]}" -gt 0 ] && term_wait_kill "${PIDS[@]}"
  rm -f "$PIDFILE"
else
  echo "no $PIDFILE — falling back to a process sweep"
fi

# sweep: catch anything the pidfile missed (stale file, manual starts, etc.)
LEFT="$(pgrep -f "varvedb serve.*--cluster-id $CLUSTER_ID" 2>/dev/null || true)"
if [ -n "$LEFT" ]; then
  echo "sweeping leftover varvedb serve --cluster-id $CLUSTER_ID ..."
  # shellcheck disable=SC2086
  term_wait_kill $LEFT
fi

if [ "$stopped" -gt 0 ]; then
  echo "stopped $stopped process(es). data + logs kept under: $DATA_DIR"
else
  echo "nothing running for cluster-id '$CLUSTER_ID' / $DATA_DIR"
fi
