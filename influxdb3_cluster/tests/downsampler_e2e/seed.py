#!/usr/bin/env python3
"""
Seed 'home' raw data across a two-ingester cluster for the downsampler e2e test.

2023-01-01 .. 2025-12-31, one point every 6h. The 6h slots are split between the
two ingesters so that EVERY daily bucket contains 2 points from node-a and 2
from node-b -- a downsampler that only read its own node's WAL would see
record_count == 2 per bucket instead of 4.

  slots 00:00, 12:00 -> NODE_A_URL
  slots 06:00, 18:00 -> NODE_B_URL

temp = 20 + slot_hour (=> 20, 26, 32, 38 over a day) + room offset
  (Kitchen 0, LivingRoom 5)
=> per room per day: sum(temp) = 116 (+ 4*offset), count = 4

Node URLs come from argv (node_a_url node_b_url) or $NODE_A_URL / $NODE_B_URL,
defaulting to the standard local ports. Stdlib only.
"""
import datetime as dt
import os
import sys
import urllib.request

DB = "testdb"
ROOMS = {"Kitchen": 0.0, "LivingRoom": 5.0}

argv = sys.argv[1:]
NODE_A = (argv[0] if len(argv) > 0 else os.environ.get("NODE_A_URL", "http://127.0.0.1:8181")).rstrip("/")
NODE_B = (argv[1] if len(argv) > 1 else os.environ.get("NODE_B_URL", "http://127.0.0.1:8182")).rstrip("/")
SLOT_TO_NODE = {0: NODE_A, 6: NODE_B, 12: NODE_A, 18: NODE_B}

START = dt.datetime(2023, 1, 1, tzinfo=dt.timezone.utc)
END = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)


def post_lp(base_url, lines):
    body = "\n".join(lines).encode()
    req = urllib.request.Request(
        f"{base_url}/api/v3/write_lp?db={DB}&precision=second&accept_partial=false",
        data=body, method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        if r.status not in (200, 204):
            raise SystemExit(f"write to {base_url} failed: {r.status} {r.read()!r}")


def main():
    buf = {NODE_A: [], NODE_B: []}
    total = 0
    t = START
    step = dt.timedelta(hours=6)
    while t < END:
        node = SLOT_TO_NODE[t.hour]
        epoch = int(t.timestamp())
        for room, off in ROOMS.items():
            temp = 20.0 + t.hour + off
            buf[node].append(f"home,room={room} temp={temp} {epoch}")
            total += 1
        if len(buf[node]) >= 5000:
            post_lp(node, buf[node]); buf[node] = []
        t += step
    for node, lines in buf.items():
        if lines:
            post_lp(node, lines)
    print(f"seeded {total} points to {NODE_A} + {NODE_B}, "
          f"{START.date()}..{(END - step).date()} step 6h, 2 rooms")


if __name__ == "__main__":
    main()
