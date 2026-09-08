"""
calendar_rollup.py — true calendar month/quarter/year rollups for the cluster-wide
downsampling live test.

The stock InfluxData downsampler bins m/q/y as fixed 30.42/91.25/365-day DATE_BIN
windows. This plugin instead uses DataFusion `date_trunc(<granularity>, time)` so the
buckets are calendar-aligned (leap years included), and it carries `temp_sum` +
`record_count` through unchanged so a weighted mean stays exact when levels cascade
(raw -> home_daily -> home_monthly -> home_yearly).

Both entrypoints do a full rebuild of the target from the source:

  process_request  — POST /api/v3/engine/<path>, JSON body:
      {"source_measurement": "...", "target_measurement": "...",
       "bucket": "month" | "quarter" | "year", "since": "2024-01-01T00:00:00Z" (optional)}

  process_scheduled_call — trigger-arguments: source_measurement, target_measurement,
      bucket (full rebuild each tick; idempotent).

Assumes the source measurement has: tag `room`, int field `record_count`, float
field `temp_sum` (the convention the downsampler writes).
"""

import json

_VALID_BUCKETS = {"minute", "hour", "day", "week", "month", "quarter", "year"}


def _rollup(influxdb3_local, source, target, bucket, since_iso=None):
    if bucket not in _VALID_BUCKETS:
        raise ValueError(f"unsupported bucket {bucket!r}; use one of {sorted(_VALID_BUCKETS)}")

    src = source.replace('"', '""')
    where = f"WHERE time >= '{since_iso}'" if since_iso else ""
    sql = f"""
        SELECT
            date_trunc('{bucket}', time) AS _time,
            SUM("temp_sum")     AS temp_sum,
            SUM("record_count") AS record_count,
            room
        FROM "{src}"
        {where}
        GROUP BY 1, room
        ORDER BY 1
    """
    rows = influxdb3_local.query(sql)
    influxdb3_local.info(
        f"calendar_rollup: {source} -> {target} ({bucket}): {len(rows)} buckets"
    )

    written = 0
    for row in rows:
        ts = row.get("_time")
        room = row.get("room")
        temp_sum = row.get("temp_sum")
        record_count = row.get("record_count")
        if ts is None or record_count is None:
            continue
        b = LineBuilder(target)
        b.time_ns(int(ts))
        if room is not None:
            b.tag("room", str(room))
        if temp_sum is not None:
            b.float64_field("temp_sum", float(temp_sum))
        b.int64_field("record_count", int(record_count))
        influxdb3_local.write(b)
        written += 1

    influxdb3_local.info(f"calendar_rollup: wrote {written} rows to {target}")
    return {"source": source, "target": target, "bucket": bucket,
            "buckets": len(rows), "written": written}


def process_request(influxdb3_local, query_parameters, request_headers, request_body, args=None):
    if not request_body:
        return {"error": "no request body; expected JSON with source_measurement/target_measurement/bucket"}
    try:
        data = json.loads(request_body)
    except Exception as e:  # noqa: BLE001
        return {"error": f"bad JSON body: {e}"}

    source = data.get("source_measurement")
    target = data.get("target_measurement")
    bucket = data.get("bucket")
    since = data.get("since")
    if not (source and target and bucket):
        return {"error": "source_measurement, target_measurement and bucket are required"}

    try:
        return _rollup(influxdb3_local, source, target, bucket, since)
    except Exception as e:  # noqa: BLE001
        influxdb3_local.error(f"calendar_rollup failed: {e}")
        return {"error": str(e)}


def process_scheduled_call(influxdb3_local, call_time, args):
    args = args or {}
    source = args.get("source_measurement")
    target = args.get("target_measurement")
    bucket = args.get("bucket")
    if not (source and target and bucket):
        influxdb3_local.error(
            "calendar_rollup: source_measurement, target_measurement and bucket "
            "trigger-arguments are required"
        )
        return
    try:
        _rollup(influxdb3_local, source, target, bucket, None)
    except Exception as e:  # noqa: BLE001
        influxdb3_local.error(f"calendar_rollup failed: {e}")
