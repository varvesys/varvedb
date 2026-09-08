# Sequence: HTTP request trigger (`request:<path>`)

See [architecture.md](architecture.md) for the component map.

```mermaid
sequenceDiagram
  autonumber
  participant C as Client
  participant H as HTTP router (influxdb3_server)
  participant Api as HttpApi
  participant PEM as ProcessingEngineManagerImpl
  participant Reg as TriggerRegistry
  participant Sch as Scheduler → SchedulerRuntime
  participant W as PythonTriggerWorker
  participant Py as Python plugin

  C->>H: POST /api/v3/engine/<path>
  H->>Api: processing_engine_request_plugin(path, req)   %% strips Authorization header
  Api->>PEM: request_trigger(path, params, headers, body)
  PEM->>Reg: request_invocation(path) → TriggerKey
  alt path not registered here (unknown, or trigger pinned to another node)
    Reg-->>Api: RequestTriggerNotFound
    Api-->>C: 404 {error:"not found"}
  else
    PEM->>Sch: enqueue(RequestPayload{oneshot tx})
    Sch->>W: submit_work
    W->>Py: spawn_blocking → execute_request_trigger:\nprocess_request(influxdb3_local, query_params, headers, body, args)
    Py-->>W: return value → process_flask_response → (status, headers, body)
    W->>Sch: handle_work_output → RequestPayload::send_work_response
    Sch-->>PEM: (via oneshot)
    PEM-->>Api: TriggerResponse
    Api-->>C: <status>, <headers>, <body>   %% any batched writes still flushed via handle_return_state
  end
```

## Notes

- There is **no cross-node routing**: `/api/v3/engine/<path>` only resolves on nodes where the
  trigger's placement (`TriggerPlacement::allows`) let it register — an unpinned trigger
  registers (and answers) on every processing-engine node; a pinned one only answers on its
  target node(s). A request that lands on a node without the route gets a plain 404, not a
  redirect or proxy — point a load balancer at the node(s) the trigger is actually placed on.
  See [Startup, registration & the placement gate](sequence-startup-and-placement.md).
- The `Authorization` header is stripped before the plugin sees the request; the plugin gets
  the remaining headers, query params, and raw body.
- The Python entrypoint's return value goes through `process_flask_response`, so a plugin can
  return a bare body, or a `(body, status)` / `(body, status, headers)` tuple Flask-style.
- Any writes the plugin batched via `influxdb3_local.write(...)` during the call are flushed
  through the same `handle_return_state` path used by the WAL and schedule triggers — see
  [WAL-flush trigger](sequence-wal-trigger.md) — before the HTTP response is sent back.
