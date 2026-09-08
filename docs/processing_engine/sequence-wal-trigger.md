# Sequence: WAL-flush trigger

See [architecture.md](architecture.md) for the component map.

```mermaid
sequenceDiagram
  autonumber
  participant WB as WriteBuffer/WAL
  participant PEM as ProcessingEngineManagerImpl (WalFileNotifier)
  participant Reg as TriggerRegistry
  participant Sch as Scheduler → SchedulerRuntime
  participant W as PythonTriggerWorker
  participant Py as Python plugin
  participant WE as WriteEndpoint (local Bufferer)

  WB->>PEM: notify(Arc<WalContents>)   %% every ~1s flush, empty flush → no-op
  loop each WalOp::Write(write_batch)
    PEM->>PEM: write_batch_to_wal_content → Arc<[WalFlushElement]>
    PEM->>Reg: wal_invocations(db_name, content)  %% drops PE-log rows, empty ⇒ []
    loop each matching WalRoute
      PEM->>Sch: enqueue(TriggerInvocation{Wal})  %% acquire per-trigger capacity Semaphore
      Sch->>Sch: dispatch_ready  (in_flight < max_concurrency)
      Sch->>W: submit_work
      W->>W: execute_wal_once  (re-check wal_contents non-empty)
      W->>Py: spawn_blocking → execute_wal_flush_trigger:\nprocess_writes(influxdb3_local, table_batches, args)
      Py-->>W: PluginReturnState{log_lines, write_db_lines}
      W->>WE: write_lp(User(db), lines.join("\n"), now, no_sync=false)   %% handle_return_state
      alt plugin error
        Sch->>Sch: handle_attempt_error →\nLog | Retry (≤5, backoff) | Disable (catalog)
      end
    end
  end
```

## Notes

- A WAL trigger only sees writes flushed on the node it runs on — the WAL is per-node. In a
  cluster, pin it to the ingest node whose data it should react to
  (`--trigger-arguments node_spec=nodes:<ingest-node-id>`); see
  [Startup, registration & the placement gate](sequence-startup-and-placement.md). Placement
  refuses (with a `warn!`, not a crash) to run a WAL trigger on a node that does not ingest —
  it would simply never fire there.
- Empty flushes are filtered at three separate points so an idle table costs nothing: the WAL
  buffer's own empty check, `TriggerRegistry::wal_invocations` returning `[]` for a flush with
  no matching rows, and the worker's own re-check in `execute_wal_once` before it pays for a
  Python call.
- Retry/backoff and the eventual `Disable` (persisted back to the catalog so every node stops
  retrying, not just this one) are identical across all three trigger kinds — see
  `SchedulerRuntime::handle_attempt_error`, `RetryPolicy` = 5 attempts, exponential backoff
  capped at 5s.
