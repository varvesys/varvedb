# Processing Engine — architecture

Component map of the Processing Engine (`influxdb3_processing_engine` + `influxdb3_py_api`),
including the cluster `TriggerPlacement` seam (`influxdb3_catalog::enterprise::trigger_placement`,
`influxdb3_cluster::pe_placement`). For how a trigger gets from an event to a Python call and
back, see the companion sequence diagrams:

- [Startup, registration & the placement gate](sequence-startup-and-placement.md)
- [WAL-flush trigger](sequence-wal-trigger.md)
- [Scheduled trigger (`every:` / `cron:`)](sequence-schedule-trigger.md)
- [HTTP request trigger (`request:<path>`)](sequence-request-trigger.md)

## Component diagram

```mermaid
flowchart TB
  subgraph serve["influxdb3 serve (command())"]
    SW["ProcessingEngineManagerImpl::new_with_options(...)"]
  end

  subgraph inputs["Trigger sources"]
    WAL["WAL flush\nwrite_buffer.wal().add_file_notifier(PEM)"]
    HTTPREQ["HTTP POST /api/v3/engine/&lt;path&gt;"]
    CATEV["Catalog events\n(TriggerCreated / Enabled / Disabled / Deleted,\nDbDeleted) via subscribe_to_updates(&quot;processing_engine&quot;)"]
  end

  subgraph PEM["ProcessingEngineManagerImpl  (influxdb3_processing_engine/src/lib.rs)"]
    ENV["environment_manager\n(plugin_dir, venv, package_manager)"]
    REG["trigger_registry: RwLock&lt;TriggerRegistry&gt;\n• wal routes (db/table)\n• request_paths: HashMap&lt;path, TriggerKey&gt;"]
    PLACE["placement: Arc&lt;dyn TriggerPlacement&gt;\ncore = DefaultTriggerPlacement (node_spec == All)\ncluster = ClusterPlacement (trigger_arguments[node_spec])"]
    SCHED["scheduler: Scheduler\n→ SchedulerRuntime::run (1 task)\nper-trigger: capacity Semaphore,\nmax_concurrency, RetryPolicy(5, exp≤5s)"]
    BGCAT["background_catalog_update (1 task)\nreacts to Catalog events → run_trigger / stop_trigger"]
    CACHE["cache: Arc&lt;Mutex&lt;CacheStore&gt;&gt;\nCacheId::Global | CacheId::Trigger{db,trig}"]
    SHUT["plugin_shutdown: CancellationToken"]
  end

  subgraph worker["PythonTriggerWorker  (worker/local.rs)  — 1 local worker"]
    EXO["execute_{wal|schedule|request}_once"]
    SB["tokio::spawn_blocking → Python::attach"]
  end

  subgraph py["Python layer  (influxdb3_py_api)"]
    VENV["&lt;plugin_dir&gt;/.venv  (or VIRTUAL_ENV)\nbuilt by a std::thread at startup"]
    API["influxdb3_local  = PyPluginCallApi\ninfo/warn/error · write / write_to_db (batched)\nwrite_sync / write_sync_to_db (blocking)\nquery(...) · cache (PyCache)"]
    FN["entrypoint: process_writes |\nprocess_scheduled_call | process_request"]
  end

  subgraph out["Side effects (in-process endpoints)"]
    WE["WriteEndpoint = InProcessWriteEndpoint\n→ Bufferer::write_lp (LOCAL node)"]
    QE["QueryEndpoint = InProcessQueryEndpoint\n→ QueryExecutor::query_sql (LOCAL node)"]
  end

  SW --> PEM
  WAL -->|"notify(Arc&lt;WalContents&gt;)"| PEM
  HTTPREQ -->|"processing_engine_request_plugin → request_trigger"| PEM
  CATEV --> BGCAT
  BGCAT -->|run_trigger| PLACE
  PLACE -->|allowed & plugin_dir set| REG
  PLACE --> SCHED
  REG --> SCHED
  SCHED --> worker
  EXO --> SB --> py
  API --> FN
  FN -->|"PluginReturnState{log_lines, write_db_lines}"| worker
  worker -->|"handle_return_state → write_lp"| WE
  API -->|"query(...)"| QE
  API --> CACHE
  ENV --> VENV
```

## Key facts

- Built **unconditionally** in `serve.rs`; **inert without `--plugin-dir`** (`run_trigger`
  short-circuits, plugin reads fail). `--plugin-dir` also builds/uses the venv and, in a
  cluster, adds `NodeMode::Process` to the node's registered modes.
- One process-wide `Scheduler` + one local `PythonTriggerWorker`. Python runs on
  `spawn_blocking` threads via `Python::attach`.
- The plugin file is read **lazily per invocation** (`worker/local.rs` `read_plugin_code`),
  not at startup — a missing file is a per-tick error (per `--error-behavior`), never a boot
  failure.
- `influxdb3_local.write()` / `.query()` hit the **local** node only. Cross-node write-back
  is left to the plugin (see `README_processing_engine.md`, "Running in a cluster").
- 3.11 reliability: per-trigger admission `Semaphore` (`queue_size`) + dispatch bound
  (`max_concurrency` from `--async-trigger-concurrency-limit`; sync triggers = 1);
  `RetryPolicy` = 5 attempts, exponential backoff capped at 5s; empty WAL flushes are
  skipped at three layers (WAL buffer check, `wal_invocations`, worker re-check).
- Trigger placement (`run_trigger`'s gate) is behind a trait
  (`influxdb3_catalog::enterprise::trigger_placement::TriggerPlacement`) so the single-node
  build's behaviour (`DefaultTriggerPlacement`: only triggers with no node targeting) is
  unchanged, and a cluster build supplies `influxdb3_cluster::pe_placement::ClusterPlacement`
  instead. See [Startup, registration & the placement gate](sequence-startup-and-placement.md).
