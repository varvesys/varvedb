# Sequence: startup, trigger registration & the placement gate

See [architecture.md](architecture.md) for the component map.

```mermaid
sequenceDiagram
  autonumber
  participant S as serve.rs command()
  participant PEM as ProcessingEngineManagerImpl
  participant Cat as Catalog
  participant Sch as Scheduler / SchedulerRuntime
  participant WAL as WAL (WalObjectStore)

  S->>PEM: new_with_options(env, catalog, node_id, write_ep, query_ep, opts)
  opt plugin_dir set
    PEM->>PEM: package_manager.init_pyenv() + virtualenv::init_pyo3()
  end
  PEM->>Cat: subscribe_to_updates("processing_engine")
  PEM->>PEM: CacheStore::new (10s cleanup)
  PEM->>Sch: Scheduler::new(node_id, make_workers) → spawn SchedulerRuntime::run
  PEM->>PEM: spawn background_catalog_update(pem, sub)
  PEM-->>S: Arc<ProcessingEngineManagerImpl>

  S->>PEM: start_triggers()
  loop each catalog.active_triggers() (non-disabled, cluster-wide)
    PEM->>PEM: run_trigger(db_id, trigger_id)
    PEM->>PEM: placement.allows(&trigger)?
    alt not placed here  (core: node_spec != All · cluster: node_spec arg excludes this node)
      PEM-->>PEM: info "trigger not placed on this node, skipping"
    else no plugin_dir
      PEM-->>PEM: info "no plugin directory configured"
    else run
      PEM->>Sch: register trigger (SchedulerConfig: run_async, async limit, RetryPolicy)
      alt WalRows
        PEM->>PEM: trigger_registry.add_wal_trigger(key, db, table filter)
      else Schedule/Every
        PEM->>PEM: spawn run_schedule_event_source_loop (cron / aligned every:)
      else RequestPath
        PEM->>PEM: trigger_registry.add_request_trigger(key, path)
      end
    end
  end

  S->>WAL: wal().add_file_notifier(pem)
  S->>PEM: shutdown_plugins_on(ShutdownToken)  %% cancels plugin_shutdown on server stop
```

`create trigger` at runtime follows the same gate: HTTP
`POST /api/v3/configure/processing_engine_trigger` → `Catalog::create_processing_engine_trigger`
→ CAS-append to the shared catalog log → `CatalogEvent::TriggerCreated` → every node's
`background_update` poller replays it → `background_catalog_update` → `run_trigger` (same
placement check as above).

## The placement gate

```mermaid
flowchart TD
  RT["run_trigger(db, trig)"] --> A{"placement.allows(&trigger)"}
  A -->|"core: DefaultTriggerPlacement"| D["matches!(trigger.node_spec, NodeSpec::All)"]
  A -->|"cluster: ClusterPlacement"| C1["read trigger_arguments[&quot;node_spec&quot;]"]
  C1 --> C2{"all / absent?"}
  C2 -->|yes| C3["type_ok(): WAL trigger &&\ncurrent node !is_ingest ⇒ warn + false"]
  C2 -->|"nodes:a,b"| C4["ApiNodeSpec::from_str →\ncatalog.node(name).node_catalog_id()"]
  C4 -->|unknown name| C5["warn + false"]
  C4 --> C6["catalog.matches_node_spec(NodeSpec::Nodes(ids))"]
  C6 -->|true| C3
  C6 -->|false| C7["false (pinned elsewhere, silent)"]
  D -->|true| OK
  C3 -->|true| OK
  OK["proceed: check plugin_dir, register with scheduler"]
```

- **Core** (`influxdb3_catalog::enterprise::trigger_placement::DefaultTriggerPlacement`):
  behaviour-preserving — a trigger runs here only if its catalog `node_spec` is `All`.
- **Cluster** (`influxdb3_cluster::pe_placement::ClusterPlacement`): placement is read from
  the trigger's `trigger_arguments["node_spec"]` (e.g.
  `--trigger-arguments node_spec=nodes:host01,host02`), not the catalog `node_spec` field
  (which the create path always leaves as `All`). A WAL trigger that resolves to a node
  which does not ingest is refused with a loud warning — it would never fire there.
- `run_trigger` is the **only** per-node "should I run this trigger" decision point; it is
  reached from `start_triggers()` (boot) and from `background_catalog_update`'s
  `TriggerCreated` / `TriggerEnabled` handling.
- Placement is **not** re-evaluated on `NodeRegistered` / `NodeUnregistered` — this is a
  deliberate choice, not a gap. To move a running trigger onto a newly-added node, edit the
  trigger (disable + enable re-runs placement everywhere) or restart that node. Rationale is
  in the `influxdb3_cluster::pe_placement` module docs and `README_processing_engine.md`.
