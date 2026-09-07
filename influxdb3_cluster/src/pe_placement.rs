//! Cluster trigger placement — the `influxdb3_cluster` implementation of
//! [`TriggerPlacement`](influxdb3_catalog::enterprise::trigger_placement::TriggerPlacement).
//!
//! Wired in `serve.rs` via `ProcessingEngineManagerOptions::with_placement` only when the node
//! is clustered. A single-node build keeps the core `DefaultTriggerPlacement` and never
//! constructs this.
//!
//! Placement is read from the trigger's `trigger_arguments`:
//!
//! ```text
//! influxdb3 create trigger --trigger-arguments node_spec=nodes:host01,host02  …
//! ```
//!
//! * absent / `""` / `all` → runs on every processing-engine node (same as core);
//! * `nodes:<id>[,<id>]` → runs only on the listed nodes.
//!
//! The dedicated `TriggerDefinition.node_spec` field is left `All` by the core create path, so
//! this side-channel is used instead — it needs no change to the CLI / wire / catalog code.

use std::str::FromStr;
use std::sync::Arc;

use influxdb3_catalog::catalog::{
    ApiNodeSpec, Catalog, NodeSpec, TriggerDefinition, TriggerSpecificationDefinition,
};
use influxdb3_catalog::enterprise::trigger_placement::{
    NODE_SPEC_TRIGGER_ARGUMENT as NODE_SPEC_ARG, TriggerPlacement,
};
use influxdb3_id::NodeId;
use observability_deps::tracing::warn;

/// The placement a trigger's `node_spec` argument asks for, before catalog resolution.
#[derive(Debug, PartialEq, Eq)]
enum PlacementIntent {
    /// No `node_spec`, empty, or `all` — run on every processing-engine node.
    All,
    /// `nodes:a,b` — run only on the named nodes (names, not yet resolved to ids).
    Nodes(Vec<String>),
    /// `node_spec` was present but unparseable.
    Invalid(String),
}

/// Read `trigger_arguments["node_spec"]` into a [`PlacementIntent`]. Pure — no catalog.
fn placement_intent(trigger: &TriggerDefinition) -> PlacementIntent {
    let Some(raw) = trigger
        .trigger_arguments
        .as_ref()
        .and_then(|args| args.get(NODE_SPEC_ARG))
        .map(|s| s.trim())
        .filter(|s| !s.is_empty() && *s != "all")
    else {
        return PlacementIntent::All;
    };
    match ApiNodeSpec::from_str(raw) {
        Ok(ApiNodeSpec::All) => PlacementIntent::All,
        // Upstream `from_str` splits on ',' without trimming; be forgiving of spaces.
        Ok(ApiNodeSpec::Nodes(names)) => PlacementIntent::Nodes(
            names
                .into_iter()
                .map(|n| n.trim().to_string())
                .filter(|n| !n.is_empty())
                .collect(),
        ),
        Err(_) => PlacementIntent::Invalid(raw.to_string()),
    }
}

/// Build the cluster [`TriggerPlacement`] for this node.
pub fn trigger_placement(catalog: Arc<Catalog>) -> Arc<dyn TriggerPlacement> {
    Arc::new(ClusterPlacement { catalog })
}

#[derive(Debug)]
struct ClusterPlacement {
    catalog: Arc<Catalog>,
}

impl TriggerPlacement for ClusterPlacement {
    fn allows(&self, trigger: &TriggerDefinition) -> bool {
        let names = match placement_intent(trigger) {
            PlacementIntent::All => return self.type_ok(trigger),
            PlacementIntent::Nodes(names) => names,
            PlacementIntent::Invalid(value) => {
                warn!(
                    value,
                    trigger = trigger.trigger_name.as_ref(),
                    "invalid node_spec trigger-argument; not running trigger on this node"
                );
                return false;
            }
        };

        // Resolve node names -> catalog ids (upstream `resolve_node_spec` is private).
        let (ids, unknown) = self.resolve_node_names(&names);
        if !unknown.is_empty() {
            // Name the offending entries specifically — with a long `node_spec` an operator
            // should not have to diff the list to find the typo. A newly-joined node that has
            // not yet propagated to this catalog replica lands here too, hence the hedge.
            warn!(
                trigger = trigger.trigger_name.as_ref(),
                unknown_nodes = unknown.join(","),
                node_spec = names.join(","),
                "node_spec names node(s) not in this catalog view; not running this trigger here \
                 (check for a typo — a newly-joined node may also not have propagated yet)"
            );
            return false;
        }

        match self.catalog.matches_node_spec(&NodeSpec::Nodes(ids)) {
            Ok(true) => self.type_ok(trigger),
            // Pinned to other nodes — expected on every node but the target(s); no log.
            Ok(false) => false,
            Err(error) => {
                warn!(
                    %error,
                    trigger = trigger.trigger_name.as_ref(),
                    "could not resolve the current node for placement; not running trigger here"
                );
                false
            }
        }
    }
}

impl ClusterPlacement {
    /// Split `names` into the catalog ids they resolve to and the names this node's catalog
    /// view has never heard of. Every name is checked, so the warning can list all typos at
    /// once rather than surfacing them one restart at a time.
    fn resolve_node_names(&self, names: &[String]) -> (Vec<NodeId>, Vec<String>) {
        let mut ids = Vec::with_capacity(names.len());
        let mut unknown = Vec::new();
        for name in names {
            match self.catalog.node(name) {
                Some(node) => ids.push(node.node_catalog_id()),
                None => unknown.push(name.clone()),
            }
        }
        (ids, unknown)
    }

    /// A WAL trigger only sees writes from the node it runs on. If placement lands it on a node
    /// that never ingests, it can never fire — say so and don't start it. (v1 rejected this at
    /// create time; this is the runtime equivalent.)
    fn type_ok(&self, trigger: &TriggerDefinition) -> bool {
        let is_wal = matches!(
            trigger.trigger,
            TriggerSpecificationDefinition::SingleTableWalWrite { .. }
                | TriggerSpecificationDefinition::AllTablesWalWrite
        );
        if is_wal
            && let Ok(node) = self.catalog.current_node()
            && !node.is_ingest()
        {
            warn!(
                trigger = trigger.trigger_name.as_ref(),
                node = node.node_id().as_ref(),
                "WAL trigger placed on a node that does not ingest; it will never fire"
            );
            return false;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hashbrown::HashMap;
    use influxdb3_catalog::catalog::{
        CatalogArgs, CatalogLimits, ErrorBehavior, NodeMode, TriggerSettings,
    };
    use influxdb3_id::TriggerId;
    use influxdb3_process::ProcessUuidWrapper;
    use iox_time::{MockProvider, Time, TimeProvider};
    use object_store::memory::InMemory;

    fn trigger_with(
        spec: TriggerSpecificationDefinition,
        args: &[(&str, &str)],
    ) -> TriggerDefinition {
        TriggerDefinition {
            trigger_id: TriggerId::new(0),
            trigger_name: "t".into(),
            plugin_filename: "p.py".into(),
            database_name: "db".into(),
            node_spec: NodeSpec::All,
            trigger: spec,
            trigger_settings: TriggerSettings {
                run_async: false,
                error_behavior: ErrorBehavior::Log,
            },
            trigger_arguments: if args.is_empty() {
                None
            } else {
                Some(
                    args.iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect::<HashMap<_, _>>(),
                )
            },
            disabled: false,
        }
    }

    /// A schedule trigger (fires on every processing-engine node it is placed on).
    fn trigger(args: &[(&str, &str)]) -> TriggerDefinition {
        trigger_with(
            TriggerSpecificationDefinition::Schedule {
                schedule: "* * * * * *".into(),
            },
            args,
        )
    }

    /// A WAL trigger (only sees the writes of the ingester it runs on).
    fn wal_trigger(args: &[(&str, &str)]) -> TriggerDefinition {
        trigger_with(TriggerSpecificationDefinition::AllTablesWalWrite, args)
    }

    /// Build an in-memory cluster catalog whose current node is `current`, with every
    /// `(name, modes)` in `nodes` registered. `current` must appear in `nodes`.
    async fn cluster_catalog(current: &str, nodes: &[(&str, &[NodeMode])]) -> Arc<Catalog> {
        let store = Arc::new(InMemory::new());
        let time: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let catalog = Catalog::new_enterprise(
            current,
            "test-cluster",
            store,
            time,
            Default::default(),
            Arc::new(CatalogLimits::none()),
            CatalogArgs::default(),
        )
        .await
        .expect("build enterprise catalog");

        for (name, modes) in nodes {
            catalog
                .register_node(
                    name,
                    4,
                    modes.to_vec(),
                    Arc::new(ProcessUuidWrapper::new()),
                    Arc::from(format!("inst-{name}")),
                    None,
                    None,
                    0,
                )
                .await
                .expect("register node");
        }
        catalog
    }

    /// `true` if the current node (per `catalog`) would run `trig`.
    fn allows(catalog: &Arc<Catalog>, trig: &TriggerDefinition) -> bool {
        trigger_placement(Arc::clone(catalog)).allows(trig)
    }

    #[tokio::test]
    async fn unpinned_trigger_runs_on_every_node() {
        let catalog = cluster_catalog(
            "q1",
            &[("q1", &[NodeMode::Query]), ("i1", &[NodeMode::Ingest])],
        )
        .await;
        assert!(allows(&catalog, &trigger(&[])));
        assert!(allows(&catalog, &trigger(&[("node_spec", "all")])));
    }

    #[tokio::test]
    async fn trigger_pinned_to_this_node_runs() {
        let catalog = cluster_catalog(
            "i1",
            &[("i1", &[NodeMode::Ingest]), ("i2", &[NodeMode::Ingest])],
        )
        .await;
        assert!(allows(&catalog, &trigger(&[("node_spec", "nodes:i1")])));
        assert!(allows(&catalog, &trigger(&[("node_spec", "nodes:i1,i2")])));
    }

    #[tokio::test]
    async fn trigger_pinned_elsewhere_does_not_run() {
        let catalog = cluster_catalog(
            "i1",
            &[("i1", &[NodeMode::Ingest]), ("i2", &[NodeMode::Ingest])],
        )
        .await;
        assert!(!allows(&catalog, &trigger(&[("node_spec", "nodes:i2")])));
    }

    #[tokio::test]
    async fn node_spec_naming_an_unknown_node_does_not_run() {
        let catalog = cluster_catalog("i1", &[("i1", &[NodeMode::Ingest])]).await;
        // `ghost` is not registered -> name resolution fails -> refuse (with a warn).
        assert!(!allows(&catalog, &trigger(&[("node_spec", "nodes:ghost")])));
        // A real node alongside an unknown one still fails as a whole.
        assert!(!allows(
            &catalog,
            &trigger(&[("node_spec", "nodes:i1,ghost")])
        ));
    }

    #[tokio::test]
    async fn resolve_node_names_reports_exactly_the_unknown_names() {
        let catalog = cluster_catalog(
            "i1",
            &[("i1", &[NodeMode::Ingest]), ("i2", &[NodeMode::Ingest])],
        )
        .await;
        let placement = ClusterPlacement { catalog };

        let (ids, unknown) = placement.resolve_node_names(&["i1".into(), "i2".into()]);
        assert_eq!(ids.len(), 2);
        assert!(unknown.is_empty());

        // The known node is resolved; only the typo is called out — every name is checked, so
        // both bad entries surface together rather than one restart at a time.
        let (ids, unknown) = placement
            .resolve_node_names(&["i1".into(), "ghost".into(), "gohst".into()]);
        assert_eq!(ids.len(), 1);
        assert_eq!(unknown, vec!["ghost".to_string(), "gohst".to_string()]);
    }

    #[tokio::test]
    async fn wal_trigger_refused_on_a_non_ingest_node() {
        let catalog = cluster_catalog(
            "q1",
            &[("q1", &[NodeMode::Query]), ("i1", &[NodeMode::Ingest])],
        )
        .await;
        // Unpinned: placement lands it here (a query node), where it could never fire.
        assert!(!allows(&catalog, &wal_trigger(&[])));
    }

    #[tokio::test]
    async fn wal_trigger_runs_on_an_ingest_node() {
        let catalog = cluster_catalog(
            "i1",
            &[("i1", &[NodeMode::Ingest]), ("q1", &[NodeMode::Query])],
        )
        .await;
        assert!(allows(&catalog, &wal_trigger(&[])));
        assert!(allows(&catalog, &wal_trigger(&[("node_spec", "nodes:i1")])));
    }

    #[tokio::test]
    async fn wal_trigger_pinned_elsewhere_is_skipped_before_the_type_check() {
        // Current node is a query node; the WAL trigger is pinned to the ingester. The pin
        // check comes first, so this node just declines quietly rather than warning.
        let catalog = cluster_catalog(
            "q1",
            &[("q1", &[NodeMode::Query]), ("i1", &[NodeMode::Ingest])],
        )
        .await;
        assert!(!allows(&catalog, &wal_trigger(&[("node_spec", "nodes:i1")])));
    }

    #[test]
    fn placement_intent_reads_the_node_spec_argument() {
        assert_eq!(placement_intent(&trigger(&[])), PlacementIntent::All);
        assert_eq!(
            placement_intent(&trigger(&[("node_spec", "all")])),
            PlacementIntent::All
        );
        assert_eq!(
            placement_intent(&trigger(&[("node_spec", "  ")])),
            PlacementIntent::All
        );
        assert_eq!(
            placement_intent(&trigger(&[("other", "x")])),
            PlacementIntent::All
        );
        assert_eq!(
            placement_intent(&trigger(&[("node_spec", "nodes:host01, host02")])),
            PlacementIntent::Nodes(vec!["host01".into(), "host02".into()])
        );
        assert_eq!(
            placement_intent(&trigger(&[("node_spec", "host01")])),
            PlacementIntent::Invalid("host01".into())
        );
    }
}
