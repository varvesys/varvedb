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
        let Some(ids) = names
            .iter()
            .map(|name| self.catalog.node(name).map(|n| n.node_catalog_id()))
            .collect::<Option<Vec<_>>>()
        else {
            warn!(
                node_spec = names.join(","),
                trigger = trigger.trigger_name.as_ref(),
                "node_spec names a node not in the cluster catalog; not running trigger here"
            );
            return false;
        };

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
    use influxdb3_catalog::catalog::{ErrorBehavior, TriggerSettings};
    use influxdb3_id::TriggerId;

    fn trigger(args: &[(&str, &str)]) -> TriggerDefinition {
        TriggerDefinition {
            trigger_id: TriggerId::new(0),
            trigger_name: "t".into(),
            plugin_filename: "p.py".into(),
            database_name: "db".into(),
            node_spec: NodeSpec::All,
            trigger: TriggerSpecificationDefinition::Schedule {
                schedule: "* * * * * *".into(),
            },
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
