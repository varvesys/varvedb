//! Trigger placement — the seam a cluster build uses to decide which node runs a trigger.
//!
//! The core build has no multi-node concept: every processing-engine node runs every trigger
//! that has no explicit node targeting. [`DefaultTriggerPlacement`] is exactly that rule. A
//! cluster build supplies its own [`TriggerPlacement`] (keyed on `TriggerDefinition.node_spec`
//! and/or `trigger_arguments`) via `ProcessingEngineManagerOptions::with_placement`.

use hashbrown::HashMap;

use crate::catalog::{NodeSpec, TriggerDefinition};

/// Decides whether the local node should run a given trigger.
pub trait TriggerPlacement: std::fmt::Debug + Send + Sync + 'static {
    /// `true` if this node should register and run `trigger`.
    fn allows(&self, trigger: &TriggerDefinition) -> bool;
}

/// The `trigger_arguments` key a cluster build reads to pin a trigger to specific nodes
/// (`all` | `nodes:<id>[,<id>]`). The core build ignores it.
pub const NODE_SPEC_TRIGGER_ARGUMENT: &str = "node_spec";

/// `trigger_arguments` entries reserved for placement / control-plane use. They are removed
/// from the argument map before it reaches a plugin, so user code neither sees them nor grows
/// a dependency on them.
pub const RESERVED_TRIGGER_ARGUMENT_KEYS: &[&str] = &[NODE_SPEC_TRIGGER_ARGUMENT];

/// `trigger_arguments` as a plugin should see them: every [`RESERVED_TRIGGER_ARGUMENT_KEYS`]
/// entry removed. Returns `None` when nothing plugin-visible remains, so a trigger whose only
/// argument was a reserved key looks the same to the plugin as one with no arguments at all.
pub fn plugin_visible_trigger_arguments(
    trigger_arguments: &Option<HashMap<String, String>>,
) -> Option<HashMap<String, String>> {
    let visible: HashMap<String, String> = trigger_arguments
        .iter()
        .flatten()
        .filter(|(key, _)| !RESERVED_TRIGGER_ARGUMENT_KEYS.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    (!visible.is_empty()).then_some(visible)
}

/// Core placement: run a trigger only when it has no explicit node targeting.
///
/// This is byte-for-byte the pre-cluster gate — `matches!(trigger.node_spec, NodeSpec::All)`.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultTriggerPlacement;

impl TriggerPlacement for DefaultTriggerPlacement {
    fn allows(&self, trigger: &TriggerDefinition) -> bool {
        matches!(trigger.node_spec, NodeSpec::All)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{
        ErrorBehavior, TriggerSettings, TriggerSpecificationDefinition,
    };
    use influxdb3_id::{NodeId, TriggerId};

    fn trig(node_spec: NodeSpec) -> TriggerDefinition {
        TriggerDefinition {
            trigger_id: TriggerId::new(0),
            trigger_name: "t".into(),
            plugin_filename: "p.py".into(),
            database_name: "db".into(),
            node_spec,
            trigger: TriggerSpecificationDefinition::AllTablesWalWrite,
            trigger_settings: TriggerSettings {
                run_async: false,
                error_behavior: ErrorBehavior::Log,
            },
            trigger_arguments: None,
            disabled: false,
        }
    }

    #[test]
    fn default_placement_is_the_pre_cluster_gate() {
        let p = DefaultTriggerPlacement;
        assert!(p.allows(&trig(NodeSpec::All)));
        assert!(!p.allows(&trig(NodeSpec::Nodes(vec![NodeId::new(1)]))));
    }

    #[test]
    fn plugin_visible_trigger_arguments_strips_reserved_keys() {
        // Nothing to strip.
        assert_eq!(plugin_visible_trigger_arguments(&None), None);
        let user_only: HashMap<_, _> = [("threshold".to_string(), "5".to_string())].into();
        assert_eq!(
            plugin_visible_trigger_arguments(&Some(user_only.clone())),
            Some(user_only)
        );

        // Reserved key removed, user keys kept.
        let mixed: HashMap<_, _> = [
            ("node_spec".to_string(), "nodes:a,b".to_string()),
            ("threshold".to_string(), "5".to_string()),
        ]
        .into();
        assert_eq!(
            plugin_visible_trigger_arguments(&Some(mixed)),
            Some([("threshold".to_string(), "5".to_string())].into())
        );

        // Reserved key was the only entry -> indistinguishable from no arguments.
        let reserved_only: HashMap<_, _> =
            [("node_spec".to_string(), "all".to_string())].into();
        assert_eq!(plugin_visible_trigger_arguments(&Some(reserved_only)), None);
    }
}
