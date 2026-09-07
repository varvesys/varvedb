//! Trigger placement — the seam a cluster build uses to decide which node runs a trigger.
//!
//! The core build has no multi-node concept: every processing-engine node runs every trigger
//! that has no explicit node targeting. [`DefaultTriggerPlacement`] is exactly that rule. A
//! cluster build supplies its own [`TriggerPlacement`] (keyed on `TriggerDefinition.node_spec`
//! and/or `trigger_arguments`) via `ProcessingEngineManagerOptions::with_placement`.

use crate::catalog::{NodeSpec, TriggerDefinition};

/// Decides whether the local node should run a given trigger.
pub trait TriggerPlacement: std::fmt::Debug + Send + Sync + 'static {
    /// `true` if this node should register and run `trigger`.
    fn allows(&self, trigger: &TriggerDefinition) -> bool;
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
}
