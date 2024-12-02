use crate::rule_model::components::term::primitive::variable::Variable;
use crate::static_checks::acyclicity_graph_constructor::AcyclicityGraphConstructor;
use crate::static_checks::acyclicity_graphs::{AcyclicityGraphCycle, JointlyAcyclicityGraph};
use crate::static_checks::{
    positions::Positions, rule_properties::RuleProperties, rule_set::RuleSet,
};

use std::collections::HashMap;

pub trait RulesPropertiesInternal {
    fn is_joinless_internal(&self) -> bool;
    fn is_linear_internal(&self) -> bool;
    fn is_guarded_internal(&self) -> bool;
    fn is_sticky_internal(&self) -> bool;
    fn is_domain_restricted_internal(&self) -> bool;
    fn is_frontier_one_internal(&self) -> bool;
    fn is_datalog_internal(&self) -> bool;
    fn is_monadic_internal(&self) -> bool;
    fn is_frontier_guarded_internal(&self) -> bool;
    fn is_weakly_guarded_internal(&self) -> bool;
    fn is_weakly_frontier_guarded_internal(&self) -> bool;
    fn is_jointly_guarded_internal(&self) -> bool;
    fn is_jointly_frontier_guarded_internal(&self) -> bool;
    fn is_weakly_acyclic_internal(&self) -> bool;
    fn is_jointly_acyclic_internal(&self) -> bool;
    fn is_weakly_sticky_internal(&self) -> bool;
    fn is_glut_guarded_internal(&self) -> bool;
    fn is_glut_frontier_guarded_internal(&self) -> bool;
    fn is_shy_internal(&self) -> bool;
    fn is_mfa_internal(&self) -> bool;
    fn is_dmfa_internal(&self) -> bool;
    fn is_rmfa_internal(&self) -> bool;
    fn is_mfc_internal(&self) -> bool;
    fn is_dmfc_internal(&self) -> bool;
    fn is_drpc_internal(&self) -> bool;
    fn is_rpc_internal(&self) -> bool;
}

impl RulesPropertiesInternal for RuleSet {
    fn is_joinless_internal(&self) -> bool {
        self.iter().all(|rule| rule.is_joinless())
    }

    fn is_linear_internal(&self) -> bool {
        self.iter().all(|rule| rule.is_linear())
    }

    fn is_guarded_internal(&self) -> bool {
        self.iter().all(|rule| rule.is_guarded())
    }

    fn is_sticky_internal(&self) -> bool {
        self.build_and_check_marking().is_some()
    }

    fn is_weakly_sticky_internal(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn is_domain_restricted_internal(&self) -> bool {
        self.iter().all(|rule| rule.is_domain_restricted())
    }

    fn is_frontier_one_internal(&self) -> bool {
        self.iter().all(|rule| rule.is_frontier_one())
    }

    fn is_datalog_internal(&self) -> bool {
        self.iter().all(|rule| rule.is_datalog())
    }

    fn is_monadic_internal(&self) -> bool {
        self.iter().all(|rule| rule.is_monadic())
    }

    fn is_frontier_guarded_internal(&self) -> bool {
        self.iter().all(|rule| rule.is_frontier_guarded())
    }

    fn is_weakly_guarded_internal(&self) -> bool {
        let affected_positions: Positions = self.affected_positions();
        self.iter()
            .all(|rule| rule.is_weakly_guarded(&affected_positions))
    }

    fn is_weakly_frontier_guarded_internal(&self) -> bool {
        let affected_positions: Positions = self.affected_positions();
        self.iter()
            .all(|rule| rule.is_weakly_frontier_guarded(&affected_positions))
    }

    fn is_jointly_guarded_internal(&self) -> bool {
        let attacked_pos_by_vars: HashMap<&Variable, Positions> =
            self.attacked_positions_by_variables();
        self.iter()
            .all(|rule| rule.is_jointly_guarded(&attacked_pos_by_vars))
    }

    fn is_jointly_frontier_guarded_internal(&self) -> bool {
        let attacked_pos_by_vars: HashMap<&Variable, Positions> =
            self.attacked_positions_by_variables();
        self.iter()
            .all(|rule| rule.is_jointly_frontier_guarded(&attacked_pos_by_vars))
    }

    fn is_weakly_acyclic_internal(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn is_jointly_acyclic_internal(&self) -> bool {
        let jo_ex_graph: JointlyAcyclicityGraph = self.jointly_acyclicity_graph();
        !jo_ex_graph.is_cyclic()
    }

    fn is_glut_guarded_internal(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn is_glut_frontier_guarded_internal(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn is_shy_internal(&self) -> bool {
        let attacked_pos_by_vars: HashMap<&Variable, Positions> =
            self.attacked_positions_by_variables();
        self.iter().all(|rule| rule.is_shy(&attacked_pos_by_vars))
    }

    fn is_mfa_internal(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn is_dmfa_internal(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn is_rmfa_internal(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn is_mfc_internal(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn is_dmfc_internal(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn is_drpc_internal(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn is_rpc_internal(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }
}
