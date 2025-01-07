use crate::static_checks::acyclicity_graph_constructor::AcyclicityGraphConstructor;
use crate::static_checks::acyclicity_graphs::{
    AcyclicityGraphCycle, JointlyAcyclicityGraph, WeaklyAcyclicityGraph, WeaklyAcyclicityGraphCycle,
};
use crate::static_checks::positions::PositionsByVariables;
use crate::static_checks::rule_set::{RuleSet, SpecialPositionsConstructor};
use crate::static_checks::{positions::Positions, rule_properties::RuleProperties};

#[allow(dead_code)]
pub trait RulesProperties {
    fn is_joinless(&self) -> bool;
    fn is_linear(&self) -> bool;
    fn is_guarded(&self) -> bool;
    fn is_sticky(&self) -> bool;
    fn is_domain_restricted(&self) -> bool;
    fn is_frontier_one(&self) -> bool;
    fn is_datalog(&self) -> bool;
    fn is_monadic(&self) -> bool;
    fn is_frontier_guarded(&self) -> bool;
    fn is_weakly_guarded(&self) -> bool;
    fn is_weakly_frontier_guarded(&self) -> bool;
    fn is_jointly_guarded(&self) -> bool;
    fn is_jointly_frontier_guarded(&self) -> bool;
    fn is_weakly_acyclic(&self) -> bool;
    fn is_jointly_acyclic(&self) -> bool;
    fn is_weakly_sticky(&self) -> bool;
    fn is_glut_guarded(&self) -> bool;
    fn is_glut_frontier_guarded(&self) -> bool;
    fn is_shy(&self) -> bool;
    fn is_mfa(&self) -> bool;
    fn is_dmfa(&self) -> bool;
    fn is_rmfa(&self) -> bool;
    fn is_mfc(&self) -> bool;
    fn is_dmfc(&self) -> bool;
    fn is_drpc(&self) -> bool;
    fn is_rpc(&self) -> bool;
}

impl RulesProperties for RuleSet {
    fn is_joinless(&self) -> bool {
        self.iter().all(|rule| rule.is_joinless())
    }

    fn is_linear(&self) -> bool {
        self.iter().all(|rule| rule.is_linear())
    }

    fn is_guarded(&self) -> bool {
        self.iter().all(|rule| rule.is_guarded())
    }

    fn is_sticky(&self) -> bool {
        self.build_and_check_marking().is_some()
    }

    fn is_weakly_sticky(&self) -> bool {
        self.build_and_check_weakly_marking().is_some()
    }

    fn is_domain_restricted(&self) -> bool {
        self.iter().all(|rule| rule.is_domain_restricted())
    }

    fn is_frontier_one(&self) -> bool {
        self.iter().all(|rule| rule.is_frontier_one())
    }

    fn is_datalog(&self) -> bool {
        self.iter().all(|rule| rule.is_datalog())
    }

    fn is_monadic(&self) -> bool {
        self.iter().all(|rule| rule.is_monadic())
    }

    fn is_frontier_guarded(&self) -> bool {
        self.iter().all(|rule| rule.is_frontier_guarded())
    }

    fn is_weakly_guarded(&self) -> bool {
        let affected_positions: Positions = self.affected_positions();
        self.iter()
            .all(|rule| rule.is_weakly_guarded(&affected_positions))
    }

    fn is_weakly_frontier_guarded(&self) -> bool {
        let affected_positions: Positions = self.affected_positions();
        self.iter()
            .all(|rule| rule.is_weakly_frontier_guarded(&affected_positions))
    }

    fn is_jointly_guarded(&self) -> bool {
        let attacked_pos_by_existential_vars: PositionsByVariables =
            self.attacked_positions_by_existential_variables();
        self.iter()
            .all(|rule| rule.is_jointly_guarded(&attacked_pos_by_existential_vars))
    }

    fn is_jointly_frontier_guarded(&self) -> bool {
        let attacked_pos_by_existential_vars: PositionsByVariables =
            self.attacked_positions_by_existential_variables();
        self.iter()
            .all(|rule| rule.is_jointly_frontier_guarded(&attacked_pos_by_existential_vars))
    }

    fn is_weakly_acyclic(&self) -> bool {
        let we_ac_graph: WeaklyAcyclicityGraph = self.weakly_acyclicity_graph();
        !we_ac_graph.contains_cycle_with_special_edge()
    }

    fn is_jointly_acyclic(&self) -> bool {
        let jo_ac_graph: JointlyAcyclicityGraph = self.jointly_acyclicity_graph();
        !jo_ac_graph.is_cyclic()
    }

    fn is_glut_guarded(&self) -> bool {
        let attacked_pos_by_cycle_vars: PositionsByVariables =
            self.attacked_positions_by_cycle_variables();
        self.iter()
            .all(|rule| rule.is_glut_guarded(&attacked_pos_by_cycle_vars))
    }

    fn is_glut_frontier_guarded(&self) -> bool {
        let attacked_pos_by_cycle_vars: PositionsByVariables =
            self.attacked_positions_by_cycle_variables();
        self.iter()
            .all(|rule| rule.is_glut_frontier_guarded(&attacked_pos_by_cycle_vars))
    }

    fn is_shy(&self) -> bool {
        let attacked_pos_by_existential_vars: PositionsByVariables =
            self.attacked_positions_by_existential_variables();
        self.iter()
            .all(|rule| rule.is_shy(&attacked_pos_by_existential_vars))
    }

    fn is_mfa(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn is_dmfa(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn is_rmfa(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn is_mfc(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn is_dmfc(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn is_drpc(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn is_rpc(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }
}
