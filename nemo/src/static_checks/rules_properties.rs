//! Functionality that provides the static checks for a RuleSet.
use crate::static_checks::acyclicity_graphs::{JointAcyclicityGraph, WeakAcyclicityGraph};
use crate::static_checks::positions::PositionsByRuleAndVariables;
use crate::static_checks::rule_set::RuleSet;
use crate::static_checks::{positions::Positions, rule_properties::RuleProperties};

/// This trait gives some static checks for some ruleset.
pub trait RulesProperties {
    /// Determines if the ruleset is joinless.
    fn is_joinless(&self) -> bool;
    /// Determines if the ruleset is linear.
    fn is_linear(&self) -> bool;
    /// Determines if the ruleset is guarded.
    fn is_guarded(&self) -> bool;
    /// Determines if the ruleset is sticky.
    fn is_sticky(&self) -> bool;
    /// Determines if the ruleset is domain restricted.
    fn is_domain_restricted(&self) -> bool;
    /// Determines if the ruleset is frontier one.
    fn is_frontier_one(&self) -> bool;
    /// Determines if the ruleset is datalog.
    fn is_datalog(&self) -> bool;
    /// Determines if the ruleset is monadic.
    fn is_monadic(&self) -> bool;
    /// Determines if the ruleset is frontier guarded.
    fn is_frontier_guarded(&self) -> bool;
    /// Determines if the ruleset is weakly guarded.
    fn is_weakly_guarded(&self) -> bool;
    /// Determines if the ruleset is weakly fronier guarded.
    fn is_weakly_frontier_guarded(&self) -> bool;
    /// Determines if the ruleset is jointly guarded.
    fn is_jointly_guarded(&self) -> bool;
    /// Determines if the ruleset is jointly frontier guarded.
    fn is_jointly_frontier_guarded(&self) -> bool;
    /// Determines if the ruleset is weakly acyclic.
    fn is_weakly_acyclic(&self) -> bool;
    /// Determines if the ruleset is jointly acyclic.
    fn is_jointly_acyclic(&self) -> bool;
    /// Determines if the ruleset is weakly sticky.
    fn is_weakly_sticky(&self) -> bool;
    /// Determines if the ruleset is glut guarded.
    fn is_glut_guarded(&self) -> bool;
    /// Determines if the ruleset is glut frontier guarded.
    fn is_glut_frontier_guarded(&self) -> bool;
    /// Determines if the ruleset is shy.
    fn is_shy(&self) -> bool;
    /// Determines if the ruleset is mfa.
    fn is_mfa(&self) -> bool;
    /// Determines if the ruleset is dmfa.
    fn is_dmfa(&self) -> bool;
    /// Determines if the ruleset is rmfa.
    fn is_rmfa(&self) -> bool;
    /// Determines if the ruleset is mfc.
    fn is_mfc(&self) -> bool;
    /// Determines if the ruleset is dmfc.
    fn is_dmfc(&self) -> bool;
    /// Determines if the ruleset is drpc.
    fn is_drpc(&self) -> bool;
    /// Determines if the ruleset is rpc.
    fn is_rpc(&self) -> bool;
}

impl RulesProperties for RuleSet {
    fn is_joinless(&self) -> bool {
        self.0.iter().all(|rule| rule.is_joinless())
    }

    fn is_linear(&self) -> bool {
        self.0.iter().all(|rule| rule.is_linear())
    }

    fn is_guarded(&self) -> bool {
        self.0.iter().all(|rule| rule.is_guarded())
    }

    fn is_sticky(&self) -> bool {
        self.build_and_check_marking().is_some()
    }

    fn is_weakly_sticky(&self) -> bool {
        self.build_and_check_weakly_marking().is_some()
    }

    fn is_domain_restricted(&self) -> bool {
        self.0.iter().all(|rule| rule.is_domain_restricted())
    }

    fn is_frontier_one(&self) -> bool {
        self.0.iter().all(|rule| rule.is_frontier_one())
    }

    fn is_datalog(&self) -> bool {
        self.0.iter().all(|rule| rule.is_datalog())
    }

    fn is_monadic(&self) -> bool {
        self.0.iter().all(|rule| rule.is_monadic())
    }

    fn is_frontier_guarded(&self) -> bool {
        self.0.iter().all(|rule| rule.is_frontier_guarded())
    }

    fn is_weakly_guarded(&self) -> bool {
        let affected_positions: Positions = self.affected_positions();
        self.0
            .iter()
            .all(|rule| rule.is_weakly_guarded(&affected_positions))
    }

    fn is_weakly_frontier_guarded(&self) -> bool {
        let affected_positions: Positions = self.affected_positions();
        self.0
            .iter()
            .all(|rule| rule.is_weakly_frontier_guarded(&affected_positions))
    }

    fn is_jointly_guarded(&self) -> bool {
        let attacked_pos_by_ex_ruleandvars: PositionsByRuleAndVariables =
            self.attacked_positions_by_existential_ruleandvariables();
        self.0
            .iter()
            .all(|rule| rule.is_jointly_guarded(&attacked_pos_by_ex_ruleandvars))
    }

    fn is_jointly_frontier_guarded(&self) -> bool {
        let attacked_pos_by_ex_ruleandvars: PositionsByRuleAndVariables =
            self.attacked_positions_by_existential_ruleandvariables();
        self.0
            .iter()
            .all(|rule| rule.is_jointly_frontier_guarded(&attacked_pos_by_ex_ruleandvars))
    }

    fn is_weakly_acyclic(&self) -> bool {
        let we_ac_graph: WeakAcyclicityGraph = WeakAcyclicityGraph::new(self);
        !we_ac_graph.contains_cycle_with_special_edge()
    }

    fn is_jointly_acyclic(&self) -> bool {
        let jo_ac_graph: JointAcyclicityGraph = JointAcyclicityGraph::new(self);
        !jo_ac_graph.is_cyclic()
    }

    fn is_glut_guarded(&self) -> bool {
        let attacked_pos_by_cycle_ruleandvars: PositionsByRuleAndVariables =
            self.attacked_positions_by_cycle_ruleandvariables();
        self.0
            .iter()
            .all(|rule| rule.is_glut_guarded(&attacked_pos_by_cycle_ruleandvars))
    }

    fn is_glut_frontier_guarded(&self) -> bool {
        let attacked_pos_by_cycle_ruleandvars: PositionsByRuleAndVariables =
            self.attacked_positions_by_cycle_ruleandvariables();
        self.0
            .iter()
            .all(|rule| rule.is_glut_frontier_guarded(&attacked_pos_by_cycle_ruleandvars))
    }

    fn is_shy(&self) -> bool {
        let attacked_pos_by_existential_ruleandvars: PositionsByRuleAndVariables =
            self.attacked_positions_by_existential_ruleandvariables();
        self.0
            .iter()
            .all(|rule| rule.is_shy(&attacked_pos_by_existential_ruleandvars))
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
