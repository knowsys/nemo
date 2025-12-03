//! Functionality that provides the static checks for a RuleSet.
use crate::static_checks::acyclicity_graphs::{JointAcyclicityGraph, WeakAcyclicityGraph};
use crate::static_checks::msa::msa_execution_engine_from_rules;
use crate::static_checks::positions::PositionsByRuleAndVariables;
use crate::static_checks::rule_set::RuleSet;
use crate::static_checks::{positions::Positions, rule_properties::RuleProperties};
use nemo::datavalues::AnyDataValue;
use nemo::execution::DefaultExecutionEngine;
use nemo::rule_model::components::tag::Tag;

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
    /// Determines if the ruleset is msa.
    async fn is_msa(&self) -> bool;
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
        self.build_and_check_sticky_marking().is_some()
    }

    fn is_weakly_sticky(&self) -> bool {
        self.build_and_check_weakly_sticky_marking().is_some()
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
        let attacked_pos_by_ex_rule_and_vars: PositionsByRuleAndVariables =
            self.attacked_positions_by_existential_rule_and_variables();
        self.0
            .iter()
            .all(|rule| rule.is_jointly_guarded(&attacked_pos_by_ex_rule_and_vars))
    }

    fn is_jointly_frontier_guarded(&self) -> bool {
        let attacked_pos_by_ex_rule_and_vars: PositionsByRuleAndVariables =
            self.attacked_positions_by_existential_rule_and_variables();
        self.0
            .iter()
            .all(|rule| rule.is_jointly_frontier_guarded(&attacked_pos_by_ex_rule_and_vars))
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
        let attacked_pos_by_cycle_rule_and_vars: PositionsByRuleAndVariables =
            self.attacked_positions_by_cycle_rule_and_variables();
        self.0
            .iter()
            .all(|rule| rule.is_glut_guarded(&attacked_pos_by_cycle_rule_and_vars))
    }

    fn is_glut_frontier_guarded(&self) -> bool {
        let attacked_pos_by_cycle_rule_and_vars: PositionsByRuleAndVariables =
            self.attacked_positions_by_cycle_rule_and_variables();
        self.0
            .iter()
            .all(|rule| rule.is_glut_frontier_guarded(&attacked_pos_by_cycle_rule_and_vars))
    }

    fn is_shy(&self) -> bool {
        let attacked_pos_by_existential_rule_and_vars: PositionsByRuleAndVariables =
            self.attacked_positions_by_existential_rule_and_variables();
        self.0
            .iter()
            .all(|rule| rule.is_shy(&attacked_pos_by_existential_rule_and_vars))
    }

    fn is_mfa(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    async fn is_msa(&self) -> bool {
        let mut msa_exec_eng: DefaultExecutionEngine =
            msa_execution_engine_from_rules(&self.0).await;
        msa_exec_eng.execute().await.expect("no errors possible");
        let c_pred: Tag = Tag::from("_msa_C");
        if msa_exec_eng
            .predicate_rows(&c_pred)
            .await
            .expect("no errors possible")
            .is_none()
        {
            return true;
        }
        false
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
