//! Functionality that provides the static checks for a [ProgramHandle].
use crate::static_checks::acyclicity_graphs::{JointAcyclicityGraph, WeakAcyclicityGraph};
use crate::static_checks::cyclicity_checks::acyclicity::AcyclicityStrategySelector;
use crate::static_checks::cyclicity_checks::acyclicity::check_acyclicity;
use crate::static_checks::cyclicity_checks::cyclicity::CyclicityStrategySelector;
use crate::static_checks::cyclicity_checks::cyclicity::check_cyclicity;
use crate::static_checks::msa::msa_execution_engine_from_handle;
use crate::static_checks::positions::PositionsByRuleAndVariables;
use crate::static_checks::rule_set::RuleSet;
use crate::static_checks::{positions::Positions, rule_properties::RuleProperties};
use nemo::execution::DefaultExecutionEngine;
use nemo::rule_model::{components::tag::Tag, programs::handle::ProgramHandle};

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
    fn is_mfa(&self) -> impl std::future::Future<Output = bool>;
    /// Determines if the ruleset is msa.
    fn is_msa(&self) -> impl std::future::Future<Output = bool>;
    /// Determines if the ruleset is dmfa.
    fn is_dmfa(&self) -> bool;
    /// Determines if the ruleset is rmfa.
    fn is_rmfa(&self) -> impl std::future::Future<Output = bool>;
    /// Determines if the ruleset is mfc.
    fn is_mfc(&self) -> impl std::future::Future<Output = bool>;
    /// Determines if the ruleset is dmfc.
    fn is_dmfc(&self) -> bool;
    /// Determines if the ruleset is drpc.
    fn is_drpc(&self) -> impl std::future::Future<Output = bool>;
    /// Determines if the ruleset is rpc.
    fn is_rpc(&self) -> bool;
}

impl RulesProperties for ProgramHandle {
    fn is_joinless(&self) -> bool {
        let rule_set = RuleSet::from(self.clone());
        rule_set.0.iter().all(|rule| rule.is_joinless())
    }

    fn is_linear(&self) -> bool {
        let rule_set = RuleSet::from(self.clone());
        rule_set.0.iter().all(|rule| rule.is_linear())
    }

    fn is_guarded(&self) -> bool {
        let rule_set = RuleSet::from(self.clone());
        rule_set.0.iter().all(|rule| rule.is_guarded())
    }

    fn is_sticky(&self) -> bool {
        RuleSet::from(self.clone())
            .build_and_check_sticky_marking()
            .is_some()
    }

    fn is_weakly_sticky(&self) -> bool {
        RuleSet::from(self.clone())
            .build_and_check_weakly_sticky_marking()
            .is_some()
    }

    fn is_domain_restricted(&self) -> bool {
        let rule_set = RuleSet::from(self.clone());
        rule_set.0.iter().all(|rule| rule.is_domain_restricted())
    }

    fn is_frontier_one(&self) -> bool {
        let rule_set = RuleSet::from(self.clone());
        rule_set.0.iter().all(|rule| rule.is_frontier_one())
    }

    fn is_datalog(&self) -> bool {
        let rule_set = RuleSet::from(self.clone());
        rule_set.0.iter().all(|rule| rule.is_datalog())
    }

    fn is_monadic(&self) -> bool {
        let rule_set = RuleSet::from(self.clone());
        rule_set.0.iter().all(|rule| rule.is_monadic())
    }

    fn is_frontier_guarded(&self) -> bool {
        let rule_set = RuleSet::from(self.clone());
        rule_set.0.iter().all(|rule| rule.is_frontier_guarded())
    }

    fn is_weakly_guarded(&self) -> bool {
        let rule_set = RuleSet::from(self.clone());
        let affected_positions: Positions = rule_set.affected_positions();
        rule_set
            .0
            .iter()
            .all(|rule| rule.is_weakly_guarded(&affected_positions))
    }

    fn is_weakly_frontier_guarded(&self) -> bool {
        let rule_set = RuleSet::from(self.clone());
        let affected_positions: Positions = rule_set.affected_positions();
        rule_set
            .0
            .iter()
            .all(|rule| rule.is_weakly_frontier_guarded(&affected_positions))
    }

    fn is_jointly_guarded(&self) -> bool {
        let rule_set = RuleSet::from(self.clone());
        let attacked_pos_by_ex_rule_and_vars: PositionsByRuleAndVariables =
            rule_set.attacked_positions_by_existential_rule_and_variables();
        rule_set
            .0
            .iter()
            .all(|rule| rule.is_jointly_guarded(&attacked_pos_by_ex_rule_and_vars))
    }

    fn is_jointly_frontier_guarded(&self) -> bool {
        let rule_set = RuleSet::from(self.clone());
        let attacked_pos_by_ex_rule_and_vars: PositionsByRuleAndVariables =
            rule_set.attacked_positions_by_existential_rule_and_variables();
        rule_set
            .0
            .iter()
            .all(|rule| rule.is_jointly_frontier_guarded(&attacked_pos_by_ex_rule_and_vars))
    }

    fn is_weakly_acyclic(&self) -> bool {
        let rule_set = RuleSet::from(self.clone());
        let we_ac_graph: WeakAcyclicityGraph = WeakAcyclicityGraph::new(&rule_set);
        !we_ac_graph.contains_cycle_with_special_edge()
    }

    fn is_jointly_acyclic(&self) -> bool {
        let rule_set = RuleSet::from(self.clone());
        let jo_ac_graph: JointAcyclicityGraph = JointAcyclicityGraph::new(&rule_set);
        !jo_ac_graph.is_cyclic()
    }

    fn is_glut_guarded(&self) -> bool {
        let rule_set = RuleSet::from(self.clone());
        let attacked_pos_by_cycle_rule_and_vars: PositionsByRuleAndVariables =
            rule_set.attacked_positions_by_cycle_rule_and_variables();
        rule_set
            .0
            .iter()
            .all(|rule| rule.is_glut_guarded(&attacked_pos_by_cycle_rule_and_vars))
    }

    fn is_glut_frontier_guarded(&self) -> bool {
        let rule_set = RuleSet::from(self.clone());
        let attacked_pos_by_cycle_rule_and_vars: PositionsByRuleAndVariables =
            rule_set.attacked_positions_by_cycle_rule_and_variables();
        rule_set
            .0
            .iter()
            .all(|rule| rule.is_glut_frontier_guarded(&attacked_pos_by_cycle_rule_and_vars))
    }

    fn is_shy(&self) -> bool {
        let rule_set = RuleSet::from(self.clone());
        let attacked_pos_by_existential_rule_and_vars: PositionsByRuleAndVariables =
            rule_set.attacked_positions_by_existential_rule_and_variables();
        rule_set.0.iter().enumerate().all(|(rule_index, rule)| {
            rule.is_shy(rule_index, &attacked_pos_by_existential_rule_and_vars)
        })
    }

    async fn is_mfa(&self) -> bool {
        check_acyclicity(self, AcyclicityStrategySelector::MFA).await
    }

    async fn is_msa(&self) -> bool {
        let mut msa_exec_eng: DefaultExecutionEngine =
            msa_execution_engine_from_handle(self.clone()).await;
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
        unreachable!();
    }

    async fn is_rmfa(&self) -> bool {
        check_acyclicity(self, AcyclicityStrategySelector::RMFA).await
    }

    async fn is_mfc(&self) -> bool {
        check_cyclicity(self, CyclicityStrategySelector::MFC).await
    }

    fn is_dmfc(&self) -> bool {
        unreachable!();
    }

    async fn is_drpc(&self) -> bool {
        check_cyclicity(self, CyclicityStrategySelector::DRPC).await
    }

    fn is_rpc(&self) -> bool {
        unreachable!();
    }
}
