//! Functionality that provides the static checks for a Rule.
use crate::rule_model::components::{
    rule::Rule, term::primitive::variable::Variable, IterablePrimitives,
};
use crate::static_checks::positions::{Positions, PositionsByRuleIdxVariables};
use crate::static_checks::rule_set::{
    ExistentialVariables, FrontierVariables, JoinVariables, VariablePair,
};

use std::collections::HashSet;

/// This trait gives some static checks for some rule.
pub trait RuleProperties {
    /// Determines if the rule is joinless.
    fn is_joinless(&self) -> bool;
    /// Determines if the rule is linear.
    fn is_linear(&self) -> bool;
    /// Determines if the rule is guarded.
    fn is_guarded(&self) -> bool;
    /// Determines if the rule is domain restricted.
    fn is_domain_restricted(&self) -> bool;
    /// Determines if the rule is frontier one.
    fn is_frontier_one(&self) -> bool;
    /// Determines if the rule is datalog.
    fn is_datalog(&self) -> bool;
    /// Determines if the rule is monadic.
    fn is_monadic(&self) -> bool;
    /// Determines if the rule is frontier guarded.
    fn is_frontier_guarded(&self) -> bool;
    /// Determines if the rule is weakly guarded.
    fn is_weakly_guarded(&self, affected_positions: &Positions) -> bool;
    /// Determines if the rule is weakly frontier guarded.
    fn is_weakly_frontier_guarded(&self, affected_positions: &Positions) -> bool;
    /// Determines if the rule is jointly guarded.
    fn is_jointly_guarded(
        &self,
        attacked_pos_by_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> bool;
    /// Determines if the rule is jointly frontier guarded.
    fn is_jointly_frontier_guarded(
        &self,
        attacked_pos_by_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> bool;
    /// Determines if the rule is glut guarded.
    fn is_glut_guarded(
        &self,
        attacked_pos_by_cycle_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> bool;
    /// Determines if the rule is glut frontier guarded.
    fn is_glut_frontier_guarded(
        &self,
        attacked_pos_by_cycle_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> bool;
    /// Determines if the rule is shy.
    fn is_shy(&self, attacked_pos_by_vars: &PositionsByRuleIdxVariables) -> bool;
    // fn is_mfa(&self) -> bool;
    // fn is_dmfa(&self) -> bool;
    // fn is_rmfa(&self) -> bool;
    // fn is_mfc(&self) -> bool;
    // fn is_dmfc(&self) -> bool;
    // fn is_drpc(&self) -> bool;
    // fn is_rpc(&self) -> bool;
}

impl RuleProperties for Rule {
    fn is_joinless(&self) -> bool {
        self.join_variables().is_empty()
    }

    fn is_linear(&self) -> bool {
        1 >= self.body().len()
    }

    fn is_guarded(&self) -> bool {
        let positive_variables: HashSet<&Variable> = self.positive_variables();
        self.is_guarded_for_variables(positive_variables)
    }

    fn is_domain_restricted(&self) -> bool {
        let positive_body_variables: HashSet<&Variable> = self.positive_variables();
        self.head().iter().all(|atom| {
            let universal_variables_of_atom: HashSet<&Variable> = atom.universal_variables();
            universal_variables_of_atom.is_empty()
                || universal_variables_of_atom == positive_body_variables
        })
    }

    fn is_frontier_one(&self) -> bool {
        1 >= self.frontier_variables().len()
    }

    fn is_datalog(&self) -> bool {
        self.existential_variables().is_empty()
    }

    fn is_monadic(&self) -> bool {
        self.head()
            .iter()
            .all(|atom| 1 == atom.primitive_terms().count())
    }

    fn is_frontier_guarded(&self) -> bool {
        let frontier_variables: HashSet<&Variable> = self.frontier_variables();
        self.is_guarded_for_variables(frontier_variables)
    }

    fn is_weakly_guarded(&self, affected_positions: &Positions) -> bool {
        let affected_universal_variables: HashSet<&Variable> =
            self.affected_universal_variables(affected_positions);
        self.is_guarded_for_variables(affected_universal_variables)
    }

    fn is_weakly_frontier_guarded(&self, affected_positions: &Positions) -> bool {
        let affected_frontier_variables: HashSet<&Variable> =
            self.affected_frontier_variables(affected_positions);
        self.is_guarded_for_variables(affected_frontier_variables)
    }

    fn is_jointly_guarded(
        &self,
        attacked_pos_by_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> bool {
        let attacked_universal_variables: HashSet<&Variable> =
            self.attacked_universal_variables(attacked_pos_by_rule_idx_vars);
        self.is_guarded_for_variables(attacked_universal_variables)
    }

    fn is_jointly_frontier_guarded(
        &self,
        attacked_pos_by_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> bool {
        let attacked_frontier_variables: HashSet<&Variable> =
            self.attacked_frontier_variables(attacked_pos_by_rule_idx_vars);
        self.is_guarded_for_variables(attacked_frontier_variables)
    }

    fn is_glut_guarded(
        &self,
        attacked_pos_by_cycle_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> bool {
        let attacked_universal_glut_variables: HashSet<&Variable> =
            self.attacked_universal_glut_variables(attacked_pos_by_cycle_rule_idx_vars);
        self.is_guarded_for_variables(attacked_universal_glut_variables)
    }

    fn is_glut_frontier_guarded(
        &self,
        attacked_pos_by_cycle_rule_idx_vars: &PositionsByRuleIdxVariables,
    ) -> bool {
        let attacked_frontier_glut_variables: HashSet<&Variable> =
            self.attacked_frontier_glut_variables(attacked_pos_by_cycle_rule_idx_vars);
        self.is_guarded_for_variables(attacked_frontier_glut_variables)
    }

    // TODO: SHORTEN FUNCTION
    fn is_shy(&self, attacked_pos_by_rule_idx_vars: &PositionsByRuleIdxVariables) -> bool {
        self.join_variables()
            .iter()
            .filter(|var| var.appears_in_multiple_positive_body_atoms(self))
            .all(|var| !var.is_attacked(self, attacked_pos_by_rule_idx_vars))
            && self
                .frontier_variable_pairs()
                .iter()
                .filter(|var_pair| var_pair.appear_in_different_positive_body_atoms(self))
                .all(|VariablePair([var1, var2])| {
                    attacked_pos_by_rule_idx_vars.0.values().all(|ex_var_pos| {
                        !var1.is_attacked_by_positions_in_rule(self, ex_var_pos)
                            || !var2.is_attacked_by_positions_in_rule(self, ex_var_pos)
                    })
                })
    }

    // fn is_mfa(&self) -> bool {
    //     todo!("IMPLEMENT");
    //     // TODO: IMPLEMENT
    // }
    //
    // fn is_dmfa(&self) -> bool {
    //     todo!("IMPLEMENT");
    //     // TODO: IMPLEMENT
    // }
    //
    // fn is_rmfa(&self) -> bool {
    //     todo!("IMPLEMENT");
    //     // TODO: IMPLEMENT
    // }
    //
    // fn is_mfc(&self) -> bool {
    //     todo!("IMPLEMENT");
    //     // TODO: IMPLEMENT
    // }
    //
    // fn is_dmfc(&self) -> bool {
    //     todo!("IMPLEMENT");
    //     // TODO: IMPLEMENT
    // }
    //
    // fn is_drpc(&self) -> bool {
    //     todo!("IMPLEMENT");
    //     // TODO: IMPLEMENT
    // }
    //
    // fn is_rpc(&self) -> bool {
    //     todo!("IMPLEMENT");
    //     // TODO: IMPLEMENT
    // }
}
