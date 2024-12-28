use crate::rule_model::components::{rule::Rule, IterablePrimitives};
use crate::static_checks::positions::{Positions, PositionsByVariables};
use crate::static_checks::rule_set::{
    AffectedVariables, Attacked, AttackedGlutVariables, AttackedVariables, ExistentialVariables,
    FrontierVariablePairs, FrontierVariables, GuardedForVariables, JoinVariables,
    UniversalVariables, Variables,
};

pub(crate) trait RulePropertiesInternal {
    fn is_joinless_internal(&self) -> bool;
    fn is_linear_internal(&self) -> bool;
    fn is_guarded_internal(&self) -> bool;
    fn is_domain_restricted_internal(&self) -> bool;
    fn is_frontier_one_internal(&self) -> bool;
    fn is_datalog_internal(&self) -> bool;
    fn is_monadic_internal(&self) -> bool;
    fn is_frontier_guarded_internal(&self) -> bool;
    fn is_weakly_guarded_internal(&self, affected_positions: &Positions) -> bool;
    fn is_weakly_frontier_guarded_internal(&self, affected_positions: &Positions) -> bool;
    fn is_jointly_guarded_internal(&self, attacked_pos_by_vars: &PositionsByVariables) -> bool;
    fn is_jointly_frontier_guarded_internal(
        &self,
        attacked_pos_by_vars: &PositionsByVariables,
    ) -> bool;
    fn is_weakly_sticky_internal(&self) -> bool;
    fn is_glut_guarded_internal(&self, attacked_pos_by_cycle_vars: &PositionsByVariables) -> bool;
    fn is_glut_frontier_guarded_internal(
        &self,
        attacked_pos_by_cycle_vars: &PositionsByVariables,
    ) -> bool;
    fn is_shy_internal(&self, attacked_pos_by_vars: &PositionsByVariables) -> bool;
    fn is_mfa_internal(&self) -> bool;
    fn is_dmfa_internal(&self) -> bool;
    fn is_rmfa_internal(&self) -> bool;
    fn is_mfc_internal(&self) -> bool;
    fn is_dmfc_internal(&self) -> bool;
    fn is_drpc_internal(&self) -> bool;
    fn is_rpc_internal(&self) -> bool;
}

impl RulePropertiesInternal for Rule {
    fn is_joinless_internal(&self) -> bool {
        self.join_variables().is_empty()
    }

    fn is_linear_internal(&self) -> bool {
        1 >= self.body().len()
    }

    fn is_guarded_internal(&self) -> bool {
        let positive_variables: Variables = self.positive_variables();
        self.is_guarded_for_variables(positive_variables)
    }

    fn is_domain_restricted_internal(&self) -> bool {
        let positive_body_variables: Variables = self.positive_variables();
        self.head().iter().all(|atom| {
            let universal_variables_of_atom: Variables = atom.universal_variables();
            universal_variables_of_atom.is_empty()
                || universal_variables_of_atom == positive_body_variables
        })
    }

    fn is_frontier_one_internal(&self) -> bool {
        1 >= self.frontier_variables().len()
    }

    fn is_datalog_internal(&self) -> bool {
        self.existential_variables().is_empty()
    }

    fn is_monadic_internal(&self) -> bool {
        self.head()
            .iter()
            .all(|atom| 1 == atom.primitive_terms().count())
    }

    fn is_frontier_guarded_internal(&self) -> bool {
        let frontier_variables: Variables = self.frontier_variables();
        self.is_guarded_for_variables(frontier_variables)
    }

    fn is_weakly_guarded_internal(&self, affected_positions: &Positions) -> bool {
        let affected_universal_variables: Variables =
            self.affected_universal_variables(affected_positions);
        self.is_guarded_for_variables(affected_universal_variables)
    }

    fn is_weakly_frontier_guarded_internal(&self, affected_positions: &Positions) -> bool {
        let affected_frontier_variables: Variables =
            self.affected_frontier_variables(affected_positions);
        self.is_guarded_for_variables(affected_frontier_variables)
    }

    fn is_jointly_guarded_internal(&self, attacked_pos_by_vars: &PositionsByVariables) -> bool {
        let attacked_universal_variables: Variables =
            self.attacked_universal_variables(attacked_pos_by_vars);
        self.is_guarded_for_variables(attacked_universal_variables)
    }

    fn is_jointly_frontier_guarded_internal(
        &self,
        attacked_pos_by_vars: &PositionsByVariables,
    ) -> bool {
        let attacked_frontier_variables: Variables =
            self.attacked_frontier_variables(attacked_pos_by_vars);
        self.is_guarded_for_variables(attacked_frontier_variables)
    }

    fn is_weakly_sticky_internal(&self) -> bool {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn is_glut_guarded_internal(&self, attacked_pos_by_cycle_vars: &PositionsByVariables) -> bool {
        let attacked_universal_glut_variables: Variables =
            self.attacked_universal_glut_variables(attacked_pos_by_cycle_vars);
        self.is_guarded_for_variables(attacked_universal_glut_variables)
    }

    fn is_glut_frontier_guarded_internal(
        &self,
        attacked_pos_by_cycle_vars: &PositionsByVariables,
    ) -> bool {
        let attacked_frontier_glut_variables: Variables =
            self.attacked_frontier_glut_variables(attacked_pos_by_cycle_vars);
        self.is_guarded_for_variables(attacked_frontier_glut_variables)
    }

    // TODO: SHORTEN FUNCTION
    fn is_shy_internal(&self, attacked_pos_by_vars: &PositionsByVariables) -> bool {
        self.join_variables()
            .iter()
            .all(|var| !var.is_attacked(self, attacked_pos_by_vars))
            && self.frontier_variable_pairs().iter().all(|[var1, var2]| {
                attacked_pos_by_vars.values().all(|ex_var_pos| {
                    !var1.is_attacked_by_positions_in_rule(self, ex_var_pos)
                        || !var2.is_attacked_by_positions_in_rule(self, ex_var_pos)
                })
            })
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
