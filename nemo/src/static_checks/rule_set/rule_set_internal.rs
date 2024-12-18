use crate::rule_model::components::term::primitive::variable::Variable;
use crate::static_checks::acyclicity_graph_constructor::AcyclicityGraphConstructor;
use crate::static_checks::acyclicity_graphs::{
    JointlyAcyclicityGraph, JointlyAcyclicityGraphCycle,
};
use crate::static_checks::collection_traits::InsertAll;
use crate::static_checks::positions::{Positions, PositionsByVariables};
use crate::static_checks::rule_set::rule_set_internal::private::SpecialPositionsInternalPrivate;
use crate::static_checks::rule_set::{ExistentialVariables, RuleSet};

use std::collections::HashSet;

pub(crate) trait SpecialPositionsInternal<'a>: SpecialPositionsInternalPrivate<'a> {
    fn affected_positions_internal(&'a self) -> Positions<'a>;
    fn attacked_positions_by_cycle_variables_internal(&'a self) -> PositionsByVariables<'a, 'a>;
    fn attacked_positions_by_existential_variables_internal(
        &'a self,
    ) -> PositionsByVariables<'a, 'a>;
    fn build_and_check_marking_internal(&'a self) -> Option<Positions<'a>>;
}

impl<'a> SpecialPositionsInternal<'a> for RuleSet {
    fn affected_positions_internal(&'a self) -> Positions<'a> {
        let mut affected_positions: Positions<'a> = self.initial_affected_positions();
        let mut new_found_affected_positions: Positions<'a> = affected_positions.clone();
        while !new_found_affected_positions.is_empty() {
            new_found_affected_positions =
                self.new_affected_positions(new_found_affected_positions, &affected_positions);
            affected_positions.insert_all(&new_found_affected_positions);
        }
        affected_positions
    }

    fn attacked_positions_by_cycle_variables_internal(&'a self) -> PositionsByVariables<'a, 'a> {
        let jo_ac_graph: JointlyAcyclicityGraph = self.jointly_acyclicity_graph();
        let cycle_variables: HashSet<&Variable> = jo_ac_graph.variables_in_cycles();
        self.attacked_positions_by_variables(&cycle_variables)
    }

    fn attacked_positions_by_existential_variables_internal(
        &'a self,
    ) -> PositionsByVariables<'a, 'a> {
        let existential_variables: HashSet<&Variable> = self.existential_variables();
        self.attacked_positions_by_variables(&existential_variables)
    }

    fn build_and_check_marking_internal(&'a self) -> Option<Positions<'a>> {
        let mut marking: Positions = self.initial_marked_positions();
        let mut new_found_marked_positions: Positions = marking.clone();
        while !new_found_marked_positions.is_empty() {
            new_found_marked_positions =
                self.new_marked_positions(new_found_marked_positions, &marking)?;
            marking.insert_all(&new_found_marked_positions);
        }
        Some(marking)
    }
}

mod private {
    use crate::rule_model::components::term::primitive::variable::Variable;
    use crate::static_checks::collection_traits::{InsertAll, RemoveAll};
    use crate::static_checks::positions::{Positions, PositionsByVariables};
    use crate::static_checks::rule_set::RuleSet;

    use std::collections::HashSet;

    pub(crate) trait SpecialPositionsInternalPrivate<'a> {
        fn attacked_positions_by_var(&'a self, variable: &'a Variable) -> Positions<'a>;
        fn attacked_positions_by_variables(
            &'a self,
            variables: &HashSet<&'a Variable>,
        ) -> PositionsByVariables<'a, 'a>;
        fn conclude_affected_positions(
            &'a self,
            last_iteration_positions: Positions<'a>,
        ) -> Positions<'a>;
        fn conclude_attacked_positions(
            &'a self,
            currently_attacked_positions: &Positions<'a>,
        ) -> Positions<'a>;
        fn conclude_marked_positions(
            &'a self,
            last_iteration_positions: Positions<'a>,
        ) -> Option<Positions<'a>>;
        fn initial_affected_positions(&'a self) -> Positions<'a>;
        fn initial_attacked_positions(&'a self, variable: &'a Variable) -> Positions<'a>;
        fn initial_marked_positions(&'a self) -> Positions<'a>;
        fn new_affected_positions(
            &'a self,
            last_iteration_positions: Positions<'a>,
            currently_affected_positions: &Positions<'a>,
        ) -> Positions<'a>;
        fn new_attacked_positions(
            &'a self,
            currently_attacked_positions: &Positions<'a>,
        ) -> Positions<'a>;
        fn new_marked_positions(
            &'a self,
            last_iteration_positions: Positions<'a>,
            current_marking: &Positions<'a>,
        ) -> Option<Positions<'a>>;
    }

    impl<'a> SpecialPositionsInternalPrivate<'a> for RuleSet {
        fn attacked_positions_by_var(&'a self, variable: &'a Variable) -> Positions<'a> {
            // let mut attacked_positions: Positions =
            //     self.initial_attacked_positions(variable , rule);
            let mut attacked_positions: Positions = self.initial_attacked_positions(variable);
            let mut new_found_attacked_positions: Positions = attacked_positions.clone();
            while !new_found_attacked_positions.is_empty() {
                new_found_attacked_positions = self.new_attacked_positions(&attacked_positions);
                attacked_positions.insert_all(&new_found_attacked_positions);
            }
            attacked_positions
        }

        fn attacked_positions_by_variables(
            &'a self,
            variables: &HashSet<&'a Variable>,
        ) -> PositionsByVariables<'a, 'a> {
            variables
                .iter()
                .map(|var| (*var, self.attacked_positions_by_var(var)))
                .collect::<PositionsByVariables<'a, 'a>>()
            // self.iter()
            //     .flat_map(|rule| {
            //         rule.existential_variables()
            //             .iter()
            //             .map(|var| (*var, self.attacked_positions_by_var_in_rule(var, rule)))
            //             .collect::<HashMap<&Variable, Positions>>()
            //     })
            //     .collect()
        }

        fn conclude_affected_positions(
            &'a self,
            last_iteration_positions: Positions<'a>,
        ) -> Positions<'a> {
            self.iter()
                .fold(Positions::new(), |new_found_affected_positions, rule| {
                    let new_aff_pos_in_rule: Positions =
                        rule.conclude_affected_positions(&last_iteration_positions);
                    new_found_affected_positions.insert_all_take_ret(new_aff_pos_in_rule)
                })
        }

        fn conclude_attacked_positions(
            &'a self,
            currently_attacked_positions: &Positions<'a>,
        ) -> Positions<'a> {
            self.iter()
                .fold(Positions::new(), |new_found_attacked_positions, rule| {
                    let new_att_pos_in_rule: Positions =
                        rule.conclude_attacked_positions(currently_attacked_positions);
                    new_found_attacked_positions.insert_all_take_ret(new_att_pos_in_rule)
                })
        }

        fn conclude_marked_positions(
            &self,
            last_iteration_positions: Positions,
        ) -> Option<Positions> {
            self.iter()
                .try_fold(Positions::new(), |new_found_marked_positions, rule| {
                    let new_mar_pos_in_rule: Positions =
                        rule.conclude_marked_positions(&last_iteration_positions)?;
                    Some(new_found_marked_positions.insert_all_take_ret(new_mar_pos_in_rule))
                })
        }

        fn initial_affected_positions(&'a self) -> Positions<'a> {
            self.iter()
                .fold(Positions::new(), |initial_affected_positions, rule| {
                    let pos_of_ex_vars: Positions = rule.positions_of_existential_variables();
                    initial_affected_positions.insert_all_take_ret(pos_of_ex_vars)
                })
        }

        fn initial_attacked_positions(&'a self, variable: &'a Variable) -> Positions<'a> {
            self.iter().fold(Positions::new(), |initial_pos, rule| {
                let initial_pos_of_rule: Positions = variable.get_positions_in_head(rule);
                initial_pos.insert_all_take_ret(initial_pos_of_rule)
            })
        }

        fn initial_marked_positions(&'a self) -> Positions<'a> {
            self.iter()
                .fold(Positions::new(), |initial_marked_positions, rule| {
                    let pos_of_join_vars: Positions = rule.positions_of_join_variables();
                    initial_marked_positions.insert_all_take_ret(pos_of_join_vars)
                })
        }

        fn new_affected_positions(
            &'a self,
            last_iteration_positions: Positions<'a>,
            currently_affected_positions: &Positions<'a>,
        ) -> Positions<'a> {
            let new_found_affected_positions: Positions<'a> =
                self.conclude_affected_positions(last_iteration_positions);
            new_found_affected_positions.remove_all_ret(currently_affected_positions)
        }

        fn new_attacked_positions(
            &'a self,
            currently_attacked_postions: &Positions<'a>,
        ) -> Positions<'a> {
            let new_found_attacked_positions: Positions =
                self.conclude_attacked_positions(currently_attacked_postions);
            new_found_attacked_positions.remove_all_ret(currently_attacked_postions)
        }

        fn new_marked_positions(
            &'a self,
            last_iteration_positions: Positions<'a>,
            current_marking: &Positions<'a>,
        ) -> Option<Positions<'a>> {
            let new_found_marked_positions: Positions =
                self.conclude_marked_positions(last_iteration_positions)?;
            Some(new_found_marked_positions.remove_all_ret(current_marking))
        }
    }
}
