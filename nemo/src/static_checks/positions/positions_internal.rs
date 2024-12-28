use crate::static_checks::collection_traits::InsertAll;
use crate::static_checks::positions::positions_internal::private::{
    AttackedPositionsBuilderInternalPrivate, SpecialPositionsBuilderInternalPrivate,
};
use crate::static_checks::positions::{
    AffectedPositions, AttackedPositions, AttackingVariables, MarkedPositions, Positions,
};
use crate::static_checks::rule_set::{RuleSet, Variables};

pub(crate) trait SpecialPositionsBuilderInternal<'a>:
    SpecialPositionsBuilderInternalPrivate<'a>
{
    fn build_positions_internal(rule_set: &'a RuleSet) -> Self;
}

impl<'a> SpecialPositionsBuilderInternal<'a> for AffectedPositions<'a> {
    fn build_positions_internal(rule_set: &'a RuleSet) -> AffectedPositions<'a> {
        let mut affected_positions: Positions<'a> = AffectedPositions::initial_positions(rule_set);
        let mut new_found_affected_positions: Positions<'a> = affected_positions.clone();
        while !new_found_affected_positions.is_empty() {
            new_found_affected_positions = AffectedPositions::new_positions(
                rule_set,
                new_found_affected_positions,
                &affected_positions,
            );
            affected_positions.insert_all(&new_found_affected_positions);
        }
        affected_positions
    }
}

impl<'a> SpecialPositionsBuilderInternal<'a> for MarkedPositions<'a> {
    fn build_positions_internal(rule_set: &'a RuleSet) -> MarkedPositions<'a> {
        let mut marking: Positions = MarkedPositions::initial_positions(rule_set).unwrap();
        let mut new_found_marked_positions: Positions = marking.clone();
        while !new_found_marked_positions.is_empty() {
            new_found_marked_positions =
                MarkedPositions::new_positions(rule_set, new_found_marked_positions, &marking)?;
            marking.insert_all(&new_found_marked_positions);
        }
        Some(marking)
    }
}

pub(crate) trait AttackedPositionsBuilderInternal<'a>:
    AttackedPositionsBuilderInternalPrivate<'a>
{
    fn build_positions_internal(att_vars: AttackingVariables, rule_set: &'a RuleSet) -> Self;
}

impl<'a> AttackedPositionsBuilderInternal<'a> for AttackedPositions<'a, 'a> {
    fn build_positions_internal(
        att_vars: AttackingVariables,
        rule_set: &'a RuleSet,
    ) -> AttackedPositions<'a, 'a> {
        let att_variables: Variables =
            AttackedPositions::match_attacking_variables(att_vars, rule_set);
        att_variables
            .iter()
            .map(|var| {
                (
                    *var,
                    AttackedPositions::attacked_positions_by_var(rule_set, var),
                )
            })
            .collect::<AttackedPositions<'a, 'a>>()
    }
}

mod private {
    use crate::rule_model::components::{
        atom::Atom, rule::Rule, term::primitive::variable::Variable,
    };
    use crate::static_checks::acyclicity_graph_constructor::AcyclicityGraphConstructor;
    use crate::static_checks::acyclicity_graphs::{
        JointlyAcyclicityGraph, JointlyAcyclicityGraphCycle,
    };
    use crate::static_checks::collection_traits::{InsertAll, RemoveAll};
    use crate::static_checks::positions::{
        AffectedPositions, AttackedPositions, AttackingVariables, MarkedPositions, Positions,
    };
    use crate::static_checks::rule_set::{
        AtomPositionsAppearance, AtomRefs, Attacked, ExistentialVariables,
        ExistentialVariablesPositions, JoinVariablesPositions, RulePositions, RuleRefs, RuleSet,
        Variables,
    };

    pub(crate) trait SpecialPositionsBuilderInternalPrivate<'a> {
        fn conclude_positions(
            rule_set: &'a RuleSet,
            last_iteration_positions: Positions<'a>,
        ) -> Self;
        fn initial_positions(rule_set: &'a RuleSet) -> Self;
        fn new_positions(
            rule_set: &'a RuleSet,
            last_iteration_positions: Positions<'a>,
            currently_affected_positions: &Positions<'a>,
        ) -> Self;
    }

    impl<'a> SpecialPositionsBuilderInternalPrivate<'a> for AffectedPositions<'a> {
        fn initial_positions(rule_set: &'a RuleSet) -> Positions<'a> {
            rule_set
                .iter()
                .fold(Positions::new(), |initial_affected_positions, rule| {
                    let pos_of_ex_vars: Positions = rule.positions_of_existential_variables();
                    initial_affected_positions.insert_all_take_ret(pos_of_ex_vars)
                })
        }

        fn conclude_positions(
            rule_set: &'a RuleSet,
            last_iteration_positions: Positions<'a>,
        ) -> Positions<'a> {
            rule_set
                .iter()
                .fold(Positions::new(), |new_found_affected_positions, rule| {
                    let new_aff_pos_in_rule: Positions =
                        rule.conclude_affected_positions(&last_iteration_positions);
                    new_found_affected_positions.insert_all_take_ret(new_aff_pos_in_rule)
                })
        }

        fn new_positions(
            rule_set: &'a RuleSet,
            last_iteration_positions: Positions<'a>,
            currently_positions: &Positions<'a>,
        ) -> Positions<'a> {
            let new_found_affected_positions: Positions<'a> =
                AffectedPositions::conclude_positions(rule_set, last_iteration_positions);
            new_found_affected_positions.remove_all_ret(currently_positions)
        }
    }

    impl<'a> SpecialPositionsBuilderInternalPrivate<'a> for MarkedPositions<'a> {
        fn conclude_positions(
            rule_set: &'a RuleSet,
            last_iteration_positions: Positions<'a>,
        ) -> MarkedPositions<'a> {
            rule_set
                .iter()
                .try_fold(Positions::new(), |new_found_marked_positions, rule| {
                    let new_mar_pos_in_rule: Positions =
                        rule.conclude_marked_positions(&last_iteration_positions)?;
                    Some(new_found_marked_positions.insert_all_take_ret(new_mar_pos_in_rule))
                })
        }

        fn initial_positions(rule_set: &'a RuleSet) -> MarkedPositions<'a> {
            Some(
                rule_set
                    .iter()
                    .fold(Positions::new(), |initial_marked_positions, rule| {
                        let pos_of_join_vars: Positions = rule.positions_of_join_variables();
                        initial_marked_positions.insert_all_take_ret(pos_of_join_vars)
                    }),
            )
        }

        fn new_positions(
            rule_set: &'a RuleSet,
            last_iteration_positions: Positions<'a>,
            current_positions: &Positions<'a>,
        ) -> MarkedPositions<'a> {
            let new_found_marked_positions: Positions =
                MarkedPositions::conclude_positions(rule_set, last_iteration_positions)?;
            Some(new_found_marked_positions.remove_all_ret(current_positions))
        }
    }

    pub(crate) trait AttackedPositionsBuilderInternalPrivate<'a> {
        fn attacked_positions_by_var(
            rule_set: &'a RuleSet,
            variable: &'a Variable,
        ) -> Positions<'a>;
        fn conclude_positions(
            rule_set: &'a RuleSet,
            currently_attacked_positions: &Positions<'a>,
        ) -> Positions<'a>;
        fn initial_positions(rule_set: &'a RuleSet, variable: &'a Variable) -> Positions<'a>;
        fn match_attacking_variables(att_vars: AttackingVariables, rule_set: &RuleSet)
            -> Variables;
        fn new_positions(rule_set: &'a RuleSet, current_positions: &Positions<'a>)
            -> Positions<'a>;
    }

    impl<'a> AttackedPositionsBuilderInternalPrivate<'a> for AttackedPositions<'a, 'a> {
        fn attacked_positions_by_var(
            rule_set: &'a RuleSet,
            variable: &'a Variable,
        ) -> Positions<'a> {
            // let mut attacked_positions: Positions =
            //     self.initial_attacked_positions(variable , rule);
            let mut attacked_positions: Positions =
                AttackedPositions::initial_positions(rule_set, variable);
            let mut new_found_attacked_positions: Positions = attacked_positions.clone();
            while !new_found_attacked_positions.is_empty() {
                new_found_attacked_positions =
                    AttackedPositions::new_positions(rule_set, &attacked_positions);
                attacked_positions.insert_all(&new_found_attacked_positions);
            }
            attacked_positions
        }

        fn conclude_positions(
            rule_set: &'a RuleSet,
            current_positions: &Positions<'a>,
        ) -> Positions<'a> {
            rule_set
                .iter()
                .fold(Positions::new(), |new_found_attacked_positions, rule| {
                    let new_att_pos_in_rule: Positions =
                        rule.conclude_attacked_positions(current_positions);
                    new_found_attacked_positions.insert_all_take_ret(new_att_pos_in_rule)
                })
        }

        fn initial_positions(rule_set: &'a RuleSet, variable: &'a Variable) -> Positions<'a> {
            rule_set.iter().fold(Positions::new(), |initial_pos, rule| {
                let initial_pos_of_rule: Positions = variable.positions_in_head(rule);
                initial_pos.insert_all_take_ret(initial_pos_of_rule)
            })
        }

        fn match_attacking_variables(
            att_vars: AttackingVariables,
            rule_set: &RuleSet,
        ) -> Variables {
            match att_vars {
                AttackingVariables::Cycle => {
                    let jo_ac_graph: JointlyAcyclicityGraph = rule_set.jointly_acyclicity_graph();
                    jo_ac_graph.variables_in_cycles()
                }
                AttackingVariables::Existential => rule_set.existential_variables(),
            }
        }

        fn new_positions(
            rule_set: &'a RuleSet,
            currently_attacked_postions: &Positions<'a>,
        ) -> Positions<'a> {
            let new_found_attacked_positions: Positions =
                AttackedPositions::conclude_positions(rule_set, currently_attacked_postions);
            new_found_attacked_positions.remove_all_ret(currently_attacked_postions)
        }
    }

    trait ConcludeAffectedPositions {
        fn conclude_affected_positions(
            &self,
            last_iteration_positions: &Positions,
        ) -> AffectedPositions;
    }

    impl ConcludeAffectedPositions for Rule {
        fn conclude_affected_positions(&self, last_iteration_positions: &Positions) -> Positions {
            self.positive_variables()
                .iter()
                .filter(|var| {
                    let positive_body_atoms: Vec<&Atom> = self.body_positive_refs();
                    var.appears_at_positions_in_atoms(
                        last_iteration_positions,
                        &positive_body_atoms,
                    )
                })
                .fold(Positions::new(), |new_aff_pos_in_rule, var| {
                    let pos_of_var_in_head: Positions = var.positions_in_head(self);
                    new_aff_pos_in_rule.insert_all_take_ret(pos_of_var_in_head)
                })
        }
    }

    trait ConcludeAttackedPositions {
        fn conclude_attacked_positions(
            &self,
            currently_attacked_positions: &Positions,
        ) -> Positions;
    }

    impl ConcludeAttackedPositions for Rule {
        fn conclude_attacked_positions(
            &self,
            currently_attacked_positions: &Positions,
        ) -> Positions {
            self.positive_variables()
                .iter()
                .filter(|var| {
                    var.is_attacked_by_positions_in_rule(self, currently_attacked_positions)
                })
                .fold(Positions::new(), |new_att_pos_in_rule, var| {
                    let pos_of_var_in_head: Positions = var.positions_in_head(self);
                    new_att_pos_in_rule.insert_all_take_ret(pos_of_var_in_head)
                })
        }
    }

    trait ConcludeMarkedPositions {
        fn conclude_marked_positions(
            &self,
            last_iteration_positions: &Positions,
        ) -> MarkedPositions;
    }

    impl ConcludeMarkedPositions for Rule {
        fn conclude_marked_positions(
            &self,
            last_iteration_positions: &Positions,
        ) -> Option<Positions> {
            self.positive_variables()
                .iter()
                .filter(|var| {
                    let positive_body_atoms: Vec<&Atom> = self.body_positive_refs();
                    var.appears_at_positions_in_atoms(
                        last_iteration_positions,
                        &positive_body_atoms,
                    )
                })
                .try_fold(Positions::new(), |new_mar_pos_in_rule, var| {
                    if self
                        .head()
                        .iter()
                        .any(|atom| !atom.variables_refs().contains(var))
                    {
                        return None;
                    }
                    let pos_of_var_in_head: Positions = var.positions_in_head(self);
                    Some(new_mar_pos_in_rule.insert_all_take_ret(pos_of_var_in_head))
                })
        }
    }
}
