use crate::rule_model::components::{
    atom::Atom, rule::Rule, tag::Tag, term::primitive::variable::Variable,
};
use crate::static_checks::acyclicity_graph_constructor::AcyclicityGraphConstructor;
use crate::static_checks::acyclicity_graphs::{
    InfiniteRankPositions, JointlyAcyclicityGraph, JointlyAcyclicityGraphCycle,
    WeaklyAcyclicityGraph,
};
use crate::static_checks::collection_traits::{Disjoint, InsertAll, RemoveAll, Superset};
use crate::static_checks::rule_set::{
    AtomPositionsAppearance, AtomRefs, Attacked, ExistentialVariables,
    ExistentialVariablesPositions, JoinVariables, RulePositions, RuleRefs, RuleSet, Variables,
};

use std::collections::{HashMap, HashSet};

pub type Index = usize;

pub type Indices = HashSet<Index>;

pub type Positions<'a> = HashMap<&'a Tag, Indices>;

pub type PositionsByVariables<'a, 'b> = HashMap<&'a Variable, Positions<'b>>;

pub type Position<'a> = (&'a Tag, Index);

pub type ExtendedPositions<'a> = HashSet<Position<'a>>;

pub enum AttackingType {
    Cycle,
    Existential,
}

pub enum MarkingType {
    Common,
    Weakly,
}

pub trait FromPositions<'a> {
    fn from_positions(positions: Positions<'a>) -> Self;
}

impl<'a> FromPositions<'a> for ExtendedPositions<'a> {
    fn from_positions(positions: Positions<'a>) -> ExtendedPositions<'a> {
        positions
            .into_iter()
            .fold(ExtendedPositions::new(), |ex_pos, (pred, indices)| {
                let pred_pos: ExtendedPositions =
                    indices.into_iter().map(|index| (pred, index)).collect();
                ex_pos.insert_all_take_ret(pred_pos)
            })
    }
}

pub trait AffectedPositionsBuilder<'a> {
    fn build_positions(rule_set: &'a RuleSet) -> Self;
}

impl<'a> AffectedPositionsBuilder<'a> for Positions<'a> {
    fn build_positions(rule_set: &'a RuleSet) -> Positions<'a> {
        let mut affected_positions: Positions<'a> = Positions::initial_affected_positions(rule_set);
        let mut new_found_affected_positions: Positions<'a> = affected_positions.clone();
        while !new_found_affected_positions.is_empty() {
            new_found_affected_positions = Positions::new_affected_positions(
                rule_set,
                new_found_affected_positions,
                &affected_positions,
            );
            affected_positions.insert_all(&new_found_affected_positions);
        }
        affected_positions
    }
}

trait AffectedPositionsBuilderPrivate<'a> {
    fn conclude_affected_positions(
        rule_set: &'a RuleSet,
        last_iteration_positions: Positions<'a>,
    ) -> Positions<'a>;
    fn initial_affected_positions(rule_set: &'a RuleSet) -> Positions<'a>;
    fn new_affected_positions(
        rule_set: &'a RuleSet,
        last_iteration_positions: Positions<'a>,
        currently_affected_positions: &Positions<'a>,
    ) -> Positions<'a>;
}

impl<'a> AffectedPositionsBuilderPrivate<'a> for Positions<'a> {
    fn initial_affected_positions(rule_set: &'a RuleSet) -> Positions<'a> {
        rule_set
            .iter()
            .fold(Positions::new(), |initial_affected_positions, rule| {
                let pos_of_ex_vars: Positions = rule.positions_of_existential_variables();
                initial_affected_positions.insert_all_take_ret(pos_of_ex_vars)
            })
    }

    fn conclude_affected_positions(
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

    fn new_affected_positions(
        rule_set: &'a RuleSet,
        last_iteration_positions: Positions<'a>,
        currently_positions: &Positions<'a>,
    ) -> Positions<'a> {
        let new_found_affected_positions: Positions<'a> =
            Positions::conclude_affected_positions(rule_set, last_iteration_positions);
        new_found_affected_positions.remove_all_ret(currently_positions)
    }
}

trait AffectedPositionsBuilderRulePrivate {
    fn conclude_affected_positions(&self, last_iteration_positions: &Positions) -> Positions;
}

impl AffectedPositionsBuilderRulePrivate for Rule {
    fn conclude_affected_positions(&self, last_iteration_positions: &Positions) -> Positions {
        self.positive_variables()
            .iter()
            .filter(|var| {
                let positive_body_atoms: Vec<&Atom> = self.body_positive_refs();
                var.appears_at_some_positions_in_atoms(
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

pub trait AttackedPositionsBuilder<'a> {
    fn build_positions(att_type: AttackingType, rule_set: &'a RuleSet) -> Self;
}

impl<'a> AttackedPositionsBuilder<'a> for PositionsByVariables<'a, 'a> {
    fn build_positions(
        att_type: AttackingType,
        rule_set: &'a RuleSet,
    ) -> PositionsByVariables<'a, 'a> {
        let att_variables: Variables =
            PositionsByVariables::match_attacking_variables(att_type, rule_set);
        att_variables
            .iter()
            .map(|var| {
                (
                    *var,
                    PositionsByVariables::attacked_positions_by_var(rule_set, var),
                )
            })
            .collect::<PositionsByVariables<'a, 'a>>()
    }
}

trait AttackedPositionsBuilderPrivate<'a> {
    fn attacked_positions_by_var(rule_set: &'a RuleSet, variable: &'a Variable) -> Positions<'a>;
    fn conclude_positions(
        rule_set: &'a RuleSet,
        currently_attacked_positions: &Positions<'a>,
    ) -> Positions<'a>;
    fn initial_positions(rule_set: &'a RuleSet, variable: &'a Variable) -> Positions<'a>;
    fn match_attacking_variables(att_type: AttackingType, rule_set: &RuleSet) -> Variables;
    fn new_positions(rule_set: &'a RuleSet, current_positions: &Positions<'a>) -> Positions<'a>;
}

trait AttackedPositionsBuilderRulePrivate {
    fn conclude_attacked_positions(&self, currently_attacked_positions: &Positions) -> Positions;
}

impl AttackedPositionsBuilderRulePrivate for Rule {
    fn conclude_attacked_positions(&self, currently_attacked_positions: &Positions) -> Positions {
        self.positive_variables()
            .iter()
            .filter(|var| var.is_attacked_by_positions_in_rule(self, currently_attacked_positions))
            .fold(Positions::new(), |new_att_pos_in_rule, var| {
                let pos_of_var_in_head: Positions = var.positions_in_head(self);
                new_att_pos_in_rule.insert_all_take_ret(pos_of_var_in_head)
            })
    }
}

impl<'a> AttackedPositionsBuilderPrivate<'a> for PositionsByVariables<'a, 'a> {
    fn attacked_positions_by_var(rule_set: &'a RuleSet, variable: &'a Variable) -> Positions<'a> {
        // let mut attacked_positions: Positions =
        //     self.initial_attacked_positions(variable , rule);
        let mut attacked_positions: Positions =
            PositionsByVariables::initial_positions(rule_set, variable);
        let mut new_found_attacked_positions: Positions = attacked_positions.clone();
        while !new_found_attacked_positions.is_empty() {
            new_found_attacked_positions =
                PositionsByVariables::new_positions(rule_set, &attacked_positions);
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

    fn match_attacking_variables(att_type: AttackingType, rule_set: &RuleSet) -> Variables {
        match att_type {
            AttackingType::Cycle => {
                let jo_ac_graph: JointlyAcyclicityGraph = rule_set.jointly_acyclicity_graph();
                jo_ac_graph.variables_in_cycles()
            }
            AttackingType::Existential => rule_set.existential_variables(),
        }
    }

    fn new_positions(
        rule_set: &'a RuleSet,
        currently_attacked_postions: &Positions<'a>,
    ) -> Positions<'a> {
        let new_found_attacked_positions: Positions =
            PositionsByVariables::conclude_positions(rule_set, currently_attacked_postions);
        new_found_attacked_positions.remove_all_ret(currently_attacked_postions)
    }
}

pub trait MarkedPositionsBuilder<'a> {
    fn build_positions(mar_type: MarkingType, rule_set: &'a RuleSet) -> Self;
}

impl<'a> MarkedPositionsBuilder<'a> for Option<Positions<'a>> {
    fn build_positions(mar_type: MarkingType, rule_set: &'a RuleSet) -> Option<Positions<'a>> {
        let mut marking: Positions =
            Option::<Positions>::match_initial_marked_positions(mar_type, rule_set)?;
        let mut new_found_marked_positions: Positions = marking.clone();
        while !new_found_marked_positions.is_empty() {
            new_found_marked_positions = Option::<Positions>::new_marked_positions(
                rule_set,
                new_found_marked_positions,
                &marking,
            )?;
            marking.insert_all(&new_found_marked_positions);
        }
        Some(marking)
    }
}

trait MarkedPositionsBuilderPrivate<'a> {
    fn conclude_marked_positions(
        rule_set: &'a RuleSet,
        last_iteration_positions: Positions<'a>,
    ) -> Option<Positions<'a>>;
    fn initial_marked_positions(rule_set: &'a RuleSet) -> Option<Positions<'a>>;
    fn initial_weakly_marked_positions(rule_set: &'a RuleSet) -> Option<Positions<'a>>;
    fn match_initial_marked_positions(
        mar_type: MarkingType,
        rule_set: &'a RuleSet,
    ) -> Option<Positions<'a>>;
    fn new_marked_positions(
        rule_set: &'a RuleSet,
        last_iteration_positions: Positions<'a>,
        currently_affected_positions: &Positions<'a>,
    ) -> Option<Positions<'a>>;
}

impl<'a> MarkedPositionsBuilderPrivate<'a> for Option<Positions<'a>> {
    fn conclude_marked_positions(
        rule_set: &'a RuleSet,
        last_iteration_positions: Positions<'a>,
    ) -> Option<Positions<'a>> {
        rule_set
            .iter()
            .try_fold(Positions::new(), |new_found_marked_positions, rule| {
                let new_mar_pos_in_rule: Positions =
                    rule.conclude_marked_positions(&last_iteration_positions)?;
                Some(new_found_marked_positions.insert_all_take_ret(new_mar_pos_in_rule))
            })
    }

    fn initial_marked_positions(rule_set: &'a RuleSet) -> Option<Positions<'a>> {
        // rule_set
        //     .iter()
        //     .fold(Positions::new(), |initial_marked_positions, rule| {
        //         let pos_of_join_vars: Positions = rule.positions_of_join_variables();
        //         initial_marked_positions.insert_all_take_ret(pos_of_join_vars)
        //     })
        rule_set
            .iter()
            .try_fold(Positions::new(), |init_mar_pos, rule| {
                let init_mar_pos_of_rule: Positions = rule.initial_marked_positions()?;
                Some(init_mar_pos.insert_all_take_ret(init_mar_pos_of_rule))
            })
    }

    fn initial_weakly_marked_positions(rule_set: &'a RuleSet) -> Option<Positions<'a>> {
        let we_ac_graph: WeaklyAcyclicityGraph = rule_set.weakly_acyclicity_graph();
        let infinite_rank_positions: Positions = we_ac_graph.infinite_rank_positions();
        rule_set
            .iter()
            .try_fold(Positions::new(), |init_we_mar_pos, rule| {
                let init_we_mar_pos_of_rule: Positions =
                    rule.initial_weakly_marked_positions(&infinite_rank_positions)?;
                Some(init_we_mar_pos.insert_all_take_ret(init_we_mar_pos_of_rule))
            })
    }

    fn match_initial_marked_positions(
        mar_type: MarkingType,
        rule_set: &'a RuleSet,
    ) -> Option<Positions<'a>> {
        match mar_type {
            MarkingType::Common => Option::<Positions>::initial_marked_positions(rule_set),
            MarkingType::Weakly => Option::<Positions>::initial_weakly_marked_positions(rule_set),
        }
    }

    fn new_marked_positions(
        rule_set: &'a RuleSet,
        last_iteration_positions: Positions<'a>,
        current_positions: &Positions<'a>,
    ) -> Option<Positions<'a>> {
        let new_found_marked_positions: Positions =
            Option::<Positions>::conclude_marked_positions(rule_set, last_iteration_positions)?;
        Some(new_found_marked_positions.remove_all_ret(current_positions))
    }
}

trait RuleMarkedPositionsBuilderPrivate {
    fn conclude_marked_positions(&self, last_iteration_positions: &Positions) -> Option<Positions>;
    fn initial_marked_positions(&self) -> Option<Positions>;
    fn initial_weakly_marked_positions(
        &self,
        infinite_rank_positions: &Positions,
    ) -> Option<Positions>;
}

impl RuleMarkedPositionsBuilderPrivate for Rule {
    fn conclude_marked_positions(&self, last_iteration_positions: &Positions) -> Option<Positions> {
        self.positive_variables()
            .iter()
            .filter(|var| {
                let positive_body_atoms: Vec<&Atom> = self.body_positive_refs();
                var.appears_at_some_positions_in_atoms(
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

    fn initial_marked_positions(&self) -> Option<Positions> {
        let join_vars: Variables = self.join_variables();
        join_vars
            .iter()
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

    fn initial_weakly_marked_positions(
        &self,
        infinite_rank_positions: &Positions,
    ) -> Option<Positions> {
        let join_vars: Variables = self.join_variables();
        join_vars
            .iter()
            .filter(|var| {
                let positive_body_atoms: Vec<&Atom> = self.body_positive_refs();
                var.appears_only_at_positions_in_atoms(
                    infinite_rank_positions,
                    &positive_body_atoms,
                )
            })
            .try_fold(Positions::new(), |new_we_mar_pos_in_rule, var| {
                if self
                    .head()
                    .iter()
                    .any(|atom| !atom.variables_refs().contains(var))
                {
                    return None;
                }
                let pos_of_var_in_head: Positions = var.positions_in_head(self);
                Some(new_we_mar_pos_in_rule.insert_all_take_ret(pos_of_var_in_head))
            })
    }
}

impl<'a> Disjoint for Positions<'a> {
    fn is_disjoint(&self, other: &Positions<'a>) -> bool {
        self.keys().all(|pred| {
            !other.contains_key(pred) || {
                let self_indices: &Indices = self.get(pred).unwrap();
                let other_indices: &Indices = other.get(pred).unwrap();
                self_indices.is_disjoint(other_indices)
            }
        })
    }
}

impl<'a> InsertAll<Positions<'a>, (&'a Tag, Indices)> for Positions<'a> {
    fn insert_all(&mut self, other: &Positions<'a>) {
        other.iter().for_each(|(pred, other_indices)| {
            if !self.contains_key(pred) {
                self.insert(pred, Indices::new());
            }
            let unioned_indices: &mut Indices = self.get_mut(pred).unwrap();
            unioned_indices.insert_all(other_indices);
        })
    }

    fn insert_all_ret(mut self, other: &Positions<'a>) -> Positions<'a> {
        self.insert_all(other);
        self
    }

    fn insert_all_take(&mut self, other: Positions<'a>) {
        other.into_iter().for_each(|(pred, other_indices)| {
            if !self.contains_key(pred) {
                self.insert(pred, Indices::new());
            }
            let unioned_indices: &mut Indices = self.get_mut(pred).unwrap();
            unioned_indices.insert_all_take(other_indices);
        })
    }

    fn insert_all_take_ret(mut self, other: Positions<'a>) -> Positions<'a> {
        self.insert_all_take(other);
        self
    }
}

impl<'a> RemoveAll<Positions<'a>, (&'a Tag, Indices)> for Positions<'a> {
    fn remove_all(&mut self, other: &Positions<'a>) {
        other.iter().for_each(|(pred, other_indices)| {
            if let Some(differenced_indices) = self.get_mut(pred) {
                differenced_indices.remove_all(other_indices);
            }
        })
    }

    fn remove_all_ret(mut self, other: &Positions<'a>) -> Positions<'a> {
        self.remove_all(other);
        self
    }

    fn remove_all_take(&mut self, other: Positions<'a>) {
        other.into_iter().for_each(|(pred, other_indices)| {
            if let Some(differenced_indices) = self.get_mut(pred) {
                differenced_indices.remove_all_take(other_indices);
            }
        })
    }

    fn remove_all_take_ret(mut self, other: Positions<'a>) -> Positions<'a> {
        self.remove_all_take(other);
        self
    }
}

impl<'a> Superset for Positions<'a> {
    fn is_superset(&self, other: &Positions<'a>) -> bool {
        other.keys().all(|pred| {
            self.contains_key(pred) && {
                let self_indices: &Indices = self.get(pred).unwrap();
                let other_indices: &Indices = other.get(pred).unwrap();
                self_indices.is_superset(other_indices)
            }
        })
    }
}
