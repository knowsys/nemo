//! Functionality that provides methods in relation with Position-Types.
use crate::rule_model::components::{atom::Atom, rule::Rule, tag::Tag};
use crate::static_checks::acyclicity_graphs::{
    AcyclicityGraphBuilder, InfiniteRankPositions, JointAcyclicityGraph, JointAcyclicityGraphCycle, WeakAcyclicityGraph,
};
use crate::static_checks::collection_traits::{Disjoint, InsertAll, RemoveAll, Superset};
use crate::static_checks::rule_set::{
    AtomPositionsAppearance, AtomRefs, Attacked, ExistentialRuleIdxVariables,
    ExistentialVariablesPositions, JoinVariables, RuleIdxVariable, RuleIdxVariables, RulePositions,
    RuleRefs, RuleSet, Variables,
};

use std::collections::{HashMap, HashSet};

/// Type to represent a position in an Atom.
pub type Index = usize;

/// Type to compress a set of Indices.
pub type Indices = HashSet<Index>;

/// Type to map some Atom(s) to some positions in it.
pub type Positions<'a> = HashMap<&'a Tag, Indices>;

/// Type to map some RuleIdxVariable(s) to some Positions.
pub type PositionsByRuleIdxVariables<'a, 'b> = HashMap<RuleIdxVariable<'a>, Positions<'b>>;

/// Type to map some Atom to some position in it.
pub type Position<'a> = (&'a Tag, Index);

/// Type to compress a set of Position(s).
pub type ExtendedPositions<'a> = HashSet<Position<'a>>;

/// Enum to distinguish between (all existential variables / existential variables that appear in
/// cycles in the joint acyclicity graph) to be the attacking variables.
#[derive(Clone, Copy, Debug)]
pub enum AttackingType {
    /// The attacking variables will be all existential variables.
    Cycle,
    /// The attacking variables will be the existential variables that appear in cycles in the
    /// joint acyclicity graph.
    Existential,
}

/// Enum to distinguish between the (common / weakly) marking to be calculated.
#[derive(Clone, Copy, Debug)]
pub enum MarkingType {
    /// The common marking will be calculated.
    Common,
    /// The weakly marking will be calculated.
    Weakly,
}

/// This Trait converts Positions to another type.
pub trait FromPositions<'a> {
    /// Converts Positions to ExtendedPositions.
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

/// This Trait converts ExtendedPositions to another type.
pub trait FromPositionSet<'a> {
    /// Converts ExtendedPositions to Positions.
    fn from_extended_positions(extended_positions: ExtendedPositions<'a>) -> Self;
}

impl<'a> FromPositionSet<'a> for Positions<'a> {
    fn from_extended_positions(extended_positions: HashSet<Position<'a>>) -> Self {
        extended_positions
            .into_iter()
            .fold(Positions::new(), |mut positions, (tag, index)| {
                if !positions.contains_key(tag) {
                    positions.insert(tag, HashSet::<usize>::new());
                }
                positions.get_mut(tag).unwrap().insert(index);
                positions
            })
    }
}

/// This Trait gives all affected positions of some RuleSet.
pub trait AffectedPositionsBuilder<'a> {
    /// Builds all affected positions of some RuleSet.
    fn build_positions(rule_set: &'a RuleSet) -> Self;
}

impl<'a> AffectedPositionsBuilder<'a> for Positions<'a> {
    fn build_positions(rule_set: &'a RuleSet) -> Positions<'a> {
        let mut aff_pos: Positions<'a> = rule_set.initial_affected_positions();
        let mut new_found_aff_pos: Positions<'a> = aff_pos.clone();
        while !new_found_aff_pos.is_empty() {
            let new_con_aff_pos: Positions<'a> =
                rule_set.conclude_affected_positions(&new_found_aff_pos);
            new_found_aff_pos = new_con_aff_pos.remove_all_ret(&aff_pos);
            aff_pos.insert_all(&new_found_aff_pos);
        }
        aff_pos
    }
}

trait AffectedPositionsBuilderPrivate<'a> {
    fn conclude_affected_positions(&'a self, last_it_pos: &Positions<'a>) -> Positions<'a>;
    fn initial_affected_positions(&'a self) -> Positions<'a>;
}

impl<'a> AffectedPositionsBuilderPrivate<'a> for RuleSet {
    fn initial_affected_positions(&'a self) -> Positions<'a> {
        self.iter().fold(Positions::new(), |init_aff_pos, rule| {
            let pos_of_ex_vars: Positions = rule.initial_affected_positions();
            init_aff_pos.insert_all_take_ret(pos_of_ex_vars)
        })
    }

    fn conclude_affected_positions(&'a self, last_it_pos: &Positions<'a>) -> Positions<'a> {
        self.iter().fold(Positions::new(), |new_con_aff_pos, rule| {
            let new_con_aff_pos_in_rule: Positions = rule.conclude_affected_positions(last_it_pos);
            new_con_aff_pos.insert_all_take_ret(new_con_aff_pos_in_rule)
        })
    }
}

impl<'a> AffectedPositionsBuilderPrivate<'a> for Rule {
    fn initial_affected_positions(&'a self) -> Positions<'a> {
        self.positions_of_existential_variables()
    }

    fn conclude_affected_positions(&'a self, last_it_pos: &Positions<'a>) -> Positions<'a> {
        self.positive_variables()
            .iter()
            .filter(|var| {
                let positive_body_atoms: Vec<&Atom> = self.body_positive_refs();
                var.appears_at_some_positions_in_atoms(last_it_pos, &positive_body_atoms)
            })
            .fold(Positions::new(), |new_aff_pos_in_rule, var| {
                let pos_of_var_in_head: Positions = var.positions_in_head(self);
                new_aff_pos_in_rule.insert_all_take_ret(pos_of_var_in_head)
            })
    }
}

/// This Trait gives all attacked positions ordered by (all existential / existential variables which appear
/// in cycles of the joint acyclicity graph) (PositionsByRuleIdxVariables) of some RuleSet.
pub trait AttackedPositionsBuilder<'a> {
    /// Builds all attacked positions ordered by (all existential / existential variables which appea
    /// in cycles of the joint acyclicity graph) (PositionsByRuleIdxVariables) of some RuleSet.
    fn build_positions(att_type: AttackingType, rule_set: &'a RuleSet) -> Self;
}

impl<'a> AttackedPositionsBuilder<'a> for PositionsByRuleIdxVariables<'a, 'a> {
    fn build_positions(
        att_type: AttackingType,
        rule_set: &'a RuleSet,
    ) -> PositionsByRuleIdxVariables<'a, 'a> {
        let att_variables: RuleIdxVariables = rule_set.match_attacking_variables(att_type);
        att_variables
            .into_iter()
            .map(|rule_idx_var| {
                (
                    rule_idx_var,
                    rule_set.attacked_positions_by_rule_idx_var(&rule_idx_var),
                )
            })
            .collect()
    }
}

trait AttackedPositionsBuilderPrivate<'a> {
    fn conclude_attacked_positions(&'a self, cur_att_pos: &Positions<'a>) -> Positions<'a>;
    fn initial_attacked_positions(&'a self, rule_idx_var: &RuleIdxVariable<'a>) -> Positions<'a>;
}

impl<'a> AttackedPositionsBuilderPrivate<'a> for RuleSet {
    fn conclude_attacked_positions(&'a self, cur_att_pos: &Positions<'a>) -> Positions<'a> {
        self.iter()
            .fold(Positions::new(), |new_found_attacked_positions, rule| {
                let new_att_pos_in_rule: Positions = rule.conclude_attacked_positions(cur_att_pos);
                new_found_attacked_positions.insert_all_take_ret(new_att_pos_in_rule)
            })
    }

    // TODO: INSTEAD OF POSITIONS_IN_HEAD CALL, CALL INIT_ATT_POS OF RULE
    fn initial_attacked_positions(
        &'a self,
        (idx, variable): &RuleIdxVariable<'a>,
    ) -> Positions<'a> {
        self.iter()
            .enumerate()
            .filter(|(r_idx, _)| r_idx == idx)
            .fold(Positions::new(), |initial_pos, (_, rule)| {
                let initial_pos_of_rule: Positions = variable.positions_in_head(rule);
                initial_pos.insert_all_take_ret(initial_pos_of_rule)
            })
    }
}

impl<'a> AttackedPositionsBuilderPrivate<'a> for Rule {
    fn conclude_attacked_positions(&self, cur_att_pos: &Positions) -> Positions {
        self.positive_variables()
            .iter()
            .filter(|var| var.is_attacked_by_positions_in_rule(self, cur_att_pos))
            .fold(Positions::new(), |new_att_pos_in_rule, var| {
                let pos_of_var_in_head: Positions = var.positions_in_head(self);
                new_att_pos_in_rule.insert_all_take_ret(pos_of_var_in_head)
            })
    }

    fn initial_attacked_positions(&'a self, _rule_idx_var: &RuleIdxVariable<'a>) -> Positions<'a> {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }
}

trait AttackedPositionsBuilderPrivateExtended<'a> {
    fn attacked_positions_by_rule_idx_var(
        &'a self,
        rule_idx_var: &RuleIdxVariable<'a>,
    ) -> Positions<'a>;
    fn match_attacking_variables(&self, att_type: AttackingType) -> RuleIdxVariables;
}

impl<'a> AttackedPositionsBuilderPrivateExtended<'a> for RuleSet {
    fn attacked_positions_by_rule_idx_var(
        &'a self,
        rule_idx_var: &RuleIdxVariable<'a>,
    ) -> Positions<'a> {
        let mut att_pos: Positions = self.initial_attacked_positions(rule_idx_var);
        let mut new_found_att_pos: Positions = att_pos.clone();
        while !new_found_att_pos.is_empty() {
            let new_con_att_pos: Positions = self.conclude_attacked_positions(&att_pos);
            new_found_att_pos = new_con_att_pos.remove_all_ret(&att_pos);
            att_pos.insert_all(&new_found_att_pos);
        }
        att_pos
    }

    fn match_attacking_variables(&self, att_type: AttackingType) -> RuleIdxVariables {
        match att_type {
            AttackingType::Cycle => {
                let jo_ac_graph = JointAcyclicityGraph::build_graph(&self);
                jo_ac_graph.rule_idx_variables_in_cycles()
            }
            AttackingType::Existential => self.existential_rule_idx_variables(),
        }
    }
}

/// This Trait gives all (common / weakly) marked positions or none if these marked positions does
/// not exist of some RuleSet.
pub trait MarkedPositionsBuilder<'a> {
    /// Builds all (common / weakly) marked positions or returns none if these marked positions does
    /// not exist of some RuleSet.
    fn build_positions(mar_type: MarkingType, rule_set: &'a RuleSet) -> Self;
}

impl<'a> MarkedPositionsBuilder<'a> for Option<Positions<'a>> {
    fn build_positions(mar_type: MarkingType, rule_set: &'a RuleSet) -> Option<Positions<'a>> {
        let mut mar_pos: Positions = rule_set.match_initial_marked_positions(mar_type)?;
        let mut new_found_mar_pos: Positions = mar_pos.clone();
        while !new_found_mar_pos.is_empty() {
            let new_con_mar_pos: Positions =
                rule_set.conclude_marked_positions(&new_found_mar_pos)?;
            new_found_mar_pos = new_con_mar_pos.remove_all_ret(&mar_pos);
            mar_pos.insert_all(&new_found_mar_pos);
        }
        Some(mar_pos)
    }
}

trait MarkedPositionsBuilderPrivate<'a> {
    fn conclude_marked_positions(&'a self, last_it_pos: &Positions<'a>) -> Option<Positions<'a>>;
    fn initial_marked_positions(&'a self) -> Option<Positions<'a>>;
    fn initial_weakly_marked_positions(&'a self, inf_rank_pos: &Positions)
        -> Option<Positions<'a>>;
}

impl<'a> MarkedPositionsBuilderPrivate<'a> for RuleSet {
    fn conclude_marked_positions(&'a self, last_it_pos: &Positions<'a>) -> Option<Positions<'a>> {
        self.iter()
            .try_fold(Positions::new(), |new_con_mar_pos, rule| {
                let new_mar_pos_in_rule: Positions = rule.conclude_marked_positions(last_it_pos)?;
                Some(new_con_mar_pos.insert_all_take_ret(new_mar_pos_in_rule))
            })
    }

    fn initial_marked_positions(&'a self) -> Option<Positions<'a>> {
        self.iter()
            .try_fold(Positions::new(), |init_mar_pos, rule| {
                let init_mar_pos_of_rule: Positions = rule.initial_marked_positions()?;
                Some(init_mar_pos.insert_all_take_ret(init_mar_pos_of_rule))
            })
    }

    fn initial_weakly_marked_positions(
        &'a self,
        _inf_rank_pos: &Positions,
    ) -> Option<Positions<'a>> {
        let we_ac_graph: WeakAcyclicityGraph = WeakAcyclicityGraph::build_graph(&self);
        let inf_rank_pos: Positions = we_ac_graph.infinite_rank_positions();
        self.iter()
            .try_fold(Positions::new(), |init_we_mar_pos, rule| {
                let init_we_mar_pos_of_rule: Positions =
                    rule.initial_weakly_marked_positions(&inf_rank_pos)?;
                Some(init_we_mar_pos.insert_all_take_ret(init_we_mar_pos_of_rule))
            })
    }
}

impl<'a> MarkedPositionsBuilderPrivate<'a> for Rule {
    fn conclude_marked_positions(&'a self, last_it_pos: &Positions<'a>) -> Option<Positions<'a>> {
        self.positive_variables()
            .iter()
            .filter(|var| {
                let positive_body_atoms: Vec<&Atom> = self.body_positive_refs();
                var.appears_at_some_positions_in_atoms(last_it_pos, &positive_body_atoms)
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

    fn initial_marked_positions(&'a self) -> Option<Positions<'a>> {
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
        &'a self,
        infinite_rank_positions: &Positions,
    ) -> Option<Positions<'a>> {
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

trait MarkedPositionsBuilderPrivateExtended<'a> {
    fn match_initial_marked_positions(&'a self, mar_type: MarkingType) -> Option<Positions<'a>>;
}

impl<'a> MarkedPositionsBuilderPrivateExtended<'a> for RuleSet {
    fn match_initial_marked_positions(&'a self, mar_type: MarkingType) -> Option<Positions<'a>> {
        match mar_type {
            MarkingType::Common => self.initial_marked_positions(),
            MarkingType::Weakly => self.initial_weakly_marked_positions(&Positions::default()),
        }
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
                if differenced_indices.is_empty() {
                    self.remove(pred);
                }
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
                if differenced_indices.is_empty() {
                    self.remove(pred);
                }
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
