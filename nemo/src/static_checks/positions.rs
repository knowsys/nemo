use crate::rule_model::components::{tag::Tag, term::primitive::variable::Variable};
use crate::static_checks::collection_traits::{Disjoint, InsertAll, RemoveAll, Superset};
use crate::static_checks::positions::positions_internal::{
    AttackedPositionsBuilderInternal, SpecialPositionsBuilderInternal,
};
use crate::static_checks::rule_set::RuleSet;

use std::collections::{HashMap, HashSet};

mod positions_internal;

pub type Index = usize;

pub type Indices = HashSet<Index>;

pub type Positions<'a> = HashMap<&'a Tag, Indices>;

pub type PositionsByVariables<'a, 'b> = HashMap<&'a Variable, Positions<'b>>;

pub type Position<'a> = (&'a Tag, Index);

pub type ExtendedPositions<'a> = HashSet<Position<'a>>;

pub enum AttackingVariables {
    Cycle,
    Existential,
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

pub trait SpecialPositionsBuilder<'a>: SpecialPositionsBuilderInternal<'a> {
    fn build_positions(rule_set: &'a RuleSet) -> Self;
}

impl<'a> SpecialPositionsBuilder<'a> for Positions<'a> {
    fn build_positions(rule_set: &'a RuleSet) -> Positions<'a> {
        Positions::build_positions_internal(rule_set)
    }
}

impl<'a> SpecialPositionsBuilder<'a> for Option<Positions<'a>> {
    fn build_positions(rule_set: &'a RuleSet) -> Option<Positions<'a>> {
        Option::<Positions>::build_positions_internal(rule_set)
    }
}

pub trait AttackedPositionsBuilder<'a>: AttackedPositionsBuilderInternal<'a> {
    fn build_positions(att_vars: AttackingVariables, rule_set: &'a RuleSet) -> Self;
}

impl<'a> AttackedPositionsBuilder<'a> for PositionsByVariables<'a, 'a> {
    fn build_positions(
        att_vars: AttackingVariables,
        rule_set: &'a RuleSet,
    ) -> PositionsByVariables<'a, 'a> {
        PositionsByVariables::build_positions_internal(att_vars, rule_set)
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
