use crate::rule_model::components::{tag::Tag, term::primitive::variable::Variable};
use std::collections::{HashMap, HashSet};

use super::collection_traits::InsertAll;

pub type Index = usize;

pub type Indices = HashSet<Index>;

pub type Positions<'a> = HashMap<&'a Tag, Indices>;

pub type PositionsByVariables<'a, 'b> = HashMap<&'a Variable, Positions<'b>>;

pub type Position<'a> = (&'a Tag, Index);

pub type ExtendedPositions<'a> = HashSet<Position<'a>>;

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

pub trait Disjoint {
    fn is_disjoint(&self, other: &Self) -> bool;
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

pub trait Superset {
    fn is_superset(&self, other: &Self) -> bool;
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
