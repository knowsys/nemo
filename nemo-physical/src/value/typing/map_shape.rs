//! The shape of maps.

use std::cmp::Ordering;

use crate::value::{
    DataValue, ValueDomain, ValueRef,
    typing::{LabelBound, LabelTypes, Shape, Type, shape::StructuralShape},
};

/// The shape of maps: label, and value types under an exact key set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapShape {
    /// What is known of the label.
    pub label: LabelBound,
    /// The value types by key, sorted by key in term order,
    /// the order map pairs are stored in.
    pairs: Vec<(DataValue, Type)>,
}

impl MapShape {
    /// Creates the shape of maps with the given label bound and value types by key,
    /// sorting the pairs by key,
    /// or `None` if two pairs share a key.
    pub fn new(label: LabelBound, pairs: Vec<(DataValue, Type)>) -> Option<Self> {
        let mut pairs = pairs;
        pairs.sort_by(|(left, _), (right, _)| left.cmp(right));
        if pairs.windows(2).any(|window| window[0].0 == window[1].0) {
            return None;
        }
        Some(Self { label, pairs })
    }

    /// Returns the value types by key, sorted by key in term order.
    pub fn pairs(&self) -> &[(DataValue, Type)] {
        &self.pairs
    }

    /// Returns the value type at a key, if the shape holds it.
    pub fn value_at(&self, key: ValueRef<'_>) -> Option<&Type> {
        self.pairs
            .binary_search_by(|(stored, _)| stored.as_ref().cmp(&key))
            .ok()
            .map(|index| &self.pairs[index].1)
    }

    /// Returns whether both shapes hold exactly the same keys.
    ///
    /// Pairs are sorted by key, so this is a positional comparison.
    fn same_keys(&self, other: &Self) -> bool {
        self.cmp_outline(other).is_eq()
    }
}

impl StructuralShape for MapShape {
    const DOMAIN: ValueDomain = ValueDomain::Map;

    fn label(&self) -> &LabelBound {
        &self.label
    }

    fn of_type(datatype: &Type) -> Option<&[Shape<Self>]> {
        datatype.map_shapes()
    }

    fn cmp_outline(&self, other: &Self) -> Ordering {
        self.pairs
            .iter()
            .map(|(key, _)| key)
            .cmp(other.pairs.iter().map(|(key, _)| key))
    }

    fn merge(&self, other: &Self) -> Self {
        Self {
            label: self.label.clone(),
            pairs: self
                .pairs
                .iter()
                .zip(&other.pairs)
                .map(|((key, left), (_, right))| (key.clone(), left.union(right)))
                .collect(),
        }
    }

    fn intersect(&self, other: &Self) -> Option<Self> {
        if !self.same_keys(other) {
            return None;
        }

        let label = self.label.intersect(&other.label)?;
        let mut pairs = Vec::with_capacity(self.pairs.len());
        for ((key, left), (_, right)) in self.pairs.iter().zip(&other.pairs) {
            let value = left.intersect(right);
            if value.is_empty() {
                return None;
            }
            pairs.push((key.clone(), value));
        }

        Some(Self { label, pairs })
    }

    fn components_within(&self, other: &Self, labels: &LabelTypes) -> bool {
        self.pairs
            .iter()
            .zip(&other.pairs)
            .all(|((_, inner), (_, outer))| inner.is_subset(outer, labels))
    }

    fn unfolded(&self, labels: &LabelTypes, depth: usize) -> Self {
        Self {
            label: self.label.clone(),
            pairs: self
                .pairs
                .iter()
                .map(|(key, value)| (key.clone(), value.unfolded(labels, depth)))
                .collect(),
        }
    }

    fn capped(&self, depth: usize) -> Self {
        Self {
            label: self.label.clone(),
            pairs: self
                .pairs
                .iter()
                .map(|(key, value)| (key.clone(), value.capped(depth - 1)))
                .collect(),
        }
    }
}
