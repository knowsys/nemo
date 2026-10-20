//! The shape of tuples.

use std::cmp::Ordering;

use crate::value::{
    ValueDomain,
    typing::{LabelBound, LabelTypes, Shape, Type, shape::StructuralShape},
};

/// The shape of tuples: label and element types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleShape {
    /// What is known of the label.
    pub label: LabelBound,
    /// The element types, fixing the arity.
    pub elements: Vec<Type>,
}

impl StructuralShape for TupleShape {
    const DOMAIN: ValueDomain = ValueDomain::Tuple;

    fn label(&self) -> &LabelBound {
        &self.label
    }

    fn of_type(datatype: &Type) -> Option<&[Shape<Self>]> {
        datatype.tuple_shapes()
    }

    fn cmp_outline(&self, other: &Self) -> Ordering {
        self.elements.len().cmp(&other.elements.len())
    }

    fn merge(&self, other: &Self) -> Self {
        Self {
            label: self.label.clone(),
            elements: self
                .elements
                .iter()
                .zip(&other.elements)
                .map(|(left, right)| left.union(right))
                .collect(),
        }
    }

    fn intersect(&self, other: &Self) -> Option<Self> {
        if self.elements.len() != other.elements.len() {
            return None;
        }

        let label = self.label.intersect(&other.label)?;
        let mut elements = Vec::with_capacity(self.elements.len());
        for (left, right) in self.elements.iter().zip(&other.elements) {
            let element = left.intersect(right);
            if element.is_empty() {
                return None;
            }
            elements.push(element);
        }

        Some(Self { label, elements })
    }

    fn components_within(&self, other: &Self, labels: &LabelTypes) -> bool {
        self.elements
            .iter()
            .zip(&other.elements)
            .all(|(inner, outer)| inner.is_subset(outer, labels))
    }

    fn unfolded(&self, labels: &LabelTypes, depth: usize) -> Self {
        Self {
            label: self.label.clone(),
            elements: self
                .elements
                .iter()
                .map(|element| element.unfolded(labels, depth))
                .collect(),
        }
    }

    fn capped(&self, depth: usize) -> Self {
        Self {
            label: self.label.clone(),
            elements: self
                .elements
                .iter()
                .map(|element| element.capped(depth - 1))
                .collect(),
        }
    }
}
