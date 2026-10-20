//! The types declared for labels.

use std::collections::BTreeMap;

use crate::value::typing::{MapShape, Shape, TupleShape, Type, shape::StructuralShape};

/// The types declared for labels, which [declared shapes](Shape::Declared) stand for.
///
/// A label's declared type has one written-out shape, carrying the label,
/// whose elements may refer to declared labels in turn.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LabelTypes(BTreeMap<Box<str>, Type>);

impl LabelTypes {
    /// No label has a declared type.
    pub const NONE: Self = Self(BTreeMap::new());

    /// Declares the type of a label's values.
    pub fn declare(&mut self, label: &str, declared: Type) {
        self.0.insert(label.into(), declared);
    }

    /// Returns the type declared for a label, if it has one.
    pub fn get(&self, label: &str) -> Option<&Type> {
        self.0.get(label)
    }

    /// Returns the tuple shape a label's declared type gives its tuples,
    /// or `None` if the label is not declared as a tuple.
    pub fn tuple_shape(&self, label: &str) -> Option<&TupleShape> {
        self.structural(label)
    }

    /// Returns the map shape a label's declared type gives its maps,
    /// or `None` if the label is not declared as a map.
    pub fn map_shape(&self, label: &str) -> Option<&MapShape> {
        self.structural(label)
    }

    /// Returns the written-out shape a label's declared type gives its structures of one kind,
    /// or `None` if the label has no such declared type.
    pub(super) fn structural<S: StructuralShape>(&self, label: &str) -> Option<&S> {
        S::of_type(self.get(label)?)?
            .iter()
            .find_map(|shape| match shape {
                Shape::Structural(structural) => Some(structural),
                Shape::Declared(_) => None,
            })
    }
}
