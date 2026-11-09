//! The types declared for labels.

use std::collections::BTreeMap;

use crate::value::typing::{MapShape, Shape, TupleShape, Type, shape::StructuralShape};

/// The types declared for labels, which [declared shapes](Shape::Declared) refer to.
///
/// The declared type of a label must have one written-out shape with that label.
/// The components of that shape may refer to declared labels in turn.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LabelTypes(BTreeMap<Box<str>, Type>);

impl LabelTypes {
    /// The declarations in which no label has a type.
    pub const NONE: Self = Self(BTreeMap::new());

    /// Declares the type of the tuples or the maps with the given label.
    pub fn declare(&mut self, label: &str, declared: Type) {
        self.0.insert(label.into(), declared);
    }

    /// Returns the type declared for a label, or `None` if it has none.
    pub fn get(&self, label: &str) -> Option<&Type> {
        self.0.get(label)
    }

    /// Returns the written-out shape of the tuples with the given label,
    /// or `None` if the label has no declared type for tuples.
    pub fn tuple_shape(&self, label: &str) -> Option<&TupleShape> {
        self.structural(label)
    }

    /// Returns the written-out shape of the maps with the given label,
    /// or `None` if the label has no declared type for maps.
    pub fn map_shape(&self, label: &str) -> Option<&MapShape> {
        self.structural(label)
    }

    /// Returns the written-out shape of the label's declared type for structures of kind `S`,
    /// or `None` if there is none.
    pub(super) fn structural<S: StructuralShape>(&self, label: &str) -> Option<&S> {
        S::of_type(self.get(label)?)?
            .iter()
            .find_map(|shape| match shape {
                Shape::Structural(structural) => Some(structural),
                Shape::Declared(_) => None,
            })
    }
}
