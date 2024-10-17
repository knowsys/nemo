//! This module defines [PrimitiveTermMap].

use std::collections::{
    hash_map::{IntoIter, Iter, IterMut},
    HashMap,
};

use super::components::{term::primitive::Primitive, IterablePrimitives};

/// Map from [Primitive] terms to each other
/// that can be used to uniformly replace terms
#[derive(Debug, Default, Clone)]
pub struct PrimitiveTermMap {
    map: HashMap<Primitive, Primitive>,
}

impl PrimitiveTermMap {
    /// Create a new [PrimitiveTermMap].
    pub fn new<Iterator: IntoIterator<Item = (Primitive, Primitive)>>(iter: Iterator) -> Self {
        Self {
            map: iter.into_iter().collect(),
        }
    }

    /// Add a new mapping.
    pub fn insert(&mut self, from: Primitive, to: Primitive) {
        self.map.insert(from, to);
    }

    /// Apply mapping to a program component.
    pub fn apply<Component: IterablePrimitives>(&self, component: &mut Component) {
        for primitive in component.primitive_terms_mut() {
            if let Some(term) = self.map.get(primitive) {
                *primitive = term.clone();
            }
        }
    }
}

impl From<HashMap<Primitive, Primitive>> for PrimitiveTermMap {
    fn from(value: HashMap<Primitive, Primitive>) -> Self {
        Self { map: value }
    }
}

impl IntoIterator for PrimitiveTermMap {
    type Item = (Primitive, Primitive);
    type IntoIter = IntoIter<Primitive, Primitive>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}

impl<'a> IntoIterator for &'a PrimitiveTermMap {
    type Item = (&'a Primitive, &'a Primitive);
    type IntoIter = Iter<'a, Primitive, Primitive>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.iter()
    }
}

impl<'a> IntoIterator for &'a mut PrimitiveTermMap {
    type Item = (&'a Primitive, &'a mut Primitive);
    type IntoIter = IterMut<'a, Primitive, Primitive>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.iter_mut()
    }
}
