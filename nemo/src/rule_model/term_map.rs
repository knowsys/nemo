//! This module defines [PrimitiveTermMap].

use std::collections::HashMap;

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
