//! This module defines [Substitution].

use std::collections::{
    hash_map::{IntoIter, Iter, IterMut},
    HashMap,
};

use super::components::{
    term::primitive::{variable::Variable, Primitive},
    IterablePrimitives,
};

/// Map from [Primitive] terms to each other
/// that can be used to uniformly replace terms
#[derive(Debug, Default, Clone)]
pub struct Substitution {
    map: HashMap<Primitive, Primitive>,
}

impl Substitution {
    /// Create a new [Substitution].
    pub fn new<From, To, Iterator>(iter: Iterator) -> Self
    where
        From: Into<Primitive>,
        To: Into<Primitive>,
        Iterator: IntoIterator<Item = (From, To)>,
    {
        Self {
            map: iter
                .into_iter()
                .map(|(from, to)| (from.into(), to.into()))
                .collect(),
        }
    }

    /// Add a new mapping.
    pub fn insert<From, To>(&mut self, from: From, to: To)
    where
        From: Into<Primitive>,
        To: Into<Primitive>,
    {
        self.map.insert(from.into(), to.into());
    }

    /// Apply mapping to a program component.
    pub fn apply<Component: IterablePrimitives>(&self, component: &mut Component) {
        for primitive in component.primitive_terms_mut() {
            if let Some(term) = self.map.get(primitive) {
                *primitive = term.clone();
            }
        }
    }

    /// Return an iterator over all mapped variables in this substition.
    pub fn variables(&self) -> impl Iterator<Item = &Variable> {
        self.map.keys().filter_map(|term| {
            if let Primitive::Variable(variable) = term {
                Some(variable)
            } else {
                None
            }
        })
    }
}

impl<TypeFrom, TypeTo> From<HashMap<TypeFrom, TypeTo>> for Substitution
where
    TypeFrom: Into<Primitive>,
    TypeTo: Into<Primitive>,
{
    fn from(value: HashMap<TypeFrom, TypeTo>) -> Self {
        Self {
            map: value
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        }
    }
}

impl IntoIterator for Substitution {
    type Item = (Primitive, Primitive);
    type IntoIter = IntoIter<Primitive, Primitive>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}

impl<'a> IntoIterator for &'a Substitution {
    type Item = (&'a Primitive, &'a Primitive);
    type IntoIter = Iter<'a, Primitive, Primitive>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.iter()
    }
}

impl<'a> IntoIterator for &'a mut Substitution {
    type Item = (&'a Primitive, &'a mut Primitive);
    type IntoIter = IterMut<'a, Primitive, Primitive>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.iter_mut()
    }
}
