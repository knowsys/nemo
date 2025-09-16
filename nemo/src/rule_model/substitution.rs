//! This module defines [Substitution].

use std::collections::{
    HashMap,
    hash_map::{IntoIter, Iter, IterMut},
};

use crate::rule_model::origin::Origin;

use super::components::{
    IterablePrimitives,
    term::{
        Term,
        primitive::{Primitive, variable::Variable},
    },
};

/// Map from [Primitive] terms to each other
/// that can be used to uniformly replace terms
#[derive(Debug, Default, Clone)]
pub struct Substitution {
    map: HashMap<Primitive, Term>,
}

impl Substitution {
    /// Create a new [Substitution].
    pub fn new<From, To, Iterator>(iter: Iterator) -> Self
    where
        From: Into<Primitive>,
        To: Into<Term>,
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
        To: Into<Term>,
    {
        self.map.insert(from.into(), to.into());
    }

    /// Apply mapping to a program component.
    pub fn apply<Component: IterablePrimitives<TermType = Term>>(&self, component: &mut Component) {
        for term in component.primitive_terms_mut() {
            let Term::Primitive(primitive) = term else {
                unreachable!()
            };

            if let Some(replacement) = self.map.get(primitive) {
                *term = Origin::substitution(primitive, replacement);
            }
        }
    }

    /// Return an iterator over all mapped variables in this substitution.
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
    TypeTo: Into<Term>,
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
    type Item = (Primitive, Term);
    type IntoIter = IntoIter<Primitive, Term>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}

impl<'a> IntoIterator for &'a Substitution {
    type Item = (&'a Primitive, &'a Term);
    type IntoIter = Iter<'a, Primitive, Term>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.iter()
    }
}

impl<'a> IntoIterator for &'a mut Substitution {
    type Item = (&'a Primitive, &'a mut Term);
    type IntoIter = IterMut<'a, Primitive, Term>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.iter_mut()
    }
}
