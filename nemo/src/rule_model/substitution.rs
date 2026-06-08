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

    /// Add a new mapping from `old_y` to `new_y` and adjust existing mappings onto `old_y`.
    pub fn remap(&mut self, old_y: Primitive, new_y: Primitive) {
        debug_assert!(old_y != new_y, "trying to add identity transmutation");
        debug_assert!(
            !self.map.contains_key(&old_y),
            "domain and range of unifier are not disjoint"
        );
        // modify all entries pointing to old_y to now point to new_y instead
        let term_old_y = Term::Primitive(old_y.clone());
        self.map
            .iter_mut()
            .filter(|(_k, v)| **v == term_old_y)
            .for_each(|(_k, v)| {
                *v = Term::Primitive(new_y.clone());
            });
        self.map.insert(old_y, Term::Primitive(new_y));
    }

    /// Resolve a primitive w.r.t. the substitution.
    pub fn get(&self, term: &Primitive) -> Option<Primitive> {
        match term {
            Primitive::Variable(_) => match self.map.get(term) {
                Some(Term::Primitive(prim)) => Some(prim.clone()),
                _ => None,
            },
            Primitive::Ground(_) => Some(term.clone()),
        }
    }

    /// Check if the gien variable is mapped by this substitution.
    pub fn contains_var(&self, k: &Variable) -> bool {
        self.map.contains_key(&Primitive::Variable(k.clone()))
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

pub trait Substitute {
    fn substitute(&mut self, eta: &Substitution) -> &mut Self;
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
