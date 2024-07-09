//! This module defines an [Atom].

use std::{fmt::Display, hash::Hash};

use crate::rule_model::{error::ProgramConstructionError, origin::Origin};

use super::{
    term::{primitive::variable::Variable, Term},
    IteratableVariables, ProgramComponent, Tag,
};

/// Atom
///
/// Tagged list of [Term]s.
/// It forms the core component of rules,
/// representing a logical proposition that can be true or false.
#[derive(Debug, Clone, Eq)]
pub struct Atom {
    /// Origin of this component.
    origin: Origin,

    /// Predicate name associated with this atom
    predicate: Tag,
    /// Subterms of the function
    terms: Vec<Term>,
}

/// Construct an [Atom].
#[macro_export]
macro_rules! atom {
    // Base case: no elements
    ($name:tt) => {
        crate::rule_model::component::atom::Atom::new($name, Vec::new())
    };
    // Recursive case: handle each term, separated by commas
    ($name:tt; $($tt:tt)*) => {{
        let mut terms = Vec::new();
        term_list!(terms; $($tt)*);
        crate::rule_model::component::atom::Atom::new($name, terms)
    }};
}

impl Atom {
    /// Create a new [Atom].
    pub fn new<Terms: IntoIterator<Item = Term>>(predicate: &str, subterms: Terms) -> Self {
        Self {
            origin: Origin::Created,
            predicate: Tag::new(predicate.to_string()),
            terms: subterms.into_iter().collect(),
        }
    }

    /// Return an iterator over the subterms of this atom.
    pub fn subterms(&self) -> impl Iterator<Item = &Term> {
        self.terms.iter()
    }

    /// Return an mutable iterator over the subterms of this atom.
    pub fn subterms_mut(&mut self) -> impl Iterator<Item = &mut Term> {
        self.terms.iter_mut()
    }
}

impl Display for Atom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}(", self.predicate))?;

        for (term_index, term) in self.terms.iter().enumerate() {
            term.fmt(f)?;

            if term_index < self.terms.len() - 1 {
                f.write_str(", ")?;
            }
        }

        f.write_str(")")
    }
}

impl PartialEq for Atom {
    fn eq(&self, other: &Self) -> bool {
        self.origin == other.origin
            && self.predicate == other.predicate
            && self.terms == other.terms
    }
}

impl Hash for Atom {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.predicate.hash(state);
        self.terms.hash(state);
    }
}

impl ProgramComponent for Atom {
    fn parse(_string: &str) -> Result<Self, ProgramConstructionError>
    where
        Self: Sized,
    {
        todo!()
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    fn set_origin(mut self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        self.origin = origin;
        self
    }

    fn validate(&self) -> Result<(), ProgramConstructionError>
    where
        Self: Sized,
    {
        if !self.predicate.is_valid() {
            todo!()
        }

        for term in self.subterms() {
            term.validate()?;
        }

        Ok(())
    }
}

impl IteratableVariables for Atom {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        Box::new(self.terms.iter().flat_map(|term| term.variables()))
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        Box::new(self.terms.iter_mut().flat_map(|term| term.variables_mut()))
    }
}

#[cfg(test)]
mod test {
    use crate::rule_model::component::{term::primitive::variable::Variable, IteratableVariables};

    #[test]
    fn atom_basic() {
        let variable = Variable::universal("u");
        let function = atom!("p"; 12, variable, !e, "abc", ?v);

        let variables = function.variables().cloned().collect::<Vec<_>>();
        assert_eq!(
            variables,
            vec![
                Variable::universal("u"),
                Variable::existential("e"),
                Variable::universal("v")
            ]
        );
    }
}
