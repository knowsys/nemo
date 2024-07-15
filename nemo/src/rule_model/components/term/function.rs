//! This module defines [FunctionTerm]

use std::{fmt::Display, hash::Hash};

use crate::rule_model::{
    components::{IterableVariables, ProgramComponent, Tag},
    error::ProgramValidationError,
    origin::Origin,
};

use super::{primitive::variable::Variable, Term};

/// Function term
///
/// List of [Term]s with a [Tag].
#[derive(Debug, Clone, Eq)]
pub struct FunctionTerm {
    /// Origin of this component
    origin: Origin,

    /// Name of the function
    tag: Tag,
    /// Subterms of the function
    terms: Vec<Term>,
}

/// Construct a [FunctionTerm].
#[macro_export]
macro_rules! function {
    // Base case: no elements
    ($name:tt) => {
        crate::rule_model::component::term::function::FunctionTerm::new($name, Vec::new())
    };
    // Recursive case: handle each term, separated by commas
    ($name:tt; $($tt:tt)*) => {{
        let mut terms = Vec::new();
        term_list!(terms; $($tt)*);
        crate::rule_model::components::term::function::FunctionTerm::new($name,terms)
    }};
}

impl FunctionTerm {
    /// Create a new [FunctionTerm].
    pub fn new<Terms: IntoIterator<Item = Term>>(name: &str, subterms: Terms) -> Self {
        Self {
            origin: Origin::Created,
            tag: Tag::new(name.to_string()),
            terms: subterms.into_iter().collect(),
        }
    }

    /// Return an iterator over the subterms of this function term.
    pub fn subterms(&self) -> impl Iterator<Item = &Term> {
        self.terms.iter()
    }

    /// Return an mutable iterator over the subterms of this function terms.
    pub fn subterms_mut(&mut self) -> impl Iterator<Item = &mut Term> {
        self.terms.iter_mut()
    }
}

impl Display for FunctionTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}(", self.tag))?;

        for (term_index, term) in self.terms.iter().enumerate() {
            term.fmt(f)?;

            if term_index < self.terms.len() - 1 {
                f.write_str(", ")?;
            }
        }

        f.write_str(")")
    }
}

impl PartialEq for FunctionTerm {
    fn eq(&self, other: &Self) -> bool {
        self.tag == other.tag && self.terms == other.terms
    }
}

impl PartialOrd for FunctionTerm {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.tag.partial_cmp(&other.tag) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.terms.partial_cmp(&other.terms)
    }
}

impl Hash for FunctionTerm {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.tag.hash(state);
        self.terms.hash(state);
    }
}

impl ProgramComponent for FunctionTerm {
    fn parse(_string: &str) -> Result<Self, ProgramValidationError>
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

    fn validate(&self) -> Result<(), ProgramValidationError>
    where
        Self: Sized,
    {
        if !self.tag.is_valid() {
            todo!()
        }

        for term in self.subterms() {
            term.validate()?
        }

        Ok(())
    }
}

impl IterableVariables for FunctionTerm {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        Box::new(self.terms.iter().flat_map(|term| term.variables()))
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        Box::new(self.terms.iter_mut().flat_map(|term| term.variables_mut()))
    }
}

#[cfg(test)]
mod test {
    use crate::rule_model::components::{term::primitive::variable::Variable, IterableVariables};

    #[test]
    fn function_basic() {
        let variable = Variable::universal("u");
        let function = function!("f"; 12, variable, !e, "abc", ?v);

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
