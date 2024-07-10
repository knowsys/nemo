//! This module defines [Tuple].

use std::{fmt::Display, hash::Hash};

use crate::rule_model::{
    component::{IteratableVariables, ProgramComponent},
    origin::Origin,
};

use super::{primitive::variable::Variable, Term};

/// Tuple
///
/// An ordered list of [Term]s.
#[derive(Debug, Clone, Eq)]
pub struct Tuple {
    /// Origin of this component
    origin: Origin,

    /// Ordered list of terms contained in this tuple
    terms: Vec<Term>,
}

/// Construct a [Tuple].
#[macro_export]
macro_rules! tuple {
    // Base case: no elements
    () => {
        crate::rule_model::component::term::tuple::Tuple::new(Vec::new())
    };
    // Recursive case: handle each term, separated by commas
    ($($tt:tt)*) => {{
        let mut terms = Vec::new();
        term_list!(terms; $($tt)*);
        crate::rule_model::component::term::tuple::Tuple::new(terms)
    }};
}

impl Tuple {
    /// Create a new [Tuple].
    pub fn new<Terms: IntoIterator<Item = Term>>(terms: Terms) -> Self {
        Self {
            origin: Origin::default(),
            terms: terms.into_iter().collect(),
        }
    }
}

impl Display for Tuple {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("(")?;

        for (term_index, term) in self.terms.iter().enumerate() {
            term.fmt(f)?;

            if term_index < self.terms.len() - 1 {
                f.write_str(", ")?;
            }
        }

        f.write_str(")")
    }
}

impl PartialEq for Tuple {
    fn eq(&self, other: &Self) -> bool {
        self.terms == other.terms
    }
}

impl Hash for Tuple {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.terms.hash(state);
    }
}

impl PartialOrd for Tuple {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.terms.partial_cmp(&other.terms)
    }
}

impl ProgramComponent for Tuple {
    fn parse(_string: &str) -> Result<Self, crate::rule_model::error::ProgramConstructionError>
    where
        Self: Sized,
    {
        todo!()
    }

    fn origin(&self) -> &Origin {
        todo!()
    }

    fn set_origin(mut self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        self.origin = origin;
        self
    }

    fn validate(&self) -> Result<(), crate::rule_model::error::ProgramConstructionError>
    where
        Self: Sized,
    {
        todo!()
    }
}

impl IteratableVariables for Tuple {
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
    fn tuple_basic() {
        let variable = Variable::universal("u");
        let tuple = tuple!(12, variable, !e, "abc", ?v);

        let variables = tuple.variables().cloned().collect::<Vec<_>>();
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
