//! This module defines [Tuple].

use std::{fmt::Display, hash::Hash};

use crate::rule_model::{
    components::{IterablePrimitives, IterableVariables, ProgramComponent, ProgramComponentKind},
    error::ValidationErrorBuilder,
    origin::Origin,
};

use super::{
    primitive::{variable::Variable, Primitive},
    value_type::ValueType,
    Term,
};

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
        $crate::rule_model::components::term::tuple::Tuple::new(Vec::new())
    };
    // Recursive case: handle each term, separated by commas
    ($($tt:tt)*) => {{
        #[allow(clippy::vec_init_then_push)] {
            let mut terms = Vec::new();
            term_list!(terms; $($tt)*);
            $crate::rule_model::components::term::tuple::Tuple::new(terms)
        }
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

    /// Return the value type of this term.
    pub fn value_type(&self) -> ValueType {
        ValueType::Tuple
    }

    /// Return an iterator over the arguments of this tuple.
    pub fn arguments(&self) -> impl Iterator<Item = &Term> {
        self.terms.iter()
    }

    /// Return whether this term is ground,
    /// i.e. if it does not contain any variables.
    pub fn is_ground(&self) -> bool {
        self.terms.iter().all(Term::is_ground)
    }

    /// Reduce each sub [Term] in the tuple returning a copy.
    pub fn reduce(&self) -> Self {
        Self {
            origin: self.origin,
            terms: self.terms.iter().map(Term::reduce).collect(),
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

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Option<()>
    where
        Self: Sized,
    {
        for term in self.arguments() {
            term.validate(builder)?;
        }

        Some(())
    }

    fn kind(&self) -> ProgramComponentKind {
        ProgramComponentKind::Tuple
    }
}

impl IterableVariables for Tuple {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        Box::new(self.terms.iter().flat_map(|term| term.variables()))
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        Box::new(self.terms.iter_mut().flat_map(|term| term.variables_mut()))
    }
}

impl IterablePrimitives for Tuple {
    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a> {
        Box::new(self.terms.iter().flat_map(|term| term.primitive_terms()))
    }

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Primitive> + 'a> {
        Box::new(
            self.terms
                .iter_mut()
                .flat_map(|term| term.primitive_terms_mut()),
        )
    }
}

#[cfg(test)]
mod test {
    use crate::rule_model::{
        components::{
            term::{primitive::variable::Variable, tuple::Tuple, Term},
            IterableVariables,
        },
        translation::TranslationComponent,
    };

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

    #[test]
    fn parse_tuple() {
        let tuple = Tuple::parse("(?x, 2)").unwrap();

        assert_eq!(
            Tuple::new(vec![Term::from(Variable::universal("x")), Term::from(2)]),
            tuple
        );
    }
}
