//! Facts, which state that a predicate holds for ground terms.

use std::fmt::{self, Display, Formatter};

use crate::model::{Identifier, Span, Term, span_ignoring_eq, term::fmt_labeled};

/// A fact, which states that a predicate holds for ground terms, such as `parent(alice, bob) .`.
///
/// The type does not ensure that the terms are ground.
/// An [Engine](crate::engine::Engine) checks that when it loads the program.
#[derive(Debug, Clone)]
pub struct Fact {
    /// The predicate.
    pub predicate: Identifier,
    /// The argument terms.
    pub terms: Vec<Term>,
    /// The names of the columns that the terms fill, one per term,
    /// if the fact is written with named arguments, as in `person(name: "Ann") .`.
    ///
    /// Each name must be a column name that `@declare` or `@input` declares for the predicate.
    pub labels: Option<Vec<String>>,
    /// The source position, or `None` for a component built programmatically.
    pub span: Option<Span>,
}

span_ignoring_eq!(Fact: predicate, terms, labels);

impl Fact {
    /// Creates a fact without a source position.
    ///
    /// A string predicate becomes a bare name.
    pub fn new(
        predicate: impl Into<Identifier>,
        terms: impl IntoIterator<Item = impl Into<Term>>,
    ) -> Self {
        Self {
            predicate: predicate.into(),
            terms: terms.into_iter().map(Into::into).collect(),
            labels: None,
            span: None,
        }
    }
}

impl Display for Fact {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.predicate)?;
        fmt_labeled(f, &self.terms, self.labels.as_deref())?;
        f.write_str(") .")
    }
}
