//! Facts: asserted ground atoms.

use std::fmt::{self, Display, Formatter};

use crate::model::{Identifier, Span, Term, span_ignoring_eq, term::fmt_labeled};

/// A fact: a predicate asserted over ground terms.
///
/// The type does not enforce groundness.
/// The engine checks it when it loads the program.
#[derive(Debug, Clone)]
pub struct Fact {
    /// The predicate.
    pub predicate: Identifier,
    /// The argument terms.
    pub terms: Vec<Term>,
    /// The written column names, aligned with the terms, `Some` for a fact naming its columns.
    ///
    /// Each name must be a column name declared for the predicate, by `@declare` or `@input`.
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
