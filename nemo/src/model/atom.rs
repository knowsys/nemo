//! Atoms: predicates applied to terms.

use std::fmt::{self, Display, Formatter};

use crate::model::{Identifier, Span, Term, span_ignoring_eq, term::fmt_labeled};

/// An atom: a predicate applied to argument terms.
#[derive(Debug, Clone)]
pub struct Atom {
    /// The predicate.
    pub predicate: Identifier,
    /// The argument terms.
    pub terms: Vec<Term>,
    /// The written column names, aligned with the terms,
    /// `Some` for an atom naming its columns, as in `person(name: ?n)`.
    ///
    /// Each name must be a column name declared for the predicate, by `@declare` or `@input`.
    pub labels: Option<Vec<String>>,
    /// The source position, or `None` for a component built programmatically.
    pub span: Option<Span>,
}

span_ignoring_eq!(Atom: predicate, terms, labels);

impl Atom {
    /// Creates an atom without a source position.
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

impl Display for Atom {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.predicate)?;
        fmt_labeled(f, &self.terms, self.labels.as_deref())?;
        f.write_str(")")
    }
}
