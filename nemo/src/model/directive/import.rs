//! Import and export directives.

use std::fmt::{self, Display, Formatter};

use crate::model::{Atom, Identifier, Operation, Span, Term, TermKind, fmt_list, span_ignoring_eq};

/// Writes a format specification: `format { key = value, … }`,
/// followed by the guard's variable bindings `, ?x = value`.
fn fmt_specification(
    f: &mut Formatter<'_>,
    format: &Identifier,
    options: &[(Term, Term)],
    guards: &[(String, Term)],
) -> fmt::Result {
    write!(f, "{format} {{")?;
    for (index, (key, value)) in options.iter().enumerate() {
        if index > 0 {
            f.write_str(",")?;
        }
        write!(f, " {key} = {value}")?;
    }
    f.write_str(" }")?;
    for (variable, value) in guards {
        write!(f, ", ?{variable} = {value}")?;
    }
    Ok(())
}

/// An import directive: `@import p from csv { resource = "p.csv" } .`.
///
/// An import may name its columns after the predicate and keep only the rows for which the
/// conditions of a `where` clause hold, in any format:
/// `@import age(?who, ?years) from csv { … } where ISINTEGER(?years) .`
/// A SPARQL import may also restrict its rows by atoms over other predicates:
/// `@import q(?x, ?y) from sparql { … } where b(?x), c(?y) .`
/// imports the query's solutions for which every atom of the `where` clause holds.
/// The engine fetches these solutions as the predicates of the `where` clause grow.
#[derive(Debug, Clone)]
pub struct ImportDirective {
    /// The predicate receiving the rows.
    pub predicate: Identifier,
    /// The names of the predicate's columns, positionally, if the directive gives them
    /// (`q(?x, ?y)`), or empty for the plain form.
    pub variables: Vec<String>,
    /// The format name, like `csv`.
    pub format: Identifier,
    /// The format options, as key–value pairs.
    pub options: Vec<(Term, Term)>,
    /// The guard bindings: variables the option values may use, each
    /// bound to one ground value.
    pub guards: Vec<(String, Term)>,
    /// The atoms of the `where` clause.
    ///
    /// A row is imported only if every atom holds,
    /// with each variable denoting the column of that name.
    pub bindings: Vec<Atom>,
    /// The conditions of the `where` clause.
    ///
    /// A row is imported only if every condition holds,
    /// with each variable denoting the column of that name.
    pub conditions: Vec<Operation>,
    /// The source position, or `None` for a component built programmatically.
    pub span: Option<Span>,
}

span_ignoring_eq!(
    ImportDirective: predicate,
    variables,
    format,
    options,
    guards,
    bindings,
    conditions
);

impl ImportDirective {
    /// Creates a plain import of a predicate from a format with options,
    /// without a source position.
    pub fn new(
        predicate: impl Into<Identifier>,
        format: impl Into<Identifier>,
        options: Vec<(Term, Term)>,
    ) -> Self {
        Self {
            predicate: predicate.into(),
            variables: Vec::new(),
            format: format.into(),
            options,
            guards: Vec::new(),
            bindings: Vec::new(),
            conditions: Vec::new(),
            span: None,
        }
    }

    /// Returns whether the directive restricts its rows by binding atoms.
    pub fn is_bound(&self) -> bool {
        !self.bindings.is_empty()
    }
}

/// What a failed fetch of a remote import does, as its `on_error` option says.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum OnError {
    /// The run fails.
    #[default]
    Fail,
    /// The run keeps what arrived, and warns.
    Warn,
}

impl OnError {
    /// The keywords the option takes.
    pub(crate) const NAMES: &'static str = "fail, warn";

    /// Returns the behavior an option value names,
    /// or `None` if the value is not one of the bare names `fail` and `warn`.
    pub(crate) fn from_term(term: &Term) -> Option<Self> {
        match &term.kind {
            TermKind::Constant(Identifier::Bare(name)) => match name.as_str() {
                "fail" => Some(Self::Fail),
                "warn" => Some(Self::Warn),
                _ => None,
            },
            _ => None,
        }
    }
}

impl Display for ImportDirective {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "@import {}", self.predicate)?;
        if !self.variables.is_empty() {
            f.write_str("(")?;
            let variables: Vec<String> = self
                .variables
                .iter()
                .map(|variable| format!("?{variable}"))
                .collect();
            fmt_list(f, &variables)?;
            f.write_str(")")?;
        }
        f.write_str(" from ")?;
        fmt_specification(f, &self.format, &self.options, &self.guards)?;
        let clause: Vec<String> = self
            .bindings
            .iter()
            .map(ToString::to_string)
            .chain(self.conditions.iter().map(ToString::to_string))
            .collect();
        if !clause.is_empty() {
            f.write_str(" where ")?;
            fmt_list(f, &clause)?;
        }
        f.write_str(" .")
    }
}

/// An export directive:
/// `@export p to csv { resource = "p.csv" } .`.
#[derive(Debug, Clone)]
pub struct ExportDirective {
    /// The predicate whose rows are written.
    pub predicate: Identifier,
    /// The format name, like `csv`.
    pub format: Identifier,
    /// The format options, as key–value pairs.
    pub options: Vec<(Term, Term)>,
    /// The guard bindings: variables the option values may use, each
    /// bound to one ground value.
    pub guards: Vec<(String, Term)>,
    /// The source position, or `None` for a component built programmatically.
    pub span: Option<Span>,
}

span_ignoring_eq!(ExportDirective: predicate, format, options, guards);

impl Display for ExportDirective {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "@export {} to ", self.predicate)?;
        fmt_specification(f, &self.format, &self.options, &self.guards)?;
        f.write_str(" .")
    }
}
