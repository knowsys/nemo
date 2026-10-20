//! Input declarations: predicates whose rows come from the program that uses the file.

use std::fmt::{self, Display, Formatter};

use crate::model::{ColumnDeclaration, Identifier, Span, fmt_list, span_ignoring_eq};

/// An input declaration: `@input edge(source: any, target: any) .`.
///
/// It declares a predicate whose rows come from the program that `@use`s the file,
/// or from the API.
/// A `@use` of the file binds it to one of the using program's predicates.
/// The columns, if given, declare the predicate like a `@declare` does:
/// its column types, its arity, and the column names atoms may use.
/// A bare `@input edge .` leaves all of that open.
#[derive(Debug, Clone)]
pub struct InputDirective {
    /// The input predicate.
    pub predicate: Identifier,
    /// The declared columns, fixing the arity,
    /// or `None` to take the arity from how the predicate is used.
    pub columns: Option<Vec<ColumnDeclaration>>,
    /// The source position, or `None` for a component built programmatically.
    pub span: Option<Span>,
}

span_ignoring_eq!(InputDirective: predicate, columns);

impl Display for InputDirective {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "@input {}", self.predicate)?;
        if let Some(columns) = &self.columns {
            f.write_str("(")?;
            fmt_list(f, columns)?;
            f.write_str(")")?;
        }
        f.write_str(" .")
    }
}
