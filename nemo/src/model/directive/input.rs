//! Input declarations, which declare predicates whose rows come from outside the file.

use std::fmt::{self, Display, Formatter};

use crate::model::{ColumnDeclaration, Identifier, Span, fmt_list, span_ignoring_eq};

/// An input declaration, such as `@input edge(source: any, target: any) .`.
///
/// It declares a predicate whose rows come from the program that `@use`s the file,
/// or through the API, such as [import_rows](crate::engine::Engine::import_rows).
/// A `@use` of the file binds the predicate to a predicate of the using program.
/// The columns, if given, declare the predicate as a `@declare` does,
/// with its column types, its arity, and the column names that atoms may use.
/// A bare `@input edge .` leaves all of that open.
#[derive(Debug, Clone)]
pub struct InputDirective {
    /// The input predicate.
    pub predicate: Identifier,
    /// The declared columns, which fix the arity,
    /// or `None` if the arity follows from how the predicate is used.
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
