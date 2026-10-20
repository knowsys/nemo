//! Module instantiations.

use std::fmt::{self, Display, Formatter};

use crate::model::{Identifier, ModuleName, Span, Term, span_ignoring_eq};

/// A module instantiation:
/// `@use lib::tc { edge = link, $file = "e.csv" } as t .`.
///
/// It does three things:
/// - It binds the file's `@input` predicates to actual predicates.
/// - It binds the file's `@parameter`s to values.
/// - It makes the file's `@output` predicates available as `alias::name`.
///
/// Bindings are by name, and a leading `$` marks a parameter.
#[derive(Debug, Clone)]
pub struct UseDirective {
    /// The module's name.
    pub module: ModuleName,
    /// The input bindings, input to actual predicate.
    pub bindings: Vec<(Identifier, Identifier)>,
    /// The value bindings, parameter name (without the `$`) to value.
    pub values: Vec<(String, Term)>,
    /// The alias output names are qualified with.
    pub alias: String,
    /// The source position, or `None` for a component built programmatically.
    pub span: Option<Span>,
}

span_ignoring_eq!(UseDirective: module, bindings, values, alias);

impl Display for UseDirective {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "@use {}", self.module)?;
        if !self.bindings.is_empty() || !self.values.is_empty() {
            f.write_str(" {")?;
            let mut first = true;
            for (input, actual) in &self.bindings {
                if !first {
                    f.write_str(",")?;
                }
                first = false;
                write!(f, " {input} = {actual}")?;
            }
            for (name, value) in &self.values {
                if !first {
                    f.write_str(",")?;
                }
                first = false;
                write!(f, " ${name} = {value}")?;
            }
            f.write_str(" }")?;
        }
        write!(f, " as {} .", self.alias)
    }
}
