//! Module instantiations.

use std::fmt::{self, Display, Formatter};

use crate::model::{Identifier, ModuleName, Span, Term, span_ignoring_eq};

/// A module instantiation, such as `@use lib::tc { edge = link, $file = "e.csv" } as t .`.
///
/// It does three things:
/// - It binds the module's `@input` predicates to predicates of the using program.
/// - It binds the module's `@parameter`s to values.
/// - It makes the module's `@output` predicates available as `alias::name`.
///
/// Bindings are by name, and a leading `$` marks a parameter.
#[derive(Debug, Clone)]
pub struct UseDirective {
    /// The module's name.
    pub module: ModuleName,
    /// The input bindings, each from an input predicate to a predicate of the using program.
    pub bindings: Vec<(Identifier, Identifier)>,
    /// The parameter bindings, each from a parameter's name, without the `$`, to a value.
    pub values: Vec<(String, Term)>,
    /// The alias that qualifies the names of the module's outputs.
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
