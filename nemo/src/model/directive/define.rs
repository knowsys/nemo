//! Derived builtin definitions.

use std::fmt::{self, Display, Formatter};

use crate::model::{Identifier, Span, Term, Variable, fmt_list, span_ignoring_eq};

/// A derived builtin: `@define hypot(?x, ?y) = SQRT(…) .`.
///
/// The body is a single expression over the parameters and existing builtins.
/// The engine ignores the directive when it loads the program, and warns about it (N0250).
#[derive(Debug, Clone)]
pub struct DefineDirective {
    /// The defined name.
    pub name: Identifier,
    /// The parameter variables.
    pub parameters: Vec<Variable>,
    /// The defining expression.
    pub body: Term,
    /// The source position, or `None` for a component built programmatically.
    pub span: Option<Span>,
}

span_ignoring_eq!(DefineDirective: name, parameters, body);

impl Display for DefineDirective {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "@define {}(", self.name)?;
        fmt_list(f, &self.parameters)?;
        write!(f, ") = {} .", self.body)
    }
}
