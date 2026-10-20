//! External builtin declarations.

use std::fmt::{self, Display, Formatter};

use crate::model::{Identifier, Span, TypeExpression, fmt_list, span_ignoring_eq};

/// An external builtin declaration: `@extern geodistance(any, any) -> double .`.
///
/// The engine ignores the directive when it loads the program, and warns about it (N0250).
#[derive(Debug, Clone)]
pub struct ExternDirective {
    /// The declared name.
    pub name: Identifier,
    /// The parameter types.
    pub parameters: Vec<TypeExpression>,
    /// The result type.
    pub result: TypeExpression,
    /// The source position, or `None` for a component built programmatically.
    pub span: Option<Span>,
}

span_ignoring_eq!(ExternDirective: name, parameters, result);

impl Display for ExternDirective {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "@extern {}(", self.name)?;
        fmt_list(f, &self.parameters)?;
        write!(f, ") -> {} .", self.result)
    }
}
