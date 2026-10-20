//! Attributes: generic statement metadata.

use std::fmt::{self, Display, Formatter};

use crate::model::{Identifier, Span, Term, fmt_list, span_ignoring_eq};

/// An attribute attached to a statement, like `#[name("reach")]`.
#[derive(Debug, Clone)]
pub struct Attribute {
    /// The attribute name.
    pub name: Identifier,
    /// The argument terms.
    pub arguments: Vec<Term>,
    /// The source position, or `None` for a component built programmatically.
    pub span: Option<Span>,
}

span_ignoring_eq!(Attribute: name, arguments);

impl Display for Attribute {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "#[{}", self.name)?;
        if !self.arguments.is_empty() {
            f.write_str("(")?;
            fmt_list(f, &self.arguments)?;
            f.write_str(")")?;
        }
        f.write_str("]")
    }
}
