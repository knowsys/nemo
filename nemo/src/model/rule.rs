//! Rules: the derivation statements.

use std::fmt::{self, Display, Formatter};

use crate::model::{Atom, Attribute, Literal, Span, fmt_list, span_ignoring_eq};

/// A rule: head atoms derived whenever all body literals are satisfied.
#[derive(Debug, Clone)]
pub struct Rule {
    /// The attributes attached to the rule, like `#[name("reach")]`.
    pub attributes: Vec<Attribute>,
    /// The head: atoms derived by the rule.
    pub head: Vec<Atom>,
    /// The body: literals that must be satisfied.
    pub body: Vec<Literal>,
    /// The source position, or `None` for a component built programmatically.
    pub span: Option<Span>,
}

span_ignoring_eq!(Rule: attributes, head, body);

impl Rule {
    /// Creates a rule without attributes or a source position.
    pub fn new(head: Vec<Atom>, body: Vec<Literal>) -> Self {
        Self {
            attributes: Vec::new(),
            head,
            body,
            span: None,
        }
    }
}

impl Display for Rule {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for attribute in &self.attributes {
            writeln!(f, "{attribute}")?;
        }
        fmt_list(f, &self.head)?;
        if !self.body.is_empty() {
            f.write_str(" :- ")?;
            fmt_list(f, &self.body)?;
        }
        f.write_str(" .")
    }
}
