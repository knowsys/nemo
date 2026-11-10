//! Applications of builtin operators.

mod operator;

pub use self::operator::Operator;

use std::fmt::{self, Display, Formatter};

use crate::model::{Span, Term, TermKind, fmt_list, span_ignoring_eq};

/// An application of an [Operator] to argument terms.
#[derive(Debug, Clone)]
pub struct Operation {
    /// The applied operator.
    pub operator: Operator,
    /// The argument terms.
    pub arguments: Vec<Term>,
    /// The source position, or `None` for a component built programmatically.
    pub span: Option<Span>,
}

span_ignoring_eq!(Operation: operator, arguments);

impl Operation {
    /// Creates an operation without a source position.
    pub fn new(operator: Operator, arguments: Vec<Term>) -> Self {
        Self {
            operator,
            arguments,
            span: None,
        }
    }

    /// Adds the conjuncts of the operation to `conjuncts`.
    ///
    /// The conjuncts of an [And](Operator::And) whose operands are all operations
    /// are the conjuncts of its operands.
    /// Any other operation is a single conjunct.
    pub(crate) fn conjuncts<'a>(&'a self, conjuncts: &mut Vec<&'a Operation>) {
        let operands: Option<Vec<&Operation>> = (self.operator == Operator::And)
            .then(|| {
                self.arguments
                    .iter()
                    .map(|argument| match &argument.kind {
                        TermKind::Operation(operation) => Some(operation),
                        _ => None,
                    })
                    .collect()
            })
            .flatten();
        match operands {
            Some(operands) => {
                for operand in operands {
                    operand.conjuncts(conjuncts);
                }
            }
            None => conjuncts.push(self),
        }
    }

    /// Returns whether the operation is displayed with its operator between the arguments,
    /// which is the case for two arguments and an operator with an infix symbol.
    fn is_infix(&self) -> bool {
        self.operator.infix().is_some() && self.arguments.len() == 2
    }

    /// Writes an operand of an infix operation whose precedence is `parent`.
    ///
    /// A nested infix operation is put in parentheses only if precedence requires it.
    /// This is the case if it binds less tightly than the parent,
    /// or equally tightly and it is the `right` operand, since infix operators associate to
    /// the left.
    fn fmt_operand(f: &mut Formatter<'_>, term: &Term, parent: u8, right: bool) -> fmt::Result {
        let needs_parentheses = match &term.kind {
            TermKind::Operation(operation) if operation.is_infix() => {
                let child = operation
                    .operator
                    .precedence()
                    .expect("infix operators have a precedence");
                child < parent || (child == parent && right)
            }
            _ => false,
        };
        if needs_parentheses {
            write!(f, "({term})")
        } else {
            write!(f, "{term}")
        }
    }
}

impl Display for Operation {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // `GET(?t, 0)` and `?t[0]` are the same operation, which is displayed in the short form.
        // Component access binds more tightly than every infix operator,
        // so an infix operation before the brackets is put in parentheses.
        if self.operator == Operator::Get && self.arguments.len() == 2 {
            let base = &self.arguments[0];
            if matches!(&base.kind, TermKind::Operation(operation) if operation.is_infix()) {
                write!(f, "({base})")?;
            } else {
                write!(f, "{base}")?;
            }
            return write!(f, "[{}]", self.arguments[1]);
        }
        if self.is_infix() {
            let symbol = self.operator.infix().expect("checked in is_infix");
            let precedence = self
                .operator
                .precedence()
                .expect("infix operators have a precedence");
            Self::fmt_operand(f, &self.arguments[0], precedence, false)?;
            write!(f, " {symbol} ")?;
            Self::fmt_operand(f, &self.arguments[1], precedence, true)
        } else {
            write!(f, "{}(", self.operator.name())?;
            fmt_list(f, &self.arguments)?;
            f.write_str(")")
        }
    }
}
