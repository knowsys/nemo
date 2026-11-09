//! The compiled positive body of a rule: its atoms, filters, bindings and computed columns.

use nemo_physical::{function::StackProgram, management::TableId};

use crate::{engine::compile::compiled::VariableId, model::Span};

/// One positive body atom, which binds a variable to each column of its table.
#[derive(Debug)]
pub(crate) struct BodyAtom {
    /// The predicate's table.
    pub(crate) table: TableId,
    /// The variable at each column, distinct within the atom.
    pub(crate) variables: Vec<VariableId>,
}

/// One filter: a condition, compiled to a program, that a row must satisfy.
#[derive(Debug)]
pub(crate) struct RuleFilter {
    /// The columns that feed the program, in input order.
    pub(crate) inputs: Vec<VariableId>,
    /// The compiled condition.
    pub(crate) program: StackProgram,
    /// The part of the rule's text that the filter comes from.
    pub(crate) origin: FilterOrigin,
}

/// The part of a rule's text that a filter comes from,
/// so that a message about the filter can point at it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FilterOrigin {
    /// A condition written in the body.
    Condition(Option<Span>),
    /// A structured pattern in a body atom,
    /// for the test of its shape or for the equality of one of its positions with a variable.
    Pattern(Option<Span>),
    /// A constant or a parameter as the argument of an atom.
    Constant(Option<Span>),
    /// An expression as the argument of an atom.
    Expression(Option<Span>),
    /// A variable written again in the same atom.
    Repeated(Option<Span>),
}

impl FilterOrigin {
    /// Returns the position of the part of the text.
    pub(crate) fn span(self) -> Option<Span> {
        match self {
            Self::Condition(span)
            | Self::Pattern(span)
            | Self::Constant(span)
            | Self::Expression(span)
            | Self::Repeated(span) => span,
        }
    }
}

/// A column of a body atom whose argument is an expression over variables
/// that the body atoms bind.
///
/// The column has a fresh variable, whose value the program computes from the expression's
/// inputs, so that the join can look the value up instead of enumerating the column.
#[derive(Debug)]
pub(crate) struct RuleBinding {
    /// The fresh variable of the column.
    pub(crate) target: VariableId,
    /// The rule columns that feed the program, in input order.
    pub(crate) inputs: Vec<VariableId>,
    /// The program that computes the column's value.
    pub(crate) program: StackProgram,
    /// The position of the expression.
    pub(crate) span: Option<Span>,
}

/// A computed column: a value that a program computes from other columns.
///
/// The compilation creates one for an assignment, an expression in the head or in an
/// aggregate, a constant in the head, and a head variable written again in the same atom.
/// A row for which the program fails is dropped.
#[derive(Debug)]
pub(crate) struct ComputedColumn {
    /// The columns that feed the program, in input order.
    pub(crate) inputs: Vec<VariableId>,
    /// The program that computes the column.
    pub(crate) program: StackProgram,
}
