//! The positive body of a compiled rule: its atoms, filters, bindings, and computed columns.

use nemo_physical::{function::StackProgram, management::TableId};

use crate::{engine::compile::compiled::VariableId, model::Span};

/// One positive body atom: a table whose columns bind variables.
#[derive(Debug)]
pub(crate) struct BodyAtom {
    /// The predicate's table.
    pub(crate) table: TableId,
    /// The variable at each column, distinct within the atom.
    pub(crate) variables: Vec<VariableId>,
}

/// One filter condition: a program over bound variables.
#[derive(Debug)]
pub(crate) struct RuleFilter {
    /// The variables feeding the program, in input order.
    pub(crate) inputs: Vec<VariableId>,
    /// The compiled condition.
    pub(crate) program: StackProgram,
    /// The part of the rule's text the filter stands for.
    pub(crate) origin: FilterOrigin,
}

/// The part of a rule's text one of its filters stands for,
/// so that a message about the filter can point at it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FilterOrigin {
    /// A condition written in the body.
    Condition(Option<Span>),
    /// A structured pattern in a body atom: the test of its shape,
    /// or an equality of one of its positions with a variable bound elsewhere.
    Pattern(Option<Span>),
    /// A constant or a parameter as the argument of an atom.
    Constant(Option<Span>),
    /// An expression as the argument of an atom.
    Expression(Option<Span>),
    /// A variable written again in the same atom.
    Repeated(Option<Span>),
}

impl FilterOrigin {
    /// Returns the position of that part of the text.
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

/// One bound body column: an expression argument of a body atom,
/// normalized to a fresh column variable whose value the program
/// determines from other columns.
#[derive(Debug)]
pub(crate) struct RuleBinding {
    /// The bound column variable.
    pub(crate) target: VariableId,
    /// The variables feeding the program, in input order.
    pub(crate) inputs: Vec<VariableId>,
    /// The program computing the column value.
    pub(crate) program: StackProgram,
    /// The position of the expression.
    pub(crate) span: Option<Span>,
}

/// One computed column of the rule's join: a program over bound variables
/// (an assignment, an expression in the head or in an aggregate, a constant,
/// or a copy of a duplicated head variable).
///
/// A row whose computed column errors is dropped.
#[derive(Debug)]
pub(crate) struct ComputedColumn {
    /// The variables feeding the program, in input order.
    pub(crate) inputs: Vec<VariableId>,
    /// The program computing the column.
    pub(crate) program: StackProgram,
}
