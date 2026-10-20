//! Negated body atoms, prepared for subtraction.

use nemo_physical::management::TableId;

use crate::engine::compile::compiled::RuleFilter;

/// One negated body atom.
///
/// A body match is dropped if the table holds a row agreeing on the shared columns and passing
/// the filters, with the private columns existentially quantified inside the negation.
#[derive(Debug)]
pub(crate) struct NegatedAtom {
    /// The predicate's table.
    pub(crate) table: TableId,
    /// The binding of each column.
    pub(crate) columns: Vec<NegatedColumn>,
    /// The filters over this atom's own columns (constants, repetitions).
    ///
    /// Program inputs index into `columns`.
    pub(crate) filters: Vec<RuleFilter>,
}

/// One column of a negated atom.
#[derive(Debug, Clone, Copy)]
pub(crate) enum NegatedColumn {
    /// Bound to one of the rule's columns.
    Shared(usize),
    /// Private to the atom, existential inside the negation.
    Private,
}

impl NegatedAtom {
    /// Returns whether no column is shared.
    ///
    /// Such an atom is an emptiness guard, checked once per stratum and never subtracted.
    pub(crate) fn is_guard(&self) -> bool {
        !self
            .columns
            .iter()
            .any(|column| matches!(column, NegatedColumn::Shared(_)))
    }
}
