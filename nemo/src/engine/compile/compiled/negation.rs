//! Compiled negated body atoms.

use nemo_physical::management::TableId;

use crate::engine::compile::compiled::RuleFilter;

/// One negated body atom.
///
/// A body match is dropped if the table has a row that agrees with it on the shared columns
/// and satisfies the filters.
/// The private columns of that row may have any value.
#[derive(Debug)]
pub(crate) struct NegatedAtom {
    /// The predicate's table.
    pub(crate) table: TableId,
    /// Whether each stored column is shared or private.
    pub(crate) columns: Vec<NegatedColumn>,
    /// The filters over the atom's own columns, for constants and repeated variables.
    ///
    /// Their inputs are positions in [columns](Self::columns).
    pub(crate) filters: Vec<RuleFilter>,
}

/// One column of a negated atom.
#[derive(Debug, Clone, Copy)]
pub(crate) enum NegatedColumn {
    /// The column must equal this rule column.
    Shared(usize),
    /// The column is private to the atom, so any value matches it.
    Private,
}

impl NegatedAtom {
    /// Returns whether a column is shared.
    ///
    /// An atom without a shared column drops every body match if its table has a row
    /// that satisfies the filters, and none otherwise.
    pub(crate) fn shares_columns(&self) -> bool {
        self.columns
            .iter()
            .any(|column| matches!(column, NegatedColumn::Shared(_)))
    }
}
