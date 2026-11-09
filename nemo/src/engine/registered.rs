//! A predicate as the engine registered it.

use nemo_physical::management::TableId;

/// A registered predicate: the table that stores it, and the table's number of columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Registered {
    /// The table that stores the predicate's rows.
    pub(crate) table: TableId,
    /// The table's number of columns,
    /// which are the columns that the predicate's loader delivers.
    pub(crate) arity: usize,
}
