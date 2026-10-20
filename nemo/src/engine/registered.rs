//! A predicate as the engine registered it.

use nemo_physical::management::TableId;

/// A registered predicate: the table storing it, and the number of columns the table holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Registered {
    /// The table storing the predicate's rows.
    pub(crate) table: TableId,
    /// The number of columns the table holds, the columns its loader delivers.
    pub(crate) arity: usize,
}
