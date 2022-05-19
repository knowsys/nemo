use super::Column;
use std::fmt::Debug;

/// Type for constructing columns.
pub trait ColumnBuilder<'a, T>: Debug {
    /// Column Type that is produced by builder
    type Col: Column<'a, T>;

    /// Adds a new value to the constructed vector.
    fn add(&mut self, value: T);

    /// Returns column that was built.
    fn finalize(self) -> Self::Col;
}
