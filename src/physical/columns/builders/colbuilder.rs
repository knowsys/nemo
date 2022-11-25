use std::fmt::Debug;

use crate::physical::columns::columns::Column;

/// Type for constructing columns.
pub trait ColumnBuilder<'a, T>: Debug {
    /// Column Type that is produced by builder
    type Col: Column<'a, T>;

    /// Adds a new value to the constructed vector.
    fn add(&mut self, value: T);

    /// Returns column that was built.
    fn finalize(self) -> Self::Col;

    /// Return the number of elements that were already added.
    fn count(&self) -> usize;
}
