use super::Column;
use std::fmt::Debug;

/// Type for constructing columns.
pub trait ColumnBuilder<'a, T: 'a>: Debug {
    /// Adds a new value to the constructed vector.
    fn add(&mut self, value: T);

    /// Returns column that was built.
    fn finalize(self) -> Box<dyn Column<T> + 'a>;
}
