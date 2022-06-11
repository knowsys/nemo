use std::fmt::Debug;

/// Iterator for a column of ordered values.
/// The iterator's associated type Item is used to define the type of the column.
pub trait ColumnScan: Debug + Iterator {
    /// Find the next value that is at least as large as the given value,
    /// advance the iterator to this position, and return the value.
    fn seek(&mut self, value: Self::Item) -> Option<Self::Item>;

    /// Return the value at the current position, if any.
    fn current(&mut self) -> Option<Self::Item>;

    /// Return to the initial state
    fn reset(&mut self);
}

/// Convert this Scan into a Cell type that offers interior mutability
pub trait ScanToCell {
    /// The Cell type wrapping this Scan.
    type Cell: Debug;

    /// Convert `self` into a Cell containing it.
    fn into_cell(self) -> Self::Cell;
}
