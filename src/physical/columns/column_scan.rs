use std::fmt::Debug;

/// Iterator for a column of ordered values.
pub trait ColumnScan<T>: Debug + Iterator<Item = T> {
    /// Find the next value that is at least as large as the given value,
    /// advance the iterator to this position, and return the value.
    fn seek(&mut self, value: T) -> Option<T>;

    /// Return the value at the current position, if any.
    fn current(&mut self) -> Option<T>;

}
