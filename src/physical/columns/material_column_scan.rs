use super::ColumnScan;


/// Iterator for a column of ordered values that is available in materialised
/// form.
pub trait MaterialColumnScan<T>: ColumnScan<T> {
    /// Return the current position of this iterator, or None if the iterator is
    /// before the first or after the last element.
    fn pos(&mut self) -> Option<usize>;
}