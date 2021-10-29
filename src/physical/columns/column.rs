use std::{fmt::Debug, ops::Index};

/// Column of ordered values.
pub trait Column<T>: Debug {
    /// Returns the number of entries in the column.
    fn len(&self) -> usize;

    /// Returns true iff the column is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the value at the given index.
    ///
    /// # Panics
    /// Panics if `index` is out of bounds.
    fn get(&self, index: usize) -> &T;
}

impl<T> Index<usize> for dyn Column<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        self.get(index)
    }
}
