use std::{fmt::Debug, ops::Index};
use crate::physical::datatypes::{Float,Double};

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

/// Enum for columns of all supported basic types.
#[derive(Debug)]
pub enum ColumnT {
    /// Case Column<u64>
    ColumnU64(Box<dyn Column<u64>>),
    /// Case Column<Float>
    ColumnFloat(Box<dyn Column<Float>>),
    /// Case Column<Double>
    ColumnDouble(Box<dyn Column<Double>>),
}
