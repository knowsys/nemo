use super::ColumnScan;
use crate::physical::datatypes::{DataValueT, Double, Float};
use std::fmt::Debug;

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
    fn get(&self, index: usize) -> T;

    /// Returns an iterator for this column.
    fn iter<'a>(&'a self) -> Box<dyn ColumnScan<Item = T> + 'a>;
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

impl ColumnT {
    /// Returns the number of entries in the [`Column`]
    pub fn len(&self) -> usize {
        match self {
            ColumnT::ColumnU64(col) => col.len(),
            ColumnT::ColumnFloat(col) => col.len(),
            ColumnT::ColumnDouble(col) => col.len(),
        }
    }

    /// Returns the value, wrapped in a [`DataValueT`][crate::physical::datatypes::DataValueT], at a given index.
    ///
    /// # Panics
    /// Panics if `index` is out of bounds.
    pub fn get(&self, index: usize) -> DataValueT {
        match self {
            ColumnT::ColumnU64(col) => DataValueT::U64(col.get(index)),
            ColumnT::ColumnFloat(col) => DataValueT::Float(col.get(index)),
            ColumnT::ColumnDouble(col) => DataValueT::Double(col.get(index)),
        }
    }

    /// Returns [`true`] iff the column is empty
    pub fn is_empty(&self) -> bool {
        match self {
            ColumnT::ColumnU64(col) => col.is_empty(),
            ColumnT::ColumnFloat(col) => col.is_empty(),
            ColumnT::ColumnDouble(col) => col.is_empty(),
        }
    }
}
