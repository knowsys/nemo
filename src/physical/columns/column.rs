use super::{
    GenericColumnScanEnum, RangedColumnScan, RangedColumnScanEnum, RleColumn, VectorColumn,
};
use crate::physical::datatypes::{Field, FloorToUsize};
use std::fmt::Debug;

/// Column of ordered values.
pub trait Column<'a, T>: Debug {
    /// ColumnScan associated with the Column
    type ColScan: 'a + RangedColumnScan<Item = T>;

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
    fn iter(&'a self) -> Self::ColScan;
}

/// Enum for column implementations
#[derive(Debug)]
pub enum ColumnEnum<T> {
    /// Case VectorColumn
    VectorColumn(VectorColumn<T>),
    /// Case RleColumn
    RleColumn(RleColumn<T>),
}

impl<'a, T> Column<'a, T> for ColumnEnum<T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    type ColScan = RangedColumnScanEnum<'a, T>;

    fn len(&self) -> usize {
        match self {
            Self::VectorColumn(col) => col.len(),
            Self::RleColumn(col) => col.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::VectorColumn(col) => col.is_empty(),
            Self::RleColumn(col) => col.is_empty(),
        }
    }

    fn get(&self, index: usize) -> T {
        match self {
            Self::VectorColumn(col) => col.get(index),
            Self::RleColumn(col) => col.get(index),
        }
    }

    fn iter(&'a self) -> Self::ColScan {
        match self {
            Self::VectorColumn(col) => RangedColumnScanEnum::GenericColumnScan(
                GenericColumnScanEnum::VectorColumn(col.iter()),
            ),
            Self::RleColumn(col) => RangedColumnScanEnum::RleColumnScan(col.iter()),
        }
    }
}
