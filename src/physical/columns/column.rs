use super::{
    GenericColumnScanEnum, RangedColumnScan, RangedColumnScanEnum, RleColumn, VectorColumn,
};
use crate::physical::datatypes::{DataValueT, Double, Field, Float, FloorToUsize};
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

/// Enum for column implementations
#[derive(Debug)]
pub enum ColumnT {
    /// Case ColumnEnum<u64>
    ColumnU64(ColumnEnum<u64>),
    /// Case ColumnEnum<Float>
    ColumnFloat(ColumnEnum<Float>),
    /// Case ColumnEnum<Double>
    ColumnDouble(ColumnEnum<Double>),
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
