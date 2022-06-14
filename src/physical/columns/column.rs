use super::{
    GenericColumnScanEnum, RangedColumnScan, RangedColumnScanEnum, RleColumn, VectorColumn,
};
use crate::{
    generate_datatype_forwarder, generate_forwarder,
    physical::datatypes::{DataValueT, Double, Field, Float, FloorToUsize},
};
use std::fmt::Debug;

/// Column of ordered values.
pub trait Column<'a, T>: Debug + Clone {
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
#[derive(Debug, Clone)]
pub enum ColumnEnum<T> {
    /// Case VectorColumn
    VectorColumn(VectorColumn<T>),
    /// Case RleColumn
    RleColumn(RleColumn<T>),
}

generate_forwarder!(forward_to_column;
                    VectorColumn,
                    RleColumn);

impl<'a, T> Column<'a, T> for ColumnEnum<T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    type ColScan = RangedColumnScanEnum<'a, T>;

    fn len(&self) -> usize {
        forward_to_column!(self, len)
    }

    fn is_empty(&self) -> bool {
        forward_to_column!(self, is_empty)
    }

    fn get(&self, index: usize) -> T {
        forward_to_column!(self, get(index))
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
#[derive(Debug, Clone)]
pub enum ColumnT {
    /// Case ColumnEnum<u64>
    U64(ColumnEnum<u64>),
    /// Case ColumnEnum<Float>
    Float(ColumnEnum<Float>),
    /// Case ColumnEnum<Double>
    Double(ColumnEnum<Double>),
}

generate_datatype_forwarder!(forward_to_column_enum);

impl ColumnT {
    /// Returns the number of entries in the [`Column`]
    pub fn len(&self) -> usize {
        forward_to_column_enum!(self, len)
    }

    /// Returns the value, wrapped in a [`DataValueT`][crate::physical::datatypes::DataValueT], at a given index.
    ///
    /// # Panics
    /// Panics if `index` is out of bounds.
    pub fn get(&self, index: usize) -> DataValueT {
        forward_to_column_enum!(self, get(index).as_variant_of(DataValueT))
    }

    /// Returns [`true`] iff the column is empty
    pub fn is_empty(&self) -> bool {
        forward_to_column_enum!(self, is_empty)
    }
}
