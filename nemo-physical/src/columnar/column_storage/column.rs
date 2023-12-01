use bytesize::ByteSize;

use crate::{
    datatypes::{ColumnDataType, Double, Float, RunLengthEncodable, StorageValueT},
    generate_datatype_forwarder, generate_forwarder,
    management::ByteSized,
};
use std::{fmt::Debug, mem::size_of};

use super::{
    column_rle::ColumnRle,
    column_vector::ColumnVector,
    columnscan::{ColumnScan, ColumnScanEnum},
};

/// A trait representing a column of data, where each entry is of type `T`.
pub trait Column<'a, T>: Debug + Clone + ByteSized {
    /// ColumnScan associated with the Column
    type Scan: 'a + ColumnScan<Item = T>;

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
    fn iter(&'a self) -> Self::Scan;
}

/// Enum for column implementations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnEnum<T: RunLengthEncodable> {
    /// Case ColumnVector
    ColumnVector(ColumnVector<T>),
    /// Case ColumnRle
    ColumnRle(ColumnRle<T>),
}

generate_forwarder!(forward_to_column;
                    ColumnVector,
                    ColumnRle);

impl<'a, T> Column<'a, T> for ColumnEnum<T>
where
    T: 'a + ColumnDataType,
{
    type Scan = ColumnScanEnum<'a, T>;

    fn len(&self) -> usize {
        forward_to_column!(self, len)
    }

    fn is_empty(&self) -> bool {
        forward_to_column!(self, is_empty)
    }

    fn get(&self, index: usize) -> T {
        forward_to_column!(self, get(index))
    }

    fn iter(&'a self) -> Self::Scan {
        match self {
            Self::ColumnVector(col) => ColumnScanEnum::ColumnScanVector(col.iter()),
            Self::ColumnRle(col) => ColumnScanEnum::ColumnScanRle(col.iter()),
        }
    }
}

impl<T: RunLengthEncodable> ByteSized for ColumnEnum<T> {
    fn size_bytes(&self) -> ByteSize {
        let size_column = forward_to_column!(self, size_bytes);
        ByteSize::b(size_of::<Self>() as u64) + size_column
    }
}

/// Enum for column implementations
#[derive(Debug, Clone)]
pub enum ColumnT {
    /// Case `ColumnEnum<u64>`
    Id32(ColumnEnum<u32>),
    /// Case `ColumnEnum<u64>`
    Id64(ColumnEnum<u64>),
    /// Case `ColumnEnum<i64>`
    Int64(ColumnEnum<i64>),
    /// Case `ColumnEnum<Float>`
    Float(ColumnEnum<Float>),
    /// Case `ColumnEnum<Double>`
    Double(ColumnEnum<Double>),
}

generate_datatype_forwarder!(forward_to_column_enum);

impl ColumnT {
    /// Returns the number of entries in the [`Column`]
    pub fn len(&self) -> usize {
        forward_to_column_enum!(self, len)
    }

    /// Returns the value, wrapped in a [`StorageValueT`], at a given index.
    ///
    /// # Panics
    /// Panics if `index` is out of bounds.
    pub fn get(&self, index: usize) -> StorageValueT {
        forward_to_column_enum!(self, get(index).as_variant_of(StorageValueT))
    }

    /// Returns [`true`] iff the column is empty
    pub fn is_empty(&self) -> bool {
        forward_to_column_enum!(self, is_empty)
    }
}
