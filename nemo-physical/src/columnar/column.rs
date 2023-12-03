//! This module defines the [Column] trait and its implementations,
//! as well as [ColumnEnum],
//! which collects all implementations into a single object.

pub mod interval;
pub mod rle;
pub mod vector;

use std::{fmt::Debug, mem::size_of};

use bytesize::ByteSize;

use crate::{
    datatypes::{ColumnDataType, RunLengthEncodable},
    generate_forwarder,
    management::ByteSized,
};

use self::{rle::ColumnRle, vector::ColumnVector};

use super::columnscan::{ColumnScan, ColumnScanEnum};

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