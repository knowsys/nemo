//! This module defines the trait [Column] and its implementations,
//! as well as [ColumnEnum],
//! which collects all implementations of [Column] into a single object.

pub(crate) mod rle;
pub(crate) mod vector;

use std::{fmt::Debug, mem::size_of};

use vector::ColumnScanVectorMut;

use crate::{
    datatypes::{ColumnDataType, RunLengthEncodable},
    generate_forwarder,
    management::bytesized::ByteSized,
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
pub(crate) enum ColumnEnum<T: RunLengthEncodable> {
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
            Self::ColumnVector(col) => ColumnScanEnum::Vector(col.iter()),
            Self::ColumnRle(col) => ColumnScanEnum::Rle(col.iter()),
        }
    }
}

impl<T: RunLengthEncodable> ByteSized for ColumnEnum<T> {
    fn size_bytes(&self) -> u64 {
        let size_column = forward_to_column!(self, size_bytes);
        size_of::<Self>() as u64 + size_column
    }
}

impl<T: RunLengthEncodable + Debug + Ord + Copy> ColumnEnum<T> {
    pub(crate) fn iter_mut(&self) -> ColumnScanVectorMut<T> {
        match self {
            ColumnEnum::ColumnVector(column_vector) => column_vector.iter_mut(),
            ColumnEnum::ColumnRle(_) => unimplemented!(),
        }
    }
}
