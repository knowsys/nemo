//! This module defines the trait [Column] and its implementations,
//! as well as [ColumnEnum],
//! which collects all implementations of [Column] into a single object.

pub(crate) mod rle;
pub(crate) mod vector;

use std::{fmt::Debug, mem::size_of};

use rle::ColumnScanRle;
use vector::ColumnScanVector;

use crate::{
    generate_forwarder, management::bytesized::ByteSized, storagevalues::storagevalue::StorageValue,
};

use self::{rle::ColumnRle, vector::ColumnVector};

use super::{
    columnscan::{ColumnScan, ColumnScanEnum},
    columntype::ColumnType,
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
pub(crate) enum ColumnEnum<T: ColumnType> {
    /// Case ColumnVector
    ColumnVector(ColumnVector<T>),
    /// Case ColumnRle
    ColumnRle(ColumnRle<T>),
}

impl<T> ColumnEnum<T>
where
    T: StorageValue,
{
    /// Returns a [ColumnScanEnum] depending on the [Column].
    pub fn scan(&self) -> ColumnScanEnum<T> {
        match self {
            Self::ColumnVector(col) => ColumnScanEnum::Vector(col.iter()),
            Self::ColumnRle(col) => ColumnScanEnum::Rle(col.iter()),
        }
    }
}

generate_forwarder!(forward_to_column;
                    ColumnVector,
                    ColumnRle);

/// Iterator for a [ColumnEnum].
#[derive(Debug)]
pub(crate) enum ColumnEnumIterator<'a, T>
where
    T: ColumnType,
{
    /// Case ColumnVector
    Vector(ColumnScanVector<'a, T>),
    /// Case ColumnRle
    Rle(ColumnScanRle<'a, T>),
}

impl<'a, T> Iterator for ColumnEnumIterator<'a, T>
where
    T: ColumnType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ColumnEnumIterator::Vector(iterator) => iterator.next(),
            ColumnEnumIterator::Rle(iterator) => iterator.next(),
        }
    }
}

impl<'a, T> ColumnScan for ColumnEnumIterator<'a, T>
where
    T: 'a + ColumnType,
{
    fn seek(&mut self, value: Self::Item) -> Option<Self::Item> {
        match self {
            ColumnEnumIterator::Vector(iterator) => iterator.seek(value),
            ColumnEnumIterator::Rle(iterator) => iterator.seek(value),
        }
    }

    fn current(&self) -> Option<Self::Item> {
        match self {
            ColumnEnumIterator::Vector(iterator) => iterator.current(),
            ColumnEnumIterator::Rle(iterator) => iterator.current(),
        }
    }

    fn reset(&mut self) {
        match self {
            ColumnEnumIterator::Vector(iterator) => iterator.reset(),
            ColumnEnumIterator::Rle(iterator) => iterator.reset(),
        }
    }

    fn pos(&self) -> Option<usize> {
        match self {
            ColumnEnumIterator::Vector(iterator) => iterator.pos(),
            ColumnEnumIterator::Rle(iterator) => iterator.pos(),
        }
    }

    fn narrow(&mut self, interval: std::ops::Range<usize>) {
        match self {
            ColumnEnumIterator::Vector(iterator) => iterator.narrow(interval),
            ColumnEnumIterator::Rle(iterator) => iterator.narrow(interval),
        }
    }
}

impl<'a, T> Column<'a, T> for ColumnEnum<T>
where
    T: 'a + ColumnType,
{
    type Scan = ColumnEnumIterator<'a, T>;

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
            Self::ColumnVector(col) => ColumnEnumIterator::Vector(col.iter()),
            Self::ColumnRle(col) => ColumnEnumIterator::Rle(col.iter()),
        }
    }
}

impl<T: ColumnType> ByteSized for ColumnEnum<T> {
    fn size_bytes(&self) -> u64 {
        let size_column = forward_to_column!(self, size_bytes);
        size_of::<Self>() as u64 + size_column
    }
}
