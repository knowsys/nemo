use bytesize::ByteSize;

use crate::datatypes::{ColumnDataType, Double, Float, StorageValueT};
use crate::datatypes::{RunLengthEncodable, StorageTypeName};
use crate::generate_datatype_forwarder;
use crate::management::ByteSized;
use std::mem::size_of;
use std::{fmt::Debug, ops::Range};

use super::column::{Column, ColumnEnum};
use super::columnscan::{ColumnScanCell, ColumnScanEnum, ColumnScanT};

/// Simple implementation of a [`Column`] that uses a second column to manage interval bounds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnWithIntervals<T: RunLengthEncodable> {
    data: ColumnEnum<T>,
    int_starts: ColumnEnum<usize>,
}

impl<T> ColumnWithIntervals<T>
where
    T: ColumnDataType,
{
    /// Constructs a new [`ColumnWithIntervals`] given data column and a column containing the intervals
    pub fn new(data: ColumnEnum<T>, int_starts: ColumnEnum<usize>) -> ColumnWithIntervals<T> {
        ColumnWithIntervals { data, int_starts }
    }

    /// Return data column
    pub fn get_data_column(&self) -> &ColumnEnum<T> {
        &self.data
    }

    /// Return column containing the intervals
    pub fn get_int_column(&self) -> &ColumnEnum<usize> {
        &self.int_starts
    }

    /// Returns the number of intervals in the interval column.
    pub fn int_len(&self) -> usize {
        self.int_starts.len()
    }

    /// Destructure into data and interval column
    pub fn as_parts(&self) -> (&ColumnEnum<T>, &ColumnEnum<usize>) {
        (&self.data, &self.int_starts)
    }

    /// Returns the smallest and largest index of the interval with the given
    /// index.
    ///
    /// # Panics
    /// Panics if `int_idx` is out of bounds.
    pub fn int_bounds(&self, int_idx: usize) -> Range<usize> {
        let start_idx = self.int_starts.get(int_idx);
        if int_idx + 1 < self.int_starts.len() {
            start_idx..self.int_starts.get(int_idx + 1)
        } else {
            start_idx..self.data.len()
        }
    }
}

impl<'a, T> Column<'a, T> for ColumnWithIntervals<T>
where
    T: 'a + ColumnDataType,
{
    type Scan = ColumnScanEnum<'a, T>;

    fn len(&self) -> usize {
        self.data.len()
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn get(&self, index: usize) -> T {
        self.data.get(index)
    }

    fn iter(&'a self) -> Self::Scan {
        self.data.iter()
    }
}

impl<T: ColumnDataType> ByteSized for ColumnWithIntervals<T> {
    fn size_bytes(&self) -> ByteSize {
        ByteSize::b(size_of::<Self>() as u64)
            + self.data.size_bytes()
            + self.int_starts.size_bytes()
    }
}

/// Enum for Interval Column with different underlying datatypes
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnWithIntervalsT {
    /// Case u32
    Id32(ColumnWithIntervals<u32>),
    /// Case u64
    Id64(ColumnWithIntervals<u64>),
    /// Case i64
    Int64(ColumnWithIntervals<i64>),
    /// Case Float
    Float(ColumnWithIntervals<Float>),
    /// Case Double
    Double(ColumnWithIntervals<Double>),
}

generate_datatype_forwarder!(forward_to_interval_column);

impl ColumnWithIntervalsT {
    /// Returns the number of intervals in the interval column.
    pub fn int_len(&self) -> usize {
        forward_to_interval_column!(self, int_len)
    }

    /// Returns the smallest and largest index of the interval with the given
    /// index.
    ///
    /// # Panics
    /// Panics if `int_idx` is out of bounds.
    pub fn int_bounds(&self, int_idx: usize) -> Range<usize> {
        forward_to_interval_column!(self, int_bounds(int_idx))
    }

    /// Destructure into a pair of data column scan and interval column
    pub fn as_parts(&self) -> (ColumnScanT<'_>, ColumnScanEnum<'_, usize>) {
        match self {
            Self::Id32(this) => {
                let (data, intervals) = this.as_parts();
                (
                    ColumnScanT::Id32(ColumnScanCell::new(data.iter())),
                    intervals.iter(),
                )
            }
            Self::Id64(this) => {
                let (data, intervals) = this.as_parts();
                (
                    ColumnScanT::Id64(ColumnScanCell::new(data.iter())),
                    intervals.iter(),
                )
            }
            Self::Int64(this) => {
                let (data, intervals) = this.as_parts();
                (
                    ColumnScanT::Int64(ColumnScanCell::new(data.iter())),
                    intervals.iter(),
                )
            }
            Self::Float(this) => {
                let (data, intervals) = this.as_parts();
                (
                    ColumnScanT::Float(ColumnScanCell::new(data.iter())),
                    intervals.iter(),
                )
            }
            Self::Double(this) => {
                let (data, intervals) = this.as_parts();
                (
                    ColumnScanT::Double(ColumnScanCell::new(data.iter())),
                    intervals.iter(),
                )
            }
        }
    }

    /// Return the data type name of the column.
    pub fn get_type(&self) -> StorageTypeName {
        match self {
            Self::Id32(_) => StorageTypeName::Id32,
            Self::Id64(_) => StorageTypeName::Id64,
            Self::Int64(_) => StorageTypeName::Int64,
            Self::Float(_) => StorageTypeName::Float,
            Self::Double(_) => StorageTypeName::Double,
        }
    }

    /// Return the u64 version of the column
    pub fn as_u64(&self) -> Option<&ColumnWithIntervals<u64>> {
        if let Self::Id64(col) = self {
            return Some(col);
        }
        None
    }

    /// Return the u32 version of the column
    pub fn as_u32(&self) -> Option<&ColumnWithIntervals<u32>> {
        if let Self::Id32(col) = self {
            return Some(col);
        }
        None
    }
}

impl<'a> Column<'a, StorageValueT> for ColumnWithIntervalsT {
    type Scan = ColumnScanT<'a>;

    fn len(&self) -> usize {
        forward_to_interval_column!(self, len)
    }
    fn is_empty(&self) -> bool {
        forward_to_interval_column!(self, is_empty)
    }
    fn get(&self, index: usize) -> StorageValueT {
        forward_to_interval_column!(self, get(index).as_variant_of(StorageValueT))
    }

    fn iter(&'a self) -> Self::Scan {
        forward_to_interval_column!(
            self,
            iter.wrap_with(ColumnScanCell::new)
                .as_variant_of(ColumnScanT)
        )
    }
}

impl ByteSized for ColumnWithIntervalsT {
    fn size_bytes(&self) -> ByteSize {
        ByteSize::b(size_of::<Self>() as u64) + forward_to_interval_column!(self, size_bytes)
    }
}

#[cfg(test)]
mod test {
    use super::ColumnWithIntervals;
    use crate::columnar::column_storage::{
        column::{Column, ColumnEnum},
        column_vector::ColumnVector,
    };
    use test_log::test;

    #[test]
    fn test_u64_column() {
        let data: Vec<u64> = vec![1, 2, 3, 10, 11, 12, 20, 30, 31];
        let int_starts: Vec<usize> = vec![0, 3, 6, 7];

        let v_data = ColumnEnum::ColumnVector(ColumnVector::new(data));
        let v_int_starts = ColumnEnum::ColumnVector(ColumnVector::new(int_starts));
        let gic = ColumnWithIntervals::new(v_data, v_int_starts);

        assert_eq!(gic.len(), 9);
        assert_eq!(gic.get(0), 1);
        assert_eq!(gic.get(1), 2);
        assert_eq!(gic.get(2), 3);
        assert_eq!(gic.get(3), 10);

        assert_eq!(gic.int_len(), 4);
        assert_eq!(gic.int_bounds(0), 0..3);
        assert_eq!(gic.int_bounds(1), 3..6);
        assert_eq!(gic.int_bounds(2), 6..7);
        assert_eq!(gic.int_bounds(3), 7..9);
    }
}
