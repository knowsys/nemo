use bytesize::ByteSize;

use nemo_physical::{
    columnar::column::{Column, ColumnEnum},
    datatypes::{
        ColumnDataType, Double, Float, RunLengthEncodable, StorageTypeName, StorageValueT,
    },
    generate_datatype_forwarder,
    management::bytesized::ByteSized,
};
use std::ops::Range;

use super::column_scan::{ColumnScanCell, ColumnScanEnum, ColumnScanT};

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

impl<T> ByteSized for ColumnWithIntervals<T>
where
    T: ColumnDataType,
{
    fn size_bytes(&self) -> ByteSize {
        todo!()
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
        match &self.data {
            ColumnEnum::ColumnVector(col) => ColumnScanEnum::ColumnScanVector(col.iter()),
            ColumnEnum::ColumnRle(col) => ColumnScanEnum::ColumnScanRle(col.iter()),
        }
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

    fn column_iter<T>(col: &ColumnEnum<T>) -> ColumnScanEnum<T>
    where
        T: ColumnDataType,
    {
        match col {
            ColumnEnum::ColumnVector(col) => ColumnScanEnum::ColumnScanVector(col.iter()),
            ColumnEnum::ColumnRle(col) => ColumnScanEnum::ColumnScanRle(col.iter()),
        }
    }

    /// Destructure into a pair of data column scan and interval column
    pub fn as_parts(&self) -> (ColumnScanT<'_>, ColumnScanEnum<'_, usize>) {
        match self {
            Self::Id32(this) => {
                let (data, intervals) = this.as_parts();
                (
                    ColumnScanT::Id32(ColumnScanCell::new(Self::column_iter(data))),
                    Self::column_iter(intervals),
                )
            }
            Self::Id64(this) => {
                let (data, intervals) = this.as_parts();
                (
                    ColumnScanT::Id64(ColumnScanCell::new(Self::column_iter(data))),
                    Self::column_iter(intervals),
                )
            }
            Self::Int64(this) => {
                let (data, intervals) = this.as_parts();
                (
                    ColumnScanT::Int64(ColumnScanCell::new(Self::column_iter(data))),
                    Self::column_iter(intervals),
                )
            }
            Self::Float(this) => {
                let (data, intervals) = this.as_parts();
                (
                    ColumnScanT::Float(ColumnScanCell::new(Self::column_iter(data))),
                    Self::column_iter(intervals),
                )
            }
            Self::Double(this) => {
                let (data, intervals) = this.as_parts();
                (
                    ColumnScanT::Double(ColumnScanCell::new(Self::column_iter(data))),
                    Self::column_iter(intervals),
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

impl ByteSized for ColumnWithIntervalsT {
    fn size_bytes(&self) -> ByteSize {
        todo!()
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
