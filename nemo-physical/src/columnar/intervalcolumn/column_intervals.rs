use std::ops::Range;

use crate::{
    columnar::{
        column::{Column, ColumnEnum},
        columnscan::{ColumnScanCell, ColumnScanEnum, ColumnScanRainbow},
    },
    datatypes::{ColumnDataType, Double, Float, StorageTypeName},
    management::ByteSized,
};

use super::interval_lookup::{lookup_column_single::IntervalLookupColumnSingle, IntervalLookup};

/// Defines the lookup method used in [IntervalColumn]
type IntervalLookupMethod = IntervalLookupColumnSingle;

#[derive(Debug, Clone)]
pub(crate) struct IntervalColumn<T>
where
    T: ColumnDataType,
{
    /// [Column][crate::columnar::column::Column]
    /// that stores the actual data
    data: ColumnEnum<T>,

    /// Associates each entry from the previous layer
    /// with a sorted interval of values in the `data` column of this layer
    interval_lookup: IntervalLookupMethod,
}

impl<'a, T> Column<'a, T> for IntervalColumn<T>
where
    T: 'a + ColumnDataType,
{
    type Scan = ColumnScanEnum<'a, T>;

    fn len(&self) -> usize {
        self.data.len()
    }

    fn get(&self, index: usize) -> T {
        self.data.get(index)
    }

    fn iter(&'a self) -> Self::Scan {
        self.data.iter()
    }
}

impl<T> IntervalLookup for IntervalColumn<T>
where
    T: ColumnDataType,
{
    fn interval_bounds(&self, index: usize) -> Option<Range<usize>> {
        self.interval_lookup.interval_bounds(index)
    }
}

impl<T> ByteSized for IntervalColumn<T>
where
    T: ColumnDataType,
{
    fn size_bytes(&self) -> bytesize::ByteSize {
        self.data.size_bytes() + self.interval_lookup.size_bytes()
    }
}

/// [IntervalColumn] for each [StorageTypeName]
#[derive(Debug, Clone)]
pub struct IntervalColumnT {
    /// Case [StorageTypeName::Id32]
    column_id32: IntervalColumn<u32>,
    /// Case [StorageTypeName::Id64]
    column_id64: IntervalColumn<u64>,
    /// Case [StorageTypeName::Int64]
    column_int64: IntervalColumn<i64>,
    /// Case [StorageTypeName::Float]
    column_float: IntervalColumn<Float>,
    /// Case [StorageTypeName::Double]
    column_double: IntervalColumn<Double>,
}

impl IntervalColumnT {
    pub fn iter(&self) -> ColumnScanRainbow {
        ColumnScanRainbow {
            scan_id32: ColumnScanCell::new(self.column_id32.iter()),
            scan_id64: ColumnScanCell::new(self.column_id64.iter()),
            scan_i64: ColumnScanCell::new(self.column_int64.iter()),
            scan_float: ColumnScanCell::new(self.column_float.iter()),
            scan_double: ColumnScanCell::new(self.column_double.iter()),
        }
    }

    pub fn interval_bounds(
        &self,
        storage_type: StorageTypeName,
        index: usize,
    ) -> Option<Range<usize>> {
        match storage_type {
            StorageTypeName::Id32 => self.column_id32.interval_bounds(index),
            StorageTypeName::Id64 => self.column_id64.interval_bounds(index),
            StorageTypeName::Int64 => self.column_int64.interval_bounds(index),
            StorageTypeName::Float => self.column_float.interval_bounds(index),
            StorageTypeName::Double => self.column_double.interval_bounds(index),
        }
    }
}
