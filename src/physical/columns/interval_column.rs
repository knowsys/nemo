use super::{
    Column, GenericColumnScanEnum, GenericIntervalColumnEnum, RangedColumnScanCell,
    RangedColumnScanEnum, RangedColumnScanT,
};
use crate::{
    generate_datatype_forwarder, generate_forwarder,
    physical::datatypes::{DataValueT, Double, Field, Float, FloorToUsize},
};
use std::{fmt::Debug, ops::Range};

/// Column of values that are grouped into numbered intervals.
pub trait IntervalColumn<'a, T>: Debug + Column<'a, T> {
    /// Returns the number of intervals in the interval column.
    fn int_len(&self) -> usize;

    /// Returns the smallest and largest index of the interval with the given
    /// index.
    ///
    /// # Panics
    /// Panics if `int_idx` is out of bounds.
    fn int_bounds(&self, int_idx: usize) -> Range<usize>;
}

/// Enum for columns of all supported basic types.
#[derive(Debug)]
pub enum IntervalColumnEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    /// Case GenericIntervalColumn
    GenericIntervalColumn(GenericIntervalColumnEnum<'a, T>),
}

generate_forwarder!(forward_to_interval_column;
                    GenericIntervalColumn);

impl<'a, T> Column<'a, T> for IntervalColumnEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    type ColScan = RangedColumnScanEnum<'a, T>;

    fn len(&self) -> usize {
        forward_to_interval_column!(self, len)
    }

    fn is_empty(&self) -> bool {
        forward_to_interval_column!(self, is_empty)
    }

    fn get(&self, index: usize) -> T {
        forward_to_interval_column!(self, get(index))
    }

    fn iter(&'a self) -> Self::ColScan {
        forward_to_interval_column!(
            self,
            iter.as_variant_of(GenericColumnScanEnum)
                .wrap_with(RangedColumnScanEnum::GenericColumnScan)
        )
    }
}

impl<'a, T> IntervalColumn<'a, T> for IntervalColumnEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn int_len(&self) -> usize {
        forward_to_interval_column!(self, int_len)
    }

    fn int_bounds(&self, int_idx: usize) -> Range<usize> {
        forward_to_interval_column!(self, int_bounds(int_idx))
    }
}

/// Enum for Interval Column with different underlying datatypes
#[derive(Debug)]
pub enum IntervalColumnT<'a> {
    /// Case u64
    U64(IntervalColumnEnum<'a, u64>),
    /// Case Float
    Float(IntervalColumnEnum<'a, Float>),
    /// Case Double
    Double(IntervalColumnEnum<'a, Double>),
}

generate_datatype_forwarder!(forward_to_interval_column_enum);

impl<'a> Column<'a, DataValueT> for IntervalColumnT<'a> {
    type ColScan = RangedColumnScanT<'a>;

    fn len(&self) -> usize {
        forward_to_interval_column_enum!(self, len)
    }

    fn is_empty(&self) -> bool {
        forward_to_interval_column_enum!(self, is_empty)
    }

    fn get(&self, index: usize) -> DataValueT {
        forward_to_interval_column_enum!(self, get(index).as_variant_of(DataValueT))
    }

    fn iter(&'a self) -> Self::ColScan {
        forward_to_interval_column_enum!(
            self,
            iter.wrap_with(RangedColumnScanCell::new)
                .as_variant_of(RangedColumnScanT)
        )
    }
}

impl<'a> IntervalColumn<'a, DataValueT> for IntervalColumnT<'a> {
    fn int_len(&self) -> usize {
        forward_to_interval_column_enum!(self, int_len)
    }

    fn int_bounds(&self, int_idx: usize) -> Range<usize> {
        forward_to_interval_column_enum!(self, int_bounds(int_idx))
    }
}
