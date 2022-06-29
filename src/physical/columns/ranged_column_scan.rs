use super::{column_scan::ColumnScan, GenericColumnScanEnum, OrderedMergeJoin, RleColumnScan};
use crate::{
    generate_datatype_forwarder, generate_forwarder,
    physical::datatypes::{DataValueT, Double, Field, Float, FloorToUsize},
};
use std::{cell::UnsafeCell, fmt::Debug, ops::Range};

/// Iterator for a sorted interval of values that also stores the current position
pub trait RangedColumnScan: ColumnScan {
    /// Return the current position of this iterator, or None if the iterator is
    /// before the first or after the last element.
    fn pos(&self) -> Option<usize>;

    /// Restricts the iterator to the given `interval`.
    fn narrow(&mut self, interval: Range<usize>);
}

/// Enum for ranged column scans for all the supported types
#[derive(Debug)]
pub enum RangedColumnScanEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    /// Case GenericColumnScan
    GenericColumnScan(GenericColumnScanEnum<'a, T>),
    /// Case RleColumnScan
    RleColumnScan(RleColumnScan<'a, T>),
    /// Case OrderedMergeJoin
    OrderedMergeJoin(OrderedMergeJoin<'a, T>),
}

impl<'a, T> From<GenericColumnScanEnum<'a, T>> for RangedColumnScanEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn from(cs: GenericColumnScanEnum<'a, T>) -> Self {
        Self::GenericColumnScan(cs)
    }
}

impl<'a, T> From<RleColumnScan<'a, T>> for RangedColumnScanEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn from(cs: RleColumnScan<'a, T>) -> Self {
        Self::RleColumnScan(cs)
    }
}

impl<'a, T> From<OrderedMergeJoin<'a, T>> for RangedColumnScanEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn from(cs: OrderedMergeJoin<'a, T>) -> Self {
        Self::OrderedMergeJoin(cs)
    }
}

generate_forwarder!(forward_to_column_scan;
                    GenericColumnScan,
                    RleColumnScan,
                    OrderedMergeJoin);

impl<'a, T> Iterator for RangedColumnScanEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        forward_to_column_scan!(self, next)
    }
}

impl<'a, T> ColumnScan for RangedColumnScanEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn seek(&mut self, value: Self::Item) -> Option<Self::Item> {
        forward_to_column_scan!(self, seek(value))
    }

    fn current(&mut self) -> Option<Self::Item> {
        forward_to_column_scan!(self, current)
    }

    fn reset(&mut self) {
        forward_to_column_scan!(self, reset)
    }
}

impl<'a, T> RangedColumnScan for RangedColumnScanEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn pos(&self) -> Option<usize> {
        forward_to_column_scan!(self, pos)
    }

    fn narrow(&mut self, interval: Range<usize>) {
        forward_to_column_scan!(self, narrow(interval))
    }
}

/// A wrapper around a cell type holding a `RangedColumnScanEnum`.
#[repr(transparent)]
pub struct RangedColumnScanCell<'a, T>(UnsafeCell<RangedColumnScanEnum<'a, T>>)
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field;

impl<T> Debug for RangedColumnScanCell<'_, T>
where
    T: Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let rcs = unsafe { &*self.0.get() };
        f.debug_tuple("RangedColumnScanCell").field(rcs).finish()
    }
}

impl<'a, T> RangedColumnScanCell<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    /// Construct a new `RangedColumnScanCell` from the given [`RangedColumnScanEnum`].
    pub fn new(cs: RangedColumnScanEnum<'a, T>) -> Self {
        Self(UnsafeCell::new(cs))
    }

    /// Forward `next` to the underlying [`RangedColumnScanEnum`].
    #[inline]
    pub fn next(&self) -> Option<<RangedColumnScanEnum<'a, T> as Iterator>::Item> {
        unsafe { &mut *self.0.get() }.next()
    }

    /// Forward `seek` to the underlying [`RangedColumnScanEnum`].
    #[inline]
    pub fn seek(
        &self,
        value: <RangedColumnScanEnum<'a, T> as Iterator>::Item,
    ) -> Option<<RangedColumnScanEnum<'a, T> as Iterator>::Item> {
        unsafe { &mut *self.0.get() }.seek(value)
    }

    /// Forward `current` to the underlying [`RangedColumnScanEnum`].
    #[inline]
    pub fn current(&self) -> Option<<RangedColumnScanEnum<'a, T> as Iterator>::Item> {
        unsafe { &mut *self.0.get() }.current()
    }

    /// Forward `reset` to the underlying [`RangedColumnScanEnum`].
    #[inline]
    pub fn reset(&self) {
        unsafe { &mut *self.0.get() }.reset()
    }

    /// Forward `pos` to the underlying [`RangedColumnScanEnum`].
    #[inline]
    pub fn pos(&self) -> Option<usize> {
        unsafe { &(*self.0.get()) }.pos()
    }

    /// Forward `narrow` to the underlying [`RangedColumnScanEnum`].
    #[inline]
    pub fn narrow(&self, interval: Range<usize>) {
        unsafe { &mut *self.0.get() }.narrow(interval)
    }
}

impl<'a, S, T> From<S> for RangedColumnScanCell<'a, T>
where
    S: Into<RangedColumnScanEnum<'a, T>>,
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn from(cs: S) -> Self {
        Self(UnsafeCell::new(cs.into()))
    }
}

/// enum for RangedColumnScan for underlying data type
#[derive(Debug)]
pub enum RangedColumnScanT<'a> {
    /// Case u64
    U64(RangedColumnScanCell<'a, u64>),
    /// Case Float
    Float(RangedColumnScanCell<'a, Float>),
    /// Case Double
    Double(RangedColumnScanCell<'a, Double>),
}

generate_datatype_forwarder!(forward_to_ranged_column_scan_cell);

impl<'a> Iterator for RangedColumnScanT<'a> {
    type Item = DataValueT;

    fn next(&mut self) -> Option<Self::Item> {
        forward_to_ranged_column_scan_cell!(self, next.map_to(DataValueT))
    }
}

impl<'a> ColumnScan for RangedColumnScanT<'a> {
    fn seek(&mut self, value: Self::Item) -> Option<Self::Item> {
        match self {
            Self::U64(cs) => match value {
                Self::Item::U64(val) => cs.seek(val).map(DataValueT::U64),
                Self::Item::Float(_val) => None,
                Self::Item::Double(_val) => None,
            },
            Self::Float(cs) => match value {
                Self::Item::U64(_val) => None,
                Self::Item::Float(val) => cs.seek(val).map(DataValueT::Float),
                Self::Item::Double(_val) => None,
            },
            Self::Double(cs) => match value {
                Self::Item::U64(_val) => None,
                Self::Item::Float(_val) => None,
                Self::Item::Double(val) => cs.seek(val).map(DataValueT::Double),
            },
        }
    }

    fn current(&mut self) -> Option<Self::Item> {
        forward_to_ranged_column_scan_cell!(self, current.map_to(DataValueT))
    }

    fn reset(&mut self) {
        forward_to_ranged_column_scan_cell!(self, reset)
    }
}

impl<'a> RangedColumnScan for RangedColumnScanT<'a> {
    fn pos(&self) -> Option<usize> {
        forward_to_ranged_column_scan_cell!(self, pos)
    }

    fn narrow(&mut self, interval: Range<usize>) {
        forward_to_ranged_column_scan_cell!(self, narrow(interval))
    }
}
