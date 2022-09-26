use super::{
    column_scan::ColumnScan, DifferenceScan, EqualColumnScan, EqualValueScan,
    GenericColumnScanEnum, MinusScan, OrderedMergeJoin, PassScan, ReorderScan, RleColumnScan,
    UnionScan,
};
use crate::{
    generate_datatype_forwarder, generate_forwarder,
    physical::datatypes::{ColumnDataType, DataValueT, Double, Float},
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
    T: 'a + ColumnDataType,
{
    /// Case GenericColumnScan
    GenericColumnScan(GenericColumnScanEnum<'a, T>),
    /// Case RleColumnScan
    RleColumnScan(RleColumnScan<'a, T>),
    /// Case OrderedMergeJoin
    OrderedMergeJoin(OrderedMergeJoin<'a, T>),
    /// Case ReorderScan
    ReorderScan(ReorderScan<'a, T>),
    /// Case EqualColumnScan
    EqualColumnScan(EqualColumnScan<'a, T>),
    /// Case EqualValueScan
    EqualValueScan(EqualValueScan<'a, T>),
    /// Case OrderedMergeJoin
    PassScan(PassScan<'a, T>),
    /// Case DifferenceScan
    DifferenceScan(DifferenceScan<'a, T>),
    /// Case MinusScan
    MinusScan(MinusScan<'a, T>),
    /// Case UnionScan
    UnionScan(UnionScan<'a, T>),
}

impl<'a, T> From<GenericColumnScanEnum<'a, T>> for RangedColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: GenericColumnScanEnum<'a, T>) -> Self {
        Self::GenericColumnScan(cs)
    }
}

impl<'a, T> From<RleColumnScan<'a, T>> for RangedColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: RleColumnScan<'a, T>) -> Self {
        Self::RleColumnScan(cs)
    }
}

impl<'a, T> From<OrderedMergeJoin<'a, T>> for RangedColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: OrderedMergeJoin<'a, T>) -> Self {
        Self::OrderedMergeJoin(cs)
    }
}

impl<'a, T> From<ReorderScan<'a, T>> for RangedColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ReorderScan<'a, T>) -> Self {
        Self::ReorderScan(cs)
    }
}

impl<'a, T> From<EqualColumnScan<'a, T>> for RangedColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: EqualColumnScan<'a, T>) -> Self {
        Self::EqualColumnScan(cs)
    }
}

impl<'a, T> From<EqualValueScan<'a, T>> for RangedColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: EqualValueScan<'a, T>) -> Self {
        Self::EqualValueScan(cs)
    }
}

impl<'a, T> From<PassScan<'a, T>> for RangedColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: PassScan<'a, T>) -> Self {
        Self::PassScan(cs)
    }
}

impl<'a, T> From<MinusScan<'a, T>> for RangedColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: MinusScan<'a, T>) -> Self {
        Self::MinusScan(cs)
    }
}

impl<'a, T> From<DifferenceScan<'a, T>> for RangedColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: DifferenceScan<'a, T>) -> Self {
        Self::DifferenceScan(cs)
    }
}

impl<'a, T> From<UnionScan<'a, T>> for RangedColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: UnionScan<'a, T>) -> Self {
        Self::UnionScan(cs)
    }
}

generate_forwarder!(forward_to_column_scan;
                    GenericColumnScan,
                    RleColumnScan,
                    OrderedMergeJoin,
                    ReorderScan, 
                    EqualColumnScan, 
                    EqualValueScan,
                    PassScan,
                    DifferenceScan,
                    MinusScan,
                    UnionScan);

impl<'a, T> RangedColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Return all positions in the underlying column the cursor is currently at
    pub fn pos_multiple(&self) -> Option<Vec<usize>> {
        match self {
            Self::ReorderScan(cs) => cs.pos_multiple(),
            _ => {
                unimplemented!("pos_multiple is only available for ReorderScan")
            }
        }
    }

    /// Set iterator to a set of possibly disjoint ranged
    pub fn narrow_ranges(&mut self, intervals: Vec<Range<usize>>) {
        match self {
            Self::ReorderScan(cs) => cs.narrow_ranges(intervals),
            _ => {
                unimplemented!("narrow_ranges is only available for ReorderScan")
            }
        }
    }

    /// For DifferenceScan, returns whether its scans point to the same value
    pub fn is_equal(&self) -> bool {
        match self {
            Self::DifferenceScan(cs) => cs.is_equal(),
            _ => unimplemented!("is_equal is only available for DifferenceScan"),
        }
    }

    /// Assumes that column scan is a union scan
    /// and returns a vector containing the positions of the scans with the smallest values
    pub fn get_smallest_scans(&self) -> &Vec<usize> {
        match self {
            Self::UnionScan(cs) => cs.get_smallest_scans(),
            _ => {
                unimplemented!("get_smallest_scans is only available for union_scans")
            }
        }
    }

    /// Assumes that column scan is a union scan
    /// Set a vector that indicates which scans are currently active and should be considered
    pub fn set_active_scans(&mut self, active_scans: Vec<usize>) {
        match self {
            Self::UnionScan(cs) => cs.set_active_scans(active_scans),
            _ => {
                unimplemented!("set_active_scans is only available for union_scans")
            }
        }
    }
}

impl<'a, T> Iterator for RangedColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        forward_to_column_scan!(self, next)
    }
}

impl<'a, T> ColumnScan for RangedColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
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
    T: 'a + ColumnDataType,
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
    T: 'a + ColumnDataType;

impl<T> Debug for RangedColumnScanCell<'_, T>
where
    T: ColumnDataType,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let rcs = unsafe { &*self.0.get() };
        f.debug_tuple("RangedColumnScanCell").field(rcs).finish()
    }
}

impl<'a, T> RangedColumnScanCell<'a, T>
where
    T: 'a + ColumnDataType,
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

    /// Forward `pos_multiple` to the underlying [`RangedColumnScanEnum`].
    pub fn pos_multiple(&self) -> Option<Vec<usize>> {
        unsafe { &mut *self.0.get() }.pos_multiple()
    }

    /// Forward `narrow_ranges` to the underlying [`RangedColumnScanEnum`].
    pub fn narrow_ranges(&mut self, intervals: Vec<Range<usize>>) {
        unsafe { &mut *self.0.get() }.narrow_ranges(intervals)
    }

    /// Forward `is_equal` to the underlying [`RangedColumnScanEnum`].
    pub fn is_equal(&self) -> bool {
        unsafe { &mut *self.0.get() }.is_equal()
    }

    /// Forward `get_smallest_scans` to the underlying [`RangedColumnScanEnum`].
    pub fn get_smallest_scans(&self) -> &Vec<usize> {
        unsafe { &mut *self.0.get() }.get_smallest_scans()
    }

    /// Forward `get_smallest_scans` to the underlying [`RangedColumnScanEnum`].
    pub fn set_active_scans(&mut self, active_scans: Vec<usize>) {
        unsafe { &mut *self.0.get() }.set_active_scans(active_scans);
    }
}

impl<'a, S, T> From<S> for RangedColumnScanCell<'a, T>
where
    S: Into<RangedColumnScanEnum<'a, T>>,
    T: 'a + ColumnDataType,
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
impl<'a> RangedColumnScanT<'a> {
    /// Return all positions in the underlying column the cursor is currently at
    pub fn pos_multiple(&self) -> Option<Vec<usize>> {
        forward_to_ranged_column_scan_cell!(self, pos_multiple)
    }

    /// Set iterator to a set of possibly disjoint ranged
    pub fn narrow_ranges(&mut self, intervals: Vec<Range<usize>>) {
        forward_to_ranged_column_scan_cell!(self, narrow_ranges(intervals))
    }

    /// For DifferenceScan, returns whether its scans point to the same value
    pub fn is_equal(&self) -> bool {
        forward_to_ranged_column_scan_cell!(self, is_equal)
    }

    /// Assumes that column scan is a union scan
    /// and returns a vector containing the positions of the scans with the smallest values
    pub fn get_smallest_scans(&self) -> &Vec<usize> {
        forward_to_ranged_column_scan_cell!(self, get_smallest_scans)
    }

    /// Assumes that column scan is a union scan
    /// Set a vector that indicates which scans are currently active and should be considered
    pub fn set_active_scans(&mut self, active_scans: Vec<usize>) {
        forward_to_ranged_column_scan_cell!(self, set_active_scans(active_scans))
    }
}

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
