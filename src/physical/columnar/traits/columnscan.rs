use super::super::column_types::{rle::ColumnScanRle, vector::ColumnScanVector};
use super::super::operations::{
    ColumnScanCastEnum, ColumnScanEqualColumn, ColumnScanEqualValue, ColumnScanFollow,
    ColumnScanJoin, ColumnScanMinus, ColumnScanPass, ColumnScanReorder, ColumnScanUnion,
};

use crate::physical::columnar::operations::{ColumnScanConstant, ColumnScanCopy, ColumnScanNulls};
use crate::{
    generate_datatype_forwarder, generate_forwarder,
    physical::datatypes::{ColumnDataType, Double, Float, StorageValueT},
};
use std::{cell::UnsafeCell, fmt::Debug, ops::Range};

/// Iterator for a sorted interval of values
pub trait ColumnScan: Debug + Iterator {
    /// Find the next value that is at least as large as the given value,
    /// advance the iterator to this position, and return the value if it exists.
    fn seek(&mut self, value: Self::Item) -> Option<Self::Item>;

    /// Return the value at the current position, if any.
    fn current(&self) -> Option<Self::Item>;

    /// Return to the initial state
    /// Typically, the state of a [`ColumnScan`] is determined by the state of its sub scans
    /// as well as some additional information.
    /// This function is only supposed to reset the latter to its initial state (i.e. after calling new).
    /// The intention of this function is to use it after the internal iterators have been reset from the outside
    /// (e.g. by calling down() on the TrieScan that owns the sub iterators).
    /// So this call should be present in every implementation of the down() method of a TrieScan.
    fn reset(&mut self);

    /// Return the current position of this iterator, or None if the iterator is
    /// before the first or after the last element.
    fn pos(&self) -> Option<usize>;

    /// Restricts the iterator to the given `interval`.
    /// Resets the iterator just before the start of the interval.
    fn narrow(&mut self, interval: Range<usize>);
}

/// Enum for [`ColumnScan`] of all supported types
#[derive(Debug)]
pub enum ColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Case ColumnScanVector
    ColumnScanVector(ColumnScanVector<'a, T>),
    /// Case ColumnRleScan
    ColumnScanRle(ColumnScanRle<'a, T>),
    /// Case ColumnScanJoin
    ColumnScanJoin(ColumnScanJoin<'a, T>),
    /// Case ColumnScanCast
    ColumnScanCast(ColumnScanCastEnum<'a, T>),
    /// Case ColumnScanReorder
    ColumnScanReorder(ColumnScanReorder<'a, T>),
    /// Case ColumnScanEqualColumn
    ColumnScanEqualColumn(ColumnScanEqualColumn<'a, T>),
    /// Case ColumnScanEqualValue
    ColumnScanEqualValue(ColumnScanEqualValue<'a, T>),
    /// Case ColumnScanJoin
    ColumnScanPass(ColumnScanPass<'a, T>),
    /// Case ColumnScanFollow
    ColumnScanFollow(ColumnScanFollow<'a, T>),
    /// Case ColumnScanMinus
    ColumnScanMinus(ColumnScanMinus<'a, T>),
    /// Case ColumnScanUnion
    ColumnScanUnion(ColumnScanUnion<'a, T>),
    /// Case ColumnScanConstant
    ColumnScanConstant(ColumnScanConstant<T>),
    /// Case ColumnScanCopy
    ColumnScanCopy(ColumnScanCopy<'a, T>),
    /// Case ColumnScanNulls
    ColumnScanNulls(ColumnScanNulls<T>),
}

/// The following impl statements allow converting from a specific [`ColumnScan`] into a gerneral [`ColumnScanEnum`]

impl<'a, T> From<ColumnScanVector<'a, T>> for ColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ColumnScanVector<'a, T>) -> Self {
        Self::ColumnScanVector(cs)
    }
}

impl<'a, T> From<ColumnScanRle<'a, T>> for ColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ColumnScanRle<'a, T>) -> Self {
        Self::ColumnScanRle(cs)
    }
}

impl<'a, T> From<ColumnScanCastEnum<'a, T>> for ColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ColumnScanCastEnum<'a, T>) -> Self {
        Self::ColumnScanCast(cs)
    }
}

impl<'a, T> From<ColumnScanJoin<'a, T>> for ColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ColumnScanJoin<'a, T>) -> Self {
        Self::ColumnScanJoin(cs)
    }
}

impl<'a, T> From<ColumnScanReorder<'a, T>> for ColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ColumnScanReorder<'a, T>) -> Self {
        Self::ColumnScanReorder(cs)
    }
}

impl<'a, T> From<ColumnScanEqualColumn<'a, T>> for ColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ColumnScanEqualColumn<'a, T>) -> Self {
        Self::ColumnScanEqualColumn(cs)
    }
}

impl<'a, T> From<ColumnScanEqualValue<'a, T>> for ColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ColumnScanEqualValue<'a, T>) -> Self {
        Self::ColumnScanEqualValue(cs)
    }
}

impl<'a, T> From<ColumnScanPass<'a, T>> for ColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ColumnScanPass<'a, T>) -> Self {
        Self::ColumnScanPass(cs)
    }
}

impl<'a, T> From<ColumnScanMinus<'a, T>> for ColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ColumnScanMinus<'a, T>) -> Self {
        Self::ColumnScanMinus(cs)
    }
}

impl<'a, T> From<ColumnScanFollow<'a, T>> for ColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ColumnScanFollow<'a, T>) -> Self {
        Self::ColumnScanFollow(cs)
    }
}

impl<'a, T> From<ColumnScanUnion<'a, T>> for ColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ColumnScanUnion<'a, T>) -> Self {
        Self::ColumnScanUnion(cs)
    }
}

/// The following block makes functions which are specific to one variant of a [`ColumnScanEnum`]
/// available on the whole [`ColumnScanEnum`]
impl<'a, T> ColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Assumes that column scan is a [`ColumnScanReorder`]
    /// Return all positions in the underlying column the cursor is currently at
    pub fn pos_multiple(&self) -> Option<Vec<usize>> {
        if let Self::ColumnScanReorder(cs) = self {
            cs.pos_multiple()
        } else {
            unimplemented!("pos_multiple is only available for ColumnScanReorder")
        }
    }

    /// Assumes that column scan is a [`ColumnScanReorder`]
    /// Set iterator to a set of possibly disjoint ranged
    pub fn narrow_ranges(&mut self, intervals: Vec<Range<usize>>) {
        if let Self::ColumnScanReorder(cs) = self {
            cs.narrow_ranges(intervals)
        } else {
            unimplemented!("narrow_ranges is only available for ColumnScanReorder")
        }
    }

    /// Assumes that column scan is a [`ColumnScanFollow`]
    /// Returns whether its scans point to the same value
    pub fn is_equal(&self) -> bool {
        if let Self::ColumnScanFollow(cs) = self {
            cs.is_equal()
        } else {
            unimplemented!("is_equal is only available for ColumnScanFollow")
        }
    }

    /// Assumes that column scan is a [`ColumnScanUnion`]
    /// and returns a vector containing the positions of the scans with the smallest values
    pub fn get_smallest_scans(&self) -> &Vec<bool> {
        if let Self::ColumnScanUnion(cs) = self {
            cs.get_smallest_scans()
        } else {
            unimplemented!("get_smallest_scans is only available for ColumnScanUnion")
        }
    }

    /// Assumes that column scan is a [`ColumnScanUnion`]
    /// Set a vector that indicates which scans are currently active and should be considered
    pub fn set_active_scans(&mut self, active_scans: Vec<usize>) {
        if let Self::ColumnScanUnion(cs) = self {
            cs.set_active_scans(active_scans)
        } else {
            unimplemented!("set_active_scans is only available for ColumnScanUnion")
        }
    }

    /// Assumes that column scan is a [`ColumnScanMinus`]
    /// Set a vector that indicates which scans are currently active and should be considered
    pub fn minus_enable(&mut self, enabled: bool) {
        if let Self::ColumnScanMinus(cs) = self {
            cs.minus_enable(enabled)
        } else {
            unimplemented!("minus_enable is only available for ColumnScanMinus")
        }
    }
}

// Generate a macro forward_to_columnscan!, which takes a [`ColumnScanEnum`] and a function as arguments
// and unfolds into a `match` statement that calls the variant specific version of that function.
// Each new variant of a [`ColumnScanEnum`] must be added here.
// See `physical/util.rs` for a more detailed description of this macro.
generate_forwarder!(forward_to_columnscan;
    ColumnScanVector,
    ColumnScanRle,
    ColumnScanCast,
    ColumnScanJoin,
    ColumnScanReorder,
    ColumnScanEqualColumn,
    ColumnScanEqualValue,
    ColumnScanPass,
    ColumnScanFollow,
    ColumnScanMinus,
    ColumnScanUnion,
    ColumnScanConstant,
    ColumnScanCopy,
    ColumnScanNulls
);

impl<'a, T> Iterator for ColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        forward_to_columnscan!(self, next)
    }
}

impl<'a, T> ColumnScan for ColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn seek(&mut self, value: Self::Item) -> Option<Self::Item> {
        forward_to_columnscan!(self, seek(value))
    }

    fn current(&self) -> Option<Self::Item> {
        forward_to_columnscan!(self, current)
    }

    fn reset(&mut self) {
        forward_to_columnscan!(self, reset)
    }

    fn pos(&self) -> Option<usize> {
        forward_to_columnscan!(self, pos)
    }

    fn narrow(&mut self, interval: Range<usize>) {
        forward_to_columnscan!(self, narrow(interval))
    }
}

/// A wrapper around a cell type holding a `ColumnScanEnum`.
#[repr(transparent)]
pub struct ColumnScanCell<'a, T>(UnsafeCell<ColumnScanEnum<'a, T>>)
where
    T: 'a + ColumnDataType;

impl<T> Debug for ColumnScanCell<'_, T>
where
    T: ColumnDataType,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let rcs = unsafe { &*self.0.get() };
        f.debug_tuple("ColumnScanCell").field(rcs).finish()
    }
}

impl<'a, T> ColumnScanCell<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Construct a new `ColumnScanCell` from the given [`ColumnScanEnum`].
    pub fn new(cs: ColumnScanEnum<'a, T>) -> Self {
        Self(UnsafeCell::new(cs))
    }

    /// Forward `next` to the underlying [`ColumnScanEnum`].
    #[inline]
    pub fn next(&self) -> Option<<ColumnScanEnum<'a, T> as Iterator>::Item> {
        unsafe { &mut *self.0.get() }.next()
    }

    /// Forward `seek` to the underlying [`ColumnScanEnum`].
    #[inline]
    pub fn seek(
        &self,
        value: <ColumnScanEnum<'a, T> as Iterator>::Item,
    ) -> Option<<ColumnScanEnum<'a, T> as Iterator>::Item> {
        unsafe { &mut *self.0.get() }.seek(value)
    }

    /// Forward `current` to the underlying [`ColumnScanEnum`].
    #[inline]
    pub fn current(&self) -> Option<<ColumnScanEnum<'a, T> as Iterator>::Item> {
        unsafe { &mut *self.0.get() }.current()
    }

    /// Forward `reset` to the underlying [`ColumnScanEnum`].
    #[inline]
    pub fn reset(&self) {
        unsafe { &mut *self.0.get() }.reset()
    }

    /// Forward `pos` to the underlying [`ColumnScanEnum`].
    #[inline]
    pub fn pos(&self) -> Option<usize> {
        unsafe { &(*self.0.get()) }.pos()
    }

    /// Forward `narrow` to the underlying [`ColumnScanEnum`].
    #[inline]
    pub fn narrow(&self, interval: Range<usize>) {
        unsafe { &mut *self.0.get() }.narrow(interval)
    }

    /// Forward `pos_multiple` to the underlying [`ColumnScanEnum`].
    pub fn pos_multiple(&self) -> Option<Vec<usize>> {
        unsafe { &mut *self.0.get() }.pos_multiple()
    }

    /// Forward `narrow_ranges` to the underlying [`ColumnScanEnum`].
    pub fn narrow_ranges(&mut self, intervals: Vec<Range<usize>>) {
        self.0.get_mut().narrow_ranges(intervals)
    }

    /// Forward `is_equal` to the underlying [`ColumnScanEnum`].
    pub fn is_equal(&self) -> bool {
        unsafe { &mut *self.0.get() }.is_equal()
    }

    /// Forward `get_smallest_scans` to the underlying [`ColumnScanEnum`].
    /// This takes an exclusive reference as opposed to an immutable one, so that none of the
    /// mutating methods on &self can be called while the result is still available
    /// (see https://github.com/knowsys/stage2/issues/137)
    pub fn get_smallest_scans(&mut self) -> &Vec<bool> {
        unsafe { &mut *self.0.get() }.get_smallest_scans()
    }

    /// Forward `get_smallest_scans` to the underlying [`ColumnScanEnum`].
    pub fn set_active_scans(&mut self, active_scans: Vec<usize>) {
        self.0.get_mut().set_active_scans(active_scans);
    }

    /// Forward `minus_enable``to the underlying [`ColumnScanEnum`]
    pub fn minus_enable(&mut self, enabled: bool) {
        unsafe { &mut *self.0.get() }.minus_enable(enabled);
    }
}

impl<'a, S, T> From<S> for ColumnScanCell<'a, T>
where
    S: Into<ColumnScanEnum<'a, T>>,
    T: 'a + ColumnDataType,
{
    fn from(cs: S) -> Self {
        Self(UnsafeCell::new(cs.into()))
    }
}

/// Enum for [`ColumnScan`] for underlying data type
#[derive(Debug)]
pub enum ColumnScanT<'a> {
    /// Case u32
    U32(ColumnScanCell<'a, u32>),
    /// Case u64
    U64(ColumnScanCell<'a, u64>),
    /// Case Float
    Float(ColumnScanCell<'a, Float>),
    /// Case Double
    Double(ColumnScanCell<'a, Double>),
}

// Generate a macro forward_to_columnscan_cell!, which takes a [`ColumnScanT`] and a function as arguments
// and unfolds into a `match` statement that calls the datatype specific variant that function.
// See `physical/util.rs` for a more detailed description of this macro.
generate_datatype_forwarder!(forward_to_columnscan_cell);

impl<'a> ColumnScanT<'a> {
    /// Return all positions in the underlying column the cursor is currently at
    pub fn pos_multiple(&self) -> Option<Vec<usize>> {
        forward_to_columnscan_cell!(self, pos_multiple)
    }

    /// Assumes that column scan is a [`ColumnScanReorder`]
    /// Set iterator to a set of possibly disjoint ranged
    pub fn narrow_ranges(&mut self, intervals: Vec<Range<usize>>) {
        forward_to_columnscan_cell!(self, narrow_ranges(intervals))
    }

    /// Assumes that column scan is a [`ColumnScanFollow`]
    /// Returns whether its scans point to the same value
    pub fn is_equal(&self) -> bool {
        forward_to_columnscan_cell!(self, is_equal)
    }

    /// Assumes that column scan is a [`ColumnScanUnion`]
    /// and returns a vector containing the positions of the scans with the smallest values
    pub fn get_smallest_scans(&mut self) -> &Vec<bool> {
        forward_to_columnscan_cell!(self, get_smallest_scans)
    }

    /// Assumes that column scan is a [`ColumnScanUnion`]
    /// Set a vector that indicates which scans are currently active and should be considered
    pub fn set_active_scans(&mut self, active_scans: Vec<usize>) {
        forward_to_columnscan_cell!(self, set_active_scans(active_scans))
    }

    /// Assumes that column scan is a [`ColumnScanMinus`]
    /// Set a vector that indicates which scans are currently active and should be considered
    pub fn minus_enable(&mut self, enabled: bool) {
        forward_to_columnscan_cell!(self, minus_enable(enabled))
    }
}

impl<'a> Iterator for ColumnScanT<'a> {
    type Item = StorageValueT;

    fn next(&mut self) -> Option<Self::Item> {
        forward_to_columnscan_cell!(self, next.map_to(DataValueT))
    }
}

impl<'a> ColumnScan for ColumnScanT<'a> {
    fn seek(&mut self, value: Self::Item) -> Option<Self::Item> {
        match self {
            Self::U32(cs) => match value {
                Self::Item::U32(val) => cs.seek(val).map(StorageValueT::U32),
                _ => None,
            },
            Self::U64(cs) => match value {
                Self::Item::U64(val) => cs.seek(val).map(StorageValueT::U64),
                _ => None,
            },
            Self::Float(cs) => match value {
                Self::Item::Float(val) => cs.seek(val).map(StorageValueT::Float),
                _ => None,
            },
            Self::Double(cs) => match value {
                Self::Item::Double(val) => cs.seek(val).map(StorageValueT::Double),
                _ => None,
            },
        }
    }

    fn current(&self) -> Option<Self::Item> {
        forward_to_columnscan_cell!(self, current.map_to(DataValueT))
    }

    fn reset(&mut self) {
        forward_to_columnscan_cell!(self, reset)
    }

    fn pos(&self) -> Option<usize> {
        forward_to_columnscan_cell!(self, pos)
    }

    fn narrow(&mut self, interval: Range<usize>) {
        forward_to_columnscan_cell!(self, narrow(interval))
    }
}
