use super::{
    ColScanEqualColumn, ColScanEqualValue, ColScanFollow, ColScanGenericEnum, ColScanJoin,
    ColScanMinus, ColScanPass, ColScanReorder, ColScanUnion,
};
use crate::{
    generate_datatype_forwarder, generate_forwarder,
    physical::{
        columns::columns::ColumnRleScan,
        datatypes::{ColumnDataType, DataValueT, Double, Float},
    },
};
use std::{cell::UnsafeCell, fmt::Debug, ops::Range};

/// Iterator for a sorted interval of values
pub trait ColScan: Debug + Iterator {
    /// Find the next value that is at least as large as the given value,
    /// advance the iterator to this position, and return the value if it exists.
    fn seek(&mut self, value: Self::Item) -> Option<Self::Item>;

    /// Return the value at the current position, if any.
    fn current(&mut self) -> Option<Self::Item>;

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
    fn narrow(&mut self, interval: Range<usize>);
}

/// Enum for ranged column scans for all the supported types
#[derive(Debug)]
pub enum ColScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Case ColScanGeneric
    ColScanGeneric(ColScanGenericEnum<'a, T>),
    /// Case ColumnRleScan
    ColumnRleScan(ColumnRleScan<'a, T>),
    /// Case ColScanJoin
    ColScanJoin(ColScanJoin<'a, T>),
    /// Case ColScanReorder
    ColScanReorder(ColScanReorder<'a, T>),
    /// Case ColScanEqualColumn
    ColScanEqualColumn(ColScanEqualColumn<'a, T>),
    /// Case ColScanEqualValue
    ColScanEqualValue(ColScanEqualValue<'a, T>),
    /// Case ColScanJoin
    ColScanPass(ColScanPass<'a, T>),
    /// Case ColScanFollow
    ColScanFollow(ColScanFollow<'a, T>),
    /// Case ColScanMinus
    ColScanMinus(ColScanMinus<'a, T>),
    /// Case ColScanUnion
    ColScanUnion(ColScanUnion<'a, T>),
}

/// The following impl statements allow converting from a specific [`ColScan`] into a gerneral [`ColScanEnum`]

impl<'a, T> From<ColScanGenericEnum<'a, T>> for ColScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ColScanGenericEnum<'a, T>) -> Self {
        Self::ColScanGeneric(cs)
    }
}

impl<'a, T> From<ColumnRleScan<'a, T>> for ColScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ColumnRleScan<'a, T>) -> Self {
        Self::ColumnRleScan(cs)
    }
}

impl<'a, T> From<ColScanJoin<'a, T>> for ColScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ColScanJoin<'a, T>) -> Self {
        Self::ColScanJoin(cs)
    }
}

impl<'a, T> From<ColScanReorder<'a, T>> for ColScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ColScanReorder<'a, T>) -> Self {
        Self::ColScanReorder(cs)
    }
}

impl<'a, T> From<ColScanEqualColumn<'a, T>> for ColScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ColScanEqualColumn<'a, T>) -> Self {
        Self::ColScanEqualColumn(cs)
    }
}

impl<'a, T> From<ColScanEqualValue<'a, T>> for ColScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ColScanEqualValue<'a, T>) -> Self {
        Self::ColScanEqualValue(cs)
    }
}

impl<'a, T> From<ColScanPass<'a, T>> for ColScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ColScanPass<'a, T>) -> Self {
        Self::ColScanPass(cs)
    }
}

impl<'a, T> From<ColScanMinus<'a, T>> for ColScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ColScanMinus<'a, T>) -> Self {
        Self::ColScanMinus(cs)
    }
}

impl<'a, T> From<ColScanFollow<'a, T>> for ColScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ColScanFollow<'a, T>) -> Self {
        Self::ColScanFollow(cs)
    }
}

impl<'a, T> From<ColScanUnion<'a, T>> for ColScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ColScanUnion<'a, T>) -> Self {
        Self::ColScanUnion(cs)
    }
}

/// The following block makes functions which are specific to one variant of a [`ColScanEnum`]
/// available on the whole [`ColScanEnum`]
impl<'a, T> ColScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Return all positions in the underlying column the cursor is currently at
    pub fn pos_multiple(&self) -> Option<Vec<usize>> {
        if let Self::ColScanReorder(cs) = self {
            cs.pos_multiple()
        } else {
            unimplemented!("pos_multiple is only available for ColScanReorder")
        }
    }

    /// Set iterator to a set of possibly disjoint ranged
    pub fn narrow_ranges(&mut self, intervals: Vec<Range<usize>>) {
        if let Self::ColScanReorder(cs) = self {
            cs.narrow_ranges(intervals)
        } else {
            unimplemented!("narrow_ranges is only available for ColScanReorder")
        }
    }

    /// For ColScanFollow, returns whether its scans point to the same value
    pub fn is_equal(&self) -> bool {
        if let Self::ColScanFollow(cs) = self {
            cs.is_equal()
        } else {
            unimplemented!("is_equal is only available for ColScanFollow")
        }
    }

    /// Assumes that column scan is a union scan
    /// and returns a vector containing the positions of the scans with the smallest values
    pub fn get_smallest_scans(&self) -> &Vec<bool> {
        match self {
            Self::ColScanUnion(cs) => cs.get_smallest_scans(),
            _ => {
                unimplemented!("get_smallest_scans is only available for union_scans")
            }
        }
    }

    /// Assumes that column scan is a union scan
    /// Set a vector that indicates which scans are currently active and should be considered
    pub fn set_active_scans(&mut self, active_scans: Vec<usize>) {
        match self {
            Self::ColScanUnion(cs) => cs.set_active_scans(active_scans),
            _ => {
                unimplemented!("set_active_scans is only available for union_scans")
            }
        }
    }
}

// Generate a macro forward_to_colscan!, which takes a [`ColumnScanEnum`] and a function as arguments
// and unfolds into a `match` statement that calls the variant specific version of that function.
// Each new variant of a [`ColumnScanEnum`] must be added here.
// See `physical/util.rs` for a more detailed description of this macro.
generate_forwarder!(forward_to_colscan;
    ColScanGeneric,
    ColumnRleScan,
    ColScanJoin,
    ColScanReorder, 
    ColScanEqualColumn, 
    ColScanEqualValue,
    ColScanPass,
    ColScanFollow,
    ColScanMinus,
    ColScanUnion);

impl<'a, T> Iterator for ColScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        forward_to_colscan!(self, next)
    }
}

impl<'a, T> ColScan for ColScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn seek(&mut self, value: Self::Item) -> Option<Self::Item> {
        forward_to_colscan!(self, seek(value))
    }

    fn current(&mut self) -> Option<Self::Item> {
        forward_to_colscan!(self, current)
    }

    fn reset(&mut self) {
        forward_to_colscan!(self, reset)
    }

    fn pos(&self) -> Option<usize> {
        forward_to_colscan!(self, pos)
    }

    fn narrow(&mut self, interval: Range<usize>) {
        forward_to_colscan!(self, narrow(interval))
    }
}

/// A wrapper around a cell type holding a `ColScanEnum`.
#[repr(transparent)]
pub struct ColScanCell<'a, T>(UnsafeCell<ColScanEnum<'a, T>>)
where
    T: 'a + ColumnDataType;

impl<T> Debug for ColScanCell<'_, T>
where
    T: ColumnDataType,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let rcs = unsafe { &*self.0.get() };
        f.debug_tuple("ColScanCell").field(rcs).finish()
    }
}

impl<'a, T> ColScanCell<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Construct a new `ColScanCell` from the given [`ColScanEnum`].
    pub fn new(cs: ColScanEnum<'a, T>) -> Self {
        Self(UnsafeCell::new(cs))
    }

    /// Forward `next` to the underlying [`ColScanEnum`].
    #[inline]
    pub fn next(&self) -> Option<<ColScanEnum<'a, T> as Iterator>::Item> {
        unsafe { &mut *self.0.get() }.next()
    }

    /// Forward `seek` to the underlying [`ColScanEnum`].
    #[inline]
    pub fn seek(
        &self,
        value: <ColScanEnum<'a, T> as Iterator>::Item,
    ) -> Option<<ColScanEnum<'a, T> as Iterator>::Item> {
        unsafe { &mut *self.0.get() }.seek(value)
    }

    /// Forward `current` to the underlying [`ColScanEnum`].
    #[inline]
    pub fn current(&self) -> Option<<ColScanEnum<'a, T> as Iterator>::Item> {
        unsafe { &mut *self.0.get() }.current()
    }

    /// Forward `reset` to the underlying [`ColScanEnum`].
    #[inline]
    pub fn reset(&self) {
        unsafe { &mut *self.0.get() }.reset()
    }

    /// Forward `pos` to the underlying [`ColScanEnum`].
    #[inline]
    pub fn pos(&self) -> Option<usize> {
        unsafe { &(*self.0.get()) }.pos()
    }

    /// Forward `narrow` to the underlying [`ColScanEnum`].
    #[inline]
    pub fn narrow(&self, interval: Range<usize>) {
        unsafe { &mut *self.0.get() }.narrow(interval)
    }

    /// Forward `pos_multiple` to the underlying [`ColScanEnum`].
    pub fn pos_multiple(&self) -> Option<Vec<usize>> {
        unsafe { &mut *self.0.get() }.pos_multiple()
    }

    /// Forward `narrow_ranges` to the underlying [`ColScanEnum`].
    pub fn narrow_ranges(&mut self, intervals: Vec<Range<usize>>) {
        unsafe { &mut *self.0.get() }.narrow_ranges(intervals)
    }

    /// Forward `is_equal` to the underlying [`ColScanEnum`].
    pub fn is_equal(&self) -> bool {
        unsafe { &mut *self.0.get() }.is_equal()
    }

    /// Forward `get_smallest_scans` to the underlying [`ColScanEnum`].
    pub fn get_smallest_scans(&self) -> &Vec<bool> {
        unsafe { &mut *self.0.get() }.get_smallest_scans()
    }

    /// Forward `get_smallest_scans` to the underlying [`ColScanEnum`].
    pub fn set_active_scans(&mut self, active_scans: Vec<usize>) {
        unsafe { &mut *self.0.get() }.set_active_scans(active_scans);
    }
}

impl<'a, S, T> From<S> for ColScanCell<'a, T>
where
    S: Into<ColScanEnum<'a, T>>,
    T: 'a + ColumnDataType,
{
    fn from(cs: S) -> Self {
        Self(UnsafeCell::new(cs.into()))
    }
}

/// Enum for [`ColScan`] for underlying data type
#[derive(Debug)]
pub enum ColScanT<'a> {
    /// Case u64
    U64(ColScanCell<'a, u64>),
    /// Case Float
    Float(ColScanCell<'a, Float>),
    /// Case Double
    Double(ColScanCell<'a, Double>),
}

// Generate a macro forward_to_colscan_cell!, which takes a [`ColScanT`] and a function as arguments
// and unfolds into a `match` statement that calls the datatype specific variant that function.
// See `physical/util.rs` for a more detailed description of this macro.
generate_datatype_forwarder!(forward_to_colscan_cell);

impl<'a> ColScanT<'a> {
    /// Return all positions in the underlying column the cursor is currently at
    pub fn pos_multiple(&self) -> Option<Vec<usize>> {
        forward_to_colscan_cell!(self, pos_multiple)
    }

    /// Set iterator to a set of possibly disjoint ranged
    pub fn narrow_ranges(&mut self, intervals: Vec<Range<usize>>) {
        forward_to_colscan_cell!(self, narrow_ranges(intervals))
    }

    /// For ColScanFollow, returns whether its scans point to the same value
    pub fn is_equal(&self) -> bool {
        forward_to_colscan_cell!(self, is_equal)
    }

    /// Assumes that column scan is a union scan
    /// and returns a vector containing the positions of the scans with the smallest values
    pub fn get_smallest_scans(&self) -> &Vec<bool> {
        forward_to_colscan_cell!(self, get_smallest_scans)
    }

    /// Assumes that column scan is a union scan
    /// Set a vector that indicates which scans are currently active and should be considered
    pub fn set_active_scans(&mut self, active_scans: Vec<usize>) {
        forward_to_colscan_cell!(self, set_active_scans(active_scans))
    }
}

impl<'a> Iterator for ColScanT<'a> {
    type Item = DataValueT;

    fn next(&mut self) -> Option<Self::Item> {
        forward_to_colscan_cell!(self, next.map_to(DataValueT))
    }
}

impl<'a> ColScan for ColScanT<'a> {
    fn seek(&mut self, value: Self::Item) -> Option<Self::Item> {
        match self {
            Self::U64(cs) => match value {
                Self::Item::U64(val) => cs.seek(val).map(DataValueT::U64),
                Self::Item::Float(_) | Self::Item::Double(_) => None,
            },
            Self::Float(cs) => match value {
                Self::Item::U64(_) | Self::Item::Double(_) => None,
                Self::Item::Float(val) => cs.seek(val).map(DataValueT::Float),
            },
            Self::Double(cs) => match value {
                Self::Item::U64(_) | Self::Item::Float(_) => None,
                Self::Item::Double(val) => cs.seek(val).map(DataValueT::Double),
            },
        }
    }

    fn current(&mut self) -> Option<Self::Item> {
        forward_to_colscan_cell!(self, current.map_to(DataValueT))
    }

    fn reset(&mut self) {
        forward_to_colscan_cell!(self, reset)
    }

    fn pos(&self) -> Option<usize> {
        forward_to_colscan_cell!(self, pos)
    }

    fn narrow(&mut self, interval: Range<usize>) {
        forward_to_colscan_cell!(self, narrow(interval))
    }
}
