use super::super::column_types::{rle::ColumnScanRle, vector::ColumnScanVector};
use super::super::operations::{
    ColumnScanEqualColumn, ColumnScanEqualValue, ColumnScanFollow, ColumnScanJoin, ColumnScanMinus,
    ColumnScanPass, ColumnScanReorder, ColumnScanUnion, ColumnScanWithTrieLookahead,
};
use crate::{
    generate_datatype_forwarder, generate_forwarder,
    physical::datatypes::{ColumnDataType, DataValueT, Double, Float},
};
use std::{cell::UnsafeCell, fmt::Debug, ops::Range, rc::Rc};

/// Iterator for a sorted interval of values
pub trait ColumnScan: Debug + Iterator {
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
    /// Resets the iterator just before the start of the interval.
    fn narrow(&mut self, interval: Range<usize>);
}

/// Enum for ranged column scans for all the supported types
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
    /// Case ColumnScanWithTrieLookahead
    ColumnScanWithTrieLookahead(ColumnScanWithTrieLookahead<'a, T>),
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
    /// Return all positions in the underlying column the cursor is currently at
    pub fn pos_multiple(&self) -> Option<Vec<usize>> {
        if let Self::ColumnScanReorder(cs) = self {
            cs.pos_multiple()
        } else {
            unimplemented!("pos_multiple is only available for ColumnScanReorder")
        }
    }

    /// Set iterator to a set of possibly disjoint ranged
    pub fn narrow_ranges(&mut self, intervals: Vec<Range<usize>>) {
        if let Self::ColumnScanReorder(cs) = self {
            cs.narrow_ranges(intervals)
        } else {
            unimplemented!("narrow_ranges is only available for ColumnScanReorder")
        }
    }

    /// For ColumnScanFollow, returns whether its scans point to the same value
    pub fn is_equal(&self) -> bool {
        if let Self::ColumnScanFollow(cs) = self {
            cs.is_equal()
        } else {
            unimplemented!("is_equal is only available for ColumnScanFollow")
        }
    }

    /// Assumes that column scan is a union scan
    /// and returns a vector containing the positions of the scans with the smallest values
    pub fn get_smallest_scans(&self) -> &Vec<bool> {
        match self {
            Self::ColumnScanUnion(cs) => cs.get_smallest_scans(),
            _ => {
                unimplemented!("get_smallest_scans is only available for union_scans")
            }
        }
    }

    /// Assumes that column scan is a union scan
    /// Set a vector that indicates which scans are currently active and should be considered
    pub fn set_active_scans(
        &mut self,
        active_scans: Vec<usize>,
        scans_for_replacement: Vec<ColumnScanRc<'a, T>>,
    ) {
        match self {
            Self::ColumnScanUnion(cs) => cs.set_active_scans(active_scans, scans_for_replacement),
            _ => {
                unimplemented!("set_active_scans is only available for union_scans")
            }
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
    ColumnScanJoin,
    ColumnScanReorder, 
    ColumnScanEqualColumn, 
    ColumnScanEqualValue,
    ColumnScanPass,
    ColumnScanFollow,
    ColumnScanMinus,
    ColumnScanUnion,
    ColumnScanWithTrieLookahead);

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

    fn current(&mut self) -> Option<Self::Item> {
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
    pub fn narrow_ranges(&self, intervals: Vec<Range<usize>>) {
        unsafe { &mut *self.0.get() }.narrow_ranges(intervals)
    }

    /// Forward `is_equal` to the underlying [`ColumnScanEnum`].
    pub fn is_equal(&self) -> bool {
        unsafe { &mut *self.0.get() }.is_equal()
    }

    /// Forward `get_smallest_scans` to the underlying [`ColumnScanEnum`].
    pub fn get_smallest_scans(&self) -> &Vec<bool> {
        unsafe { &mut *self.0.get() }.get_smallest_scans()
    }

    /// Forward `get_smallest_scans` to the underlying [`ColumnScanEnum`].
    pub fn set_active_scans(
        &self,
        active_scans: Vec<usize>,
        scans_for_replacement: Vec<ColumnScanRc<'a, T>>,
    ) {
        unsafe { &mut *self.0.get() }.set_active_scans(active_scans, scans_for_replacement);
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

/// A wrapper around a Rc type holding a `ColumnScanCell`.
#[derive(Debug)]
pub struct ColumnScanRc<'a, T>(Rc<ColumnScanCell<'a, T>>)
where
    T: 'a + ColumnDataType;

impl<'a, T> ColumnScanRc<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Construct a new `ColumnScanRc` from the given [`ColumnScanCell`].
    pub fn new(cs: ColumnScanCell<'a, T>) -> Self {
        Self(Rc::new(cs))
    }

    /// Forward `next` to the underlying [`ColumnScanCell`].
    pub fn next(&self) -> Option<<ColumnScanEnum<'a, T> as Iterator>::Item> {
        self.0.next()
    }

    /// Forward `seek` to the underlying [`ColumnScanCell`].
    pub fn seek(
        &self,
        value: <ColumnScanEnum<'a, T> as Iterator>::Item,
    ) -> Option<<ColumnScanEnum<'a, T> as Iterator>::Item> {
        self.0.seek(value)
    }

    /// Forward `current` to the underlying [`ColumnScanCell`].
    pub fn current(&self) -> Option<<ColumnScanEnum<'a, T> as Iterator>::Item> {
        self.0.current()
    }

    /// Forward `reset` to the underlying [`ColumnScanCell`].
    pub fn reset(&self) {
        self.0.reset()
    }

    /// Forward `pos` to the underlying [`ColumnScanCell`].
    pub fn pos(&self) -> Option<usize> {
        self.0.pos()
    }

    /// Forward `narrow` to the underlying [`ColumnScanCell`].
    pub fn narrow(&self, interval: Range<usize>) {
        self.0.narrow(interval)
    }

    /// Forward `pos_multiple` to the underlying [`ColumnScanCell`].
    pub fn pos_multiple(&self) -> Option<Vec<usize>> {
        self.0.pos_multiple()
    }

    /// Forward `narrow_ranges` to the underlying [`ColumnScanCell`].
    pub fn narrow_ranges(&self, intervals: Vec<Range<usize>>) {
        self.0.narrow_ranges(intervals)
    }

    /// Forward `is_equal` to the underlying [`ColumnScanCell`].
    pub fn is_equal(&self) -> bool {
        self.0.is_equal()
    }

    /// Forward `get_smallest_scans` to the underlying [`ColumnScanCell`].
    pub fn get_smallest_scans(&self) -> &Vec<bool> {
        self.0.get_smallest_scans()
    }

    /// Forward `set_active_scans` to the underlying [`ColumnScanCell`].
    pub fn set_active_scans(
        &self,
        active_scans: Vec<usize>,
        scans_for_replacement: Vec<ColumnScanRc<'a, T>>,
    ) {
        self.0.set_active_scans(active_scans, scans_for_replacement);
    }
}

impl<'a, T> Clone for ColumnScanRc<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<'a, S, T> From<S> for ColumnScanRc<'a, T>
where
    S: Into<ColumnScanCell<'a, T>>,
    T: 'a + ColumnDataType,
{
    fn from(cs: S) -> Self {
        Self(Rc::new(cs.into()))
    }
}

/// Enum for [`ColumnScan`] for underlying data type
#[derive(Debug)]
pub enum ColumnScanT<'a> {
    /// Case u64
    U64(ColumnScanRc<'a, u64>),
    /// Case Float
    Float(ColumnScanRc<'a, Float>),
    /// Case Double
    Double(ColumnScanRc<'a, Double>),
}

// Generate a macro forward_to_columnscan_rc!, which takes a [`ColumnScanT`] and a function as arguments
// and unfolds into a `match` statement that calls the datatype specific variant that function.
// See `physical/util.rs` for a more detailed description of this macro.
generate_datatype_forwarder!(forward_to_columnscan_rc);

impl<'a> ColumnScanT<'a> {
    /// Return all positions in the underlying column the cursor is currently at
    pub fn pos_multiple(&self) -> Option<Vec<usize>> {
        forward_to_columnscan_rc!(self, pos_multiple)
    }

    /// Set iterator to a set of possibly disjoint ranged
    pub fn narrow_ranges(&self, intervals: Vec<Range<usize>>) {
        forward_to_columnscan_rc!(self, narrow_ranges(intervals))
    }

    /// For ColumnScanFollow, returns whether its scans point to the same value
    pub fn is_equal(&self) -> bool {
        forward_to_columnscan_rc!(self, is_equal)
    }

    /// Assumes that column scan is a union scan
    /// and returns a vector containing the positions of the scans with the smallest values
    pub fn get_smallest_scans(&self) -> &Vec<bool> {
        forward_to_columnscan_rc!(self, get_smallest_scans)
    }

    /// Assumes that column scan is a union scan
    /// Set a vector that indicates which scans are currently active and should be considered
    pub fn set_active_scans(
        &self,
        active_scans: Vec<usize>,
        scans_for_replacement: Vec<ColumnScanT<'a>>,
    ) {
        macro_rules! forward_set_active_scans {
            ($variant:ident, $type:ty, $cs:ident) => {{
                let mut scans: Vec<ColumnScanRc<$type>> = vec![];

                for scan in scans_for_replacement {
                    if let ColumnScanT::$variant(scan_rc) = scan {
                        scans.push(scan_rc);
                    } else {
                        panic!("Expected a column scan of type {}", stringify!($type));
                    }
                }

                $cs.set_active_scans(active_scans, scans);
            }};
        }

        match self {
            Self::U64(cs) => forward_set_active_scans!(U64, u64, cs),
            Self::Float(cs) => forward_set_active_scans!(Float, Float, cs),
            Self::Double(cs) => forward_set_active_scans!(Double, Double, cs),
        }
    }
}

impl<'a> Iterator for ColumnScanT<'a> {
    type Item = DataValueT;

    fn next(&mut self) -> Option<Self::Item> {
        forward_to_columnscan_rc!(self, next.map_to(DataValueT))
    }
}

impl<'a> ColumnScan for ColumnScanT<'a> {
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
        forward_to_columnscan_rc!(self, current.map_to(DataValueT))
    }

    fn reset(&mut self) {
        forward_to_columnscan_rc!(self, reset)
    }

    fn pos(&self) -> Option<usize> {
        forward_to_columnscan_rc!(self, pos)
    }

    fn narrow(&mut self, interval: Range<usize>) {
        forward_to_columnscan_rc!(self, narrow(interval))
    }
}
