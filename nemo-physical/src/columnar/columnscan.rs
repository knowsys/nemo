//! This module defines the trait [ColumnScan] as well as [ColumnScanEnum],
//! which collects all implemenations of that trait
//! into a single object.

use std::{cell::UnsafeCell, fmt::Debug, ops::Range};

use bitvec::vec::BitVec;

use crate::{
    datatypes::{ColumnDataType, Double, Float, StorageTypeName, StorageValueT},
    generate_datatype_forwarder, generate_forwarder,
};

use super::{
    column::{rle::ColumnScanRle, vector::ColumnScanVector},
    operations::{
        ColumnScanArithmetic, ColumnScanCastEnum, ColumnScanConstant, ColumnScanCopy,
        ColumnScanEqualColumn, ColumnScanFollow, ColumnScanJoin, ColumnScanMinus, ColumnScanNulls,
        ColumnScanPass, ColumnScanPrune, ColumnScanReorder, ColumnScanRestrictValues,
        ColumnScanSubtract, ColumnScanUnion,
    },
};

/// Iterator for a sorted interval of values
pub trait ColumnScan: Debug + Iterator {
    /// Starting from the current position, find a value that is at least as large as the given value,
    /// advance the iterator to this position, and return the value if it exists.
    /// If the current value is already larger or equal to the given value,
    /// this value will be returned and the `ColumnScan` will not be advanced.
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
    /// Case ColumnScanRestrictValues
    ColumnScanRestrictValues(ColumnScanRestrictValues<'a, T>),
    /// Case ColumnScanJoin
    ColumnScanPass(ColumnScanPass<'a, T>),
    /// Case ColumnScanPrune
    ColumnScanPrune(ColumnScanPrune<'a, T>),
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
    /// Case ColumnScanSubtract
    ColumnScanSubtract(ColumnScanSubtract<'a, T>),
    /// Case ColumnScanArithmetic
    ColumnScanArithmetic(ColumnScanArithmetic<'a, T>),
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

impl<'a, T> From<ColumnScanRestrictValues<'a, T>> for ColumnScanEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ColumnScanRestrictValues<'a, T>) -> Self {
        Self::ColumnScanRestrictValues(cs)
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
            unimplemented!("narrow_ranges is only available for ColumnScanReorder")
        }
    }

    /// Assumes that column scan is a [`ColumnScanUnion`]
    /// and returns a vector containing the positions of the scans with the smallest values
    pub fn union_get_smallest(&self) -> BitVec {
        if let Self::ColumnScanUnion(cs) = self {
            cs.get_smallest_scans()
        } else {
            unimplemented!("union_get_smallest is only available for ColumnScanUnion")
        }
    }

    /// Assumes that column scan is a [`ColumnScanUnion`]
    /// Set a vector that indicates which scans are currently active and should be considered
    pub fn union_set_active(&mut self, active_scans: BitVec) {
        if let Self::ColumnScanUnion(cs) = self {
            cs.set_active_scans(active_scans)
        } else {
            unimplemented!("union_set_active is only available for ColumnScanUnion")
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

    /// Assumes that column scan is a [`ColumnScanSubtract`]
    /// Return a vector indicating which subiterators point to the same value as the main one.
    pub fn subtract_get_equal(&self) -> BitVec {
        if let Self::ColumnScanSubtract(cs) = self {
            cs.get_equal_values()
        } else {
            unimplemented!("subtract_get_equal is only available for ColumnScanSubtract")
        }
    }

    /// Assumes that column scan is a [`ColumnScanSubtract`]
    /// Set which sub iterators should be active.
    pub fn subtract_set_active(&mut self, active_scans: BitVec) {
        if let Self::ColumnScanSubtract(cs) = self {
            cs.set_active_scans(active_scans)
        } else {
            unimplemented!("subtract_set_active is only available for ColumnScanSubtract")
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
    ColumnScanRestrictValues,
    ColumnScanPass,
    ColumnScanPrune,
    ColumnScanFollow,
    ColumnScanMinus,
    ColumnScanUnion,
    ColumnScanConstant,
    ColumnScanCopy,
    ColumnScanNulls,
    ColumnScanSubtract,
    ColumnScanArithmetic
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
pub struct ColumnScanCell<'a, T>(pub UnsafeCell<ColumnScanEnum<'a, T>>)
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

    /// Forward `union_get_smallest` to the underlying [`ColumnScanEnum`].
    /// This takes an exclusive reference as opposed to an immutable one, so that none of the
    /// mutating methods on &self can be called while the result is still available
    /// (see <https://github.com/knowsys/nemo/issues/137>)
    pub fn union_get_smallest(&mut self) -> BitVec {
        unsafe { &mut *self.0.get() }.union_get_smallest()
    }

    /// Forward `union_get_smallest` to the underlying [`ColumnScanEnum`].
    pub fn union_set_active(&mut self, active_scans: BitVec) {
        self.0.get_mut().union_set_active(active_scans);
    }

    /// Forward `minus_enable` to the underlying [`ColumnScanEnum`].
    pub fn minus_enable(&mut self, enabled: bool) {
        self.0.get_mut().minus_enable(enabled);
    }

    /// Forward `subtract_get_equal` to the underlying [`ColumnScanEnum`].
    pub fn subtract_get_equal(&mut self) -> BitVec {
        self.0.get_mut().subtract_get_equal()
    }

    /// Forward `subtract_set_active` to the underlying [`ColumnScanEnum`].
    pub fn subtract_set_active(&mut self, active_scans: BitVec) {
        unsafe { &mut *self.0.get() }.subtract_set_active(active_scans)
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
    Id32(ColumnScanCell<'a, u32>),
    /// Case u64
    Id64(ColumnScanCell<'a, u64>),
    /// Case i64
    Int64(ColumnScanCell<'a, i64>),
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
    pub fn union_get_smallest(&mut self) -> &Vec<bool> {
        forward_to_columnscan_cell!(self, union_get_smallest)
    }

    /// Assumes that column scan is a [`ColumnScanUnion`]
    /// Set a vector that indicates which scans are currently active and should be considered
    pub fn union_set_active(&mut self, active_scans: BitVec) {
        forward_to_columnscan_cell!(self, union_set_active(active_scans))
    }

    /// Assumes that column scan is a [`ColumnScanMinus`]
    /// Set a vector that indicates which scans are currently active and should be considered
    pub fn minus_enable(&mut self, enabled: bool) {
        forward_to_columnscan_cell!(self, minus_enable(enabled))
    }

    /// Assumes that column scan is a [`ColumnScanSubtract`]
    /// Return a vector indicating which subiterators point to the same value as the main one.
    pub fn subtract_get_equal(&mut self) -> BitVec {
        forward_to_columnscan_cell!(self, subtract_get_equal)
    }

    /// Assumes that column scan is a [`ColumnScanSubtract`]
    /// Set which sub iterators should be enabled.
    pub fn subtract_set_active(&mut self, active_scans: BitVec) {
        forward_to_columnscan_cell!(self, subtract_set_active(active_scans))
    }
}

impl<'a> Iterator for ColumnScanT<'a> {
    type Item = StorageValueT;

    fn next(&mut self) -> Option<Self::Item> {
        forward_to_columnscan_cell!(self, next.map_to(StorageValueT))
    }
}

impl<'a> ColumnScan for ColumnScanT<'a> {
    fn seek(&mut self, value: Self::Item) -> Option<Self::Item> {
        match self {
            Self::Id32(cs) => match value {
                Self::Item::Id32(val) => cs.seek(val).map(StorageValueT::Id32),
                _ => None,
            },
            Self::Id64(cs) => match value {
                Self::Item::Id64(val) => cs.seek(val).map(StorageValueT::Id64),
                _ => None,
            },
            Self::Int64(cs) => match value {
                Self::Item::Int64(val) => cs.seek(val).map(StorageValueT::Int64),
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
        forward_to_columnscan_cell!(self, current.map_to(StorageValueT))
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

// Replaces [ColumnScanT]
#[derive(Debug)]
pub struct ColumnScanRainbow<'a> {
    /// Case [StorageTypeName::Id32][super::super::datatypes::storage_type_name::StorageTypeName]
    pub scan_id32: ColumnScanCell<'a, u32>,
    /// Case [StorageTypeName::Id64][super::super::datatypes::storage_type_name::StorageTypeName]
    pub scan_id64: ColumnScanCell<'a, u64>,
    /// Case [StorageTypeName::Int64][super::super::datatypes::storage_type_name::StorageTypeName]
    pub scan_i64: ColumnScanCell<'a, i64>,
    /// Case [StorageTypeName::Float][super::super::datatypes::storage_type_name::StorageTypeName]
    pub scan_float: ColumnScanCell<'a, Float>,
    /// Case [StorageTypeName::Double][super::super::datatypes::storage_type_name::StorageTypeName]
    pub scan_double: ColumnScanCell<'a, Double>,
}

impl<'a> ColumnScanRainbow<'a> {
    /// Create a new [ColumnScanRainbow].
    pub fn new(
        scan_id32: ColumnScanEnum<'a, u32>,
        scan_id64: ColumnScanEnum<'a, u64>,
        scan_i64: ColumnScanEnum<'a, i64>,
        scan_float: ColumnScanEnum<'a, Float>,
        scan_double: ColumnScanEnum<'a, Double>,
    ) -> ColumnScanRainbow<'a> {
        Self {
            scan_id32: ColumnScanCell::new(scan_id32),
            scan_id64: ColumnScanCell::new(scan_id64),
            scan_i64: ColumnScanCell::new(scan_i64),
            scan_float: ColumnScanCell::new(scan_float),
            scan_double: ColumnScanCell::new(scan_double),
        }
    }

    /// Return the current position of a scan of the given [StorageTypeName].
    pub fn pos(&mut self, storage_type: StorageTypeName) -> Option<usize> {
        match storage_type {
            StorageTypeName::Id32 => self.scan_id32.pos(),
            StorageTypeName::Id64 => self.scan_id64.pos(),
            StorageTypeName::Int64 => self.scan_i64.pos(),
            StorageTypeName::Float => self.scan_float.pos(),
            StorageTypeName::Double => self.scan_double.pos(),
        }
    }

    /// Return a scan of the given [StorageTypeName] to its initial state.
    pub fn reset(&mut self, storage_type: StorageTypeName) {
        match storage_type {
            StorageTypeName::Id32 => self.scan_id32.reset(),
            StorageTypeName::Id64 => self.scan_id64.reset(),
            StorageTypeName::Int64 => self.scan_i64.reset(),
            StorageTypeName::Float => self.scan_float.reset(),
            StorageTypeName::Double => self.scan_double.reset(),
        }
    }

    /// Restricts the iterator of the given [StorageTypeName] to the given `interval`.
    /// Resets the iterator just before the start of the interval.
    pub fn narrow(&mut self, storage_type: StorageTypeName, interval: Range<usize>) {
        match storage_type {
            StorageTypeName::Id32 => self.scan_id32.narrow(interval),
            StorageTypeName::Id64 => self.scan_id64.narrow(interval),
            StorageTypeName::Int64 => self.scan_i64.narrow(interval),
            StorageTypeName::Float => self.scan_float.narrow(interval),
            StorageTypeName::Double => self.scan_double.narrow(interval),
        }
    }

    /// Assumes that column scan is a [`ColumnScanUnion`]
    /// and returns a vector containing the positions of the scans with the smallest values
    pub fn union_get_smallest(&mut self, storage_type: StorageTypeName) -> BitVec {
        match storage_type {
            StorageTypeName::Id32 => self.scan_id32.union_get_smallest(),
            StorageTypeName::Id64 => self.scan_id64.union_get_smallest(),
            StorageTypeName::Int64 => self.scan_i64.union_get_smallest(),
            StorageTypeName::Float => self.scan_float.union_get_smallest(),
            StorageTypeName::Double => self.scan_double.union_get_smallest(),
        }
    }

    /// Assumes that column scan is a [`ColumnScanUnion`]
    /// Set a vector that indicates which scans are currently active and should be considered
    pub fn union_set_active(&mut self, storage_type: StorageTypeName, active_scans: BitVec) {
        match storage_type {
            StorageTypeName::Id32 => self.scan_id32.union_set_active(active_scans),
            StorageTypeName::Id64 => self.scan_id64.union_set_active(active_scans),
            StorageTypeName::Int64 => self.scan_i64.union_set_active(active_scans),
            StorageTypeName::Float => self.scan_float.union_set_active(active_scans),
            StorageTypeName::Double => self.scan_double.union_set_active(active_scans),
        }
    }

    /// Assumes that column scan is a [`ColumnScanSubtract`]
    /// Return a vector indicating which subiterators point to the same value as the main one.
    pub fn subtract_get_equal(&mut self, storage_type: StorageTypeName) -> BitVec {
        match storage_type {
            StorageTypeName::Id32 => self.scan_id32.subtract_get_equal(),
            StorageTypeName::Id64 => self.scan_id64.subtract_get_equal(),
            StorageTypeName::Int64 => self.scan_i64.subtract_get_equal(),
            StorageTypeName::Float => self.scan_float.subtract_get_equal(),
            StorageTypeName::Double => self.scan_double.subtract_get_equal(),
        }
    }

    /// Assumes that column scan is a [`ColumnScanSubtract`]
    /// Set which sub iterators should be enabled.
    pub fn subtract_set_active(&mut self, storage_type: StorageTypeName, active_scans: BitVec) {
        match storage_type {
            StorageTypeName::Id32 => self.scan_id32.subtract_set_active(active_scans),
            StorageTypeName::Id64 => self.scan_id64.subtract_set_active(active_scans),
            StorageTypeName::Int64 => self.scan_i64.subtract_set_active(active_scans),
            StorageTypeName::Float => self.scan_float.subtract_set_active(active_scans),
            StorageTypeName::Double => self.scan_double.subtract_set_active(active_scans),
        }
    }
}
