//! This module defines the trait [ColumnScan] as well as [ColumnScanEnum],
//! which collects all implemenations of that trait
//! into a single object.

use std::{cell::UnsafeCell, fmt::Debug, ops::Range};

use crate::{
    generate_forwarder,
    storagevalues::{
        double::Double,
        float::Float,
        storagetype::StorageType,
        storagevalue::{StorageValue, StorageValueT},
    },
};

use super::{
    column::{rle::ColumnScanRle, vector::ColumnScanVector},
    operations::{
        constant::ColumnScanConstant, filter::ColumnScanFilter,
        filter_constant::ColumnScanFilterConstant, filter_equal::ColumnScanFilterEqual,
        filter_interval::ColumnScanFilterInterval, join::ColumnScanJoin, pass::ColumnScanPass,
        prune::ColumnScanPrune, subtract::ColumnScanSubtract, union::ColumnScanUnion,
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
    /// Typically, the state of a [ColumnScan] is determined by the state of its sub scans
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

/// Enum for [ColumnScan] of all supported types
#[derive(Debug)]
pub(crate) enum ColumnScanEnum<'a, T>
where
    T: 'a + StorageValue,
{
    /// Case ColumnScanVector
    Vector(ColumnScanVector<'a, T>),
    /// Case ColumnRleScan
    Rle(ColumnScanRle<'a, T>),
    /// Case ColumnScanConstant
    Constant(ColumnScanConstant<T>),
    /// Case ColumnScanFilter
    Filter(ColumnScanFilter<'a, T>),
    /// Case ColumnScanFilterConstant
    FilterConstant(ColumnScanFilterConstant<'a, T>),
    /// Case ColumnScanFilterEqual
    FilterEqual(ColumnScanFilterEqual<'a, T>),
    /// Case ColumnScanFilterInterval
    FilterInterval(ColumnScanFilterInterval<'a, T>),
    /// Case ColumnScanJoin
    Join(ColumnScanJoin<'a, T>),
    /// Case ColumnScanPass
    Pass(ColumnScanPass<'a, T>),
    /// Case ColumnScanPrune
    Prune(ColumnScanPrune<'a, T>),
    /// Case ColumnScanSubtract
    Subtract(ColumnScanSubtract<'a, T>),
    /// Case ColumnScanUnion
    Union(ColumnScanUnion<'a, T>),
}

// The following impl statements allow converting from a specific [ColumnScan] into a general [ColumnScanEnum]

impl<'a, T> From<ColumnScanVector<'a, T>> for ColumnScanEnum<'a, T>
where
    T: 'a + StorageValue,
{
    fn from(cs: ColumnScanVector<'a, T>) -> Self {
        Self::Vector(cs)
    }
}

impl<'a, T> From<ColumnScanRle<'a, T>> for ColumnScanEnum<'a, T>
where
    T: 'a + StorageValue,
{
    fn from(cs: ColumnScanRle<'a, T>) -> Self {
        Self::Rle(cs)
    }
}

impl<T> From<ColumnScanConstant<T>> for ColumnScanEnum<'_, T>
where
    T: StorageValue,
{
    fn from(cs: ColumnScanConstant<T>) -> Self {
        Self::Constant(cs)
    }
}

impl<'a, T> From<ColumnScanFilter<'a, T>> for ColumnScanEnum<'a, T>
where
    T: 'a + StorageValue,
{
    fn from(cs: ColumnScanFilter<'a, T>) -> Self {
        Self::Filter(cs)
    }
}

impl<'a, T> From<ColumnScanFilterConstant<'a, T>> for ColumnScanEnum<'a, T>
where
    T: 'a + StorageValue,
{
    fn from(cs: ColumnScanFilterConstant<'a, T>) -> Self {
        Self::FilterConstant(cs)
    }
}

impl<'a, T> From<ColumnScanFilterEqual<'a, T>> for ColumnScanEnum<'a, T>
where
    T: 'a + StorageValue,
{
    fn from(cs: ColumnScanFilterEqual<'a, T>) -> Self {
        Self::FilterEqual(cs)
    }
}

impl<'a, T> From<ColumnScanFilterInterval<'a, T>> for ColumnScanEnum<'a, T>
where
    T: 'a + StorageValue,
{
    fn from(cs: ColumnScanFilterInterval<'a, T>) -> Self {
        Self::FilterInterval(cs)
    }
}

impl<'a, T> From<ColumnScanJoin<'a, T>> for ColumnScanEnum<'a, T>
where
    T: 'a + StorageValue,
{
    fn from(cs: ColumnScanJoin<'a, T>) -> Self {
        Self::Join(cs)
    }
}

impl<'a, T> From<ColumnScanPass<'a, T>> for ColumnScanEnum<'a, T>
where
    T: 'a + StorageValue,
{
    fn from(cs: ColumnScanPass<'a, T>) -> Self {
        Self::Pass(cs)
    }
}

impl<'a, T> From<ColumnScanPrune<'a, T>> for ColumnScanEnum<'a, T>
where
    T: 'a + StorageValue,
{
    fn from(cs: ColumnScanPrune<'a, T>) -> Self {
        Self::Prune(cs)
    }
}

impl<'a, T> From<ColumnScanSubtract<'a, T>> for ColumnScanEnum<'a, T>
where
    T: 'a + StorageValue,
{
    fn from(cs: ColumnScanSubtract<'a, T>) -> Self {
        Self::Subtract(cs)
    }
}

impl<'a, T> From<ColumnScanUnion<'a, T>> for ColumnScanEnum<'a, T>
where
    T: 'a + StorageValue,
{
    fn from(cs: ColumnScanUnion<'a, T>) -> Self {
        Self::Union(cs)
    }
}

/// The following block makes functions which are specific to one variant of a [ColumnScanEnum]
/// available on the whole [ColumnScanEnum]
impl<'a, T> ColumnScanEnum<'a, T>
where
    T: 'a + StorageValue,
{
    /// Assumes that column scan is a [ColumnScanConstant].
    ///
    /// Set a new constant value that will be returned by this scan.
    pub(crate) fn constant_set(&mut self, constant: Option<T>) {
        if let Self::Constant(scan) = self {
            scan.set_constant(constant)
        } else {
            unimplemented!("subtract_set_active is only available for ColumnScanConstant")
        }
    }
}

// Generate a macro forward_to_columnscan!, which takes a [ColumnScanEnum] and a function as arguments
// and unfolds into a `match` statement that calls the variant specific version of that function.
// Each new variant of a [ColumnScanEnum] must be added here.
// See `physical/util.rs` for a more detailed description of this macro.
generate_forwarder!(forward_to_columnscan;
    Vector,
    Rle,
    Constant,
    Filter,
    FilterConstant,
    FilterEqual,
    FilterInterval,
    Join,
    Pass,
    Prune,
    Subtract,
    Union
);

impl<'a, T> Iterator for ColumnScanEnum<'a, T>
where
    T: 'a + StorageValue,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        forward_to_columnscan!(self, next)
    }
}

impl<'a, T> ColumnScan for ColumnScanEnum<'a, T>
where
    T: 'a + StorageValue,
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
pub(crate) struct ColumnScanCell<'a, T>(pub UnsafeCell<ColumnScanEnum<'a, T>>)
where
    T: 'a + StorageValue;

impl<T> Debug for ColumnScanCell<'_, T>
where
    T: StorageValue,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let rcs = unsafe { &*self.0.get() };
        f.debug_tuple("ColumnScanCell").field(rcs).finish()
    }
}

impl<'a, T> ColumnScanCell<'a, T>
where
    T: 'a + StorageValue,
{
    /// Construct a new `ColumnScanCell` from the given [ColumnScanEnum].
    pub(crate) fn new(cs: ColumnScanEnum<'a, T>) -> Self {
        Self(UnsafeCell::new(cs))
    }

    /// Forward `next` to the underlying [ColumnScanEnum].
    #[inline]
    pub(crate) fn next(&self) -> Option<<ColumnScanEnum<'a, T> as Iterator>::Item> {
        unsafe { &mut *self.0.get() }.next()
    }

    /// Forward `seek` to the underlying [ColumnScanEnum].
    #[inline]
    pub(crate) fn seek(
        &self,
        value: <ColumnScanEnum<'a, T> as Iterator>::Item,
    ) -> Option<<ColumnScanEnum<'a, T> as Iterator>::Item> {
        unsafe { &mut *self.0.get() }.seek(value)
    }

    /// Forward `current` to the underlying [ColumnScanEnum].
    #[inline]
    pub(crate) fn current(&self) -> Option<<ColumnScanEnum<'a, T> as Iterator>::Item> {
        unsafe { &mut *self.0.get() }.current()
    }

    /// Forward `reset` to the underlying [ColumnScanEnum].
    #[inline]
    pub(crate) fn reset(&self) {
        unsafe { &mut *self.0.get() }.reset()
    }

    /// Forward `pos` to the underlying [ColumnScanEnum].
    #[inline]
    pub(crate) fn pos(&self) -> Option<usize> {
        unsafe { &(*self.0.get()) }.pos()
    }

    /// Forward `narrow` to the underlying [ColumnScanEnum].
    #[inline]
    pub(crate) fn narrow(&self, interval: Range<usize>) {
        unsafe { &mut *self.0.get() }.narrow(interval)
    }

    /// Forward `constant_set` to the underlying [ColumnScanEnum].
    pub(crate) fn constant_set(&mut self, constant: Option<T>) {
        unsafe { &mut *self.0.get() }.constant_set(constant)
    }
}

impl<'a, S, T> From<S> for ColumnScanCell<'a, T>
where
    S: Into<ColumnScanEnum<'a, T>>,
    T: 'a + StorageValue,
{
    fn from(cs: S) -> Self {
        Self(UnsafeCell::new(cs.into()))
    }
}

/// Contains a [ColumnScan] for every used [super::super::storagevalues::storagetype_name::StorageType]
#[derive(Debug)]
pub(crate) struct ColumnScanT<'a> {
    /// Case [StorageType::Id32][super::super::storagevalues::storagetype_name::StorageType]
    pub scan_id32: ColumnScanCell<'a, u32>,
    /// Case [StorageType::Id64][super::super::storagevalues::storagetype_name::StorageType]
    pub scan_id64: ColumnScanCell<'a, u64>,
    /// Case [StorageType::Int64][super::super::storagevalues::storagetype_name::StorageType]
    pub scan_i64: ColumnScanCell<'a, i64>,
    /// Case [StorageType::Float][super::super::storagevalues::storagetype_name::StorageType]
    pub scan_float: ColumnScanCell<'a, Float>,
    /// Case [StorageType::Double][super::super::storagevalues::storagetype_name::StorageType]
    pub scan_double: ColumnScanCell<'a, Double>,
}

impl<'a> ColumnScanT<'a> {
    /// Create a new [ColumnScanT].
    pub(crate) fn new(
        scan_id32: ColumnScanEnum<'a, u32>,
        scan_id64: ColumnScanEnum<'a, u64>,
        scan_i64: ColumnScanEnum<'a, i64>,
        scan_float: ColumnScanEnum<'a, Float>,
        scan_double: ColumnScanEnum<'a, Double>,
    ) -> ColumnScanT<'a> {
        Self {
            scan_id32: ColumnScanCell::new(scan_id32),
            scan_id64: ColumnScanCell::new(scan_id64),
            scan_i64: ColumnScanCell::new(scan_i64),
            scan_float: ColumnScanCell::new(scan_float),
            scan_double: ColumnScanCell::new(scan_double),
        }
    }

    /// For the given [StorageValueT], find it in the associated [ColumnScan]
    /// or the first value that is higher.
    ///
    /// Returns `None` if there is no such value.
    #[allow(dead_code)]
    pub(crate) fn seek(&mut self, value: StorageValueT) -> Option<StorageValueT> {
        match value {
            StorageValueT::Id32(value) => self.scan_id32.seek(value).map(StorageValueT::Id32),
            StorageValueT::Id64(value) => self.scan_id64.seek(value).map(StorageValueT::Id64),
            StorageValueT::Int64(value) => self.scan_i64.seek(value).map(StorageValueT::Int64),
            StorageValueT::Float(value) => self.scan_float.seek(value).map(StorageValueT::Float),
            StorageValueT::Double(value) => self.scan_double.seek(value).map(StorageValueT::Double),
        }
    }

    /// Advance the [ColumnScan] of the given [StorageType] to the next value
    /// and return it.
    ///
    /// Returns `None` if there is no such value.
    pub(crate) fn next(&mut self, storage_type: StorageType) -> Option<StorageValueT> {
        match storage_type {
            StorageType::Id32 => self.scan_id32.next().map(StorageValueT::Id32),
            StorageType::Id64 => self.scan_id64.next().map(StorageValueT::Id64),
            StorageType::Int64 => self.scan_i64.next().map(StorageValueT::Int64),
            StorageType::Float => self.scan_float.next().map(StorageValueT::Float),
            StorageType::Double => self.scan_double.next().map(StorageValueT::Double),
        }
    }

    /// Return the current value of a scan of the given [StorageType].
    pub(crate) fn current(&self, storage_type: StorageType) -> Option<StorageValueT> {
        match storage_type {
            StorageType::Id32 => self.scan_id32.current().map(StorageValueT::Id32),
            StorageType::Id64 => self.scan_id64.current().map(StorageValueT::Id64),
            StorageType::Int64 => self.scan_i64.current().map(StorageValueT::Int64),
            StorageType::Float => self.scan_float.current().map(StorageValueT::Float),
            StorageType::Double => self.scan_double.current().map(StorageValueT::Double),
        }
    }

    /// Return the current position of a scan of the given [StorageType].
    pub(crate) fn pos(&mut self, storage_type: StorageType) -> Option<usize> {
        match storage_type {
            StorageType::Id32 => self.scan_id32.pos(),
            StorageType::Id64 => self.scan_id64.pos(),
            StorageType::Int64 => self.scan_i64.pos(),
            StorageType::Float => self.scan_float.pos(),
            StorageType::Double => self.scan_double.pos(),
        }
    }

    /// Return a scan of the given [StorageType] to its initial state.
    pub(crate) fn reset(&mut self, storage_type: StorageType) {
        match storage_type {
            StorageType::Id32 => self.scan_id32.reset(),
            StorageType::Id64 => self.scan_id64.reset(),
            StorageType::Int64 => self.scan_i64.reset(),
            StorageType::Float => self.scan_float.reset(),
            StorageType::Double => self.scan_double.reset(),
        }
    }

    /// Restricts the iterator of the given [StorageType] to the given `interval`.
    /// Resets the iterator just before the start of the interval.
    pub(crate) fn narrow(&mut self, storage_type: StorageType, interval: Range<usize>) {
        match storage_type {
            StorageType::Id32 => self.scan_id32.narrow(interval),
            StorageType::Id64 => self.scan_id64.narrow(interval),
            StorageType::Int64 => self.scan_i64.narrow(interval),
            StorageType::Float => self.scan_float.narrow(interval),
            StorageType::Double => self.scan_double.narrow(interval),
        }
    }

    /// Assumes that column scan is a [ColumnScanConstant].
    /// Set a new constant value that will be returned by this scan.
    pub(crate) fn constant_set(&mut self, constant: StorageValueT) {
        match constant {
            StorageValueT::Id32(value) => self.scan_id32.constant_set(Some(value)),
            StorageValueT::Id64(value) => self.scan_id64.constant_set(Some(value)),
            StorageValueT::Int64(value) => self.scan_i64.constant_set(Some(value)),
            StorageValueT::Float(value) => self.scan_float.constant_set(Some(value)),
            StorageValueT::Double(value) => self.scan_double.constant_set(Some(value)),
        }
    }

    /// Assumes that column scan is a [ColumnScanConstant].
    /// Set its value to `None`.
    pub(crate) fn constant_set_none(&mut self, storage_type: StorageType) {
        match storage_type {
            StorageType::Id32 => self.scan_id32.constant_set(None),
            StorageType::Id64 => self.scan_id64.constant_set(None),
            StorageType::Int64 => self.scan_i64.constant_set(None),
            StorageType::Float => self.scan_float.constant_set(None),
            StorageType::Double => self.scan_double.constant_set(None),
        }
    }

    /// Assumes that column scan is a [ColumnScanConstant].
    /// Set the value of all types to `None`.
    pub(crate) fn constant_set_none_all(&mut self) {
        self.scan_id32.constant_set(None);
        self.scan_id64.constant_set(None);
        self.scan_i64.constant_set(None);
        self.scan_float.constant_set(None);
        self.scan_double.constant_set(None);
    }
}
