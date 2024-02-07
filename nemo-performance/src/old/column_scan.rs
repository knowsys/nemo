use std::{cell::UnsafeCell, fmt::Debug, ops::Range};

use nemo_physical::{
    columnar::{
        column::{rle::ColumnScanRle, vector::ColumnScanVector},
        columnscan::ColumnScan,
    },
    datatypes::{ColumnDataType, Double, Float, StorageValueT},
    generate_datatype_forwarder, generate_forwarder,
};

use super::column_scan_prune::ColumnScanPrune;

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

    ColumnScanPrune(ColumnScanPrune<'a, T>),
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

// Generate a macro forward_to_columnscan!, which takes a [`ColumnScanEnum`] and a function as arguments
// and unfolds into a `match` statement that calls the variant specific version of that function.
// Each new variant of a [`ColumnScanEnum`] must be added here.
// See `physical/util.rs` for a more detailed description of this macro.
generate_forwarder!(forward_to_columnscan;
    ColumnScanVector,
    ColumnScanRle,
    ColumnScanPrune
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
