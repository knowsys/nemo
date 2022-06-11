use super::{
    column_scan::{ColumnScan, ScanToCell},
    GenericColumnScanEnum, OrderedMergeJoin, RleColumnScan,
};
use crate::physical::datatypes::{DataValueT, Double, Field, Float, FloorToUsize};
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

impl<'a, T> ScanToCell for RangedColumnScanEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    type Cell = RangedColumnScanCell<'a, T>;

    fn into_cell(self) -> Self::Cell {
        Self::Cell::new(self)
    }
}

impl<'a, T> Iterator for RangedColumnScanEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::GenericColumnScan(cs) => cs.next(),
            Self::RleColumnScan(cs) => cs.next(),
            Self::OrderedMergeJoin(cs) => cs.next(),
        }
    }
}

impl<'a, T> ColumnScan for RangedColumnScanEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn seek(&mut self, value: Self::Item) -> Option<Self::Item> {
        match self {
            RangedColumnScanEnum::GenericColumnScan(cs) => cs.seek(value),
            RangedColumnScanEnum::RleColumnScan(cs) => cs.seek(value),
            RangedColumnScanEnum::OrderedMergeJoin(cs) => cs.seek(value),
        }
    }

    fn current(&mut self) -> Option<Self::Item> {
        match self {
            RangedColumnScanEnum::GenericColumnScan(cs) => cs.current(),
            RangedColumnScanEnum::RleColumnScan(cs) => cs.current(),
            RangedColumnScanEnum::OrderedMergeJoin(cs) => cs.current(),
        }
    }

    fn reset(&mut self) {
        match self {
            RangedColumnScanEnum::GenericColumnScan(cs) => cs.reset(),
            RangedColumnScanEnum::RleColumnScan(cs) => cs.reset(),
            RangedColumnScanEnum::OrderedMergeJoin(cs) => cs.reset(),
        }
    }
}

impl<'a, T> RangedColumnScan for RangedColumnScanEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn pos(&self) -> Option<usize> {
        match self {
            RangedColumnScanEnum::GenericColumnScan(cs) => cs.pos(),
            RangedColumnScanEnum::RleColumnScan(cs) => cs.pos(),
            RangedColumnScanEnum::OrderedMergeJoin(cs) => cs.pos(),
        }
    }

    fn narrow(&mut self, interval: Range<usize>) {
        match self {
            RangedColumnScanEnum::GenericColumnScan(cs) => cs.narrow(interval),
            RangedColumnScanEnum::RleColumnScan(cs) => cs.narrow(interval),
            RangedColumnScanEnum::OrderedMergeJoin(cs) => cs.narrow(interval),
        }
    }
}

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
    pub fn new(cs: RangedColumnScanEnum<'a, T>) -> Self {
        Self(UnsafeCell::new(cs))
    }

    #[inline]
    pub fn next(&self) -> Option<<RangedColumnScanEnum<'a, T> as Iterator>::Item> {
        unsafe { &mut *self.0.get() }.next()
    }

    #[inline]
    pub fn seek(
        &self,
        value: <RangedColumnScanEnum<'a, T> as Iterator>::Item,
    ) -> Option<<RangedColumnScanEnum<'a, T> as Iterator>::Item> {
        unsafe { &mut *self.0.get() }.seek(value)
    }

    #[inline]
    pub fn current(&self) -> Option<<RangedColumnScanEnum<'a, T> as Iterator>::Item> {
        unsafe { &mut *self.0.get() }.current()
    }

    #[inline]
    pub fn reset(&self) {
        unsafe { &mut *self.0.get() }.reset()
    }

    #[inline]
    pub fn pos(&self) -> Option<usize> {
        unsafe { &(*self.0.get()) }.pos()
    }

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

impl<'a> Iterator for RangedColumnScanT<'a> {
    type Item = DataValueT;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::U64(cs) => cs.next().map(DataValueT::U64),
            Self::Float(cs) => cs.next().map(DataValueT::Float),
            Self::Double(cs) => cs.next().map(DataValueT::Double),
        }
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
        match self {
            Self::U64(cs) => cs.current().map(DataValueT::U64),
            Self::Float(cs) => cs.current().map(DataValueT::Float),
            Self::Double(cs) => cs.current().map(DataValueT::Double),
        }
    }

    fn reset(&mut self) {
        match self {
            Self::U64(cs) => cs.reset(),
            Self::Float(cs) => cs.reset(),
            Self::Double(cs) => cs.reset(),
        }
    }
}

impl<'a> RangedColumnScan for RangedColumnScanT<'a> {
    fn pos(&self) -> Option<usize> {
        match self {
            Self::U64(cs) => cs.pos(),
            Self::Float(cs) => cs.pos(),
            Self::Double(cs) => cs.pos(),
        }
    }

    fn narrow(&mut self, interval: Range<usize>) {
        match self {
            Self::U64(cs) => cs.narrow(interval),
            Self::Float(cs) => cs.narrow(interval),
            Self::Double(cs) => cs.narrow(interval),
        }
    }
}
