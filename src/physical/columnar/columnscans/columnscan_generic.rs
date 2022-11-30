use crate::generate_forwarder;
use crate::physical::columnar::columns::{Column, ColumnVector, IntervalColumnGeneric};
use crate::physical::datatypes::ColumnDataType;
use std::marker::PhantomData;
use std::{fmt::Debug, ops::Range};

use super::colscan::ColScan;

/// Simple implementation of [`ColumnScan`] for an arbitrary [`Column`].
#[derive(Debug)]
pub struct ColScanGeneric<'a, T, Col: Column<'a, T>> {
    _t: PhantomData<T>,
    column: &'a Col,
    pos: Option<usize>,
    interval: Range<usize>,
}

/// Enum encapsulating implementations of ColScanGenerics
#[derive(Debug)]
pub enum ColScanGenericEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Case Scan with ColumnVector
    ColumnVector(ColScanGeneric<'a, T, ColumnVector<T>>),
    /// Case Scan with IntervalColumnGeneric
    IntervalColumnGeneric(ColScanGeneric<'a, T, IntervalColumnGeneric<T>>),
}

generate_forwarder!(forward_to_column_scan;
                    ColumnVector,
                    IntervalColumnGeneric);

impl<'a, T> From<ColScanGeneric<'a, T, ColumnVector<T>>> for ColScanGenericEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ColScanGeneric<'a, T, ColumnVector<T>>) -> Self {
        Self::ColumnVector(cs)
    }
}

impl<'a, T> From<ColScanGeneric<'a, T, IntervalColumnGeneric<T>>> for ColScanGenericEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn from(cs: ColScanGeneric<'a, T, IntervalColumnGeneric<T>>) -> Self {
        Self::IntervalColumnGeneric(cs)
    }
}

impl<'a, T, Col> ColScanGeneric<'a, T, Col>
where
    T: 'a + Debug + Copy + Ord,
    Col: Column<'a, T>,
{
    /// Defines the lower limit of elements in the interval where a binary search is used instead of a vector-scan
    const SEEK_BINARY_SEARCH: usize = 10;

    /// Constructs a new [`ColScanGeneric`] for a Column.
    pub fn new(column: &'a Col) -> Self {
        debug_assert!(column.len() > 0);

        Self {
            _t: PhantomData,
            column,
            pos: None,
            interval: 0..column.len(),
        }
    }

    /// Constructs a new [`ColScanGeneric`] for a Column, narrowed
    /// to the given interval.
    pub fn narrowed(column: &'a Col, interval: Range<usize>) -> Self {
        debug_assert!(interval.end >= interval.start);

        let result = Self {
            _t: PhantomData,
            column,
            pos: None,
            interval,
        };
        result.validate_interval();
        result
    }

    fn validate_interval(&self) {
        assert!(
            self.interval.end <= self.column.len(),
            "Cannot narrow to an interval larger than the column."
        );
    }

    /// Lifts any restriction of the interval to some interval.
    pub fn widen(&mut self) -> &mut Self {
        debug_assert!(self.column.len() > 0);

        self.interval = 0..self.column.len();
        self.pos = None;
        self
    }

    /// Returns the first column index of the iterator.
    pub fn lower_bound(&self) -> usize {
        self.interval.start
    }

    /// Returns the smallest column index of that is not part of the
    /// iterator). This need not be a valid column index.
    pub fn upper_bound(&self) -> usize {
        self.interval.end
    }
}

impl<'a, T, Col> Iterator for ColScanGeneric<'a, T, Col>
where
    T: 'a + Debug + Copy + Ord,
    Col: Column<'a, T>,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let pos = self.pos.map_or_else(|| self.interval.start, |pos| pos + 1);
        self.pos = Some(pos);
        (pos < self.interval.end).then(|| self.column.get(pos))
    }
}

impl<'a, T, Col> ColScan for ColScanGeneric<'a, T, Col>
where
    T: 'a + Debug + Copy + Ord,
    Col: Column<'a, T>,
{
    fn seek(&mut self, value: T) -> Option<T> {
        let pos = self.pos.get_or_insert(self.interval.start);
        let mut lower = *pos;
        let mut upper = self.interval.end - 1;

        // check if position is out of bounds
        if *pos > upper {
            return None;
        }
        // check if value exceeds the greatest element in column
        if self.column.get(upper) < value {
            *pos = self.column.len();
            return None;
        }
        // check if lower bound is already the target value
        if self.column.get(lower) >= value {
            *pos = lower;
            return Some(self.column.get(*pos));
        }

        // do binary search till interval is small enough to be scanned
        while upper - lower >= Self::SEEK_BINARY_SEARCH {
            let mid = (lower + upper) / 2;
            if self.column.get(mid) < value {
                lower = mid + 1;
            } else {
                upper = mid;
            }
        }

        *pos = lower;
        // scan the interval
        while *pos <= upper && self.column.get(*pos) < value {
            *pos += 1;
        }
        // if it is out of bounds
        if *pos > upper {
            None
        } else {
            Some(self.column.get(*pos))
        }
    }

    fn current(&mut self) -> Option<T> {
        self.pos
            .and_then(|pos| (pos < self.interval.end).then(|| self.column.get(pos)))
    }

    fn reset(&mut self) {
        self.pos = None;
    }

    fn pos(&self) -> Option<usize> {
        self.pos
            .and_then(|pos| (pos < self.interval.end).then_some(pos))
    }

    fn narrow(&mut self, interval: Range<usize>) {
        debug_assert!(interval.end >= interval.start);

        self.interval = interval;
        self.pos = None;
        self.validate_interval();
    }
}

impl<'a, T> Iterator for ColScanGenericEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        forward_to_column_scan!(self, next)
    }
}

impl<'a, T> ColScan for ColScanGenericEnum<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn seek(&mut self, value: T) -> Option<T> {
        forward_to_column_scan!(self, seek(value))
    }

    fn current(&mut self) -> Option<T> {
        forward_to_column_scan!(self, current)
    }

    fn reset(&mut self) {
        forward_to_column_scan!(self, reset)
    }

    fn pos(&self) -> Option<usize> {
        forward_to_column_scan!(self, pos)
    }

    fn narrow(&mut self, interval: Range<usize>) {
        forward_to_column_scan!(self, narrow(interval))
    }
}

#[cfg(test)]
mod test {
    use test_log::test;

    use crate::physical::columnar::{
        colscans::{ColScan, ColScanGeneric},
        columns::ColumnVector,
    };

    fn get_test_column() -> ColumnVector<u64> {
        let data: Vec<u64> = vec![1, 2, 5];
        ColumnVector::new(data)
    }

    fn get_test_column_large() -> ColumnVector<u64> {
        let data: Vec<u64> = vec![
            1, 2, 5, 9, 12, 14, 16, 18, 21, 25, 28, 29, 30, 35, 37, 39, 40, 45, 47, 49,
        ];
        ColumnVector::new(data)
    }

    #[test]
    fn u64_iterate_column() {
        let test_column = get_test_column();
        let mut gcs: ColScanGeneric<u64, ColumnVector<u64>> = ColScanGeneric::new(&test_column);
        assert_eq!(gcs.pos(), None);
        assert_eq!(gcs.current(), None);
        assert_eq!(gcs.next(), Some(1));
        assert_eq!(gcs.current(), Some(1));
        assert_eq!(gcs.pos(), Some(0));
        assert_eq!(gcs.next(), Some(2));
        assert_eq!(gcs.current(), Some(2));
        assert_eq!(gcs.pos(), Some(1));
        assert_eq!(gcs.next(), Some(5));
        assert_eq!(gcs.current(), Some(5));
        assert_eq!(gcs.pos(), Some(2));
        assert_eq!(gcs.next(), None);
        assert_eq!(gcs.pos(), None);
        assert_eq!(gcs.current(), None);
    }

    #[test]
    fn u64_seek_column() {
        let test_column = get_test_column();
        let mut gcs: ColScanGeneric<u64, ColumnVector<u64>> = ColScanGeneric::new(&test_column);
        assert_eq!(gcs.pos(), None);
        assert_eq!(gcs.seek(2), Some(2));
        assert_eq!(gcs.pos(), Some(1));
        assert_eq!(gcs.seek(2), Some(2));
        assert_eq!(gcs.pos(), Some(1));
        assert_eq!(gcs.seek(3), Some(5));
        assert_eq!(gcs.pos(), Some(2));
        assert_eq!(gcs.seek(6), None);
        assert_eq!(gcs.pos(), None);
        assert_eq!(gcs.seek(3), None);
        assert_eq!(gcs.pos(), None);
    }

    #[test]
    fn u64_narrow() {
        let test_column = get_test_column();
        let mut gcs = ColScanGeneric::new(&test_column);
        gcs.narrow(0..2);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![1, 2]);
        let mut gcs = ColScanGeneric::new(&test_column);
        gcs.narrow(1..2);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![2]);
        let mut gcs = ColScanGeneric::new(&test_column);
        gcs.narrow(1..3);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![2, 5]);
        let mut gcs = ColScanGeneric::new(&test_column);
        gcs.narrow(1..1);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![]);
        let mut gcs = ColScanGeneric::new(&test_column);
        gcs.narrow(0..3);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![1, 2, 5]);
    }

    #[test]
    fn u64_narrowed() {
        let test_column = get_test_column();
        let gcs = ColScanGeneric::narrowed(&test_column, 0..2);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![1, 2]);
        let gcs = ColScanGeneric::narrowed(&test_column, 1..2);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![2]);
        let gcs = ColScanGeneric::narrowed(&test_column, 1..3);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![2, 5]);
        let gcs = ColScanGeneric::narrowed(&test_column, 1..1);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![]);
        let gcs = ColScanGeneric::narrowed(&test_column, 0..3);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![1, 2, 5]);
    }

    #[test]
    fn u64_narrow_and_widen() {
        let test_column = get_test_column();
        let mut gcs = ColScanGeneric::new(&test_column);
        gcs.narrow(1..1);
        gcs.widen();
        assert_eq!(gcs.collect::<Vec<_>>(), vec![1, 2, 5]);
        let mut gcs = ColScanGeneric::new(&test_column);
        gcs.widen().narrow(1..2);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![2]);
    }

    #[test]
    #[should_panic(expected = "Cannot narrow to an interval larger than the column.")]
    fn u64_narrow_to_invalid() {
        let test_column = get_test_column();
        let mut gcs = ColScanGeneric::new(&test_column);
        gcs.narrow(1..23);
    }

    #[test]
    #[should_panic(expected = "Cannot narrow to an interval larger than the column.")]
    fn u64_narrowed_to_invalid() {
        let test_column = get_test_column();
        let _ = ColScanGeneric::narrowed(&test_column, 1..23);
    }

    #[test]
    fn u64_narrow_after_use() {
        let test_column = get_test_column();
        let mut gcs = ColScanGeneric::new(&test_column);
        assert_eq!(gcs.next(), Some(1));
        gcs.narrow(0..2);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![1, 2]);
    }

    #[test]
    fn u64_widen_after_use() {
        let test_column = get_test_column();
        let mut gcs = ColScanGeneric::new(&test_column);
        gcs.narrow(1..2);
        assert_eq!(gcs.next(), Some(2));
        gcs.widen();
        assert_eq!(gcs.collect::<Vec<_>>(), vec![1, 2, 5]);
    }

    #[test]
    fn u64_seek_interval() {
        let test_column = get_test_column_large();
        let mut gcs = ColScanGeneric::new(&test_column);

        gcs.narrow(4..16);
        assert_eq!(gcs.seek(2), Some(12));
        assert_eq!(gcs.current(), Some(12));
        assert_eq!(gcs.seek(17), Some(18));
        assert_eq!(gcs.current(), Some(18));
        assert_eq!(gcs.seek(25), Some(25));
        assert_eq!(gcs.current(), Some(25));
        assert_eq!(gcs.seek(39), Some(39));
        assert_eq!(gcs.seek(40), None);
        assert_eq!(gcs.current(), None);
    }
}
