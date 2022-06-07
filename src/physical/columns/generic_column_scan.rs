use super::{Column, ColumnScan, GenericIntervalColumnEnum, RangedColumnScan, VectorColumn};
use crate::physical::datatypes::{Field, FloorToUsize};
use std::cell::Cell;
use std::marker::PhantomData;
use std::{fmt::Debug, ops::Range};

/// Simple implementation of [`ColumnScan`] for an arbitrary [`Column`].
pub struct GenericColumnScan<'a, T, Col: Column<'a, T>> {
    _t: PhantomData<T>,
    column: &'a Col,
    pos: Cell<Option<usize>>,
    interval: Cell<Range<usize>>,
}

impl<'a, T, Col> Debug for GenericColumnScan<'a, T, Col>
where
    T: 'a + Debug + Copy + Ord,
    Col: Column<'a, T>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let interval = self.lower_bound()..self.upper_bound();
        f.debug_struct("GenericColumnScan")
            .field("column", &self.column)
            .field("pos", &self.pos)
            .field("interval", &interval)
            .finish()
    }
}

/// Enum encapsulating implementations of GenericColumnScans
#[derive(Debug)]
pub enum GenericColumnScanEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    /// Case Scan with VectorColumn
    VectorColumn(GenericColumnScan<'a, T, VectorColumn<T>>),
    /// Case Scan with GenericIntervalColumn
    GenericIntervalColumn(GenericColumnScan<'a, T, GenericIntervalColumnEnum<'a, T>>),
}

impl<'a, T, Col> GenericColumnScan<'a, T, Col>
where
    T: 'a + Debug + Copy + Ord,
    Col: Column<'a, T>,
{
    /// Defines the lower limit of elements in the interval where a binary search is used instead of a vector-scan
    const SEEK_BINARY_SEARCH: usize = 10;

    /// Constructs a new [`GenericColumnScan`] for a Column.
    pub fn new(column: &'a Col) -> Self {
        Self {
            _t: PhantomData,
            column,
            pos: Cell::new(None),
            interval: Cell::new(0..column.len()),
        }
    }

    /// Constructs a new [`GenericColumnScan`] for a Column, narrowed
    /// to the given interval.
    pub fn narrowed(column: &'a Col, interval: Range<usize>) -> Self {
        let result = Self {
            _t: PhantomData,
            column,
            pos: Cell::new(None),
            interval: Cell::new(interval),
        };
        result.validate_interval();
        result
    }

    fn validate_interval(&self) {
        let interval = self.interval.take();
        assert!(
            interval.end <= self.column.len(),
            "Cannot narrow to an interval larger than the column."
        );
        self.interval.set(interval);
    }

    /// Lifts any restriction of the interval to some interval.
    pub fn widen(&mut self) -> &mut Self {
        self.interval.set(0..self.column.len());
        self.pos.set(None);
        self
    }

    /// Returns the first column index of the iterator.
    pub fn lower_bound(&self) -> usize {
        unsafe { (*self.interval.as_ptr()).start }
    }

    /// Returns the smallest column index of that is not part of the
    /// iterator). This need not be a valid column index.
    pub fn upper_bound(&self) -> usize {
        unsafe { (*self.interval.as_ptr()).end }
    }
}

impl<'a, T, Col> Iterator for GenericColumnScan<'a, T, Col>
where
    T: 'a + Debug + Copy + Ord,
    Col: Column<'a, T>,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let position = self.pos.get_mut();
        let pos = position.map_or_else(|| self.lower_bound(), |pos| pos + 1);
        self.pos.set(Some(pos));
        (pos < self.upper_bound()).then(|| self.column.get(pos))
    }
}

impl<'a, T, Col> ColumnScan for GenericColumnScan<'a, T, Col>
where
    T: 'a + Debug + Copy + Ord,
    Col: Column<'a, T>,
{
    fn seek(&mut self, value: T) -> Option<T> {
        let lower_bound = self.lower_bound();
        let mut upper = self.upper_bound() - 1;
        let position = self.pos.get_mut();
        let pos = position.get_or_insert(lower_bound);
        let mut lower = *pos;

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
        let position = self.pos.get_mut();
        position.and_then(|pos| (pos < self.upper_bound()).then(|| self.column.get(pos)))
    }

    fn reset(&mut self) {
        self.pos.set(None);
    }
}

impl<'a, T, Col> RangedColumnScan for GenericColumnScan<'a, T, Col>
where
    T: 'a + Debug + Copy + Ord,
    Col: Column<'a, T>,
{
    fn pos(&self) -> Option<usize> {
        unsafe { (*self.pos.as_ptr()).and_then(|pos| (pos < self.upper_bound()).then(|| pos)) }
    }

    fn narrow(&mut self, interval: Range<usize>) {
        self.interval.replace(interval);
        self.pos.set(None);
        self.validate_interval();
    }

    fn narrow_unsafe(&self, interval: Range<usize>) {
        self.interval.replace(interval);
        self.pos.set(None);
        self.validate_interval()
    }
}

impl<'a, T> Iterator for GenericColumnScanEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::VectorColumn(col) => col.next(),
            Self::GenericIntervalColumn(col) => col.next(),
        }
    }
}

impl<'a, T> ColumnScan for GenericColumnScanEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn seek(&mut self, value: T) -> Option<T> {
        match self {
            Self::VectorColumn(col) => col.seek(value),
            Self::GenericIntervalColumn(col) => col.seek(value),
        }
    }

    fn current(&mut self) -> Option<T> {
        match self {
            Self::VectorColumn(col) => col.current(),
            Self::GenericIntervalColumn(col) => col.current(),
        }
    }

    fn reset(&mut self) {
        match self {
            Self::VectorColumn(col) => col.reset(),
            Self::GenericIntervalColumn(col) => col.reset(),
        }
    }
}

impl<'a, T> RangedColumnScan for GenericColumnScanEnum<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn pos(&self) -> Option<usize> {
        match self {
            Self::VectorColumn(col) => col.pos.get(),
            Self::GenericIntervalColumn(col) => col.pos.get(),
        }
    }

    fn narrow(&mut self, interval: Range<usize>) {
        match self {
            Self::VectorColumn(col) => col.narrow(interval),
            Self::GenericIntervalColumn(col) => col.narrow(interval),
        }
    }

    fn narrow_unsafe(&self, interval: Range<usize>) {
        match self {
            Self::VectorColumn(col) => col.narrow_unsafe(interval),
            Self::GenericIntervalColumn(col) => col.narrow_unsafe(interval),
        }
    }
}

#[cfg(test)]
mod test {
    use super::super::VectorColumn;
    use super::{ColumnScan, GenericColumnScan, RangedColumnScan}; // < TODO: is this a nice way to write this use?
    use test_log::test;

    fn get_test_column() -> VectorColumn<u64> {
        let data: Vec<u64> = vec![1, 2, 5];
        VectorColumn::new(data)
    }

    fn get_test_column_large() -> VectorColumn<u64> {
        let data: Vec<u64> = vec![
            1, 2, 5, 9, 12, 14, 16, 18, 21, 25, 28, 29, 30, 35, 37, 39, 40, 45, 47, 49,
        ];
        VectorColumn::new(data)
    }

    #[test]
    fn u64_iterate_column() {
        let test_column = get_test_column();
        let mut gcs: GenericColumnScan<u64, VectorColumn<u64>> =
            GenericColumnScan::new(&test_column);
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
        let mut gcs: GenericColumnScan<u64, VectorColumn<u64>> =
            GenericColumnScan::new(&test_column);
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
        let mut gcs = GenericColumnScan::new(&test_column);
        gcs.narrow(0..2);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![1, 2]);
        let mut gcs = GenericColumnScan::new(&test_column);
        gcs.narrow(1..2);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![2]);
        let mut gcs = GenericColumnScan::new(&test_column);
        gcs.narrow(1..3);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![2, 5]);
        let mut gcs = GenericColumnScan::new(&test_column);
        gcs.narrow(1..1);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![]);
        let mut gcs = GenericColumnScan::new(&test_column);
        gcs.narrow(0..3);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![1, 2, 5]);
    }

    #[test]
    fn u64_narrow_unsafe() {
        let test_column = get_test_column();
        let gcs = GenericColumnScan::new(&test_column);
        gcs.narrow_unsafe(0..2);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![1, 2]);
        let gcs = GenericColumnScan::new(&test_column);
        gcs.narrow_unsafe(1..2);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![2]);
        let gcs = GenericColumnScan::new(&test_column);
        gcs.narrow_unsafe(1..3);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![2, 5]);
        let gcs = GenericColumnScan::new(&test_column);
        gcs.narrow_unsafe(1..1);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![]);
        let gcs = GenericColumnScan::new(&test_column);
        gcs.narrow_unsafe(0..3);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![1, 2, 5]);
    }

    #[test]
    fn u64_narrowed() {
        let test_column = get_test_column();
        let gcs = GenericColumnScan::narrowed(&test_column, 0..2);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![1, 2]);
        let gcs = GenericColumnScan::narrowed(&test_column, 1..2);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![2]);
        let gcs = GenericColumnScan::narrowed(&test_column, 1..3);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![2, 5]);
        let gcs = GenericColumnScan::narrowed(&test_column, 1..1);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![]);
        let gcs = GenericColumnScan::narrowed(&test_column, 0..3);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![1, 2, 5]);
    }

    #[test]
    fn u64_narrow_and_widen() {
        let test_column = get_test_column();
        let mut gcs = GenericColumnScan::new(&test_column);
        gcs.narrow(1..1);
        gcs.widen();
        assert_eq!(gcs.collect::<Vec<_>>(), vec![1, 2, 5]);
        let mut gcs = GenericColumnScan::new(&test_column);
        gcs.widen().narrow(1..2);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![2]);
    }

    #[test]
    fn u64_narrow_and_widen_unsafe() {
        let test_column = get_test_column();
        let mut gcs = GenericColumnScan::new(&test_column);
        gcs.narrow_unsafe(1..1);
        gcs.widen();
        assert_eq!(gcs.collect::<Vec<_>>(), vec![1, 2, 5]);
        let mut gcs = GenericColumnScan::new(&test_column);
        gcs.widen().narrow_unsafe(1..2);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![2]);
    }

    #[test]
    #[should_panic(expected = "Cannot narrow to an interval larger than the column.")]
    fn u64_narrow_to_invalid() {
        let test_column = get_test_column();
        let mut gcs = GenericColumnScan::new(&test_column);
        gcs.narrow(1..23);
    }

    #[test]
    #[should_panic(expected = "Cannot narrow to an interval larger than the column.")]
    fn u64_narrowed_to_invalid() {
        let test_column = get_test_column();
        let _ = GenericColumnScan::narrowed(&test_column, 1..23);
    }

    #[test]
    fn u64_narrow_after_use() {
        let test_column = get_test_column();
        let mut gcs = GenericColumnScan::new(&test_column);
        assert_eq!(gcs.next(), Some(1));
        gcs.narrow(0..2);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![1, 2]);
    }

    #[test]
    fn u64_narrow_after_use_unsafe() {
        let test_column = get_test_column();
        let mut gcs = GenericColumnScan::new(&test_column);
        assert_eq!(gcs.next(), Some(1));
        gcs.narrow_unsafe(0..2);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![1, 2]);
    }

    #[test]
    fn u64_widen_after_use() {
        let test_column = get_test_column();
        let mut gcs = GenericColumnScan::new(&test_column);
        gcs.narrow(1..2);
        assert_eq!(gcs.next(), Some(2));
        gcs.widen();
        assert_eq!(gcs.collect::<Vec<_>>(), vec![1, 2, 5]);
    }

    #[test]
    fn u64_widen_after_use_unsafe() {
        let test_column = get_test_column();
        let mut gcs = GenericColumnScan::new(&test_column);
        gcs.narrow_unsafe(1..2);
        assert_eq!(gcs.next(), Some(2));
        gcs.widen();
        assert_eq!(gcs.collect::<Vec<_>>(), vec![1, 2, 5]);
    }

    #[test]
    fn u64_seek_interval() {
        let test_column = get_test_column_large();
        let mut gcs = GenericColumnScan::new(&test_column);

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
