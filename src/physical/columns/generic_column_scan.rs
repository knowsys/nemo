use super::{Column, ColumnScan, MaterialColumnScan};
use std::{fmt::Debug, ops::Range};

/// Simple implementation of [`ColumnScan`] for an arbitrary [`Column`].
#[derive(Debug)]
pub struct GenericColumnScan<'a, T> {
    column: &'a dyn Column<T>,
    pos: Option<usize>,
    interval: Option<Range<usize>>,
}

impl<'a, T> GenericColumnScan<'a, T> {
    /// Constructs a new [`GenericColumnScan`] for a Column.
    pub fn new(column: &'a dyn Column<T>) -> GenericColumnScan<'a, T> {
        GenericColumnScan {
            column,
            pos: None,
            interval: None,
        }
    }

    /// Restricts the iterator to the given `interval`.
    pub fn narrow(&mut self, interval: Range<usize>) -> &mut Self {
        assert!(
            interval.end <= self.column.len(),
            "Cannot narrow to an interval larger than the column."
        );

        self.interval = Some(interval);
        self
    }

    /// Lifts any restriction of the interval to some interval.
    pub fn widen(&mut self) -> &mut Self {
        self.interval = None;
        self
    }

    /// Returns the first column index of the iterator.
    pub fn lower_bound(&self) -> usize {
        match &self.interval {
            Some(range) => range.start,
            None => 0,
        }
    }

    /// Returns the smallest column index of that is not part of the
    /// iterator). This need not be a valid column index.
    pub fn upper_bound(&self) -> usize {
        match &self.interval {
            Some(range) => range.end,
            None => self.column.len(),
        }
    }
}

impl<'a, T: Debug + Copy> Iterator for GenericColumnScan<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let pos = self.pos.map_or_else(|| self.lower_bound(), |pos| pos + 1);
        self.pos = Some(pos);
        (pos < self.upper_bound()).then(|| self.column[pos])
    }
}

impl<'a, T: Ord + Copy + Debug> ColumnScan for GenericColumnScan<'a, T> {
    fn seek(&mut self, value: T) -> Option<T> {
        // Brute-force scan:
        while self.pos.unwrap_or_default() < self.upper_bound() {
            let pos = self.pos.get_or_insert(0);

            if self.column[*pos] >= value {
                return Some(self.column[*pos]);
            }

            *pos += 1;
        }
        None
    }

    fn current(&mut self) -> Option<T> {
        self.pos
            .and_then(|pos| (pos < self.upper_bound()).then(|| self.column[pos]))
    }
}

impl<'a, T: Ord + Copy + Debug> MaterialColumnScan for GenericColumnScan<'a, T> {
    fn pos(&mut self) -> Option<usize> {
        self.pos
            .and_then(|pos| (pos < self.upper_bound()).then(|| pos))
    }
}

#[cfg(test)]
mod test {
    use super::super::VectorColumn;
    use super::{ColumnScan, GenericColumnScan, MaterialColumnScan}; // < TODO: is this a nice way to write this use?
    use test_env_log::test;

    fn get_test_column() -> VectorColumn<u64> {
        let data: Vec<u64> = vec![1, 2, 5];
        VectorColumn::new(data)
    }

    #[test]
    fn u64_iterate_column() {
        let test_column = get_test_column();
        let mut gcs: GenericColumnScan<u64> = GenericColumnScan::new(&test_column);
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
        let mut gcs: GenericColumnScan<u64> = GenericColumnScan::new(&test_column);
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
    fn u64_narrow_and_widen() {
        let test_column = get_test_column();
        let mut gcs = GenericColumnScan::new(&test_column);
        gcs.narrow(1..1).widen();
        assert_eq!(gcs.collect::<Vec<_>>(), vec![1, 2, 5]);
        let mut gcs = GenericColumnScan::new(&test_column);
        gcs.widen().narrow(1..2);
        assert_eq!(gcs.collect::<Vec<_>>(), vec![2]);
    }

    #[test]
    #[should_panic(expected = "Cannot narrow to an interval larger than the column.")]
    fn u64_narrow_to_invalid() {
        let test_column = get_test_column();
        let mut gcs = GenericColumnScan::new(&test_column);
        gcs.narrow(1..23);
    }
}
