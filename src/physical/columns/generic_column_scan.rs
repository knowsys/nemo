use super::{Column, ColumnScan, MaterialColumnScan};
use std::fmt::Debug;

/// Simple implementation of [`ColumnScan`] for an arbitrary [`Column`].
#[derive(Debug)]
pub struct GenericColumnScan<'a, T> {
    column: &'a dyn Column<T>,
    pos: Option<usize>,
}

impl<'a, T> GenericColumnScan<'a, T> {
    /// Defines the lower limit of elements in the interval where a binary search is used instead of a vector-scan
    const SEEK_BINARY_SEARCH: usize = 2;
    /// Constructs a new [`GenericColumnScan`] for a Column.
    pub fn new(column: &'a dyn Column<T>) -> GenericColumnScan<'a, T> {
        GenericColumnScan { column, pos: None }
    }
}

impl<'a, T: Debug + Copy> Iterator for GenericColumnScan<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let pos = self.pos.map_or_else(Default::default, |pos| pos + 1);
        self.pos = Some(pos);
        (pos < self.column.len()).then(|| self.column[pos])
    }
}

impl<'a, T: Ord + Copy + Debug> ColumnScan for GenericColumnScan<'a, T> {
    fn seek(&mut self, value: T) -> Option<T> {
        let pos = self.pos.get_or_insert(0);
        let mut lower = *pos;
        let mut upper = self.column.len() - 1;
        // check if position is out of bounds
        if *pos > upper {
            return None;
        }
        // check if value exceeds the greatest element in column
        if self.column[upper] < value {
            *pos = self.column.len();
            return None;
        }
        // check if lower bound is already the target value
        if self.column[lower] >= value {
            *pos = lower;
            return Some(self.column[*pos]);
        }
        // do binary search till interval is small enough to be scanned
        while upper - lower >= Self::SEEK_BINARY_SEARCH {
            let mid = (lower + upper) / 2;
            if self.column[mid] < value {
                lower = mid + 1;
            } else {
                upper = mid;
            }
        }

        *pos = lower;
        // scan the interval
        while *pos <= upper && self.column[*pos] < value {
            *pos += 1;
        }
        // if it is out of bounds
        if *pos > upper {
            None
        } else {
            Some(self.column[*pos])
        }
    }

    fn current(&mut self) -> Option<T> {
        self.pos
            .and_then(|pos| (pos < self.column.len()).then(|| self.column[pos]))
    }
}

impl<'a, T: Ord + Copy + Debug> MaterialColumnScan for GenericColumnScan<'a, T> {
    fn pos(&mut self) -> Option<usize> {
        self.pos
            .and_then(|pos| (pos < self.column.len()).then(|| pos))
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
    fn test_u64_iterate_column() {
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
    fn test_u64_seek_column() {
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
}
