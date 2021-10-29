use super::{Column, ColumnScan};
use std::fmt::Debug;

/// Simple implementation of [`ColumnScan`] for an arbitrary [`Column`].
#[derive(Debug)]
pub struct GenericColumnScan<T> {
    column: Box<dyn Column<T>>,
    pos: Option<usize>,
}

impl<T> GenericColumnScan<T> {
    /// Constructs a new VectorColumnScan for a Column.
    pub fn new(column: Box<dyn Column<T>>) -> GenericColumnScan<T> {
        GenericColumnScan { column, pos: None }
    }
}

impl<T: Debug + Copy> Iterator for GenericColumnScan<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let pos = self.pos.map_or_else(Default::default, |pos| pos + 1);
        self.pos = Some(pos);
        (pos < self.column.len()).then(|| self.column[pos])
    }
}

impl<T: Ord + Copy + Debug> ColumnScan<T> for GenericColumnScan<T> {
    fn seek(&mut self, value: T) -> Option<T> {
        // Brute-force scan:
        while self.pos.unwrap_or_default() < self.column.len() {
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
            .and_then(|pos| (pos < self.column.len()).then(|| self.column[pos]))
    }

    fn pos(&mut self) -> Option<usize> {
        self.pos
            .and_then(|pos| (pos < self.column.len()).then(|| pos))
    }
}

#[cfg(test)]
mod test {
    use super::super::VectorColumn;
    use super::{Column, ColumnScan, GenericColumnScan}; // < TODO: is this a nice way to write this use?

    fn get_test_column() -> Box<dyn Column<u64>> {
        let data: Vec<u64> = vec![1, 2, 5];
        Box::new(VectorColumn::new(data))
    }

    #[test]
    fn test_u64_iterate_column() {
        let mut gcs: GenericColumnScan<u64> = GenericColumnScan::new(get_test_column());
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
        let mut gcs: GenericColumnScan<u64> = GenericColumnScan::new(get_test_column());
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
