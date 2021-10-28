use super::{Column,ColumnScan};
use std::fmt::Debug;

/// Simple implementation of [`ColumnScan`] for an arbitrary [`Column`].
#[derive(Debug)]
pub struct GenericColumnScan<T> {
    column: Box<dyn Column<T>>,
    pos: usize,
    started: bool,
}

impl<T> GenericColumnScan<T> {
    /// Constructs a new VectorColumnScan for a Column.
    pub fn new(column: Box<dyn Column<T>>) -> GenericColumnScan<T> {
        GenericColumnScan{ 
            column: column, 
            pos: 0,
            started: false,
        }
    }
}

impl<T: Debug + Copy> Iterator for GenericColumnScan<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.started {
            self.pos += 1;
        } else {
            self.started = true;
        }

        if self.pos < self.column.len() {
            Some(self.column.get(self.pos))
        } else {
            None
        }
    }
}

impl<T: Ord + Copy + Debug> ColumnScan<T> for GenericColumnScan<T> {

    fn seek(&mut self, value: T) -> Option<T> {
        // Brute-force scan:
        self.started = true;
        while self.pos < self.column.len() {
            if self.column.get(self.pos) >= value {
                return Some(self.column.get(self.pos))
            }
            self.pos += 1;
        }
        None
    }

    fn current(&mut self) -> Option<T> {
        if self.started && self.pos < self.column.len() {
            Some(self.column.get(self.pos))
        } else {
            None
        }
    }

    fn pos(&mut self) -> Option<usize> {
        if self.started && self.pos < self.column.len() {
            Some(self.pos)
        } else {
            None
        }
    }
}


#[cfg(test)]
mod test {
    use super::{GenericColumnScan, Column, ColumnScan};
    use super::super::VectorColumn; // < TODO: is this a nice way to write this use?

    fn get_test_column() -> Box<dyn Column<u64>> {
        let data: Vec<u64> = vec![1,2,5];
        Box::new(VectorColumn::new(data))
    }

    #[test]
    fn test_u64_iterate_column() {
        let mut gcs : GenericColumnScan<u64> = GenericColumnScan::new(get_test_column());
        assert_eq!(gcs.pos(),None);
        assert_eq!(gcs.current(),None);
        assert_eq!(gcs.next(),Some(1));
        assert_eq!(gcs.current(),Some(1));
        assert_eq!(gcs.pos(),Some(0));
        assert_eq!(gcs.next(),Some(2));
        assert_eq!(gcs.current(),Some(2));
        assert_eq!(gcs.pos(),Some(1));
        assert_eq!(gcs.next(),Some(5));
        assert_eq!(gcs.current(),Some(5));
        assert_eq!(gcs.pos(),Some(2));
        assert_eq!(gcs.next(),None);
        assert_eq!(gcs.pos(),None);
        assert_eq!(gcs.current(),None);
    }

    #[test]
    fn test_u64_seek_column() {
        let mut gcs : GenericColumnScan<u64> = GenericColumnScan::new(get_test_column());
        assert_eq!(gcs.pos(),None);
        assert_eq!(gcs.seek(2),Some(2));
        assert_eq!(gcs.pos(),Some(1));
        assert_eq!(gcs.seek(2),Some(2));
        assert_eq!(gcs.pos(),Some(1));
        assert_eq!(gcs.seek(3),Some(5));
        assert_eq!(gcs.pos(),Some(2));
        assert_eq!(gcs.seek(6),None);
        assert_eq!(gcs.pos(),None);
        assert_eq!(gcs.seek(3),None);
        assert_eq!(gcs.pos(),None);
    }
}