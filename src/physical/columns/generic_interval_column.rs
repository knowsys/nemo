use super::{Column, ColumnScan, GenericColumnScan, IntervalColumn};
use std::fmt::Debug;

/// Simple implementation of [`IntervalColumn`] that uses a second column to manage interval bounds.
#[derive(Debug)]
pub struct GenericIntervalColumn<T> {
    data: Box<dyn Column<T>>,
    int_starts: Box<dyn Column<usize>>,
}

impl<T> GenericIntervalColumn<T> {
    /// Constructs a new VectorColumn from a vector of the suitable type.
    pub fn new(
        data: Box<dyn Column<T>>,
        int_starts: Box<dyn Column<usize>>,
    ) -> GenericIntervalColumn<T> {
        GenericIntervalColumn { data, int_starts }
    }
}

impl<T: Debug + Copy + Ord> Column<T> for GenericIntervalColumn<T> {
    fn len(&self) -> usize {
        self.data.len()
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn get(&self, index: usize) -> T {
        self.data.get(index)
    }

    fn iter<'a>(&'a self) -> Box<dyn ColumnScan<Item = T> + 'a> {
        Box::new(GenericColumnScan::new(self))
    }
}

//impl<T: Debug + Copy + Ord> Index<usize> for GenericIntervalColumn<T> {
    //type Output = T;

    //fn index(&self, index: usize) -> &Self::Output {
        //&self.get(index)
    //}
//}

impl<T: Debug + Copy + Ord> IntervalColumn<T> for GenericIntervalColumn<T> {
    fn int_len(&self) -> usize {
        self.int_starts.len()
    }

    fn int_bounds(&self, int_idx: usize) -> (usize, usize) {
        let start_idx = self.int_starts.get(int_idx);
        if int_idx + 1 < self.int_starts.len() {
            (start_idx, self.int_starts.get(int_idx + 1) - 1)
        } else {
            (start_idx, self.data.len() - 1)
        }
    }
}

#[cfg(test)]
mod test {
    use super::super::VectorColumn;
    use super::{Column, GenericIntervalColumn, IntervalColumn};
    use test_log::test;

    #[test]
    fn test_u64_column() {
        let data: Vec<u64> = vec![1, 2, 3, 10, 11, 12, 20, 30, 31];
        let int_starts: Vec<usize> = vec![0, 3, 6, 7];

        let v_data: VectorColumn<u64> = VectorColumn::new(data);
        let v_int_starts: VectorColumn<usize> = VectorColumn::new(int_starts);
        let gic: GenericIntervalColumn<u64> =
            GenericIntervalColumn::new(Box::new(v_data), Box::new(v_int_starts));
        assert_eq!(gic.len(), 9);
        assert_eq!(gic.get(0), 1);
        assert_eq!(gic.get(1), 2);
        assert_eq!(gic.get(2), 3);
        assert_eq!(gic.get(3), 10);

        assert_eq!(gic.int_len(), 4);
        assert_eq!(gic.int_bounds(0), (0, 2));
        assert_eq!(gic.int_bounds(1), (3, 5));
        assert_eq!(gic.int_bounds(2), (6, 6));
        assert_eq!(gic.int_bounds(3), (7, 8));
    }
}
