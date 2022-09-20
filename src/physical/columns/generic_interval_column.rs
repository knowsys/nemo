use super::{Column, ColumnEnum, GenericColumnScan, IntervalColumn};
use crate::physical::datatypes::{Field, FloorToUsize};
use std::{fmt::Debug, ops::Range};

/// Simple implementation of [`IntervalColumn`] that uses a second column to manage interval bounds.
#[derive(Debug, Clone, PartialEq)]
pub struct GenericIntervalColumn<T> {
    data: ColumnEnum<T>,
    int_starts: ColumnEnum<usize>,
}

impl<T> GenericIntervalColumn<T> {
    /// Constructs a new [`GenericIntervalColumn`] given data column and a column containing the intervals
    pub fn new(data: ColumnEnum<T>, int_starts: ColumnEnum<usize>) -> GenericIntervalColumn<T> {
        GenericIntervalColumn { data, int_starts }
    }

    /// Return data column
    pub fn get_data_column(&self) -> &ColumnEnum<T> {
        &self.data
    }

    /// Return column containing the intervals
    pub fn get_int_column(&self) -> &ColumnEnum<usize> {
        &self.int_starts
    }
}

impl<'a, T> Column<'a, T> for GenericIntervalColumn<T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    type ColScan = GenericColumnScan<'a, T, GenericIntervalColumn<T>>;

    fn len(&self) -> usize {
        self.data.len()
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn get(&self, index: usize) -> T {
        self.data.get(index)
    }

    fn iter(&'a self) -> Self::ColScan {
        GenericColumnScan::new(self)
    }
}

impl<'a, T> IntervalColumn<'a, T> for GenericIntervalColumn<T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn int_len(&self) -> usize {
        self.int_starts.len()
    }

    fn int_bounds(&self, int_idx: usize) -> Range<usize> {
        let start_idx = self.int_starts.get(int_idx);
        if int_idx + 1 < self.int_starts.len() {
            start_idx..self.int_starts.get(int_idx + 1)
        } else {
            start_idx..self.data.len()
        }
    }
}

#[cfg(test)]
mod test {
    use super::super::{Column, ColumnEnum, VectorColumn};
    use super::{GenericIntervalColumn, IntervalColumn};
    use test_log::test;

    #[test]
    fn test_u64_column() {
        let data: Vec<u64> = vec![1, 2, 3, 10, 11, 12, 20, 30, 31];
        let int_starts: Vec<usize> = vec![0, 3, 6, 7];

        let v_data = ColumnEnum::VectorColumn(VectorColumn::new(data));
        let v_int_starts = ColumnEnum::VectorColumn(VectorColumn::new(int_starts));
        let gic = GenericIntervalColumn::new(v_data, v_int_starts);
        assert_eq!(gic.len(), 9);
        assert_eq!(gic.get(0), 1);
        assert_eq!(gic.get(1), 2);
        assert_eq!(gic.get(2), 3);
        assert_eq!(gic.get(3), 10);

        assert_eq!(gic.int_len(), 4);
        assert_eq!(gic.int_bounds(0), 0..3);
        assert_eq!(gic.int_bounds(1), 3..6);
        assert_eq!(gic.int_bounds(2), 6..7);
        assert_eq!(gic.int_bounds(3), 7..9);
    }
}
