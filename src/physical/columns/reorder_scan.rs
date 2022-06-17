use super::{Column, ColumnEnum, ColumnScan, RangedColumnScan};
use crate::logical::Permutator;
use crate::physical::datatypes::{Field, FloorToUsize};
use std::fmt::Debug;
use std::ops::Range;

// TODO: Maybe this should only have Optional<Permutator>
//       for cases where data happens to be sorted already
/// Scan which reorders its underlying Column according to a permutator
#[derive(Debug)]
pub struct ReorderScan<'a, T> {
    column: &'a ColumnEnum<T>,
    permutator: Permutator,
    current_value: Option<T>,
    current_index: Option<usize>,
    range: Range<usize>,
}

impl<'a, T> ReorderScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    /// Construct a new ReorderScan for a Column.
    pub fn new(column: &'a ColumnEnum<T>) -> Self {
        ReorderScan::narrowed(column, 0..column.len())
    }

    /// Construct a new ReorderedScan for a Column restricted to given range.
    pub fn narrowed(column: &'a ColumnEnum<T>, range: Range<usize>) -> Self {
        Self {
            column,
            permutator: Permutator::sort_from_column_range(column, &range),
            current_value: None,
            current_index: None,
            range,
        }
    }
}

impl<'a, T: Eq + Debug + Copy> Iterator for ReorderScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let next_index = self.current_index.map_or(0, |i| i + 1);
        if next_index >= self.range.end - self.range.start {
            self.current_value = None;
            return None;
        }

        self.current_index = Some(next_index);

        self.current_value = Some(self.column.get(self.permutator.get_sort_vec()[next_index]));
        self.current_value
    }
}

impl<'a, T: Ord + Copy + Debug> ColumnScan for ReorderScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn seek(&mut self, value: T) -> Option<T> {
        if self.current_value? >= value {
            return self.current_value;
        }
        while value > self.next()? {}

        self.current_value
    }

    fn current(&mut self) -> Option<T> {
        self.current_value
    }

    fn reset(&mut self) {
        self.current_index = None;
        self.current_value = None;
    }
}

impl<'a, T: Ord + Copy + Debug> RangedColumnScan for ReorderScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn pos(&self) -> Option<usize> {
        Some(self.permutator.get_sort_vec()[self.current_index?])
    }
    fn narrow(&mut self, interval: Range<usize>) {
        self.range = interval;
        self.permutator = Permutator::sort_from_column_range(self.column, &self.range);
        self.reset();
    }
}

#[cfg(test)]
mod test {
    use super::ReorderScan;
    use crate::physical::columns::{ColumnEnum, ColumnScan, VectorColumn};
    use test_log::test;

    #[test]
    fn test_u64() {
        let values: Vec<u64> = vec![0, 2, 1, 7, 4, 9, 12, 8, 4, 7, 4, 14];
        let column = ColumnEnum::VectorColumn(VectorColumn::new(values));

        let mut scan = ReorderScan::narrowed(&column, 3..9);

        assert_eq!(scan.current(), None);
        assert_eq!(scan.next(), Some(4));
        assert_eq!(scan.current(), Some(4));
        assert_eq!(scan.next(), Some(4));
        assert_eq!(scan.current(), Some(4));
        assert_eq!(scan.next(), Some(7));
        assert_eq!(scan.current(), Some(7));
        assert_eq!(scan.seek(11), Some(12));
        assert_eq!(scan.current(), Some(12));
        assert_eq!(scan.next(), None);
        assert_eq!(scan.current(), None);
    }
}
