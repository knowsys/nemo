use crate::logical::Permutator;
use crate::physical::columns::columns::{Column, ColumnEnum};
use crate::physical::datatypes::ColumnDataType;
use std::fmt::Debug;
use std::ops::Range;

use super::colscan::ColScan;

// TODO: Maybe this should only have Optional<Permutator>
//       for cases where data happens to be sorted already
/// Scan which reorders its underlying [`ColumnEnum`] according to a permutator
#[derive(Debug)]
pub struct ColScanReorder<'a, T> {
    column: &'a ColumnEnum<T>,
    permutator: Permutator,
    current_value: Option<T>,
    current_index: Option<usize>,
    same_value_count: usize,
    ranges: Vec<Range<usize>>,
}

impl<'a, T> ColScanReorder<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Construct a new ColScanReorder for a Column.
    pub fn new(column: &'a ColumnEnum<T>) -> Self {
        ColScanReorder::narrowed(column, vec![0..column.len()])
    }

    /// Construct a new ReorderedScan for a Column restricted to given range.
    pub fn narrowed(column: &'a ColumnEnum<T>, ranges: Vec<Range<usize>>) -> Self {
        Self {
            column,
            permutator: Permutator::sort_from_column_range(column, &ranges),
            current_value: None,
            current_index: None,
            same_value_count: 0,
            ranges,
        }
    }

    /// Return all positions in the underlying column the cursor is currently at
    /// (The cursor can be at mutliple positions since there may be duplicates)
    pub fn pos_multiple(&self) -> Option<Vec<usize>> {
        self.current_value?;

        let mut result = Vec::<usize>::with_capacity(self.same_value_count);
        for same_index in 0..self.same_value_count {
            let sort_index = self.current_index? + same_index;
            result.push(self.permutator.get_sort_vec()[sort_index]);
        }

        Some(result)
    }

    /// Set iterator to a set of possibly disjoint ranged
    pub fn narrow_ranges(&mut self, intervals: Vec<Range<usize>>) {
        self.ranges = intervals;
        self.permutator = Permutator::sort_from_column_range(self.column, &self.ranges);
        self.reset();
    }
}

impl<'a, T: Eq + Debug + Copy> Iterator for ColScanReorder<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let next_index = self.current_index.map_or(0, |i| i + self.same_value_count);
        if next_index >= self.permutator.get_sort_vec().len() {
            self.current_value = None;
            return None;
        }

        self.current_index = Some(next_index);

        let next_value = self.column.get(self.permutator.get_sort_vec()[next_index]);
        self.same_value_count = 1;
        for peek_index in (next_index + 1)..self.permutator.get_sort_vec().len() {
            let peeked_value = self.column.get(self.permutator.get_sort_vec()[peek_index]);
            if next_value == peeked_value {
                self.same_value_count += 1;
            } else {
                break;
            }
        }

        self.current_value = Some(next_value);
        log::debug!("{self:#?}");
        self.current_value
    }
}

impl<'a, T: Ord + Copy + Debug> ColScan for ColScanReorder<'a, T>
where
    T: 'a + ColumnDataType,
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
        self.same_value_count = 0;
    }

    fn pos(&self) -> Option<usize> {
        unimplemented!("Because of possible duplicates, use pos_multiple");
    }
    fn narrow(&mut self, _interval: Range<usize>) {
        unimplemented!("Because of possible duplicates, use narrow_ranges");
    }
}

#[cfg(test)]
mod test {
    use crate::physical::columns::{
        colscans::ColScan,
        columns::{ColumnEnum, ColumnVector},
    };

    use super::ColScanReorder;
    use test_log::test;

    #[test]
    fn test_single_range() {
        let values: Vec<u64> = vec![0, 2, 1, 7, 4, 9, 12, 8, 4, 7, 4, 14];
        let column = ColumnEnum::ColumnVector(ColumnVector::new(values));

        let mut scan = ColScanReorder::narrowed(&column, vec![3..9]);

        assert_eq!(scan.current(), None);
        assert_eq!(scan.pos_multiple(), None);
        assert_eq!(scan.next(), Some(4));
        assert_eq!(scan.current(), Some(4));
        assert_eq!(scan.pos_multiple(), Some(vec![4, 8]));
        assert_eq!(scan.next(), Some(7));
        assert_eq!(scan.current(), Some(7));
        assert_eq!(scan.pos_multiple(), Some(vec![3]));
        assert_eq!(scan.seek(11), Some(12));
        assert_eq!(scan.current(), Some(12));
        assert_eq!(scan.pos_multiple(), Some(vec![6]));
        assert_eq!(scan.next(), None);
        assert_eq!(scan.current(), None);
        assert_eq!(scan.pos_multiple(), None);
    }

    #[test]
    fn test_multiple_ranges() {
        let values: Vec<u64> = vec![0, 2, 1, 7, 4, 9, 12, 8, 4, 7, 4, 14];
        let column = ColumnEnum::ColumnVector(ColumnVector::new(values));

        let mut scan = ColScanReorder::narrowed(&column, vec![1..5, 7..11]);

        assert_eq!(scan.current(), None);
        assert_eq!(scan.pos_multiple(), None);
        assert_eq!(scan.next(), Some(1));
        assert_eq!(scan.current(), Some(1));
        assert_eq!(scan.pos_multiple(), Some(vec![2]));
        assert_eq!(scan.next(), Some(2));
        assert_eq!(scan.current(), Some(2));
        assert_eq!(scan.pos_multiple(), Some(vec![1]));
        assert_eq!(scan.next(), Some(4));
        assert_eq!(scan.current(), Some(4));
        assert_eq!(scan.pos_multiple(), Some(vec![4, 8, 10]));
        assert_eq!(scan.next(), Some(7));
        assert_eq!(scan.current(), Some(7));
        assert_eq!(scan.pos_multiple(), Some(vec![3, 9]));
        assert_eq!(scan.next(), Some(8));
        assert_eq!(scan.current(), Some(8));
        assert_eq!(scan.pos_multiple(), Some(vec![7]));
        assert_eq!(scan.next(), None);
        assert_eq!(scan.current(), None);
        assert_eq!(scan.pos_multiple(), None);
    }
}
