use super::super::traits::{
    column::{Column, ColumnEnum},
    columnscan::{ColumnScan, ColumnScanEnum},
};
use crate::physical::datatypes::ColumnDataType;
use std::fmt::Debug;
use std::ops::Range;


/// Scan which can operate on multiple ordered ranges of its underlying [`ColumnEnum`] 
#[derive(Debug)]
pub struct ColumnScanMultipleRanges<'a, T>
where
    T: 'a + ColumnDataType,
{
    column: &'a ColumnEnum<T>,
    column_scans: Vec<ColumnScanEnum<'a, T>>,
    current_value: Option<T>,
    current_positions: Option<Vec<usize>>,
}

impl<'a, T> ColumnScanMultipleRanges<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Construct a new [`ColumnScanMultipleRanges`].
    pub fn new(column: &'a ColumnEnum<T>, ranges: Vec<Range<usize>>) -> Self {
        let column_scans = ranges.iter()
            .map(|range| {
                let mut scan = column.iter();
                scan.narrow(range.clone());
                scan
            }).collect();

        Self {
            column,
            column_scans,
            current_value: None,
            current_positions: None
        }
    }

    /// Return all positions in the underlying column the cursor is currently at
    /// (The cursor can be at mutliple positions since there may be duplicates)
    pub fn pos_multiple(&self) -> Option<Vec<usize>> {
        self.current_value?;
        self.current_positions.clone()
    }

    /// Set iterator to a set of possibly disjoint ranged
    pub fn narrow_ranges(&mut self, ranges: Vec<Range<usize>>) {
        self.reset();
        self.column_scans = ranges.iter()
            .map(|range| {
                let mut scan = self.column.iter();
                scan.narrow(range.clone());
                scan
            }).collect();
    }
}

impl<'a, T: Eq + Debug + Copy> Iterator for ColumnScanMultipleRanges<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.column_scans.iter_mut()
            .for_each(|scan| if scan.current() == self.current_value {
                scan.next();
            });

        self.current_value = self.column_scans.iter()
            .filter(|scan| scan.current().is_some())
            .map(|scan| scan.current().unwrap())
            .min();

        self.current_positions = self.column_scans.iter()
            .filter(|scan| scan.current() == self.current_value)
            .map(|scan| scan.pos())
            .collect();

        log::debug!("{self:#?}");
        self.current_value
    }
}

impl<'a, T: Ord + Copy + Debug> ColumnScan for ColumnScanMultipleRanges<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn seek(&mut self, value: T) -> Option<T> {
        if self.current_value.is_some() && self.current_value.unwrap() >= value {
            return self.current_value;
        }

        self.current_value = self.column_scans.iter_mut()
            .map(|scan| scan.seek(value))
            .filter(|value| value.is_some())
            .map(|value| value.unwrap())
            .min();

        self.current_positions = self.column_scans.iter()
            .filter(|scan| scan.current() == self.current_value)
            .map(|scan| scan.pos())
            .collect();
        
        self.current_value
    }

    fn current(&self) -> Option<T> {
        self.current_value
    }

    fn reset(&mut self) {
        self.current_value = None;
        self.current_positions = None;
        self.column_scans.iter_mut().for_each(|scan| scan.reset());
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
    use crate::physical::columnar::{
        column_types::vector::ColumnVector,
        traits::{column::ColumnEnum, columnscan::ColumnScan},
    };

    use super::ColumnScanMultipleRanges;
    use test_log::test;

    #[test]
    fn test_single_range() {
        let values: Vec<u64> = vec![0, 2, 3, 5, 7, 9, 10, 11, 13, 15];
        let column = ColumnEnum::ColumnVector(ColumnVector::new(values));

        let mut scan = ColumnScanMultipleRanges::new(&column, vec![3..9]);

        assert_eq!(scan.current(), None);
        assert_eq!(scan.pos_multiple(), None);

        assert_eq!(scan.next(), Some(5));
        assert_eq!(scan.current(), Some(5));
        assert_eq!(scan.pos_multiple(), Some(vec![3]));

        assert_eq!(scan.next(), Some(7));
        assert_eq!(scan.current(), Some(7));
        assert_eq!(scan.pos_multiple(), Some(vec![4]));

        assert_eq!(scan.seek(10), Some(10));
        assert_eq!(scan.current(), Some(10));
        assert_eq!(scan.pos_multiple(), Some(vec![6]));

        assert_eq!(scan.seek(12), Some(13));
        assert_eq!(scan.current(), Some(13));
        assert_eq!(scan.pos_multiple(), Some(vec![8]));

        assert_eq!(scan.next(), None);
        assert_eq!(scan.current(), None);
        assert_eq!(scan.pos_multiple(), None);
    }

    #[test]
    fn test_multiple_ranges() {
        let values: Vec<u64> = vec![0, 1, 2, 7, 9, 8, 12, 4, 7, 10, 14];
        let column = ColumnEnum::ColumnVector(ColumnVector::new(values));

        let mut scan = ColumnScanMultipleRanges::new(&column, vec![1..5, 7..11]);

        assert_eq!(scan.current(), None);
        assert_eq!(scan.pos_multiple(), None);

        assert_eq!(scan.next(), Some(1));
        assert_eq!(scan.current(), Some(1));
        assert_eq!(scan.pos_multiple(), Some(vec![1]));

        assert_eq!(scan.next(), Some(2));
        assert_eq!(scan.current(), Some(2));
        assert_eq!(scan.pos_multiple(), Some(vec![2]));

        assert_eq!(scan.next(), Some(4));
        assert_eq!(scan.current(), Some(4));
        assert_eq!(scan.pos_multiple(), Some(vec![7]));

        assert_eq!(scan.next(), Some(7));
        assert_eq!(scan.current(), Some(7));
        assert_eq!(scan.pos_multiple(), Some(vec![3, 8]));

        assert_eq!(scan.next(), Some(9));
        assert_eq!(scan.current(), Some(9));
        assert_eq!(scan.pos_multiple(), Some(vec![4]));

        assert_eq!(scan.seek(11), Some(14));
        assert_eq!(scan.current(), Some(14));
        assert_eq!(scan.pos_multiple(), Some(vec![10]));
        
        assert_eq!(scan.next(), None);
        assert_eq!(scan.current(), None);
        assert_eq!(scan.pos_multiple(), None);
    }

    #[test]
    fn test_reset() {
        let values: Vec<u64> = vec![0, 1, 2, 7, 9, 8, 12, 4, 7, 10, 14];
        let column = ColumnEnum::ColumnVector(ColumnVector::new(values));

        let mut scan = ColumnScanMultipleRanges::new(&column, vec![1..5, 7..11]);

        assert_eq!(scan.seek(8), Some(9));
        assert_eq!(scan.current(), Some(9));
        assert_eq!(scan.pos_multiple(), Some(vec![4]));

        scan.reset();
        
        assert_eq!(scan.current(), None);
        assert_eq!(scan.pos_multiple(), None);

        assert_eq!(scan.next(), Some(1));
        assert_eq!(scan.current(), Some(1));
        assert_eq!(scan.pos_multiple(), Some(vec![1]));
    }
}
