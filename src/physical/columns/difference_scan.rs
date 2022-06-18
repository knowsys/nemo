use super::{ColumnScan, RangedColumnScan, RangedColumnScanCell};
use crate::physical::datatypes::{Field, FloorToUsize};
use std::fmt::Debug;
use std::ops::Range;

/// Iterator for the set difference of two iterators
#[derive(Debug)]
pub struct DifferenceScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    scan_left: &'a RangedColumnScanCell<'a, T>,
    scan_right: &'a RangedColumnScanCell<'a, T>,
    equal: bool,
    current: Option<T>,
}

impl<'a, T> DifferenceScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    /// Constructs a new VectorColumnScan for a Column.
    pub fn new(
        scan_left: &'a RangedColumnScanCell<'a, T>,
        scan_right: &'a RangedColumnScanCell<'a, T>,
    ) -> DifferenceScan<'a, T> {
        DifferenceScan {
            scan_left,
            scan_right,
            equal: true,
            current: None,
        }
    }

    /// Returns a bool indicating whether scan_left and scan_right point to the same value
    pub fn is_equal(&self) -> bool {
        self.equal
    }
}

impl<'a, T> Iterator for DifferenceScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.current = self.scan_left.next();

        if self.equal {
            self.scan_right.next();
        }

        self.equal = self.current.is_some() && self.current == self.scan_right.current();
        self.current
    }
}

impl<'a, T> ColumnScan for DifferenceScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn seek(&mut self, value: T) -> Option<T> {
        self.current = self.scan_left.seek(value);
        self.scan_right.seek(value);

        self.equal = self.current.is_some() && self.current == self.scan_right.current();
        self.current
    }

    fn current(&mut self) -> Option<T> {
        self.current
    }

    fn reset(&mut self) {
        self.equal = false;
        self.current = None;
    }
}

impl<'a, T> RangedColumnScan for DifferenceScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn pos(&self) -> Option<usize> {
        unimplemented!(
            "This function only exists because RangedColumnScans cannnot be ColumnScans"
        );
    }
    fn narrow(&mut self, _interval: Range<usize>) {
        unimplemented!(
            "This function only exists because RangedColumnScans cannnot be ColumnScans"
        );
    }
}

#[cfg(test)]
mod test {
    use super::DifferenceScan;
    use crate::physical::columns::{Column, ColumnScan, VectorColumn};
    use test_log::test;

    #[test]
    fn test_u64() {
        let left_column = VectorColumn::new(vec![0u64, 2, 3, 5, 6, 8, 10, 11, 12]);
        let right_column = VectorColumn::new(vec![0u64, 3, 7, 11]);

        let mut left_iter = left_column.iter();
        let mut right_iter = right_column.iter();

        let mut diff_scan = DifferenceScan::new(&mut left_iter, &mut right_iter);

        assert_eq!(diff_scan.current(), None);
        assert_eq!(diff_scan.is_equal(), true);
        assert_eq!(diff_scan.next(), Some(0));
        assert_eq!(diff_scan.current(), Some(0));
        assert_eq!(diff_scan.is_equal(), true);
        assert_eq!(diff_scan.next(), Some(2));
        assert_eq!(diff_scan.current(), Some(2));
        assert_eq!(diff_scan.is_equal(), false);
        assert_eq!(diff_scan.next(), Some(3));
        assert_eq!(diff_scan.current(), Some(3));
        assert_eq!(diff_scan.is_equal(), true);
        assert_eq!(diff_scan.next(), Some(5));
        assert_eq!(diff_scan.current(), Some(5));
        assert_eq!(diff_scan.is_equal(), false);
        assert_eq!(diff_scan.seek(8), Some(8));
        assert_eq!(diff_scan.current(), Some(8));
        assert_eq!(diff_scan.is_equal(), false);
        assert_eq!(diff_scan.seek(11), Some(11));
        assert_eq!(diff_scan.current(), Some(11));
        assert_eq!(diff_scan.is_equal(), true);
        assert_eq!(diff_scan.next(), Some(12));
        assert_eq!(diff_scan.current(), Some(12));
        assert_eq!(diff_scan.is_equal(), false);
        assert_eq!(diff_scan.next(), None);
        assert_eq!(diff_scan.current(), None);
        assert_eq!(diff_scan.is_equal(), false);
    }
}
