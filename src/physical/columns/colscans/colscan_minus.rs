use crate::physical::datatypes::ColumnDataType;
use std::fmt::Debug;
use std::ops::Range;

use super::colscan::ColScan;
use super::ColScanCell;

/// Iterator used in the upper levels of the trie difference operator
#[derive(Debug)]
pub struct ColScanFollow<'a, T>
where
    T: 'a + ColumnDataType,
{
    scan_left: &'a ColScanCell<'a, T>,
    scan_right: &'a ColScanCell<'a, T>,
    equal: bool,
    current: Option<T>,
}

impl<'a, T> ColScanFollow<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Constructs a new ColumnVectorScan for a Column.
    pub fn new(
        scan_left: &'a ColScanCell<'a, T>,
        scan_right: &'a ColScanCell<'a, T>,
    ) -> ColScanFollow<'a, T> {
        ColScanFollow {
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

impl<'a, T> Iterator for ColScanFollow<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.current = self.scan_left.next();

        if let Some(value) = self.current {
            if self.equal {
                self.scan_right.seek(value);
            }
        }

        self.equal = self.current.is_some() && self.current == self.scan_right.current();
        self.current
    }
}

impl<'a, T> ColScan for ColScanFollow<'a, T>
where
    T: 'a + ColumnDataType,
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
        self.equal = true;
        self.current = None;
    }

    fn pos(&self) -> Option<usize> {
        unimplemented!("This functions is not implemented for column operators");
    }

    fn narrow(&mut self, _interval: Range<usize>) {
        unimplemented!("This functions is not implemented for column operators");
    }
}

/// Iterator which computes the set difference between two scans
#[derive(Debug)]
pub struct ColScanMinus<'a, T>
where
    T: 'a + ColumnDataType,
{
    scan_left: &'a ColScanCell<'a, T>,
    scan_right: &'a ColScanCell<'a, T>,
    current: Option<T>,
}

impl<'a, T> ColScanMinus<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Constructs a new ColumnVectorScan for a Column.
    pub fn new(scan_left: &'a ColScanCell<'a, T>, scan_right: &'a ColScanCell<'a, T>) -> Self {
        Self {
            scan_left,
            scan_right,
            current: None,
        }
    }
}

impl<'a, T: Eq + Debug + Copy> Iterator for ColScanMinus<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.current = self.scan_left.next();

            if let Some(current_left) = self.current {
                if let Some(current_right) = self.scan_right.seek(current_left) {
                    if current_left != current_right {
                        break;
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        self.current
    }
}

impl<'a, T: Ord + Copy + Debug> ColScan for ColScanMinus<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn seek(&mut self, value: T) -> Option<T> {
        self.current = self.scan_left.seek(value);

        if let Some(current_left) = self.current {
            if let Some(current_right) = self.scan_right.seek(current_left) {
                if current_left == current_right {
                    self.next();
                }
            }
        }

        self.current
    }

    fn current(&mut self) -> Option<T> {
        self.current
    }

    fn reset(&mut self) {
        self.current = None;
    }

    fn pos(&self) -> Option<usize> {
        unimplemented!("This functions is not implemented for column operators");
    }
    fn narrow(&mut self, _interval: Range<usize>) {
        unimplemented!("This functions is not implemented for column operators");
    }
}

#[cfg(test)]
mod test {
    use crate::physical::columns::{
        colscans::{ColScan, ColScanCell, ColScanEnum, ColScanGenericEnum},
        columns::{Column, ColumnVector},
    };

    use super::{ColScanFollow, ColScanMinus};

    use test_log::test;

    #[test]
    fn test_difference_scan() {
        let left_column = ColumnVector::new(vec![0u64, 2, 3, 5, 6, 8, 10, 11, 12]);
        let right_column = ColumnVector::new(vec![0u64, 1, 3, 7, 11]);

        let left_iter = ColScanCell::new(ColScanEnum::ColScanGeneric(
            ColScanGenericEnum::ColumnVector(left_column.iter()),
        ));
        let right_iter = ColScanCell::new(ColScanEnum::ColScanGeneric(
            ColScanGenericEnum::ColumnVector(right_column.iter()),
        ));

        let mut diff_scan = ColScanFollow::new(&left_iter, &right_iter);

        assert_eq!(diff_scan.current(), None);
        assert!(diff_scan.is_equal());

        assert_eq!(diff_scan.next(), Some(0));
        assert_eq!(diff_scan.current(), Some(0));
        assert!(diff_scan.is_equal());

        assert_eq!(diff_scan.next(), Some(2));
        assert_eq!(diff_scan.current(), Some(2));
        assert!(!diff_scan.is_equal());

        assert_eq!(diff_scan.next(), Some(3));
        assert_eq!(diff_scan.current(), Some(3));
        assert!(diff_scan.is_equal());

        assert_eq!(diff_scan.next(), Some(5));
        assert_eq!(diff_scan.current(), Some(5));
        assert!(!diff_scan.is_equal());

        assert_eq!(diff_scan.seek(8), Some(8));
        assert_eq!(diff_scan.current(), Some(8));
        assert!(!diff_scan.is_equal());

        assert_eq!(diff_scan.seek(11), Some(11));
        assert_eq!(diff_scan.current(), Some(11));
        assert!(diff_scan.is_equal());

        assert_eq!(diff_scan.next(), Some(12));
        assert_eq!(diff_scan.current(), Some(12));
        assert!(!diff_scan.is_equal());

        assert_eq!(diff_scan.next(), None);
        assert_eq!(diff_scan.current(), None);
        assert!(!diff_scan.is_equal());
    }

    #[test]
    fn test_minus() {
        let left_column = ColumnVector::new(vec![0u64, 2, 3, 5, 6, 8, 10, 11, 12]);
        let right_column = ColumnVector::new(vec![0u64, 3, 7, 11]);

        let left_iter = ColScanCell::new(ColScanEnum::ColScanGeneric(
            ColScanGenericEnum::ColumnVector(left_column.iter()),
        ));
        let right_iter = ColScanCell::new(ColScanEnum::ColScanGeneric(
            ColScanGenericEnum::ColumnVector(right_column.iter()),
        ));

        let mut diff_scan = ColScanMinus::new(&left_iter, &right_iter);

        assert_eq!(diff_scan.current(), None);
        assert_eq!(diff_scan.next(), Some(2));
        assert_eq!(diff_scan.current(), Some(2));
        assert_eq!(diff_scan.next(), Some(5));
        assert_eq!(diff_scan.current(), Some(5));
        assert_eq!(diff_scan.seek(8), Some(8));
        assert_eq!(diff_scan.current(), Some(8));
        assert_eq!(diff_scan.seek(11), Some(12));
        assert_eq!(diff_scan.current(), Some(12));
        assert_eq!(diff_scan.next(), None);
        assert_eq!(diff_scan.current(), None);
    }
}
