use super::super::traits::columnscan::{ColumnScan, ColumnScanCell};
use crate::physical::datatypes::ColumnDataType;
use std::fmt::Debug;
use std::ops::Range;

/// [`ColumnScan`] consisting of two sub scans (named main and follower)
/// such that if main advances the follower seeks main's new value
/// Used for computing the minus operation for tries
#[derive(Debug)]
pub struct ColumnScanFollow<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// The value of this sub scan determines the value of the whole scan
    scan_main: &'a ColumnScanCell<'a, T>,

    /// Scan that seeks the current value of `main_scan`
    scan_follower: &'a ColumnScanCell<'a, T>,

    /// Indicates whether `scan_follower` points to the same value as `scan_main`
    equal: bool,

    /// Current value of this scan
    current_value: Option<T>,
}

impl<'a, T> ColumnScanFollow<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Construcs a new [`ColumnScanFollow`].
    pub fn new(
        scan_main: &'a ColumnScanCell<'a, T>,
        scan_follower: &'a ColumnScanCell<'a, T>,
    ) -> ColumnScanFollow<'a, T> {
        ColumnScanFollow {
            scan_main,
            scan_follower,
            equal: true,
            current_value: None,
        }
    }

    /// Return a bool indicating whether both sub scans point to the same value
    pub fn is_equal(&self) -> bool {
        self.equal
    }
}

impl<'a, T> Iterator for ColumnScanFollow<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.current_value = self.scan_main.next();

        if let Some(value) = self.current_value {
            self.scan_follower.seek(value);
        }

        self.equal =
            self.current_value.is_some() && self.current_value == self.scan_follower.current();
        self.current_value
    }
}

impl<'a, T> ColumnScan for ColumnScanFollow<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn seek(&mut self, value: T) -> Option<T> {
        self.current_value = self.scan_main.seek(value);
        self.scan_follower.seek(value);

        self.equal =
            self.current_value.is_some() && self.current_value == self.scan_follower.current();
        self.current_value
    }

    fn current(&mut self) -> Option<T> {
        self.current_value
    }

    fn reset(&mut self) {
        self.equal = true;
        self.current_value = None;
    }

    fn pos(&self) -> Option<usize> {
        unimplemented!("This functions is not implemented for column operators");
    }

    fn narrow(&mut self, _interval: Range<usize>) {
        unimplemented!("This functions is not implemented for column operators");
    }
}

/// [`ColumnScan`] that returns only values that are present in one sub scan ("left") and not in the other ("right")
#[derive(Debug)]
pub struct ColumnScanMinus<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Scan from which the values are subtracted
    scan_left: &'a ColumnScanCell<'a, T>,

    /// Scan containing the values which will be subtracted
    scan_right: &'a ColumnScanCell<'a, T>,

    /// Whether to subtract `scan_right` or not
    enabled: bool,

    /// Current value of this scan
    current_value: Option<T>,
}

impl<'a, T> ColumnScanMinus<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Constructs a new ColumnVectorScan for a Column.
    pub fn new(
        scan_left: &'a ColumnScanCell<'a, T>,
        scan_right: &'a ColumnScanCell<'a, T>,
    ) -> Self {
        Self {
            scan_left,
            scan_right,
            enabled: true,
            current_value: None,
        }
    }

    /// Enabled means that the minus operation is performed;
    /// otherwise this scan acts like a [`PassScan`]
    pub fn minus_enable(&mut self, enabled: bool) {
        self.enabled = enabled;
    }
}

impl<'a, T: Eq + Debug + Copy> Iterator for ColumnScanMinus<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.enabled {
            self.current_value = self.scan_left.next();
            return self.current_value;
        }

        // Call `next` on `scan_left` as long `scan_right` contains the same value
        loop {
            self.current_value = self.scan_left.next();

            if let Some(current_left) = self.current_value {
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

        self.current_value
    }
}

impl<'a, T: Ord + Copy + Debug> ColumnScan for ColumnScanMinus<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn seek(&mut self, value: T) -> Option<T> {
        self.current_value = self.scan_left.seek(value);

        if !self.enabled {
            return self.current_value;
        }

        if let Some(current_left) = self.current_value {
            if let Some(current_right) = self.scan_right.seek(current_left) {
                if current_left == current_right {
                    // If `scan_right` contains the value `scan_left` is set to
                    // then find the next value in `scan_left` that is not in `scan_right`
                    self.next();
                }
            }
        }

        self.current_value
    }

    fn current(&mut self) -> Option<T> {
        self.current_value
    }

    fn reset(&mut self) {
        self.enabled = true;
        self.current_value = None;
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
    use crate::physical::columnar::{
        column_types::vector::ColumnVector,
        traits::{
            column::Column,
            columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum},
        },
    };

    use super::{ColumnScanFollow, ColumnScanMinus};

    use test_log::test;

    #[test]
    fn test_scan_follower() {
        let left_column = ColumnVector::new(vec![0u64, 2, 3, 5, 9, 11, 13, 15, 16, 17]);
        let right_column = ColumnVector::new(vec![0u64, 1, 3, 6, 9, 14, 16]);

        let left_iter = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(left_column.iter()));
        let right_iter = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(right_column.iter()));

        let mut follower_scan = ColumnScanFollow::new(&left_iter, &right_iter);

        assert_eq!(follower_scan.current(), None);
        assert!(follower_scan.is_equal());

        assert_eq!(follower_scan.next(), Some(0));
        assert_eq!(follower_scan.current(), Some(0));
        assert!(follower_scan.is_equal());

        assert_eq!(follower_scan.next(), Some(2));
        assert_eq!(follower_scan.current(), Some(2));
        assert!(!follower_scan.is_equal());

        assert_eq!(follower_scan.next(), Some(3));
        assert_eq!(follower_scan.current(), Some(3));
        assert!(follower_scan.is_equal());

        assert_eq!(follower_scan.next(), Some(5));
        assert_eq!(follower_scan.current(), Some(5));
        assert!(!follower_scan.is_equal());

        assert_eq!(follower_scan.next(), Some(9));
        assert_eq!(follower_scan.current(), Some(9));
        assert!(follower_scan.is_equal());

        assert_eq!(follower_scan.seek(13), Some(13));
        assert_eq!(follower_scan.current(), Some(13));
        assert!(!follower_scan.is_equal());

        assert_eq!(follower_scan.seek(16), Some(16));
        assert_eq!(follower_scan.current(), Some(16));
        assert!(follower_scan.is_equal());

        assert_eq!(follower_scan.next(), Some(17));
        assert_eq!(follower_scan.current(), Some(17));
        assert!(!follower_scan.is_equal());

        assert_eq!(follower_scan.next(), None);
        assert_eq!(follower_scan.current(), None);
        assert!(!follower_scan.is_equal());
    }

    #[test]
    fn test_minus() {
        let left_column = ColumnVector::new(vec![0u64, 2, 3, 5, 6, 8, 10, 11, 12]);
        let right_column = ColumnVector::new(vec![0u64, 3, 7, 11]);

        let left_iter = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(left_column.iter()));
        let right_iter = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(right_column.iter()));

        let mut follower_scan = ColumnScanMinus::new(&left_iter, &right_iter);

        assert_eq!(follower_scan.current(), None);
        assert_eq!(follower_scan.next(), Some(2));
        assert_eq!(follower_scan.current(), Some(2));
        assert_eq!(follower_scan.next(), Some(5));
        assert_eq!(follower_scan.current(), Some(5));
        assert_eq!(follower_scan.seek(8), Some(8));
        assert_eq!(follower_scan.current(), Some(8));
        assert_eq!(follower_scan.seek(11), Some(12));
        assert_eq!(follower_scan.current(), Some(12));
        assert_eq!(follower_scan.next(), None);
        assert_eq!(follower_scan.current(), None);
    }
}
