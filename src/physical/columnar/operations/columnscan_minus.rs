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

    fn current(&self) -> Option<T> {
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
    /// Constructs a new [`ColumnScanMinus`] for a Column.
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
    /// otherwise this scan acts like a [`ColumnScanPass`]
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

    fn current(&self) -> Option<T> {
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

/// [`ColumnScan`] that consists of two types of subscans:
///  * a main column scan
///  * a list of follow column scans
/// If the main scan moves to some value then the followers will point to value equal or greater than that of main.
/// Some of the followers can be set to "subtract" which means that the main scan will skip all the values in that specific follow scan.
#[derive(Debug)]
pub struct ColumnScanSubtract<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// The value of this sub scan determines the value of the whole scan
    scan_main: &'a ColumnScanCell<'a, T>,
    /// Scans that always point to a value greater or equal to `main_scan`
    scans_follower: Vec<Option<&'a ColumnScanCell<'a, T>>>,

    /// Vector of indices into `scans_follower` such that
    /// values from `scans_follower[i]` are subtracted from `scan_main`
    /// if i appears in this vector.
    subtract_indices: Vec<usize>,

    /// Vector of indices in `scans_follower` of column scans
    /// which should just "follow" the main column scan without influencing its values.
    follow_indices: Vec<usize>,

    /// Whether the ith follow scan points to the same value as `scan_main`.
    equal_values: Vec<bool>,

    /// `enabled[i] == false` means to skip the ith follow scan.
    enabled: Vec<bool>,

    /// Current value of this scan.
    current_value: Option<T>,
}

impl<'a, T> ColumnScanSubtract<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Constructs a new [`ColumnScanSubtract`].
    pub fn new(
        scan_main: &'a ColumnScanCell<'a, T>,
        scans_follower: Vec<Option<&'a ColumnScanCell<'a, T>>>,
        subtract_indices: Vec<usize>,
        follow_indices: Vec<usize>,
    ) -> Self {
        debug_assert!(!subtract_indices.is_empty() || !follow_indices.is_empty());
        debug_assert!(
            subtract_indices.is_empty()
                || *subtract_indices.iter().max().unwrap() < scans_follower.len()
        );
        debug_assert!(
            follow_indices.is_empty()
                || *follow_indices.iter().max().unwrap() < scans_follower.len()
        );

        let follower_count = scans_follower.len();

        Self {
            scan_main,
            scans_follower,
            subtract_indices,
            follow_indices,
            equal_values: vec![true; follower_count],
            enabled: vec![true; follower_count],
            current_value: None,
        }
    }

    /// Return a vector where the ith value indicates
    /// whether the ith "follow" scan points to the same value as the main scan.
    pub fn equal_values(&self) -> &Vec<bool> {
        &self.equal_values
    }

    /// Set which sub iterators should be enabled.
    pub fn enable(&mut self, enabled: &[bool]) {
        self.enabled = enabled.to_vec();
    }

    fn move_follow_scans(&mut self, mut next_value: T) -> Option<T> {
        let mut subtracted_values = true;

        while subtracted_values {
            subtracted_values = false;

            for &subtract_index in &self.subtract_indices {
                if !self.enabled[subtract_index] {
                    continue;
                }

                let subtract_scan = self.scans_follower[subtract_index]
                    .expect("This vector should not point to None entries.");

                if let Some(subtract_value) = subtract_scan.seek(next_value) {
                    if next_value == subtract_value {
                        next_value = self.scan_main.next()?;
                        subtracted_values = true;
                    }
                }
            }
        }

        self.equal_values.fill(false);
        for &follow_index in &self.follow_indices {
            if !self.enabled[follow_index] {
                continue;
            }

            let follow_scan = self.scans_follower[follow_index]
                .expect("This vector should not point to None entries.");

            if let Some(follow_value) = follow_scan.seek(next_value) {
                if next_value == follow_value {
                    self.equal_values[follow_index] = true;
                }
            }
        }

        Some(next_value)
    }
}

impl<'a, T> Iterator for ColumnScanSubtract<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.current_value = if let Some(next_value) = self.scan_main.next() {
            self.move_follow_scans(next_value)
        } else {
            None
        };

        self.current_value
    }
}

impl<'a, T: Ord + Copy + Debug> ColumnScan for ColumnScanSubtract<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn seek(&mut self, value: T) -> Option<T> {
        self.current_value = if let Some(next_value) = self.scan_main.seek(value) {
            self.move_follow_scans(next_value)
        } else {
            None
        };

        self.current_value
    }

    fn current(&self) -> Option<T> {
        self.current_value
    }

    fn reset(&mut self) {
        self.equal_values.fill(true);
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
        operations::ColumnScanSubtract,
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

    #[test]
    fn test_subtract() {
        let column_main = ColumnVector::new(vec![1u64, 2, 4, 5, 7, 10, 12, 14, 15]);
        let column_subtract = ColumnVector::new(vec![0u64, 2, 4, 8, 9, 12, 17]);
        let column_follow = ColumnVector::new(vec![0u64, 1, 2, 5, 8, 10, 12, 14]);

        let iter_main = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(column_main.iter()));
        let iter_subtract =
            ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(column_subtract.iter()));
        let iter_follow =
            ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(column_follow.iter()));

        let mut subtract_scan = ColumnScanSubtract::new(
            &iter_main,
            vec![Some(&iter_subtract), None, Some(&iter_follow)],
            vec![0],
            vec![2],
        );

        assert!(subtract_scan.current().is_none());
        assert_eq!(subtract_scan.equal_values(), &[true, true, true]);

        assert_eq!(subtract_scan.next(), Some(1));
        assert_eq!(subtract_scan.current(), Some(1));
        assert_eq!(subtract_scan.equal_values(), &[false, false, true]);

        assert_eq!(subtract_scan.next(), Some(5));
        assert_eq!(subtract_scan.current(), Some(5));
        assert_eq!(subtract_scan.equal_values(), &[false, false, true]);

        assert_eq!(subtract_scan.next(), Some(7));
        assert_eq!(subtract_scan.current(), Some(7));
        assert_eq!(subtract_scan.equal_values(), &[false, false, false]);

        assert_eq!(subtract_scan.seek(11), Some(14));
        assert_eq!(subtract_scan.current(), Some(14));
        assert_eq!(subtract_scan.equal_values(), &[false, false, true]);

        assert_eq!(subtract_scan.next(), Some(15));
        assert_eq!(subtract_scan.current(), Some(15));
        assert_eq!(subtract_scan.equal_values(), &[false, false, false]);

        assert_eq!(subtract_scan.next(), None);
        assert_eq!(subtract_scan.current(), None);
        assert_eq!(subtract_scan.equal_values(), &[false, false, false]);
    }
}
