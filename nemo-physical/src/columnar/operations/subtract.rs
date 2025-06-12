use std::cell::RefCell;
use std::rc::Rc;
use std::{fmt::Debug, ops::Range};

use crate::columnar::columnscan::{ColumnScan, ColumnScanCell};
use crate::datatypes::ColumnDataType;

/// [ColumnScan] that consists of two types of subscans:
///  * a main column scan
///  * a list of follow column scans
///
/// If the main scan moves to some value then the followers will point to value equal or greater than that of main.
/// Some of the followers can be set to "subtract" which means that the main scan will skip all the values in that specific follow scan.
#[derive(Debug)]
pub(crate) struct ColumnScanSubtract<'a, T>
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
    equal_values: Rc<RefCell<Vec<bool>>>,

    /// `enabled[i] == false` means to skip the ith follow scan.
    active_scans: Rc<RefCell<Vec<bool>>>,

    /// Current value of this scan.
    current_value: Option<T>,
}

impl<'a, T> ColumnScanSubtract<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Constructs a new [ColumnScanSubtract].
    pub(crate) fn new(
        scan_main: &'a ColumnScanCell<'a, T>,
        scans_follower: Vec<Option<&'a ColumnScanCell<'a, T>>>,
        subtract_indices: Vec<usize>,
        follow_indices: Vec<usize>,
        active_scans: Rc<RefCell<Vec<bool>>>,
        equal_values: Rc<RefCell<Vec<bool>>>,
    ) -> Self {
        debug_assert!(
            subtract_indices.is_empty()
                || *subtract_indices.iter().max().unwrap() < scans_follower.len()
        );
        debug_assert!(
            follow_indices.is_empty()
                || *follow_indices.iter().max().unwrap() < scans_follower.len()
        );

        Self {
            scan_main,
            scans_follower,
            subtract_indices,
            follow_indices,
            equal_values,
            active_scans,
            current_value: None,
        }
    }

    fn move_follow_scans(&mut self, mut next_value: T) -> Option<T> {
        let mut subtracted_values = true;

        let equal_values = &mut *self.equal_values.borrow_mut();
        equal_values.clone_from(&self.active_scans.borrow());

        while subtracted_values {
            subtracted_values = false;

            for &subtract_index in &self.subtract_indices {
                equal_values[subtract_index] = false;

                if !self.active_scans.borrow()[subtract_index] {
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

        for &follow_index in &self.follow_indices {
            if !self.active_scans.borrow()[follow_index] {
                continue;
            }

            let follow_scan = self.scans_follower[follow_index]
                .expect("This vector should not point to None entries.");

            if let Some(follow_value) = follow_scan.seek(next_value) {
                if next_value != follow_value {
                    equal_values[follow_index] = false;
                }
            } else {
                equal_values[follow_index] = false;
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
        self.current_value = self
            .scan_main
            .next()
            .and_then(|next_value| self.move_follow_scans(next_value));

        self.current_value
    }
}

impl<'a, T: Ord + Copy + Debug> ColumnScan for ColumnScanSubtract<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn seek(&mut self, value: T) -> Option<T> {
        self.current_value = self
            .scan_main
            .seek(value)
            .and_then(|next_value| self.move_follow_scans(next_value));

        self.current_value
    }

    fn current(&self) -> Option<T> {
        self.current_value
    }

    fn reset(&mut self) {
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
    use std::{cell::RefCell, rc::Rc};

    #[cfg(not(miri))]
    use test_log::test;

    use crate::columnar::{
        column::{vector::ColumnVector, Column},
        columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum},
    };

    use super::ColumnScanSubtract;

    #[test]
    fn columnscan_subtract() {
        let active = Rc::new(RefCell::new(vec![true; 3]));
        let equal = Rc::new(RefCell::new(vec![true; 3]));

        let column_main = ColumnVector::new(vec![1u64, 2, 4, 5, 7, 10, 12, 14, 15]);
        let column_subtract = ColumnVector::new(vec![0u64, 2, 4, 8, 9, 12, 17]);
        let column_follow = ColumnVector::new(vec![0u64, 1, 2, 5, 8, 10, 12, 14]);

        let iter_main = ColumnScanCell::new(ColumnScanEnum::Vector(column_main.iter()));
        let iter_subtract = ColumnScanCell::new(ColumnScanEnum::Vector(column_subtract.iter()));
        let iter_follow = ColumnScanCell::new(ColumnScanEnum::Vector(column_follow.iter()));

        let mut subtract_scan = ColumnScanSubtract::new(
            &iter_main,
            vec![Some(&iter_subtract), None, Some(&iter_follow)],
            vec![0],
            vec![2],
            active,
            equal,
        );

        assert!(subtract_scan.current().is_none());
        assert_eq!(*subtract_scan.equal_values.borrow(), vec![true, true, true]);

        assert_eq!(subtract_scan.next(), Some(1));
        assert_eq!(subtract_scan.current(), Some(1));
        assert_eq!(
            *subtract_scan.equal_values.borrow(),
            vec![false, true, true]
        );

        assert_eq!(subtract_scan.next(), Some(5));
        assert_eq!(subtract_scan.current(), Some(5));
        assert_eq!(
            *subtract_scan.equal_values.borrow(),
            vec![false, true, true]
        );

        assert_eq!(subtract_scan.next(), Some(7));
        assert_eq!(subtract_scan.current(), Some(7));
        assert_eq!(
            *subtract_scan.equal_values.borrow(),
            vec![false, true, false]
        );

        assert_eq!(subtract_scan.seek(11), Some(14));
        assert_eq!(subtract_scan.current(), Some(14));
        assert_eq!(
            *subtract_scan.equal_values.borrow(),
            vec![false, true, true]
        );

        assert_eq!(subtract_scan.next(), Some(15));
        assert_eq!(subtract_scan.current(), Some(15));
        assert_eq!(
            *subtract_scan.equal_values.borrow(),
            vec![false, true, false]
        );

        assert_eq!(subtract_scan.next(), None);
        assert_eq!(subtract_scan.current(), None);
        assert_eq!(
            *subtract_scan.equal_values.borrow(),
            vec![false, true, false]
        );
    }
}
