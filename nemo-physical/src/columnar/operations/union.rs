//! This module defines [ColumnScanUnion].

use std::{fmt::Debug, ops::Range};

use crate::{
    columnar::columnscan::{ColumnScan, ColumnScanCell},
    datatypes::ColumnDataType,
};

/// [ColumnScan] representing the union of its sub scans
#[derive(Debug)]
pub(crate) struct ColumnScanUnion<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Sub scans of which the union is computed
    column_scans: Vec<&'a ColumnScanCell<'a, T>>,

    /// Marks which scans in `column_scans` are active
    ///
    /// Non-active scans are ignored during the computation of the union.
    active_scans: Vec<bool>,

    /// Marks which of the active scans in `column_scans` currently point to the smallest value
    ///
    /// `smallest_scans[i]` indicates whether the ith scan points to the smallest element.
    smallest_scans: Vec<bool>,
    /// Smallest value pointed to by the sub scans
    smallest_value: Option<T>,
}

impl<'a, T> ColumnScanUnion<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Constructs a new [ColumnScanUnion].
    pub(crate) fn new(column_scans: Vec<&'a ColumnScanCell<'a, T>>) -> ColumnScanUnion<'a, T> {
        let scans_len = column_scans.len();
        ColumnScanUnion {
            column_scans,
            smallest_scans: vec![true; scans_len],
            smallest_value: None,
            active_scans: vec![true; scans_len],
        }
    }

    /// Returns a vector containing the indices of those scans which point to the currently smallest values
    pub fn get_smallest_scans(&self) -> Vec<bool> {
        self.smallest_scans.clone()
    }

    /// Set a vector that indicates which scans are currently active and should be considered
    pub fn set_active_scans(&mut self, active_scans: Vec<bool>) {
        self.active_scans = active_scans;
    }

    /// Return the initial state of `self.smallest_scans`,
    /// which is a bit vector containing only zeros.
    fn smallest_scans_default(&self) -> Vec<bool> {
        vec![false; self.smallest_scans.len()]
    }

    /// For every iteration over all input `column_scan`,
    /// computes `self.smallest_value` and `self.smallest_scans`
    fn update_smallest(
        smallest_value: &mut Option<T>,
        smallest_scans: &mut Vec<bool>,
        current_element: Option<T>,
        index: usize,
        smallest_scans_default: Vec<bool>,
    ) {
        if smallest_value.is_none() {
            *smallest_value = current_element;
        } else if current_element.is_some() && current_element.unwrap() < smallest_value.unwrap() {
            *smallest_value = current_element;
            *smallest_scans = smallest_scans_default;
        }

        if smallest_value.is_some() && *smallest_value == current_element {
            smallest_scans[index] = true;
        }
    }
}

impl<'a, T> Iterator for ColumnScanUnion<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let smallest_scans_default = self.smallest_scans_default();

        let mut smallest_value = None;
        let mut smallest_scans = self.smallest_scans_default();

        for (index, scan) in self.column_scans.iter().enumerate() {
            if !self.active_scans[index] {
                // Non active scans are skipped
                continue;
            }

            let current_element = if self.smallest_scans[index] {
                // If the scan points to the smallest element then advance it
                scan.next()
            } else {
                // Otherwise we just take the current value
                scan.current()
            };

            Self::update_smallest(
                &mut smallest_value,
                &mut smallest_scans,
                current_element,
                index,
                smallest_scans_default.clone(),
            );
        }

        self.smallest_scans = smallest_scans;
        self.smallest_value = smallest_value;

        self.smallest_value
    }
}

impl<'a, T: Ord + Copy + Debug + PartialOrd> ColumnScan for ColumnScanUnion<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn seek(&mut self, value: T) -> Option<T> {
        let smallest_scans_default = self.smallest_scans_default();

        let mut smallest_value = None;
        let mut smallest_scans = self.smallest_scans_default();

        for (index, scan) in self.column_scans.iter().enumerate() {
            if !self.active_scans[index] {
                // Non active scans are skipped
                continue;
            }

            let current_element = scan.seek(value);

            Self::update_smallest(
                &mut smallest_value,
                &mut smallest_scans,
                current_element,
                index,
                smallest_scans_default.clone(),
            );
        }

        self.smallest_scans = smallest_scans;
        self.smallest_value = smallest_value;

        self.smallest_value
    }

    fn current(&self) -> Option<T> {
        self.smallest_value
    }

    fn reset(&mut self) {
        self.smallest_scans.fill(true);
        self.smallest_value = None;
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
    use crate::columnar::{
        column::{vector::ColumnVector, Column},
        columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum},
    };

    use super::ColumnScanUnion;
    use test_log::test;

    #[test]
    fn columnscan_union_basic() {
        let column_fst = ColumnVector::new(vec![0u64, 1, 3, 5, 15]);
        let column_snd = ColumnVector::new(vec![0u64, 1, 2, 7, 9]);
        let column_trd = ColumnVector::new(vec![0u64, 2, 4, 11]);

        let mut iter_fst = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(column_fst.iter()));
        let mut iter_snd = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(column_snd.iter()));
        let mut iter_trd = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(column_trd.iter()));

        let mut union_iter =
            ColumnScanUnion::new(vec![&mut iter_fst, &mut iter_snd, &mut iter_trd]);
        assert_eq!(union_iter.current(), None);
        assert_eq!(union_iter.next(), Some(0));
        assert_eq!(union_iter.current(), Some(0));
        assert_eq!(union_iter.next(), Some(1));
        assert_eq!(union_iter.current(), Some(1));
        assert_eq!(union_iter.next(), Some(2));
        assert_eq!(union_iter.current(), Some(2));
        assert_eq!(union_iter.next(), Some(3));
        assert_eq!(union_iter.current(), Some(3));
        assert_eq!(union_iter.next(), Some(4));
        assert_eq!(union_iter.current(), Some(4));
        assert_eq!(union_iter.seek(7), Some(7));
        assert_eq!(union_iter.current(), Some(7));
        assert_eq!(union_iter.seek(12), Some(15));
        assert_eq!(union_iter.current(), Some(15));
        assert_eq!(union_iter.next(), None);
        assert_eq!(union_iter.current(), None);
    }
}
