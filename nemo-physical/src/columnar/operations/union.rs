//! This module defines [ColumnScanUnion].

use std::{fmt::Debug, ops::Range};

use crate::{
    columnar::columnscan::{ColumnScan, ColumnScanCell},
    datatypes::ColumnDataType,
};

/// [ColumnScan] representing the union of its sub scans
#[derive(Debug)]
pub struct ColumnScanUnion<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Sub scans of which the union is computed
    column_scans: Vec<&'a ColumnScanCell<'a, T>>,

    /// Contains the indices of the entries in `column_scans` that are active
    ///
    /// Non-active scans are ignored during the computation of the union.
    active_scans: Vec<usize>,

    /// Marks which of the active scans in `column_scans` currently point to the smallest value
    ///
    /// `smallest_scans[i]` indicates whether the ith scan points to the smallest element
    smallest_scans: Vec<bool>,

    /// Smallest value pointed to by the sub scans
    smallest_value: Option<T>,
}

impl<'a, T> ColumnScanUnion<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Constructs a new [ColumnScanUnion].
    pub fn new(column_scans: Vec<&'a ColumnScanCell<'a, T>>) -> ColumnScanUnion<'a, T> {
        let scans_len = column_scans.len();
        ColumnScanUnion {
            column_scans,
            smallest_scans: vec![true; scans_len],
            active_scans: (0..scans_len).collect(),
            smallest_value: None,
        }
    }

    /// Check whether the [ColumnScan] of the given index points the currently smallest value.
    pub fn is_smallest_scans(&self, index: usize) -> bool {
        self.smallest_scans[index]
    }

    /// Set a vector that indicates which scans are currently active and should be considered
    pub fn set_active_scans(&mut self, active_scans: Vec<usize>) {
        self.active_scans = active_scans;
    }
}

impl<'a, T> Iterator for ColumnScanUnion<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let mut next_smallest: Option<T> = None;

        for (active_index, &scan_index) in self.active_scans.iter().enumerate() {
            let current_element = if self.smallest_scans[scan_index] {
                // If the scan points to the smallest element then advance it
                // and update active_value
                self.column_scans[scan_index].next()
            } else {
                self.column_scans[scan_index].current()
            };

            self.smallest_scans[scan_index] = false;

            if next_smallest.is_none() {
                next_smallest = current_element;
            } else if current_element.is_some() && current_element.unwrap() < next_smallest.unwrap()
            {
                next_smallest = current_element;

                // New smallest element has been found, hence all previous scans don't point to it
                for &prvious_index in self.active_scans.iter().take(active_index) {
                    self.smallest_scans[prvious_index] = false;
                }
            }

            if next_smallest.is_some() && next_smallest == current_element {
                self.smallest_scans[scan_index] = true;
            }
        }

        self.smallest_value = next_smallest;

        next_smallest
    }
}

impl<'a, T: Ord + Copy + Debug + PartialOrd> ColumnScan for ColumnScanUnion<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn seek(&mut self, value: T) -> Option<T> {
        self.smallest_scans.fill(false);
        let mut next_smallest: Option<T> = None;

        for (active_index, &scan_index) in self.active_scans.iter().enumerate() {
            let current_element = self.column_scans[scan_index].seek(value);

            if next_smallest.is_none() {
                next_smallest = current_element;
            } else if current_element.is_some() && current_element.unwrap() < next_smallest.unwrap()
            {
                next_smallest = current_element;

                // New smallest element has been found, hence all previous scans don't point to it
                for &prvious_index in self.active_scans.iter().take(active_index) {
                    self.smallest_scans[prvious_index] = false;
                }
            }

            if next_smallest.is_some() && next_smallest == current_element {
                self.smallest_scans[scan_index] = true;
            }
        }

        self.smallest_value = next_smallest;

        next_smallest
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
