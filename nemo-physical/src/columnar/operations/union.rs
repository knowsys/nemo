//! This module defines [ColumnScanUnion].

use std::{cell::UnsafeCell, fmt::Debug, ops::Range, rc::Rc};

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

    /// Contains the indices of the entries in `column_scans` that are active
    ///
    /// Non-active scans are ignored during the computation of the union.
    active_scans: Rc<UnsafeCell<Vec<usize>>>,

    /// Contains the indices of the entries in `column_scans` that are pointing to the smallest element
    ///
    /// Non-active scans are ignored during the computation of the union.
    smallest_scans: Rc<UnsafeCell<Vec<usize>>>,

    /// Smallest value pointed to by the sub scans
    smallest_value: Option<T>,
}

impl<'a, T> ColumnScanUnion<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Constructs a new [ColumnScanUnion].
    pub(crate) fn new(
        column_scans: Vec<&'a ColumnScanCell<'a, T>>,
        active_scans: Rc<UnsafeCell<Vec<usize>>>,
        smallest_scans: Rc<UnsafeCell<Vec<usize>>>,
    ) -> ColumnScanUnion<'a, T> {
        ColumnScanUnion {
            column_scans,
            active_scans,
            smallest_scans,
            smallest_value: None,
        }
    }
}

impl<'a, T> Iterator for ColumnScanUnion<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let mut next_smallest: Option<T> = None;
        let current_smallest = self.current();

        let active_scans: &Vec<usize> = unsafe { &*self.active_scans.get() };
        let smallest_scans: &mut Vec<usize> = unsafe { &mut *self.smallest_scans.get() };
        smallest_scans.clear();

        for &scan_index in active_scans {
            let current_element = self.column_scans[scan_index].current();

            let compare_element = if current_element == current_smallest {
                self.column_scans[scan_index].next()
            } else {
                current_element
            };

            if next_smallest.is_none() {
                next_smallest = compare_element;
            } else if compare_element.is_some() && compare_element.unwrap() < next_smallest.unwrap()
            {
                next_smallest = compare_element;
                smallest_scans.clear();
            }

            if next_smallest.is_some() && next_smallest == compare_element {
                smallest_scans.push(scan_index);
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
        let active_scans: &Vec<usize> = unsafe { &*self.active_scans.get() };
        let smallest_scans: &mut Vec<usize> = unsafe { &mut *self.smallest_scans.get() };
        smallest_scans.clear();

        let mut next_smallest: Option<T> = None;

        for &scan_index in active_scans {
            let current_element = self.column_scans[scan_index].seek(value);

            if next_smallest.is_none() {
                next_smallest = current_element;
            } else if current_element.is_some() && current_element.unwrap() < next_smallest.unwrap()
            {
                next_smallest = current_element;
                smallest_scans.clear();
            }

            if next_smallest.is_some() && next_smallest == current_element {
                smallest_scans.push(scan_index);
            }
        }

        self.smallest_value = next_smallest;

        next_smallest
    }

    fn current(&self) -> Option<T> {
        self.smallest_value
    }

    fn reset(&mut self) {
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
    use std::{cell::UnsafeCell, rc::Rc};

    use crate::columnar::{
        column::{vector::ColumnVector, Column},
        columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum},
    };

    use super::ColumnScanUnion;
    use test_log::test;

    #[test]
    fn columnscan_union_basic() {
        let active = Rc::new(UnsafeCell::new((0..3).collect::<Vec<usize>>()));
        let smallest = Rc::new(UnsafeCell::new(vec![]));

        let column_fst = ColumnVector::new(vec![0u64, 1, 3, 5, 15]);
        let column_snd = ColumnVector::new(vec![0u64, 1, 2, 7, 9]);
        let column_trd = ColumnVector::new(vec![0u64, 2, 4, 11]);

        let mut iter_fst = ColumnScanCell::new(ColumnScanEnum::Vector(column_fst.iter()));
        let mut iter_snd = ColumnScanCell::new(ColumnScanEnum::Vector(column_snd.iter()));
        let mut iter_trd = ColumnScanCell::new(ColumnScanEnum::Vector(column_trd.iter()));

        let mut union_iter = ColumnScanUnion::new(
            vec![&mut iter_fst, &mut iter_snd, &mut iter_trd],
            active,
            smallest,
        );
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
