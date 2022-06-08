use super::{ColumnScan, RangedColumnScan, RangedColumnScanCell};
use crate::physical::datatypes::{Field, FloorToUsize};
use std::fmt::Debug;
use std::ops::Range;

/// Iterator representing a union of column iterators
#[derive(Debug)]
pub struct UnionScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    column_scans: Vec<&'a RangedColumnScanCell<'a, T>>,
    smallest_scans: Vec<usize>,
    smallest_value: Option<T>,
}

impl<'a, T> UnionScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    /// Constructs a new VectorColumnScan for a Column.
    pub fn new(column_scans: Vec<&'a RangedColumnScanCell<'a, T>>) -> UnionScan<'a, T> {
        UnionScan {
            column_scans,
            smallest_scans: vec![],
            smallest_value: None,
        }
    }

    /// Returns vector contianing the indices of those scans which point to the currently smallest values
    pub fn get_smallest_scans(&self) -> &Vec<usize> {
        &self.smallest_scans
    }
}

impl<'a, T> Iterator for UnionScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let mut next_smallest_scans = Vec::<usize>::with_capacity(self.column_scans.len());
        let mut next_smallest: Option<T> = None;
        let mut smallest_scans_pointer = 0;

        for (index, scan) in self.column_scans.iter_mut().enumerate() {
            let current_smallest = self.smallest_scans[smallest_scans_pointer];

            let current_element = if index == current_smallest {
                smallest_scans_pointer += 1;

                scan.next()
            } else {
                scan.current()
            };

            if next_smallest.is_none() {
                next_smallest = current_element;
            } else {
                if current_element.is_some() && current_element.unwrap() < next_smallest.unwrap() {
                    next_smallest = current_element;

                    next_smallest_scans.clear();
                }
            }

            if next_smallest == current_element {
                next_smallest_scans.push(index);
            }
        }

        self.smallest_value = next_smallest;
        self.smallest_scans = next_smallest_scans;

        next_smallest
    }
}

impl<'a, T: Ord + Copy + Debug + PartialOrd> ColumnScan for UnionScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn seek(&mut self, value: T) -> Option<T> {
        let mut next_smallest_scans = Vec::<usize>::with_capacity(self.column_scans.len());
        let mut next_smallest: Option<T> = None;
        for (index, scan) in self.column_scans.iter_mut().enumerate() {
            let current_element = scan.seek(value);

            if next_smallest.is_none() {
                next_smallest = current_element;
            } else {
                if current_element.is_some() && current_element.unwrap() < next_smallest.unwrap() {
                    next_smallest = current_element;

                    next_smallest_scans.clear();
                }
            }

            if next_smallest == current_element {
                next_smallest_scans.push(index);
            }
        }

        self.smallest_value = next_smallest;
        self.smallest_scans = next_smallest_scans;

        next_smallest
    }

    fn current(&mut self) -> Option<T> {
        self.smallest_value
    }

    fn reset(&mut self) {
        self.smallest_scans.clear();
        self.smallest_value = None;
    }
}

impl<'a, T: Ord + Copy + Debug> RangedColumnScan for UnionScan<'a, T>
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
