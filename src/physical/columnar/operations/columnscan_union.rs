use super::super::traits::columnscan::{ColumnScan, ColumnScanCell};
use crate::physical::datatypes::ColumnDataType;
use std::fmt::Debug;
use std::ops::Range;

/// Iterator representing a union of column iterators
#[derive(Debug)]
pub struct ColumnScanUnion<'a, T>
where
    T: 'a + ColumnDataType,
{
    column_scans: Vec<&'a ColumnScanCell<'a, T>>,
    smallest_scans: Vec<bool>,
    smallest_value: Option<T>,

    active_scans: Vec<usize>,
    active_values: Vec<Option<T>>,
}

impl<'a, T> ColumnScanUnion<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Constructs a new ColumnVectorScan for a Column.
    pub fn new(column_scans: Vec<&'a ColumnScanCell<'a, T>>) -> ColumnScanUnion<'a, T> {
        let scans_len = column_scans.len();
        ColumnScanUnion {
            column_scans,
            smallest_scans: vec![true; scans_len],
            smallest_value: None,
            active_scans: (0..scans_len).collect(),
            active_values: vec![None; scans_len],
        }
    }

    /// Returns vector contianing the indices of those scans which point to the currently smallest values
    pub fn get_smallest_scans(&self) -> &Vec<bool> {
        &self.smallest_scans
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

        for &index in &self.active_scans {
            let current_element = if self.smallest_scans[index] {
                let next_value = self.column_scans[index].next();
                self.active_values[index] = next_value;

                next_value
            } else {
                self.active_values[index]
            };

            self.smallest_scans[index] = false;

            if next_smallest.is_none() {
                next_smallest = current_element;
            } else if current_element.is_some() && current_element.unwrap() < next_smallest.unwrap()
            {
                next_smallest = current_element;

                for value in self.smallest_scans.iter_mut().take(index) {
                    *value = false;
                }
            }

            if next_smallest.is_some() && next_smallest == current_element {
                self.smallest_scans[index] = true;
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
        self.smallest_scans = vec![false; self.smallest_scans.len()];
        let mut next_smallest: Option<T> = None;

        for &index in &self.active_scans {
            let current_element = self.column_scans[index].seek(value);
            self.active_values[index] = current_element;

            if next_smallest.is_none() {
                next_smallest = current_element;
            } else if current_element.is_some() && current_element.unwrap() < next_smallest.unwrap()
            {
                next_smallest = current_element;

                for value in self.smallest_scans.iter_mut().take(index) {
                    *value = false;
                }
            }

            if next_smallest.is_some() && next_smallest == current_element {
                self.smallest_scans[index] = true;
            }
        }

        self.smallest_value = next_smallest;

        next_smallest
    }

    fn current(&mut self) -> Option<T> {
        self.smallest_value
    }

    fn reset(&mut self) {
        self.smallest_scans = vec![true; self.smallest_scans.len()];
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
    use crate::physical::columnar::{
        column_types::vector::ColumnVector,
        traits::{
            column::Column,
            columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum},
        },
    };

    use super::ColumnScanUnion;
    use test_log::test;

    #[test]
    fn test_u64() {
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
