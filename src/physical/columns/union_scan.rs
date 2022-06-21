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

        if self.smallest_value.is_none() {
            for (index, scan) in self.column_scans.iter_mut().enumerate() {
                let current_element = scan.next();

                if next_smallest.is_none() {
                    next_smallest = current_element;
                } else {
                    if current_element.is_some()
                        && current_element.unwrap() < next_smallest.unwrap()
                    {
                        next_smallest = current_element;
                        next_smallest_scans.clear();
                    }
                }
                if next_smallest == current_element {
                    next_smallest_scans.push(index);
                }
            }
        } else {
            for (index, scan) in self.column_scans.iter_mut().enumerate() {
                let current_smallest = if smallest_scans_pointer < self.smallest_scans.len() {
                    Some(self.smallest_scans[smallest_scans_pointer])
                } else {
                    None
                };

                let current_element =
                    if current_smallest.is_some() && index == current_smallest.unwrap() {
                        smallest_scans_pointer += 1;
                        scan.next()
                    } else {
                        scan.current()
                    };

                if next_smallest.is_none() {
                    next_smallest = current_element;
                } else {
                    if current_element.is_some()
                        && current_element.unwrap() < next_smallest.unwrap()
                    {
                        next_smallest = current_element;
                        next_smallest_scans.clear();
                        smallest_scans_pointer = 0;
                    }
                }
                if next_smallest.is_some() && next_smallest == current_element {
                    next_smallest_scans.push(index);
                }
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

#[cfg(test)]
mod test {
    use super::UnionScan;
    use crate::physical::columns::{Column, ColumnScan, VectorColumn};
    use test_log::test;

    #[test]
    fn test_u64() {
        let column_fst = VectorColumn::new(vec![0u64, 1, 3, 5, 15]);
        let column_snd = VectorColumn::new(vec![0u64, 1, 2, 7, 9]);
        let column_trd = VectorColumn::new(vec![0u64, 2, 4, 11]);
        let mut iter_fst = column_fst.iter();
        let mut iter_snd = column_snd.iter();
        let mut iter_trd = column_trd.iter();
        let mut union_iter = UnionScan::new(vec![&mut iter_fst, &mut iter_snd, &mut iter_trd]);
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
