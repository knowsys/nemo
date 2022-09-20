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

    active_scans: Vec<usize>,
}

impl<'a, T> UnionScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    /// Constructs a new VectorColumnScan for a Column.
    pub fn new(column_scans: Vec<&'a RangedColumnScanCell<'a, T>>) -> UnionScan<'a, T> {
        let scans_len = column_scans.len();
        UnionScan {
            column_scans,
            smallest_scans: vec![],
            smallest_value: None,
            active_scans: (0..scans_len).collect(),
        }
    }

    /// Returns vector contianing the indices of those scans which point to the currently smallest values
    pub fn get_smallest_scans(&self) -> &Vec<usize> {
        &self.smallest_scans
    }

    /// Set a vector that indicates which scans are currently active and should be considered
    pub fn set_active_scans(&mut self, active_scans: Vec<usize>) {
        self.active_scans = active_scans;
    }
}

impl<'a, T> Iterator for UnionScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.smallest_value.is_some() {
            // At the beginning, we need to move every scan one forward
            for &index in &self.smallest_scans {
                let scan = &mut self.column_scans[index];
                scan.next();
            }
        } else {
            // You only call next on those scans containing the smallest values
            for &index in &self.active_scans {
                let scan = &mut self.column_scans[index];
                scan.next();
            }
        }

        let mut next_smallest_scans = Vec::<usize>::with_capacity(self.column_scans.len());
        let mut next_smallest: Option<T> = None;

        // Simply find the smallest value among the active scans
        for &index in &self.active_scans {
            let scan = &mut self.column_scans[index];
            let current_element = scan.current();

            if next_smallest.is_none() {
                next_smallest = current_element;
            } else if current_element.is_some() && current_element.unwrap() < next_smallest.unwrap()
            {
                next_smallest = current_element;
                next_smallest_scans.clear();
            }

            if next_smallest.is_some() && next_smallest == current_element {
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

        for &index in &self.active_scans {
            let scan = &mut self.column_scans[index];
            let current_element = scan.seek(value);

            if next_smallest.is_none() {
                next_smallest = current_element;
            } else if current_element.is_some() && current_element.unwrap() < next_smallest.unwrap()
            {
                next_smallest = current_element;
                next_smallest_scans.clear();
            }

            if next_smallest.is_some() && next_smallest == current_element {
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
    use crate::physical::columns::{
        Column, ColumnScan, GenericColumnScanEnum, RangedColumnScanCell, RangedColumnScanEnum,
        VectorColumn,
    };
    use test_log::test;

    #[test]
    fn test_u64() {
        let column_fst = VectorColumn::new(vec![0u64, 1, 3, 5, 15]);
        let column_snd = VectorColumn::new(vec![0u64, 1, 2, 7, 9]);
        let column_trd = VectorColumn::new(vec![0u64, 2, 4, 11]);

        let mut iter_fst = RangedColumnScanCell::new(RangedColumnScanEnum::GenericColumnScan(
            GenericColumnScanEnum::VectorColumn(column_fst.iter()),
        ));
        let mut iter_snd = RangedColumnScanCell::new(RangedColumnScanEnum::GenericColumnScan(
            GenericColumnScanEnum::VectorColumn(column_snd.iter()),
        ));
        let mut iter_trd = RangedColumnScanCell::new(RangedColumnScanEnum::GenericColumnScan(
            GenericColumnScanEnum::VectorColumn(column_trd.iter()),
        ));

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
