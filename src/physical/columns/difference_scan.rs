use super::{ColumnScan, RangedColumnScan, RangedColumnScanCell};
use crate::physical::datatypes::{Field, FloorToUsize};
use std::fmt::Debug;
use std::ops::Range;

/// Iterator for the set difference of two iterators
#[derive(Debug)]
pub struct DifferenceScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    scan_left: &'a RangedColumnScanCell<'a, T>,
    scan_right: &'a RangedColumnScanCell<'a, T>,
    equal: bool,
    current: Option<T>,
}

impl<'a, T> DifferenceScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    /// Constructs a new VectorColumnScan for a Column.
    pub fn new(
        scan_left: &'a RangedColumnScanCell<'a, T>,
        scan_right: &'a RangedColumnScanCell<'a, T>,
    ) -> DifferenceScan<'a, T> {
        DifferenceScan {
            scan_left,
            scan_right,
            equal: false,
            current: None,
        }
    }
}

impl<'a, T> Iterator for DifferenceScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.current = self.scan_left.next();

        if self.equal {
            self.scan_right.next();
        }

        self.equal = self.current.is_some() && self.current == self.scan_right.current();
        self.current
    }
}

impl<'a, T> ColumnScan for DifferenceScan<'a, T>
where
    T: 'a + Debug + Copy + Ord + TryFrom<usize> + FloorToUsize + Field,
{
    fn seek(&mut self, value: T) -> Option<T> {
        self.current = self.scan_left.seek(value);

        if self.equal {
            self.scan_right.seek(value);
        }

        self.equal = self.current.is_some() && self.current == self.scan_right.current();
        self.current
    }

    fn current(&mut self) -> Option<T> {
        self.current
    }

    fn reset(&mut self) {
        self.equal = false;
        self.current = None;
    }
}

impl<'a, T> RangedColumnScan for DifferenceScan<'a, T>
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
