//! This module defines [ColumnScanJoin].

use crate::columnar::columnscan::{ColumnScan, ColumnScanCell};
use crate::datatypes::ColumnDataType;

use std::fmt::Debug;
use std::ops::Range;

/// Implementation of [ColumnScan] for the result of joining a list of [ColumnScan] objects.
#[derive(Debug)]
pub struct ColumnScanJoin<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// List of subiterators to be joined
    column_scans: Vec<&'a ColumnScanCell<'a, T>>,

    /// Index of the scan which is currently being advanced
    active_index: usize,

    /// Current value of the [ColumnScanJoin]; its also the value pointed to by each sub scan
    current_value: Option<T>,
}

impl<'a, T> ColumnScanJoin<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Constructs a new [ColumnScanJoin].
    pub fn new(column_scans: Vec<&'a ColumnScanCell<'a, T>>) -> Self {
        ColumnScanJoin {
            column_scans,
            active_index: 0,
            current_value: None,
        }
    }
}

impl<'a, T> ColumnScanJoin<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Sets each sub scan to the currently largest value until
    /// either all sub scans point to the same value
    /// or one sub scan reaches its end
    fn next_loop(&mut self, mut current_max: T) -> Option<T> {
        // Number of scans that point to the same value
        let mut matched_scans: usize = 1;

        loop {
            // If all the sub scans point to the same value we found the next match
            if matched_scans == self.column_scans.len() {
                return Some(current_max);
            }

            // Select the next sub scan
            self.active_index = (self.active_index + 1) % self.column_scans.len();
            let active_scan = self.column_scans[self.active_index];

            if let Some(active_value) = active_scan.seek(current_max) {
                if active_value == current_max {
                    // If the values are equal we increment the counter
                    matched_scans += 1;
                } else {
                    // If the value of the current scan does not equal `current_max` then it must be larger
                    current_max = active_value;
                    matched_scans = 1;
                }
            } else {
                // If we reach the end of one sub scan then we have reached the end of the join
                return None;
            }
        }
    }
}

impl<'a, T> Iterator for ColumnScanJoin<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.current_value =
            if let Some(next_active_value) = self.column_scans[self.active_index].next() {
                self.next_loop(next_active_value)
            } else {
                None
            };

        self.current_value
    }
}

impl<'a, T> ColumnScan for ColumnScanJoin<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn seek(&mut self, value: T) -> Option<T> {
        self.current_value =
            if let Some(next_active_value) = self.column_scans[self.active_index].seek(value) {
                self.next_loop(next_active_value)
            } else {
                None
            };

        self.current_value
    }

    fn current(&self) -> Option<T> {
        self.current_value
    }

    fn reset(&mut self) {
        self.active_index = 0;
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
    use test_log::test;

    use crate::columnar::{
        column::vector::{ColumnScanVector, ColumnVector},
        columnscan::{ColumnScan, ColumnScanEnum},
    };

    use super::ColumnScanJoin;

    #[test]
    fn test_u64_simple_join<'a>() {
        let data1: Vec<u64> = vec![1, 3, 5, 7, 9];
        let vc1: ColumnVector<u64> = ColumnVector::new(data1);
        let mut gcs1 = ColumnScanEnum::ColumnScanVector(ColumnScanVector::new(&vc1)).into();

        let data2: Vec<u64> = vec![1, 5, 6, 7, 9, 10];
        let vc2: ColumnVector<u64> = ColumnVector::new(data2);
        let mut gcs2 = ColumnScanEnum::ColumnScanVector(ColumnScanVector::new(&vc2)).into();

        let data3: Vec<u64> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let vc3: ColumnVector<u64> = ColumnVector::new(data3);
        let mut gcs3 = ColumnScanEnum::ColumnScanVector(ColumnScanVector::new(&vc3)).into();

        let mut omj = ColumnScanJoin::new(vec![&mut gcs1, &mut gcs2, &mut gcs3]);

        assert_eq!(omj.next(), Some(1));
        assert_eq!(omj.current(), Some(1));
        assert_eq!(omj.next(), Some(5));
        assert_eq!(omj.current(), Some(5));
        assert_eq!(omj.next(), Some(7));
        assert_eq!(omj.current(), Some(7));
        assert_eq!(omj.next(), Some(9));
        assert_eq!(omj.current(), Some(9));
        assert_eq!(omj.next(), None);
        assert_eq!(omj.current(), None);
        assert_eq!(omj.next(), None);

        let mut gcs1 = ColumnScanEnum::ColumnScanVector(ColumnScanVector::new(&vc1)).into();
        let mut gcs2 = ColumnScanEnum::ColumnScanVector(ColumnScanVector::new(&vc2)).into();
        let mut gcs3 = ColumnScanEnum::ColumnScanVector(ColumnScanVector::new(&vc3)).into();

        let mut omj = ColumnScanJoin::new(vec![&mut gcs1, &mut gcs2, &mut gcs3]);

        assert_eq!(omj.seek(5), Some(5));
        assert_eq!(omj.current(), Some(5));
        assert_eq!(omj.seek(7), Some(7));
        assert_eq!(omj.current(), Some(7));
        assert_eq!(omj.seek(8), Some(9));
        assert_eq!(omj.current(), Some(9));
        assert_eq!(omj.seek(10), None);
        assert_eq!(omj.current(), None);
    }
}
