use crate::physical::datatypes::ColumnDataType;

use std::fmt::Debug;
use std::ops::Range;

use super::super::traits::columnscan::{ColumnScan, ColumnScanCell};

/// Implementation of [`ColumnScan`] for the result of joining a list of [`ColumnScan`] structs.
#[derive(Debug)]
pub struct ColumnScanJoin<'a, T>
where
    T: 'a + ColumnDataType,
{
    column_scans: Vec<&'a ColumnScanCell<'a, T>>,
    active_scan: usize,
    active_max: Option<T>,
    current: Option<T>,
}

impl<'a, T> ColumnScanJoin<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Constructs a new ColumnVectorScan for a Column.
    pub fn new(column_scans: Vec<&'a ColumnScanCell<'a, T>>) -> Self {
        ColumnScanJoin {
            column_scans,
            active_scan: 0,
            active_max: None,
            current: None,
        }
    }
}

impl<'a, T> ColumnScanJoin<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn next_loop(&mut self) -> Option<T> {
        let mut matched_scans: usize = 1;

        loop {
            self.active_scan = (self.active_scan + 1) % self.column_scans.len();
            if self.active_max == self.column_scans[self.active_scan].seek(self.active_max.unwrap())
            {
                matched_scans += 1;
                if matched_scans == self.column_scans.len() {
                    self.current = self.active_max;
                    return self.current;
                }
            } else {
                self.active_max = self.column_scans[self.active_scan].current();
                matched_scans = 1;
                if self.active_max.is_none() {
                    self.current = None;
                    return None;
                }
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
        self.active_max = self.column_scans[self.active_scan].next();
        if self.active_max.is_none() {
            self.current = None;
            return None;
        }
        if self.column_scans.len() > 1 {
            self.next_loop()
        } else {
            self.current = self.active_max;
            self.active_max
        }
    }
}

impl<'a, T> ColumnScan for ColumnScanJoin<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn seek(&mut self, value: T) -> Option<T> {
        let seek_result = self.column_scans[self.active_scan].seek(value);

        if seek_result.is_none() {
            self.current = None;
        }

        self.active_max = Some(seek_result?);

        if self.column_scans.len() > 1 {
            self.next_loop()
        } else {
            self.current = self.active_max;
            self.active_max
        }
    }

    fn current(&mut self) -> Option<T> {
        self.current
    }

    fn reset(&mut self) {
        self.active_scan = 0;
        self.active_max = None;
        self.current = None;
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

    use super::ColumnScanJoin;
    use crate::physical::columnar::{
        column_types::vector::{ColumnScanVector, ColumnVector},
        traits::columnscan::{ColumnScan, ColumnScanEnum},
    };

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
