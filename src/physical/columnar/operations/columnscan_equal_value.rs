use super::super::traits::columnscan::{ColumnScan, ColumnScanCell};
use crate::physical::{
    datatypes::ColumnDataType,
    util::interval::{Interval, IntervalBound},
};
use std::{fmt::Debug, ops::Range};

#[derive(Debug)]
enum ColumnScanStatus {
    Before,
    Normal,
    After,
}

/// [`ColumnScan`] which allows its sub scan to only jump to a certain value
#[derive(Debug)]
pub struct ColumnScanEqualValue<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// The sub scan
    scan: &'a ColumnScanCell<'a, T>,

    /// The value the scan jumps to
    interval: Interval<T>,

    /// Status of this scan.
    status: ColumnScanStatus,
}
impl<'a, T> ColumnScanEqualValue<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Constructs a new [`ColumnScanEqualValue`].
    pub fn new(
        scan: &'a ColumnScanCell<'a, T>,
        interval: Interval<T>,
    ) -> ColumnScanEqualValue<'a, T> {
        ColumnScanEqualValue {
            scan,
            interval,
            status: ColumnScanStatus::Before,
        }
    }

    fn satisfy_lower_bound(&mut self) -> bool {
        match self.interval.lower {
            IntervalBound::Inclusive(bound) => {
                self.scan.seek(bound);
                true
            }
            IntervalBound::Exclusive(bound) => {
                if let Some(seeked) = self.scan.seek(bound) {
                    if seeked == bound {
                        self.scan.next();
                    }
                }

                true
            }
            IntervalBound::Unbounded => false,
        }
    }
}

impl<'a, T> Iterator for ColumnScanEqualValue<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if let ColumnScanStatus::After = self.status {
            return None;
        }

        let has_moved = if let ColumnScanStatus::Before = self.status {
            self.status = ColumnScanStatus::Normal;
            self.satisfy_lower_bound()
        } else {
            false
        };

        if !has_moved {
            self.scan.next();
        }

        if let Some(current) = self.scan.current() {
            if self.interval.upper.below(&current) {
                return self.scan.current();
            }
        }

        self.status = ColumnScanStatus::After;
        None
    }
}

impl<'a, T> ColumnScan for ColumnScanEqualValue<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn seek(&mut self, value: T) -> Option<T> {
        if let ColumnScanStatus::Before = self.status {
            self.satisfy_lower_bound();
            self.status = ColumnScanStatus::Normal;
        }

        self.scan.seek(value);

        if let Some(current) = self.scan.current() {
            if self.interval.upper.below(&current) {
                return self.scan.current();
            }
        }

        self.status = ColumnScanStatus::After;
        None
    }

    fn current(&self) -> Option<T> {
        if let ColumnScanStatus::After = self.status {
            return None;
        }

        self.scan.current()
    }

    fn reset(&mut self) {
        self.status = ColumnScanStatus::Before;
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
    use crate::physical::{
        columnar::{
            column_types::vector::ColumnVector,
            traits::{
                column::Column,
                columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum},
            },
        },
        util::interval::{Interval, IntervalBound},
    };

    use super::ColumnScanEqualValue;

    use test_log::test;

    #[test]
    fn test_u64_equal() {
        let col = ColumnVector::new(vec![1u64, 4, 8]);
        let col_iter = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(col.iter()));

        let mut equal_scan = ColumnScanEqualValue::new(&col_iter, Interval::single(4));
        assert_eq!(equal_scan.current(), None);
        assert_eq!(equal_scan.next(), Some(4));
        assert_eq!(equal_scan.current(), Some(4));
        assert_eq!(equal_scan.next(), None);
        assert_eq!(equal_scan.current(), None);

        let col_iter = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(col.iter()));
        let mut equal_scan = ColumnScanEqualValue::new(&col_iter, Interval::single(7));
        assert_eq!(equal_scan.current(), None);
        assert_eq!(equal_scan.next(), None);
        assert_eq!(equal_scan.current(), None);
    }

    #[test]
    fn test_u64_interval() {
        let col = ColumnVector::new(vec![1u64, 2, 4, 8]);
        let col_iter = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(col.iter()));

        let mut equal_scan = ColumnScanEqualValue::new(
            &col_iter,
            Interval::new(IntervalBound::Exclusive(1), IntervalBound::Inclusive(4)),
        );

        assert_eq!(equal_scan.current(), None);
        assert_eq!(equal_scan.next(), Some(2));
        assert_eq!(equal_scan.current(), Some(2));
        assert_eq!(equal_scan.next(), Some(4));
        assert_eq!(equal_scan.current(), Some(4));
        assert_eq!(equal_scan.next(), None);
        assert_eq!(equal_scan.current(), None);
    }
}
