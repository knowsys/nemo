use super::super::traits::columnscan::{ColumnScan, ColumnScanCell};
use crate::{
    datatypes::ColumnDataType,
    util::interval::{Interval, IntervalBound},
};
use std::{fmt::Debug, ops::Range};

/// Interval bound which may either be a constant or given as a column scan value.
#[derive(Debug, Clone)]
pub enum IntervalValue<T>
where
    T: Clone,
{
    /// Interval bound is given as a value pointed by a column scan
    Column,
    /// Interval bound is a constant
    Constant(T),
}

/// Interval with bounds that may either be a constants or given as column scan values.
pub type ColumnScanInterval<T> = Interval<IntervalValue<T>>;

#[derive(Debug)]
enum ColumnScanStatus {
    /// Iterator is before the lower bound of the interval.
    Before,
    /// Iterator is withing the bounds of the interval.
    Within,
    /// Iterator is past the upper bound of the interval.
    After,
}

/// [`ColumnScan`] which allows its sub scan to only jump to a certain value
#[derive(Debug)]
pub struct ColumnScanEqualValue<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// The sub scan that provides the values
    scan_value: &'a ColumnScanCell<'a, T>,
    /// Optional lower bound in form of another sub scan.
    scan_lower: Option<&'a ColumnScanCell<'a, T>>,
    /// Optional upper bound in form of another sub scan.
    scan_upper: Option<&'a ColumnScanCell<'a, T>>,

    /// The value the scan jumps to
    interval: ColumnScanInterval<T>,

    /// Status of this scan.
    status: ColumnScanStatus,
}
impl<'a, T> ColumnScanEqualValue<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Constructs a new [`ColumnScanEqualValue`].
    pub fn new(
        scan_value: &'a ColumnScanCell<'a, T>,
        scan_lower: Option<&'a ColumnScanCell<'a, T>>,
        scan_upper: Option<&'a ColumnScanCell<'a, T>>,
        interval: ColumnScanInterval<T>,
    ) -> ColumnScanEqualValue<'a, T> {
        ColumnScanEqualValue {
            scan_value,
            scan_lower,
            scan_upper,
            interval,
            status: ColumnScanStatus::Before,
        }
    }

    fn get_bound_lower(&self, value: &IntervalValue<T>) -> T {
        match value {
            IntervalValue::Column => self.scan_lower.expect("If the bound is set to Column then scan_lower must be Some").current().expect("ColumnScan must be built in such a way, that the input scans always point to a value."),
            IntervalValue::Constant(constant) => *constant,
        }
    }

    fn get_bound_upper(&self, value: &IntervalValue<T>) -> T {
        match value {
            IntervalValue::Column => self.scan_upper.expect("If the bound is set to Column then scan_lower must be Some").current().expect("ColumnScan must be built in such a way, that the input scans always point to a value."),
            IntervalValue::Constant(constant) => *constant,
        }
    }

    fn satisfy_lower_bound(&mut self) {
        match &self.interval.lower {
            IntervalBound::Inclusive(bound) => {
                let bound_value = self.get_bound_lower(&bound);
                self.scan_value.seek(bound_value);
            }
            IntervalBound::Exclusive(bound) => {
                let bound_value = self.get_bound_lower(&bound);
                if let Some(seeked) = self.scan_value.seek(bound_value) {
                    if seeked == bound_value {
                        self.scan_value.next();
                    }
                }
            }
            IntervalBound::Unbounded => {
                self.scan_value.next();
            }
        }

        self.status = ColumnScanStatus::Within;
    }

    fn check_upper_bound(&self) -> bool {
        if let Some(current) = self.current() {
            match &self.interval.upper {
                IntervalBound::Inclusive(bound) => {
                    let bound_value = self.get_bound_upper(&bound);

                    current <= bound_value
                }
                IntervalBound::Exclusive(bound) => {
                    let bound_value = self.get_bound_upper(&bound);

                    current < bound_value
                }
                IntervalBound::Unbounded => true,
            }
        } else {
            false
        }
    }

    // /// Move the inner scan to such a position that it satisfies the lower bound of the interval.
    // /// Returns true if the inner iterator has been moved.
    // fn satisfy_lower_bound(&mut self) -> bool {
    //     match self.interval.lower {
    //         IntervalBound::Inclusive(bound) => {
    //             self.scan_value.seek(bound);
    //             true
    //         }
    //         IntervalBound::Exclusive(bound) => {
    //             if let Some(seeked) = self.scan_value.seek(bound) {
    //                 if seeked == bound {
    //                     self.scan_value.next();
    //                 }
    //             }

    //             true
    //         }
    //         IntervalBound::Unbounded => false,
    //     }
    // }
}

impl<'a, T> Iterator for ColumnScanEqualValue<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.status {
            ColumnScanStatus::Before => {
                self.satisfy_lower_bound();
                self.status = ColumnScanStatus::Within;
            }
            ColumnScanStatus::Within => {
                self.scan_value.next();
            }
            ColumnScanStatus::After => return None,
        }

        if self.check_upper_bound() {
            self.current()
        } else {
            self.status = ColumnScanStatus::After;
            None
        }
    }
}

impl<'a, T> ColumnScan for ColumnScanEqualValue<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn seek(&mut self, value: T) -> Option<T> {
        match self.status {
            ColumnScanStatus::Before => {
                self.satisfy_lower_bound();
                self.status = ColumnScanStatus::Within;

                self.scan_value.seek(value);
            }
            ColumnScanStatus::Within => {
                self.scan_value.seek(value);
            }
            ColumnScanStatus::After => return None,
        }

        if self.check_upper_bound() {
            self.current()
        } else {
            self.status = ColumnScanStatus::After;
            None
        }
    }

    fn current(&self) -> Option<T> {
        if let ColumnScanStatus::After = self.status {
            return None;
        }

        self.scan_value.current()
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
    use crate::{
        columnar::{
            column_types::vector::ColumnVector,
            operations::columnscan_equal_value::IntervalValue,
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

        let mut equal_scan = ColumnScanEqualValue::new(
            &col_iter,
            None,
            None,
            Interval::single(IntervalValue::Constant(4)),
        );
        assert_eq!(equal_scan.current(), None);
        assert_eq!(equal_scan.next(), Some(4));
        assert_eq!(equal_scan.current(), Some(4));
        assert_eq!(equal_scan.next(), None);
        assert_eq!(equal_scan.current(), None);

        let col_iter = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(col.iter()));
        let mut equal_scan = ColumnScanEqualValue::new(
            &col_iter,
            None,
            None,
            Interval::single(IntervalValue::Constant(7)),
        );
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
            None,
            None,
            Interval::new(
                IntervalBound::Exclusive(IntervalValue::Constant(1)),
                IntervalBound::Inclusive(IntervalValue::Constant(4)),
            ),
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
