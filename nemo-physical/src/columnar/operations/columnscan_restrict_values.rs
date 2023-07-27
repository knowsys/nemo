use super::super::traits::columnscan::{ColumnScan, ColumnScanCell};
use crate::datatypes::ColumnDataType;
use std::{fmt::Debug, ops::Range};

/// Concrete value an interval bound can take.
#[derive(Debug, Clone)]
pub enum FilterValue<T>
where
    T: Clone,
{
    /// Interval bound is given as a value pointed by a column scan with the given index.
    Column(usize),
    /// Interval bound by the given constant.
    Constant(T),
}

impl<T> FilterValue<T>
where
    T: Clone,
{
    /// Return the column index this value refers to.
    /// Returns None if this is a constant.
    pub fn column_index(&self) -> Option<usize> {
        match self {
            FilterValue::Column(index) => Some(*index),
            FilterValue::Constant(_) => None,
        }
    }

    /// Return a mutable reference to the column index this value refers to.
    /// Returns None if this is a constant.
    pub fn column_index_mut(&mut self) -> Option<&mut usize> {
        match self {
            FilterValue::Column(index) => Some(index),
            FilterValue::Constant(_) => None,
        }
    }
}

/// Represents a bound
#[derive(Debug, Clone)]
pub enum FilterBound<T>
where
    T: Clone,
{
    /// Bound includes the given value.
    Inclusive(FilterValue<T>),
    /// Bound exclusdes the given value.
    Exclusive(FilterValue<T>),
}

impl<T> FilterBound<T>
where
    T: Clone,
{
    /// Return the column index which this bound references.
    /// Return `None` if the bound is given by a constant.
    pub fn column_index(&self) -> Option<usize> {
        match self {
            FilterBound::Inclusive(value) | FilterBound::Exclusive(value) => value.column_index(),
        }
    }

    /// Return a mutable reference to the column index which this bound references.
    /// Return `None` if the bound is given by a constant.
    pub fn column_index_mut(&mut self) -> Option<&mut usize> {
        match self {
            FilterBound::Inclusive(value) | FilterBound::Exclusive(value) => {
                value.column_index_mut()
            }
        }
    }

    /// Return a mutable reference to the [`FilterValue`].
    pub fn value_mut(&mut self) -> &mut FilterValue<T> {
        match self {
            FilterBound::Inclusive(value) | FilterBound::Exclusive(value) => value,
        }
    }
}

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
pub struct ColumnScanRestrictValues<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// The sub scan that provides the values
    scan_value: &'a ColumnScanCell<'a, T>,
    /// The sub scans relative to which `scan_value` will be restricted.
    scans_restriction: Vec<&'a ColumnScanCell<'a, T>>,

    /// Lower bounds for the value of `scan_value`.
    lower_bounds: Vec<FilterBound<T>>,
    /// Upper bounds for the vlaue of `scan_value`.
    upper_bounds: Vec<FilterBound<T>>,
    /// Values that are skipped in `scan_value`.
    avoid_values: Vec<FilterValue<T>>,

    /// Status of this scan.
    status: ColumnScanStatus,
}
impl<'a, T> ColumnScanRestrictValues<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Constructs a new [`ColumnScanRestrictValues`].
    pub fn new(
        scan_value: &'a ColumnScanCell<'a, T>,
        scans_restriction: Vec<&'a ColumnScanCell<'a, T>>,
        lower_bounds: Vec<FilterBound<T>>,
        upper_bounds: Vec<FilterBound<T>>,
        avoid_values: Vec<FilterValue<T>>,
    ) -> ColumnScanRestrictValues<'a, T> {
        ColumnScanRestrictValues {
            scan_value,
            scans_restriction,
            lower_bounds,
            upper_bounds,
            avoid_values,
            status: ColumnScanStatus::Before,
        }
    }

    fn get_value(&self, value: &FilterValue<T>) -> T {
        match value {
            FilterValue::Column(index) => self.scans_restriction[*index]
                .current()
                .expect("If the bound is set to Column then scan_lower must be Some"),
            FilterValue::Constant(constant) => *constant,
        }
    }

    fn satisfy_lower_bounds(&mut self) {
        for lower_bound in &self.lower_bounds {
            match lower_bound {
                FilterBound::Inclusive(bound) => {
                    let bound_value = self.get_value(bound);
                    self.scan_value.seek(bound_value);
                }
                FilterBound::Exclusive(bound) => {
                    let bound_value = self.get_value(bound);
                    if let Some(seeked) = self.scan_value.seek(bound_value) {
                        if seeked == bound_value {
                            self.scan_value.next();
                        }
                    }
                }
            }
        }

        if self.lower_bounds.is_empty() {
            self.scan_value.next();
        }

        self.status = ColumnScanStatus::Within;
    }

    fn check_upper_bounds(&self) -> bool {
        if let Some(current) = self.current() {
            for upper_bound in &self.upper_bounds {
                match upper_bound {
                    FilterBound::Inclusive(bound) => {
                        if current > self.get_value(bound) {
                            return false;
                        }
                    }
                    FilterBound::Exclusive(bound) => {
                        if current >= self.get_value(bound) {
                            return false;
                        }
                    }
                }
            }

            return true;
        }

        false
    }

    fn check_avoid_values(&self) -> bool {
        if let Some(current) = self.current() {
            for avoid_value in &self.avoid_values {
                if current == self.get_value(avoid_value) {
                    return false;
                }
            }

            return true;
        }

        false
    }
}

impl<'a, T> Iterator for ColumnScanRestrictValues<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.status {
            ColumnScanStatus::Before => {
                self.satisfy_lower_bounds();
                self.status = ColumnScanStatus::Within;
            }
            ColumnScanStatus::Within => {
                self.scan_value.next();
            }
            ColumnScanStatus::After => return None,
        }

        if !self.check_avoid_values() {
            self.scan_value.next();
        }

        if self.check_upper_bounds() {
            self.current()
        } else {
            self.status = ColumnScanStatus::After;
            None
        }
    }
}

impl<'a, T> ColumnScan for ColumnScanRestrictValues<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn seek(&mut self, value: T) -> Option<T> {
        match self.status {
            ColumnScanStatus::Before => {
                self.satisfy_lower_bounds();
                self.status = ColumnScanStatus::Within;

                self.scan_value.seek(value);
            }
            ColumnScanStatus::Within => {
                self.scan_value.seek(value);
            }
            ColumnScanStatus::After => return None,
        }

        if !self.check_avoid_values() {
            self.scan_value.next();
        }

        if self.check_upper_bounds() {
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
    use crate::columnar::{
        column_types::vector::ColumnVector,
        operations::columnscan_restrict_values::{FilterBound, FilterValue},
        traits::{
            column::Column,
            columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum},
        },
    };

    use super::ColumnScanRestrictValues;

    use test_log::test;

    #[test]
    fn restrict_equal() {
        let col = ColumnVector::new(vec![1u64, 4, 8]);
        let col_iter = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(col.iter()));

        let mut restrict_scan = ColumnScanRestrictValues::new(
            &col_iter,
            vec![],
            vec![FilterBound::Inclusive(FilterValue::Constant(4))],
            vec![FilterBound::Inclusive(FilterValue::Constant(4))],
            vec![],
        );

        assert_eq!(restrict_scan.current(), None);
        assert_eq!(restrict_scan.next(), Some(4));
        assert_eq!(restrict_scan.current(), Some(4));
        assert_eq!(restrict_scan.next(), None);
        assert_eq!(restrict_scan.current(), None);

        let col_iter = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(col.iter()));
        let mut restrict_scan = ColumnScanRestrictValues::new(
            &col_iter,
            vec![],
            vec![FilterBound::Inclusive(FilterValue::Constant(7))],
            vec![FilterBound::Inclusive(FilterValue::Constant(7))],
            vec![],
        );
        assert_eq!(restrict_scan.current(), None);
        assert_eq!(restrict_scan.next(), None);
        assert_eq!(restrict_scan.current(), None);
    }

    #[test]
    fn restrict_interval() {
        let col = ColumnVector::new(vec![1u64, 2, 4, 8]);
        let col_iter = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(col.iter()));

        let mut restrict_scan = ColumnScanRestrictValues::new(
            &col_iter,
            vec![],
            vec![FilterBound::Exclusive(FilterValue::Constant(1))],
            vec![FilterBound::Inclusive(FilterValue::Constant(4))],
            vec![],
        );

        assert_eq!(restrict_scan.current(), None);
        assert_eq!(restrict_scan.next(), Some(2));
        assert_eq!(restrict_scan.current(), Some(2));
        assert_eq!(restrict_scan.next(), Some(4));
        assert_eq!(restrict_scan.current(), Some(4));
        assert_eq!(restrict_scan.next(), None);
        assert_eq!(restrict_scan.current(), None);
    }

    #[test]
    fn restrict_columns() {
        let column_value = ColumnVector::new(vec![1u64, 2, 4, 6, 8]);
        let column_bounds = ColumnVector::new(vec![2u64, 7]);

        let value_iter = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(column_value.iter()));
        let lower_bound =
            ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(column_bounds.iter()));
        let upper_bound =
            ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(column_bounds.iter()));

        lower_bound.next();
        upper_bound.next();
        upper_bound.next();

        let mut restrict_scan = ColumnScanRestrictValues::new(
            &value_iter,
            vec![&lower_bound, &upper_bound],
            vec![FilterBound::Exclusive(FilterValue::Column(0))],
            vec![FilterBound::Inclusive(FilterValue::Column(1))],
            vec![],
        );

        assert_eq!(restrict_scan.current(), None);
        assert_eq!(restrict_scan.next(), Some(4));
        assert_eq!(restrict_scan.current(), Some(4));
        assert_eq!(restrict_scan.next(), Some(6));
        assert_eq!(restrict_scan.current(), Some(6));
        assert_eq!(restrict_scan.next(), None);
        assert_eq!(restrict_scan.current(), None);
    }

    #[test]
    fn restrict_unequal() {
        let column_value = ColumnVector::new(vec![1u64, 2, 3, 4, 5, 6, 8]);
        let column_unequal_1 = ColumnVector::new(vec![1u64]);
        let column_unequal_2 = ColumnVector::new(vec![1u64, 3]);

        let value_iter = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(column_value.iter()));
        let unequal_1 =
            ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(column_unequal_1.iter()));
        let unequal_2 =
            ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(column_unequal_2.iter()));

        unequal_1.next();
        unequal_2.next();
        unequal_2.next();

        let mut restrict_scan = ColumnScanRestrictValues::new(
            &value_iter,
            vec![&unequal_1, &unequal_2],
            vec![],
            vec![],
            vec![
                FilterValue::Column(0),
                FilterValue::Column(1),
                FilterValue::Constant(5),
            ],
        );

        assert_eq!(restrict_scan.current(), None);
        assert_eq!(restrict_scan.next(), Some(2));
        assert_eq!(restrict_scan.current(), Some(2));
        assert_eq!(restrict_scan.next(), Some(4));
        assert_eq!(restrict_scan.current(), Some(4));
        assert_eq!(restrict_scan.next(), Some(6));
        assert_eq!(restrict_scan.current(), Some(6));
        assert_eq!(restrict_scan.next(), Some(8));
        assert_eq!(restrict_scan.current(), Some(8));
        assert_eq!(restrict_scan.next(), None);
        assert_eq!(restrict_scan.current(), None);
    }

    #[test]
    fn restrict_multiple_conditions() {
        let column_value = ColumnVector::new(vec![1u64, 2, 3, 4, 5, 6, 8]);
        let column_filter = ColumnVector::new(vec![2u64, 5, 7]);

        let value_iter = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(column_value.iter()));
        let lower_bound =
            ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(column_filter.iter()));
        let upper_bound =
            ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(column_filter.iter()));
        let unequals = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(column_filter.iter()));

        lower_bound.next();
        upper_bound.next();
        upper_bound.next();
        upper_bound.next();
        unequals.next();
        unequals.next();

        let mut restrict_scan = ColumnScanRestrictValues::new(
            &value_iter,
            vec![&lower_bound, &upper_bound, &unequals],
            vec![FilterBound::Exclusive(FilterValue::Column(0))],
            vec![FilterBound::Inclusive(FilterValue::Column(1))],
            vec![FilterValue::Column(2), FilterValue::Constant(3)],
        );

        assert_eq!(restrict_scan.current(), None);
        assert_eq!(restrict_scan.next(), Some(4));
        assert_eq!(restrict_scan.current(), Some(4));
        assert_eq!(restrict_scan.next(), Some(6));
        assert_eq!(restrict_scan.current(), Some(6));
        assert_eq!(restrict_scan.next(), None);
        assert_eq!(restrict_scan.current(), None);
    }
}
