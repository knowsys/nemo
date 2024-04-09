//! This module defines [ColumnScanFilterInterval]

use std::cmp::Ordering;

use crate::{
    columnar::columnscan::{ColumnScan, ColumnScanCell},
    datatypes::ColumnDataType,
};

/// [ColumnScanFilterInterval] does only return values within a certain interval
/// Therefore, it can be only in of three states.
#[derive(Debug)]
enum ColumnScanState<T>
where
    T: ColumnDataType,
{
    /// [ColumnScanFilterInterval]'s value is lower than the lower bound of the interval
    Before,
    /// [ColumnScanFilterInterval]'s value is within the interval, in particular not passed the upper bound stored here
    Within(Option<IntervalBoundConstant<T>>),
    /// [ColumnScanFilterInterval]'s value is greater than the upper bound of the interval
    After,
}

/// Type of interval boundary
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum BoundaryType {
    Inclusive,
    Exclusive,
}

/// Constant interval bound
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IntervalBoundConstant<T>
where
    T: ColumnDataType,
{
    /// Whether the value is included or excluded in this bound
    boundary_type: BoundaryType,
    /// The value
    value: T,
}

impl<T> IntervalBoundConstant<T>
where
    T: ColumnDataType,
{
    /// Create a new inclusive [IntervalBoundConstant].
    #[allow(dead_code)]
    pub(crate) fn inclusive(value: T) -> Self {
        Self {
            boundary_type: BoundaryType::Inclusive,
            value,
        }
    }

    /// Create a new exclusive [IntervalBoundConstant].
    #[allow(dead_code)]
    pub(crate) fn exclusive(value: T) -> Self {
        Self {
            boundary_type: BoundaryType::Exclusive,
            value,
        }
    }
}

impl<T> PartialOrd for IntervalBoundConstant<T>
where
    T: ColumnDataType,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for IntervalBoundConstant<T>
where
    T: ColumnDataType,
{
    fn cmp(&self, other: &Self) -> Ordering {
        match self.value.cmp(&other.value) {
            Ordering::Equal => match (&self.boundary_type, &other.boundary_type) {
                (BoundaryType::Inclusive, BoundaryType::Exclusive) => Ordering::Less,
                (BoundaryType::Exclusive, BoundaryType::Inclusive) => Ordering::Greater,
                _ => Ordering::Equal,
            },
            ordering => ordering,
        }
    }
}

/// Interval bound whose value is derived from an input [ColumnScan]
#[derive(Debug)]
pub(crate) struct IntervalBoundVariable {
    /// Whether the value is included or excluded in this bound
    boundary_type: BoundaryType,
    /// Index of the input [ColumnScan] from which the value is derived
    input_index: usize,
}

impl IntervalBoundVariable {
    /// Create a new inclusive [IntervalBoundVariable].
    #[allow(dead_code)]
    pub(crate) fn inclusive(input_index: usize) -> Self {
        Self {
            boundary_type: BoundaryType::Inclusive,
            input_index,
        }
    }

    /// Create a new inclusive [IntervalBoundVariable].
    #[allow(dead_code)]
    pub(crate) fn exclusive(input_index: usize) -> Self {
        Self {
            boundary_type: BoundaryType::Exclusive,
            input_index,
        }
    }
}

/// [ColumnScan] that restricts its subscan to an interval of permissable value.
#[derive(Debug)]
pub(crate) struct ColumnScanFilterInterval<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// [ColumnScan] that provides the output values for this scan
    value_scan: &'a ColumnScanCell<'a, T>,

    /// [ColumnScan] which provide non constant bounds for `value_scan`
    input_scans: Vec<&'a ColumnScanCell<'a, T>>,

    /// The current value has to satisfy all the lower bounds in this list
    input_lower: Vec<IntervalBoundVariable>,
    /// The current value has to satisfy all the upper bounds in this list
    input_upper: Vec<IntervalBoundVariable>,

    /// The current value has to satisfy this constant lower bound.
    constant_lower: Option<IntervalBoundConstant<T>>,
    /// The current value has to satisfy this constant upper bound.
    constant_upper: Option<IntervalBoundConstant<T>>,

    /// The current [ColumnScanState]
    current_state: ColumnScanState<T>,
}

impl<'a, T> ColumnScanFilterInterval<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Create a new [ColumnScanFilterInterval].
    #[allow(dead_code)]
    pub(crate) fn new(
        value_scan: &'a ColumnScanCell<'a, T>,
        input_scans: Vec<&'a ColumnScanCell<'a, T>>,
        input_lower: Vec<IntervalBoundVariable>,
        input_upper: Vec<IntervalBoundVariable>,
        constant_lower: Vec<IntervalBoundConstant<T>>,
        constant_upper: Vec<IntervalBoundConstant<T>>,
    ) -> Self {
        Self {
            value_scan,
            input_scans,
            input_lower,
            input_upper,
            constant_lower: Self::aggregate_lower_bounds(constant_lower.into_iter()),
            constant_upper: Self::aggregate_upper_bounds(constant_upper.into_iter()),
            current_state: ColumnScanState::Before,
        }
    }

    /// Aggregate all provided lower bounds into one (equivalent) lower bound.
    fn aggregate_lower_bounds<Iter: Iterator<Item = IntervalBoundConstant<T>>>(
        bounds: Iter,
    ) -> Option<IntervalBoundConstant<T>> {
        bounds.max()
    }

    /// Aggregate all provided lower bounds into one (equivalent) lower bound.
    fn aggregate_upper_bounds<Iter: Iterator<Item = IntervalBoundConstant<T>>>(
        bounds: Iter,
    ) -> Option<IntervalBoundConstant<T>> {
        bounds.min()
    }

    /// Return the current lower bound.
    fn current_lower_bound(&self) -> Option<IntervalBoundConstant<T>> {
        Self::aggregate_lower_bounds(self.constant_lower.iter().cloned().chain(
            self.input_lower.iter().map(|bound| {
                IntervalBoundConstant {
                    boundary_type: bound.boundary_type,
                    value: self.input_scans[bound.input_index]
                        .current()
                        .expect("Every input scan must return a value."),
                }
            }),
        ))
    }

    /// Return the current upper bound.
    fn current_upper_bound(&self) -> Option<IntervalBoundConstant<T>> {
        Self::aggregate_upper_bounds(self.constant_upper.iter().cloned().chain(
            self.input_upper.iter().map(|bound| {
                IntervalBoundConstant {
                    boundary_type: bound.boundary_type,
                    value: self.input_scans[bound.input_index]
                        .current()
                        .expect("Every input scan must return a value."),
                }
            }),
        ))
    }

    fn satisfy_lower_bound(&mut self, lower_bound: IntervalBoundConstant<T>) {
        let seeked_value = self.value_scan.seek(lower_bound.value);

        if lower_bound.boundary_type == BoundaryType::Exclusive
            && seeked_value == Some(lower_bound.value)
        {
            self.value_scan.next();
        }
    }

    fn check_upper_bound(&self, upper_bound_option: &Option<IntervalBoundConstant<T>>) -> bool {
        if let Some(current_value) = self.value_scan.current() {
            if let Some(upper_bound) = upper_bound_option {
                match upper_bound.boundary_type {
                    BoundaryType::Inclusive => current_value <= upper_bound.value,
                    BoundaryType::Exclusive => current_value < upper_bound.value,
                }
            } else {
                true
            }
        } else {
            false
        }
    }
}

impl<'a, T> Iterator for ColumnScanFilterInterval<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.current_state {
            ColumnScanState::Before => {
                let option_lower_bound = self.current_lower_bound();
                let option_upper_bound = self.current_upper_bound();

                if let Some(lower_bound) = option_lower_bound {
                    self.satisfy_lower_bound(lower_bound);
                } else {
                    self.value_scan.next();
                }

                if self.check_upper_bound(&option_upper_bound) {
                    self.current_state = ColumnScanState::Within(option_upper_bound);
                } else {
                    self.current_state = ColumnScanState::After;
                }
            }
            ColumnScanState::Within(upper_bound_option) => {
                self.value_scan.next();

                if !self.check_upper_bound(upper_bound_option) {
                    self.current_state = ColumnScanState::After;
                }
            }
            ColumnScanState::After => {}
        }

        self.current()
    }
}

impl<'a, T> ColumnScan for ColumnScanFilterInterval<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn seek(&mut self, value: Self::Item) -> Option<Self::Item> {
        match &self.current_state {
            ColumnScanState::Before => {
                let option_lower_bound = self.current_lower_bound().map(|lower_bound| {
                    lower_bound.max(IntervalBoundConstant {
                        boundary_type: BoundaryType::Inclusive,
                        value,
                    })
                });
                let option_upper_bound = self.current_upper_bound();

                if let Some(lower_bound) = option_lower_bound {
                    self.satisfy_lower_bound(lower_bound);
                } else {
                    self.value_scan.next();
                }

                if self.check_upper_bound(&option_upper_bound) {
                    self.current_state = ColumnScanState::Within(option_upper_bound);
                } else {
                    self.current_state = ColumnScanState::After;
                }
            }
            ColumnScanState::Within(upper_bound_option) => {
                self.value_scan.seek(value);

                if !self.check_upper_bound(upper_bound_option) {
                    self.current_state = ColumnScanState::After;
                }
            }
            ColumnScanState::After => {}
        }

        self.current()
    }

    fn current(&self) -> Option<Self::Item> {
        if let ColumnScanState::Within(_) = self.current_state {
            self.value_scan.current()
        } else {
            None
        }
    }

    fn reset(&mut self) {
        self.current_state = ColumnScanState::Before;
    }

    fn pos(&self) -> Option<usize> {
        unimplemented!("This functions is not implemented for column operators");
    }

    fn narrow(&mut self, _interval: std::ops::Range<usize>) {
        unimplemented!("This functions is not implemented for column operators");
    }
}

#[cfg(test)]
mod test {
    use crate::columnar::{
        column::{vector::ColumnVector, Column},
        columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum},
        operations::filter_interval::{
            ColumnScanFilterInterval, IntervalBoundConstant, IntervalBoundVariable,
        },
    };

    #[test]
    fn columnscan_filter_constant() {
        let column = ColumnVector::new(vec![-1i64, 1, 2, 4, 8]);
        let scan_column = ColumnScanCell::new(ColumnScanEnum::Vector(column.iter()));

        let mut scan_filter = ColumnScanFilterInterval::new(
            &scan_column,
            vec![],
            vec![],
            vec![],
            vec![
                IntervalBoundConstant::exclusive(0),
                IntervalBoundConstant::inclusive(-1),
            ],
            vec![
                IntervalBoundConstant::inclusive(7),
                IntervalBoundConstant::exclusive(9),
            ],
        );

        assert_eq!(scan_filter.current(), None);
        assert_eq!(scan_filter.next(), Some(1));
        assert_eq!(scan_filter.current(), Some(1));
        assert_eq!(scan_filter.next(), Some(2));
        assert_eq!(scan_filter.current(), Some(2));
        assert_eq!(scan_filter.next(), Some(4));
        assert_eq!(scan_filter.current(), Some(4));
        assert_eq!(scan_filter.next(), None);
        assert_eq!(scan_filter.current(), None);
        assert_eq!(scan_filter.next(), None);
        assert_eq!(scan_filter.current(), None);

        scan_column.reset();
        scan_filter.reset();

        assert_eq!(scan_filter.current(), None);
        assert_eq!(scan_filter.seek(0), Some(1));
        assert_eq!(scan_filter.current(), Some(1));
        assert_eq!(scan_filter.seek(-1), Some(1));
        assert_eq!(scan_filter.current(), Some(1));
        assert_eq!(scan_filter.seek(3), Some(4));
        assert_eq!(scan_filter.current(), Some(4));
        assert_eq!(scan_filter.seek(5), None);
        assert_eq!(scan_filter.current(), None);
        assert_eq!(scan_filter.seek(2), None);
        assert_eq!(scan_filter.current(), None);
    }

    #[test]
    fn columnscan_filter_variables() {
        let column_value = ColumnVector::new(vec![1i64, 2, 4, 6, 8, 11, 15]);
        let column_bounds = ColumnVector::new(vec![2i64, 7, 12]);

        let scan_value = ColumnScanCell::new(ColumnScanEnum::Vector(column_value.iter()));
        let scan_lower = ColumnScanCell::new(ColumnScanEnum::Vector(column_bounds.iter()));
        let scan_upper = ColumnScanCell::new(ColumnScanEnum::Vector(column_bounds.iter()));

        scan_lower.next(); // lower_bound = 2
        scan_upper.next();
        scan_upper.next(); // upper_bound = 7

        let mut scan_filter = ColumnScanFilterInterval::new(
            &scan_value,
            vec![&scan_lower, &scan_upper],
            vec![IntervalBoundVariable::inclusive(0)],
            vec![IntervalBoundVariable::exclusive(1)],
            vec![IntervalBoundConstant::exclusive(-2)],
            vec![IntervalBoundConstant::inclusive(11)],
        );

        assert_eq!(scan_filter.current(), None);
        assert_eq!(scan_filter.next(), Some(2));
        assert_eq!(scan_filter.current(), Some(2));
        assert_eq!(scan_filter.next(), Some(4));
        assert_eq!(scan_filter.current(), Some(4));
        assert_eq!(scan_filter.next(), Some(6));
        assert_eq!(scan_filter.current(), Some(6));
        assert_eq!(scan_filter.next(), None);
        assert_eq!(scan_filter.current(), None);

        scan_upper.next(); // upper_bound = 12
        scan_value.reset();
        scan_filter.reset();

        assert_eq!(scan_filter.current(), None);
        assert_eq!(scan_filter.next(), Some(2));
        assert_eq!(scan_filter.current(), Some(2));
        assert_eq!(scan_filter.next(), Some(4));
        assert_eq!(scan_filter.current(), Some(4));
        assert_eq!(scan_filter.next(), Some(6));
        assert_eq!(scan_filter.current(), Some(6));
        assert_eq!(scan_filter.next(), Some(8));
        assert_eq!(scan_filter.current(), Some(8));
        assert_eq!(scan_filter.next(), Some(11));
        assert_eq!(scan_filter.current(), Some(11));
        assert_eq!(scan_filter.next(), None);
        assert_eq!(scan_filter.current(), None);
    }
}
