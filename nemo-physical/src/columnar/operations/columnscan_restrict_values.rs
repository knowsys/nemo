use crate::{
    arithmetic::{
        expression::{StackProgram, StackValue},
        traits::ArithmeticOperations,
    },
    columnar::columnscan::{ColumnScan, ColumnScanCell},
    condition::statement::{ConditionOperator, ConditionStatement},
    datatypes::ColumnDataType,
};
use std::{fmt::Debug, iter::repeat_with, ops::Range};

#[derive(Debug)]
enum ColumnScanStatus {
    /// Iterator is before the lower bound of the interval.
    Before,
    /// Iterator is withing the bounds of the interval.
    Within,
    /// Iterator is past the upper bound of the interval.
    After,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum BoundEndpoint {
    Inclusive,
    Exclusive,
}

#[derive(Debug, Clone)]
struct Bound<T> {
    endpoint: BoundEndpoint,
    value: StackProgram<T>,
}

#[derive(Debug, Copy, Clone)]
struct UpperEndpoint(Option<BoundEndpoint>);

#[derive(Debug, Copy, Clone)]
struct LowerEndpoint(Option<BoundEndpoint>);

/// The [`StackProgram`] will reference the `value_scan` (i.e. the
/// column being restricted) with this value
pub const VALUE_SCAN_INDEX: usize = 0;

impl<T> ConditionStatement<T>
where
    T: Clone,
{
    fn try_into_bounds(self) -> Result<(UpperEndpoint, LowerEndpoint, StackProgram<T>), Self> {
        if self.operation == ConditionOperator::Unequal {
            return Err(self);
        }

        if let Some(StackValue::Reference(index)) = self.lhs.trivial() {
            if index == VALUE_SCAN_INDEX {
                let upper_bound = UpperEndpoint(match self.operation {
                    ConditionOperator::Equal => Some(BoundEndpoint::Inclusive),
                    ConditionOperator::Unequal => unreachable!("checked above"),
                    ConditionOperator::LessThan => Some(BoundEndpoint::Exclusive),
                    ConditionOperator::LessThanEqual => Some(BoundEndpoint::Inclusive),
                    ConditionOperator::GreaterThan => None,
                    ConditionOperator::GreaterThanEqual => None,
                });

                let lower_bound = LowerEndpoint(match self.operation {
                    ConditionOperator::Equal => Some(BoundEndpoint::Inclusive),
                    ConditionOperator::Unequal => unreachable!("checked above"),
                    ConditionOperator::LessThan => None,
                    ConditionOperator::LessThanEqual => None,
                    ConditionOperator::GreaterThan => Some(BoundEndpoint::Exclusive),
                    ConditionOperator::GreaterThanEqual => Some(BoundEndpoint::Inclusive),
                });

                return Ok((upper_bound, lower_bound, self.rhs));
            }
        } else if let Some(StackValue::Reference(index)) = self.rhs.trivial() {
            if index == VALUE_SCAN_INDEX {
                let lower_bound = LowerEndpoint(match self.operation {
                    ConditionOperator::Equal => Some(BoundEndpoint::Inclusive),
                    ConditionOperator::Unequal => unreachable!("checked above"),
                    ConditionOperator::LessThan => Some(BoundEndpoint::Exclusive),
                    ConditionOperator::LessThanEqual => Some(BoundEndpoint::Inclusive),
                    ConditionOperator::GreaterThan => None,
                    ConditionOperator::GreaterThanEqual => None,
                });

                let upper_bound = UpperEndpoint(match self.operation {
                    ConditionOperator::Equal => Some(BoundEndpoint::Inclusive),
                    ConditionOperator::Unequal => unreachable!("checked above"),
                    ConditionOperator::LessThan => None,
                    ConditionOperator::LessThanEqual => None,
                    ConditionOperator::GreaterThan => Some(BoundEndpoint::Exclusive),
                    ConditionOperator::GreaterThanEqual => Some(BoundEndpoint::Inclusive),
                });

                return Ok((upper_bound, lower_bound, self.lhs));
            }
        }

        Err(self)
    }
}

/// [`ColumnScan`] which allows its sub scan to only jump to a certain value
#[derive(Debug)]
pub struct ColumnScanRestrictValues<'a, T>
where
    T: 'a + ColumnDataType + ArithmeticOperations,
{
    /// Current scan the values of which are being restricted
    value_scan: &'a ColumnScanCell<'a, T>,
    /// Additional scans which may provide values for the conditions.
    /// It is assumed that each of them is pointing at some value.
    input_scans: Vec<&'a ColumnScanCell<'a, T>>,

    /// Conditions that should be fulfilled.
    /// `value_scan` can be referenced with 0
    /// `input_scans` are referenced with their index incremented by 1.
    conditions: Vec<ConditionStatement<T>>,
    /// Lower bounds for the value of of the currently restricted scan.
    /// These are used instead of conditions as an optimization,
    /// as this allows you to use `seek`.
    lower_bounds: Vec<Bound<T>>,
    /// Upper bounds for the value of of the currently restricted scan.
    /// These are used instead of conditions as an optimization,
    /// as this allows you to abort computation early.
    upper_bounds: Vec<Bound<T>>,

    /// Status of this scan.
    status: ColumnScanStatus,

    /// Buffer for the current input values
    referenced_values: Box<[T]>,

    /// Buffer for stack calculations
    stack: Vec<T>,
}

impl<'a, T> ColumnScanRestrictValues<'a, T>
where
    T: 'a + ColumnDataType + ArithmeticOperations,
{
    /// Constructs a new [`ColumnScanRestrictValues`].
    pub fn new(
        value_scan: &'a ColumnScanCell<'a, T>,
        input_scans: Vec<&'a ColumnScanCell<'a, T>>,
        input_conditions: Vec<ConditionStatement<T>>,
    ) -> ColumnScanRestrictValues<'a, T> {
        // Find conditions that are lower/upper bounds for the currently restricted scan
        let mut lower_bounds = Vec::<Bound<T>>::new();
        let mut upper_bounds = Vec::<Bound<T>>::new();
        let mut conditions = Vec::new();

        let mut stack_size = 0;

        for condition in input_conditions {
            match condition.try_into_bounds() {
                Ok((UpperEndpoint(upper), LowerEndpoint(lower), value)) => {
                    stack_size = std::cmp::max(stack_size, value.space_requirement());

                    if let Some(endpoint) = upper {
                        upper_bounds.push(Bound {
                            endpoint,
                            value: value.clone(),
                        });
                    }
                    if let Some(endpoint) = lower {
                        lower_bounds.push(Bound { endpoint, value });
                    }
                }
                Err(condition) => {
                    stack_size = std::cmp::max(stack_size, condition.space_requirement());
                    conditions.push(condition);
                }
            }
        }

        // allocate buffer for referenced values
        let referenced_values = repeat_with(T::zero).take(input_scans.len() + 1).collect();

        ColumnScanRestrictValues {
            value_scan,
            input_scans,
            conditions,
            lower_bounds,
            upper_bounds,
            status: ColumnScanStatus::Before,
            referenced_values,
            stack: Vec::with_capacity(stack_size),
        }
    }

    /// Prepares input values to evaluate the [`ArithmeticTree`] in a condition
    /// Since there is special handling for `self.value_scan` this inserts a placeholder at the front.
    /// (We do not read value_scan yet because it might only be set later)
    fn update_referenced_values(&mut self) {
        // the 0s  entry is where the VALUE_SCAN_INDEX points
        self.referenced_values[0] = T::zero();

        for (index, scan) in self.input_scans.iter().enumerate() {
            self.referenced_values[index + 1] = scan
                .current()
                .expect("Every input should have a current value.")
        }
    }

    /// Set `self.value_scan` such that it satisfies each of the `self.lower_bounds`
    ///
    /// Returns `false` if the expression encoding the lower bound is undefined.
    /// In such a case, we can enter the state `ColumnScanStatus::After`
    /// as the bounds do not change with respect to `self.value_scan`.
    ///
    /// Returns `true` in all other cases
    fn satisfy_lower_bounds(&mut self) -> bool {
        for lower_bound in &self.lower_bounds {
            let Some(lower_value) = lower_bound
                .value
                .evaluate(&mut self.stack, &self.referenced_values)
            else {
                return false;
            };

            match lower_bound.endpoint {
                BoundEndpoint::Inclusive => {
                    self.value_scan.seek(lower_value);
                }
                BoundEndpoint::Exclusive => {
                    if let Some(seeked) = self.value_scan.seek(lower_value) {
                        if seeked == lower_value {
                            self.value_scan.next();
                        }
                    }
                }
            }
        }

        if self.lower_bounds.is_empty() {
            self.value_scan.next();
        }

        self.status = ColumnScanStatus::Within;
        true
    }

    /// Check if `self.value_scan` satisfies each of the `self.upper_bounds`.
    ///
    /// Returns `false` if there is a bound that is not satisfied
    /// or its expression is undefined.
    ///
    /// Returns `true` if all bounds are satisfied.
    fn check_upper_bounds(&mut self) -> bool {
        for bound in &self.upper_bounds {
            let Some(upper_value) = bound
                .value
                .evaluate(&mut self.stack, &self.referenced_values)
            else {
                return false;
            };

            let Some(current_value) = self.current() else {
                return false;
            };

            let condition = match bound.endpoint {
                BoundEndpoint::Inclusive => current_value <= upper_value,
                BoundEndpoint::Exclusive => current_value < upper_value,
            };

            if !condition {
                return false;
            }
        }

        true
    }

    /// Check if `self.value_scan` satisfies each of the `self.conditions`.
    ///
    /// Returns `false` if there is an condition which is not satisfied
    /// or which is undefined.
    ///
    /// Returns `true` if all conditions are satisfied.
    fn check_conditions(&mut self) -> bool {
        for condition in &self.conditions {
            if let Some(true) = condition.evaluate(&mut self.stack, &self.referenced_values) {
                continue;
            }

            return false;
        }

        true
    }
}

impl<'a, T> Iterator for ColumnScanRestrictValues<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.update_referenced_values();

        match self.status {
            ColumnScanStatus::Before => {
                if !self.satisfy_lower_bounds() {
                    self.status = ColumnScanStatus::After;
                    return None;
                }

                self.status = ColumnScanStatus::Within;
            }
            ColumnScanStatus::Within => {
                self.value_scan.next();
            }
            ColumnScanStatus::After => return None,
        }

        loop {
            self.referenced_values[VALUE_SCAN_INDEX] = self.value_scan.current()?;

            if self.check_conditions() {
                break;
            } else {
                self.value_scan.next();
            }
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
        self.update_referenced_values();

        match self.status {
            ColumnScanStatus::Before => {
                if !self.satisfy_lower_bounds() {
                    self.status = ColumnScanStatus::After;
                    return None;
                }

                self.status = ColumnScanStatus::Within;

                self.value_scan.seek(value);
            }
            ColumnScanStatus::Within => {
                self.value_scan.seek(value);
            }
            ColumnScanStatus::After => return None,
        }

        loop {
            self.referenced_values[VALUE_SCAN_INDEX] = self.value_scan.current()?;

            if self.check_conditions() {
                break;
            } else {
                self.value_scan.next();
            }
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

        self.value_scan.current()
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
        arithmetic::expression::{BinaryOperation, StackOperation, StackProgram, StackValue},
        columnar::{
            column::{vector::ColumnVector, Column},
            columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum},
        },
        condition::statement::{ConditionOperator, ConditionStatement},
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
            vec![ConditionStatement::equal(
                StackValue::Reference(0),
                StackValue::Constant(4),
            )],
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
            vec![ConditionStatement::equal(
                StackValue::Reference(0),
                StackValue::Constant(7),
            )],
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
            vec![
                ConditionStatement::greater_than(StackValue::Reference(0), StackValue::Constant(1)),
                ConditionStatement::less_than_equal(
                    StackValue::Reference(0),
                    StackValue::Constant(4),
                ),
            ],
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

        lower_bound.next(); // lower_bound = 2
        upper_bound.next();
        upper_bound.next(); // upper_bound = 7

        let mut restrict_scan = ColumnScanRestrictValues::new(
            &value_iter,
            vec![&lower_bound, &upper_bound],
            vec![
                ConditionStatement::greater_than(
                    StackValue::Reference(0),
                    StackValue::Reference(1),
                ),
                ConditionStatement::less_than_equal(
                    StackValue::Reference(0),
                    StackValue::Reference(2),
                ),
            ],
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

        unequal_1.next(); // unequal_1 = 1
        unequal_2.next();
        unequal_2.next(); // unequal_2 = 3

        let mut restrict_scan = ColumnScanRestrictValues::new(
            &value_iter,
            vec![&unequal_1, &unequal_2],
            vec![
                ConditionStatement::unequal(StackValue::Reference(0), StackValue::Reference(1)),
                ConditionStatement::unequal(StackValue::Reference(0), StackValue::Reference(2)),
                ConditionStatement::unequal(StackValue::Reference(0), StackValue::Constant(5)),
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

        lower_bound.next(); // lower_bound = 2
        upper_bound.next();
        upper_bound.next();
        upper_bound.next(); // upper_bound = 7
        unequals.next();
        unequals.next(); // unequals = 5

        let mut restrict_scan = ColumnScanRestrictValues::new(
            &value_iter,
            vec![&lower_bound, &upper_bound, &unequals],
            vec![
                ConditionStatement::greater_than(
                    StackValue::Reference(0),
                    StackValue::Reference(1),
                ),
                ConditionStatement::less_than_equal(
                    StackValue::Reference(0),
                    StackValue::Reference(2),
                ),
                ConditionStatement::unequal(StackValue::Reference(0), StackValue::Reference(3)),
            ],
        );

        assert_eq!(restrict_scan.current(), None);
        assert_eq!(restrict_scan.next(), Some(3));
        assert_eq!(restrict_scan.current(), Some(3));
        assert_eq!(restrict_scan.next(), Some(4));
        assert_eq!(restrict_scan.current(), Some(4));
        assert_eq!(restrict_scan.next(), Some(6));
        assert_eq!(restrict_scan.current(), Some(6));
        assert_eq!(restrict_scan.next(), None);
        assert_eq!(restrict_scan.current(), None);
    }

    #[test]
    fn restrict_complex() {
        let column_value = ColumnVector::new(vec![1u64, 2, 3, 4, 5, 6, 8]);
        let column_filter = ColumnVector::new(vec![2u64, 5, 7]);

        let value_iter = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(column_value.iter()));
        let filter_iter =
            ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(column_filter.iter()));

        filter_iter.next();
        filter_iter.next(); // filter = 5

        // value*value < value * filter + 7
        // stack notation: [value value *] < [value filter * 7 +]
        let condition_one = ConditionStatement {
            operation: ConditionOperator::LessThan,
            lhs: StackProgram::new([
                StackOperation::Push(StackValue::Reference(0)),
                StackOperation::Push(StackValue::Reference(0)),
                StackOperation::BinaryOperation(BinaryOperation::Multiplication),
            ])
            .unwrap(),
            rhs: StackProgram::new([
                StackOperation::Push(StackValue::Reference(0)),
                StackOperation::Push(StackValue::Reference(1)),
                StackOperation::BinaryOperation(BinaryOperation::Multiplication),
                StackOperation::Push(StackValue::Constant(7)),
                StackOperation::BinaryOperation(BinaryOperation::Addition),
            ])
            .unwrap(),
        };

        // value * 2 >= filter
        // stack notation: [value 2 *] >= filter
        let condition_two = ConditionStatement {
            operation: ConditionOperator::GreaterThanEqual,
            lhs: StackProgram::new([
                StackOperation::Push(StackValue::Reference(0)),
                StackOperation::Push(StackValue::Constant(2)),
                StackOperation::BinaryOperation(BinaryOperation::Multiplication),
            ])
            .unwrap(),
            rhs: StackValue::Reference(1).into(),
        };

        let mut restrict_scan = ColumnScanRestrictValues::new(
            &value_iter,
            vec![&filter_iter],
            vec![condition_one, condition_two],
        );

        assert_eq!(restrict_scan.current(), None);
        assert_eq!(restrict_scan.next(), Some(3));
        assert_eq!(restrict_scan.current(), Some(3));
        assert_eq!(restrict_scan.next(), Some(4));
        assert_eq!(restrict_scan.current(), Some(4));
        assert_eq!(restrict_scan.seek(5), Some(5));
        assert_eq!(restrict_scan.current(), Some(5));
        assert_eq!(restrict_scan.seek(6), Some(6));
        assert_eq!(restrict_scan.current(), Some(6));
        assert_eq!(restrict_scan.next(), None);
        assert_eq!(restrict_scan.current(), None);
    }
}
