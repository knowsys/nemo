use super::{
    super::traits::columnscan::{ColumnScan, ColumnScanCell},
    arithmetic::{
        expression::{ArithmeticTree, ArithmeticTreeLeaf},
        traits::ArithmeticOperations,
    },
    condition::statement::ConditionStatement,
};
use crate::datatypes::ColumnDataType;
use std::{fmt::Debug, ops::Range};

#[derive(Debug)]
enum ColumnScanStatus {
    /// Iterator is before the lower bound of the interval.
    Before,
    /// Iterator is withing the bounds of the interval.
    Within,
    /// Iterator is past the upper bound of the interval.
    After,
}

#[derive(Debug)]
enum Bound<T> {
    Inclusive(ArithmeticTree<T>),
    Exclusive(ArithmeticTree<T>),
}

#[derive(Debug)]
enum BoundKind<T> {
    Lower(Bound<T>),
    Upper(Bound<T>),
}

/// [`ArithmeticTree`] will reference the `value_scan` with this value
const VALUE_SCAN_INDEX: usize = 0;

impl<T> ConditionStatement<T>
where
    T: Clone,
{
    /// Check if condition is of the type
    /// `value_scan <op> <expression>`
    /// where `<expression>` must not depend on `value_scan`
    fn special_form<'a>(
        tree_value: &'a ArithmeticTree<T>,
        tree_other: &'a ArithmeticTree<T>,
    ) -> bool {
        let value_is_reference = if let ArithmeticTree::Reference(index) = tree_value {
            *index == VALUE_SCAN_INDEX
        } else {
            false
        };

        let other_not_contains_value_reference = tree_other.leaves().iter().all(|l| {
            if let ArithmeticTreeLeaf::Reference(index) = l {
                *index != VALUE_SCAN_INDEX
            } else {
                true
            }
        });

        value_is_reference && other_not_contains_value_reference
    }

    fn as_bound(&self) -> Option<BoundKind<T>> {
        match self {
            ConditionStatement::Unequal(_, _) => None,
            ConditionStatement::Equal(left, right) => {
                if Self::special_form(left, right) {
                    Some(BoundKind::Lower(Bound::Inclusive(right.clone())))
                } else if Self::special_form(right, left) {
                    Some(BoundKind::Lower(Bound::Inclusive(left.clone())))
                } else {
                    None
                }
            }
            ConditionStatement::LessThan(left, right) => {
                if Self::special_form(left, right) {
                    Some(BoundKind::Upper(Bound::Exclusive(right.clone())))
                } else if Self::special_form(right, left) {
                    Some(BoundKind::Lower(Bound::Exclusive(left.clone())))
                } else {
                    None
                }
            }
            ConditionStatement::LessThanEqual(left, right) => {
                if Self::special_form(left, right) {
                    Some(BoundKind::Upper(Bound::Inclusive(right.clone())))
                } else if Self::special_form(right, left) {
                    Some(BoundKind::Lower(Bound::Inclusive(left.clone())))
                } else {
                    None
                }
            }
            ConditionStatement::GreaterThan(left, right) => {
                if Self::special_form(left, right) {
                    Some(BoundKind::Lower(Bound::Exclusive(right.clone())))
                } else if Self::special_form(right, left) {
                    Some(BoundKind::Upper(Bound::Exclusive(left.clone())))
                } else {
                    None
                }
            }
            ConditionStatement::GreaterThanEqual(left, right) => {
                if Self::special_form(left, right) {
                    Some(BoundKind::Lower(Bound::Inclusive(right.clone())))
                } else if Self::special_form(right, left) {
                    Some(BoundKind::Upper(Bound::Inclusive(left.clone())))
                } else {
                    None
                }
            }
        }
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
}
impl<'a, T> ColumnScanRestrictValues<'a, T>
where
    T: 'a + ColumnDataType + ArithmeticOperations,
{
    /// Constructs a new [`ColumnScanRestrictValues`].
    pub fn new(
        value_scan: &'a ColumnScanCell<'a, T>,
        input_scans: Vec<&'a ColumnScanCell<'a, T>>,
        conditions: Vec<ConditionStatement<T>>,
    ) -> ColumnScanRestrictValues<'a, T> {
        // Find conditions that are lower/upper bounds for the currently restricted scan
        let mut lower_bounds = Vec::<Bound<T>>::new();
        let mut upper_bounds = Vec::<Bound<T>>::new();

        let conditions = conditions
            .into_iter()
            .filter(|condition| {
                if let Some(bound) = condition.as_bound() {
                    match bound {
                        BoundKind::Lower(b) => {
                            lower_bounds.push(b);
                        }
                        BoundKind::Upper(b) => {
                            upper_bounds.push(b);
                        }
                    }

                    return false;
                }

                true
            })
            .collect();

        ColumnScanRestrictValues {
            value_scan,
            input_scans,
            conditions,
            lower_bounds,
            upper_bounds,
            status: ColumnScanStatus::Before,
        }
    }

    /// Prepares input values to evaluate the [`ArithmeticTree`] in a condition
    /// Since there is special handling for `self.value_scan` this inserts a placeholder at the front.
    /// (We do not read value_scan yet because it might only be set later)
    fn get_referenced_values(&self) -> Vec<T> {
        let mut result = vec![T::zero()];

        result.extend(self.input_scans.iter().map(|s| {
            s.current()
                .expect("Every referenced scan must have a value.")
        }));

        result
    }

    /// Set `self.value_scan` such that it satisfies each of the `self.lower_bounds`
    fn satisfy_lower_bounds(&mut self, referenced_values: &[T]) -> Option<()> {
        for lower_bound in &self.lower_bounds {
            match lower_bound {
                Bound::Inclusive(tree) => {
                    let lower_value = tree.evaluate(referenced_values)?;
                    self.value_scan.seek(lower_value);
                }
                Bound::Exclusive(tree) => {
                    let lower_value = tree.evaluate(referenced_values)?;
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
        Some(())
    }

    /// Check if `self.value_scan` satisfies each of the `self.upper_bounds`.
    /// Returns `None` if a expression was undefined.
    fn check_upper_bounds(&self, referenced_values: &[T]) -> Option<bool> {
        for bound in &self.upper_bounds {
            match bound {
                Bound::Inclusive(tree) => {
                    let upper_value = tree.evaluate(referenced_values)?;

                    if self.current()? > upper_value {
                        return Some(false);
                    }
                }
                Bound::Exclusive(tree) => {
                    let upper_value = tree.evaluate(referenced_values)?;

                    if self.current()? >= upper_value {
                        return Some(false);
                    }
                }
            }
        }

        Some(true)
    }

    /// Check if `self.value_scan` satisfies each of the `self.conditions`.
    /// Returns `None` if a expression was undefined.
    fn check_conditions(&self, referenced_values: &[T]) -> Option<bool> {
        for condition in &self.conditions {
            if !condition.evaluate(referenced_values)? {
                return Some(false);
            }
        }

        Some(true)
    }
}

impl<'a, T> Iterator for ColumnScanRestrictValues<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let mut referenced_values = self.get_referenced_values();

        match self.status {
            ColumnScanStatus::Before => {
                self.satisfy_lower_bounds(&referenced_values)?;
                self.status = ColumnScanStatus::Within;
            }
            ColumnScanStatus::Within => {
                self.value_scan.next();
            }
            ColumnScanStatus::After => return None,
        }

        loop {
            referenced_values[VALUE_SCAN_INDEX] = self.value_scan.current()?;

            if self.check_conditions(&referenced_values)? {
                break;
            } else {
                self.value_scan.next();
            }
        }

        if self.check_upper_bounds(&referenced_values)? {
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
        let mut referenced_values = self.get_referenced_values();

        match self.status {
            ColumnScanStatus::Before => {
                self.satisfy_lower_bounds(&referenced_values);
                self.status = ColumnScanStatus::Within;

                self.value_scan.seek(value);
            }
            ColumnScanStatus::Within => {
                self.value_scan.seek(value);
            }
            ColumnScanStatus::After => return None,
        }

        loop {
            referenced_values[VALUE_SCAN_INDEX] = self.value_scan.current()?;

            if self.check_conditions(&referenced_values)? {
                break;
            } else {
                self.value_scan.next();
            }
        }

        if self.check_upper_bounds(&referenced_values)? {
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
    use crate::columnar::{
        column_types::vector::ColumnVector,
        operations::{
            arithmetic::expression::ArithmeticTree, condition::statement::ConditionStatement,
        },
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
            vec![ConditionStatement::Equal(
                ArithmeticTree::Reference(0),
                ArithmeticTree::Constant(4),
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
            vec![ConditionStatement::Equal(
                ArithmeticTree::Reference(0),
                ArithmeticTree::Constant(7),
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
                ConditionStatement::GreaterThan(
                    ArithmeticTree::Reference(0),
                    ArithmeticTree::Constant(1),
                ),
                ConditionStatement::LessThanEqual(
                    ArithmeticTree::Reference(0),
                    ArithmeticTree::Constant(4),
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
                ConditionStatement::GreaterThan(
                    ArithmeticTree::Reference(0),
                    ArithmeticTree::Reference(1),
                ),
                ConditionStatement::LessThanEqual(
                    ArithmeticTree::Reference(0),
                    ArithmeticTree::Reference(2),
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
                ConditionStatement::Unequal(
                    ArithmeticTree::Reference(0),
                    ArithmeticTree::Reference(1),
                ),
                ConditionStatement::Unequal(
                    ArithmeticTree::Reference(0),
                    ArithmeticTree::Reference(2),
                ),
                ConditionStatement::Unequal(
                    ArithmeticTree::Reference(0),
                    ArithmeticTree::Constant(5),
                ),
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
                ConditionStatement::GreaterThan(
                    ArithmeticTree::Reference(0),
                    ArithmeticTree::Reference(1),
                ),
                ConditionStatement::LessThanEqual(
                    ArithmeticTree::Reference(0),
                    ArithmeticTree::Reference(2),
                ),
                ConditionStatement::Unequal(
                    ArithmeticTree::Reference(0),
                    ArithmeticTree::Reference(3),
                ),
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

        // value^2 < value * filter + 7
        // value * 2 >= filter
        let mut restrict_scan = ColumnScanRestrictValues::new(
            &value_iter,
            vec![&filter_iter],
            vec![
                ConditionStatement::LessThan(
                    ArithmeticTree::Multiplication(vec![
                        ArithmeticTree::Reference(0),
                        ArithmeticTree::Reference(0),
                    ]),
                    ArithmeticTree::Addition(vec![
                        ArithmeticTree::Multiplication(vec![
                            ArithmeticTree::Reference(0),
                            ArithmeticTree::Reference(1),
                        ]),
                        ArithmeticTree::Constant(7),
                    ]),
                ),
                ConditionStatement::GreaterThanEqual(
                    ArithmeticTree::Multiplication(vec![
                        ArithmeticTree::Reference(0),
                        ArithmeticTree::Constant(2),
                    ]),
                    ArithmeticTree::Reference(1),
                ),
            ],
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
