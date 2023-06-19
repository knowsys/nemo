use super::super::traits::columnscan::ColumnScan;
use crate::{
    columnar::traits::columnscan::ColumnScanCell,
    datatypes::{ColumnDataType, DataTypeName, Double, Float},
    generate_datatype_forwarder,
    util::mapping::permutation::Permutation,
};
use std::{fmt::Debug, ops::Range};

/// Tree representing a mathematical term.
#[derive(Debug, Clone)]
pub enum OperationTree<T> {
    /// Value is tied to the current value of a column_scan
    Variable(usize),
    /// Value is the given constant.
    Constant(T),
    /// Value is the sum of the values of the two subtrees.
    Addition(Box<OperationTree<T>>, Box<OperationTree<T>>),
    /// Value is the difference of the value of the two subtrees.
    Subtraction(Box<OperationTree<T>>, Box<OperationTree<T>>),
    /// Value is the product of the values of the two subtrees.
    Multiplication(Box<OperationTree<T>>, Box<OperationTree<T>>),
    /// Value is the quotient of the values of the  two subtrees.
    Division(Box<OperationTree<T>>, Box<OperationTree<T>>),
}

impl<T> OperationTree<T> {
    /// Create a new [`OperationTree`] which evaluates
    /// to the value pointed to by [`ColumnScan`] with the given index.
    pub fn variable(index: usize) -> Self {
        Self::Variable(index)
    }

    /// Create a new [`OperationTree`] which evaluates to the given constant.
    pub fn constant(value: T) -> Self {
        Self::Constant(value)
    }

    /// Create a new [`OperationTree`] which evaluates
    /// to the sum of the given [`OperationTree`].
    pub fn addition(left: OperationTree<T>, right: OperationTree<T>) -> Self {
        Self::Addition(Box::new(left), Box::new(right))
    }

    /// Create a new [`OperationTree`] which evaluates
    /// to the difference of the given [`OperationTree`].
    pub fn subtraction(left: OperationTree<T>, right: OperationTree<T>) -> Self {
        Self::Subtraction(Box::new(left), Box::new(right))
    }

    /// Create a new [`OperationTree`] which evaluates
    /// to the product of the given [`OperationTree`].
    pub fn multiplication(left: OperationTree<T>, right: OperationTree<T>) -> Self {
        Self::Multiplication(Box::new(left), Box::new(right))
    }

    /// Create a new [`OperationTree`] which evaluates
    /// to the quotient of the given [`OperationTree`].
    pub fn division(left: OperationTree<T>, right: OperationTree<T>) -> Self {
        Self::Division(Box::new(left), Box::new(right))
    }
}

impl<T> OperationTree<T> {
    fn input_indices_recursive(tree: &OperationTree<T>) -> Vec<usize> {
        let mut result = Vec::<usize>::new();

        result.extend(match tree {
            OperationTree::Variable(variable) => vec![*variable],
            OperationTree::Constant(_) => vec![],
            OperationTree::Addition(left, right)
            | OperationTree::Subtraction(left, right)
            | OperationTree::Multiplication(left, right)
            | OperationTree::Division(left, right) => {
                let mut sub_vector = Vec::<usize>::new();
                sub_vector.extend(Self::input_indices_recursive(left));
                sub_vector.extend(Self::input_indices_recursive(right));

                sub_vector
            }
        });

        result
    }

    /// Return all indices that are used in the input of the [`OperationTree`].
    pub fn input_indices(&self) -> Vec<usize> {
        Self::input_indices_recursive(self)
    }

    fn apply_permutation_recursive(tree: &mut OperationTree<T>, permutation: &Permutation) {
        match tree {
            OperationTree::Variable(variable) => *variable = permutation.get(*variable),
            OperationTree::Constant(_) => {}
            OperationTree::Addition(left, right)
            | OperationTree::Subtraction(left, right)
            | OperationTree::Multiplication(left, right)
            | OperationTree::Division(left, right) => {
                Self::apply_permutation_recursive(left, permutation);
                Self::apply_permutation_recursive(right, permutation);
            }
        }
    }

    /// Apply a [`Permutation`] to the input column indices of the [`OperationTree`].
    pub fn apply_permutation(&mut self, permutation: &Permutation) {
        Self::apply_permutation_recursive(self, permutation);
    }
}

/// Variant of an [`OperationTree`] for each [`crate::datatypes::StorageTypeName`].
#[derive(Debug, Clone)]
pub enum OperationTreeT {
    /// Data type [`u32`].
    U32(OperationTree<u32>),
    /// Data type [`u64`].
    U64(OperationTree<u64>),
    /// Data type [`i64`]
    I64(OperationTree<i64>),
    /// Data type [`Float`]
    Float(OperationTree<Float>),
    /// Data type [`Double`]
    Double(OperationTree<Double>),
}

generate_datatype_forwarder!(forward_to_type);

impl OperationTreeT {
    /// Return all indices that are used in the input of the [`OperationTreeT`].
    pub fn input_indices(&self) -> Vec<usize> {
        forward_to_type!(self, input_indices())
    }

    /// Return the [`DataTypeName`] for this operation.
    pub fn data_type(&self) -> DataTypeName {
        match self {
            OperationTreeT::U32(_) => DataTypeName::U32,
            OperationTreeT::U64(_) => DataTypeName::U64,
            OperationTreeT::I64(_) => DataTypeName::I64,
            OperationTreeT::Float(_) => DataTypeName::Float,
            OperationTreeT::Double(_) => DataTypeName::Double,
        }
    }
}

/// Cursor position of the scan
#[derive(Debug, Eq, PartialEq)]
enum CursorPosition {
    Before,
    At,
    After,
}

/// [`ColumnScan`] which represents a column which contains fresh nulls.
#[derive(Debug)]
pub struct ColumnScanArithmetic<'a, T>
where
    T: ColumnDataType,
{
    /// List of subiterators that will be used in computing the arithmetic operation.
    /// [`OperationTree`] refers to a [`ColumnScan`] by its position in this `Vec`.
    column_scans: Vec<&'a ColumnScanCell<'a, T>>,

    /// The current value.
    value: Option<T>,

    /// [`OperationTree`] representing the operation that will be performed
    operation: OperationTree<T>,

    /// Where the virtual cursor is.
    cursor: CursorPosition,
}
impl<'a, T> ColumnScanArithmetic<'a, T>
where
    T: ColumnDataType,
{
    /// Constructs a new [`ColumnScanArithmetic`].
    pub fn new(column_scans: Vec<&'a ColumnScanCell<'a, T>>, operation: OperationTree<T>) -> Self {
        Self {
            column_scans,
            operation,
            value: None,
            cursor: CursorPosition::Before,
        }
    }

    fn evaluate_recursive(&self, operation: &OperationTree<T>) -> Option<T> {
        match operation {
            OperationTree::Variable(index) => self.column_scans[*index].current(),
            OperationTree::Constant(constant) => Some(*constant),
            OperationTree::Addition(left, right) => Some(
                self.evaluate_recursive(left.as_ref())?
                    + self.evaluate_recursive(right.as_ref())?,
            ),
            OperationTree::Subtraction(left, right) => Some(
                self.evaluate_recursive(left.as_ref())?
                    - self.evaluate_recursive(right.as_ref())?,
            ),
            OperationTree::Multiplication(left, right) => Some(
                self.evaluate_recursive(left.as_ref())?
                    * self.evaluate_recursive(right.as_ref())?,
            ),
            OperationTree::Division(left, right) => Some(
                self.evaluate_recursive(left.as_ref())?
                    / self.evaluate_recursive(right.as_ref())?,
            ),
        }
    }

    fn evaluate(&self) -> Option<T> {
        self.evaluate_recursive(&self.operation)
    }
}

impl<'a, T> Iterator for ColumnScanArithmetic<'a, T>
where
    T: ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.cursor {
            CursorPosition::Before => {
                self.cursor = CursorPosition::At;
                self.value = self.evaluate();

                self.value
            }
            CursorPosition::At => {
                self.cursor = CursorPosition::After;
                self.value = None;
                None
            }
            CursorPosition::After => None,
        }
    }
}

impl<'a, T> ColumnScan for ColumnScanArithmetic<'a, T>
where
    T: ColumnDataType,
{
    fn seek(&mut self, seek_value: T) -> Option<T> {
        if let Some(value) = self.value {
            if seek_value > value {
                self.next()
            } else {
                self.current()
            }
        } else {
            if self.cursor == CursorPosition::Before {
                self.next();
                self.seek(seek_value)
            } else {
                None
            }
        }
    }

    fn current(&self) -> Option<T> {
        self.value
    }

    fn reset(&mut self) {
        self.cursor = CursorPosition::Before;
        self.value = None;
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
        column_types::vector::{ColumnScanVector, ColumnVector},
        operations::columnscan_arithmetic::OperationTree,
        traits::columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum},
    };

    use super::ColumnScanArithmetic;

    use test_log::test;

    #[test]
    fn column_scan_arithmetic() {
        let column_a: ColumnVector<u64> = ColumnVector::new(vec![10]);
        let mut scan_a = ColumnScanEnum::ColumnScanVector(ColumnScanVector::new(&column_a));
        scan_a.next();
        let scan_a_cell = ColumnScanCell::new(scan_a);

        let column_b: ColumnVector<u64> = ColumnVector::new(vec![7]);
        let mut scan_b = ColumnScanEnum::ColumnScanVector(ColumnScanVector::new(&column_b));
        scan_b.next();
        let scan_b_cell = ColumnScanCell::new(scan_b);

        // a + (b/2 - 1) * 5
        let operation: OperationTree<u64> = OperationTree::addition(
            OperationTree::variable(0),
            OperationTree::multiplication(
                OperationTree::subtraction(
                    OperationTree::division(OperationTree::variable(1), OperationTree::constant(2)),
                    OperationTree::constant(1),
                ),
                OperationTree::constant(5),
            ),
        );
        let scans = vec![&scan_a_cell, &scan_b_cell];
        let mut arithmetic = ColumnScanArithmetic::new(scans, operation);

        assert_eq!(arithmetic.current(), None);
        assert_eq!(arithmetic.next(), Some(20));
        assert_eq!(arithmetic.current(), Some(20));
        assert_eq!(arithmetic.next(), None);
        assert_eq!(arithmetic.current(), None);
        assert_eq!(arithmetic.next(), None);
        assert_eq!(arithmetic.current(), None);
    }
}
