use super::super::traits::columnscan::ColumnScan;
use crate::{
    columnar::traits::columnscan::ColumnScanCell, datatypes::ColumnDataType,
    util::tagged_tree::TaggedTree,
};
use std::{fmt::Debug, ops::Range};

/// Operands that appear in a [`ColumnScanArithmetic`]
#[derive(Clone)]
pub enum ArithmeticOperand<T> {
    /// Value is the given constant.
    Constant(T),
    /// Value is read off the column scan with the given index.
    ColumnScan(usize),
}

/// Operation that can be exectued by a [`ColumnScanArithmetic`].
#[derive(Copy, Clone, Debug)]
pub enum ArithmeticOperation {
    /// Value is the sum of the values of the given subtrees.
    Addition,
    /// Value is the difference between the value of the first subtree and the second.
    Subtraction,
    /// Value is the product of the values of the given subtrees.
    Multiplication,
    /// Value is the quotient of the value of the first subtree and the second.
    Division,
}

impl<T: Debug> Debug for ArithmeticOperand<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Constant(constant) => write!(f, "{:?}", constant),
            Self::ColumnScan(index) => write!(f, "Column({:?})", index),
        }
    }
}

/// A [`TaggedTree`] that represents a series of arithmetic operations to be executed by the [`ColumnScanArithmetic`].
pub type OperationTree<T> = TaggedTree<ArithmeticOperation, ArithmeticOperand<T>>;

impl<T> OperationTree<T> {
    /// Return a list of all the column scan indices used in this tree.
    pub fn input_indices(&self) -> Vec<&usize> {
        self.leaves()
            .into_iter()
            .filter_map(|operand| match operand {
                ArithmeticOperand::Constant(_) => None,
                ArithmeticOperand::ColumnScan(i) => Some(i),
            })
            .collect()
    }

    /// Return a list with mutable references to all column scan indices used in this tree.
    pub fn input_indices_mut(&mut self) -> Vec<&mut usize> {
        self.leaves_mut()
            .into_iter()
            .filter_map(|operand| match operand {
                ArithmeticOperand::Constant(_) => None,
                ArithmeticOperand::ColumnScan(i) => Some(i),
            })
            .collect()
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

    fn evaluate_recursive(&self, tree: &OperationTree<T>) -> Option<T> {
        match tree {
            TaggedTree::Leaf(operand) => match operand {
                ArithmeticOperand::ColumnScan(index) => self.column_scans[*index].current(),
                ArithmeticOperand::Constant(constant) => Some(*constant),
            },
            TaggedTree::Node { tag, subtrees } => match tag {
                ArithmeticOperation::Addition => {
                    let mut result = T::zero();
                    for subtree in subtrees {
                        result = result + self.evaluate_recursive(subtree)?;
                    }
                    Some(result)
                }
                ArithmeticOperation::Subtraction => {
                    let left = self.evaluate_recursive(&subtrees[0])?;
                    let right = self.evaluate_recursive(&subtrees[1])?;

                    Some(left - right)
                }
                ArithmeticOperation::Multiplication => {
                    let mut result = T::one();
                    for subtree in subtrees {
                        result = result * self.evaluate_recursive(subtree)?;
                    }
                    Some(result)
                }
                ArithmeticOperation::Division => {
                    let left = self.evaluate_recursive(&subtrees[0])?;
                    let right = self.evaluate_recursive(&subtrees[1])?;

                    if right == T::zero() {
                        return None;
                    }

                    Some(left / right)
                }
            },
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
        } else if self.cursor == CursorPosition::Before {
            self.next();
            self.seek(seek_value)
        } else {
            None
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
        operations::columnscan_arithmetic::{ArithmeticOperand, OperationTree},
        traits::columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum},
    };

    use super::{ArithmeticOperation, ColumnScanArithmetic};

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
        let operation: OperationTree<u64> = OperationTree::tree(
            ArithmeticOperation::Addition,
            vec![
                OperationTree::leaf(ArithmeticOperand::ColumnScan(0)),
                OperationTree::tree(
                    ArithmeticOperation::Multiplication,
                    vec![
                        OperationTree::tree(
                            ArithmeticOperation::Subtraction,
                            vec![
                                OperationTree::tree(
                                    ArithmeticOperation::Division,
                                    vec![
                                        OperationTree::leaf(ArithmeticOperand::ColumnScan(1)),
                                        OperationTree::leaf(ArithmeticOperand::Constant(2)),
                                    ],
                                ),
                                OperationTree::leaf(ArithmeticOperand::Constant(1)),
                            ],
                        ),
                        OperationTree::leaf(ArithmeticOperand::Constant(5)),
                    ],
                ),
            ],
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
