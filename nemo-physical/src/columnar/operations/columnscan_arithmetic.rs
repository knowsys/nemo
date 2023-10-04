use super::super::traits::columnscan::ColumnScan;
use crate::{columnar::traits::columnscan::ColumnScanCell, datatypes::ColumnDataType};
use std::{fmt::Debug, ops::Range};

#[derive(Copy, Clone, Debug)]
/// A Binary operation performed during execution of [`ColumnScanArithmetic`]
pub enum BinaryOperation {
    /// Value is the sum of the values of the given subtrees.
    Addition,
    /// Value is the difference between the value of the first subtree and the second.
    Subtraction,
    /// Value is the product of the values of the given subtrees.
    Multiplication,
    /// Value is the quotient of the value of the first subtree and the second.
    Division,
}

/// Operation that can be exectued by a [`ColumnScanArithmetic`].
#[derive(Clone)]
pub enum ArithmeticOperation<T> {
    /// Value is the given constant.
    PushConst(T),
    /// Value is read off the column scan with the given index.
    PushRef(usize),
    /// Perform a binary operation.
    BinaryOperation(BinaryOperation),
}

impl<T> Debug for ArithmeticOperation<T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PushConst(constant) => write!(f, "{:?}", constant),
            Self::PushRef(index) => write!(f, "Column({:?})", index),
            Self::BinaryOperation(op) => write!(f, "{:?}", op),
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
    operation: Vec<ArithmeticOperation<T>>,

    /// Where the virtual cursor is.
    cursor: CursorPosition,
}

impl<'a, T> ColumnScanArithmetic<'a, T>
where
    T: ColumnDataType,
{
    /// Constructs a new [`ColumnScanArithmetic`].
    pub fn new(
        column_scans: Vec<&'a ColumnScanCell<'a, T>>,
        operation: Vec<ArithmeticOperation<T>>,
    ) -> Self {
        Self {
            column_scans,
            operation,
            value: None,
            cursor: CursorPosition::Before,
        }
    }

    fn evaluate(&self) -> Option<T> {
        let mut stack = Vec::new();

        for operation in &self.operation {
            match operation {
                ArithmeticOperation::PushConst(c) => stack.push(*c),
                ArithmeticOperation::PushRef(index) => {
                    stack.push(self.column_scans[*index].current()?)
                }
                ArithmeticOperation::BinaryOperation(op) => {
                    let rhs = stack.pop()?;
                    let lhs = stack.pop()?;

                    stack.push(match op {
                        BinaryOperation::Addition => lhs.add(rhs),
                        BinaryOperation::Subtraction => lhs.sub(rhs),
                        BinaryOperation::Multiplication => lhs.mul(rhs),
                        BinaryOperation::Division => (!lhs.is_zero()).then(|| lhs.div(rhs))?,
                    })
                }
            }
        }

        stack.pop()
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
        operations::columnscan_arithmetic::BinaryOperation,
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
        // in reverse polish notation: a b 2 / 1 - 5 * +
        let operation = vec![
            ArithmeticOperation::PushRef(0),
            ArithmeticOperation::PushRef(1),
            ArithmeticOperation::PushConst(2),
            ArithmeticOperation::BinaryOperation(BinaryOperation::Division),
            ArithmeticOperation::PushConst(1),
            ArithmeticOperation::BinaryOperation(BinaryOperation::Subtraction),
            ArithmeticOperation::PushConst(5),
            ArithmeticOperation::BinaryOperation(BinaryOperation::Multiplication),
            ArithmeticOperation::BinaryOperation(BinaryOperation::Addition),
        ];

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
