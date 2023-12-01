use crate::{
    arithmetic::expression::StackProgram,
    columnar::column_storage::columnscan::{ColumnScan, ColumnScanCell},
    datatypes::ColumnDataType,
};
use std::{fmt::Debug, iter::repeat_with, ops::Range};

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
    /// The [`StackProgram`] references [`ColumnScan`]s by its position in this `Vec`.
    column_scans: Vec<&'a ColumnScanCell<'a, T>>,

    /// The current value.
    value: Option<T>,

    /// [`StackProgram`] representing the operation that will be performed.
    expression: StackProgram<T>,

    /// Where the virtual cursor is.
    cursor: CursorPosition,

    /// Buffer for the referenced input values
    inputs: Box<[T]>,

    /// The stack for computing
    stack: Vec<T>,
}

impl<'a, T> ColumnScanArithmetic<'a, T>
where
    T: ColumnDataType,
{
    /// Constructs a new [`ColumnScanArithmetic`].
    pub fn new(column_scans: Vec<&'a ColumnScanCell<'a, T>>, expression: StackProgram<T>) -> Self {
        let inputs = repeat_with(T::zero).take(column_scans.len()).collect();
        let stack_size = expression.space_requirement();

        Self {
            column_scans,
            value: None,
            expression,
            cursor: CursorPosition::Before,
            stack: Vec::with_capacity(stack_size),
            inputs,
        }
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
                for (index, column_scan) in self.column_scans.iter().enumerate() {
                    self.inputs[index] = column_scan.current()?;
                }

                self.cursor = CursorPosition::At;
                self.value = self.expression.evaluate(&mut self.stack, &self.inputs);

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
    use crate::{
        arithmetic::expression::{BinaryOperation, StackOperation, StackProgram, StackValue},
        columnar::column_storage::{
            column_vector::{ColumnScanVector, ColumnVector},
            columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum},
        },
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
        // stack notation: a b 2 / 1 - 5 * +
        let operation = StackProgram::new([
            StackOperation::Push(StackValue::Reference(0)),
            StackOperation::Push(StackValue::Reference(1)),
            StackOperation::Push(StackValue::Constant(2)),
            StackOperation::BinaryOperation(BinaryOperation::Division),
            StackOperation::Push(StackValue::Constant(1)),
            StackOperation::BinaryOperation(BinaryOperation::Subtraction),
            StackOperation::Push(StackValue::Constant(5)),
            StackOperation::BinaryOperation(BinaryOperation::Multiplication),
            StackOperation::BinaryOperation(BinaryOperation::Addition),
        ])
        .unwrap();
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
