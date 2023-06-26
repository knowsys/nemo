use super::super::traits::columnscan::ColumnScan;
use crate::{
    columnar::traits::columnscan::ColumnScanCell, datatypes::ColumnDataType, util::OperationTree,
};
use std::{fmt::Debug, ops::Range};

/// Represents an [`OperationTree`] where variables are given as indices to a vector of column scans.
pub type IndexOperationTree<TypeConstant> = OperationTree<TypeConstant, usize>;

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
    operation: IndexOperationTree<T>,

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
        operation: IndexOperationTree<T>,
    ) -> Self {
        Self {
            column_scans,
            operation,
            value: None,
            cursor: CursorPosition::Before,
        }
    }

    fn evaluate_recursive(&self, operation: &IndexOperationTree<T>) -> Option<T> {
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
        operations::columnscan_arithmetic::{IndexOperationTree, OperationTree},
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
        let operation: IndexOperationTree<u64> = OperationTree::addition(
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
