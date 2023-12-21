use crate::columnar::columnscan::{ColumnScan, ColumnScanCell};
use crate::datatypes::ColumnDataType;
use std::{fmt::Debug, ops::Range};

/// Cursor position of the scan
#[derive(Debug)]
enum CursorPosition {
    Before,
    At,
    After,
}

/// [`ColumnScan`] which copies the values of another [`ColumnScan`]
#[derive(Debug)]
pub struct ColumnScanCopy<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Values are copied from this scan.
    reference_scan: &'a ColumnScanCell<'a, T>,

    /// Where the virtual cursor is.
    cursor: CursorPosition,
}
impl<'a, T> ColumnScanCopy<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Constructs a new [`ColumnScanCopy`].
    pub fn new(reference_scan: &'a ColumnScanCell<'a, T>) -> ColumnScanCopy<'a, T> {
        ColumnScanCopy {
            reference_scan,
            cursor: CursorPosition::Before,
        }
    }
}

impl<'a, T> Iterator for ColumnScanCopy<'a, T>
where
    T: 'a + ColumnDataType + Eq,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.cursor {
            CursorPosition::Before => {
                self.cursor = CursorPosition::At;
                self.reference_scan.current()
            }
            CursorPosition::At => {
                self.cursor = CursorPosition::After;
                None
            }
            CursorPosition::After => None,
        }
    }
}

impl<'a, T> ColumnScan for ColumnScanCopy<'a, T>
where
    T: 'a + ColumnDataType + Eq,
{
    fn seek(&mut self, value: T) -> Option<T> {
        if let Some(reference_value) = self.current() {
            if value <= reference_value {
                self.cursor = CursorPosition::At;
                Some(reference_value)
            } else {
                self.cursor = CursorPosition::After;
                None
            }
        } else {
            None
        }
    }

    fn current(&self) -> Option<T> {
        if let CursorPosition::At = self.cursor {
            self.reference_scan.current()
        } else {
            None
        }
    }

    fn reset(&mut self) {
        self.cursor = CursorPosition::Before;
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
        column::{vector::ColumnVector, Column},
        columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum},
    };

    use super::ColumnScanCopy;
    use test_log::test;

    #[test]
    fn test_u64() {
        let ref_col = ColumnVector::new(vec![0u64, 4, 7]);
        let ref_iter = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(ref_col.iter()));

        let mut copy_scan = ColumnScanCopy::new(&ref_iter);

        ref_iter.seek(4);

        assert_eq!(copy_scan.current(), None);
        assert_eq!(copy_scan.next(), Some(4));
        assert_eq!(copy_scan.current(), Some(4));
        assert_eq!(copy_scan.next(), None);
        assert_eq!(copy_scan.current(), None);

        ref_iter.next();
        copy_scan.reset();

        assert_eq!(copy_scan.current(), None);
        assert_eq!(copy_scan.next(), Some(7));
        assert_eq!(copy_scan.current(), Some(7));
        assert_eq!(copy_scan.next(), None);
        assert_eq!(copy_scan.current(), None);
    }
}
