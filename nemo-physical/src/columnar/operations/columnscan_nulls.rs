use crate::columnar::column_storage::columnscan::ColumnScan;
use crate::datatypes::ColumnDataType;
use std::{fmt::Debug, ops::Range};

/// Cursor position of the scan
#[derive(Debug)]
enum CursorPosition {
    Before,
    At,
    After,
}

/// [`ColumnScan`] which represents a column which contains fresh nulls.
#[derive(Debug)]
pub struct ColumnScanNulls<T>
where
    T: ColumnDataType,
{
    /// The current value.
    value: T,

    /// The amount by which `value` is increased every read
    step: T,

    /// Where the virtual cursor is.
    cursor: CursorPosition,
}
impl<T> ColumnScanNulls<T>
where
    T: ColumnDataType,
{
    /// Constructs a new [`ColumnScanNulls`].
    pub fn new(value: T, step: T) -> Self {
        ColumnScanNulls {
            value,
            step,
            cursor: CursorPosition::Before,
        }
    }
}

impl<T> Iterator for ColumnScanNulls<T>
where
    T: ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.cursor {
            CursorPosition::Before => {
                self.cursor = CursorPosition::At;
                self.value = self.value + self.step;

                Some(self.value)
            }
            CursorPosition::At => {
                self.cursor = CursorPosition::After;
                None
            }
            CursorPosition::After => None,
        }
    }
}

impl<T> ColumnScan for ColumnScanNulls<T>
where
    T: ColumnDataType,
{
    fn seek(&mut self, _value: T) -> Option<T> {
        unimplemented!("ColumnScanNulls does not support seek");
    }

    fn current(&self) -> Option<T> {
        if let CursorPosition::At = self.cursor {
            Some(self.value)
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
    use crate::columnar::column_storage::columnscan::ColumnScan;

    use super::ColumnScanNulls;

    use test_log::test;

    #[test]
    fn test_u64() {
        let mut scan = ColumnScanNulls::new(4u64, 2);
        assert_eq!(scan.current(), None);
        assert_eq!(scan.next(), Some(6));
        assert_eq!(scan.current(), Some(6));
        assert_eq!(scan.next(), None);
        assert_eq!(scan.current(), None);

        scan.reset();

        assert_eq!(scan.current(), None);
        assert_eq!(scan.next(), Some(8));
        assert_eq!(scan.current(), Some(8));
        assert_eq!(scan.next(), None);
        assert_eq!(scan.current(), None);
    }
}
