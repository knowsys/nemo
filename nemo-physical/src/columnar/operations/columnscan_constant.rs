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

/// [`ColumnScan`] which represents a column which holds one (possibly repeated) value.
#[derive(Debug)]
pub struct ColumnScanConstant<T>
where
    T: ColumnDataType,
{
    /// The value that is always returned.
    value: T,

    /// Where the virtual cursor is.
    cursor: CursorPosition,
}
impl<T> ColumnScanConstant<T>
where
    T: ColumnDataType,
{
    /// Constructs a new [`ColumnScanConstant`].
    pub fn new(value: T) -> Self {
        ColumnScanConstant {
            value,
            cursor: CursorPosition::Before,
        }
    }
}

impl<T> Iterator for ColumnScanConstant<T>
where
    T: ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.cursor {
            CursorPosition::Before => {
                self.cursor = CursorPosition::At;
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

impl<T> ColumnScan for ColumnScanConstant<T>
where
    T: ColumnDataType,
{
    fn seek(&mut self, value: T) -> Option<T> {
        if value <= self.value {
            if let CursorPosition::After = self.cursor {
                None
            } else {
                self.cursor = CursorPosition::At;

                Some(self.value)
            }
        } else {
            self.cursor = CursorPosition::After;
            None
        }
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

    use super::ColumnScanConstant;

    use test_log::test;

    #[test]
    fn test_u64() {
        let mut scan = ColumnScanConstant::new(4u32);
        assert_eq!(scan.current(), None);
        assert_eq!(scan.next(), Some(4));
        assert_eq!(scan.current(), Some(4));
        assert_eq!(scan.next(), None);
        assert_eq!(scan.current(), None);

        scan.reset();

        assert_eq!(scan.current(), None);
        assert_eq!(scan.seek(3), Some(4));
        assert_eq!(scan.seek(4), Some(4));
        assert_eq!(scan.seek(10), None);
        assert_eq!(scan.seek(1), None);
    }
}
