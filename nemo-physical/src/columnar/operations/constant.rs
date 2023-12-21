//! This module defines [ColumnScanConstant].

use std::{fmt::Debug, ops::Range};

use crate::{columnar::columnscan::ColumnScan, datatypes::ColumnDataType};

/// Position of the cursor in [ColumnScanConstant]
/// relative to the single value its going to outout
#[derive(Debug)]
enum ColumnScanStatus {
    /// Before the constant value
    ///
    /// This is the initial state.
    Before,
    /// At the constant value
    At,
    /// After the constant value
    After,
}

/// [`ColumnScan`] which represents a column which holds one value.
#[derive(Debug)]
pub struct ColumnScanConstant<T>
where
    T: ColumnDataType,
{
    /// The value that is always returned
    constant: Option<T>,

    /// Status of this scan
    status: ColumnScanStatus,
}
impl<T> ColumnScanConstant<T>
where
    T: ColumnDataType,
{
    /// Constructs a new [`ColumnScanConstant`].
    pub(crate) fn new(constant: Option<T>) -> Self {
        Self {
            constant,
            status: ColumnScanStatus::Before,
        }
    }

    /// Set a new constant value that will be returned by this scan
    pub(crate) fn set_constant(&mut self, constant: Option<T>) {
        self.constant = constant;
    }
}

impl<T> Iterator for ColumnScanConstant<T>
where
    T: ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.status {
            ColumnScanStatus::Before => {
                self.status = ColumnScanStatus::At;
                self.constant
            }
            ColumnScanStatus::At => {
                self.status = ColumnScanStatus::After;
                None
            }
            ColumnScanStatus::After => None,
        }
    }
}

impl<T> ColumnScan for ColumnScanConstant<T>
where
    T: ColumnDataType,
{
    fn seek(&mut self, value: T) -> Option<T> {
        if let Some(constant) = self.constant {
            if value <= constant {
                if let ColumnScanStatus::After = self.status {
                    None
                } else {
                    self.status = ColumnScanStatus::At;
                    self.constant
                }
            } else {
                self.status = ColumnScanStatus::After;
                None
            }
        } else {
            None
        }
    }

    fn current(&self) -> Option<T> {
        if let ColumnScanStatus::At = self.status {
            self.constant
        } else {
            None
        }
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
    use crate::columnar::columnscan::ColumnScan;

    use super::ColumnScanConstant;

    use test_log::test;

    #[test]
    fn columnscan_constant_some() {
        let mut scan = ColumnScanConstant::new(Some(4u32));
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

    #[test]
    fn columnscan_constant_none() {
        let mut scan = ColumnScanConstant::new(None);
        assert_eq!(scan.current(), None);
        assert_eq!(scan.next(), None);
        assert_eq!(scan.current(), None);
        assert_eq!(scan.next(), None);
        assert_eq!(scan.current(), None);

        scan.reset();

        assert_eq!(scan.current(), None);
        assert_eq!(scan.seek(3i64), None);
        assert_eq!(scan.seek(4), None);
        assert_eq!(scan.seek(10), None);
        assert_eq!(scan.seek(1), None);
    }
}
