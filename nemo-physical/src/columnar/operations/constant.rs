//! This module defines [ColumnScanConstant].

use std::{fmt::Debug, ops::Range};

use crate::{columnar::columnscan::ColumnScan, datatypes::ColumnDataType};

/// [ColumnScanConstant] returns exactly one value.
/// Therefore, it can be only in of three states.
#[derive(Debug)]
enum ColumnScanState {
    /// [ColumnScanConstant] is in its initial position
    Start,
    /// [ColumnScanConstant] points to its output value
    Value,
    /// [ColumnScanConstant] passed its output value
    End,
}

/// [ColumnScan] which represents a column which holds one value.
#[derive(Debug)]
pub(crate) struct ColumnScanConstant<T>
where
    T: ColumnDataType,
{
    /// The value that is always returned
    constant: Option<T>,

    /// Current [ColumnScanState] of this scan
    state: ColumnScanState,
}
impl<T> ColumnScanConstant<T>
where
    T: ColumnDataType,
{
    /// Constructs a new [ColumnScanConstant].
    pub(crate) fn new(constant: Option<T>) -> Self {
        Self {
            constant,
            state: ColumnScanState::Start,
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
        match self.state {
            ColumnScanState::Start => {
                self.state = ColumnScanState::Value;
                self.constant
            }
            ColumnScanState::Value => {
                self.state = ColumnScanState::End;
                None
            }
            ColumnScanState::End => None,
        }
    }
}

impl<T> ColumnScan for ColumnScanConstant<T>
where
    T: ColumnDataType,
{
    fn seek(&mut self, value: T) -> Option<T> {
        if value > self.constant? {
            self.state = ColumnScanState::End;
        }

        if let ColumnScanState::End = self.state {
            None
        } else {
            self.state = ColumnScanState::Value;
            self.constant
        }
    }

    fn current(&self) -> Option<T> {
        if let ColumnScanState::Value = self.state {
            self.constant
        } else {
            None
        }
    }

    fn reset(&mut self) {
        self.state = ColumnScanState::Start;
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

    #[cfg(not(miri))]
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
