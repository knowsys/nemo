//! This module defines [ColumnScanFilterConstant].

use crate::{
    columnar::columnscan::{ColumnScan, ColumnScanCell},
    datatypes::ColumnDataType,
};

/// [ColumnScanFilterConstant] does only return up to one value.
/// Therefore, it can be only in of three states.
#[derive(Debug)]
enum ColumnScanState {
    /// [ColumnScanFilterConstant] is in its initial position
    Start,
    /// [ColumnScanFilterConstant] points to its output value
    Value,
    /// [ColumnScanFilterConstant] passed its output value
    End,
}

/// [ColumnScan] that restricts a [ColumnScan] to values that are equal to some given constant
#[derive(Debug)]
pub(crate) struct ColumnScanFilterConstant<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// [ColumnScan] that provides the output values for this scan
    value_scan: &'a ColumnScanCell<'a, T>,

    /// The constant that the `value_scan` will be restricted to
    constant: T,

    /// Current [ColumnScanState] of this scan
    state: ColumnScanState,
}

impl<'a, T> ColumnScanFilterConstant<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Create a new [ColumnScanFilterConstant].
    pub(crate) fn new(value_scan: &'a ColumnScanCell<'a, T>, constant: T) -> Self {
        Self {
            value_scan,
            constant,
            state: ColumnScanState::Start,
        }
    }
}

impl<'a, T> Iterator for ColumnScanFilterConstant<'a, T>
where
    T: 'a + ColumnDataType + Eq,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            ColumnScanState::Start => {
                if let Some(value) = self
                    .value_scan
                    .seek(self.constant)
                    .filter(|&next| next == self.constant)
                {
                    self.state = ColumnScanState::Value;
                    Some(value)
                } else {
                    self.state = ColumnScanState::End;
                    None
                }
            }
            ColumnScanState::Value => {
                self.state = ColumnScanState::End;
                None
            }
            ColumnScanState::End => None,
        }
    }
}

impl<'a, T> ColumnScan for ColumnScanFilterConstant<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn seek(&mut self, value: Self::Item) -> Option<Self::Item> {
        if value > self.constant {
            self.state = ColumnScanState::End;
        }

        if let ColumnScanState::End = self.state {
            None
        } else {
            self.state = ColumnScanState::Start;
            self.next()
        }
    }

    fn current(&self) -> Option<Self::Item> {
        if matches!(self.state, ColumnScanState::Value) {
            self.value_scan.current()
        } else {
            None
        }
    }

    fn reset(&mut self) {
        self.state = ColumnScanState::Start
    }

    fn pos(&self) -> Option<usize> {
        unimplemented!("This functions is not implemented for column operators");
    }

    fn narrow(&mut self, _interval: std::ops::Range<usize>) {
        unimplemented!("This functions is not implemented for column operators");
    }
}

#[cfg(test)]
mod test {
    use crate::columnar::{
        column::{vector::ColumnVector, Column},
        columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum},
    };

    use super::ColumnScanFilterConstant;

    #[test]
    fn columnscan_filter_constant_basic() {
        let column_value = ColumnVector::new(vec![1i64, 4, 8]);
        let value_scan = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(column_value.iter()));

        let mut constant_scan = ColumnScanFilterConstant::new(&value_scan, 4);

        assert_eq!(constant_scan.current(), None);
        assert_eq!(constant_scan.next(), Some(4));
        assert_eq!(constant_scan.current(), Some(4));
        assert_eq!(constant_scan.seek(2), Some(4));
        assert_eq!(constant_scan.current(), Some(4));
        assert_eq!(constant_scan.seek(4), Some(4));
        assert_eq!(constant_scan.current(), Some(4));
        assert_eq!(constant_scan.next(), None);
        assert_eq!(constant_scan.current(), None);
        assert_eq!(constant_scan.next(), None);
        assert_eq!(constant_scan.current(), None);

        value_scan.reset();
        constant_scan.reset();

        assert_eq!(constant_scan.current(), None);
        assert_eq!(constant_scan.seek(3), Some(4));
        assert_eq!(constant_scan.current(), Some(4));
        assert_eq!(constant_scan.seek(7), None);
        assert_eq!(constant_scan.current(), None);
        assert_eq!(constant_scan.seek(2), None);
        assert_eq!(constant_scan.current(), None);
        assert_eq!(constant_scan.next(), None);
        assert_eq!(constant_scan.current(), None);
    }
}
