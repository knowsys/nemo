//! This module defines [ColumnScanFilterEqual].

use crate::{
    columnar::columnscan::{ColumnScan, ColumnScanCell},
    storagevalues::ColumnDataType,
};

/// [ColumnScanFilterEqual] does only return up to one value.
/// Therefore, it can be only in of three states.
#[derive(Debug)]
enum ColumnScanState {
    /// [ColumnScanFilterEqual] is in its initial position
    Start,
    /// [ColumnScanFilterEqual] points to its output value
    Value,
    /// [ColumnScanFilterEqual] passed its output value
    End,
}

/// [ColumnScan] that restricts a [ColumnScan] to values contained in another [ColumnScan]
#[derive(Debug)]
pub(crate) struct ColumnScanFilterEqual<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// [ColumnScan] that provides the output values for this scan
    value_scan: &'a ColumnScanCell<'a, T>,

    /// The current value of this [ColumnScan] will be used
    /// to restrict the output of `value_scan`.
    reference_scan: &'a ColumnScanCell<'a, T>,

    /// Current [ColumnScanState] of this scan
    state: ColumnScanState,
}

impl<'a, T> ColumnScanFilterEqual<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Create a new [ColumnScanFilterEqual].
    ///
    /// Note: This operator assumes that `reset` is called on this scan
    /// after there has been a change in `reference_scan`,
    /// i.e. this scan should appear at a lower level in the trie than the `reference_scan`.
    #[allow(dead_code)]
    pub(crate) fn new(
        value_scan: &'a ColumnScanCell<'a, T>,
        reference_scan: &'a ColumnScanCell<'a, T>,
    ) -> Self {
        Self {
            value_scan,
            reference_scan,
            state: ColumnScanState::Start,
        }
    }
}

impl<'a, T> Iterator for ColumnScanFilterEqual<'a, T>
where
    T: 'a + ColumnDataType + Eq,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.state {
            ColumnScanState::Start => {
                let reference_value = self.reference_scan.current()?;

                if let Some(value) = self
                    .value_scan
                    .seek(reference_value)
                    .filter(|&next| next == reference_value)
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

impl<'a, T> ColumnScan for ColumnScanFilterEqual<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn seek(&mut self, value: Self::Item) -> Option<Self::Item> {
        let reference_value = self.reference_scan.current()?;

        if value > reference_value {
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

    use super::ColumnScanFilterEqual;

    #[test]
    fn columnscan_equal_basic() {
        let ref_col = ColumnVector::new(vec![0u64, 4, 7]);
        let val_col = ColumnVector::new(vec![1u64, 4, 8]);

        let ref_iter = ColumnScanCell::new(ColumnScanEnum::Vector(ref_col.iter()));
        let val_iter = ColumnScanCell::new(ColumnScanEnum::Vector(val_col.iter()));

        ref_iter.seek(4);

        let mut equal_scan = ColumnScanFilterEqual::new(&val_iter, &ref_iter);
        assert_eq!(equal_scan.current(), None);
        assert_eq!(equal_scan.next(), Some(4));
        assert_eq!(equal_scan.current(), Some(4));
        assert_eq!(equal_scan.next(), None);
        assert_eq!(equal_scan.current(), None);
        assert_eq!(equal_scan.next(), None);
        assert_eq!(equal_scan.current(), None);

        let ref_iter = ColumnScanCell::new(ColumnScanEnum::Vector(ref_col.iter()));
        let val_iter = ColumnScanCell::new(ColumnScanEnum::Vector(val_col.iter()));

        ref_iter.seek(7);

        let mut equal_scan = ColumnScanFilterEqual::new(&ref_iter, &val_iter);
        assert_eq!(equal_scan.current(), None);
        assert_eq!(equal_scan.next(), None);
        assert_eq!(equal_scan.current(), None);
        assert_eq!(equal_scan.next(), None);
        assert_eq!(equal_scan.current(), None);
    }
}
