//! This module defines [ColumnScanPass].

use crate::columnar::columnscan::{ColumnScan, ColumnScanCell};
use crate::datatypes::ColumnDataType;
use std::{fmt::Debug, ops::Range};

/// Dummy Iterator that defers everything to its sub iterator
#[derive(Debug)]
pub struct ColumnScanPass<'a, T>
where
    T: 'a + ColumnDataType,
{
    reference_scan: &'a ColumnScanCell<'a, T>,
}
impl<'a, T> ColumnScanPass<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Constructs a new [`ColumnScanPass`].
    pub fn new(reference_scan: &'a ColumnScanCell<'a, T>) -> ColumnScanPass<'a, T> {
        ColumnScanPass { reference_scan }
    }
}

impl<'a, T> Iterator for ColumnScanPass<'a, T>
where
    T: 'a + ColumnDataType + PartialOrd + Eq,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.reference_scan.next()
    }
}

impl<'a, T> ColumnScan for ColumnScanPass<'a, T>
where
    T: 'a + ColumnDataType + PartialOrd + Eq,
{
    fn seek(&mut self, value: T) -> Option<T> {
        self.reference_scan.seek(value)
    }

    fn current(&self) -> Option<T> {
        self.reference_scan.current()
    }

    fn reset(&mut self) {
        self.reference_scan.reset()
    }

    fn pos(&self) -> Option<usize> {
        self.reference_scan.pos()
    }
    fn narrow(&mut self, interval: Range<usize>) {
        self.reference_scan.narrow(interval);
    }
}

#[cfg(test)]
mod test {
    use crate::columnar::{
        column::{vector::ColumnVector, Column},
        columnscan::{ColumnScan, ColumnScanCell, ColumnScanEnum},
    };

    use super::ColumnScanPass;
    use test_log::test;

    #[test]
    fn test_u64() {
        let ref_col = ColumnVector::new(vec![0u32, 4, 7]);
        let ref_col_iter = ColumnScanCell::new(ColumnScanEnum::ColumnScanVector(ref_col.iter()));

        let mut pass_scan = ColumnScanPass::new(&ref_col_iter);

        assert_eq!(pass_scan.current(), None);
        assert_eq!(pass_scan.next(), Some(0));
        assert_eq!(pass_scan.current(), Some(0));
        assert_eq!(pass_scan.seek(6), Some(7));
        assert_eq!(pass_scan.current(), Some(7));
        assert_eq!(pass_scan.next(), None);
        assert_eq!(pass_scan.current(), None);
    }
}
