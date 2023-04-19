use crate::physical::columnar::traits::columnscan::ColumnScanCell;
use crate::physical::datatypes::ColumnDataType;
use crate::physical::tabular::operations::triescan_prune::SharedTrieScanPruneState;
use std::fmt::Debug;
use std::ops::Range;

use super::super::traits::columnscan::ColumnScan;

/// Forwards calls to this column though it's [`SharedTrieScanPruneState`] to the input trie.
/// See `TrieScanPrune` for more information.
#[derive(Debug)]
pub struct ColumnScanPrune<'a, T>
where
    T: 'a + ColumnDataType,
{
    state: SharedTrieScanPruneState<'a>,
    /// Index this column scan corresponds to in the input trie
    column_scan_index: usize,
    reference_scan: &'a ColumnScanCell<'a, T>,
}

impl<'a, T> ColumnScanPrune<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Constructs a new column scan. This should not be called directory, but only by `TrieScanPrune`
    pub fn new(
        state: SharedTrieScanPruneState<'a>,
        column_scan_index: usize,
        reference_scan: &'a ColumnScanCell<'a, T>,
    ) -> Self {
        Self {
            state,
            column_scan_index,
            reference_scan,
        }
    }
}

impl<'a, T> Iterator for ColumnScanPrune<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe { (*self.state.get()).get_next_value(self.column_scan_index) };
        self.reference_scan.current()
    }
}

impl<'a, T> ColumnScan for ColumnScanPrune<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn seek(&mut self, minimum_value: T) -> Option<T> {
        loop {
            match self.next() {
                Some(next_value) => {
                    if next_value >= minimum_value {
                        return Some(next_value);
                    }
                }
                None => return None,
            }
        }
    }

    fn current(&self) -> Option<T> {
        if unsafe { (*self.state.get()).is_column_peeked(self.column_scan_index) } {
            None
        } else {
            self.reference_scan.current()
        }
    }

    fn reset(&mut self) {
        unimplemented!("The reset function is currently not implemented for ColumnScanPrune. If required, this could probably be implemented in the future. Doing so would require not violating the guarantees of the TrieScanPrune.");
    }

    fn pos(&self) -> Option<usize> {
        unimplemented!("This function is not implemented for column operators");
    }
    fn narrow(&mut self, _interval: Range<usize>) {
        unimplemented!("This function is not implemented for column operators");
    }
}
