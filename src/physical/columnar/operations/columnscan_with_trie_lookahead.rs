use super::super::traits::columnscan::{ColumnScan, ColumnScanRc};
use crate::physical::{
    datatypes::ColumnDataType,
    tabular::traits::{
        table_schema::TableSchema,
        triescan::{TrieScan, TrieScanEnum},
    },
};
use std::{cell::RefCell, fmt::Debug, ops::Range, rc::Rc};

/// Column iterator for a column that represents a layer of partial trie iterator
/// Only returns those values which would be included if the trie where to be materialized
#[derive(Debug)]
pub struct ColumnScanWithTrieLookahead<'a, T>
where
    T: 'a + ColumnDataType,
{
    trie_scan: Rc<RefCell<TrieScanEnum<'a>>>,
    current_layer: usize,
    col_scan: ColumnScanRc<'a, T>, // NOTE: this needs to be the current column scan from the trie_scan
}

impl<'a, T> ColumnScanWithTrieLookahead<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Constructs a new ColumnScanWithTrieLookahead for a Column.
    pub fn new(
        trie_scan: Rc<RefCell<TrieScanEnum<'a>>>,
        current_layer: usize,
        col_scan: ColumnScanRc<'a, T>,
    ) -> ColumnScanWithTrieLookahead<'a, T> {
        ColumnScanWithTrieLookahead {
            trie_scan,
            current_layer,
            col_scan,
        }
    }

    // Check whether the current value of the given trie scan actually exists
    fn value_exists(&mut self) -> bool {
        if self.current_layer == self.trie_scan.borrow().get_schema().arity() - 1 {
            return true;
        }

        // Iterate through the trie_scan in a dfs manner
        self.trie_scan.borrow_mut().down();
        let mut current_layer = self.current_layer + 1;
        loop {
            let is_last_layer = current_layer >= self.trie_scan.borrow().get_schema().arity() - 1;
            let next_value = self.trie_scan.borrow_mut().current_scan().unwrap().next();

            if next_value.is_none() {
                self.trie_scan.borrow_mut().up();
                current_layer -= 1;

                if current_layer == self.current_layer {
                    break;
                }

                continue;
            }

            if !is_last_layer {
                self.trie_scan.borrow_mut().down();
                current_layer += 1;
            } else {
                let arity = self.trie_scan.borrow().get_schema().arity();
                for _ in self.current_layer..(arity - 1) {
                    self.trie_scan.borrow_mut().up();
                }

                return true;
            }
        }

        false
    }
}

impl<'a, T> Iterator for ColumnScanWithTrieLookahead<'a, T>
where
    T: 'a + ColumnDataType,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let possible_result = Some(self.col_scan.next()?);
            if self.value_exists() {
                return possible_result;
            }
        }
    }
}

impl<'a, T> ColumnScan for ColumnScanWithTrieLookahead<'a, T>
where
    T: 'a + ColumnDataType,
{
    fn seek(&mut self, value: T) -> Option<T> {
        let possible_result = Some(self.col_scan.seek(value)?);

        if self.value_exists() {
            possible_result
        } else {
            self.next()
        }
    }

    fn current(&mut self) -> Option<T> {
        self.col_scan.current()
    }

    fn reset(&mut self) {
        self.col_scan.reset();
    }

    fn pos(&self) -> Option<usize> {
        self.col_scan.pos()
    }
    fn narrow(&mut self, interval: Range<usize>) {
        self.col_scan.narrow(interval);
    }
}
