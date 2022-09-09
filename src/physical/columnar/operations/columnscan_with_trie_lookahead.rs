use super::super::traits::columnscan::{ColumnScan, ColumnScanRc, ColumnScanT};
use crate::physical::{
    datatypes::{ColumnDataType, DataTypeName},
    tabular::traits::triescan::{TrieScan, TrieScanEnum},
};
use std::{cell::UnsafeCell, fmt::Debug, mem, ops::Range, rc::Rc};

/// Column iterator for a column that represents a layer of partial trie iterator
/// Only returns those values which would be included if the trie where to be materialized
#[derive(Debug)]
pub struct ColumnScanWithTrieLookahead<'a, T>
where
    T: 'a + ColumnDataType,
{
    // TODO: we could build something like RangedColumnScanRc and RangedColumnScanRc trie scans
    trie_scan: Rc<UnsafeCell<TrieScanEnum<'a>>>,
    current_layer: usize,
    current: Option<T>,
    datatype_name: DataTypeName,
}

impl<'a, T> ColumnScanWithTrieLookahead<'a, T>
where
    T: 'a + ColumnDataType,
{
    /// Constructs a new ColumnScanWithTrieLookahead for a Column.
    pub fn new(
        trie_scan: Rc<UnsafeCell<TrieScanEnum<'a>>>,
        current_layer: usize,
        datatype_name: DataTypeName,
    ) -> ColumnScanWithTrieLookahead<'a, T> {
        ColumnScanWithTrieLookahead {
            trie_scan,
            current: None,
            current_layer,
            datatype_name,
        }
    }

    /// Returns the column scan of the current layer
    fn get_current_columnscan(&self) -> &ColumnScanRc<'a, T> {
        macro_rules! return_scan_for_datatype {
            ($variant:ident) => {
                unsafe {
                    if let ColumnScanT::$variant(base_scan) = (*self.trie_scan.get()).current_scan().unwrap()
                    {
                        // TODO: get rid of this; having LookaheadScan generic over T is somewhat unintuitive since T does not occur on any of the inputs in new
                        return mem::transmute(base_scan);
                    } else {
                        panic!("Expected a column scan of type {}", stringify!($variant));
                    }
                }
            };
        }

        match self.datatype_name {
            DataTypeName::U64 => return_scan_for_datatype!(U64),
            DataTypeName::Float => return_scan_for_datatype!(Float),
            DataTypeName::Double => return_scan_for_datatype!(Double),
        };
    }

    // Check whether the current value of the given trie scan actually exists
    fn value_exists(&mut self) -> bool {
        if self.current_layer == unsafe { &*self.trie_scan.get() }.get_schema().arity() - 1 {
            return true;
        }

        // Iterate through the trie_scan in a dfs manner
        unsafe { &mut *self.trie_scan.get() }.down();
        let mut current_layer = self.current_layer + 1;
        loop {
            let is_last_layer =
                current_layer >= unsafe { &*self.trie_scan.get() }.get_schema().arity() - 1;
            let next_value = self.get_current_columnscan().next();

            if next_value.is_none() {
                unsafe { &mut *self.trie_scan.get() }.up();
                current_layer -= 1;

                if current_layer == self.current_layer {
                    break;
                }

                continue;
            }

            if !is_last_layer {
                unsafe { &mut *self.trie_scan.get() }.down();
                current_layer += 1;
            } else {
                for _ in
                    self.current_layer..(unsafe { &*self.trie_scan.get() }.get_schema().arity() - 1)
                {
                    unsafe { &mut *self.trie_scan.get() }.up();
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
        self.current = None;

        loop {
            let possible_result = Some(self.get_current_columnscan().next()?);
            if self.value_exists() {
                self.current = possible_result;
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
        self.current = None;

        let possible_result = Some(self.get_current_columnscan().seek(value)?);

        if self.value_exists() {
            self.current = possible_result;
            possible_result
        } else {
            self.next()
        }
    }

    fn current(&mut self) -> Option<T> {
        self.current
    }

    fn reset(&mut self) {
        self.get_current_columnscan().reset();
    }

    fn pos(&self) -> Option<usize> {
        self.get_current_columnscan().pos()
    }
    fn narrow(&mut self, interval: Range<usize>) {
        self.get_current_columnscan().narrow(interval);
    }
}
