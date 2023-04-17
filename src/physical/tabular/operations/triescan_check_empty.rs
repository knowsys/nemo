use std::cell::UnsafeCell;

use crate::physical::{
    columnar::traits::columnscan::ColumnScanT, datatypes::StorageTypeName,
    tabular::traits::triescan::TrieScan,
};

/// [`TrieScan`] which represents an empty table.
#[derive(Debug)]
pub struct TrieScanEmpty {
    types: Vec<StorageTypeName>,
}

impl TrieScanEmpty {
    /// Construct new [`TrieScanJoin`] object.
    /// Assumes that each entry in `bindings``is sorted and does not contain duplicates
    pub fn new() -> Self {
        Self { types: Vec::new() }
    }
}

impl Default for TrieScanEmpty {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> TrieScan<'a> for TrieScanEmpty {
    fn up(&mut self) {}

    fn down(&mut self) {}

    fn current_scan(&self) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        None
    }

    fn get_scan(&self, _index: usize) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        None
    }

    fn get_types(&self) -> &Vec<StorageTypeName> {
        &self.types
    }
}
