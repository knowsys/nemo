use crate::generate_forwarder;
use crate::physical::columnar::traits::columnscan::ColumnScanT;
use crate::physical::tabular::operations::{
    TrieScanJoin, TrieScanMinus, TrieScanProject, TrieScanReorderProject,
    TrieScanSelectEqual, TrieScanSelectValue, TrieScanUnion,
};
use crate::physical::tabular::table_types::trie::TrieScanGeneric;
use std::cell::UnsafeCell;
use std::fmt::Debug;

use super::table_schema::TableColumnTypes;

/// Iterator for a Trie datastructure.
/// Allows for vertical traversal through the tree and can return
/// its current position as a [`ColumnScanT`] object.
pub trait TrieScan<'a>: Debug {
    /// Return to the upper layer.
    fn up(&mut self);

    /// Enter the next layer based on the position of the iterator in the current layer.
    fn down(&mut self);

    /// Return the current position of the scan as a [`ColumnScanT`].
    fn current_scan(&self) -> Option<&UnsafeCell<ColumnScanT<'a>>>;

    /// Return the underlying [`ColumnScanT`] object given an index.
    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColumnScanT<'a>>>;

    /// Return the underlying [`TableSchema`].
    fn get_types(&self) -> &TableColumnTypes;
}

/// Enum for TrieScan Variants
#[derive(Debug)]
pub enum TrieScanEnum<'a> {
    /// Case TrieScanGeneric
    TrieScanGeneric(TrieScanGeneric<'a>),
    /// Case TrieScanJoin
    TrieScanJoin(TrieScanJoin<'a>),
    /// Case TrieScanProject
    TrieScanProject(TrieScanProject<'a>),
    /// Case TrieScanReorderProject
    TrieScanReorderProject(TrieScanReorderProject<'a>),
    /// Case TrieScanMinus
    TrieScanMinus(TrieScanMinus<'a>),
    /// Case TrieScanUnion
    TrieScanUnion(TrieScanUnion<'a>),
    /// Case TrieScanSelectEqual
    TrieScanSelectEqual(TrieScanSelectEqual<'a>),
    /// Case TrieScanSelectValue
    TrieScanSelectValue(TrieScanSelectValue<'a>),
}

generate_forwarder!(forward_to_scan; TrieScanGeneric, TrieScanJoin, TrieScanProject, TrieScanReorderProject, TrieScanMinus, TrieScanSelectEqual, TrieScanSelectValue, TrieScanUnion);

impl<'a> TrieScan<'a> for TrieScanEnum<'a> {
    fn up(&mut self) {
        forward_to_scan!(self, up)
    }

    fn down(&mut self) {
        forward_to_scan!(self, down)
    }

    fn current_scan(&self) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        forward_to_scan!(self, current_scan)
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        forward_to_scan!(self, get_scan(index))
    }

    fn get_types(&self) -> &TableColumnTypes {
        forward_to_scan!(self, get_types)
    }
}
