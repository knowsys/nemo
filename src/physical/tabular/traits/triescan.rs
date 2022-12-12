use crate::generate_forwarder;
use crate::physical::columnar::traits::columnscan::ColumnScanT;
use crate::physical::tabular::operations::{
    TrieScanJoin, TrieScanMinus, TrieScanProject, TrieScanSelectEqual, TrieScanSelectValue,
    TrieScanUnion,
};
use crate::physical::tabular::table_types::trie::{TrieScanGeneric, TrieSchema};
use std::fmt::Debug;

/// Iterator for a Trie datastructure.
/// Allows for vertical traversal through the tree and can return
/// its current position as a [`ColumnScanT`] object.
pub trait TrieScan<'a>: Debug {
    /// Return to the upper layer.
    fn up(&mut self);

    /// Enter the next layer based on the position of the iterator in the current layer.
    fn down(&mut self);

    /// Return the current position of the scan as a [`ColumnScanT`].
    fn current_scan(&mut self) -> Option<&mut ColumnScanT<'a>>;

    /// Return the underlying [`ColumnScanT`] object given an index.
    fn get_scan(&mut self, index: usize) -> Option<&mut ColumnScanT<'a>>;

    /// Return the underlying [`TableSchema`].
    fn get_schema(&self) -> TrieSchema;
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
    /// Case TrieScanMinus
    TrieScanMinus(TrieScanMinus<'a>),
    /// Case TrieScanUnion
    TrieScanUnion(TrieScanUnion<'a>),
    /// Case TrieScanSelectEqual
    TrieScanSelectEqual(TrieScanSelectEqual<'a>),
    /// Case TrieScanSelectValue
    TrieScanSelectValue(TrieScanSelectValue<'a>),
}

generate_forwarder!(forward_to_scan; TrieScanGeneric, TrieScanJoin, TrieScanProject, TrieScanMinus, TrieScanSelectEqual, TrieScanSelectValue, TrieScanUnion);

impl<'a> TrieScan<'a> for TrieScanEnum<'a> {
    fn up(&mut self) {
        forward_to_scan!(self, up)
    }

    fn down(&mut self) {
        forward_to_scan!(self, down)
    }

    fn current_scan(&mut self) -> Option<&mut ColumnScanT<'a>> {
        forward_to_scan!(self, current_scan)
    }

    fn get_scan(&mut self, index: usize) -> Option<&mut ColumnScanT<'a>> {
        forward_to_scan!(self, get_scan(index))
    }

    fn get_schema(&self) -> TrieSchema {
        forward_to_scan!(self, get_schema)
    }
}
