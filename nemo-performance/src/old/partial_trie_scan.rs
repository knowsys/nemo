use std::cell::UnsafeCell;
use std::fmt::Debug;

use nemo_physical::datatypes::StorageTypeName;
use nemo_physical::generate_forwarder;

use super::column_scan::ColumnScanT;
use super::trie::TrieScanGeneric;

/// Iterator for a Trie datastructure.
/// Allows for vertical traversal through the tree and can return
/// its current position as a [`ColumnScanT`] object.
pub trait PartialTrieScan<'a>: Debug {
    /// Return to the upper layer.
    /// This may only be called if the trie scan is not already at the topmost layer. TODO: Is this restriction required? Check implementations if you want to change this
    fn up(&mut self);

    /// Enter the next layer based on the position of the iterator in the current layer.
    /// This may only be called if the trie scan is not already at the deepest layer. TODO: Is this restriction required? Check implementations if you want to change this
    fn down(&mut self);

    /// Return the current position of the scan as a [`ColumnScanT`].
    fn current_scan(&mut self) -> Option<&mut ColumnScanT<'a>>;

    /// Return the current layer of this scan.
    fn current_layer(&self) -> Option<usize>;

    /// Return the underlying [`ColumnScanT`] object given an index.
    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColumnScanT<'a>>>;

    /// Return the underlying [`StorageTypeName`]s.
    fn get_types(&self) -> &Vec<StorageTypeName>;
}

/// Enum for TrieScan Variants
#[derive(Debug)]
pub enum TrieScanEnum<'a> {
    /// Case TrieScanGeneric
    TrieScanGeneric(TrieScanGeneric<'a>),
}

generate_forwarder!(forward_to_scan;
    TrieScanGeneric
);

impl<'a> PartialTrieScan<'a> for TrieScanEnum<'a> {
    fn up(&mut self) {
        forward_to_scan!(self, up)
    }

    fn down(&mut self) {
        forward_to_scan!(self, down)
    }

    fn current_scan(&mut self) -> Option<&mut ColumnScanT<'a>> {
        forward_to_scan!(self, current_scan)
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        forward_to_scan!(self, get_scan(index))
    }

    fn get_types(&self) -> &Vec<StorageTypeName> {
        forward_to_scan!(self, get_types)
    }

    fn current_layer(&self) -> Option<usize> {
        forward_to_scan!(self, current_layer)
    }
}
