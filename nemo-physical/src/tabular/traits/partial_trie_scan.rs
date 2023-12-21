use crate::columnar::columnscan::ColumnScanT;
use crate::datatypes::StorageTypeName;
use crate::generate_forwarder;
use crate::tabular::operations::triescan_aggregate::TrieScanAggregateWrapper;
use crate::tabular::operations::triescan_append::TrieScanAppend;
use crate::tabular::operations::triescan_minus::TrieScanSubtract;
use crate::tabular::operations::{
    TrieScanJoin, TrieScanMinus, TrieScanNulls, TrieScanProject, TrieScanPrune,
    TrieScanRestrictValues, TrieScanSelectEqual, TrieScanUnion,
};
use crate::tabular::table_types::trie::TrieScanGeneric;
use std::cell::UnsafeCell;
use std::fmt::Debug;

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
    /// Case TrieScanJoin
    TrieScanJoin(TrieScanJoin<'a>),
    /// Case TrieScanProject
    TrieScanProject(TrieScanProject<'a>),
    /// Case TrieScanPrune
    TrieScanPrune(TrieScanPrune<'a>),
    /// Case TrieScanMinus
    TrieScanMinus(TrieScanMinus<'a>),
    /// Case TrieScanUnion
    TrieScanUnion(TrieScanUnion<'a>),
    /// Case TrieScanSelectEqual
    TrieScanSelectEqual(TrieScanSelectEqual<'a>),
    /// Case TrieScanRestrictValues
    TrieScanRestrictValues(TrieScanRestrictValues<'a>),
    /// Case TrieScanAppend
    TrieScanAppend(TrieScanAppend<'a>),
    /// Case TrieScanAppend
    TrieScanNulls(TrieScanNulls<'a>),
    /// Case TrieScanSubtract
    TrieScanSubtract(TrieScanSubtract<'a>),
    /// Case TrieScanAggregateWrapper
    TrieScanAggregateWrapper(TrieScanAggregateWrapper<'a>),
}

generate_forwarder!(forward_to_scan;
    TrieScanGeneric,
    TrieScanJoin,
    TrieScanProject,
    TrieScanPrune,
    TrieScanMinus,
    TrieScanSelectEqual,
    TrieScanRestrictValues,
    TrieScanUnion,
    TrieScanAppend,
    TrieScanNulls,
    TrieScanSubtract,
    TrieScanAggregateWrapper
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
