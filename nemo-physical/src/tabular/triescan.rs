//! This module defines the trait [PartialTrieScan],
//! which is an iterator for [Trie][super::trie::Trie].

use std::{cell::UnsafeCell, fmt::Debug};

use delegate::delegate;

use crate::{
    columnar::columnscan::ColumnScanRainbow,
    datatypes::{StorageTypeName, StorageValueT},
};

use super::{
    operations::{
        filter::TrieScanFilter, function::TrieScanFunction, join::TrieScanJoin,
        prune::TrieScanPrune, subtract::TrieScanSubtract, union::TrieScanUnion,
    },
    trie::TrieScanGeneric,
};

/// Iterator for a [Trie][super::trie::Trie] data structure
///
/// It allows for vertical traversal between layers via the `up` and `down` methods,
/// and horizontal traversal via [`ColumnScanT`].
pub trait PartialTrieScan<'a>: Debug {
    /// Return to the upper layer.
    ///
    /// # Panics
    /// May panic if this method is called while being on the upper most layer.
    fn up(&mut self);

    /// Go down into the next layer restricted to the provided type.
    ///
    /// # Panics
    /// May panic if this method is called while being on the lowest layer
    /// or if this method is called while the iterator of the current layer does not point to any element.
    fn down(&mut self, storage_type: StorageTypeName);

    /// Return the storage type that is "active" on each layer.
    fn path_types(&self) -> &[StorageTypeName];

    /// Return the number of columns associated with this scan.
    fn arity(&self) -> usize;

    /// Return the index of the current layer for this scan.
    fn current_layer(&self) -> Option<usize> {
        self.path_types().len().checked_sub(1)
    }

    /// Return the underlying [ColumnScanT] given an index.
    ///
    /// # Panics
    /// Panics if the requested layer is higher than the arity of this scan.
    fn scan<'b>(&'b self, layer: usize) -> &'b UnsafeCell<ColumnScanRainbow<'a>>;

    /// Return the [ColumnScanT] at the current layer.
    fn current_scan<'b>(&'b self) -> Option<&UnsafeCell<ColumnScanRainbow<'a>>> {
        Some(self.scan(self.current_layer()?))
    }
}

/// Enum containing all implementations of [PartialTrieScan]
#[derive(Debug)]
pub enum TrieScanEnum<'a> {
    /// Case [TrieScanGeneric]
    TrieScanGeneric(TrieScanGeneric<'a>),
    /// Case [TrieScanJoin]
    TrieScanJoin(TrieScanJoin<'a>),
    /// Case [TrieScanUnion]
    TrieScanUnion(TrieScanUnion<'a>),
    /// Case [TrieScanSubtract]
    TrieScanSubtract(TrieScanSubtract<'a>),
    /// Case [TrieScanPrune]
    TrieScanPrune(TrieScanPrune<'a>),
    /// Case [TrieScanFunction]
    TrieScanFunction(TrieScanFunction<'a>),
    /// Case [TrieScanFilter]
    TrieScanFilter(TrieScanFilter<'a>),
}

impl<'a> PartialTrieScan<'a> for TrieScanEnum<'a> {
    delegate! {
        to match self {
            TrieScanEnum::TrieScanGeneric(scan) => scan,
            TrieScanEnum::TrieScanJoin(scan) => scan,
            TrieScanEnum::TrieScanUnion(scan) => scan,
            TrieScanEnum::TrieScanSubtract(scan) => scan,
            TrieScanEnum::TrieScanPrune(scan) => scan,
            TrieScanEnum::TrieScanFunction(scan) => scan,
            TrieScanEnum::TrieScanFilter(scan) => scan,
        } {
            fn up(&mut self);
            fn down(&mut self, storage_type: StorageTypeName);
            fn path_types(&self) -> &[StorageTypeName];
            fn arity(&self) -> usize;
            fn current_layer(&self) -> Option<usize>;
            fn scan<'b>(&'b self, layer: usize) -> &'b UnsafeCell<ColumnScanRainbow<'a>>;
            fn current_scan<'b>(&'b self) -> Option<&UnsafeCell<ColumnScanRainbow<'a>>>;
        }
    }
}

/// An iterator over a trie, which can call next on every layer of the trie
pub trait TrieScan {
    /// Return the number of output columns of this scan.
    fn num_columns(&self) -> usize;

    /// Advance trie at the specified layer. This might cause calls to next
    /// at layers above the specified layer. If there is no next element at
    /// the specified layer, returns none. Otherwise returns the index of the
    /// uppermost changed layer.
    fn advance_on_layer(&mut self, layer: usize) -> Option<usize>;

    /// After a call to [TrieScan::advance_on_layer], this returns the current
    /// value the specified layer. This is only allowed to call, if [TrieScan::advance_on_layer]
    /// returned [Some].
    ///
    /// # Panics
    /// If there is no current element ([TrieScan::advance_on_layer] was not
    /// called or returned [None]).
    fn current_value(&mut self, layer: usize) -> StorageValueT;
}
