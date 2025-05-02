//! This module defines the trait [PartialTrieScan],
//! which is an iterator for [Trie][super::trie::Trie].

use std::{cell::UnsafeCell, fmt::Debug};

use delegate::delegate;

use crate::{
    columnar::columnscan::ColumnScanT,
    datatypes::{storage_type_name::StorageTypeBitSet, StorageTypeName, StorageValueT},
};

use super::{
    operations::{
        aggregate::TrieScanAggregateWrapper, filter::TrieScanFilter,
        filter_hook::TrieScanFilterHook, function::TrieScanFunction, join::TrieScanJoin,
        null::TrieScanNull, subtract::TrieScanSubtract, union::TrieScanUnion,
    },
    trie::TrieScanGeneric,
};

/// Iterator for a [Trie][super::trie::Trie] data structure
///
/// It allows for vertical traversal between layers via the `up` and `down` methods,
/// and horizontal traversal via [ColumnScanT].
pub(crate) trait PartialTrieScan<'a>: Debug {
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

    /// Return a list of possible types for a given layer.
    fn possible_types(&self, layer: usize) -> StorageTypeBitSet;

    /// Return the number of columns associated with this scan.
    fn arity(&self) -> usize;

    /// Return the index of the current layer for this scan.
    fn current_layer(&self) -> Option<usize>;

    /// Return the underlying [ColumnScanT] given an index.
    ///
    /// # Panics
    /// Panics if the requested layer is higher than the arity of this scan.
    fn scan<'b>(&'b self, layer: usize) -> &'b UnsafeCell<ColumnScanT<'a>>;

    /// Return the [ColumnScanT] at the current layer.
    fn current_scan<'b>(&'b self) -> Option<&'b UnsafeCell<ColumnScanT<'a>>> {
        Some(self.scan(self.current_layer()?))
    }
}

/// Enum containing all implementations of [PartialTrieScan]
#[derive(Debug)]
pub(crate) enum TrieScanEnum<'a> {
    /// Case [TrieScanAggregateWrapper]
    AggregateWrapper(TrieScanAggregateWrapper<'a>),
    /// Case [TrieScanFilter]
    Filter(TrieScanFilter<'a>),
    /// Case [TrieScanFilterHook]
    FilterHook(TrieScanFilterHook<'a>),
    /// Case [TrieScanFunction]
    Function(TrieScanFunction<'a>),
    /// Case [TrieScanGeneric]
    Generic(TrieScanGeneric<'a>),
    /// Case [TrieScanJoin]
    Join(TrieScanJoin<'a>),
    /// Case [TrieScanNull]
    Null(TrieScanNull<'a>),
    /// Case [TrieScanSubtract]
    Subtract(TrieScanSubtract<'a>),
    /// Case [TrieScanUnion]
    Union(TrieScanUnion<'a>),
    #[cfg(test)]
    /// Case [super::operations::prune::TrieScanPrune]
    Prune(super::operations::prune::TrieScanPrune<'a>),
}

impl<'a> PartialTrieScan<'a> for TrieScanEnum<'a> {
    delegate! {
        to match self {
            Self::AggregateWrapper(scan) => scan,
            Self::Filter(scan) => scan,
            Self::FilterHook(scan) => scan,
            Self::Function(scan) => scan,
            Self::Generic(scan) => scan,
            Self::Join(scan) => scan,
            Self::Null(scan) => scan,
            #[cfg(test)]
            Self::Prune(scan) => scan,
            Self::Subtract(scan) => scan,
            Self::Union(scan) => scan,
        } {
            fn up(&mut self);
            fn down(&mut self, storage_type: StorageTypeName);
            fn possible_types(&self, layer: usize) -> StorageTypeBitSet;
            fn arity(&self) -> usize;
            fn current_layer(&self) -> Option<usize>;
            fn scan(&self, layer: usize) -> &UnsafeCell<ColumnScanT<'a>>;
            fn current_scan(&self) -> Option<&UnsafeCell<ColumnScanT<'a>>>;
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
