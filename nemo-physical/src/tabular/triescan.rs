use std::{cell::UnsafeCell, fmt::Debug};

use crate::{columnar::columnscan::ColumnScanRainbow, datatypes::StorageTypeName};

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
    fn current_layer(&self) -> Option<usize>;

    /// Return the underlying [ColumnScanT] given an index.
    ///
    /// # Panics
    /// Panics if the requested layer is higher than the arity of this scan.
    fn scan<'b: 'a>(&'b self, layer: usize) -> &'b UnsafeCell<ColumnScanRainbow<'a>>;

    /// Return the [ColumnScanT] at the current layer.
    fn current_scan<'b>(&'a self) -> Option<&UnsafeCell<ColumnScanRainbow<'a>>> {
        Some(self.scan(self.current_layer()?))
    }
}
