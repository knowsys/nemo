use crate::physical::datatypes::{StorageTypeName, StorageValueT};

/// An iterator over a trie, which can call next on every layer of the trie
pub trait TrieScan {
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
    fn current(&mut self, layer: usize) -> StorageValueT;

    /// Returns the number of layers of the current trie scan
    fn column_types(&self) -> &[StorageTypeName];
}
