//! The stores of structured values.

use crate::dictionary::store::{PairPool, content::BlobsRef};

/// A store of structured values, tuples or maps,
/// kept as pairs of a type blob and a content blob.
///
/// The type blob, the shape and the label, is the frequent part,
/// which all values of one shape share.
/// A tuple's elements are the blob's elements;
/// a map's are its key-value pairs, flattened in the map's canonical key order.
pub(crate) struct StructureStore {
    /// The interned (type blob, content blob) pairs.
    pairs: PairPool,
}

impl StructureStore {
    /// Creates an empty store.
    pub(crate) fn new() -> Self {
        Self {
            pairs: PairPool::new(),
        }
    }

    /// Returns the number of entries.
    pub(crate) fn len(&self) -> u64 {
        self.pairs.len()
    }

    /// Returns the number of distinct type blobs.
    #[cfg(test)]
    pub(crate) fn shape_count(&self) -> u64 {
        self.pairs.frequents_len()
    }

    /// Interns an encoded value and returns its index,
    /// or `None` if the store cannot hold it.
    pub(crate) fn intern(&mut self, type_blob: &[u8], content: &[u8]) -> Option<u64> {
        self.pairs.intern(type_blob, content)
    }

    /// Returns the index of an encoded value, or `None` if it was never interned.
    pub(crate) fn find(&self, type_blob: &[u8], content: &[u8]) -> Option<u64> {
        self.pairs.find(type_blob, content)
    }

    /// Returns the blobs of the entry with the given index, read in place,
    /// or `None` for an unknown index.
    pub(crate) fn get(&self, index: u64) -> Option<BlobsRef<'_>> {
        self.pairs
            .get(index)
            .map(|(type_blob, content)| BlobsRef::trusted(type_blob, content))
    }

    /// Returns the allocated bytes of the store.
    pub(crate) fn memory_bytes(&self) -> usize {
        self.pairs.memory_bytes()
    }
}
