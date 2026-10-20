//! The store of literals of unsupported datatypes.

use crate::dictionary::store::PairPool;

/// The store of literals whose datatype this crate has no value for,
/// kept as (datatype IRI, lexical form) pairs.
///
/// The datatype is the frequent part.
pub(crate) struct OtherStore {
    /// The interned (datatype, lexical form) pairs.
    pairs: PairPool,
}

impl OtherStore {
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

    /// Interns a literal and returns its index,
    /// or `None` if the store cannot hold it.
    pub(crate) fn intern(&mut self, datatype: &str, lexical: &str) -> Option<u64> {
        self.pairs.intern(datatype.as_bytes(), lexical.as_bytes())
    }

    /// Returns the index of a literal, or `None` if it was never interned.
    pub(crate) fn find(&self, datatype: &str, lexical: &str) -> Option<u64> {
        self.pairs.find(datatype.as_bytes(), lexical.as_bytes())
    }

    /// Returns the datatype and the lexical form of the entry with the given index,
    /// borrowed from the store, or `None` for an unknown index.
    pub(crate) fn get(&self, index: u64) -> Option<(&str, &str)> {
        // SAFETY: the pool is fed by `intern` alone,
        // which stores the bytes of `&str`s.
        self.pairs.get(index).map(|(datatype, lexical)| unsafe {
            (
                std::str::from_utf8_unchecked(datatype),
                std::str::from_utf8_unchecked(lexical),
            )
        })
    }

    /// Returns the allocated bytes of the store.
    pub(crate) fn memory_bytes(&self) -> usize {
        self.pairs.memory_bytes()
    }
}
