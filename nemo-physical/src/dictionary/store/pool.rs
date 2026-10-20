//! Interning pools of plain byte strings.

use crate::dictionary::store::record::RecordPool;

/// An interning pool of byte strings:
/// each distinct string is stored once
/// and identified by a dense index assigned in first-insertion order.
///
/// It is a [RecordPool] whose records have no header.
pub(crate) struct StringPool {
    /// The strings, as records without a header.
    records: RecordPool<()>,
}

impl StringPool {
    /// Creates an empty pool.
    pub(crate) fn new() -> Self {
        Self {
            records: RecordPool::new(),
        }
    }

    /// Returns the number of distinct strings.
    pub(crate) fn len(&self) -> u64 {
        self.records.len()
    }

    /// Interns a byte string and returns its index,
    /// or `None` if the pool cannot hold it.
    pub(crate) fn intern(&mut self, bytes: &[u8]) -> Option<u64> {
        self.records.intern((), bytes)
    }

    /// Returns the index of a byte string, or `None` if it was never interned.
    pub(crate) fn find(&self, bytes: &[u8]) -> Option<u64> {
        self.records.find((), bytes)
    }

    /// Returns the bytes of an entry, or `None` for an unknown index.
    pub(crate) fn get(&self, index: u64) -> Option<&[u8]> {
        self.records.get(index).map(|((), bytes)| bytes)
    }

    /// Returns the allocated bytes of the pool, the lookup table and the entry lists included.
    pub(crate) fn memory_bytes(&self) -> usize {
        self.records.memory_bytes()
    }
}
