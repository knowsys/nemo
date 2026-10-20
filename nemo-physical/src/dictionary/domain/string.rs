//! The store of plain strings.

use crate::dictionary::store::StringPool;

/// The store of plain strings.
pub(crate) struct StringStore {
    /// The interned strings.
    pool: StringPool,
}

impl StringStore {
    /// Creates an empty store.
    pub(crate) fn new() -> Self {
        Self {
            pool: StringPool::new(),
        }
    }

    /// Returns the number of entries.
    pub(crate) fn len(&self) -> u64 {
        self.pool.len()
    }

    /// Interns a string and returns its index,
    /// or `None` if the store cannot hold it.
    pub(crate) fn intern(&mut self, string: &str) -> Option<u64> {
        self.pool.intern(string.as_bytes())
    }

    /// Returns the index of a string, or `None` if it was never interned.
    pub(crate) fn find(&self, string: &str) -> Option<u64> {
        self.pool.find(string.as_bytes())
    }

    /// Returns the string with the given index, borrowed from the store,
    /// or `None` for an unknown index.
    pub(crate) fn get(&self, index: u64) -> Option<&str> {
        // SAFETY: the pool is fed by `intern` alone,
        // which stores the bytes of a `&str`.
        self.pool
            .get(index)
            .map(|bytes| unsafe { std::str::from_utf8_unchecked(bytes) })
    }

    /// Returns the allocated bytes of the store.
    pub(crate) fn memory_bytes(&self) -> usize {
        self.pool.memory_bytes()
    }
}
