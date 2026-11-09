//! The store of IRIs.

use crate::{dictionary::store::PairPool, value::IriRef};

/// The store of IRIs, each split into its namespace and its local part.
///
/// The split is the one that [IriRef] makes, after the last `/` or `#`.
/// So an [IriRef] is interned from its two parts, and read back as one, without copying.
/// The namespaces repeat across the IRIs of a dataset,
/// so they are the frequent part of a [PairPool] and are stored once.
pub(crate) struct IriStore {
    /// The interned (namespace, local part) pairs.
    pairs: PairPool,
}

impl IriStore {
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

    /// Returns the number of distinct namespaces.
    #[cfg(test)]
    pub(crate) fn namespace_count(&self) -> u64 {
        self.pairs.frequents_len()
    }

    /// Interns an IRI and returns its index, or `None` if a part is longer than
    /// [LENGTH_LIMIT](crate::dictionary::Dictionary::LENGTH_LIMIT).
    pub(crate) fn intern(&mut self, iri: IriRef<'_>) -> Option<u64> {
        self.pairs
            .intern(iri.namespace().as_bytes(), iri.local().as_bytes())
    }

    /// Returns the index of an IRI, or `None` if it was never interned.
    pub(crate) fn find(&self, iri: IriRef<'_>) -> Option<u64> {
        self.pairs
            .find(iri.namespace().as_bytes(), iri.local().as_bytes())
    }

    /// Returns the IRI with the given index, borrowed from the store,
    /// or `None` for an unknown index.
    pub(crate) fn get(&self, index: u64) -> Option<IriRef<'_>> {
        // SAFETY: only `intern` adds pairs to the pool,
        // and the two parts that it stores are the segments of an `IriRef`, both `&str`s.
        self.pairs.get(index).map(|(namespace, local)| unsafe {
            IriRef::from_parts(
                std::str::from_utf8_unchecked(namespace),
                std::str::from_utf8_unchecked(local),
            )
        })
    }

    /// Returns the allocated bytes of the store.
    pub(crate) fn memory_bytes(&self) -> usize {
        self.pairs.memory_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// IRIs with the same namespace share one stored copy of it.
    #[test]
    fn namespaces_are_shared() {
        let mut store = IriStore::new();

        for name in ["a", "b", "c"] {
            let iri = format!("http://example.org/{name}");
            store.intern(IriRef::new(&iri)).expect("fits");
        }

        store
            .intern(IriRef::new("http://example.org/x#y"))
            .expect("fits");

        assert_eq!(store.len(), 4);
        assert_eq!(store.namespace_count(), 2);
    }

    /// An IRI without `/` or `#` is stored with an empty namespace.
    #[test]
    fn separator_free_iris_have_an_empty_namespace() {
        let mut store = IriStore::new();
        let plain = store.intern(IriRef::new("urn:uuid:1234")).expect("fits");
        let stored = store.get(plain).expect("interned");

        assert_eq!((stored.namespace(), stored.local()), ("", "urn:uuid:1234"));
    }
}
