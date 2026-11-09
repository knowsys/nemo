//! The store of language-tagged strings.

use crate::dictionary::store::PairPool;

/// The store of language-tagged strings.
///
/// The language tag is the frequent part of a [PairPool], stored once per language,
/// since a dataset uses few languages for many texts.
pub(crate) struct LanguageStringStore {
    /// The interned (language, text) pairs.
    pairs: PairPool,
}

impl LanguageStringStore {
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

    /// Interns a language-tagged string and returns its index, or `None` if a part is longer
    /// than [LENGTH_LIMIT](crate::dictionary::Dictionary::LENGTH_LIMIT).
    ///
    /// The language must be in its canonical lowercase form,
    /// as [ValueRef](crate::value::ValueRef) guarantees.
    pub(crate) fn intern(&mut self, text: &str, language: &str) -> Option<u64> {
        self.pairs.intern(language.as_bytes(), text.as_bytes())
    }

    /// Returns the index of a language-tagged string,
    /// or `None` if it was never interned.
    pub(crate) fn find(&self, text: &str, language: &str) -> Option<u64> {
        self.pairs.find(language.as_bytes(), text.as_bytes())
    }

    /// Returns the text and the language of the entry with the given index,
    /// borrowed from the store, or `None` for an unknown index.
    pub(crate) fn get(&self, index: u64) -> Option<(&str, &str)> {
        // SAFETY: only `intern` adds pairs to the pool,
        // and it stores the bytes of `&str`s.
        self.pairs.get(index).map(|(language, text)| unsafe {
            (
                std::str::from_utf8_unchecked(text),
                std::str::from_utf8_unchecked(language),
            )
        })
    }

    /// Returns the allocated bytes of the store.
    pub(crate) fn memory_bytes(&self) -> usize {
        self.pairs.memory_bytes()
    }
}
