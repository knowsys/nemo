//! This module defines [BytesPairDictionary].

use super::{
    bytes_buffer::{BytesRef, GlobalBytesBuffer},
    AddResult,
};
use crate::dictionary::{datavalue_dictionary::SMALL_KNOWN_ID_MARK, KNOWN_ID_MARK};

use crate::dictionary::bytes_dictionary::IdUtils;

use hashbrown::{HashMap, HashSet};

/// A struct that implements a bijection between a pair of bytes and integers.
/// Integers are automatically assigned upon insertion.
#[derive(Debug)]
pub(crate) struct BytesPairDictionary<B: GlobalBytesBuffer> {
    set: HashSet<BytesRef<B>>, // to know what is in there
    short_map: HashMap<[BytesRef<B>; 2], u32>,
    long_map: HashMap<[BytesRef<B>; 2], u64>,
    store: Vec<[BytesRef<B>; 2]>,
    buffer_id: usize,
    has_known_mark: bool,
}

impl<B: GlobalBytesBuffer> BytesPairDictionary<B> {
    /// Construct a new and empty BytesPair dictionary.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Adds a new bytes pair to the dictionary when the pair is not stored yet.
    /// Ids are assigned in consecutive order.
    ///
    /// The result is an [AddResult] that indicates if the bytes pair was newly added,
    /// or previoulsy present.
    pub(crate) fn add_bytes_pair(&mut self, first: &[u8], second: &[u8]) -> AddResult {
        self.inner_add_bytes_pair(first, second, true)
    }

    /// Looks for a given [&str] slice pair and returns `Some(id)` if the pair is in the dictionary,
    /// and `None` otherwise.
    pub(crate) fn bytes_pair_to_id(&self, first: &[u8], second: &[u8]) -> Option<usize> {
        match (self.set.get(first), self.set.get(second)) {
            (Some(fbref), Some(sbref)) => {
                let pair = [*fbref, *sbref];
                if let Some(id) = self.short_map.get(&pair).copied() {
                    if id == SMALL_KNOWN_ID_MARK {
                        return Some(KNOWN_ID_MARK);
                    } else {
                        return Some(IdUtils::id32_to_id(id));
                    }
                } else if let Some(id) = self.long_map.get(&pair).copied() {
                    return Some(IdUtils::id64_to_id(id));
                }
                None
            }
            (_, _) => None,
        }
    }

    /// Returns the bytes pair associated with the `id`, or `None` if `id` doesn't
    /// exists in the dictionary.
    pub(crate) fn id_to_bytes_pair(&self, id: usize) -> Option<[Vec<u8>; 2]> {
        self.store
            .get(id)
            .map(|[first_bref, second_bref]| [first_bref.as_vec(), second_bref.as_vec()])
    }

    /// Returns true if a value is associated with the id.
    pub(crate) fn knows_id(&self, id: usize) -> bool {
        id < self.store.len()
    }

    /// Returns the number of elements in the dictionary. It does not consider
    /// marked elements.
    pub(crate) fn len(&self) -> usize {
        self.store.len()
    }

    /// Marks the given pair of byte array as being known without storing it
    /// under an own id. If the entry exists already, the old id will be kept
    /// and returned.
    ///
    /// Once a pair of arrays has been marked, it cannot be added anymore,
    /// since it will be known already. A use case that would require other
    /// behavior is not known so far, so we do not make any effort there.
    pub(crate) fn mark_bytes(&mut self, first: &[u8], second: &[u8]) -> AddResult {
        self.has_known_mark = true;
        self.inner_add_bytes_pair(first, second, false)
    }

    /// Returns true if the dictionary contains any marked elements.
    pub(crate) fn has_marked(&self) -> bool {
        self.has_known_mark
    }

    /// Adds a pair of bytes arrays to the dictionary when it is not already
    /// present. If `insert` is true, the pair will be given a new id and
    /// stored under this id. Otherwise, the bytes will merely be marked as
    /// known.
    ///
    /// If the given array is known but not assigned an ID (indicated by
    /// [SMALL_KNOWN_ID_MARK]), then the operation will still not assign an
    /// ID either, independently of `insert`.
    #[inline(always)]
    pub(crate) fn inner_add_bytes_pair(
        &mut self,
        first: &[u8],
        second: &[u8],
        insert: bool,
    ) -> AddResult {
        let first_bref = self.get_or_insert_bref(first);
        let second_bref = self.get_or_insert_bref(second);
        let pair = [first_bref, second_bref];
        match self.short_map.get(&pair) {
            Some(id) => {
                if *id == SMALL_KNOWN_ID_MARK {
                    AddResult::Known(KNOWN_ID_MARK)
                } else {
                    AddResult::Known(IdUtils::id32_to_id(*id))
                }
            }
            None => match self.long_map.get(&pair) {
                Some(id) => AddResult::Known(IdUtils::id64_to_id(*id)),
                None => {
                    if insert {
                        let new_id = self.store.len();
                        let inner_id = IdUtils::id_to_innerid(new_id);
                        self.store.push(pair);
                        if let Ok(id_u32) = u32::try_from(inner_id) {
                            self.short_map.insert(pair, id_u32);
                        } else if let Ok(id_u64) = u64::try_from(inner_id) {
                            self.long_map.insert(pair, id_u64);
                        } else {
                            panic!("no support for platforms with more than 64bits");
                        }
                        AddResult::Fresh(new_id)
                    } else {
                        self.short_map.insert(pair, SMALL_KNOWN_ID_MARK);
                        AddResult::Fresh(KNOWN_ID_MARK)
                    }
                }
            },
        }
    }

    /// Helper function to get a BytesRef from a array of u8 characters using
    /// the BytesBuffer
    fn get_or_insert_bref(&mut self, bytes: &[u8]) -> BytesRef<B> {
        match self.set.get(bytes) {
            Some(bref) => *bref,
            None => {
                let bref = B::push_bytes(self.buffer_id, bytes);
                self.set.insert(bref);
                bref
            }
        }
    }
}

impl<B: GlobalBytesBuffer> Default for BytesPairDictionary<B> {
    fn default() -> Self {
        BytesPairDictionary {
            set: HashSet::new(),
            short_map: HashMap::new(),
            long_map: HashMap::new(),
            store: Vec::new(),
            buffer_id: B::init_buffer(),
            has_known_mark: false,
        }
    }
}

impl<B: GlobalBytesBuffer> Drop for BytesPairDictionary<B> {
    fn drop(&mut self) {
        B::drop_buffer(self.buffer_id);
    }
}

#[cfg(test)]
mod test {
    use crate::dictionary::{
        bytes_buffer::{BytesBuffer, GlobalBytesBuffer},
        bytes_pair_dictionary::BytesPairDictionary,
        AddResult,
    };

    crate::dictionary::bytes_buffer::declare_bytes_buffer!(TestBytesBuffer, TEST_BUFFER);

    #[test]
    fn add_and_get() {
        let mut dict: BytesPairDictionary<TestBytesBuffer> = BytesPairDictionary::new();

        assert_eq!(
            dict.add_bytes_pair("a".as_bytes(), "b".as_bytes()),
            AddResult::Fresh(0)
        );
        assert_eq!(
            dict.add_bytes_pair("a".as_bytes(), "b".as_bytes()),
            AddResult::Known(0)
        );

        assert_eq!(
            dict.bytes_pair_to_id("a".as_bytes(), "b".as_bytes()),
            Some(0)
        );
        assert_eq!(dict.bytes_pair_to_id("b".as_bytes(), "a".as_bytes()), None);
    }
}
