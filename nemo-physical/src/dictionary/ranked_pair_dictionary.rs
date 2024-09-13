//! This module defines dictionaries for pairs of byte arrays (resp. strings) where
//! one part is "frequent" (few values used often) and another is "rare"
//! (many values, used rarely).

use crate::management::bytesized::ByteSized;

use super::{
    bytes_buffer::{BytesBuffer, GlobalBytesBuffer},
    bytes_dictionary::BytesDictionary,
    AddResult,
};

/// Smallest "frequent ID" that cannot be stored in a single byte.
///
/// We store IDs of frequent parts of pairs using 1-4 bytes, where
/// the first two bits indicate how many bytes were used.
const MAX_ID_1B: usize = 1 << 6;
/// Smallest "frequent ID" that cannot be stored in two bytes.
/// See [MAX_ID_1B].
const MAX_ID_2B: usize = 1 << (6 + 8);
/// Smallest "frequent ID" that cannot be stored in three bytes.
/// See [MAX_ID_1B].
const MAX_ID_3B: usize = 1 << (6 + 8 + 8);
/// Smallest "frequent ID" that cannot be stored in four bytes.
/// See [MAX_ID_1B].
const MAX_ID_4B: usize = 1 << (6 + 8 + 8 + 8);

/// Number of bytes in usize. Just for making the code below more readable.
const USIZE_BYTES: usize = size_of::<usize>();
/// The bits we consider for encoding information about the length of the frequent ids
/// in our encoding.
const FTYPE_BIT_MASK: u8 = 0b1100_0000;
/// The bits in the first frequent id byte that are part of the id, without the bits
/// used to encode the type.
const FID_BIT_MASK: u8 = 0b0011_1111;

/// Simple least-recently used data structure that can be used as a cache for
/// mappings from byte arrays to usize ids. The implementation is intended for
/// very small cache sizes, where the most efficient way of getting an item is
/// to iterate over all entries.
#[derive(Debug)]
struct LruArray {
    cache: Vec<(Vec<u8>, usize)>,
    top: usize,
    len: usize,
}
impl LruArray {
    fn create(capacity: usize) -> Self {
        LruArray {
            cache: vec![(vec![0], usize::MAX); capacity],
            top: 0,
            len: capacity,
        }
    }

    /// Puts a new entry into the cache, replacing the oldest entry.
    /// There is not duplicate check, i.e., the implementation allows
    /// the same byte array to occur in several entries. This should be
    /// checked before.
    fn put(&mut self, data: &[u8], id: usize) {
        self.top = (self.top + 1) % self.cache.len();
        let mut data_vec = vec![0; data.len()];
        data_vec.copy_from_slice(data);
        self.cache[self.top] = (data_vec, id);
    }

    /// Gets the index stored for a given byte array. If the byte array is
    /// not in the cache, `usize::MAX` is returned instead.
    /// Getting a known entry will move it up to the highest position,
    /// shifting other entries down to maintain order.
    fn get(&mut self, data: &[u8]) -> usize {
        let mut idx = usize::MAX;
        for i in 0..self.len {
            let v = &self.cache[(self.top + self.len - i) % self.len].0;
            if data.len() == v.len() && data == v.as_slice() {
                idx = (self.top + self.len - i) % self.len;
                break;
            }
        }

        if idx < usize::MAX {
            if idx == (self.top + 1) % self.len {
                self.top = (self.top + 1) % self.cache.len();
            } else if idx != self.top {
                for k in 0..self.top + self.len - idx {
                    unsafe {
                        self.cache
                            .swap_unchecked((idx + k) % self.len, (idx + k + 1) % self.len)
                    }
                }
            }
            self.cache[self.top].1
        } else {
            usize::MAX
        }
    }
}

/// A struct that implements a bijection between pairs of byte arrays and integers,
/// where the integers are automatically assigned upon insertion.
/// Data is stored in a the given [GlobalBytesBuffer].
///
/// The implementation optimizes for a situation where one component of the pair is
/// "frequent" (few values used often) and another is "rare" (many values, used rarely).
/// The optimization aims to reduce memory usage by keeping the frequent values separately,
/// storing each of them only once, at the cost of insertion/retrieval speeds.
///
/// The implementation will also work if the assumption about frequency does not hold in
/// a strong sense, but there will be no memory savings in such cases, whereas the slower
/// access might be more pronounced.
///
/// # Implementation details
///
/// The implementation chains two dictionaries: the dictionary for frequent arrays assigns
/// ids to this part of a pair in the usual way; the dictionary for pairs assigns ids to a
/// byte array that combines the rare part with an encoding of the id for the frequent part.
/// This encoding will use 1, 2, 3, or 4 bytes, where the first two bits (of the first byte)
/// are used to mark which case we are in. Therefore, the largest frequent-bytes id that can
/// be stored in 3 bytes uses 22 bits (8 * 3 - 2). The maximal number of frequent byte arrays
/// that can be stored is therefore 2 to the power of 30. However, the typical usage should
/// allow us to work with much smaller numbers.
#[derive(Debug)]
pub(crate) struct GenericRankedPairDictionary<B: GlobalBytesBuffer> {
    frequent_dict: BytesDictionary<B>,
    pair_dict: BytesDictionary<B>,
    recent_array: LruArray,
}
impl<B: GlobalBytesBuffer> GenericRankedPairDictionary<B> {
    /// Helper function to produce the bytes that are to be inserted in the
    /// pair dictionary for a given frequent id and rare byte array.
    #[inline(always)]
    fn pair_bytes(fid: usize, rare: &[u8]) -> Vec<u8> {
        let mut pair_bytes: Vec<u8>;

        match fid {
            0..MAX_ID_1B => {
                pair_bytes = vec![0; rare.len() + 1];
                pair_bytes[0] = fid.to_be_bytes()[USIZE_BYTES - 1]; // leading "00" means "1 byte representation"
                pair_bytes[1..].copy_from_slice(rare);
            }
            MAX_ID_1B..MAX_ID_2B => {
                pair_bytes = vec![0; rare.len() + 2];
                pair_bytes[0..=1].copy_from_slice(&fid.to_be_bytes()[USIZE_BYTES - 2..]);
                pair_bytes[0] |= 0b0100_0000; // "01" means "2 byte representation"
                pair_bytes[2..].copy_from_slice(rare);
            }
            MAX_ID_2B..MAX_ID_3B => {
                pair_bytes = vec![0; rare.len() + 3];
                pair_bytes[0..=2].copy_from_slice(&fid.to_be_bytes()[USIZE_BYTES - 3..]);
                pair_bytes[0] |= 0b1000_0000; // "10" means "3 byte representation"
                pair_bytes[3..].copy_from_slice(rare);
            }
            MAX_ID_3B..MAX_ID_4B => {
                pair_bytes = vec![0; rare.len() + 4];
                pair_bytes[0..=3].copy_from_slice(&fid.to_be_bytes()[USIZE_BYTES - 4..]);
                pair_bytes[0] |= 0b1100_0000; // "11" means "4 byte representation"
                pair_bytes[4..].copy_from_slice(rare);
            }
            _ => {
                panic!("dictionary overflow: pair dictionary can hold at most 2^30 different frequent values");
            }
        };

        pair_bytes
    }

    /// Adds a new pair to the dictionary. If the pair is not known yet, it will
    /// be assigned a new id.
    ///
    /// The result is an [AddResult] that indicates if the pair was newly added,
    /// or previoulsy present (it will never be rejected). The result then also yields
    /// the pair's id.
    ///
    /// The components of the pair should be ordered in such a way that the `frequent`
    /// part is likely to take relatively few distinct values across all pairs, whereas
    /// the `rare` part is more likely to take a large number of distinct values, possibly
    /// in the order of the total number of pairs.
    pub(crate) fn add_pair(&mut self, frequent: &[u8], rare: &[u8]) -> AddResult {
        let mut fid  = self.recent_array.get(frequent);
        if fid == usize::MAX {
            fid = match self.frequent_dict.add_bytes(frequent) {
                // In theory, we could exploit the Fresh case to save the check for
                // existing values in the pairs dictionary. However, there is currently
                // no API for adding values in such unchecked manner, and the case should
                // be rare under the assumption that "frequent" values are not added a lot.
                AddResult::Fresh(id) | AddResult::Known(id) => {
                    self.recent_array.put(frequent, id);
                    id
                }
                AddResult::Rejected => unreachable!("the BytesDictionary never rejects values"),
            };
        }

        self.pair_dict
            .add_bytes(Self::pair_bytes(fid, rare).as_slice())
    }

    /// Looks for a given pair and returns `Some(id)` if it is in the dictionary,
    /// and `None` otherwise. The special value [super::KNOWN_ID_MARK] will be returned
    /// if the pair was marked but not actually inserted.
    pub(crate) fn pair_to_id(&self, frequent: &[u8], rare: &[u8]) -> Option<usize> {
        if let Some(fid) = self.frequent_dict.bytes_to_id(frequent) {
            self.pair_dict.bytes_to_id(&Self::pair_bytes(fid, rare))
        } else {
            None
        }
    }

    /// Returns the pair associated with the `id`, or `None`` if no string has been
    /// associated to this id. The first value of the result is the "frequent" part,
    /// the second the "rare".
    pub(crate) fn id_to_pair(&self, id: usize) -> Option<(Vec<u8>, Vec<u8>)> {
        if let Some(pair_bytes) = self.pair_dict.id_to_bytes(id) {
            let mut rare: Vec<u8>;
            let fid: usize;
            let fid_len: usize;
            match pair_bytes[0] & FTYPE_BIT_MASK {
                0b0000_0000 => {
                    // 1 byte for frequent id
                    fid = pair_bytes[0] as usize;
                    fid_len = 1;
                }
                0b0100_0000 => {
                    let mut fid_bytes: [u8; USIZE_BYTES] = usize::to_be_bytes(0);
                    fid_bytes[USIZE_BYTES - 1] = pair_bytes[1];
                    fid_bytes[USIZE_BYTES - 2] = pair_bytes[0] & FID_BIT_MASK;
                    fid = usize::from_be_bytes(fid_bytes);
                    fid_len = 2;
                }
                0b1000_0000 => {
                    let mut fid_bytes: [u8; USIZE_BYTES] = usize::to_be_bytes(0);
                    fid_bytes[USIZE_BYTES - 3..].copy_from_slice(&pair_bytes[0..=2]);
                    fid_bytes[USIZE_BYTES - 3] &= FID_BIT_MASK;
                    fid = usize::from_be_bytes(fid_bytes);
                    fid_len = 3;
                }
                0b1100_0000 => {
                    let mut fid_bytes: [u8; USIZE_BYTES] = usize::to_be_bytes(0);
                    fid_bytes[USIZE_BYTES - 4..].copy_from_slice(&pair_bytes[0..=3]);
                    fid_bytes[USIZE_BYTES - 4] &= FID_BIT_MASK;
                    fid = usize::from_be_bytes(fid_bytes);
                    fid_len = 4;
                }
                _ => unreachable!("The bit mask ensures that values match one of the other arms"),
            }

            rare = vec![0; pair_bytes.len() - fid_len];
            rare.copy_from_slice(&pair_bytes[fid_len..]);
            let frequent = self.frequent_dict.id_to_bytes(fid).expect("");

            Some((frequent, rare))
        } else {
            None
        }
    }

    /// Returns true if a value is associated with the id.
    pub(crate) fn knows_id(&self, id: usize) -> bool {
        self.pair_dict.knows_id(id)
    }

    /// Returns the number of pairs in the dictionary. Pairs that are merely
    /// marked are not counted here.
    pub(crate) fn len(&self) -> usize {
        self.pair_dict.len()
    }

    /// Returns the number of frequent elements stored in the dictionary. This includes
    /// frequent elements that were added to mark a pairs that were not truly added.
    #[allow(dead_code)]
    pub(crate) fn frequent_len(&self) -> usize {
        self.frequent_dict.len()
    }

    /// Marks the given pair as being known without storing it under an own id.
    /// If the entry exists already, the old id will be kept and returned.
    ///
    /// Once a pair has been marked, it cannot be added anymore, since it will
    /// be known already. A use case that would require other behavior is not
    /// known so far, so we do not make any effort there.
    pub(crate) fn mark_pair(&mut self, frequent: &[u8], rare: &[u8]) -> AddResult {
        let fid = match self.frequent_dict.add_bytes(frequent) {
            AddResult::Fresh(id) | AddResult::Known(id) => id,
            AddResult::Rejected => unreachable!("the BytesDictionary never rejects values"),
        };

        self.pair_dict
            .mark_bytes(Self::pair_bytes(fid, rare).as_slice())
    }

    /// Returns true if the dictionary contains any marked elements.
    pub(crate) fn has_marked(&self) -> bool {
        self.pair_dict.has_marked()
    }
}

impl<B: GlobalBytesBuffer> Default for GenericRankedPairDictionary<B> {
    fn default() -> Self {
        GenericRankedPairDictionary {
            frequent_dict: Default::default(),
            pair_dict: Default::default(),
            recent_array: LruArray::create(3),
        }
    }
}

impl<B: GlobalBytesBuffer> ByteSized for GenericRankedPairDictionary<B> {
    fn size_bytes(&self) -> u64 {
        // Code for debugging/profiling dictionary
        // let sizef = self.frequent_dict.size_bytes();
        // let sizep = self.pair_dict.size_bytes();
        // println!("Ranked Pair dict: Frequent {}, Pairs {}", sizef, sizep);
        // sizef + sizep
        self.frequent_dict.size_bytes() + self.pair_dict.size_bytes()
    }
}

/// A struct that implements a bijection between pairs of strings and integers, where the integers
/// are automatically assigned upon insertion.
/// Data is stored in a the given [GlobalBytesBuffer]. See [GenericRankedPairDictionary] for
/// remarks on the underlying frequent-rare implementation.
#[derive(Debug)]
pub(crate) struct GenericRankedStringPairDictionary<B: GlobalBytesBuffer> {
    ranked_pair_dict: GenericRankedPairDictionary<B>,
}
impl<B: GlobalBytesBuffer> GenericRankedStringPairDictionary<B> {
    /// Adds a new string pair to the dictionary. If the pair is not known yet, it will
    /// be assigned a new id.
    ///
    /// The result is an [AddResult] that indicates if the string was newly added,
    /// or previoulsy present (it will never be rejected). The result then also yields
    /// the pair's id.
    pub(crate) fn add_pair(&mut self, frequent: &str, rare: &str) -> AddResult {
        self.ranked_pair_dict
            .add_pair(frequent.as_bytes(), rare.as_bytes())
    }

    /// Looks for a given string pair and returns `Some(id)` if it is in the dictionary,
    /// and `None` otherwise. The special value [super::KNOWN_ID_MARK] will be returned
    /// if the pair was marked but not actually inserted.
    pub(crate) fn pair_to_id(&self, frequent: &str, rare: &str) -> Option<usize> {
        self.ranked_pair_dict
            .pair_to_id(frequent.as_bytes(), rare.as_bytes())
    }

    /// Returns the pair of [String]s associated with the `id`, or `None`` if no string pair has been
    /// associated to this id.
    pub(crate) fn id_to_pair(&self, id: usize) -> Option<(String, String)> {
        self.ranked_pair_dict
            .id_to_pair(id)
            .map(|bytes_pair| unsafe {
                (
                    String::from_utf8_unchecked(bytes_pair.0),
                    String::from_utf8_unchecked(bytes_pair.1),
                )
            })
    }

    /// Returns true if a value is associated with the id.
    pub(crate) fn knows_id(&self, id: usize) -> bool {
        self.ranked_pair_dict.knows_id(id)
    }

    /// Returns the number of elements in the dictionary. Pairs that are merely
    /// marked are not counted here.
    pub(crate) fn len(&self) -> usize {
        self.ranked_pair_dict.len()
    }

    /// Returns the number of frequent elements stored in the dictionary. This includes
    /// frequent elements that were added to mark a pairs that were not truly added.
    #[allow(dead_code)]
    pub(crate) fn frequent_len(&self) -> usize {
        self.ranked_pair_dict.frequent_len()
    }

    /// Marks the given pair as being known without storing it under an own id.
    /// If the entry exists already, the old id will be kept and returned.
    ///
    /// Once a pair has been marked, it cannot be added anymore, since it will
    /// be known already. A use case that would require other behavior is not
    /// known so far, so we do not make any effort there.
    pub(crate) fn mark_pair(&mut self, frequent: &str, rare: &str) -> AddResult {
        self.ranked_pair_dict
            .mark_pair(frequent.as_bytes(), rare.as_bytes())
    }

    /// Returns true if the dictionary contains any marked elements.
    pub(crate) fn has_marked(&self) -> bool {
        self.ranked_pair_dict.has_marked()
    }
}

impl<B: GlobalBytesBuffer> Default for GenericRankedStringPairDictionary<B> {
    fn default() -> Self {
        GenericRankedStringPairDictionary {
            ranked_pair_dict: Default::default(),
        }
    }
}

impl<B: GlobalBytesBuffer> ByteSized for GenericRankedStringPairDictionary<B> {
    fn size_bytes(&self) -> u64 {
        self.ranked_pair_dict.size_bytes()
    }
}

crate::dictionary::bytes_buffer::declare_bytes_buffer!(
    StringPairDictBytesBuffer,
    STRING_PAIR_DICT_BYTES_BUFFER
);
pub(crate) type StringPairDictionary = GenericRankedStringPairDictionary<StringPairDictBytesBuffer>;

#[cfg(test)]
mod test {
    use crate::dictionary::{
        bytes_buffer::{BytesBuffer, GlobalBytesBuffer},
        AddResult, KNOWN_ID_MARK,
    };

    use super::GenericRankedPairDictionary;

    crate::dictionary::bytes_buffer::declare_bytes_buffer!(TestBytesBuffer, TEST_BUFFER);
    type TestPairDictionary = GenericRankedPairDictionary<TestBytesBuffer>;

    #[test]
    fn add_and_get() {
        let mut dict: TestPairDictionary = GenericRankedPairDictionary::default();

        let res1 = dict.add_pair(&[1, 2], &[5, 6, 7]);
        let res2 = dict.add_pair(&[2, 2], &[5, 6, 7, 8]);
        let res3 = dict.add_pair(&[1, 2], &[9, 10]);
        let res4 = dict.add_pair(&[1, 2], &[5, 6, 7]);

        assert_eq!(res1, AddResult::Fresh(0));
        assert_eq!(res2, AddResult::Fresh(1));
        assert_eq!(res3, AddResult::Fresh(2));
        assert_eq!(res4, AddResult::Known(0));

        assert_eq!(dict.pair_to_id(&[1, 2], &[5, 6, 7]), Some(0));
        assert_eq!(dict.pair_to_id(&[2, 2], &[5, 6, 7, 8]), Some(1));
        assert_eq!(dict.pair_to_id(&[1, 2], &[9, 10]), Some(2));
        assert_eq!(dict.pair_to_id(&[1, 2], &[123, 45, 6]), None);
        assert_eq!(dict.pair_to_id(&[123, 45, 6], &[5, 6, 7]), None);

        assert_eq!(dict.id_to_pair(0), Some((vec![1, 2], vec![5, 6, 7])));
        assert_eq!(dict.id_to_pair(1), Some((vec![2, 2], vec![5, 6, 7, 8])));
        assert_eq!(dict.id_to_pair(2), Some((vec![1, 2], vec![9, 10])));
        assert_eq!(dict.id_to_pair(3), None);

        assert!(dict.knows_id(0));
        assert!(dict.knows_id(1));
        assert!(!dict.knows_id(3));

        assert_eq!(dict.len(), 3);
        assert!(!dict.has_marked());
    }

    #[test]
    fn empty_bytes() {
        let mut dict: TestPairDictionary = GenericRankedPairDictionary::default();

        assert_eq!(dict.add_pair(&[], &[]), AddResult::Fresh(0));
        assert_eq!(dict.id_to_pair(0), Some((vec![], vec![])));
        assert_eq!(dict.pair_to_id(&[], &[]), Some(0));
        assert_eq!(dict.len(), 1);
        assert!(!dict.has_marked());
    }

    #[test]
    fn mark_pair() {
        let mut dict: TestPairDictionary = GenericRankedPairDictionary::default();

        assert_eq!(dict.add_pair(&[1, 2], &[5, 6, 7]), AddResult::Fresh(0));
        assert_eq!(dict.add_pair(&[2, 2], &[5, 6, 7, 8]), AddResult::Fresh(1));
        assert_eq!(dict.add_pair(&[1, 2], &[5, 6, 7]), AddResult::Known(0));
        assert_eq!(
            dict.mark_pair(&[3, 4], &[5, 6, 7]),
            AddResult::Fresh(KNOWN_ID_MARK)
        );
        assert_eq!(
            dict.mark_pair(&[5, 6], &[5, 6, 7]),
            AddResult::Fresh(KNOWN_ID_MARK)
        );
        assert_eq!(
            dict.add_pair(&[3, 4], &[5, 6, 7]),
            AddResult::Known(KNOWN_ID_MARK)
        );

        assert_eq!(dict.len(), 2);
        assert!(dict.has_marked());

        assert_eq!(dict.id_to_pair(0), Some((vec![1, 2], vec![5, 6, 7])));
        assert_eq!(dict.pair_to_id(&[1, 2], &[5, 6, 7]), Some(0));
        assert_eq!(dict.pair_to_id(&[2, 2], &[5, 6, 7, 8]), Some(1));
        assert_eq!(dict.pair_to_id(&[3, 4], &[5, 6, 7]), Some(KNOWN_ID_MARK));
        assert_eq!(dict.pair_to_id(&[5, 6], &[5, 6, 7]), Some(KNOWN_ID_MARK));
    }
}
