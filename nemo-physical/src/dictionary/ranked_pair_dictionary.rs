//! This module defines a dictionary for pairs of byte arrays where
//! one part is "frequent" (few values used often) and another is "rare"
//! (many values, used rarely).

use crate::management::bytesized::ByteSized;

use super::{bytes_buffer::GlobalBytesBuffer, bytes_dictionary::BytesDictionary, AddResult};

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

/// A struct that implements a bijection between pairs of byte arrays and integers,
/// where the integers are automatically assigned upon insertion.
/// Data is stored in a the given [GlobalBytesBuffer].
///
/// The implementation optimizes for a situation, where one component of the pair is
/// "frequent" (few values used often) and another is "rare" (many values, used rarely).
/// The optimization aims to reduce memory usage by keeping the frequent values separately,
/// storing each of them only once, at the cost of insertion/retrieval speeds.
///
/// The implementation will also work if the assumption about frequency does not hold in
/// a strong sense, but there will be no memory savings in such cases, whereas the slower
/// access might be more pronounced.
#[derive(Debug)]
pub(crate) struct GenericRankedPairDictionary<B: GlobalBytesBuffer> {
    frequent_dict: BytesDictionary<B>,
    pair_dict: BytesDictionary<B>,
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
        // DEBUG:
        // println!("Fid {}, rare {:?}, pbytes {:?}", fid, rare, pair_bytes);

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
        let fid = match self.frequent_dict.add_bytes(frequent) {
            // In theory, we could exploit the Fresh case to save the check for
            // existing values in the pairs dictionary. However, there is currently
            // no API for adding values in such unchecked manner, and the case should
            // be rare under the assumption that "frequent" values are not added a lot.
            AddResult::Fresh(id) | AddResult::Known(id) => id,
            AddResult::Rejected => unreachable!("the BytesDictionary never rejects values"),
        };

        // DEBUG:
        // println!("Fid bytes: {:?} bits {:#b}", fid.to_be_bytes(), fid);

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

    /// True when the dictionary is empty. False otherwise.
    pub(crate) fn is_empty(&self) -> bool {
        self.pair_dict.is_empty()
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
        }
    }
}

impl<B: GlobalBytesBuffer> ByteSized for GenericRankedPairDictionary<B> {
    fn size_bytes(&self) -> u64 {
        self.frequent_dict.size_bytes() + self.pair_dict.size_bytes()
    }
}

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
        assert!(!dict.is_empty());
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
