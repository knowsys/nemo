use hashbrown::HashMap;

use super::bytes_buffer::{BytesRef, GlobalBytesBuffer};
use super::{AddResult, KNOWN_ID_MARK};

/// A marker id reserved for playing the role of [KNOWN_ID_MARK] while we are workin
/// with 32bit ids. This is internal and always replaced by [KNOWN_ID_MARK] to the outside.
#[cfg(not(test))]
const SMALL_KNOWN_ID_MARK: u32 = u32::MAX;
#[cfg(test)]
// Note: A smaller value is used to exercise the relevant code in the unit tests as well.
const SMALL_KNOWN_ID_MARK: u32 = 3;

const SMALL_KNOWN_ID_MARK_AS_USIZE: usize = SMALL_KNOWN_ID_MARK as usize;

/// A struct that implements a bijection between byte arrays and integers, where the integers
/// are automatically assigned upon insertion.
/// Byte arrays are stored in a compact buffer to reduce memory overhead and fragmentation.
#[derive(Debug)]
pub(crate) struct BytesDictionary<B: GlobalBytesBuffer> {
    store: Vec<BytesRef<B>>,
    map_short: HashMap<BytesRef<B>, u32>,
    map_long: HashMap<BytesRef<B>, u64>,
    buffer_id: usize,
    has_known_mark: bool,
}

impl<B: GlobalBytesBuffer> BytesDictionary<B> {
    /// Adds a new byte array to the dictionary. If the array is not known yet, it will
    /// be assigned a new id.
    ///
    /// The result is an [AddResult] that indicates if the array was newly added,
    /// or previoulsy present (it will never be rejected). The result then also yields
    /// the array's id.
    pub(crate) fn add_bytes(&mut self, bytes: &[u8]) -> AddResult {
        self.add_bytes_inner(bytes, true)
    }

    /// Looks for a given `&[u8]` slice and returns `Some(id)` if it is in the dictionary,
    /// and `None` otherwise. The special value [`super::KNOWN_ID_MARK`] will be returned
    /// if the array was marked but not actually inserted.
    pub(crate) fn bytes_to_id(&self, bytes: &[u8]) -> Option<usize> {
        if let Some(id) = self.map_short.get(bytes).copied() {
            if id == SMALL_KNOWN_ID_MARK {
                return Some(KNOWN_ID_MARK);
            } else {
                return Some(Self::id32_to_id(id));
            }
        } else if !self.map_long.is_empty() {
            if let Some(id) = self.map_long.get(bytes).copied() {
                return Some(Self::id64_to_id(id));
            }
        }
        None
    }

    /// Returns the vector of bytes associated with the `id`, or `None`` if no byte array has been
    /// associated to this id.
    pub(crate) fn id_to_bytes(&self, id: usize) -> Option<Vec<u8>> {
        self.store.get(id).map(|entry| entry.to_vec())
    }

    /// Returns true if a value is associated with the id.
    pub(crate) fn knows_id(&self, id: usize) -> bool {
        id < self.store.len()
    }

    /// Returns the number of elements in the dictionary. Strings that are merely
    /// marked are not counted here.
    pub(crate) fn len(&self) -> usize {
        self.store.len()
    }

    /// Marks the given byte array as being known without storing it under an own id.
    /// If the entry exists already, the old id will be kept and returned.
    ///
    /// Once a array has been marked, it cannot be added anymore, since it will
    /// be known already. A use case that would require other behavior is not
    /// known so far, so we do not make any effort there.
    pub(crate) fn mark_bytes(&mut self, bytes: &[u8]) -> AddResult {
        self.has_known_mark = true;
        self.add_bytes_inner(bytes, false)
    }

    /// Returns true if the dictionary contains any marked elements.
    pub(crate) fn has_marked(&self) -> bool {
        self.has_known_mark
    }

    /// Check if a bytes array is already in the dictionary, and if not,
    /// add it. If `insert` is true, the bytes will be given a new id and
    /// sotred under this id. Otherwise, the bytes will merely be marked as
    /// known.
    ///
    /// If the given array is known but not assigned an ID (indicated by
    /// [SMALL_KNOWN_ID_MARK]), then the operation will still not assign an
    /// ID either, independently of `insert`.
    #[inline(always)]
    fn add_bytes_inner(&mut self, bytes: &[u8], insert: bool) -> AddResult {
        match self.map_short.get(bytes) {
            Some(idx) => {
                if *idx == SMALL_KNOWN_ID_MARK {
                    AddResult::Known(KNOWN_ID_MARK)
                } else {
                    AddResult::Known(Self::id32_to_id(*idx))
                }
            }
            None => match self.map_long.get(bytes) {
                Some(idx) => AddResult::Known(Self::id64_to_id(*idx)),
                None => {
                    let sref = B::push_bytes(self.buffer_id, bytes);
                    if insert {
                        let id = self.store.len();
                        let inner_id = Self::id_to_innerid(id);
                        self.store.push(sref);
                        if let Ok(id_u32) = u32::try_from(inner_id) {
                            self.map_short.insert(sref, id_u32);
                        } else {
                            self.map_long.insert(
                                sref,
                                u64::try_from(inner_id)
                                    .expect("no support for platforms with more than 64bits"),
                            );
                        }
                        AddResult::Fresh(id)
                    } else {
                        self.map_short.insert(sref, SMALL_KNOWN_ID_MARK);
                        AddResult::Fresh(KNOWN_ID_MARK)
                    }
                }
            },
        }
    }

    /// Convert an id as reported to the outside to the value that
    /// would be stored to represent it in our maps (leaving space for
    /// [SMALL_KNOWN_ID_MARK]).
    fn id_to_innerid(id: usize) -> usize {
        if id < SMALL_KNOWN_ID_MARK_AS_USIZE {
            id
        } else {
            id + 1
        }
    }

    /// Convert a 32bit id that is stored in our maps to the id that should
    /// be reported to the outside (removing the space for
    /// [SMALL_KNOWN_ID_MARK]).
    fn id32_to_id(id32: u32) -> usize {
        if id32 < SMALL_KNOWN_ID_MARK {
            id32 as usize
        } else {
            (id32 - 1) as usize
        }
    }

    /// Convert a 64bit id that is stored in our maps to the id that should
    /// be reported to the outside (removing the space for
    /// [SMALL_KNOWN_ID_MARK]).
    fn id64_to_id(id64: u64) -> usize {
        assert!(id64 > u32::MAX as u64);
        usize::try_from(id64 - 1).expect("64bit ids should only occur on 64bit platforms")
    }
}

impl<B: GlobalBytesBuffer> Default for BytesDictionary<B> {
    fn default() -> Self {
        BytesDictionary {
            store: Vec::new(),
            map_short: HashMap::new(),
            map_long: HashMap::new(),
            buffer_id: B::init_buffer(),
            has_known_mark: false,
        }
    }
}

impl<B: GlobalBytesBuffer> Drop for BytesDictionary<B> {
    fn drop(&mut self) {
        B::drop_buffer(self.buffer_id);
    }
}

#[cfg(test)]
mod test {
    use crate::dictionary::{
        bytes_buffer::{BytesBuffer, GlobalBytesBuffer},
        bytes_dictionary::BytesDictionary,
        AddResult, KNOWN_ID_MARK,
    };

    crate::dictionary::bytes_buffer::declare_bytes_buffer!(TestBytesBuffer, TEST_BUFFER);
    type TestBytesDictionary = BytesDictionary<TestBytesBuffer>;

    #[test]
    fn add_and_get() {
        let mut dict: TestBytesDictionary = BytesDictionary::default();
        let mut dict2: TestBytesDictionary = BytesDictionary::default();

        let res1 = dict.add_bytes(&[1, 2, 3]);
        dict.add_bytes(&[42]);
        dict2.add_bytes(&[10, 9, 8]);
        dict.add_bytes(&[5, 6, 7]);
        dict.add_bytes(&[42]);
        let res2 = dict.add_bytes(&[1, 2, 3]);
        dict2.add_bytes(&[7, 6]);
        dict.add_bytes(&[5, 6, 7]);

        assert_eq!(dict.id_to_bytes(0), Some(vec![1, 2, 3]));
        assert_eq!(dict.id_to_bytes(1), Some(vec![42]));
        assert_eq!(dict.id_to_bytes(2), Some(vec![5, 6, 7]));
        assert_eq!(dict.id_to_bytes(3), None);
        assert_eq!(dict.id_to_bytes(1), Some(vec![42]));
        assert_eq!(dict2.id_to_bytes(0), Some(vec![10, 9, 8]));
        assert_eq!(dict2.id_to_bytes(1), Some(vec![7, 6]));
        assert_eq!(dict2.id_to_bytes(2), None);

        assert_eq!(res1, AddResult::Fresh(0));
        assert_eq!(res2, AddResult::Known(0));

        assert_eq!(dict.len(), 3);
        assert!(!dict.has_marked());
    }

    #[test]
    fn fetch_id() {
        let mut dict: TestBytesDictionary = BytesDictionary::default();

        dict.add_bytes(&[1, 2, 3]);
        dict.add_bytes(&[42]);

        assert_eq!(dict.bytes_to_id(&[1, 2, 3]), Some(0));
        assert_eq!(dict.bytes_to_id(&[42]), Some(1));
        assert_eq!(dict.bytes_to_id(&[1, 2]), None);
        assert_eq!(dict.bytes_to_id(&[]), None);
    }

    #[test]
    fn empty_bytes() {
        let mut dict: TestBytesDictionary = BytesDictionary::default();

        assert_eq!(dict.add_bytes(&[]), AddResult::Fresh(0));
        assert_eq!(dict.id_to_bytes(0), Some(vec![]));
        assert_eq!(dict.bytes_to_id(&[]), Some(0));
        assert_eq!(dict.len(), 1);
        assert!(!dict.has_marked());
    }

    #[test]
    fn mark_str() {
        let mut dict: TestBytesDictionary = BytesDictionary::default();

        assert_eq!(dict.add_bytes(&[1]), AddResult::Fresh(0));
        assert_eq!(dict.add_bytes(&[2]), AddResult::Fresh(1));
        assert_eq!(dict.mark_bytes(&[1]), AddResult::Known(0));
        assert_eq!(dict.mark_bytes(&[3]), AddResult::Fresh(KNOWN_ID_MARK));
        assert_eq!(dict.mark_bytes(&[4]), AddResult::Fresh(KNOWN_ID_MARK));
        assert_eq!(dict.add_bytes(&[3]), AddResult::Known(KNOWN_ID_MARK));

        assert_eq!(dict.len(), 2);
        assert!(dict.has_marked());

        assert_eq!(dict.id_to_bytes(0), Some(vec![1]));
        assert_eq!(dict.bytes_to_id(&[1]), Some(0));
        assert_eq!(dict.bytes_to_id(&[2]), Some(1));
        assert_eq!(dict.bytes_to_id(&[3]), Some(KNOWN_ID_MARK));
        assert_eq!(dict.bytes_to_id(&[4]), Some(KNOWN_ID_MARK));
    }
}
