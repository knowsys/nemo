use hashbrown::HashMap;

use super::bytes_buffer::{BytesRef, GlobalBytesBuffer};
use super::{AddResult, KNOWN_ID_MARK};

/// This macro declares a new BytesDictionary of the given name,
/// which uses the given buffer name to store its global data.
/// The buffer name must be unique to each declared dictionary type,
/// but upper case (which we do not attempt to achieve in the macro ...).
/// All declarations are at the place where the macro is invoked and
/// are private. There is no need for wider visibility, since one can better
/// always create a new type for a new use of such a dictionary.
#[macro_export]
macro_rules! declare_bytes_dictionary {
    ($dict_name:ident, $buf_name:ident) => {
        static mut $buf_name: BytesBuffer = BytesBuffer::new();
        paste::paste! {
            #[derive(Debug)]
            struct [<$dict_name GlobalBuffer>];
            unsafe impl GlobalBytesBuffer for [<$dict_name GlobalBuffer>] {
                unsafe fn get() -> &'static mut BytesBuffer {
                    &mut $buf_name
                }
            }
            type $dict_name = BytesDictionary<[<$dict_name GlobalBuffer>]>;
        }
    };
}

/// A struct that implements a bijection between byte arrays and integers, where the integers
/// are automatically assigned upon insertion.
/// Byte arrays are stored in a compact buffer to reduce memory overhead and fragmentation.
#[derive(Debug)]
pub(crate) struct BytesDictionary<B: GlobalBytesBuffer> {
    store: Vec<BytesRef<B>>,
    mapping: HashMap<BytesRef<B>, usize>,
    buffer_id: usize,
    has_known_mark: bool,
}

impl<B: GlobalBytesBuffer> BytesDictionary<B> {
    /// Construct a new and empty bytes dictionary.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Adds a new byte array to the dictionary. If the array is not known yet, it will
    /// be assigned a new id.
    ///
    /// The result is an [AddResult] that indicates if the array was newly added,
    /// or previoulsy present (it will never be rejected). The result then also yields
    /// the array's id.
    pub(crate) fn add_bytes(&mut self, bytes: &[u8]) -> AddResult {
        self.add_bytes_with_id(bytes, self.store.len())
    }

    /// Looks for a given `&[u8]` slice and returns `Some(id)` if it is in the dictionary,
    /// and `None` otherwise. The special value [`super::KNOWN_ID_MARK`] will be returned
    /// if the array was marked but not actually inserted.
    pub(crate) fn bytes_to_id(&self, bytes: &[u8]) -> Option<usize> {
        self.mapping.get(bytes).copied()
    }

    /// Returns the vector of bytes associated with the `id`, or `None`` if no byte array has been
    /// associated to this id.
    pub(crate) fn id_to_bytes(&self, id: usize) -> Option<Vec<u8>> {
        self.store.get(id).map(|entry| entry.to_vec())
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
        self.add_bytes_with_id(bytes, KNOWN_ID_MARK)
    }

    /// Returns true if the dictionary contains any marked elements.
    pub(crate) fn has_marked(&self) -> bool {
        self.has_known_mark
    }

    /// Check if a bytes array is already in the dictionary, and if not,
    /// set its id to the given value. This is an internal helper method
    /// that is only ever called with `self.store.len()` or with
    /// [KNOWN_ID_MARK] as id. Other non-consecutive ID assignments
    /// will generally lead to errors, since the same ID might be assigned
    /// later on again.
    ///
    /// If the given array is known but not assigned an ID (indicated by
    /// [KNOWN_ID_MARK]), then the operation will still not assign an
    /// ID either.
    #[inline(always)]
    fn add_bytes_with_id(&mut self, bytes: &[u8], id: usize) -> AddResult {
        match self.mapping.get(bytes) {
            Some(idx) => AddResult::Known(*idx),
            None => {
                let sref = B::push_bytes(self.buffer_id, bytes);
                if id != KNOWN_ID_MARK {
                    self.store.push(sref);
                }
                self.mapping.insert(sref, id);
                AddResult::Fresh(id)
            }
        }
    }
}

impl<B: GlobalBytesBuffer> Default for BytesDictionary<B> {
    fn default() -> Self {
        BytesDictionary {
            store: Vec::new(),
            mapping: HashMap::new(),
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
    use crate::declare_bytes_dictionary;
    use crate::dictionary::{
        bytes_buffer::{BytesBuffer, GlobalBytesBuffer},
        bytes_dictionary::BytesDictionary,
        AddResult, KNOWN_ID_MARK,
    };

    declare_bytes_dictionary!(TestBytesDictionary, TEST_BUFFER);

    #[test]
    fn add_and_get() {
        let mut dict: TestBytesDictionary = BytesDictionary::new();
        let mut dict2: TestBytesDictionary = BytesDictionary::new();

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
        assert_eq!(dict.has_marked(), false);
    }

    #[test]
    fn fetch_id() {
        let mut dict: TestBytesDictionary = BytesDictionary::new();

        dict.add_bytes(&[1, 2, 3]);
        dict.add_bytes(&[42]);

        assert_eq!(dict.bytes_to_id(&[1, 2, 3]), Some(0));
        assert_eq!(dict.bytes_to_id(&[42]), Some(1));
        assert_eq!(dict.bytes_to_id(&[1, 2]), None);
        assert_eq!(dict.bytes_to_id(&[]), None);
    }

    #[test]
    fn empty_bytes() {
        let mut dict: TestBytesDictionary = BytesDictionary::new();

        assert_eq!(dict.add_bytes(&[]), AddResult::Fresh(0));
        assert_eq!(dict.id_to_bytes(0), Some(vec![]));
        assert_eq!(dict.bytes_to_id(&[]), Some(0));
        assert_eq!(dict.len(), 1);
        assert_eq!(dict.has_marked(), false);
    }

    #[test]
    fn mark_str() {
        let mut dict: TestBytesDictionary = BytesDictionary::new();

        assert_eq!(dict.add_bytes(&[1]), AddResult::Fresh(0));
        assert_eq!(dict.add_bytes(&[2]), AddResult::Fresh(1));
        assert_eq!(dict.mark_bytes(&[1]), AddResult::Known(0));
        assert_eq!(dict.mark_bytes(&[3]), AddResult::Fresh(KNOWN_ID_MARK));
        assert_eq!(dict.mark_bytes(&[4]), AddResult::Fresh(KNOWN_ID_MARK));
        assert_eq!(dict.add_bytes(&[3]), AddResult::Known(KNOWN_ID_MARK));

        assert_eq!(dict.len(), 2);
        assert_eq!(dict.has_marked(), true);

        assert_eq!(dict.id_to_bytes(0), Some(vec![1]));
        assert_eq!(dict.bytes_to_id(&[1]), Some(0));
        assert_eq!(dict.bytes_to_id(&[2]), Some(1));
        assert_eq!(dict.bytes_to_id(&[3]), Some(KNOWN_ID_MARK));
        assert_eq!(dict.bytes_to_id(&[4]), Some(KNOWN_ID_MARK));
    }
}
