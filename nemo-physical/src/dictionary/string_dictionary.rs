use super::{
    bytes_buffer::{BytesBuffer, GlobalBytesBuffer},
    bytes_dictionary::BytesDictionary,
    AddResult,
};

/// A struct that implements a bijection between strings and integers, where the integers
/// are automatically assigned upon insertion.
/// Data is stored in a the given [GlobalBytesBuffer].
#[derive(Debug)]
pub(crate) struct GenericStringDictionary<B: GlobalBytesBuffer> {
    bytes_dict: BytesDictionary<B>,
}
impl<B: GlobalBytesBuffer> GenericStringDictionary<B> {
    /// Adds a new string to the dictionary. If the string is not known yet, it will
    /// be assigned a new id.
    ///
    /// The result is an [AddResult] that indicates if the string was newly added,
    /// or previoulsy present (it will never be rejected). The result then also yields
    /// the strings id.
    pub(crate) fn add_str(&mut self, string: &str) -> AddResult {
        self.bytes_dict.add_bytes(string.as_bytes())
    }

    /// Looks for a given [&str] slice and returns `Some(id)` if it is in the dictionary,
    /// and `None` otherwise. The special value [`super::KNOWN_ID_MARK`] will be returned
    /// if the string was marked but not actually inserted.
    pub(crate) fn str_to_id(&self, string: &str) -> Option<usize> {
        self.bytes_dict.bytes_to_id(string.as_bytes())
    }

    /// Returns the [String] associated with the `id`, or `None`` if no string has been
    /// associated to this id.
    pub(crate) fn id_to_string(&self, id: usize) -> Option<String> {
        self.bytes_dict
            .id_to_bytes(id)
            .map(|bytes| unsafe { String::from_utf8_unchecked(bytes) })
    }

    /// Returns true if a value is associated with the id.
    pub(crate) fn knows_id(&self, id: usize) -> bool {
        self.bytes_dict.knows_id(id)
    }

    /// Returns the number of elements in the dictionary. Strings that are merely
    /// marked are not counted here.
    pub(crate) fn len(&self) -> usize {
        self.bytes_dict.len()
    }

    /// Marks the given string as being known without storing it under an own id.
    /// If the entry exists already, the old id will be kept and returned.
    ///
    /// Once a string has been marked, it cannot be added anymore, since it will
    /// be known already. A use case that would require other behavior is not
    /// known so far, so we do not make any effort there.
    pub(crate) fn mark_str(&mut self, string: &str) -> AddResult {
        self.bytes_dict.mark_bytes(string.as_bytes())
    }

    /// Returns true if the dictionary contains any marked elements.
    pub(crate) fn has_marked(&self) -> bool {
        self.bytes_dict.has_marked()
    }
}

impl<B: GlobalBytesBuffer> Default for GenericStringDictionary<B> {
    fn default() -> Self {
        GenericStringDictionary {
            bytes_dict: Default::default(),
        }
    }
}

crate::dictionary::bytes_buffer::declare_bytes_buffer!(
    StringDictBytesBuffer,
    STRING_DICT_BYTES_BUFFER
);
pub(crate) type StringDictionary = GenericStringDictionary<StringDictBytesBuffer>;

/// This implementation is only provided public to benchmark this part of the code.
/// May be removed at some point. Don't use.
#[derive(Debug, Default)]
pub struct BenchmarkStringDictionary(StringDictionary);
impl BenchmarkStringDictionary {
    /// Adds a new string to the dictionary. If the string is not known yet, it will
    /// be assigned a new id.
    pub fn add_str(&mut self, string: &str) -> AddResult {
        self.0.add_str(string)
    }

    /// Returns the number of elements in the dictionary. Strings that are merely
    /// marked are not counted here.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

#[cfg(test)]
mod test {
    use super::StringDictionary;
    use crate::dictionary::{AddResult, KNOWN_ID_MARK};
    use std::borrow::Borrow;

    fn create_dict() -> StringDictionary {
        let mut dict = StringDictionary::default();
        let vec: Vec<&str> = vec![
            "a",
            "b",
            "c",
            "a",
            "b",
            "c",
            "Position 3",
            "Position 4",
            "Position 3",
            "Position 5",
        ];

        for i in vec {
            dict.add_str(i);
        }
        dict
    }

    #[test]
    fn get() {
        let mut dict = create_dict();

        let mut dict2 = StringDictionary::default();
        dict2.add_str("entry0");
        dict.add_str("another entry");
        dict2.add_str("entry1");

        assert_eq!(dict.id_to_string(0), Some("a".to_string()));
        assert_eq!(dict.id_to_string(1), Some("b".to_string()));
        assert_eq!(dict.id_to_string(2), Some("c".to_string()));
        assert_eq!(dict.id_to_string(3), Some("Position 3".to_string()));
        assert_eq!(dict.id_to_string(4), Some("Position 4".to_string()));
        assert_eq!(dict.id_to_string(5), Some("Position 5".to_string()));
        assert_eq!(dict.id_to_string(6), Some("another entry".to_string()));
        assert_eq!(dict.id_to_string(7), None);
        assert_eq!(dict.id_to_string(3), Some("Position 3".to_string()));

        assert_eq!(dict2.id_to_string(0), Some("entry0".to_string()));
        assert_eq!(dict2.id_to_string(1), Some("entry1".to_string()));
        assert_eq!(dict2.id_to_string(2), None);
    }

    #[test]
    fn fetch_id() {
        let dict = create_dict();
        assert_eq!(dict.str_to_id("a".to_string().borrow()), Some(0));
        assert_eq!(dict.str_to_id("b".to_string().borrow()), Some(1));
        assert_eq!(dict.str_to_id("c".to_string().borrow()), Some(2));
        assert_eq!(dict.str_to_id("Position 3".to_string().borrow()), Some(3));
        assert_eq!(dict.str_to_id("Position 4".to_string().borrow()), Some(4));
        assert_eq!(dict.str_to_id("Position 5".to_string().borrow()), Some(5));
        assert_eq!(dict.str_to_id("d".to_string().borrow()), None);
        assert_eq!(dict.str_to_id("Pos".to_string().borrow()), None);
        assert_eq!(dict.str_to_id("Pos"), None);
        assert_eq!(dict.str_to_id("b"), Some(1));
    }

    #[test]
    fn add() {
        let mut dict = create_dict();
        assert_eq!(dict.add_str("a"), AddResult::Known(0));
        assert_eq!(dict.add_str("new value"), AddResult::Fresh(6));
    }

    #[test]
    fn empty_str() {
        let mut dict = StringDictionary::default();
        assert_eq!(dict.add_str(""), AddResult::Fresh(0));
        assert_eq!(dict.id_to_string(0), Some("".to_string()));
        assert_eq!(dict.str_to_id(""), Some(0));
        assert_eq!(dict.len(), 1);
        assert_eq!(dict.has_marked(), false);
    }

    #[test]
    fn mark_str() {
        let mut dict = StringDictionary::default();

        assert_eq!(dict.add_str("entry1"), AddResult::Fresh(0));
        assert_eq!(dict.add_str("entry2"), AddResult::Fresh(1));
        assert_eq!(dict.mark_str("entry1"), AddResult::Known(0));
        assert_eq!(dict.mark_str("entry3"), AddResult::Fresh(KNOWN_ID_MARK));
        assert_eq!(dict.mark_str("entry4"), AddResult::Fresh(KNOWN_ID_MARK));
        assert_eq!(dict.add_str("entry3"), AddResult::Known(KNOWN_ID_MARK));

        assert_eq!(dict.len(), 2);
        assert_eq!(dict.has_marked(), true);

        assert_eq!(dict.id_to_string(0), Some("entry1".to_string()));
        assert_eq!(dict.str_to_id("entry1"), Some(0));
        assert_eq!(dict.str_to_id("entry2"), Some(1));
        assert_eq!(dict.str_to_id("entry3"), Some(KNOWN_ID_MARK));
        assert_eq!(dict.str_to_id("entry4"), Some(KNOWN_ID_MARK));
    }
}
