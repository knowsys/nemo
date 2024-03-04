use super::{
    bytes_buffer::{BytesBuffer, GlobalBytesBuffer},
    bytes_pair_dictionary::BytesPairDictionary,
    AddResult,
};

/// A struct that implements a bijection between strings pairs and integers
/// ids. Ids are automatically assigned upon insertion. Data is stored in a the
/// given [GlobalBytesBuffer].
#[derive(Debug)]
pub(crate) struct GenericStringPairDictionary<B: GlobalBytesBuffer> {
    bytes_pair_dict: BytesPairDictionary<B>,
}
impl<B: GlobalBytesBuffer> GenericStringPairDictionary<B> {
    /// Adds a new string pair to the dictionary. If the string pair is not
    /// known yet, it will be assigned a new id.
    ///
    /// The result is an [AddResult] that indicates if the string was newly added,
    /// or previoulsy present (it will never be rejected). The result then also yields
    /// the strings id.
    pub(crate) fn add_str_pair(&mut self, first: &str, second: &str) -> AddResult {
        self.bytes_pair_dict
            .add_bytes_pair(first.as_bytes(), second.as_bytes())
    }

    /// Looks for a given [&str] slice pairs and returns `Some(id)` if it is in
    /// the dictionary, and `None` otherwise. The special value
    /// [`super::KNOWN_ID_MARK`] will be returned if the string pair was marked
    /// but not actually inserted.
    pub(crate) fn str_pair_to_id(&self, first: &str, second: &str) -> Option<usize> {
        self.bytes_pair_dict
            .bytes_pair_to_id(first.as_bytes(), second.as_bytes())
    }

    /// Returns the [String] associated with the `id`, or `None`` if no string has been
    /// associated to this id.
    pub(crate) fn id_to_string_pair(&self, id: usize) -> Option<[String; 2]> {
        self.bytes_pair_dict
            .id_to_bytes_pair(id)
            .map(|[first, second]| {
                [unsafe { String::from_utf8_unchecked(first) }, unsafe {
                    String::from_utf8_unchecked(second)
                }]
            })
    }

    /// Returns true if a value is associated with the id.
    pub(crate) fn knows_id(&self, id: usize) -> bool {
        self.bytes_pair_dict.knows_id(id)
    }

    /// Returns the number of string pairs in the dictionary. Strings pairs
    /// that are marked are not counted here.
    pub(crate) fn len(&self) -> usize {
        self.bytes_pair_dict.len()
    }

    /// Marks the given string pair as being known without storing it under an
    /// own id. If the entry exists already, the old id will be kept and
    /// returned.
    ///
    /// Once a string has been marked, it cannot be added anymore, since it will
    /// be known already. A use case that would require other behavior is not
    /// known so far, so we do not make any effort there.
    pub(crate) fn mark_str_pair(&mut self, first: &str, second: &str) -> AddResult {
        self.bytes_pair_dict
            .mark_bytes(first.as_bytes(), second.as_bytes())
    }

    /// Returns true if the dictionary contains any marked elements.
    pub(crate) fn has_marked(&self) -> bool {
        self.bytes_pair_dict.has_marked()
    }
}

impl<B: GlobalBytesBuffer> Default for GenericStringPairDictionary<B> {
    fn default() -> Self {
        GenericStringPairDictionary {
            bytes_pair_dict: Default::default(),
        }
    }
}

crate::dictionary::bytes_buffer::declare_bytes_buffer!(
    StringDictBytesPairBuffer,
    STRING_DICT_BYTES_BUFFER
);

pub(crate) type StringPairDictionary = GenericStringPairDictionary<StringDictBytesPairBuffer>;

/* /// Do I need this?????
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
} */

#[cfg(test)]
mod test {
    use super::StringPairDictionary;
    use crate::dictionary::{AddResult, KNOWN_ID_MARK};
    use std::borrow::Borrow;

    fn create_dict() -> StringPairDictionary {
        let mut dict = StringPairDictionary::default();
        let vec = vec![
            ["a", "b"],
            ["b", "c"],
            ["c", "a"],
            ["Position 3.1", "Position 3.1"],
            ["Position 4.2", "Position 4.1"],
            ["Position 5.3", "Position 5.1"],
            ["Position 6.4", "Position 6.1"],
        ];

        for [a, b] in vec {
            dict.add_str_pair(a, b);
        }
        dict
    }

    #[test]
    fn get() {
        let mut dict = create_dict();

        let mut dict2 = StringPairDictionary::default();
        dict2.add_str_pair("E0.1", "E0.2");
        dict.add_str_pair("another entry 1", "another entry 2");
        dict2.add_str_pair("E2.1", "E2.2");

        assert_eq!(
            dict.id_to_string_pair(0),
            Some(["a".to_string(), "b".to_string()])
        );
        assert_eq!(
            dict.id_to_string_pair(1),
            Some(["b".to_string(), "c".to_string()])
        );
        assert_eq!(
            dict.id_to_string_pair(2),
            Some(["c".to_string(), "a".to_string()])
        );
        assert_eq!(
            dict.id_to_string_pair(3),
            Some(["Position 3.1".to_string(), "Position 3.2".to_string()])
        );
        assert_eq!(
            dict.id_to_string_pair(4),
            Some(["Position 4.1".to_string(), "Position 4.2".to_string()])
        );
        assert_eq!(
            dict.id_to_string_pair(5),
            Some(["Position 5.1".to_string(), "Position 5.2".to_string()])
        );
        assert_eq!(
            dict.id_to_string_pair(6),
            Some(["Position 6.1".to_string(), "Position 6.2".to_string()])
        );
        assert_eq!(dict.id_to_string_pair(7), None);

        assert_eq!(
            dict2.id_to_string_pair(0),
            Some(["E0.1".to_string(), "E0.2".to_string()])
        );
        assert_eq!(
            dict2.id_to_string_pair(1),
            Some(["another entry 1".to_string(), "another entry 2".to_string()])
        );
        assert_eq!(
            dict2.id_to_string_pair(2),
            Some(["E2.1".to_string(), "E2.2".to_string()])
        );
        assert_eq!(dict2.id_to_string_pair(3), None);
    }

    #[test]
    fn fetch_id() {
        let dict = create_dict();
        assert_eq!(dict.str_pair_to_id("a", "b"), Some(0));
        assert_eq!(dict.str_pair_to_id("b", "c"), Some(1));
        assert_eq!(dict.str_pair_to_id("c", "a"), Some(2));
        assert_eq!(dict.str_pair_to_id("Position 3.1", "Position 3.1"), Some(3));
        assert_eq!(dict.str_pair_to_id("Position 4.1", "Position 4.1"), Some(4));
        assert_eq!(dict.str_pair_to_id("Position 5.1", "Position 5.1"), Some(5));
        assert_eq!(dict.str_pair_to_id("Position 6.1", "Position 6.1"), Some(6));
        assert_eq!(dict.str_pair_to_id("", ""), None);
        assert_eq!(dict.str_pair_to_id("a", "a"), None);
        assert_eq!(dict.str_pair_to_id("a", "d"), None);
    }

    #[test]
    fn add() {
        let mut dict = create_dict();
        assert_eq!(dict.add_str_pair("a", "b"), AddResult::Known(0));
        assert_eq!(dict.add_str_pair("a", "a"), AddResult::Fresh(7));
    }

    #[test]
    fn empty_str() {
        let mut dict = StringPairDictionary::default();
        assert_eq!(dict.add_str_pair("", ""), AddResult::Fresh(0));
        assert_eq!(
            dict.id_to_string_pair(0),
            Some(["".to_string(), "".to_string()])
        );
        assert_eq!(dict.str_pair_to_id("", ""), Some(0));
        assert_eq!(dict.len(), 1);
        assert!(!dict.has_marked());
    }

    #[test]
    fn mark_str() {
        let mut dict = StringPairDictionary::default();

        assert_eq!(dict.add_str_pair("e01", "e02"), AddResult::Fresh(0));
        assert_eq!(dict.add_str_pair("e11", "e12"), AddResult::Fresh(1));
        assert_eq!(dict.mark_str_pair("e01", "e02"), AddResult::Known(0));
        assert_eq!(
            dict.mark_str_pair("e21", "e22"),
            AddResult::Fresh(KNOWN_ID_MARK)
        );
        assert_eq!(
            dict.mark_str_pair("e31", "e32"),
            AddResult::Fresh(KNOWN_ID_MARK)
        );
        assert_eq!(
            dict.add_str_pair("e21", "e22"),
            AddResult::Known(KNOWN_ID_MARK)
        );

        assert_eq!(dict.len(), 2);
        assert!(dict.has_marked());

        assert_eq!(
            dict.id_to_string_pair(0),
            Some(["e01".to_string(), "e02".to_string()])
        );
        assert_eq!(dict.str_pair_to_id("e01", "e02"), Some(0));
        assert_eq!(dict.str_pair_to_id("e11", "e12"), Some(1));
        assert_eq!(dict.str_pair_to_id("e21", "e22"), Some(KNOWN_ID_MARK));
        assert_eq!(dict.str_pair_to_id("e31", "e32"), Some(KNOWN_ID_MARK));
    }
}
