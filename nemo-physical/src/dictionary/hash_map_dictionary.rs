use super::{AddResult, Dictionary, DictionaryString, StringDictionary};

/// A read-only, hashmap-based [Dictionary] to implement a bijection between strings and integers.  
/// Strings are stored in a compact buffer to reduce memory overhead and fragmentation.
#[derive(Debug, Default)]
pub struct HashMapDictionary {
    string_dict: StringDictionary,
}

impl HashMapDictionary {
    /// Construct a new and empty dictionary.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Dictionary for HashMapDictionary {
    fn add_string(&mut self, string: String) -> AddResult {
        self.string_dict.add_str(string.as_str())
    }

    fn add_str(&mut self, string: &str) -> AddResult {
        self.string_dict.add_str(string)
    }

    fn add_dictionary_string(&mut self, ds: DictionaryString) -> AddResult {
        self.string_dict.add_str(ds.as_str())
    }

    fn fetch_id(&self, string: &str) -> Option<usize> {
        self.string_dict.str_to_id(string)
    }

    fn get(&self, id: usize) -> Option<String> {
        self.string_dict.id_to_string(id)
    }

    fn len(&self) -> usize {
        self.string_dict.len()
    }

    fn mark_str(&mut self, string: &str) -> AddResult {
        self.string_dict.mark_str(string)
    }

    fn has_marked(&self) -> bool {
        self.string_dict.has_marked()
    }
}

#[cfg(test)]
mod test {
    use crate::dictionary::AddResult;
    use crate::dictionary::Dictionary;

    use super::HashMapDictionary;

    fn create_dict() -> HashMapDictionary {
        let mut dict = HashMapDictionary::default();
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
            dict.add_string(i.to_string());
        }
        dict
    }

    #[test]
    fn get() {
        let mut dict = create_dict();

        let mut dict2 = HashMapDictionary::default();
        dict2.add_string("entry0".to_string());
        dict.add_string("another entry".to_string());
        dict2.add_string("entry1".to_string());

        assert_eq!(dict.get(0), Some("a".to_string()));
        assert_eq!(dict.get(1), Some("b".to_string()));
        assert_eq!(dict.get(2), Some("c".to_string()));
        assert_eq!(dict.get(3), Some("Position 3".to_string()));
        assert_eq!(dict.get(4), Some("Position 4".to_string()));
        assert_eq!(dict.get(5), Some("Position 5".to_string()));
        assert_eq!(dict.get(6), Some("another entry".to_string()));
        assert_eq!(dict.get(7), None);
        assert_eq!(dict.get(3), Some("Position 3".to_string()));

        assert_eq!(dict2.get(0), Some("entry0".to_string()));
        assert_eq!(dict2.get(1), Some("entry1".to_string()));
        assert_eq!(dict2.get(2), None);
    }

    #[test]
    fn fetch_id() {
        let dict = create_dict();
        assert_eq!(dict.fetch_id("a"), Some(0));
        assert_eq!(dict.fetch_id("b"), Some(1));
        assert_eq!(dict.fetch_id("c"), Some(2));
        assert_eq!(dict.fetch_id("Position 3"), Some(3));
        assert_eq!(dict.fetch_id("Position 4"), Some(4));
        assert_eq!(dict.fetch_id("Position 5"), Some(5));
        assert_eq!(dict.fetch_id("d"), None);
        assert_eq!(dict.fetch_id("Pos"), None);
        assert_eq!(dict.fetch_id("Pos"), None);
        assert_eq!(dict.fetch_id("b"), Some(1));
    }

    #[test]
    fn add() {
        let mut dict = create_dict();
        assert_eq!(dict.add_string("a".to_string()), AddResult::Known(0));
        assert_eq!(
            dict.add_string("new value".to_string()),
            AddResult::Fresh(6)
        );
    }

    #[test]
    fn empty_str() {
        let mut dict = HashMapDictionary::default();
        assert_eq!(dict.add_string("".to_string()), AddResult::Fresh(0));
        assert_eq!(dict.get(0), Some("".to_string()));
        assert_eq!(dict.fetch_id(""), Some(0));
    }
}
