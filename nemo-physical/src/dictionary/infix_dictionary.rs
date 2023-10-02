use super::Dictionary;
use super::AddResult;
use super::DictionaryString;
use super::hash_map_dictionary::HashMapDictionary;

/// A read-only [Dictionary] to implement a bijection between integer ids and strings that start and end
/// with a certain fixed prefix and postfix, respectively. Strings that do not have this shape will be
/// rejected.
#[derive(Clone, Debug)]
pub struct InfixDictionary {
    dict: HashMapDictionary,
    prefix: String,
    suffix: String,
}

impl InfixDictionary {
    /// Construct a new and empty dictionary for the given prefix and suffix.
    pub fn new(prefix: String, suffix: String) -> Self {
        InfixDictionary{dict: HashMapDictionary::new(), prefix, suffix}
    }

    /// Add a given infix string to the internal dictionary
    fn add_infix_str(&mut self, string: &str) -> AddResult {
        self.dict.add_str(string)
    }
}

impl Dictionary for InfixDictionary {
    /// Adds a string to the disctionary. It is checked if the string has the required prefix and suffix
    /// to be in this dictionary. If this check fails, the string is rejected.
    fn add_string(&mut self, string: String) -> AddResult {
        self.add_str(string.as_str())
    }

    /// Adds a string to the disctionary. It is checked if the string has the required prefix and suffix
    /// to be in this dictionary. If this check fails, the string is rejected.
    fn add_str(&mut self, string: &str) -> AddResult {
        if string.starts_with(self.prefix.as_str()) && string.ends_with(self.suffix.as_str()) {
            self.add_infix_str(&string[self.prefix.len()..string.len()-self.suffix.len()])
        } else {
            AddResult::Rejected
        }
    }

    /// Add a string to the dictionary, but assume that the added string
    /// consists of the fixed prefix, followed by the given string's infix,
    /// followed by the fixed suffix. There is no check that the given prefix
    /// and suffix are actually the same as the fixed ones.
    fn add_dictionary_string(&mut self, ds: DictionaryString) -> AddResult {
        self.add_infix_str(ds.infix())
    }

    fn fetch_id(&self, string: &str) -> Option<usize> {
        if string.starts_with(self.prefix.as_str()) && string.ends_with(self.suffix.as_str()) {
            unsafe{
                self.dict.fetch_id(&string.get_unchecked(self.prefix.len()..string.len()-self.suffix.len()))
            }
        } else {
            None
        }
    }

    /// Look up a string in the dictionary, but assume that the added string
    /// consists of the fixed prefix, followed by the given string's infix,
    /// followed by the fixed suffix. There is no check that the given prefix
    /// and suffix are actually the same as the fixed ones.
    fn fetch_id_for_dictionary_string(&self, ds: &DictionaryString) -> Option<usize> {
        self.dict.fetch_id(ds.infix())
    }

    fn get(&self, id: usize) -> Option<String> {
        let subresult = self.dict.get(id);
        if subresult.is_some() {
            return Some(self.prefix.clone() + subresult.unwrap().as_str() + self.suffix.as_str());
        }
        None
    }

    fn len(&self) -> usize {
        self.dict.len()
    }

    /// Marks a string for this dictionary, as described in the documentation of [Dictionary].
    /// The given string must use the dictionary's prefix and suffix: no further checks are
    /// performed here. 
    fn mark_str(&mut self, string: &str) -> AddResult {
        self.dict.mark_str(&string[self.prefix.len()..string.len()-self.suffix.len()])
    }

    fn has_marked(&self) -> bool {
        self.dict.has_marked()
    }
}

#[cfg(test)]
mod test {

    use crate::dictionary::AddResult;
    use crate::dictionary::Dictionary;
    use crate::dictionary::DictionaryString;

    use super::InfixDictionary;

    #[test]
    fn add_and_get() {
        let mut dict = InfixDictionary::new("<https://example.org/".to_string(), ">".to_string());

        assert_eq!(dict.add_str("<https://example.org/test>"), AddResult::Fresh(0));
        assert_eq!(dict.add_str("<https://wronginfix.example.org/test>"), AddResult::Rejected);
        assert_eq!(dict.add_str("<https://example.org/test>wrongsuffix"), AddResult::Rejected);
        assert_eq!(dict.get(0), Some("<https://example.org/test>".to_string()));
        assert_eq!(dict.fetch_id("<https://example.org/test>"), Some(0));
    }

    #[test]
    fn add_and_get_ds() {
        let mut dict = InfixDictionary::new("<https://example.org/".to_string(), ">".to_string());

        let ds1 = DictionaryString::new("<https://example.org/test>");
        let ds2 = DictionaryString::new("<https://not.example.org/test2>"); // should still be accepted

        assert_eq!(dict.add_dictionary_string(ds1), AddResult::Fresh(0));
        assert_eq!(dict.add_dictionary_string(ds2), AddResult::Fresh(1));
        assert_eq!(dict.get(0), Some("<https://example.org/test>".to_string()));
        assert_eq!(dict.get(1), Some("<https://example.org/test2>".to_string()));
        assert_eq!(dict.fetch_id("<https://example.org/test>"), Some(0));
        assert_eq!(dict.fetch_id("<https://example.org/test2>"), Some(1));
    }
}