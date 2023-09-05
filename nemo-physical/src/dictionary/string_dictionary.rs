use super::Dictionary;
use super::AddResult;
use super::DictionaryString;
use std::collections::HashMap;
use std::rc::Rc;

/// Offers a simple way to store multiple [String] objects, associate them to a [usize] and manage ownership for them
#[derive(Clone, Debug, Default)]
pub struct StringDictionary {
    store: Vec<Rc<String>>,
    mapping: HashMap<Rc<String>, usize>,
}

impl Dictionary for StringDictionary {
    fn new() -> Self {
        Default::default()
    }

    fn add_string(&mut self, entry: String) -> AddResult {
        match self.mapping.get(&entry) {
            Some(idx) => AddResult::Known(*idx),
            None => {
                let len = self.store.len();
                self.store.push(Rc::new(entry));
                self.mapping.insert(self.store[len].clone(), len);
                AddResult::Fresh(len)
            }
        }
    }

    fn add_str(&mut self, string: &str) -> AddResult {
        self.add_string(string.to_string())
    }

    fn add_dictionary_string(&mut self, ds: &mut DictionaryString) -> AddResult {
        self.add_str(ds.as_str())
    }

    fn fetch_id(&self, entry: &str) -> Option<usize> {
        self.mapping.get(&entry.to_string()).copied()
    }

    fn get(&self, index: usize) -> Option<String> {
        self.store
            .get(index)
            .map(|entry| -> String { Rc::clone(entry).to_string() })
    }

    fn len(&self) -> usize {
        self.mapping.len()
    }
}

#[cfg(test)]
mod test {
    use std::borrow::Borrow;

    use crate::dictionary::Dictionary;
    use crate::dictionary::AddResult;

    use super::StringDictionary;

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
            dict.add_string(i.to_string());
        }
        dict
    }

    #[test]
    fn entry() {
        let dict = create_dict();
        assert_eq!(dict.get(0), Some("a".to_string()));
        assert_eq!(dict.get(1), Some("b".to_string()));
        assert_eq!(dict.get(2), Some("c".to_string()));
        assert_eq!(dict.get(3), Some("Position 3".to_string()));
        assert_eq!(dict.get(4), Some("Position 4".to_string()));
        assert_eq!(dict.get(5), Some("Position 5".to_string()));
        assert_eq!(dict.get(6), None);
        assert_eq!(dict.get(3), Some("Position 3".to_string()));
    }

    #[test]
    fn index_of() {
        let dict = create_dict();
        assert_eq!(dict.fetch_id("a".to_string().borrow()), Some(0));
        assert_eq!(dict.fetch_id("b".to_string().borrow()), Some(1));
        assert_eq!(dict.fetch_id("c".to_string().borrow()), Some(2));
        assert_eq!(dict.fetch_id("Position 3".to_string().borrow()), Some(3));
        assert_eq!(dict.fetch_id("Position 4".to_string().borrow()), Some(4));
        assert_eq!(dict.fetch_id("Position 5".to_string().borrow()), Some(5));
        assert_eq!(dict.fetch_id("d".to_string().borrow()), None);
        assert_eq!(dict.fetch_id("Pos".to_string().borrow()), None);
        assert_eq!(dict.fetch_id("Pos"), None);
        assert_eq!(dict.fetch_id("b"), Some(1));
    }

    #[test]
    fn add() {
        let mut dict = create_dict();
        assert_eq!(dict.add_string("a".to_string()), AddResult::Known(0));
        assert_eq!(dict.add_string("new value".to_string()), AddResult::Fresh(6));
    }
}
