use super::Dictionary;
use super::EntryStatus;
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

    fn add(&mut self, entry: String) -> EntryStatus {
        match self.mapping.get(&entry) {
            Some(idx) => EntryStatus::Known(*idx),
            None => {
                let len = self.store.len();
                self.store.push(Rc::new(entry));
                self.mapping.insert(self.store[len].clone(), len);
                EntryStatus::Fresh(len)
            }
        }
    }

    fn index_of(&self, entry: &str) -> Option<usize> {
        self.mapping.get(&entry.to_string()).copied()
    }

    fn entry(&self, index: usize) -> Option<String> {
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
    use crate::dictionary::EntryStatus;

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
            dict.add(i.to_string());
        }
        dict
    }

    #[test]
    fn entry() {
        let dict = create_dict();
        assert_eq!(dict.entry(0), Some("a".to_string()));
        assert_eq!(dict.entry(1), Some("b".to_string()));
        assert_eq!(dict.entry(2), Some("c".to_string()));
        assert_eq!(dict.entry(3), Some("Position 3".to_string()));
        assert_eq!(dict.entry(4), Some("Position 4".to_string()));
        assert_eq!(dict.entry(5), Some("Position 5".to_string()));
        assert_eq!(dict.entry(6), None);
        assert_eq!(dict.entry(3), Some("Position 3".to_string()));
    }

    #[test]
    fn index_of() {
        let dict = create_dict();
        assert_eq!(dict.index_of("a".to_string().borrow()), Some(0));
        assert_eq!(dict.index_of("b".to_string().borrow()), Some(1));
        assert_eq!(dict.index_of("c".to_string().borrow()), Some(2));
        assert_eq!(dict.index_of("Position 3".to_string().borrow()), Some(3));
        assert_eq!(dict.index_of("Position 4".to_string().borrow()), Some(4));
        assert_eq!(dict.index_of("Position 5".to_string().borrow()), Some(5));
        assert_eq!(dict.index_of("d".to_string().borrow()), None);
        assert_eq!(dict.index_of("Pos".to_string().borrow()), None);
        assert_eq!(dict.index_of("Pos"), None);
        assert_eq!(dict.index_of("b"), Some(1));
    }

    #[test]
    fn add() {
        let mut dict = create_dict();
        assert_eq!(dict.add("a".to_string()), EntryStatus::Known(0));
        assert_eq!(dict.add("new value".to_string()), EntryStatus::Fresh(6));
    }
}
