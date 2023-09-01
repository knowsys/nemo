use super::Dictionary;
use super::EntryStatus;

use once_cell::sync::Lazy;

use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
};

static mut BUFFER: Lazy<StringBuffer> = Lazy::new(||StringBuffer::new());

const LOW_24_BITS_MASK: u64 = 0b00000000_00000000_00000000_00000000_00000000_11111111_11111111_11111111;
const PAGE_ADDR_BITS: usize = 25; // 32MB
const PAGE_SIZE: usize = 1 << PAGE_ADDR_BITS;

/// A buffer for string data using compact memory regions that are managed in pages.
struct StringBuffer {
    pages: Vec<String>,
    tmp: String,
}
impl StringBuffer {
    /// Constructor.
    fn new() -> Self {
        // We initialise a first page for data. If this ever changed (e.g., wen adding support for
        // organizing pages by caller, where we cannot have a page for all possible cases upfront),
        // then the constructor might become const and Lazy/once_cell above can be dropped.
        StringBuffer{ pages: vec!(String::with_capacity(PAGE_SIZE)), tmp: String::new()}
    }

    /// Inserts a string into the buffer and returns a [StringRef] that points to it.
    fn push_str(&mut self,s: &str) -> StringRef {
        let len = s.len();
        assert!(len < 1<<25);
        let mut page_num = self.pages.len()-1;
        if self.pages[page_num].len() + len > PAGE_SIZE {
            self.pages.push(String::with_capacity(PAGE_SIZE));
            page_num += 1;
        }
        let page_addr = self.pages[page_num].len();
        self.pages[page_num].push_str(s);

        StringRef::new(page_num*PAGE_SIZE + page_addr, s.len())
    }

    /// Returns a direct string slice reference for this data.
    /// This is a pointer to global mutable data, and cannot be used safely.
    fn get_str(&self, address: usize, length: usize) -> &str {
        let page_num = address >> PAGE_ADDR_BITS;
        let page_addr = address % PAGE_SIZE;
        &self.pages[page_num][page_addr..page_addr+length]
    }

    /// Creates a temporary mock object that stores its contents outside of 
    /// the global buffer.
    fn get_tmp_string_ref(&mut self, s: &str) -> StringRef {
        self.tmp.clear();
        self.tmp.push_str(s);
        StringRef{reference: std::u64::MAX}
    }

    /// Returns the current contents of the temporary string.
    fn get_tmp_string(&self) -> &str {
        self.tmp.as_str()
    }
}

/// Memory-optimized reference to a string in the dictionary.
#[derive(Clone, Copy, Debug, Default)]
struct StringRef {
    /// The 64bits reference consists of 40bits that encode a starting address within
    /// the buffer, and 24bits that encode the string length.
    /// This limits the maximal buffer size to 1TB of string data, and the maximal length
    /// of a single string to 16M bytes.
    reference: u64,
}
impl StringRef {
    /// Creates a temporary mock object that stores its contents outside of 
    /// the global buffer.
    fn tmp(s: &str) -> Self {
        unsafe {
            BUFFER.get_tmp_string_ref(s)
        }
    }

    /// Creates a reference to the specific string slice in the buffer.
    /// It is not checked if that slice is allocated.
    fn new(address: usize, len: usize) -> Self {
        assert!(len < 1<<25);
        assert!(address < 1<<41);
        let u64add: u64 = address.try_into().unwrap();
        let u64len: u64 = len.try_into().unwrap();
        StringRef{reference: (u64add << 24) + u64len}
    }

    /// Returns the start address for the string that this refers to.
    /// For temporary references that do not point to the buffer, the result is meaningless.
    fn address(&self) -> usize {
        (self.reference >> 24).try_into().unwrap()
    }

    /// Returns the length of the string that this refers to.
    /// For temporary references that do not point to the buffer, the result is meaningless.
    fn len(&self) -> usize {
        (self.reference & LOW_24_BITS_MASK).try_into().unwrap()
    }

    /// Returns a direct string slice reference for this data.
    /// This is a pointer to global mutable data, and cannot be used safely.
    fn as_str(&self) -> &str {
        if self.reference != std::u64::MAX {
            unsafe {
                BUFFER.get_str(self.address(), self.len())
            }
        } else {
            unsafe {
                BUFFER.get_tmp_string()
            } 
        }
    }

    /// Returns a String that contains a copy of the data that this reference points to.
    fn to_string(&self) -> String {
        String::from(self.as_str())
    }
}

impl Hash for StringRef {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.as_str().hash(state)
    }
}
impl PartialEq for StringRef {
    fn eq(&self, other: &StringRef) -> bool {
        self.as_str().eq(other.as_str())
    }
}
impl Eq for StringRef {}



/// A read-only, hashmap-based [Dictionary] to implement a bijection between strings and integers.  
/// Strings are stored in a compact buffer to reduce memory overhead and fragmentation.
#[derive(Clone, Debug, Default)]
pub struct HashMapDictionary {
    store: Vec<StringRef>,
    mapping: HashMap<StringRef, usize>,
}

impl Dictionary for HashMapDictionary {
    fn new() -> Self {
        Default::default()
    }

    fn add(&mut self, entry: String) -> EntryStatus {
        match self.mapping.get(&StringRef::tmp(entry.as_str())) {
            Some(idx) => EntryStatus::Known(*idx),
            None => {
                unsafe {
                    let sref = BUFFER.push_str(entry.as_str());
                    let nxt_id = self.store.len();
                    self.store.push(sref);
                    self.mapping.insert(sref, nxt_id);
                    EntryStatus::Fresh(nxt_id)
                }
            }
        }
    }

    fn index_of(&self, entry: &str) -> Option<usize> {
        self.mapping.get(&StringRef::tmp(entry)).copied()
    }

    fn entry(&self, index: usize) -> Option<String> {
        self.store
            .get(index)
            .map(|entry| -> String { entry.to_string() })
    }

    fn len(&self) -> usize {
        self.mapping.len()
    }

    fn is_empty(&self) -> bool {
        self.mapping.is_empty()
    }
}


#[cfg(test)]
mod test {
    use std::borrow::Borrow;

    use crate::dictionary::Dictionary;
    use crate::dictionary::EntryStatus;

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
