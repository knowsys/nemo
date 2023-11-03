use super::{AddResult, Dictionary, DictionaryString};

use std::{
    collections::HashMap,
    fmt::Display,
    hash::{Hash, Hasher},
};

use std::sync::atomic::{AtomicBool, Ordering};

/// Global string buffer for dictionary data.
/// This is global here to allow keys in the hashmap to access it for computing equality and hashes,
/// without the need to re-implement the whole hashmap to inject such an object.
static mut BUFFER: StringBuffer = StringBuffer::new();
// The following code is needed if allocations are done while constructing [StringBuffer]:
//use once_cell::sync::Lazy;
//static mut BUFFER: Lazy<StringBuffer> = Lazy::new(||StringBuffer::new());

/// Address size of pages in the string buffer
const PAGE_ADDR_BITS: usize = 25; // 32MB
/// Size of pages in the string buffer
const PAGE_SIZE: usize = 1 << PAGE_ADDR_BITS;
/// Bit mask that keeps only the (lower) PAGE_ADDR_BITS-1 bits, for extracting a string's length
const LENGTH_BITS_MASK: u64 = (1 << (PAGE_ADDR_BITS - 1)) - 1;

/// A buffer for string data using compact memory regions that are managed in pages.
/// New buffers need to be initialized, upn which they will receive an identifying buffer id
/// that is used whenever the data accessed.
///
/// The implementaion is not fully thread-safe, but it is thread-safe as long as each buffer
/// is used in only one thread. That is, parallel threads can safely create buffers (which will
/// have different ids), as long as all their operations use the buffer id that they were given.
struct StringBuffer {
    /// Vector of buffer ids and string buffers
    pages: Vec<(usize, String)>,
    /// Single temporary string per buffer. [StringRef] uses this for representing strings that are not in the buffer.
    ///
    /// TODO: It would be possible and more elegant to have a special alternative key implementation for our hashmap,
    /// where the key has the relevant data instead of pointing to a temporary buffer.
    tmp_strings: Vec<String>,
    /// Currently active page for each buffer
    cur_pages: Vec<usize>,
    /// Lock to guard page assignment operations when using multiple threads
    lock: AtomicBool,
}

impl StringBuffer {
    /// Constructor.
    const fn new() -> Self {
        StringBuffer {
            pages: Vec::new(),
            tmp_strings: Vec::new(),
            cur_pages: Vec::new(),
            lock: AtomicBool::new(false),
        }
    }

    /// Initializes a new buffer and returns a handle that can henceforth be used to access it.
    fn init_buffer(&mut self) -> usize {
        self.acquire_page_lock();
        let buf_id = self.cur_pages.len();
        self.pages.push((buf_id, String::with_capacity(PAGE_SIZE)));
        self.cur_pages.push(self.pages.len() - 1);
        self.tmp_strings.push(String::new());
        self.release_page_lock();
        buf_id
    }

    /// Frees the memory used by the pages of the dropped buffer.
    /// No other pages are affected or moved.
    fn drop_buffer(&mut self, buffer: usize) {
        self.acquire_page_lock();
        for (b, s) in self.pages.iter_mut() {
            if buffer == *b {
                s.clear();
                s.shrink_to_fit();
                *b = usize::MAX;
            }
        }
        self.release_page_lock();
    }

    /// Inserts a string into the buffer and returns a [StringRef] that points to it.
    ///
    /// TODO: Allocation of new pages could re-use freed pages instead of always appending.
    fn push_str(&mut self, buffer: usize, s: &str) -> StringRef {
        let len = s.len();
        assert!(len < PAGE_SIZE);
        let mut page_num = self.cur_pages[buffer];
        if self.pages[page_num].1.len() + len > PAGE_SIZE {
            self.acquire_page_lock();
            self.pages.push((buffer, String::with_capacity(PAGE_SIZE)));
            page_num = self.pages.len() - 1;
            self.cur_pages[buffer] = page_num;
            self.release_page_lock();
        }
        let page_addr = self.pages[page_num].1.len();
        self.pages[page_num].1.push_str(s);

        StringRef::new(page_num * PAGE_SIZE + page_addr, s.len())
    }

    /// Returns a direct string slice reference for this data.
    /// This is a pointer to global mutable data, and cannot be used safely.
    fn get_str(&self, address: usize, length: usize) -> &str {
        let page_num = address >> PAGE_ADDR_BITS;
        let page_addr = address % PAGE_SIZE;
        unsafe {
            self.pages[page_num]
                .1
                .get_unchecked(page_addr..page_addr + length)
        }
    }

    /// Creates a reference to the given string without adding the string to the buffer.
    fn get_tmp_string_ref(&mut self, buffer: usize, s: &str) -> StringRef {
        self.tmp_strings[buffer].clear();
        self.tmp_strings[buffer].push_str(s);
        StringRef::new_tmp(buffer)
    }

    /// Returns the current contents of the temporary string.
    fn get_tmp_string(&self, buffer: usize) -> &str {
        self.tmp_strings[buffer].as_str()
    }

    /// Acquire the lock that we use for operations that add new pages or change
    /// the assignment of pages to buffers in any way.
    fn acquire_page_lock(&mut self) {
        while self
            .lock
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Acquire)
            .is_err()
        {}
    }

    /// Release the lock.
    fn release_page_lock(&mut self) {
        self.lock.store(false, Ordering::Release);
    }
}

const STRINGREF_STRING_LEGHT_BITS: usize = 24;
const STRINGREF_STARTING_ADDRESS_BITS: usize = 40;
const MAX_STRINGREF_STRING_LEGHT: usize = 1 << STRINGREF_STRING_LEGHT_BITS;
const MAX_STRINGREF_STARTING_ADDRESS: usize = 1 << STRINGREF_STARTING_ADDRESS_BITS;

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
    /// Creates an object that refers to the current contents of the
    /// buffer's temporary String.
    fn new_tmp(buffer: usize) -> Self {
        assert!(buffer < MAX_STRINGREF_STRING_LEGHT);
        let u64buffer: u64 = buffer.try_into().unwrap();
        StringRef {
            reference: (u64::MAX << STRINGREF_STRING_LEGHT_BITS) + u64buffer,
        }
    }

    /// Creates a reference to the specific string slice in the buffer.
    /// It is not checked if that slice is allocated.
    fn new(address: usize, len: usize) -> Self {
        assert!(len < MAX_STRINGREF_STRING_LEGHT);
        assert!(address < MAX_STRINGREF_STARTING_ADDRESS);
        let u64add: u64 = address.try_into().unwrap();
        let u64len: u64 = len.try_into().unwrap();
        StringRef {
            reference: (u64add << STRINGREF_STRING_LEGHT_BITS) + u64len,
        }
    }

    /// Returns the stored start address for the string that this refers to.
    /// For temporary references that do not point to the buffer, the result is meaningless.
    fn address(&self) -> usize {
        (self.reference >> STRINGREF_STRING_LEGHT_BITS)
            .try_into()
            .unwrap()
    }

    /// Returns the stored length of the string that this refers to.
    /// For temporary references that do not point to the buffer, the result is meaningless.
    fn len(&self) -> usize {
        (self.reference & LENGTH_BITS_MASK).try_into().unwrap()
    }

    /// Returns a direct string slice reference for this data.
    /// This is a pointer to global mutable data, and cannot be used safely.
    fn as_str(&self) -> &str {
        if ((!self.reference) >> STRINGREF_STRING_LEGHT_BITS) != 0 {
            unsafe { BUFFER.get_str(self.address(), self.len()) }
        } else {
            unsafe { BUFFER.get_tmp_string(self.len()) }
        }
    }
}

impl Display for StringRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write! {f, "{}", self.as_str()}
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
#[derive(Clone, Debug)]
pub struct HashMapDictionary {
    store: Vec<StringRef>,
    mapping: HashMap<StringRef, usize>,
    buffer: usize,
    has_known_mark: bool,
}

impl HashMapDictionary {
    /// Construct a new and empty dictionary.
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if a string is already in the dictionary, and if not,
    /// set its id to the given value. IDs are normally assigned consecutively
    /// by this dictionary, but one can also assign [super::KNOWN_ID_MARK] to
    /// merely mark a string as known. Other non-consecutive assignments
    /// will generally lead to errors, since the same ID might be assigned
    /// later on again.
    ///
    /// If the given string is known but not assigned an ID (indicated by
    /// [super::KNOWN_ID_MARK]), then the operation will still not assign an
    /// ID either. In such a case, another dictionary should have that ID.
    #[inline(always)]
    fn add_str_with_id(&mut self, string: &str, id: usize) -> AddResult {
        match self
            .mapping
            .get(unsafe { &BUFFER.get_tmp_string_ref(self.buffer, string) })
        {
            Some(idx) => {
                // if *idx == super::KNOWN_ID_MARK {
                //     println!("Got KID for {} when attempting to add id {}",string,id);
                // }
                AddResult::Known(*idx)
            }
            None => {
                let sref = unsafe { BUFFER.push_str(self.buffer, string) };
                if id != super::KNOWN_ID_MARK {
                    self.store.push(sref);
                }
                self.mapping.insert(sref, id);
                AddResult::Fresh(id)
            }
        }
    }
}

impl Default for HashMapDictionary {
    fn default() -> Self {
        HashMapDictionary {
            store: Vec::new(),
            mapping: HashMap::new(),
            buffer: unsafe { BUFFER.init_buffer() },
            has_known_mark: false,
        }
    }
}

impl Drop for HashMapDictionary {
    fn drop(&mut self) {
        unsafe {
            BUFFER.drop_buffer(self.buffer);
        }
    }
}

impl Dictionary for HashMapDictionary {
    fn add_string(&mut self, string: String) -> AddResult {
        self.add_str_with_id(string.as_str(), self.store.len())
    }

    fn add_str(&mut self, string: &str) -> AddResult {
        self.add_str_with_id(string, self.store.len())
    }

    fn add_dictionary_string(&mut self, ds: DictionaryString) -> AddResult {
        self.add_str_with_id(ds.as_str(), self.store.len())
    }

    fn fetch_id(&self, string: &str) -> Option<usize> {
        self.mapping
            .get(unsafe { &BUFFER.get_tmp_string_ref(self.buffer, string) })
            .copied()
    }

    fn get(&self, id: usize) -> Option<String> {
        self.store
            .get(id)
            .map(|entry| -> String { entry.to_string() })
    }

    fn len(&self) -> usize {
        self.store.len()
    }

    fn mark_str(&mut self, string: &str) -> AddResult {
        self.has_known_mark = true;
        self.add_str_with_id(string, super::KNOWN_ID_MARK)
    }

    fn has_marked(&self) -> bool {
        self.has_known_mark
    }
}

#[cfg(test)]
mod test {
    use std::borrow::Borrow;

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
