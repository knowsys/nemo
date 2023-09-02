use super::{Dictionary,EntryStatus};

use std::{
    collections::HashMap,
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

const LOW_24_BITS_MASK: u64 = 0b00000000_00000000_00000000_00000000_00000000_11111111_11111111_11111111;
/// Address size of pages in the string buffer
const PAGE_ADDR_BITS: usize = 25; // 32MB
/// Size of pages in the string buffer
const PAGE_SIZE: usize = 1 << PAGE_ADDR_BITS;

/// A buffer for string data using compact memory regions that are managed in pages.
/// New buffers need to be initialized, upn which they will receive an identifying buffer id
/// that is used whenever the data accessed.
/// 
/// The implementaion is not fully thread-safe, but it is thread-safe as long as each buffer
/// is used in only one thread. That is, parallel threads can safely create buffers (which will
/// have different ids), as long as all their operations use the buffer id that they were given.
/// 
/// FIXME: The current use of temporary strings is not thread-safe, since it neither uses a lock
/// nor a buffer id. This leaves a small race condition. The preferred solution would be to use
/// a buffer id, which temporary [StringRef]s would then need to encode in their data (invariably
/// reducing their address space a bit). One could also make the whole buffer thread-local to prevent
/// these particular issues altogether, but this might prevent some desired multi-threading of
/// operations that need the dictoinary.
struct StringBuffer {
    /// Vector of buffer ids and string buffers
    pages: Vec<(usize,String)>,
    /// Single temporary string. [StringRef] uses this for representing strings that are not in the buffer.
    tmp: String,
    /// Currently active page for each buffer
    cur_page: Vec<usize>,
    /// Lock to guard page assignment operations when using multiple threads
    lock: AtomicBool,
}
impl StringBuffer {
    /// Constructor.
    const fn new() -> Self {
        StringBuffer{ pages: Vec::new(), tmp: String::new(), cur_page: Vec::new(), lock: AtomicBool::new(false)}
    }

    // fn debug_buffers(&self) {
    //     let mut i = 0;
    //     println!("BUFFER CONTENTS:");
    //     for (b,s) in &self.pages {
    //         println!(" #{}, buffer {}, contents: {}", i, b, s);
    //         i += 1;
    //     }
    // }

    /// Initializes a new buffer and returns a handle that can henceforth be used to access it.
    fn init_buffer(&mut self) -> usize {
        self.acquire_page_lock();
        let buf_id = self.cur_page.len();
        self.pages.push( (buf_id, String::with_capacity(PAGE_SIZE)) );
        self.cur_page.push(self.pages.len()-1);
        self.release_page_lock();
        buf_id
    }

    /// Frees the memory used by the pages of the dropped buffer.
    /// No other pages are affected or moved.
    ///
    /// TODO: Allocation of new pages should re-use freed pages instead of always appending.
    fn drop_buffer(&mut self, buffer: usize) {
        self.acquire_page_lock();
        for (b,s) in self.pages.iter_mut() {
            if buffer == *b {
                s.clear();
                s.shrink_to_fit();
                *b = usize::MAX;
            }
        }
        self.release_page_lock();
    }

    /// Inserts a string into the buffer and returns a [StringRef] that points to it.
    fn push_str(&mut self, buffer: usize, s: &str) -> StringRef {
        let len = s.len();
        assert!(len < 1<<25);
        let mut page_num = self.cur_page[buffer];
        if self.pages[page_num].1.len() + len > PAGE_SIZE {
            self.acquire_page_lock();
            self.pages.push((buffer,String::with_capacity(PAGE_SIZE)));
            page_num = self.pages.len()-1;
            self.cur_page[buffer] = page_num;
            self.release_page_lock();
        }
        let page_addr = self.pages[page_num].1.len();
        self.pages[page_num].1.push_str(s);

        StringRef::new(page_num*PAGE_SIZE + page_addr, s.len())
    }

    /// Returns a direct string slice reference for this data.
    /// This is a pointer to global mutable data, and cannot be used safely.
    fn get_str(&self, address: usize, length: usize) -> &str {
        let page_num = address >> PAGE_ADDR_BITS;
        let page_addr = address % PAGE_SIZE;
        &self.pages[page_num].1[page_addr..page_addr+length]
    }

    /// Creates a temporary mock object that stores its contents outside of 
    /// the global buffer.
    fn get_tmp_string_ref(&mut self, s: &str) -> StringRef {
        self.tmp.clear();
        self.tmp.push_str(s);
        StringRef{reference: u64::MAX}
    }

    /// Returns the current contents of the temporary string.
    fn get_tmp_string(&self) -> &str {
        self.tmp.as_str()
    }

    /// Acquire the lock that we use for operations that add new pages or change
    /// the assignment of pages to buffers in any way.
    fn acquire_page_lock(&mut self) {
        while self.lock.compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Acquire).is_err() { }
    }

    /// Release the lock.
    fn release_page_lock(&mut self) {
        self.lock.store(false, Ordering::Release);
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
        assert!(len < 1<<24);
        assert!(address < 1<<40);
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
#[derive(Clone, Debug)]
pub struct HashMapDictionary {
    store: Vec<StringRef>,
    mapping: HashMap<StringRef, usize>,
    buffer: usize,
}

impl Default for HashMapDictionary {
    fn default() -> Self {
        unsafe {
            HashMapDictionary{store: Vec::new(), mapping: HashMap::new(), buffer: BUFFER.init_buffer()}
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
    fn new() -> Self {
        Self::default()
    }

    fn add(&mut self, entry: String) -> EntryStatus {
        match self.mapping.get(&StringRef::tmp(entry.as_str())) {
            Some(idx) => EntryStatus::Known(*idx),
            None => {
                unsafe {
                    let sref = BUFFER.push_str(self.buffer, entry.as_str());
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
        let mut dict = create_dict();

        let mut dict2 = HashMapDictionary::default();
        dict2.add("entry0".to_string());
        dict.add("another entry".to_string());
        dict2.add("entry1".to_string());
        
        assert_eq!(dict.entry(0), Some("a".to_string()));
        assert_eq!(dict.entry(1), Some("b".to_string()));
        assert_eq!(dict.entry(2), Some("c".to_string()));
        assert_eq!(dict.entry(3), Some("Position 3".to_string()));
        assert_eq!(dict.entry(4), Some("Position 4".to_string()));
        assert_eq!(dict.entry(5), Some("Position 5".to_string()));
        assert_eq!(dict.entry(6), Some("another entry".to_string()));
        assert_eq!(dict.entry(7), None);
        assert_eq!(dict.entry(3), Some("Position 3".to_string()));

        assert_eq!(dict2.entry(0), Some("entry0".to_string()));
        assert_eq!(dict2.entry(1), Some("entry1".to_string()));
        assert_eq!(dict2.entry(2), None);
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
