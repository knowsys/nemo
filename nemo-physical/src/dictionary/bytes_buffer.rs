//! This module defines [GlobalBytesBuffer] and related code.

use std::{
    fmt::{Debug, Display},
    hash::{Hash, Hasher},
    marker::PhantomData,
    sync::atomic::{AtomicBool, Ordering},
};

/// Address size of pages in the buffer
const PAGE_ADDR_BITS: usize = 25; // 32MB
/// Size of pages in the buffer
const PAGE_SIZE: usize = 1 << PAGE_ADDR_BITS;

/// A manager for buffers for byte array data, using compact memory regions managed in pages.
/// New buffers need to be initialized, upon which they will receive an identifying buffer id
/// that is used whenever the data is accessed. Arrays of bytes are always added to buffers, possibly
/// requiring new pages to be started. At each time, there is one (latest) active page per buffer.
/// Buffers might be dropped, upon which all of its pages will be freed. There is no other way
/// of removing contents from a buffer.
///
/// Individual pages have a size of at most [PAGE_SIZE`] bytes, so that [`PAGE_ADDR_BITS]
/// are needed to specify a position within a page. References to buffered strings are represented
/// by [BytesRef], which stores a starting address and length of the slice. The `usize` starting
/// address is global (uniform for all buffers), with the lower [PAGE_ADDR_BITS] bits encoding a position within a page,
/// and the remaining higher bits encoding the numeric id of the page (whatever buffer it belongs to).
/// Since this address must fit into usize, 32bit platforms can only support 2 to the power of
/// (32-[PAGE_ADDR_BITS]) pages. Moreover, since [BytesRef] combines the address and the slice length
/// into a single `u64` (on all platforms), even 64bit platforms cannot use all 64bits for byte array addresses.
/// The number of bits reserved for length is [BYTESREF_BYTES_LENGTH_BITS], which should always be less
/// than [PAGE_ADDR_BITS] since longer tuples would not fit any buffer page anyway.
///
/// The implementaion can be used in multiple parallel threads.
///
/// Note: The multi-thrading support is based on aggressive locking of all major operations. It might be
/// possible to reduce the amount of locking by designing more careful data structures. For example, locking
/// could be limited to the rare page-writing operations if Vectors would not move existing entries on (some)
/// writes, which causes races that may lead to reading errors unless all reads are also locked.
pub(crate) struct BytesBuffer {
    /// Vector of all string buffer pages with the id of the buffer they belong to.
    pages: Vec<(usize, Vec<u8>)>,
    /// Currently active page for each buffer. This is always the last page that was allocated for the buffer.
    cur_pages: Vec<usize>,
    /// Lock to guard page assignment operations when using multiple threads
    lock: AtomicBool,
    /// Marker that prevents StringBuffers from being send over thread boundaries
    _marker: PhantomData<*mut str>,
}

impl BytesBuffer {
    /// Constructor.
    pub(crate) const fn new() -> Self {
        BytesBuffer {
            pages: Vec::new(),
            cur_pages: Vec::new(),
            lock: AtomicBool::new(false),
            _marker: PhantomData,
        }
    }

    /// Initializes a new buffer and returns a handle that can henceforth be used to access it.
    fn init_buffer(&mut self) -> usize {
        self.acquire_page_lock();
        let buf_id = self.cur_pages.len();
        self.pages.push((buf_id, Vec::with_capacity(PAGE_SIZE)));
        self.cur_pages.push(self.pages.len() - 1);
        self.release_page_lock();
        buf_id
    }

    /// Frees the memory used by the pages of the dropped buffer.
    /// No other pages are affected or moved.
    fn drop_buffer(&mut self, buffer: usize) {
        self.acquire_page_lock();
        for (b, v) in self.pages.iter_mut() {
            if buffer == *b {
                v.clear();
                v.shrink_to_fit();
                *b = usize::MAX;
            }
        }
        self.release_page_lock();
    }

    /// Inserts a byte array into the buffer and returns its address and length.
    /// This data can be turned into a [BytesRef] by a [GlobalBytesBuffer].
    ///
    /// TODO: Allocation of new pages could re-use freed pages instead of always appending.
    fn push_bytes(&mut self, buffer: usize, bytes: &[u8]) -> (usize, usize) {
        let len = bytes.len();
        assert!(len < PAGE_SIZE);

        self.acquire_page_lock();
        let mut page_num = self.cur_pages[buffer];
        if self.pages[page_num].1.len() + len > PAGE_SIZE {
            self.pages.push((buffer, Vec::with_capacity(PAGE_SIZE)));
            page_num = self.pages.len() - 1;
            self.cur_pages[buffer] = page_num;
        }
        let page_inner_addr = self.pages[page_num].1.len();
        // TODO: Presumably the following is faster than extend_from_slice(), but this needs benchmarking
        let old_len = self.pages[page_num].1.len();
        self.pages[page_num]
            .1
            .resize_with(old_len + bytes.len(), || 0);
        self.pages[page_num].1[old_len..].copy_from_slice(bytes);
        self.release_page_lock();

        (page_num * PAGE_SIZE + page_inner_addr, len)
    }

    /// Returns a reference to a slice of a byte array for this data.
    /// This is a pointer to global mutable data, and cannot be used safely.
    fn get_bytes(&self, address: usize, length: usize) -> &[u8] {
        let page_num = address >> PAGE_ADDR_BITS;
        let page_inner_addr = address % PAGE_SIZE;

        unsafe {
            self.get_page(page_num)
                .get_unchecked(page_inner_addr..page_inner_addr + length)
        }
    }

    /// Acquire the lock that we use for operations that read or write any of the internal data
    /// structures that multiple buffers might use.
    fn acquire_page_lock(&self) {
        while self
            .lock
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Acquire)
            .is_err()
        {}
    }

    /// Release the lock.
    fn release_page_lock(&self) {
        self.lock.store(false, Ordering::Release);
    }

    fn get_page(&self, page_num: usize) -> &Vec<u8> {
        self.acquire_page_lock();
        let result = &self.pages[page_num].1;
        self.release_page_lock();
        result
    }
}

/// Trait to encapsulate (static) functions for accessing a single [BytesBuffer].
/// This can be implemented mutiple times to select the buffer in different contexts
/// based on a generic parameter. It is still global, but this can further reduce the
/// mutual influence across buffers used for different purposes (and in particular the
/// implicit capacity limit of any single buffer).
///
/// # Safety
/// Our standard implementations of this use raw pointers to mutable globals as memory
/// pools. This construction only can work safely if the application as a whole ensures
/// that the mutable globals are not moved while we are accessing them. In our code, it is
/// ensured that the unsafe functions of this trait are only used in private functions of
/// other structs, and refered to global memory is copied before further use.
pub(crate) unsafe trait GlobalBytesBuffer: Debug + Sized {
    /// Returns a specific global buffer.
    unsafe fn get() -> *mut BytesBuffer;

    /// Initializes a new (sub)buffer of this buffer, and returns a handle that can henceforth be used to access it.
    #[must_use = "don't forget to take your buffer id"]
    fn init_buffer() -> usize {
        unsafe { BytesBuffer::init_buffer(&mut *Self::get()) }
    }

    /// Inserts a byte array into the buffer and returns its address and length.
    fn push_bytes(buffer: usize, bytes: &[u8]) -> BytesRef<Self> {
        unsafe {
            let (address, len) = BytesBuffer::push_bytes(&mut *Self::get(), buffer, bytes);
            BytesRef::new(address, len)
        }
    }

    /// Returns a reference to a slice of a byte array for this data.
    /// This is a pointer to global mutable data, and cannot be used safely.
    unsafe fn get_bytes(address: usize, length: usize) -> &'static [u8] {
        BytesBuffer::get_bytes(&*Self::get(), address, length)
    }

    /// Frees the memory used by the pages of the dropped buffer.
    fn drop_buffer(buffer: usize) {
        unsafe {
            BytesBuffer::drop_buffer(&mut *Self::get(), buffer);
        }
    }
}

/// Number of bits reserved for encoding the length of referenced byte arrays.
/// This should be at most [PAGE_ADDR_BITS], the maximal length in any page
/// in the [BytesBuffer], but it could conceivably also be less.
const BYTESREF_BYTES_LENGTH_BITS: u64 = 24;
/// Bit mask that keeps only the (lower) BYTESREF_BYTES_LENGTH_BITS-1 bits, for extracting a string's length
const LENGTH_BITS_MASK: u64 = (1 << BYTESREF_BYTES_LENGTH_BITS) - 1;
/// Number of bits reserved for the starting address of a byte array.
const BYTESREF_STARTING_ADDRESS_BITS: u64 = 64 - BYTESREF_BYTES_LENGTH_BITS;
/// Largest number that can specify the length of a byte array in a [BytesRef].
const MAX_BYTESREF_BYTES_LENGTH: u64 = (1 << BYTESREF_BYTES_LENGTH_BITS) - 1;
/// Largest number that can specify the starting address of a byte array in a [BytesRef].
const MAX_BYTESREF_STARTING_ADDRESS: u64 = (1 << BYTESREF_STARTING_ADDRESS_BITS) - 1;

/// Memory-optimized reference to a byte array in the buffer.
///
/// Internally, a single u64 number is used to combine the starting address of an
/// array in a [BytesBuffer] and its length.
/// See [BytesBuffer] for a discussion of the resulting constraints.
///
/// The generic parameter is used to obtain the global buffer that is to be uesd
/// here.
#[derive(Debug)]
pub(crate) struct BytesRef<B: GlobalBytesBuffer> {
    /// The 64bits reference consists of 40bits that encode a starting address within
    /// the buffer, and 24bits that encode the string length.
    /// This limits the maximal buffer size to 1TB of string data, and the maximal length
    /// of a single string to 16M bytes.
    reference: [u8; 8],
    _phantom: PhantomData<B>,
}

impl<B: GlobalBytesBuffer> BytesRef<B> {
    /// Creates a reference to the specific byte array slice in the buffer.
    /// It is not checked if that slice is allocated.
    fn new(address: usize, len: usize) -> Self {
        assert!(u64::try_from(len).unwrap() <= MAX_BYTESREF_BYTES_LENGTH);
        assert!(u64::try_from(address).unwrap() <= MAX_BYTESREF_STARTING_ADDRESS);
        let u64add: u64 = address.try_into().unwrap();
        let u64len: u64 = len.try_into().unwrap();
        BytesRef {
            reference: ((u64add << BYTESREF_BYTES_LENGTH_BITS) + u64len).to_ne_bytes(),
            _phantom: PhantomData,
        }
    }

    /// Returns the stored start address for the bytes that this refers to.
    /// For temporary references that do not point to the buffer, the result is meaningless.
    fn address(&self) -> usize {
        (self.reference() >> BYTESREF_BYTES_LENGTH_BITS)
            .try_into()
            .unwrap()
    }

    /// Returns the stored length of the bytes that this refers to.
    /// For temporary references that do not point to the buffer, the result is meaningless.
    pub(crate) fn len(&self) -> usize {
        (self.reference() & LENGTH_BITS_MASK).try_into().unwrap()
    }

    /// Returns a reference to a slice of a byte array for this data.
    /// This is a pointer to global mutable data, and cannot be used safely.
    unsafe fn as_bytes(&self) -> &[u8] {
        debug_assert!(((!self.reference()) >> BYTESREF_BYTES_LENGTH_BITS) != 0);
        unsafe { B::get_bytes(self.address(), self.len()) }
    }

    /// Returns a copy of the data that this reference points to.
    pub(crate) fn as_vec(&self) -> Vec<u8> {
        unsafe {
            let bytes = self.as_bytes();
            // TODO: Presumably the following is faster than extend_from_slice(), but this needs benchmarking
            let mut result = vec![0; bytes.len()];
            result.copy_from_slice(bytes);
            result
        }
    }

    fn reference(&self) -> u64 {
        u64::from_ne_bytes(self.reference)
    }
}

impl<B: GlobalBytesBuffer> Display for BytesRef<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe {
            write! {f, "{:?}", self.as_bytes()}
        }
    }
}

impl<B: GlobalBytesBuffer> Hash for BytesRef<B> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        unsafe { self.as_bytes().hash(state) }
    }
}

impl<B: GlobalBytesBuffer> hashbrown::Equivalent<BytesRef<B>> for [u8] {
    fn equivalent(&self, key: &BytesRef<B>) -> bool {
        unsafe { key.as_bytes().eq(self) }
    }
}

impl<B: GlobalBytesBuffer> PartialEq for BytesRef<B> {
    fn eq(&self, other: &BytesRef<B>) -> bool {
        unsafe { self.as_bytes() == (other.as_bytes()) }
    }
}

impl<B: GlobalBytesBuffer> Eq for BytesRef<B> {}

impl<B: GlobalBytesBuffer> Copy for BytesRef<B> {}

impl<B: GlobalBytesBuffer> Clone for BytesRef<B> {
    fn clone(&self) -> Self {
        *self
    }
}

/// This macro declares a new GlobalBytesBuffer of the given name.
/// The buffer name must be unique, but upper case (which we do not
/// attempt to achieve in the macro ...).
/// All declarations are at the place where the macro is invoked and
/// are private to the crate, which is the widest visibility intended
/// for the unsafe code involved.
macro_rules! declare_bytes_buffer {
    ($buf_name:ident, $buf_name_upper:ident) => {
        pub(crate) static mut $buf_name_upper: BytesBuffer = BytesBuffer::new();
        #[derive(Debug)]
        pub(crate) struct $buf_name;
        unsafe impl GlobalBytesBuffer for $buf_name {
            unsafe fn get() -> *mut BytesBuffer {
                std::ptr::addr_of_mut!($buf_name_upper)
            }
        }
    };
}
pub(crate) use declare_bytes_buffer;

#[cfg(test)]
mod test {
    use super::{BytesBuffer, GlobalBytesBuffer};

    crate::dictionary::bytes_buffer::declare_bytes_buffer!(TestGlobalBuffer, TEST_BUFFER);
    crate::dictionary::bytes_buffer::declare_bytes_buffer!(TestGlobalBuffer2, TEST_BUFFER2);

    #[test]
    fn test_buffer() {
        let bufid1 = TestGlobalBuffer::init_buffer();
        let bufid2 = TestGlobalBuffer::init_buffer();
        let bufid3 = TestGlobalBuffer2::init_buffer();

        let data1: [u8; 4] = [1, 2, 3, 4];
        let bytes_ref1 = TestGlobalBuffer::push_bytes(bufid1, &data1);
        let data2: [u8; 2] = [5, 6];
        let bytes_ref2 = TestGlobalBuffer::push_bytes(bufid1, &data2);
        let data3: [u8; 4] = [10, 20, 30, 40];
        let bytes_ref3 = TestGlobalBuffer::push_bytes(bufid2, &data3);
        let data4: [u8; 4] = [8, 9, 10, 11];
        let bytes_ref4 = TestGlobalBuffer2::push_bytes(bufid3, &data4);

        assert_eq!(bytes_ref1.as_vec(), vec!(1, 2, 3, 4));
        assert_eq!(bytes_ref2.as_vec(), vec!(5, 6));
        assert_eq!(bytes_ref3.as_vec(), vec!(10, 20, 30, 40));
        assert_eq!(bytes_ref4.as_vec(), vec!(8, 9, 10, 11));
        // The two global buffers should behave identical on this data of same length, yet have distinct memory:
        assert_eq!(bufid1, bufid3); // < Note that this check would be subject to a race if the TestGlobalBuffers would be used in any other test!
        assert_eq!(bytes_ref1.len(), bytes_ref4.len());
        assert_eq!(bytes_ref1.address(), bytes_ref4.address());

        assert_ne!(bytes_ref1, bytes_ref2);
        assert_eq!(bytes_ref1, bytes_ref1);

        TestGlobalBuffer::drop_buffer(bufid1);
        TestGlobalBuffer::drop_buffer(bufid2);
        TestGlobalBuffer2::drop_buffer(bufid3);
    }
}
