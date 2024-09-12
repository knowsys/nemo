//! This module defines [DictionaryString].

use std::cell::UnsafeCell;

pub(crate) const LONG_STRING_THRESHOLD: usize = 1000;

/// Internal struct where keep locations extracted from strings. This is separated
/// to enable an iner mutability pattern.
#[derive(Debug, Clone, Copy)]
struct DictionaryStringLocations {
    prefix_length: usize,
    infix_length: usize,
    infix_done: bool,
}

impl DictionaryStringLocations {
    fn new() -> Self {
        DictionaryStringLocations {
            prefix_length: 0,
            infix_length: 0,
            infix_done: false,
        }
    }
}

/// String that computes and caches checks relevant for dictionary selection.
#[derive(Debug)]
pub struct DictionaryString {
    string: String,
    positions: UnsafeCell<DictionaryStringLocations>,
}
impl DictionaryString {
    /// Constructor
    pub(crate) fn new(s: &str) -> Self {
        DictionaryString {
            string: s.to_string(),
            positions: UnsafeCell::new(DictionaryStringLocations::new()),
        }
    }

    /// Constructor, taking ownership of the given string
    pub(crate) fn from_string(s: String) -> Self {
        DictionaryString {
            string: s,
            positions: UnsafeCell::new(DictionaryStringLocations::new()),
        }
    }

    /// Returns true if the string is considered "long". Long strings may be handled differently
    /// in some dictionaries.
    pub(crate) fn is_long(&self) -> bool {
        self.string.len() > LONG_STRING_THRESHOLD
    }

    /// Returns the complete string data.
    pub(crate) fn as_str(&self) -> &str {
        self.string.as_str()
    }

    /// Returns the first part of the standard split into pieces
    pub(crate) fn prefix(&self) -> &str {
        self.set_pieces();
        unsafe {
            let prefix_length = (*self.positions.get()).prefix_length;
            self.string.as_str().get_unchecked(..prefix_length)
        }
    }

    /// Returns the middle part of the standard split into pieces
    pub(crate) fn infix(&self) -> &str {
        self.set_pieces();
        unsafe {
            let prefix_end = (*self.positions.get()).prefix_length;
            let infix_end = prefix_end + (*self.positions.get()).infix_length;
            self.string.as_str().get_unchecked(prefix_end..infix_end)
        }
    }

    /// Returns the last part of the standard split into pieces
    pub(crate) fn suffix(&self) -> &str {
        self.set_pieces();
        unsafe {
            let prefix_end = (*self.positions.get()).prefix_length;
            let infix_end = prefix_end + (*self.positions.get()).infix_length;
            self.string.as_str().get_unchecked(infix_end..)
        }
    }

    /// Checks if the string can be viewed as an infix that is enclosed by the given prefix and suffix.
    /// Note that this uses the standard splitting approach to determine prefix and suffix, which means
    /// that the funciton may return `false` even if the string actually starts and ends with the prefix
    /// and suffix given (if these are not the ones detected).
    pub(crate) fn has_infix(&self, prefix: &str, suffix: &str) -> bool {
        self.prefix() == prefix && self.suffix() == suffix
    }

    /// Checks if the string has a non-empty prefix or suffix.
    pub(crate) fn infixable(&self) -> bool {
        self.set_pieces();
        unsafe { (*self.positions.get()).infix_length < self.string.len() }
    }

    /// Computes the pieces from the string.
    fn set_pieces(&self) {
        if (unsafe { *self.positions.get() }).infix_done {
            return;
        }

        let bytes = self.string.as_bytes();

        let mut prefix_length: usize = 0;
        let mut infix_length: usize = bytes.len();

        if infix_length > 0 && bytes[infix_length - 1] == b'>' {
            if bytes[0] == b'<' {
                // using bytes is safe at pos 0; we know string!="" from above
                let pos = DictionaryString::rfind_hashslash_plus(bytes);
                if pos > 0 {
                    prefix_length = pos; // note that pos is +1 the actual pos
                    infix_length = bytes.len() - prefix_length - 1;
                }
            } else if bytes[0] == b'"' {
                // using bytes is safe at pos 0; we know string!="" from above
                let pos = DictionaryString::find_quote_plus(unsafe { bytes.get_unchecked(1..) });
                if pos > 0 {
                    prefix_length = 1;
                    infix_length = pos - 1; // note that pos is relative to the slice that starts at 1, and that it is +1 the actual position
                }
            }
        } // else: use defaults from above

        let positions = unsafe { &mut *self.positions.get() };
        positions.prefix_length = prefix_length;
        positions.infix_length = infix_length;
        positions.infix_done = true;
    }

    /// Finds the last position in UTF-8 str slice (given as `&[u8]` bytes) where the characters '/' or '#' occur,
    /// and returns the successor of that position. If the character is not found, 0 is returned.
    /// The method avoids any UTF decoding, because it is unnecessary for characters in the ASCII range (<128).
    #[inline(always)]
    fn rfind_hashslash_plus(s: &[u8]) -> usize {
        let mut pos: usize = s.len();
        let mut iter = s.iter().copied();
        while let Some(ch) = iter.next_back() {
            if ch == b'/' || ch == b'#' {
                return pos;
            }
            pos -= 1;
        }
        pos
    }

    /// Finds the first position in UTF-8 str slice (given as `&[u8]` bytes) where the character '"' occurs,
    /// and returns the successor of that position. If the character is not found, 0 is returned.
    /// The method avoids any UTF decoding, because it is unnecessary for characters in the ASCII range (<128).
    #[inline(always)]
    fn find_quote_plus(s: &[u8]) -> usize {
        let mut pos: usize = 1;
        for ch in s.iter().copied() {
            if ch == b'"' {
                return pos;
            }
            pos += 1;
        }
        0
    }
}

#[cfg(test)]
mod test {
    use super::DictionaryString;

    #[test]
    fn split_parts_qids() {
        let ds = DictionaryString::new("<http://www.wikidata.org/entity/Q233>");
        assert_eq!(ds.prefix(), "<http://www.wikidata.org/entity/");
        assert_eq!(ds.infix(), "Q233");
        assert_eq!(ds.suffix(), ">");
    }

    #[test]
    fn split_parts_rdf_type() {
        let ds = DictionaryString::new("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");
        assert_eq!(ds.prefix(), "<http://www.w3.org/1999/02/22-rdf-syntax-ns#");
        assert_eq!(ds.infix(), "type");
        assert_eq!(ds.suffix(), ">");
    }

    #[test]
    fn split_parts_integer() {
        let ds = DictionaryString::new("\"305\"^^<http://www.w3.org/2001/XMLSchema#integer>");
        assert_eq!(ds.prefix(), "\"");
        assert_eq!(ds.infix(), "305");
        assert_eq!(
            ds.suffix(),
            "\"^^<http://www.w3.org/2001/XMLSchema#integer>"
        );
    }

    #[test]
    fn findr_hashslash_plus() {
        let s1 = "A täst /stri#ng / with non-ASCII unicöde in it";
        let s2 = "A täst /st#ring # with non-ASCII unicöde in it";
        let s3 = "A täst string  with non-ASCII unicöde in it";
        let pos1 = DictionaryString::rfind_hashslash_plus(s1.as_bytes());
        let pos2 = DictionaryString::rfind_hashslash_plus(s2.as_bytes());
        let pos3 = DictionaryString::rfind_hashslash_plus(s3.as_bytes());

        assert_eq!(pos1, s1.rfind(['/', '#']).unwrap() + 1);
        assert_eq!(pos2, s2.rfind(['/', '#']).unwrap() + 1);
        assert_eq!(pos3, 0);
    }

    #[test]
    fn find_quote_plus() {
        let s1 = "A täst /stri\"ng / with non-ASC\"II unicöde in it";
        let pos1 = DictionaryString::find_quote_plus(s1.as_bytes());
        let s2 = "A täst /string / with non-ASCII unicöde in it";
        let pos2 = DictionaryString::find_quote_plus(s2.as_bytes());

        assert_eq!(pos1, s1.find('"').unwrap() + 1);
        assert_eq!(pos2, 0);
    }
}
