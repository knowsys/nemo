use std::cell::UnsafeCell;
use regex::Regex;
use once_cell::sync::Lazy;

pub(crate) const LONG_STRING_THRESHOLD: usize = 1000;

/// String that computes and caches checks relevant for dicitonary selection.
#[derive(Debug)]
pub struct DictionaryString<'a> {
    string: String,
    pieces: UnsafeCell<Option<(&'a str,&'a str,&'a str)>>,
}
impl<'a> DictionaryString<'a> {
    /// Constructor
    pub fn new(s: &str) -> Self {
        DictionaryString{string: s.to_string(), pieces: UnsafeCell::new(None)}
    }

    /// Constructor, taking ownership of the given string
    pub fn from_string(s: String) -> Self {
        DictionaryString{string: s, pieces: UnsafeCell::new(None)}
    }

    /// Returns true if the string is considered "long". Long strings may be handled differently
    /// in some dictionaries.
    pub fn is_long(&self) -> bool {
        self.string.len() > LONG_STRING_THRESHOLD
    }

    /// Returns the complete string data.
    pub fn as_str(&self) -> &str {
        self.string.as_str()
    }

    /// Returns the first part of the standard split into pieces
    pub fn prefix(&'a self) -> &str {
        self.set_pieces();
        unsafe {
            (*self.pieces.get()).unwrap().0
        }
    }

    /// Returns the middle part of the standard split into pieces
    pub fn infix(&'a self) -> &str {
        self.set_pieces();
        unsafe {
            (*self.pieces.get()).unwrap().1
        }
    }

    /// Returns the last part of the standard split into pieces
    pub fn postfix(&'a self) -> &str {
        self.set_pieces();
        unsafe {
            (*self.pieces.get()).unwrap().2
        }
    }

    /// Computes the pieces from the string.
    fn set_pieces(&'a self) {
        unsafe{
            if (*self.pieces.get()).is_none() {
                *self.pieces.get() = Some(Self::compute_pieces(&self.string));
            }
        }
    }

    /// Computes the pieces from the string.
    fn compute_pieces(string: &String) -> (&str,&str,&str) {
        static RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^([<].*?/)([^>/]*)([>])$").unwrap());
        let Some(caps) = RE.captures(string) else {
            return ("",string.as_str(),"");
        };
        (caps.get(1).unwrap().as_str(),caps.get(2).unwrap().as_str(),caps.get(3).unwrap().as_str())
    }

}

#[cfg(test)]
mod test {
    use super::DictionaryString;

    #[test]
    fn split_parts() {
        let ds = DictionaryString::new("<https://example.org/test/local>");
        assert_eq!(ds.prefix(),"<https://example.org/test/");
        assert_eq!(ds.infix(),"local");
        assert_eq!(ds.postfix(),">");
    }
}