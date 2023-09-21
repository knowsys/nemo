use std::cell::UnsafeCell;

pub(crate) const LONG_STRING_THRESHOLD: usize = 1000;

/// Inner struct where keep locations extracted from strings. This is separated
/// to enable an iner mutability patters.
#[derive(Debug,Clone,Copy)]
struct DictionaryStringLocations {
    prefix_length: usize,
    infix_length: usize,
    infix_done: bool,
}

impl DictionaryStringLocations{
    fn new() -> Self {
        DictionaryStringLocations { prefix_length: 0, infix_length: 0, infix_done: false }
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
    pub fn new(s: &str) -> Self {
        DictionaryString {
            string: s.to_string(),
            positions: UnsafeCell::new(DictionaryStringLocations::new())
        }
    }

    /// Constructor, taking ownership of the given string
    pub fn from_string(s: String) -> Self {
        DictionaryString {
            string: s,
            positions: UnsafeCell::new(DictionaryStringLocations::new())
        }
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
    pub fn prefix(&self) -> &str {
        self.set_pieces();
        unsafe {
            let prefix_length = (*self.positions.get()).prefix_length;
            &self.string[..prefix_length]
        }
    }

    /// Returns the middle part of the standard split into pieces
    pub fn infix(&self) -> &str {
        self.set_pieces();
        unsafe {
            let prefix_end =  (*self.positions.get()).prefix_length;
            let infix_end =  prefix_end + (*self.positions.get()).infix_length;
            &self.string[prefix_end..infix_end]
        }
    }

    /// Returns the last part of the standard split into pieces
    pub fn suffix(&self) -> &str {
        self.set_pieces();
        unsafe {
            let prefix_end =  (*self.positions.get()).prefix_length;
            let infix_end =  prefix_end + (*self.positions.get()).infix_length;
            &self.string[infix_end..]
        }
    }

    /// Checks if the string can be viewed as an infix that is enclosed by the given prefix and suffix.
    pub fn has_infix(&self, prefix: &str, suffix: &str) -> bool {
        self.prefix() == prefix && self.suffix() == suffix 
    }

    /// Checks if the string has a non-empty prefix or suffix.
    pub fn infixable(&self) -> bool {
        self.set_pieces();
        unsafe {
            (*self.positions.get()).infix_length < self.string.len()
        }
    }

    /// Computes the pieces from the string.
    fn set_pieces(&self) {
        unsafe {
            if  (*self.positions.get()).infix_done {
                return
            }
        }

        let mut prefix_length: usize = 0;
        let mut infix_length: usize = self.string.len();

        if self.string.starts_with("<") && self.string.ends_with(">") {
            match self.string.as_str().rfind(|c: char| c=='/' || c=='#') {
                Some(pos) => {
                    prefix_length = pos+1;
                    infix_length = self.string.len()-prefix_length-1;
                },
                None => {},
            }
        } else if self.string.starts_with("\"") && self.string.ends_with(">") {
            match self.string.as_str()[1..].find('"') {
                Some(pos) => {
                    prefix_length = 1;
                    infix_length = pos; // note that pos is relative to the slice that starts at 1
                },
                None => {},
            }
        } // else: use defaults from above

        unsafe{
            (*self.positions.get()).prefix_length = prefix_length;
            (*self.positions.get()).infix_length = infix_length;
            (*self.positions.get()).infix_done = true;
        }
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
        assert_eq!(ds.suffix(), "\"^^<http://www.w3.org/2001/XMLSchema#integer>");
    }
}
