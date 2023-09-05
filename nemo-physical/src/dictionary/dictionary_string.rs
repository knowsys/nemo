pub(crate) const LONG_STRING_THRESHOLD: usize = 1000;

/// String that computes and caches checks relevant for dicitonary selection.
#[derive(Clone, Debug)]
pub struct DictionaryString {
    string: String,
}
impl DictionaryString {
    /// Constructor
    pub fn new(s: &str) -> Self {
        DictionaryString{string: s.to_string()}
    }

    /// Returns true if the string is considered "long". Long strings may be handled differently
    /// in some dictionaries.
    pub fn is_long(&self) -> bool {
        self.string.len() > LONG_STRING_THRESHOLD
    }

}