//! Dictionary description (Trait for general Dictionaries)

//pub mod prefixed_string_dictionary;
pub mod string_dictionary;

/// This Dictionary Trait defines dictionaries, which keep ownership of the inserted elements.
///
trait Dictionary: std::ops::Index<usize> {
    fn add(&mut self, entry: String) -> usize;
    fn index_of(&self, entry: &str) -> Option<usize>;
    fn entry(&self, index: usize) -> Option<&str>;
}
