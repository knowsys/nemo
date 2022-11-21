//! This module provides different dictionary functionalities
//! In general these dictionary functionalities allow to represent [String] values as [usize] values

/// Module to define a [PrefixedStringDictionary]
/// This will provide a more memory-efficient storage of [String] values if they share equivalent prefixes (such as IRIs)
/// The prefixes of the [String] will be stored as a Triestructure.
pub mod prefixed_string_dictionary;
pub use prefixed_string_dictionary::PrefixedStringDictionary;
/// Module to define a simple [StringDictionary]
pub mod string_dictionary;
pub use string_dictionary::StringDictionary;

/// This Dictionary Trait defines dictionaries, which keep ownership of the inserted elements.
pub trait Dictionary {
    /// Construct a new and empty [`Dictionary`]
    fn new() -> Self
    where
        Self: Sized + Default,
    {
        Self::default()
    }
    /// Add a new string to the dictionary
    /// and returns the associated [usize] value to the added string
    /// Note that duplicates will not be added and the existing [usize] will be returned
    fn add(&mut self, entry: String) -> usize;
    /// Looks for a given [&str] slice and returns `Some(position)` if there is a match or `None` if there is no match.
    fn index_of(&self, entry: &str) -> Option<usize>;
    /// Returns an equivalent [String] to the one associated with the `index` or None if the `index` is out of bounds
    fn entry(&self, index: usize) -> Option<String>;
}
