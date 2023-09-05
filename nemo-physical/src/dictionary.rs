//! This module provides different dictionary functionalities
//! In general these dictionary functionalities allow to represent [String] values as [usize] values

use std::fmt::Debug;

/// Module to define the [DictionaryString]
pub mod dictionary_string;
pub use dictionary_string::DictionaryString;
/// Module to define the [PrefixedStringDictionary]
pub mod prefixed_string_dictionary;
pub use prefixed_string_dictionary::PrefixedStringDictionary;
/// Module to define a simple [StringDictionary]
pub mod string_dictionary;
pub use string_dictionary::StringDictionary;
/// Module to define [HashMapDictionary]
pub mod hash_map_dictionary;
pub use hash_map_dictionary::HashMapDictionary;
/// Module to define [MetaDictionary]
pub mod meta_dictionary;
pub use meta_dictionary::MetaDictionary;
/// Module mapping physical types into logical types into Strings
pub mod value_serializer;
pub use value_serializer::ValueSerializer;

/// Status of a dictionary entry when adding new values.
/// It indicates if the value was previously present or not.
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum EntryStatus {
    /// Entry was new and has been freshly assinged the given index.
    Fresh(usize),
    /// Entry was already known and has the given index.
    Known(usize),
}
impl EntryStatus {
    /// Returns the actual index.
    pub fn value(&self) -> usize {
        match self {
            EntryStatus::Fresh(value) => *value,
            EntryStatus::Known(value) => *value
        }
    }
}


/// This Dictionary Trait defines dictionaries, which keep ownership of the inserted elements.
pub trait Dictionary: Debug {
    /// Construct a new and empty [`Dictionary`]
    fn new() -> Self
    where
        Self: Sized + Default;
    /// Add a new string to the dictionary
    /// and returns the associated [usize] value to the added string
    /// Note that duplicates will not be added and the existing [usize] will be returned
    fn add(&mut self, entry: String) -> EntryStatus;
    /// Looks for a given [&str] slice and returns `Some(position)` if there is a match or `None` if there is no match.
    fn index_of(&self, entry: &str) -> Option<usize>;
    /// Returns an equivalent [String] to the one associated with the `index` or None if the `index` is out of bounds
    fn entry(&self, index: usize) -> Option<String>;
    /// Returns the number of elements in the dictionary.
    fn len(&self) -> usize;
    /// Returns whether the dictionary is empty.
    fn is_empty(&self) -> bool;
}
