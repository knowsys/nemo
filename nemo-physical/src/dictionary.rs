//! This module provides functionalities for creating and maintaining dictionaries.
//! A dictionary is a data structure that assigns numeric ids to complex objects (such as [String]s),
//! and that provides an bijective (invertible) mapping between the two.

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

/// Result of adding new values to a dictionary.
/// It indicates if the operation was successful, and whether the value was previously present or not.
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum AddResult {
    /// Element was new and has been freshly assinged the given id.
    Fresh(usize),
    /// Element was already known and has the given id.
    Known(usize),
    /// Element not supported by dictionary.
    Rejected,
}
impl AddResult {
    /// Returns the actual index.
    /// In case of [AddResult::Rejected], `usize::MAX` is returned.
    pub fn value(&self) -> usize {
        match self {
            AddResult::Fresh(value) => *value,
            AddResult::Known(value) => *value,
            AddResult::Rejected => usize::MAX,
        }
    }
}


/// A Dictionary represents a bijective (invertible) mapping from objects to numeric ids.
/// The "objects" are provided when the dictionary is used, whereas the ids are newly
/// assigned by the dictionary itself.
pub trait Dictionary: Debug {
    /// Construct a new and empty [`Dictionary`]
    fn new() -> Self
    where
        Self: Sized + Default;

    /// Adds a new string to the dictionary. If the string is not known yet, it will
    /// be assigned a new id. Unsupported strings can also be rejected, which specialized
    /// dictionary implementations might do.
    /// 
    /// The result is an [EntryStatus] that indicates if the string was newly added,
    /// previoulsy present, or rejected. In the first two cases, the result yields
    /// the strings id.
    fn add_string(&mut self, string: String) -> AddResult;

    /// Adds a new string to the dictionary. If the string is not known yet, it will
    /// be assigned a new id. Unsupported strings can also be rejected, which specialized
    /// dictionary implementations might do.
    /// 
    /// The result is an [EntryStatus] that indicates if the string was newly added,
    /// previoulsy present, or rejected. In the first two cases, the result yields
    /// the strings id.
    fn add_str(&mut self, string: &str) -> AddResult;

    /// Adds a new string to the dictionary. This method is similar to `add_string()` but uses a
    /// pre-processed string. Some dictionary implementations may extract only parts of
    /// the string to fit internal assumptions (e.g., a dictionary that requires a fixed
    /// prefix may ignore the prefix and only store the rest, as if the prefix would
    /// match). To perform checks and possibly reject data, `add_string()` or `add_str()` should be used.
    fn add_dictionary_string(&mut self, ds: DictionaryString) -> AddResult;

    /// Looks for a given [&str] slice and returns `Some(id)` if it is in the dictionary, and `None` otherwise.
    fn fetch_id(&self, string: &str) -> Option<usize>;

    /// Returns the [String] to the one associated with the `id` or None if the `id` is out of bounds
    fn get(&self, id: usize) -> Option<String>;

    /// Returns the number of elements in the dictionary.
    fn len(&self) -> usize;
}
