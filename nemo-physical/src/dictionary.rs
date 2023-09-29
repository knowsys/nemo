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
/// Module to define [InfixDictionary]
pub mod infix_dictionary;
pub use infix_dictionary::InfixDictionary;
/// Module to define [MetaDictionary]
pub mod meta_dictionary;
pub use meta_dictionary::MetaDictionary;
/// Module mapping physical types into logical types into Strings
pub mod value_serializer;
pub use value_serializer::ValueSerializer;

/// Fake id that dictionaries use to indicate that an entry has no id.
const NONEXISTING_ID_MARK: usize = usize::MAX; 
/// Fake id that dictionaries use to indicate that an entry is known
/// in some other dictionary (indicating that a search across multiple dictionaries
/// should be continued).
const KNOWN_ID_MARK: usize = usize::MAX-1;

/// Result of adding new values to a dictionary.
/// It indicates if the operation was successful, and whether the value was previously present or not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// In case of [AddResult::Rejected], [NONEXISTING_ID_MARK] is returned.
    pub fn value(&self) -> usize {
        match self {
            AddResult::Fresh(value) => *value,
            AddResult::Known(value) => *value,
            AddResult::Rejected => NONEXISTING_ID_MARK,
        }
    }
}

/// A Dictionary represents a bijective (invertible) mapping from objects to numeric ids.
/// The "objects" are provided when the dictionary is used, whereas the ids are newly
/// assigned by the dictionary itself.
pub trait Dictionary: Debug {
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

    /// Looks for a string and returns `Some(id)` if it is in the dictionary, and `None` otherwise.
    /// This method is similar to `fetch_id()` but uses a pre-processed string. Some dictionary implementations
    /// may extract only parts of the string to fit internal assumptions (e.g., a dictionary that requires a fixed
    /// prefix may ignore the prefix and only look up the rest, as if the prefix would
    /// match). To perform checks and possibly reject data, `fetch_id()` should be used.
    fn fetch_id_for_dictionary_string(&self, ds: &DictionaryString) -> Option<usize> {
        self.fetch_id(ds.as_str())
    }

    /// Returns the [String] to the one associated with the `id` or None if the `id` is out of bounds
    fn get(&self, id: usize) -> Option<String>;

    /// Returns the number of elements in the dictionary. For dictionaries that support marking elements as
    /// known without giving IDs to them, such elements should not be counted.
    fn len(&self) -> usize;

    /// Marks the given string as being known using the special id [KNOWN_ID_MARK] without
    /// assigning an own id to it. If the entry exists already, the old id will be kept and
    /// returned. If is possible to return [AddResult::Rejected] to indicate that the dictionary
    /// does not support marking of strings.
    fn mark_str(&mut self, _string: &str) -> AddResult {
        AddResult::Rejected
    }

    /// Returns true if the dictionary contains any marked elements. The intention is that code marks all elements
    /// that are relevant to this dictionary, or none at all, so that a return value of `true` indicates that
    /// one can rely on unknown and non-marked elements to be missing in all dictionaries.
    fn has_marked(&self) -> bool {
        false
    }
}
