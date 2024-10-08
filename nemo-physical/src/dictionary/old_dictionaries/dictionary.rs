//! This module defines the historic dictionary trait for string dictionaries

use std::fmt::Debug;

use crate::dictionary::AddResult;

use super::DictionaryString;

/// A Dictionary represents a bijective (invertible) mapping from objects to numeric ids.
///
/// The "objects" are provided when the dictionary is used, whereas the ids are newly
/// assigned by the dictionary itself.
pub trait Dictionary: Debug {
    /// Adds a new string to the dictionary. If the string is not known yet, it will
    /// be assigned a new id. Unsupported strings can also be rejected, which specialized
    /// dictionary implementations might do.
    ///
    /// The result is an [AddResult] that indicates if the string was newly added,
    /// previoulsy present, or rejected. In the first two cases, the result yields
    /// the strings id.
    fn add_string(&mut self, string: String) -> AddResult;

    /// Adds a new string to the dictionary. If the string is not known yet, it will
    /// be assigned a new id. Unsupported strings can also be rejected, which specialized
    /// dictionary implementations might do.
    ///
    /// The result is an [AddResult] that indicates if the string was newly added,
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

    /// Returns true if the dictionary is empty. False otherwise
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Marks the given string as being known ---using the special id [u64::MAX] - 1--- without
    /// assigning an own id to it. If the entry exists already, the old id will be kept and
    /// returned. It is possible to return [AddResult::Rejected] to indicate that the dictionary
    /// does not support marking of strings. Implementors of [Dictionary::mark_str] must also implement [Dictionary::has_marked].
    fn mark_str(&mut self, _string: &str) -> AddResult {
        AddResult::Rejected
    }

    /// Returns true if the dictionary contains any marked elements (See [Dictionary::mark_str]). The intention is that code marks
    /// all elements that are relevant to this dictionary, or none at all, so that a return value of `true` indicates
    /// that one can rely on unknown and non-marked elements to be missing in all dictionaries. Implementors of
    /// [Dictionary::has_marked] must also implement [Dictionary::mark_str].
    fn has_marked(&self) -> bool {
        false
    }
}
