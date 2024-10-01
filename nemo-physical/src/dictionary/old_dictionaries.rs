//! This module collects historic dictionary code, mostly for keeping it available for benchmarks.

pub mod dictionary;
pub(crate) mod dictionary_string;

pub use dictionary_string::DictionaryString;

pub mod hash_map_dictionary;
pub(crate) use hash_map_dictionary::HashMapDictionary;

pub(crate) mod infix_dictionary;
pub(crate) use infix_dictionary::InfixDictionary;

pub mod meta_dictionary;
