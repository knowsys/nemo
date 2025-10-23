//! This module provides functionalities for creating and maintaining dictionaries.
//!
//! A dictionary is a data structure that assigns numeric ids to complex objects (such as [String]s),
//! and that provides an bijective (invertible) mapping between the two.

pub mod datavalue_dictionary;
pub use datavalue_dictionary::AddResult;
pub use datavalue_dictionary::DvDict;
pub use datavalue_dictionary::KNOWN_ID_MARK;
pub use datavalue_dictionary::NONEXISTING_ID_MARK;

pub mod string_dictionary;
pub(crate) use string_dictionary::StringDictionary;
pub(crate) mod bytes_buffer;
pub(crate) mod bytes_dictionary;
pub(crate) mod ranked_pair_dictionary;
pub(crate) mod ranked_pair_dv_dict;
pub(crate) mod ranked_pair_iri_dv_dict;
pub(crate) mod ranked_pair_other_dv_dict;
// pub(crate) mod ranked_pair_langstring_dv_dict;

pub mod string_map;

pub(crate) mod string_dv_dict;
pub(crate) use string_dv_dict::StringDvDictionary;
// pub(crate) mod string_iri_dv_dict;
// pub(crate) mod string_other_dv_dict;
pub(crate) mod string_langstring_dv_dict;

pub(crate) mod null_dv_dict;
pub(crate) use null_dv_dict::NullDvDictionary;

pub(crate) mod map_dv_dict;
pub(crate) mod tuple_dv_dict;

pub mod meta_dv_dict;

#[cfg(feature = "old_dictionaries")]
pub mod old_dictionaries;
