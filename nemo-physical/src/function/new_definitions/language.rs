//! This module defines all supported functions relating to language-tagged strings.

use crate::{
    datavalues::{AnyDataValue, DataValue},
    storagevalues::{double::Double, float::Float, storagevalue::StorageValueT},
};

/// Trait for types for which generic operations are defined
pub(crate) trait OperableLanguage {
    /// Language tag
    ///
    /// Returns the the language tag as a string of a language tagged string.
    /// Returns `None` if value is not a languaged tagged string.
    #[allow(unused)]
    fn language_tag(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }
}

// Use default implementation for all storage values
impl OperableLanguage for i64 {}
impl OperableLanguage for Float {}
impl OperableLanguage for Double {}
impl OperableLanguage for StorageValueT {}

impl OperableLanguage for AnyDataValue {
    fn language_tag(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        parameter
            .to_language_tagged_string()
            .map(|(_, tag)| AnyDataValue::new_plain_string(tag))
    }
}
