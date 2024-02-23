//! This module defines functions relating to language tagged strings

use crate::datavalues::{AnyDataValue, DataValue};

use super::UnaryFunction;

/// Language tag
///
/// Returns the the language tag as a string of a language tagged string.
/// Returns `None` if value is not a languaged tagged string.
#[derive(Debug, Copy, Clone)]
pub struct LanguageTag;
impl UnaryFunction for LanguageTag {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        parameter
            .to_language_tagged_string()
            .map(|(_, tag)| AnyDataValue::new_plain_string(tag))
    }
}
