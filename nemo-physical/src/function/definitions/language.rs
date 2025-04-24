//! This module defines functions relating to language tagged strings

use crate::{
    datatypes::StorageTypeName,
    datavalues::{AnyDataValue, DataValue},
};

use super::{FunctionTypePropagation, UnaryFunction};

/// Language tag
///
/// Returns the language tag as a string of a language tagged string.
/// Returns the empty string if value is a string.
/// Returns `None` if value is neither string nor language tagged string.
#[derive(Debug, Copy, Clone)]
pub struct LanguageTag;
impl UnaryFunction for LanguageTag {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        parameter
            .to_language_tagged_string()
            .map(|(_, tag)| AnyDataValue::new_plain_string(tag))
            .or_else(|| {
                parameter
                    .to_plain_string()
                    .map(|_| AnyDataValue::new_plain_string(String::new()))
            })
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

#[cfg(test)]
mod test {
    use crate::{
        datavalues::AnyDataValue,
        function::definitions::{language::LanguageTag, UnaryFunction},
    };

    #[test]
    fn test_lang() {
        let lang_tag =
            AnyDataValue::new_language_tagged_string("Roberto".to_string(), "en".to_string());
        let exp_result = AnyDataValue::new_plain_string("en".to_string());
        let result = LanguageTag.evaluate(lang_tag).unwrap();
        assert_eq!(result, exp_result);

        let string = AnyDataValue::new_plain_string("Roberto".to_string());
        let empty_result = AnyDataValue::new_plain_string("".to_string());
        let result = LanguageTag.evaluate(string).unwrap();
        assert_eq!(result, empty_result);
    }
}
