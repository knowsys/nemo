//! This module defines functions relating to language tagged strings

use crate::datavalues::DataValue;
use crate::function::definitions::string::LangTaggedString;
use crate::function::definitions::BinaryFunction;
use crate::{datatypes::StorageTypeName, datavalues::AnyDataValue};

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
        LangTaggedString::try_from(parameter)
            .ok()?
            .tag_into_data_value()
            .or_else(|| Some(AnyDataValue::new_plain_string(String::default())))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

/// Language String
///
/// Construct a language tagged string from two strings
/// the first being the tagged string and the second the language tag.
///
/// Returns `None` if the input parameters are not strings.
#[derive(Debug, Copy, Clone)]
pub struct LanguageString;
impl BinaryFunction for LanguageString {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        let string = parameter_first.to_plain_string()?;
        let tag = parameter_second.to_plain_string()?;

        Some(AnyDataValue::new_language_tagged_string(string, tag))
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
        function::definitions::{UnaryFunction, language::LanguageTag},
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
