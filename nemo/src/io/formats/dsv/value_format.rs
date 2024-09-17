//! This module defines the syntactic formats supported with values in DSV files.
//! This includes reading (parsing) and writing (serialization)
//! for each supported format.

use enum_assoc::Assoc;
use nemo_physical::datavalues::{AnyDataValue, DataValue, DataValueCreationError};

use crate::{
    parser::{ast::token::Token, input::ParserInput, ParserState},
    rule_model::components::{import_export::ImportExportDirective, term::tuple::Tuple},
    syntax::directive::value_formats,
};

pub(super) type DataValueParserFunction =
    fn(String) -> Result<AnyDataValue, DataValueCreationError>;

pub(super) type DataValueSerializerFunction = fn(&AnyDataValue) -> Option<String>;

/// Enum for the various formats that are supported for encoding values
/// in DSV. Since DSV has no own type system, the encoding of data must be
/// controlled through this external mechanism.
#[derive(Assoc, Debug, Clone, Copy, PartialEq, Eq)]
#[func(pub fn name(&self) -> &'static str)]
#[func(pub fn from_name(name: &str) -> Option<Self>)]
pub(crate) enum DsvValueFormat {
    /// Format that tries various heuristics to interpret and represent values
    /// in the most natural way. The format can interpret any content (the final
    /// fallback is to use it as a string).
    #[assoc(name = value_formats::ANY)]
    #[assoc(from_name = value_formats::ANY)]
    Anything,
    /// Format that interprets the DSV values as literal string values.
    /// All data will be interpreted in this way.
    #[assoc(name = value_formats::STRING)]
    #[assoc(from_name = value_formats::STRING)]
    String,
    /// Format that interprets numeric DSV values as integers, and rejects
    /// all values that are not in this form.
    #[assoc(name = value_formats::INT)]
    #[assoc(from_name = value_formats::INT)]
    Integer,
    /// Format that interprets numeric DSV values as double-precision floating
    /// point numbers, and rejects all values that are not in this form.
    #[assoc(name = value_formats::DOUBLE)]
    #[assoc(from_name = value_formats::DOUBLE)]
    Double,
    /// Special format to indicate that the value should be skipped as if the whole
    /// column where not there.
    #[assoc(name = value_formats::SKIP)]
    #[assoc(from_name = value_formats::SKIP)]
    Skip,
}

/// Indicate what value parser should be used for each column.
#[derive(Debug, Clone)]
pub struct DsvValueFormats(Vec<DsvValueFormat>);

impl DsvValueFormats {
    pub(crate) fn new(formats: Vec<DsvValueFormat>) -> Self {
        Self(formats)
    }

    /// Return a list of value formats with default entries.
    pub fn default(arity: usize) -> Self {
        Self((0..arity).map(|_| DsvValueFormat::Anything).collect())
    }

    /// Create a [DsvValueFormats] from a [Tuple].
    ///
    /// Returns `None` if tuple contains an unknown value.
    pub(crate) fn from_tuple(tuple: &Tuple) -> Option<Self> {
        let mut result = Vec::new();

        for value in tuple.arguments() {
            if let Some(format) = ImportExportDirective::plain_value(value)
                .and_then(|name| DsvValueFormat::from_name(&name))
            {
                result.push(format);
                continue;
            }

            return None;
        }

        Some(Self::new(result))
    }

    /// Return the arity (ignoring the skipped columns)
    pub(crate) fn arity(&self) -> usize {
        let mut arity = 0;

        for &format in &self.0 {
            if format != DsvValueFormat::Skip {
                arity += 1;
            }
        }

        arity
    }

    /// Return the length of the format tuple.
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    /// Return whether the tuple is empty.
    pub(crate) fn _is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return an iterator over the [DsvValueFormat]s.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &DsvValueFormat> {
        self.0.iter()
    }
}

impl DsvValueFormat {
    /// Return a function for parsing value strings for this format.
    pub(super) fn data_value_parser_function(&self) -> DataValueParserFunction {
        match self {
            DsvValueFormat::Anything => Self::parse_any_value_from_string,
            DsvValueFormat::String => Self::parse_string_from_string,
            DsvValueFormat::Integer => AnyDataValue::new_from_integer_literal,
            DsvValueFormat::Double => AnyDataValue::new_from_double_literal,
            DsvValueFormat::Skip => Self::parse_string_from_string, // irrelevant
        }
    }

    /// Return a function for parsing value strings for this format.
    pub(super) fn data_value_serializer_function(&self) -> DataValueSerializerFunction {
        match self {
            DsvValueFormat::Anything => Self::serialize_any_value_to_string,
            DsvValueFormat::String => AnyDataValue::to_plain_string,
            DsvValueFormat::Integer => Self::serialize_integer_to_string,
            DsvValueFormat::Double => Self::serialize_double_to_string,
            DsvValueFormat::Skip => Self::serialize_any_value_to_string, // irrelevant
        }
    }

    /// Simple wrapper function that makes CSV strings into [AnyDataValue]. We wrap this
    /// to match the error-producing signature of other parsing functions.
    pub(super) fn parse_string_from_string(
        input: String,
    ) -> Result<AnyDataValue, DataValueCreationError> {
        Ok(AnyDataValue::new_plain_string(input))
    }

    /// Best-effort parsing function for strings from CSV. True to the nature of CSV, this function
    /// will try hard to find a usable value in the string.
    ///
    /// TODO: This function could possibly share some methods with the parser code later on.
    /// TODO: Currently no support for guessing floating point values, only decimal.
    pub(super) fn parse_any_value_from_string(
        input: String,
    ) -> Result<AnyDataValue, DataValueCreationError> {
        // TOOD: is trimming justified? CSV is sensitive to spaces!
        let input = input.trim();

        // Represent empty cells as empty strings
        if input.is_empty() {
            return Ok(AnyDataValue::new_plain_string("".to_string()));
        }
        assert!(!input.is_empty());

        match input.as_bytes()[0] {
            b'<' => {
                if input.as_bytes()[input.len() - 1] == b'>' {
                    return Ok(AnyDataValue::new_iri(input[1..input.len() - 1].to_string()));
                }
            }
            b'0'..=b'9' | b'+' | b'-' => {
                if let Ok(dv) = AnyDataValue::new_from_decimal_literal(input.to_string()) {
                    return Ok(dv);
                }
            }
            b'"' => {
                if let Some(pos) = input.rfind('\"') {
                    if pos == input.len() - 1 {
                        return Ok(AnyDataValue::new_plain_string(
                            input[1..input.len() - 1].to_string(),
                        ));
                    } else if input.as_bytes()[pos + 1] == b'@' {
                        return Ok(AnyDataValue::new_language_tagged_string(
                            input[1..pos].to_string(),
                            input[pos + 2..input.len()].to_string(),
                        ));
                    } else if input.as_bytes()[input.len() - 1] == b'>'
                        && input.len() > pos + 4
                        && &input[pos..pos + 4] == "\"^^<"
                    {
                        if let Ok(dv) = AnyDataValue::new_from_typed_literal(
                            input[1..pos].to_string(),
                            input[pos + 4..input.len() - 1].to_string(),
                        ) {
                            return Ok(dv);
                        }
                    }
                }
            }
            _ => {}
        }

        // Check if it's a valid tag name
        let parser_input = ParserInput::new(input, ParserState::default());
        if let Ok((rest, _)) = Token::name(parser_input) {
            if rest.span.0.is_empty() {
                return Ok(AnyDataValue::new_iri(input.to_string()));
            }
        }

        // Might still be a full IRI
        let parser_input = ParserInput::new(input, ParserState::default());
        if let Ok((rest, iri)) = Token::iri(parser_input) {
            if rest.span.0.is_empty() {
                return Ok(AnyDataValue::new_iri(iri.to_string()));
            }
        }

        // Otherwise treat the input as a string literal
        Ok(AnyDataValue::new_plain_string(input.to_string()))
    }

    /// Serialize [AnyDataValue]s into CSV strings. This method handles
    /// all values, and chooses simpler formats whenever possible.
    pub(super) fn serialize_any_value_to_string(value: &AnyDataValue) -> Option<String> {
        let int_string = Self::serialize_integer_to_string(value);
        if int_string.is_some() {
            return int_string;
        }

        match value.value_domain() {
            nemo_physical::datavalues::ValueDomain::UnsignedLong
            | nemo_physical::datavalues::ValueDomain::NonNegativeLong
            | nemo_physical::datavalues::ValueDomain::UnsignedInt
            | nemo_physical::datavalues::ValueDomain::NonNegativeInt
            | nemo_physical::datavalues::ValueDomain::Long
            | nemo_physical::datavalues::ValueDomain::Int => {
                unreachable!("we checked for integers above")
            }
            nemo_physical::datavalues::ValueDomain::PlainString => {
                // This is already a short version without XSD type
                Some(value.canonical_string())
            }
            nemo_physical::datavalues::ValueDomain::Iri => {
                // strings that parse as IRIs should always be recognized as such, so we don't need the < and >
                Some(value.to_iri_unchecked())
            }
            nemo_physical::datavalues::ValueDomain::LanguageTaggedString
            | nemo_physical::datavalues::ValueDomain::Float
            | nemo_physical::datavalues::ValueDomain::Double
            | nemo_physical::datavalues::ValueDomain::Tuple
            | nemo_physical::datavalues::ValueDomain::Map
            | nemo_physical::datavalues::ValueDomain::Boolean
            | nemo_physical::datavalues::ValueDomain::Null
            | nemo_physical::datavalues::ValueDomain::Other => Some(value.canonical_string()),
        }
    }

    /// Serialize [AnyDataValue]s that are integers into CSV strings.
    pub(super) fn serialize_integer_to_string(value: &AnyDataValue) -> Option<String> {
        match value.value_domain() {
            nemo_physical::datavalues::ValueDomain::UnsignedLong
            | nemo_physical::datavalues::ValueDomain::NonNegativeLong
            | nemo_physical::datavalues::ValueDomain::UnsignedInt
            | nemo_physical::datavalues::ValueDomain::NonNegativeInt
            | nemo_physical::datavalues::ValueDomain::Long
            | nemo_physical::datavalues::ValueDomain::Int => Some(value.lexical_value()),
            nemo_physical::datavalues::ValueDomain::PlainString
            | nemo_physical::datavalues::ValueDomain::LanguageTaggedString
            | nemo_physical::datavalues::ValueDomain::Iri
            | nemo_physical::datavalues::ValueDomain::Float
            | nemo_physical::datavalues::ValueDomain::Double
            | nemo_physical::datavalues::ValueDomain::Tuple
            | nemo_physical::datavalues::ValueDomain::Map
            | nemo_physical::datavalues::ValueDomain::Boolean
            | nemo_physical::datavalues::ValueDomain::Null => None,
            nemo_physical::datavalues::ValueDomain::Other => {
                // big integers can also be exported:
                if value.datatype_iri() == "http://www.w3.org/2001/XMLSchema#integer" {
                    Some(value.lexical_value())
                } else {
                    None
                }
            }
        }
    }

    /// Serialize [AnyDataValue]s that are doubles into CSV strings.
    ///
    /// Note that some of the results can look like integers, so this
    /// format is not suitable in a mixed-type setting where we might have
    /// integer values as well.
    pub(super) fn serialize_double_to_string(value: &AnyDataValue) -> Option<String> {
        match value.value_domain() {
            nemo_physical::datavalues::ValueDomain::Double => Some(value.lexical_value()),
            nemo_physical::datavalues::ValueDomain::UnsignedLong
            | nemo_physical::datavalues::ValueDomain::NonNegativeLong
            | nemo_physical::datavalues::ValueDomain::UnsignedInt
            | nemo_physical::datavalues::ValueDomain::NonNegativeInt
            | nemo_physical::datavalues::ValueDomain::Long
            | nemo_physical::datavalues::ValueDomain::Int
            | nemo_physical::datavalues::ValueDomain::PlainString
            | nemo_physical::datavalues::ValueDomain::LanguageTaggedString
            | nemo_physical::datavalues::ValueDomain::Iri
            | nemo_physical::datavalues::ValueDomain::Float
            | nemo_physical::datavalues::ValueDomain::Tuple
            | nemo_physical::datavalues::ValueDomain::Map
            | nemo_physical::datavalues::ValueDomain::Boolean
            | nemo_physical::datavalues::ValueDomain::Null
            | nemo_physical::datavalues::ValueDomain::Other => None,
        }
    }
}
