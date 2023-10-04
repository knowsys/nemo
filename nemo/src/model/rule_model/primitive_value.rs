use std::path::PathBuf;

use nemo_physical::datatypes::Double;
use nemo_physical::error::ReadingError;
use sanitise_file_name::{sanitise_with_options, Options};
use thiserror::Error;

use crate::model::types::primitive_logical_value::{LogicalString, LOGICAL_NULL_PREFIX};

/// XSD type for string
pub const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";
/// XSD type for double
pub const XSD_DOUBLE: &str = "http://www.w3.org/2001/XMLSchema#double";
/// XSD type for decimal
pub const XSD_DECIMAL: &str = "http://www.w3.org/2001/XMLSchema#decimal";
/// XSD type for integer
pub const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

/// An identifier for, e.g., a Term or a Predicate.
#[derive(Debug, Eq, PartialEq, Hash, Clone, PartialOrd, Ord)]
pub struct Identifier(pub(crate) String);

impl Identifier {
    /// Returns the associated name
    pub fn name(&self) -> String {
        self.0.clone()
    }

    /// Returns a sanitised path with respect to the associated name
    pub fn sanitised_file_name(&self, mut path: PathBuf) -> PathBuf {
        let sanitise_options = Options::<Option<char>> {
            url_safe: true,
            ..Default::default()
        };
        let file_name = sanitise_with_options(&self.name(), &sanitise_options);
        path.push(file_name);
        path
    }
}

impl std::fmt::Display for Identifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.name())
    }
}

impl From<String> for Identifier {
    fn from(value: String) -> Self {
        Identifier(value)
    }
}

/// Terms occurring in programs.
#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub enum PrimitiveValue {
    /// An (abstract) constant.
    Constant(Identifier),
    /// A numeric literal.
    NumericLiteral(NumericLiteral),
    /// A string literal.
    StringLiteral(String),
    /// An RDF literal.
    RdfLiteral(RdfLiteral),
}

impl std::fmt::Display for PrimitiveValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            PrimitiveValue::Constant(Identifier(s)) => {
                // Nulls on logical level start with __Null# and shall be wrapped in angle brackets
                // blank nodes and anything that starts with an ascii letter (like bare names)
                // should not be wrapped in angle brackets
                if !s.starts_with(LOGICAL_NULL_PREFIX)
                    && s.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
                {
                    write!(f, "{s}")
                }
                // everything else (including nulls) shall be wrapped in angle_brackets
                else {
                    write!(f, "<{s}>")
                }
            }
            PrimitiveValue::NumericLiteral(term) => write!(f, "{term}"),
            PrimitiveValue::StringLiteral(term) => write!(f, "\"{term}\""),
            PrimitiveValue::RdfLiteral(term) => write!(f, "{term}"),
        }
    }
}

impl PrimitiveValue {
    /// Check if the term is ground.
    pub fn is_ground(&self) -> bool {
        matches!(
            self,
            Self::Constant(_) | Self::NumericLiteral(_) | Self::RdfLiteral(_)
        )
    }
}

/// A numerical literal.
#[derive(Debug, Eq, PartialEq, Copy, Clone, PartialOrd, Ord)]
pub enum NumericLiteral {
    /// An integer literal.
    Integer(i64),
    /// A decimal literal.
    Decimal(i64, u64),
    /// A double literal.
    Double(Double),
}

impl std::fmt::Display for NumericLiteral {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NumericLiteral::Integer(value) => write!(f, "{value}"),
            NumericLiteral::Decimal(left, right) => write!(f, "{left}.{right}"),
            NumericLiteral::Double(value) => write!(f, "{:E}", f64::from(*value)),
        }
    }
}

/// An RDF literal.
#[derive(Debug, Eq, PartialEq, Hash, Clone, PartialOrd, Ord)]
pub enum RdfLiteral {
    /// A language string.
    LanguageString {
        /// The literal value.
        value: String,
        /// The language tag.
        tag: String,
    },
    /// A literal with a datatype.
    DatatypeValue {
        /// The literal value.
        value: String,
        /// The datatype IRI.
        datatype: String,
    },
}

impl std::fmt::Display for RdfLiteral {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RdfLiteral::LanguageString { value, tag } => write!(f, "\"{value}\"@{tag}"),
            RdfLiteral::DatatypeValue { value, datatype } => write!(f, "\"{value}\"^^<{datatype}>"),
        }
    }
}

/// An [`InvalidRdfLiteral`]
#[derive(Debug, Error, PartialEq)]
#[error("The literal \"{}\" is not valid.", .literal)]
pub struct InvalidRdfLiteral {
    literal: RdfLiteral,
}

impl InvalidRdfLiteral {
    /// Create new `InvalidRdfLiteral` error
    pub fn new(literal: RdfLiteral) -> Self {
        Self { literal }
    }
}

impl From<InvalidRdfLiteral> for ReadingError {
    fn from(value: InvalidRdfLiteral) -> Self {
        Self::InvalidRdfLiteral(value.literal.to_string())
    }
}

impl TryFrom<RdfLiteral> for PrimitiveValue {
    type Error = InvalidRdfLiteral;

    fn try_from(literal: RdfLiteral) -> Result<Self, Self::Error> {
        match literal {
            RdfLiteral::LanguageString { .. } => Ok(Self::RdfLiteral(literal)),
            RdfLiteral::DatatypeValue {
                ref value,
                ref datatype,
            } => match datatype.as_ref() {
                XSD_STRING => Ok(LogicalString::from(value.to_string()).into()),
                XSD_INTEGER => {
                    let trimmed = value.strip_prefix(['-', '+']).unwrap_or(value);

                    if !trimmed.chars().all(|c| c.is_ascii_digit()) {
                        Err(InvalidRdfLiteral::new(literal.clone()))
                    } else {
                        Ok(value
                            .parse()
                            .map(|v| Self::NumericLiteral(NumericLiteral::Integer(v)))
                            .unwrap_or(Self::RdfLiteral(literal)))
                    }
                }
                XSD_DECIMAL => match value.rsplit_once('.') {
                    Some((a, b)) => {
                        let trimmed_a = a.strip_prefix(['-', '+']).unwrap_or(a);
                        let is_valid = trimmed_a.chars().all(|c| c.is_ascii_digit())
                            && b.chars().all(|c| c.is_ascii_digit());

                        if !is_valid {
                            Err(InvalidRdfLiteral::new(literal.clone()))
                        } else {
                            Ok(a.parse()
                                .ok()
                                .and_then(|a| Some((a, b.parse().ok()?)))
                                .map(|(a, b)| Self::NumericLiteral(NumericLiteral::Decimal(a, b)))
                                .unwrap_or(Self::RdfLiteral(literal)))
                        }
                    }
                    None => {
                        let trimmed = value.strip_prefix(['-', '+']).unwrap_or(value);
                        let is_valid = trimmed.chars().all(|c| c.is_ascii_digit());

                        if !is_valid {
                            Err(InvalidRdfLiteral::new(literal.clone()))
                        } else {
                            Ok(value
                                .parse()
                                .ok()
                                .map(|v| Self::NumericLiteral(NumericLiteral::Decimal(v, 0)))
                                .unwrap_or(Self::RdfLiteral(literal)))
                        }
                    }
                },
                XSD_DOUBLE => Ok(value
                    .parse()
                    .ok()
                    .and_then(|f64| Double::new(f64).ok())
                    .map(|d| Self::NumericLiteral(NumericLiteral::Double(d)))
                    .unwrap_or(Self::RdfLiteral(literal))),
                _ => Ok(Self::RdfLiteral(literal)),
            },
        }
    }
}

#[cfg(test)]
mod test {
    use std::assert_eq;

    use super::*;

    #[test]
    fn rdf_literal_normalization() {
        let language_string_literal = RdfLiteral::LanguageString {
            value: "language string".to_string(),
            tag: "en".to_string(),
        };
        let random_datavalue_literal = RdfLiteral::DatatypeValue {
            value: "some random datavalue".to_string(),
            datatype: "a datatype that I totally did not just make up".to_string(),
        };
        let string_datavalue_literal = RdfLiteral::DatatypeValue {
            value: "string datavalue".to_string(),
            datatype: XSD_STRING.to_string(),
        };
        let integer_datavalue_literal = RdfLiteral::DatatypeValue {
            value: "73".to_string(),
            datatype: XSD_INTEGER.to_string(),
        };
        let decimal_datavalue_literal = RdfLiteral::DatatypeValue {
            value: "1.23".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        };
        let signed_decimal_datavalue_literal = RdfLiteral::DatatypeValue {
            value: "+1.23".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        };
        let negative_decimal_datavalue_literal = RdfLiteral::DatatypeValue {
            value: "-1.23".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        };
        let pointless_decimal_datavalue_literal = RdfLiteral::DatatypeValue {
            value: "23".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        };
        let signed_pointless_decimal_datavalue_literal = RdfLiteral::DatatypeValue {
            value: "+23".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        };
        let negative_pointless_decimal_datavalue_literal = RdfLiteral::DatatypeValue {
            value: "-23".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        };
        let double_datavalue_literal = RdfLiteral::DatatypeValue {
            value: "3.33".to_string(),
            datatype: XSD_DOUBLE.to_string(),
        };
        let large_integer_literal = RdfLiteral::DatatypeValue {
            value: "9950000000000000000".to_string(),
            datatype: XSD_INTEGER.to_string(),
        };
        let large_decimal_literal = RdfLiteral::DatatypeValue {
            value: "9950000000000000001".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        };
        let invalid_integer_literal = RdfLiteral::DatatypeValue {
            value: "123.45".to_string(),
            datatype: XSD_INTEGER.to_string(),
        };
        let invalid_decimal_literal = RdfLiteral::DatatypeValue {
            value: "123.45a".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        };

        let expected_language_string_literal =
            PrimitiveValue::RdfLiteral(language_string_literal.clone());
        let expected_random_datavalue_literal =
            PrimitiveValue::RdfLiteral(random_datavalue_literal.clone());
        let expected_string_datavalue_literal =
            PrimitiveValue::StringLiteral("string datavalue".to_string());
        let expected_integer_datavalue_literal =
            PrimitiveValue::NumericLiteral(NumericLiteral::Integer(73));
        let expected_decimal_datavalue_literal =
            PrimitiveValue::NumericLiteral(NumericLiteral::Decimal(1, 23));
        let expected_signed_decimal_datavalue_literal =
            PrimitiveValue::NumericLiteral(NumericLiteral::Decimal(1, 23));
        let expected_negative_decimal_datavalue_literal =
            PrimitiveValue::NumericLiteral(NumericLiteral::Decimal(-1, 23));
        let expected_pointless_decimal_datavalue_literal =
            PrimitiveValue::NumericLiteral(NumericLiteral::Decimal(23, 0));
        let expected_signed_pointless_decimal_datavalue_literal =
            PrimitiveValue::NumericLiteral(NumericLiteral::Decimal(23, 0));
        let expected_negative_pointless_decimal_datavalue_literal =
            PrimitiveValue::NumericLiteral(NumericLiteral::Decimal(-23, 0));
        let expected_double_datavalue_literal =
            PrimitiveValue::NumericLiteral(NumericLiteral::Double(Double::new(3.33).unwrap()));
        let expected_large_integer_literal =
            PrimitiveValue::RdfLiteral(RdfLiteral::DatatypeValue {
                value: "9950000000000000000".to_string(),
                datatype: XSD_INTEGER.to_string(),
            });
        let expected_large_decimal_literal =
            PrimitiveValue::RdfLiteral(RdfLiteral::DatatypeValue {
                value: "9950000000000000001".to_string(),
                datatype: XSD_DECIMAL.to_string(),
            });
        let expected_invalid_integer_literal = InvalidRdfLiteral::new(RdfLiteral::DatatypeValue {
            value: "123.45".to_string(),
            datatype: XSD_INTEGER.to_string(),
        });
        let expected_invalid_decimal_literal = InvalidRdfLiteral::new(RdfLiteral::DatatypeValue {
            value: "123.45a".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        });

        assert_eq!(
            PrimitiveValue::try_from(language_string_literal).unwrap(),
            expected_language_string_literal
        );
        assert_eq!(
            PrimitiveValue::try_from(random_datavalue_literal).unwrap(),
            expected_random_datavalue_literal
        );
        assert_eq!(
            PrimitiveValue::try_from(string_datavalue_literal).unwrap(),
            expected_string_datavalue_literal
        );
        assert_eq!(
            PrimitiveValue::try_from(integer_datavalue_literal).unwrap(),
            expected_integer_datavalue_literal
        );
        assert_eq!(
            PrimitiveValue::try_from(decimal_datavalue_literal).unwrap(),
            expected_decimal_datavalue_literal
        );
        assert_eq!(
            PrimitiveValue::try_from(signed_decimal_datavalue_literal).unwrap(),
            expected_signed_decimal_datavalue_literal
        );
        assert_eq!(
            PrimitiveValue::try_from(negative_decimal_datavalue_literal).unwrap(),
            expected_negative_decimal_datavalue_literal
        );
        assert_eq!(
            PrimitiveValue::try_from(pointless_decimal_datavalue_literal).unwrap(),
            expected_pointless_decimal_datavalue_literal
        );
        assert_eq!(
            PrimitiveValue::try_from(signed_pointless_decimal_datavalue_literal).unwrap(),
            expected_signed_pointless_decimal_datavalue_literal
        );
        assert_eq!(
            PrimitiveValue::try_from(negative_pointless_decimal_datavalue_literal).unwrap(),
            expected_negative_pointless_decimal_datavalue_literal
        );
        assert_eq!(
            PrimitiveValue::try_from(double_datavalue_literal).unwrap(),
            expected_double_datavalue_literal
        );
        assert_eq!(
            PrimitiveValue::try_from(large_integer_literal).unwrap(),
            expected_large_integer_literal
        );
        assert_eq!(
            PrimitiveValue::try_from(large_decimal_literal).unwrap(),
            expected_large_decimal_literal
        );
        assert_eq!(
            PrimitiveValue::try_from(invalid_integer_literal).unwrap_err(),
            expected_invalid_integer_literal
        );
        assert_eq!(
            PrimitiveValue::try_from(invalid_decimal_literal).unwrap_err(),
            expected_invalid_decimal_literal
        );
    }
}
