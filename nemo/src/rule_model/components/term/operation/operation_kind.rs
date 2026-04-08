//! This module defines [OperationKind].
#![allow(missing_docs)]

use std::fmt::Display;

use enum_assoc::Assoc;
use strum_macros::EnumIter;

use crate::{rule_model::components::term::value_type::ValueType, syntax::builtin::function};

/// Number of arguments supported by an operation
#[derive(Debug)]
pub(crate) enum OperationNumArguments {
    /// Operation requires one argument
    Unary,
    /// Operation requires two arguments
    Binary,
    /// Operation requires three arguments
    _Ternary,
    /// Operation supports arbitrary many arguments (including zero)
    Arbitrary,
    /// Operation supports arguments that satisfy one of the given requirements
    Choice(Vec<usize>),
}

impl OperationNumArguments {
    /// Return whether the given number of arguments satisfies this constraint.
    pub(crate) fn validate(&self, num_arguments: usize) -> bool {
        match self {
            OperationNumArguments::Unary => num_arguments == 1,
            OperationNumArguments::Binary => num_arguments == 2,
            OperationNumArguments::_Ternary => num_arguments == 3,
            OperationNumArguments::Arbitrary => true,
            OperationNumArguments::Choice(choice) => choice.contains(&num_arguments),
        }
    }
}

impl Display for OperationNumArguments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationNumArguments::Unary => write!(f, "1"),
            OperationNumArguments::Binary => write!(f, "2"),
            OperationNumArguments::_Ternary => write!(f, "3"),
            OperationNumArguments::Arbitrary => write!(f, ""),
            OperationNumArguments::Choice(choice) => match choice.len() {
                0 => write!(f, "0"),
                1 => write!(f, "{}", choice[0]),
                2 => write!(f, "{} or {}", choice[0], choice[1]),
                _ => {
                    for (index, value) in choice.iter().enumerate() {
                        write!(f, "{value}")?;

                        match index.cmp(&(choice.len() - 2)) {
                            std::cmp::Ordering::Less => write!(f, ", ")?,
                            std::cmp::Ordering::Equal => write!(f, ", or ")?,
                            std::cmp::Ordering::Greater => {}
                        }
                    }

                    Ok(())
                }
            },
        }
    }
}

/// Supported operations
#[derive(Assoc, EnumIter, Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd)]
#[func(pub fn name(&self) -> &'static str)]
#[func(pub fn num_arguments(&self) -> OperationNumArguments)]
#[func(pub fn return_type(&self) -> ValueType)]
pub enum OperationKind {
    /// Equality
    #[assoc(name = function::EQUAL)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(return_type = ValueType::Boolean)]
    Equal,
    /// Inequality
    #[assoc(name = function::UNEQUAL)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(return_type = ValueType::Boolean)]
    Unequals,
    /// Sum of numeric values
    #[assoc(name = function::SUM)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(return_type = ValueType::Number)]
    NumericSum,
    /// Subtraction between two numeric values
    #[assoc(name = function::SUBTRACTION)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(return_type = ValueType::Number)]
    NumericSubtraction,
    /// Product of numeric values
    #[assoc(name = function::PRODUCT)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(return_type = ValueType::Number)]
    NumericProduct,
    /// Division between two numeric values
    #[assoc(name = function::DIVISION)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(return_type = ValueType::Number)]
    NumericDivision,
    /// Logarithm of a numeric value to some numeric base
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(name = function::LOGARITHM)]
    #[assoc(return_type = ValueType::Number)]
    NumericLogarithm,
    /// Numeric value raised to another numeric value
    #[assoc(name = function::POW)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(return_type = ValueType::Number)]
    NumericPower,
    /// Remainder of a division between two numeric values
    #[assoc(name = function::REM)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(return_type = ValueType::Number)]
    NumericRemainder,
    /// Numeric greater than or equals comparison
    #[assoc(name = function::GREATEREQ)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(return_type = ValueType::Boolean)]
    NumericGreaterthaneq,
    /// Numeric greater than comparison
    #[assoc(name = function::GREATER)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(return_type = ValueType::Boolean)]
    NumericGreaterthan,
    /// Numeric less than or equals comparison
    #[assoc(name = function::LESSEQ)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(return_type = ValueType::Boolean)]
    NumericLessthaneq,
    /// Numeric less than comparison
    #[assoc(name = function::LESS)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(return_type = ValueType::Boolean)]
    NumericLessthan,
    /// Lexicographic comparison between strings
    #[assoc(name = function::COMPARE)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(return_type = ValueType::Number)]
    StringCompare,
    /// Check whether string is contained in another, correspondng to SPARQL function CONTAINS.
    #[assoc(name = function::CONTAINS)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(return_type = ValueType::Boolean)]
    StringContains,
    /// Check whether the pattern given as a regular expression holds
    #[assoc(name = function::REGEX)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(return_type = ValueType::Boolean)]
    StringRegex,
    /// String starting at some start position
    #[assoc(name = function::SUBSTR)]
    #[assoc(num_arguments = OperationNumArguments::Choice(vec![2, 3]))]
    #[assoc(return_type = ValueType::String)]
    StringSubstring,
    /// First part of a string split by some other string
    #[assoc(name = function::STRBEFORE)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(return_type = ValueType::String)]
    StringBefore,
    /// Second part of a string split by some other string
    #[assoc(name = function::STRAFTER)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(return_type = ValueType::String)]
    StringAfter,
    /// Whether string starts with a certain string
    #[assoc(name = function::STRSTARTS)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(return_type = ValueType::Boolean)]
    StringStarts,
    /// Whether string ends with a certain string
    #[assoc(name = function::STRENDS)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(return_type = ValueType::Boolean)]
    StringEnds,
    /// Levenshtein distance
    #[assoc(name = function::LEVENSHTEIN)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(return_type = ValueType::Number)]
    StringLevenshtein,
    /// Boolean negation
    #[assoc(name = function::NOT)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Boolean)]
    BooleanNegation,
    /// Cast to double
    #[assoc(name = function::DOUBLE)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Number)]
    CastToDouble,
    /// Cast to float
    #[assoc(name = function::FLOAT)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Number)]
    CastToFloat,
    /// Cast to integer
    #[assoc(name = function::INT)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Number)]
    CastToInteger,
    /// Cast to IRI
    #[assoc(name = function::IRI)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Constant)]
    CastToIRI,
    /// Canonical string representation of a value
    #[assoc(name = function::FULLSTR)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::String)]
    CanonicalString,
    /// Check if value is an integer
    #[assoc(name = function::IS_INTEGER)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Boolean)]
    CheckIsInteger,
    /// Check if value is a float
    #[assoc(name = function::IS_FLOAT)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Boolean)]
    CheckIsFloat,
    /// Check if value is a double
    #[assoc(name = function::IS_DOUBLE)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Boolean)]
    CheckIsDouble,
    /// Check if value is an iri
    #[assoc(name = function::IS_IRI)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Boolean)]
    CheckIsIri,
    /// Check if value is numeric
    #[assoc(name = function::IS_NUMERIC)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Boolean)]
    CheckIsNumeric,
    /// Check if value is a null
    #[assoc(name = function::IS_NULL)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Boolean)]
    CheckIsNull,
    /// Check if value is a string
    #[assoc(name = function::IS_STRING)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Boolean)]
    CheckIsString,
    /// Get datatype of a value
    #[assoc(name = function::DATATYPE)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Constant)]
    Datatype,
    /// Construct a language tagged string
    #[assoc(name = function::STRLANG)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(return_type = ValueType::LanguageString)]
    LanguageString,
    /// Check if a language tag matches a language range, corresponding to SPARQL function langMatches.
    /// Must appear before LanguageTag ("LANG") since "langMatches" starts with "lang" (case-insensitive).
    #[assoc(name = function::LANGMATCHES)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(return_type = ValueType::Boolean)]
    StringLangMatches,
    /// Get language tag of a languaged tagged string
    #[assoc(name = function::LANG)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::String)]
    LanguageTag,
    /// Absolute value of a numeric value
    #[assoc(name = function::ABS)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Number)]
    NumericAbsolute,
    /// Cosine of a numeric valueloga
    #[assoc(name = function::COS)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Number)]
    NumericCosine,
    /// Rounding up of a numeric value
    #[assoc(name = function::CEIL)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Number)]
    NumericCeil,
    /// Rounding down of a numeric value
    #[assoc(name = function::FLOOR)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Number)]
    NumericFloor,
    /// Additive inverse of a numeric value
    #[assoc(name = function::INVERTSIGN)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Number)]
    NumericNegation,
    /// Rounding of a numeric value
    #[assoc(name = function::ROUND)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Number)]
    NumericRound,
    /// Sine of a numeric value
    #[assoc(name = function::SIN)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Number)]
    NumericSine,
    /// Square root of a numeric value
    #[assoc(name = function::SQRT)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Number)]
    NumericSquareroot,
    /// Tangent of a numeric value
    #[assoc(name = function::TAN)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Number)]
    NumericTangent,
    /// Length of a string value
    #[assoc(name = function::STRLEN)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Number)]
    StringLength,
    /// String with leading whitespace removed
    #[assoc(name = function::STRTRIMSTART)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::String)]
    StringTrimStart,
    /// String with trailing whitespace removed
    #[assoc(name = function::STRTRIMEND)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::String)]
    StringTrimEnd,
    /// String with leading and trailing whitespace removed
    #[assoc(name = function::STRTRIM)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::String)]
    StringTrim,
    /// Reverse of a string value
    #[assoc(name = function::STRREV)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::String)]
    StringReverse,
    /// String converted to lowercase letters
    #[assoc(name = function::LCASE)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::String)]
    StringLowercase,
    /// String converted to uppercase letters
    #[assoc(name = function::UCASE)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::String)]
    StringUppercase,
    /// String percent-encoded for URIs
    #[assoc(name = function::URIENCODE)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::String)]
    StringUriEncode,
    /// String percent-decoded for URIs
    #[assoc(name = function::URIDECODE)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::String)]
    StringUriDecode,
    /// Bitwise and operation
    #[assoc(name = function::BITAND)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(return_type = ValueType::Number)]
    BitAnd,
    /// Bitwise or operation
    #[assoc(name = function::BITOR)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(return_type = ValueType::Number)]
    BitOr,
    /// Bitwise xor operation
    #[assoc(name = function::BITXOR)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(return_type = ValueType::Number)]
    BitXor,
    /// Perform left arithmetic shift
    #[assoc(name = function::BITSHL)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(return_type = ValueType::Number)]
    BitShl,
    /// Perform right unsigned (logical) shift
    #[assoc(name = function::BITSHRU)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(return_type = ValueType::Number)]
    BitShru,
    /// Perform right arithmetic shift
    #[assoc(name = function::BITSHR)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(return_type = ValueType::Number)]
    BitShr,
    /// Conjunction of boolean values
    #[assoc(name = function::AND)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(return_type = ValueType::Boolean)]
    BooleanConjunction,
    /// Disjunction of boolean values
    #[assoc(name = function::OR)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(return_type = ValueType::Boolean)]
    BooleanDisjunction,
    /// Extract the minutes from an XSD dateTime/time value, corresponding to SPARQL MINUTES.
    /// Must appear before NumericMinimum ("MIN") since "min" is a prefix of "minutes".
    #[assoc(name = function::MINUTES)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Number)]
    DateTimeMinutes,
    /// Minimum of numeric values
    #[assoc(name = function::MIN)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(return_type = ValueType::Number)]
    NumericMinimum,
    /// Maximum of numeric values
    #[assoc(name = function::MAX)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(return_type = ValueType::Number)]
    NumericMaximum,
    /// Lukasiewicz norm of numeric values
    #[assoc(name = function::LUKA)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(return_type = ValueType::Number)]
    NumericLukasiewicz,
    /// Concatenation of two string values, corresponding to SPARQL function CONCAT.
    #[assoc(name = function::CONCAT)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(return_type = ValueType::String)]
    StringConcatenation,
    /// Construct a typed literal from a lexical value and a datatype IRI, corresponding to SPARQL function STRDT.
    /// Must appear before LexicalValue ("STR") since "STRDT" starts with "STR" (case-insensitive).
    #[assoc(name = function::STRDT)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(return_type = ValueType::Any)]
    TypedLiteral,
    /// Return a fresh UUID as a plain string, corresponding to SPARQL STRUUID.
    /// Must appear before LexicalValue ("STR") since "STRUUID" starts with "STR" (case-insensitive).
    #[assoc(name = function::STRUUID)]
    #[assoc(num_arguments = OperationNumArguments::Choice(vec![0]))]
    #[assoc(return_type = ValueType::String)]
    FuncStruuid,
    /// Lexical value
    #[assoc(name = function::STR)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::String)]
    LexicalValue,
    /// Replace occurrences of a regex pattern in a string, corresponding to SPARQL function REPLACE.
    #[assoc(name = function::REPLACE)]
    #[assoc(num_arguments = OperationNumArguments::Choice(vec![3, 4]))]
    #[assoc(return_type = ValueType::String)]
    StringReplace,
    /// Compute the MD5 hash of a string, corresponding to SPARQL function MD5.
    #[assoc(name = function::MD5)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::String)]
    StringMd5,
    /// Compute the SHA1 hash of a string, corresponding to SPARQL function SHA1.
    #[assoc(name = function::SHA1)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::String)]
    StringSha1,
    /// Compute the SHA256 hash of a string, corresponding to SPARQL function SHA256.
    #[assoc(name = function::SHA256)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::String)]
    StringSha256,
    /// Compute the SHA384 hash of a string, corresponding to SPARQL function SHA384.
    #[assoc(name = function::SHA384)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::String)]
    StringSha384,
    /// Compute the SHA512 hash of a string, corresponding to SPARQL function SHA512.
    #[assoc(name = function::SHA512)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::String)]
    StringSha512,
    /// Extract the year from an XSD date/dateTime, corresponding to SPARQL YEAR.
    #[assoc(name = function::YEAR)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Number)]
    DateTimeYear,
    /// Extract the month from an XSD date/dateTime, corresponding to SPARQL MONTH.
    #[assoc(name = function::MONTH)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Number)]
    DateTimeMonth,
    /// Extract the day from an XSD date/dateTime, corresponding to SPARQL DAY.
    #[assoc(name = function::DAY)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Number)]
    DateTimeDay,
    /// Extract the hours from an XSD dateTime/time, corresponding to SPARQL HOURS.
    #[assoc(name = function::HOURS)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Number)]
    DateTimeHours,
    /// Extract the seconds from an XSD dateTime/time, corresponding to SPARQL SECONDS.
    #[assoc(name = function::SECONDS)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Number)]
    DateTimeSeconds,
    /// Extract the timezone as xsd:dayTimeDuration, corresponding to SPARQL TIMEZONE.
    /// Must appear before DateTimeTz ("TZ") since "tz" is a prefix of "timezone".
    #[assoc(name = function::TIMEZONE)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::Any)]
    DateTimeTimezone,
    /// Extract the timezone as a plain string, corresponding to SPARQL TZ.
    #[assoc(name = function::TZ)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(return_type = ValueType::String)]
    DateTimeTz,
    /// Return a pseudo-random double in [0, 1), corresponding to SPARQL RAND.
    #[assoc(name = function::RAND)]
    #[assoc(num_arguments = OperationNumArguments::Choice(vec![0]))]
    #[assoc(return_type = ValueType::Number)]
    FuncRand,
    /// Return a fresh UUID as an IRI, corresponding to SPARQL UUID.
    #[assoc(name = function::UUID)]
    #[assoc(num_arguments = OperationNumArguments::Choice(vec![0]))]
    #[assoc(return_type = ValueType::Constant)]
    FuncUuid,
    /// Return the current date/time as xsd:dateTime, corresponding to SPARQL NOW.
    #[assoc(name = function::NOW)]
    #[assoc(num_arguments = OperationNumArguments::Choice(vec![0]))]
    #[assoc(return_type = ValueType::Any)]
    FuncNow,
}

impl OperationKind {
    /// Precedence of operations for display purposes.
    pub(crate) fn precedence(&self) -> usize {
        match &self {
            Self::NumericSum => 2,
            Self::NumericSubtraction => 2,
            Self::NumericProduct => 3,
            Self::NumericDivision => 3,
            Self::Equal => 0,
            Self::Unequals => 0,
            _ => 1,
        }
    }
}

impl Display for OperationKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod test {
    use strum::IntoEnumIterator;

    use super::OperationKind;

    #[test]
    fn operation_order() {
        // For the parsing to work correctly,
        // every entry in `OperationKind`
        // must be arranged in such a way that no operation name
        // is the prefix of a subsequent operation name

        let names = OperationKind::iter()
            .map(|kind| kind.name().to_lowercase())
            .collect::<Vec<_>>();

        for (name_index, name) in names.iter().enumerate() {
            if name_index == names.len() - 1 {
                break;
            }

            assert!(
                names[(name_index + 1)..]
                    .iter()
                    .all(|remaining| !remaining.starts_with(name.as_str())),
                "Operation name {:?} is a case-insensitive prefix of a later operation name",
                name,
            )
        }
    }
}
