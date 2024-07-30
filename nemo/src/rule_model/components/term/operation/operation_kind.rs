//! This module defines [OperationKind].
#![allow(missing_docs)]

use std::fmt::Display;

use enum_assoc::Assoc;
use strum_macros::EnumIter;

use crate::syntax::builtin::function;

/// Number of arguments supported by an operation
#[derive(Debug)]
pub(crate) enum OperationNumArguments {
    /// Operation requires one argument
    Unary,
    /// Operation requires two arguments
    Binary,
    /// Operation requires three arguments
    Ternary,
    /// Operation supports arbitrary many arguments (including zero)
    Arbitrary,
    /// Operation supports arguments that satisfy one of the given requirements
    Choice(Vec<OperationNumArguments>),
}

impl OperationNumArguments {
    /// Return whether the given number of arguments satisfies this constraint.
    pub(crate) fn validate(&self, num_arguments: usize) -> bool {
        match self {
            OperationNumArguments::Unary => num_arguments == 1,
            OperationNumArguments::Binary => num_arguments == 2,
            OperationNumArguments::Ternary => num_arguments == 3,
            OperationNumArguments::Arbitrary => true,
            OperationNumArguments::Choice(choice) => {
                choice.iter().any(|num| num.validate(num_arguments))
            }
        }
    }
}

/// Supported operations
#[derive(Assoc, EnumIter, Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd)]
#[func(pub fn name(&self) -> &'static str)]
#[func(pub fn num_arguments(&self) -> OperationNumArguments)]
#[func(pub fn is_boolean(&self) -> bool)]
pub enum OperationKind {
    /// Equality
    #[assoc(name = function::EQUAL)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = true)]
    Equal,
    /// Inequality
    #[assoc(name = function::UNEQUAL)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = true)]
    Unequals,
    /// Sum of numeric values
    #[assoc(name = function::SUM)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(is_boolean = false)]
    NumericSum,
    /// Subtraction between two numeric values
    #[assoc(name = function::SUBTRACTION)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = false)]
    NumericSubtraction,
    /// Product of numeric values
    #[assoc(name = function::PRODUCT)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(is_boolean = false)]
    NumericProduct,
    /// Division between two numeric values
    #[assoc(name = function::DIVISION)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = false)]
    NumericDivision,
    /// Logarithm of a numeric value to some numeric base
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(name = function::LOGARITHM)]
    #[assoc(is_boolean = false)]
    NumericLogarithm,
    /// Numeric value raised to another numeric value
    #[assoc(name = function::POW)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = false)]
    NumericPower,
    /// Remainder of a division between two numeric values
    #[assoc(name = function::REM)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = false)]
    NumericRemainder,
    /// Numeric greater than or equals comparison
    #[assoc(name = function::GREATEREQ)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = false)]
    NumericGreaterthaneq,
    /// Numeric greater than comparison
    #[assoc(name = function::GREATER)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = false)]
    NumericGreaterthan,
    /// Numeric less than or equals comparison
    #[assoc(name = function::LESSEQ)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = false)]
    NumericLessthaneq,
    /// Numeric less than comparison
    #[assoc(name = function::LESS)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = false)]
    NumericLessthan,
    /// Lexicographic comparison between strings
    #[assoc(name = function::COMPARE)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = false)]
    StringCompare,
    /// Check whether string is contained in another, correspondng to SPARQL function CONTAINS.
    #[assoc(name = function::CONTAINS)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = true)]
    StringContains,
    /// String starting at some start position
    #[assoc(name = function::SUBSTR)]
    #[assoc(num_arguments = OperationNumArguments::Choice(vec![OperationNumArguments::Binary, OperationNumArguments::Ternary]))]
    #[assoc(is_boolean = false)]
    StringSubstring,
    /// First part of a string split by some other string
    #[assoc(name = function::STRBEFORE)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = false)]
    StringBefore,
    /// Second part of a string split by some other string
    #[assoc(name = function::STRAFTER)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = false)]
    StringAfter,
    /// Whether string starts with a certain string
    #[assoc(name = function::STRSTARTS)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = true)]
    StringStarts,
    /// Whether string ends with a certain string
    #[assoc(name = function::STRENDS)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = true)]
    StringEnds,
    /// Boolean negation
    #[assoc(name = function::NOT)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = true)]
    BooleanNegation,
    /// Cast to double
    #[assoc(name = function::DOUBLE)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    CastToDouble,
    /// Cast to float
    #[assoc(name = function::FLOAT)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    CastToFloat,
    /// Cast to integer
    #[assoc(name = function::INT)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    CastToInteger,
    /// Canonical string representation of a value
    #[assoc(name = function::FULLSTR)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    CanonicalString,
    /// Check if value is an integer
    #[assoc(name = function::IS_INTEGER)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = true)]
    CheckIsInteger,
    /// Check if value is a float
    #[assoc(name = function::IS_FLOAT)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = true)]
    CheckIsFloat,
    /// Check if value is a double
    #[assoc(name = function::IS_DOUBLE)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = true)]
    CheckIsDouble,
    /// Check if value is an iri
    #[assoc(name = function::IS_IRI)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = true)]
    CheckIsIri,
    /// Check if value is numeric
    #[assoc(name = function::IS_NUMERIC)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = true)]
    CheckIsNumeric,
    /// Check if value is a null
    #[assoc(name = function::IS_NULL)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = true)]
    CheckIsNull,
    /// Check if value is a string
    #[assoc(name = function::IS_STRING)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = true)]
    CheckIsString,
    /// Get datatype of a value
    #[assoc(name = function::DATATYPE)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    Datatype,
    /// Get language tag of a languaged tagged string
    #[assoc(name = function::LANG)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    LanguageTag,
    /// Absolute value of a numeric value
    #[assoc(name = function::ABS)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    NumericAbsolute,
    /// Cosine of a numeric valueloga
    #[assoc(name = function::COS)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    NumericCosine,
    /// Rounding up of a numeric value
    #[assoc(name = function::CEIL)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    NumericCeil,
    /// Rounding down of a numeric value
    #[assoc(name = function::FLOOR)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    NumericFloor,
    /// Additive inverse of a numeric value
    #[assoc(name = function::INVERSE)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    NumericNegation,
    /// Rounding of a numeric value
    #[assoc(name = function::ROUND)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    NumericRound,
    /// Sine of a numeric value
    #[assoc(name = function::SIN)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    NumericSine,
    /// Square root of a numeric value
    #[assoc(name = function::SQRT)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    NumericSquareroot,
    /// Tangent of a numeric value
    #[assoc(name = function::TAN)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    NumericTangent,
    /// Length of a string value
    #[assoc(name = function::STRLEN)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    StringLength,
    /// Reverse of a string value
    #[assoc(name = function::STRREV)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    StringReverse,
    /// String converted to lowercase letters
    #[assoc(name = function::LCASE)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    StringLowercase,
    /// String converted to uppercase letters
    #[assoc(name = function::UCASE)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    StringUppercase,
    /// Bitwise and operation
    #[assoc(name = function::BITAND)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    BitAnd,
    /// Bitwise or operation
    #[assoc(name = function::BITOR)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    BitOr,
    /// Bitwise xor operation
    #[assoc(name = function::BITXOR)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    BitXor,
    /// Conjunction of boolean values
    #[assoc(name = function::AND)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(is_boolean = true)]
    BooleanConjunction,
    /// Disjunction of boolean values
    #[assoc(name = function::OR)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(is_boolean = true)]
    BooleanDisjunction,
    /// Minimum of numeric values
    #[assoc(name = function::MIN)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(is_boolean = false)]
    NumericMinimum,
    /// Maximum of numeric values
    #[assoc(name = function::MAX)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(is_boolean = false)]
    NumericMaximum,
    /// Lukasiewicz norm of numeric values
    #[assoc(name = function::LUKA)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(is_boolean = false)]
    NumericLukasiewicz,
    /// Concatentation of two string values, correspondng to SPARQL function CONCAT.
    #[assoc(name = function::CONCAT)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    StringConcatenation,
    /// Lexical value
    #[assoc(name = function::STR)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    LexicalValue,
}

impl OperationKind {
    /// Precendence of operations for display purposes.
    pub(crate) fn precedence(&self) -> usize {
        match &self {
            Self::NumericSum => 1,
            Self::NumericSubtraction => 1,
            Self::NumericProduct => 2,
            Self::NumericDivision => 2,
            _ => 3,
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
            .map(|kind| kind.name())
            .collect::<Vec<_>>();

        for (name_index, name) in names.iter().enumerate() {
            if name_index == names.len() - 1 {
                break;
            }

            assert!(names[(name_index + 1)..]
                .iter()
                .all(|remaining| !remaining.starts_with(name)))
        }
    }
}
