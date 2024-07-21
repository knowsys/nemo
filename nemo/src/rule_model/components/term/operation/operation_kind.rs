//! This module defines [OperationKind].
#![allow(missing_docs)]

use std::fmt::Display;

use enum_assoc::Assoc;
use strum_macros::EnumIter;

use crate::rule_model::syntax::builtins;

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
    #[assoc(name = builtins::BUILTIN_EQUAL)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = true)]
    Equal,
    /// Inequality
    #[assoc(name = builtins::BUILTIN_UNEQUAL)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = true)]
    Unequals,
    /// Sum of numeric values
    #[assoc(name = builtins::BUILTIN_SUM)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(is_boolean = false)]
    NumericSum,
    /// Subtraction between two numeric values
    #[assoc(name = builtins::BUILTIN_SUBTRACTION)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = false)]
    NumericSubtraction,
    /// Product of numeric values
    #[assoc(name = builtins::BUILTIN_PRODUCT)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(is_boolean = false)]
    NumericProduct,
    /// Division between two numeric values
    #[assoc(name = builtins::BUILTIN_DIVISION)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = false)]
    NumericDivision,
    /// Logarithm of a numeric value to some numeric base
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(name = builtins::BUILTIN_LOGARITHM)]
    #[assoc(is_boolean = false)]
    NumericLogarithm,
    /// Numeric value raised to another numeric value
    #[assoc(name = builtins::BUILTIN_POW)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = false)]
    NumericPower,
    /// Remainder of a division between two numeric values
    #[assoc(name = builtins::BUILTIN_REM)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = false)]
    NumericRemainder,
    /// Numeric greater than or equals comparison
    #[assoc(name = builtins::BUILTIN_GREATEREQ)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = false)]
    NumericGreaterthaneq,
    /// Numeric greater than comparison
    #[assoc(name = builtins::BUILTIN_GREATER)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = false)]
    NumericGreaterthan,
    /// Numeric less than or equals comparison
    #[assoc(name = builtins::BUILTIN_LESSEQ)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = false)]
    NumericLessthaneq,
    /// Numeric less than comparison
    #[assoc(name = builtins::BUILTIN_LESS)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = false)]
    NumericLessthan,
    /// Lexicographic comparison between strings
    #[assoc(name = builtins::BUILTIN_COMPARE)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = false)]
    StringCompare,
    /// Check whether string is contained in another, correspondng to SPARQL function CONTAINS.
    #[assoc(name = builtins::BUILTIN_CONTAINS)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = true)]
    StringContains,
    /// String starting at some start position
    #[assoc(name = builtins::BUILTIN_SUBSTR)]
    #[assoc(num_arguments = OperationNumArguments::Choice(vec![OperationNumArguments::Binary, OperationNumArguments::Ternary]))]
    #[assoc(is_boolean = false)]
    StringSubstring,
    /// First part of a string split by some other string
    #[assoc(name = builtins::BUILTIN_STRBEFORE)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = false)]
    StringBefore,
    /// Second part of a string split by some other string
    #[assoc(name = builtins::BUILTIN_STRAFTER)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = false)]
    StringAfter,
    /// Whether string starts with a certain string
    #[assoc(name = builtins::BUILTIN_STRSTARTS)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = true)]
    StringStarts,
    /// Whether string ends with a certain string
    #[assoc(name = builtins::BUILTIN_STRENDS)]
    #[assoc(num_arguments = OperationNumArguments::Binary)]
    #[assoc(is_boolean = true)]
    StringEnds,
    /// Boolean negation
    #[assoc(name = builtins::BUILTIN_NOT)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = true)]
    BooleanNegation,
    /// Cast to double
    #[assoc(name = builtins::BUILTIN_DOUBLE)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    CastToDouble,
    /// Cast to float
    #[assoc(name = builtins::BUILTIN_FLOAT)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    CastToFloat,
    /// Cast to integer
    #[assoc(name = builtins::BUILTIN_INT)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    CastToInteger,
    /// Canonical string representation of a value
    #[assoc(name = builtins::BUILTIN_FULLSTR)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    CanonicalString,
    /// Check if value is an integer
    #[assoc(name = builtins::BUILTIN_IS_INTEGER)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = true)]
    CheckIsInteger,
    /// Check if value is a float
    #[assoc(name = builtins::BUILTIN_IS_FLOAT)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = true)]
    CheckIsFloat,
    /// Check if value is a double
    #[assoc(name = builtins::BUILTIN_IS_DOUBLE)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = true)]
    CheckIsDouble,
    /// Check if value is an iri
    #[assoc(name = builtins::BUILTIN_IS_IRI)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = true)]
    CheckIsIri,
    /// Check if value is numeric
    #[assoc(name = builtins::BUILTIN_IS_NUMERIC)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = true)]
    CheckIsNumeric,
    /// Check if value is a null
    #[assoc(name = builtins::BUILTIN_IS_NULL)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = true)]
    CheckIsNull,
    /// Check if value is a string
    #[assoc(name = builtins::BUILTIN_IS_STRING)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = true)]
    CheckIsString,
    /// Get datatype of a value
    #[assoc(name = builtins::BUILTIN_DATATYPE)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    Datatype,
    /// Get language tag of a languaged tagged string
    #[assoc(name = builtins::BUILTIN_LANG)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    LanguageTag,
    /// Absolute value of a numeric value
    #[assoc(name = builtins::BUILTIN_ABS)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    NumericAbsolute,
    /// Cosine of a numeric valueloga
    #[assoc(name = builtins::BUILTIN_COS)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    NumericCosine,
    /// Rounding up of a numeric value
    #[assoc(name = builtins::BUILTIN_CEIL)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    NumericCeil,
    /// Rounding down of a numeric value
    #[assoc(name = builtins::BUILTIN_FLOOR)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    NumericFloor,
    /// Additive inverse of a numeric value
    #[assoc(name = builtins::BUILTIN_INVERSE)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    NumericNegation,
    /// Rounding of a numeric value
    #[assoc(name = builtins::BUILTIN_ROUND)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    NumericRound,
    /// Sine of a numeric value
    #[assoc(name = builtins::BUILTIN_SIN)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    NumericSine,
    /// Square root of a numeric value
    #[assoc(name = builtins::BUILTIN_SQRT)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    NumericSquareroot,
    /// Tangent of a numeric value
    #[assoc(name = builtins::BUILTIN_TAN)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    NumericTangent,
    /// Length of a string value
    #[assoc(name = builtins::BUILTIN_STRLEN)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    StringLength,
    /// Reverse of a string value
    #[assoc(name = builtins::BUILTIN_STRREV)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    StringReverse,
    /// String converted to lowercase letters
    #[assoc(name = builtins::BUILTIN_LCASE)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    StringLowercase,
    /// String converted to uppercase letters
    #[assoc(name = builtins::BUILTIN_UCASE)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    StringUppercase,
    /// Bitwise and operation
    #[assoc(name = builtins::BUILTIN_BITAND)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    BitAnd,
    /// Bitwise or operation
    #[assoc(name = builtins::BUILTIN_BITOR)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    BitOr,
    /// Bitwise xor operation
    #[assoc(name = builtins::BUILTIN_BITXOR)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    BitXor,
    /// Conjunction of boolean values
    #[assoc(name = builtins::BUILTIN_AND)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(is_boolean = true)]
    BooleanConjunction,
    /// Disjunction of boolean values
    #[assoc(name = builtins::BUILTIN_OR)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(is_boolean = true)]
    BooleanDisjunction,
    /// Minimum of numeric values
    #[assoc(name = builtins::BUILTIN_MIN)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(is_boolean = false)]
    NumericMinimum,
    /// Maximum of numeric values
    #[assoc(name = builtins::BUILTIN_MAX)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(is_boolean = false)]
    NumericMaximum,
    /// Lukasiewicz norm of numeric values
    #[assoc(name = builtins::BUILTIN_LUKA)]
    #[assoc(num_arguments = OperationNumArguments::Arbitrary)]
    #[assoc(is_boolean = false)]
    NumericLukasiewicz,
    /// Concatentation of two string values, correspondng to SPARQL function CONCAT.
    #[assoc(name = builtins::BUILTIN_CONCAT)]
    #[assoc(num_arguments = OperationNumArguments::Unary)]
    #[assoc(is_boolean = false)]
    StringConcatenation,
    /// Lexical value
    #[assoc(name = builtins::BUILTIN_STR)]
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
