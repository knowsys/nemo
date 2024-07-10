//! This module defines [OperationKind].

use std::fmt::Display;

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
    /// Operation supports the given number of arguments
    Exact(usize),
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
            OperationNumArguments::Exact(exact) => num_arguments == *exact,
            OperationNumArguments::Choice(choice) => {
                choice.iter().any(|num| num.validate(num_arguments))
            }
        }
    }
}

/// Supported operations
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub enum OperationKind {
    /// Equality
    Equal,
    /// Inequality
    Unequals,
    /// Sum of numeric values
    NumericSum,
    /// Subtraction between two numeric values
    NumericSubtraction,
    /// Product of numeric values
    NumericProduct,
    /// Division between two numeric values
    NumericDivision,
    /// Logarithm of a numeric value to some numeric base
    NumericLogarithm,
    /// Numeric value raised to another numeric value
    NumericPower,
    /// Remainder of a division between two numeric values
    NumericRemainder,
    /// Numeric greater than comparison
    NumericGreaterthan,
    /// Numeric greater than or equals comparison
    NumericGreaterthaneq,
    /// Numeric less than comparison
    NumericLessthan,
    /// Numeric less than or equals comparison
    NumericLessthaneq,
    /// Lexicographic comparison between strings
    StringCompare,
    /// Check whether string is contained in another, correspondng to SPARQL function CONTAINS.
    StringContains,
    /// String starting at some start position
    StringSubstring,
    /// First part of a string split by some other string
    StringBefore,
    /// Second part of a string split by some other string
    StringAfter,
    /// Whether string starts with a certain string
    StringStarts,
    /// Whether string ends with a certain string
    StringEnds,
    /// Boolean negation
    BooleanNegation,
    /// Cast to double
    CastToDouble,
    /// Cast to float
    CastToFloat,
    /// Cast to integer
    CastToInteger,
    /// Canonical string representation of a value
    CanonicalString,
    /// Check if value is an integer
    CheckIsInteger,
    /// Check if value is a float
    CheckIsFloat,
    /// Check if value is a double
    CheckIsDouble,
    /// Check if value is an iri
    CheckIsIri,
    /// Check if value is numeric
    CheckIsNumeric,
    /// Check if value is a null
    CheckIsNull,
    /// Check if value is a string
    CheckIsString,
    /// Get datatype of a value
    Datatype,
    /// Get language tag of a languaged tagged string
    LanguageTag,
    /// Lexical value
    LexicalValue,
    /// Absolute value of a numeric value
    NumericAbsolute,
    /// Cosine of a numeric value
    NumericCosine,
    /// Rounding up of a numeric value
    NumericCeil,
    /// Rounding down of a numeric value
    NumericFloor,
    /// Additive inverse of a numeric value
    NumericNegation,
    /// Rounding of a numeric value
    NumericRound,
    /// Sine of a numeric value
    NumericSine,
    /// Square root of a numeric value
    NumericSquareroot,
    /// Tangent of a numeric value
    NumericTangent,
    /// Length of a string value
    StringLength,
    /// Reverse of a string value
    StringReverse,
    /// String converted to lowercase letters
    StringLowercase,
    /// String converted to uppercase letters
    StringUppercase,
    /// Bitwise and operation
    BitAnd,
    /// Bitwise or operation
    BitOr,
    /// Bitwise xor operation
    BitXor,
    /// Conjunction of boolean values
    BooleanConjunction,
    /// Disjunction of boolean values
    BooleanDisjunction,
    /// Minimum of numeric values
    NumericMinimum,
    /// Maximum of numeric values
    NumericMaximum,
    /// Lukasiewicz norm of numeric values
    NumericLukasiewicz,
    /// Concatentation of two string values, correspondng to SPARQL function CONCAT.
    StringConcatenation,
}

impl OperationKind {
    /// Return the [OperationKind] corresponding to the given operation name or `None` if there is no such operation.
    pub fn from_name(name: &str) -> Option<OperationKind> {
        Some(match name.to_uppercase().as_str() {
            "+" => Self::NumericSum,
            "-" => Self::NumericSubtraction,
            "/" => Self::NumericDivision,
            "*" => Self::NumericProduct,
            "<" => Self::NumericLessthan,
            ">" => Self::NumericGreaterthan,
            "<=" => Self::NumericLessthaneq,
            ">=" => Self::NumericGreaterthaneq,
            "isInteger" => Self::CheckIsInteger,
            "isFloat" => Self::CheckIsFloat,
            "isDouble" => Self::CheckIsDouble,
            "isIri" => Self::CheckIsIri,
            "isNumeric" => Self::CheckIsNumeric,
            "isNull" => Self::CheckIsNull,
            "isString" => Self::CheckIsString,
            "ABS" => Self::NumericAbsolute,
            "SQRT" => Self::NumericSquareroot,
            "NOT" => Self::BooleanNegation,
            "fullStr" => Self::CanonicalString,
            "STR" => Self::LexicalValue,
            "SIN" => Self::NumericSine,
            "COS" => Self::NumericCosine,
            "TAN" => Self::NumericTangent,
            "STRLEN" => Self::StringLength,
            "STRREV" => Self::StringReverse,
            "UCASE" => Self::StringLowercase,
            "LCASE" => Self::StringUppercase,
            "ROUND" => Self::NumericRound,
            "CEIL" => Self::NumericCeil,
            "FLOOR" => Self::NumericFloor,
            "DATATYPE" => Self::Datatype,
            "LANG" => Self::LanguageTag,
            "INT" => Self::CastToInteger,
            "DOUBLE" => Self::CastToDouble,
            "FLOAT" => Self::CastToFloat,
            "LOG" => Self::NumericLogarithm,
            "POW" => Self::NumericPower,
            "COMPARE" => Self::StringCompare,
            "CONTAINS" => Self::StringContains,
            "SUBSTR" => Self::StringSubstring,
            "STRSTARTS" => Self::StringStarts,
            "STRENDS" => Self::StringEnds,
            "STRBEFORE" => Self::StringBefore,
            "STRAFTER" => Self::StringAfter,
            "REM" => Self::NumericRemainder,
            "BITAND" => Self::BitAnd,
            "BITOR" => Self::BitOr,
            "BITXOR" => Self::BitXor,
            "MAX" => Self::NumericMaximum,
            "MIN" => Self::NumericMinimum,
            "LUKA" => Self::NumericLukasiewicz,
            "SUM" => Self::NumericSum,
            "PROD" => Self::NumericProduct,
            "AND" => Self::BooleanConjunction,
            "OR" => Self::BooleanDisjunction,
            "CONCAT" => Self::StringConcatenation,
            _ => return None,
        })
    }

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

    /// Return whether the operation returns a boolean value.
    pub(crate) fn is_boolean(&self) -> bool {
        match self {
            OperationKind::Equal => true,
            OperationKind::Unequals => true,
            OperationKind::NumericSum => false,
            OperationKind::NumericSubtraction => false,
            OperationKind::NumericProduct => false,
            OperationKind::NumericDivision => false,
            OperationKind::NumericLogarithm => false,
            OperationKind::NumericPower => false,
            OperationKind::NumericRemainder => false,
            OperationKind::NumericGreaterthan => true,
            OperationKind::NumericGreaterthaneq => true,
            OperationKind::NumericLessthan => true,
            OperationKind::NumericLessthaneq => true,
            OperationKind::StringCompare => false,
            OperationKind::StringContains => true,
            OperationKind::StringSubstring => false,
            OperationKind::StringBefore => true,
            OperationKind::StringAfter => true,
            OperationKind::StringStarts => true,
            OperationKind::StringEnds => true,
            OperationKind::BooleanNegation => true,
            OperationKind::CastToDouble => false,
            OperationKind::CastToFloat => false,
            OperationKind::CastToInteger => false,
            OperationKind::CanonicalString => false,
            OperationKind::CheckIsInteger => true,
            OperationKind::CheckIsFloat => true,
            OperationKind::CheckIsDouble => true,
            OperationKind::CheckIsIri => true,
            OperationKind::CheckIsNumeric => true,
            OperationKind::CheckIsNull => true,
            OperationKind::CheckIsString => true,
            OperationKind::Datatype => false,
            OperationKind::LanguageTag => false,
            OperationKind::LexicalValue => false,
            OperationKind::NumericAbsolute => false,
            OperationKind::NumericCosine => false,
            OperationKind::NumericCeil => false,
            OperationKind::NumericFloor => false,
            OperationKind::NumericNegation => false,
            OperationKind::NumericRound => false,
            OperationKind::NumericSine => false,
            OperationKind::NumericSquareroot => false,
            OperationKind::NumericTangent => false,
            OperationKind::StringLength => false,
            OperationKind::StringReverse => false,
            OperationKind::StringLowercase => false,
            OperationKind::StringUppercase => false,
            OperationKind::BitAnd => false,
            OperationKind::BitOr => false,
            OperationKind::BitXor => false,
            OperationKind::BooleanConjunction => true,
            OperationKind::BooleanDisjunction => true,
            OperationKind::NumericMinimum => false,
            OperationKind::NumericMaximum => false,
            OperationKind::NumericLukasiewicz => false,
            OperationKind::StringConcatenation => false,
        }
    }

    /// Return the number of arguments accepted by this operation
    pub(crate) fn number_arguments(&self) {}
}

impl Display for OperationKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let string = match self {
            OperationKind::Equal => "EQUAL",
            OperationKind::Unequals => "UNEQUAL",
            OperationKind::NumericSum => "SUM",
            OperationKind::NumericSubtraction => "MINUS",
            OperationKind::NumericProduct => "PROD",
            OperationKind::NumericDivision => "DIV",
            OperationKind::NumericLogarithm => "LOG",
            OperationKind::NumericPower => "POW",
            OperationKind::NumericRemainder => "REM",
            OperationKind::NumericGreaterthan => "GT",
            OperationKind::NumericGreaterthaneq => "GTE",
            OperationKind::NumericLessthan => "LT",
            OperationKind::NumericLessthaneq => "LTE",
            OperationKind::StringCompare => "COMPARE",
            OperationKind::StringContains => "CONTAINS",
            OperationKind::StringSubstring => "SUBSTR",
            OperationKind::StringBefore => "STRBEFORE",
            OperationKind::StringAfter => "STRAFTER",
            OperationKind::StringStarts => "STRSTARTS",
            OperationKind::StringEnds => "STRENDS",
            OperationKind::BooleanNegation => "NOT",
            OperationKind::CastToDouble => "DOUBLE",
            OperationKind::CastToFloat => "FLOAT",
            OperationKind::CastToInteger => "INT",
            OperationKind::CanonicalString => "fullStr",
            OperationKind::CheckIsInteger => "isInteger",
            OperationKind::CheckIsFloat => "isFloat",
            OperationKind::CheckIsDouble => "isDouble",
            OperationKind::CheckIsIri => "isIri",
            OperationKind::CheckIsNumeric => "isNumeric",
            OperationKind::CheckIsNull => "isNull",
            OperationKind::CheckIsString => "isString",
            OperationKind::Datatype => "DATATYPE",
            OperationKind::LanguageTag => "LANG",
            OperationKind::LexicalValue => "STR",
            OperationKind::NumericAbsolute => "ABS",
            OperationKind::NumericCosine => "COS",
            OperationKind::NumericCeil => "CEIL",
            OperationKind::NumericFloor => "FLOOR",
            OperationKind::NumericNegation => "MINUS",
            OperationKind::NumericRound => "ROUND",
            OperationKind::NumericSine => "SIN",
            OperationKind::NumericSquareroot => "SQRT",
            OperationKind::NumericTangent => "TAN",
            OperationKind::StringLength => "STRLEN",
            OperationKind::StringReverse => "STRREV",
            OperationKind::StringLowercase => "LCASE",
            OperationKind::StringUppercase => "UCASE",
            OperationKind::BitAnd => "BITAND",
            OperationKind::BitOr => "BITOR",
            OperationKind::BitXor => "BITXOR",
            OperationKind::BooleanConjunction => "AND",
            OperationKind::BooleanDisjunction => "OR",
            OperationKind::NumericMinimum => "MIN",
            OperationKind::NumericMaximum => "MAX",
            OperationKind::NumericLukasiewicz => "LUKA",
            OperationKind::StringConcatenation => "CONCAT",
        };

        write!(f, "{}", string)
    }
}
