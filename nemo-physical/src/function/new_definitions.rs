//! This module defines all supported functions.

use boolean::OperableBoolean;
use casting::OperableCasting;
use checktype::OperableCheckType;
use generic::OperableGeneric;
use language::OperableLanguage;
use numeric::OperableNumeric;
use string::OperableString;

pub(crate) mod boolean;
pub(crate) mod casting;
pub(crate) mod checktype;
pub(crate) mod generic;
pub(crate) mod language;
pub(crate) mod numeric;
pub(crate) mod string;

/// Trait for types on which Nemo's functions can be evaluated
pub(crate) trait Operable:
    OperableBoolean
    + OperableCasting
    + OperableCheckType
    + OperableGeneric
    + OperableNumeric
    + OperableLanguage
    + OperableString
{
}

impl<T> Operable for T where
    T: OperableBoolean
        + OperableCasting
        + OperableCheckType
        + OperableGeneric
        + OperableNumeric
        + OperableLanguage
        + OperableString
{
}

/// Enum containing all supported functions
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum Functions {
    Constant,
    BooleanNegation,
    CanonicalString,
    CastingIntoInteger64,
    CastingIntoDouble,
    CastingIntoFloat,
    CastingIntoIri,
    CheckIsDouble,
    CheckIsFloat,
    CheckIsInteger,
    CheckIsIri,
    CheckIsNull,
    CheckIsNumeric,
    CheckIsString,
    Datatype,
    LanguageTag,
    LexicalValue,
    NumericAbsolute,
    NumericCeil,
    NumericCosine,
    NumericFloor,
    NumericNegation,
    NumericRound,
    NumericSine,
    NumericSquareroot,
    NumericTangent,
    StringLength,
    StringReverse,
    StringLowercase,
    StringUppercase,
    StringUriEncode,
    StringUriDecode,
    Equals,
    Unequals,
    NumericAddition,
    NumericSubtraction,
    NumericMultiplication,
    NumericDivision,
    NumericLogarithm,
    NumericPower,
    NumericRemainder,
    NumericLessthan,
    NumericLessthaneq,
    NumericGreaterthan,
    NumericGreaterthaneq,
    StringAfter,
    StringBefore,
    StringCompare,
    StringContains,
    StringRegex,
    StringLevenshtein,
    StringEnds,
    StringStarts,
    StringSubstring,
    BitShiftLeft,
    BitShiftRight,
    BitShiftRightUnsigned,
    StringSubstringLength,
    BitAnd,
    BitOr,
    BitXor,
    BooleanConjunction,
    BooleanDisjunction,
    NumericMaximum,
    NumericMinimum,
    NumericLukasiewicz,
    NumericSum,
    NumericProduct,
    StringConcatenation,
}

// Macro for generating constructors for function trees (see tree.rs)
macro_rules! generate_tree_constructor {
    (unary, $variant:ident, $function:ident) => {
        #[doc = concat!("Construct a tree node for the function ", stringify!($variant), ".")]
        pub fn $function(sub: Self) -> Self {
            Self::Function(Functions::$variant, vec![sub])
        }
    };
    (binary, $variant:ident, $function:ident) => {
        #[doc = concat!("Construct a tree node for the function ", stringify!($variant), ".")]
        pub fn $function(left: Self, right: Self) -> Self {
            Self::Function(Functions::$variant, vec![left, right])
        }
    };
    (ternary, $variant:ident, $function:ident) => {
        #[doc = concat!("Construct a tree node for the function ", stringify!($variant), ".")]
        pub fn $function(left: Self, middle: Self, right: Self) -> Self {
            Self::Function(Functions::$variant, vec![left, middle, right])
        }
    };
    (nary, $variant:ident, $function:ident) => {
        #[doc = concat!("Construct a tree node for the function ", stringify!($variant), ".")]
        pub fn $function(sub: Vec<Self>) -> Self {
            Self::Function(Functions::$variant, sub)
        }
    };
    // Ignore other variants (if any)
    ($_:ident, $_content:ident) => {};
}

macro_rules! generate_macros_functions {
    // Accept entries such as (VariantA => (function_a(x, y), binary), VariantB => (function_b(x), unary))
    ($($variant:ident => ( $func:ident ( $($param:ident),* ), $arity:ident ) ),*) => {
        /// Generate constructors for function trees
        #[macro_export]
        macro_rules! generate_tree_constructors {
            () => {
                $(
                    generate_tree_constructor!($arity, $variant, $func);
                )*
            };
        }

        /// Implementation of StackProgram::evaluate
        #[macro_export]
        macro_rules! match_function {
            ($var_enum:expr, $stack:expr, $constants:expr, $extra:expr) => {
                match $var_enum {
                    Functions::Constant => {
                        $stack.push($constants[$extra].clone());
                    }
                    $(
                        Functions::$variant => {
                            #[allow(unused_parens)]
                            let ($($param),*) = $stack.$arity($extra);
                            let result = T::$func($($param),*)?;
                            *$stack.top_mut() = result;
                        }
                    )*
                }
            };
        }
    };
}

generate_macros_functions!(
    BooleanNegation => (boolean_negation(parameter), unary),
    CanonicalString => (canonical_string(parameter), unary),
    CastingIntoInteger64 => (casting_into_integer(parameter), unary),
    CastingIntoDouble => (casting_into_double(parameter), unary),
    CastingIntoFloat => (casting_into_float(parameter), unary),
    CastingIntoIri => (casting_into_iri(parameter), unary),
    CheckIsDouble => (check_is_double(parameter), unary),
    CheckIsFloat => (check_is_float(parameter), unary),
    CheckIsInteger => (check_is_integer(parameter), unary),
    CheckIsIri => (check_is_iri(parameter), unary),
    CheckIsNull => (check_is_null(parameter), unary),
    CheckIsNumeric => (check_is_numeric(parameter), unary),
    CheckIsString => (check_is_string(parameter), unary),
    Datatype => (datatype(parameter), unary),
    LanguageTag => (language_tag(parameter), unary),
    LexicalValue => (lexical_value(parameter), unary),
    NumericAbsolute => (numeric_absolute(parameter), unary),
    NumericCeil => (numeric_ceil(parameter), unary),
    NumericCosine => (numeric_cosine(parameter), unary),
    NumericFloor => (numeric_floor(parameter), unary),
    NumericNegation => (numeric_negation(parameter), unary),
    NumericRound => (numeric_round(parameter), unary),
    NumericSine => (numeric_sine(parameter), unary),
    NumericSquareroot => (numeric_squareroot(parameter), unary),
    NumericTangent => (numeric_tangent(parameter), unary),
    StringLength => (string_length(parameter), unary),
    StringReverse => (string_reverse(parameter), unary),
    StringLowercase => (string_lowercase(parameter), unary),
    StringUppercase => (string_uppercase(parameter), unary),
    StringUriEncode => (string_uri_encode(parameter), unary),
    StringUriDecode => (string_uri_decode(parameter), unary),
    Equals => (equals(first, second), binary),
    Unequals => (unequals(first, second), binary),
    NumericAddition => (numeric_addition(first, second), binary),
    NumericSubtraction => (numeric_subtraction(first, second), binary),
    NumericMultiplication => (numeric_multiplication(first, second), binary),
    NumericDivision => (numeric_division(first, second), binary),
    NumericLogarithm => (numeric_logarithm(first, second), binary),
    NumericPower => (numeric_power(first, second), binary),
    NumericRemainder => (numeric_remainder(first, second), binary),
    NumericLessthan => (numeric_less_than(first, second), binary),
    NumericLessthaneq => (numeric_less_than_eq(first, second), binary),
    NumericGreaterthan => (numeric_greater_than(first, second), binary),
    NumericGreaterthaneq => (numeric_greater_than_eq(first, second), binary),
    StringAfter => (string_after(first, second), binary),
    StringBefore => (string_before(first, second), binary),
    StringCompare => (string_compare(first, second), binary),
    StringContains => (string_contains(first, second), binary),
    StringRegex => (string_regex(first, second), binary),
    StringLevenshtein => (string_levenshtein(first, second), binary),
    StringEnds => (string_ends(first, second), binary),
    StringStarts => (string_starts(first, second), binary),
    StringSubstring => (string_substring(first, second), binary),
    BitShiftLeft => (numeric_bit_shift_left(first, second), binary),
    BitShiftRight => (numeric_bit_shift_right(first, second), binary),
    BitShiftRightUnsigned => (numeric_bit_shift_right_unsigned(first, second), binary),
    StringSubstringLength => (string_substring_length(first, second, third), ternary),
    BitAnd => (numeric_bit_and(parameters), nary),
    BitOr => (numeric_bit_or(parameters), nary),
    BitXor => (numeric_bit_xor(parameters), nary),
    BooleanConjunction => (boolean_conjunction(parameters), nary),
    BooleanDisjunction => (boolean_disjunction(parameters), nary),
    NumericMaximum => (numeric_maximum(parameters), nary),
    NumericMinimum => (numeric_minimum(parameters), nary),
    NumericLukasiewicz => (numeric_lukasiewicz(parameters), nary),
    NumericSum => (numeric_sum(parameters), nary),
    NumericProduct => (numeric_product(parameters), nary),
    StringConcatenation => (string_concatenation(parameters), nary)
);
