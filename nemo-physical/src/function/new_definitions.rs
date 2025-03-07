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

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum Functions {
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
