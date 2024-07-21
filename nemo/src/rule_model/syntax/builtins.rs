//! This module contains constants relating to the builtin functions that are supported.

/// Check if two values are equal to each other
pub(crate) const BUILTIN_EQUAL: &str = "EQUAL";
/// Check if two values are not equal to each other
pub(crate) const BUILTIN_UNEQUAL: &str = "UNEQUAL";
/// Check if a numeric value is greater than another
pub(crate) const BUILTIN_GREATER: &str = "GREATER";
/// Check if a numeric value is greater or equal to another
pub(crate) const BUILTIN_GREATEREQ: &str = "GREATEREQ";
/// Check if a numeric value is smaller than another
pub(crate) const BUILTIN_LESS: &str = "LESS";
/// Check if a numeric value is smaller or equal to another
pub(crate) const BUILTIN_LESSEQ: &str = "LESSEQ";
/// Check if value is an integer
pub(crate) const BUILTIN_IS_INTEGER: &str = "isInteger";
/// Check if value is a 32bit floating point number
pub(crate) const BUILTIN_IS_FLOAT: &str = "isFloat";
/// Check if value is a 64bit floating point number
pub(crate) const BUILTIN_IS_DOUBLE: &str = "isDouble";
/// Check if value is an iri
pub(crate) const BUILTIN_IS_IRI: &str = "isIri";
/// Check if value is numeric
pub(crate) const BUILTIN_IS_NUMERIC: &str = "isNumeric";
/// Check if value is null
pub(crate) const BUILTIN_IS_NULL: &str = "isNull";
/// Check if value is string
pub(crate) const BUILTIN_IS_STRING: &str = "isString";
/// Compute the absoule value of a number
pub(crate) const BUILTIN_ABS: &str = "ABS";
/// Compute the square root of a number
pub(crate) const BUILTIN_SQRT: &str = "SQRT";
/// Logical negation of a boolean value
pub(crate) const BUILTIN_NOT: &str = "NOT";
/// String representation of a value
pub(crate) const BUILTIN_FULLSTR: &str = "fullStr";
/// Lexical value
pub(crate) const BUILTIN_STR: &str = "STR";
/// Compute the sine of a value
pub(crate) const BUILTIN_SIN: &str = "SIN";
/// Compute the cosine of a value
pub(crate) const BUILTIN_COS: &str = "COS";
/// Compute the tangent of a value
pub(crate) const BUILTIN_TAN: &str = "TAN";
/// Compute the length of a string
pub(crate) const BUILTIN_STRLEN: &str = "STRLEN";
/// Compute the reverse of a string value
pub(crate) const BUILTIN_STRREV: &str = "STRREV";
/// Replace characters in strings with their upper case version
pub(crate) const BUILTIN_UCASE: &str = "UCASE";
/// Replace characters in strings with their lower case version
pub(crate) const BUILTIN_LCASE: &str = "LCASE";
/// Round a value to the nearest integer
pub(crate) const BUILTIN_ROUND: &str = "ROUND";
/// Round up to the nearest integer
pub(crate) const BUILTIN_CEIL: &str = "CEIL";
/// Round down to the neatest integer
pub(crate) const BUILTIN_FLOOR: &str = "FLOOR";
/// Return the datatype of the value
pub(crate) const BUILTIN_DATATYPE: &str = "DATATYPE";
/// Return the language tag of the value
pub(crate) const BUILTIN_LANG: &str = "LANG";
/// Convert the value to an integer
pub(crate) const BUILTIN_INT: &str = "INT";
/// Convert the value to a 64bit floating point number
pub(crate) const BUILTIN_DOUBLE: &str = "DOUBLE";
/// Convert the value to a 32bit floating point number
pub(crate) const BUILTIN_FLOAT: &str = "FLOAT";
/// Compute the logarithm of the numerical value
pub(crate) const BUILTIN_LOGARITHM: &str = "LOG";
/// Raise the numerical value to a power
pub(crate) const BUILTIN_POW: &str = "POW";
/// Compare two string values
pub(crate) const BUILTIN_COMPARE: &str = "COMPARE";
/// Check if one string value is contained in another
pub(crate) const BUILTIN_CONTAINS: &str = "CONTAINS";
/// Return a substring of a given string value
pub(crate) const BUILTIN_SUBSTR: &str = "SUBSTR";
/// Check if a string starts with a certain string
pub(crate) const BUILTIN_STRSTARTS: &str = "STRSTARTS";
/// Check if a string ends with a certain string
pub(crate) const BUILTIN_STRENDS: &str = "STRENDS";
/// Return the first part of a string split by some other string
pub(crate) const BUILTIN_STRBEFORE: &str = "STRBEFORE";
/// Return the second part of a string split by some other string
pub(crate) const BUILTIN_STRAFTER: &str = "STRAFTER";
/// Compute the remainder of two numerical values
pub(crate) const BUILTIN_REM: &str = "REM";
/// Compute the and on the bit representation of integer values
pub(crate) const BUILTIN_BITAND: &str = "BITAND";
/// Compute the or on the bit representation of integer values
pub(crate) const BUILTIN_BITOR: &str = "BITOR";
/// Compute the exclusive or on the bit representation of integer values
pub(crate) const BUILTIN_BITXOR: &str = "BITXOR";
/// Compute the maximum of numeric values
pub(crate) const BUILTIN_MAX: &str = "MAX";
/// Compute the minimum of numeric values
pub(crate) const BUILTIN_MIN: &str = "MIN";
/// Compute the lukasiewicz norm of numeric values
pub(crate) const BUILTIN_LUKA: &str = "LUKA";
/// Compute the sum of numerical values
pub(crate) const BUILTIN_SUM: &str = "SUM";
/// Compute the product of numerical values
pub(crate) const BUILTIN_PRODUCT: &str = "PROD";
/// Compute the difference between to numeric values
pub(crate) const BUILTIN_SUBTRACTION: &str = "MINUS";
/// Compute the quotient of two numeric values
pub(crate) const BUILTIN_DIVISION: &str = "DIV";
/// Compute the multiplicative inverse of a numeric value
pub(crate) const BUILTIN_INVERSE: &str = "INVERSE";
/// Compute the logical and between boolean values
pub(crate) const BUILTIN_AND: &str = "AND";
/// Compute the logical or between boolean values
pub(crate) const BUILTIN_OR: &str = "OR";
/// Compute the concatenation of string values
pub(crate) const BUILTIN_CONCAT: &str = "CONCAT";
