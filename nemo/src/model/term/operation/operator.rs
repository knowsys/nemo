//! The builtin operators.

use nemo_physical::value::BUILTIN_NAMES;

/// The builtin operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operator {
    /// Term identity, written `=`.
    ///
    /// `?x = ?y` holds exactly if both are the same term.
    /// Joins and duplicate elimination use this equality.
    /// It never raises an evaluation error.
    Equal,
    /// Term distinctness, written `!=`, which is the negation of [Equal](Self::Equal).
    Unequal,
    /// Value equality, written `==`.
    ///
    /// Numbers are compared across domains, so `1 == 1.0d` holds.
    /// DateTimes, dates and times are compared as instants, and durations by their length.
    /// Comparing two literals that cannot be compared is an evaluation error, as in SPARQL.
    ValueEqual,
    /// Value inequality, written `!==`, which is the negation of [ValueEqual](Self::ValueEqual).
    ///
    /// It is an evaluation error wherever [ValueEqual](Self::ValueEqual) is one.
    ValueUnequal,
    /// Greater-than comparison, written `>`.
    Greater,
    /// Greater-or-equal comparison, written `>=`.
    GreaterEq,
    /// Less-than comparison, written `<`.
    Less,
    /// Less-or-equal comparison, written `<=`.
    LessEq,
    /// Addition, written `+`, of numbers, of durations, or of a point in time and a duration.
    Add,
    /// Subtraction, written `-`, of numbers, of durations, of a duration from a point in time,
    /// or of two points in time, which gives a duration.
    Subtract,
    /// Multiplication, written `*`, of numbers, or of a duration and a number.
    Multiply,
    /// Division, written `/`, of numbers, of a duration by a number,
    /// or of two durations, which gives a decimal.
    Divide,
    /// Numeric negation.
    Negate,
    /// Absolute value of a number.
    Abs,
    /// Largest of the numeric arguments.
    Max,
    /// Smallest of the numeric arguments.
    Min,
    /// Boolean conjunction.
    And,
    /// Boolean disjunction.
    Or,
    /// Boolean negation.
    Not,
    /// Whether the argument is an IRI.
    IsIri,
    /// Whether the argument is numeric.
    IsNumeric,
    /// Length of a string.
    StringLength,
    /// A string in upper case.
    UpperCase,
    /// A string in lower case.
    LowerCase,
    /// Whether a string contains another.
    Contains,
    /// Whether a string starts with another.
    StartsWith,
    /// Whether a string ends with another.
    EndsWith,
    /// Concatenation of strings.
    Concat,
    /// The lexical form of a value as a plain string.
    Str,
    /// Whether the argument is a plain string.
    IsString,
    /// Whether the argument is an integer.
    IsInteger,
    /// Whether the argument is a 32-bit float.
    IsFloat,
    /// Whether the argument is a 64-bit float.
    IsDouble,
    /// Whether the argument is a labeled null.
    IsNull,
    /// Whether the argument is a decimal number, integers included.
    IsDecimal,
    /// Whether the argument is a boolean.
    IsBoolean,
    /// Whether the argument is a language-tagged string.
    IsLangString,
    /// Whether the argument is a dateTime.
    IsDateTime,
    /// Whether the argument is a date.
    IsDate,
    /// Whether the argument is a time.
    IsTime,
    /// Whether the argument is a duration of any kind.
    IsDuration,
    /// Whether the argument is a dayTimeDuration.
    IsDayTimeDuration,
    /// Whether the argument is a yearMonthDuration.
    IsYearMonthDuration,
    /// The datatype IRI of a value.
    Datatype,
    /// The canonical form of a value as a string.
    FullStr,
    /// The language tag of a tagged string.
    Lang,
    /// A string tagged with a language.
    StrLang,
    /// Whether a language tag matches a range.
    LangMatches,
    /// A typed literal from a lexical form and a datatype.
    StrDt,
    /// A value cast to an integer.
    CastInt,
    /// A value cast to a 32-bit float.
    CastFloat,
    /// A value cast to a 64-bit float.
    CastDouble,
    /// A string cast to an IRI.
    CastIri,
    /// A value cast to a dateTime.
    CastDateTime,
    /// A value cast to a date.
    CastDate,
    /// A value cast to a time.
    CastTime,
    /// A value cast to a duration.
    CastDuration,
    /// A value cast to a dayTimeDuration.
    CastDayTimeDuration,
    /// A value cast to a yearMonthDuration.
    CastYearMonthDuration,
    /// A number rounded to the nearest integer value.
    Round,
    /// The first number raised to the second.
    Pow,
    /// The logarithm of a number in a given base.
    Log,
    /// The sine of a number.
    Sin,
    /// The square root of a number.
    Sqrt,
    /// The cosine of a number.
    Cos,
    /// The tangent of a number.
    Tan,
    /// A number rounded up to the nearest integer value.
    Ceil,
    /// A number rounded down to the nearest integer value.
    Floor,
    /// The remainder of integer division.
    Rem,
    /// The bitwise and of the integer arguments.
    BitAnd,
    /// The bitwise or of the integer arguments.
    BitOr,
    /// The bitwise xor of the integer arguments.
    BitXor,
    /// An integer shifted left.
    BitShl,
    /// An integer shifted right, arithmetically.
    BitShr,
    /// The Lukasiewicz t-norm of the arguments.
    Luka,
    /// The order of two values, as -1, 0 or 1.
    Compare,
    /// The Levenshtein distance of two strings.
    Levenshtein,
    /// A string reversed.
    StrRev,
    /// A string without surrounding whitespace.
    Trim,
    /// A string without leading whitespace.
    TrimStart,
    /// A string without trailing whitespace.
    TrimEnd,
    /// The part of a string before another.
    StrBefore,
    /// The part of a string after another.
    StrAfter,
    /// A string percent-encoded.
    UriEncode,
    /// A string percent-decoded.
    UriDecode,
    /// A substring of a string, from a 1-based position.
    Substr,
    /// Whether a regular expression matches a string.
    Regex,
    /// A string with the matches of a regular expression replaced.
    Replace,
    /// The MD5 digest of a string, in hex.
    Md5,
    /// The SHA-1 digest of a string, in hex.
    Sha1,
    /// The SHA-256 digest of a string, in hex.
    Sha256,
    /// The SHA-384 digest of a string, in hex.
    Sha384,
    /// The SHA-512 digest of a string, in hex.
    Sha512,
    /// The year of a date or dateTime, or the whole years of a duration.
    Year,
    /// The month of a date or dateTime, or the months of a duration beyond its years.
    Month,
    /// The day of a date or dateTime, or the whole days of a duration.
    Day,
    /// The hours of a time, dateTime or duration.
    Hours,
    /// The minutes of a time, dateTime or duration.
    Minutes,
    /// The seconds of a time, dateTime or duration.
    Seconds,
    /// The timezone of a dateTime, date or time as a string.
    TzString,
    /// The timezone of a dateTime, date or time as a duration.
    Timezone,
    /// A dateTime, date or time adjusted to the timezone a duration names.
    Adjust,
    /// A random double in [0, 1).
    Rand,
    /// A fresh UUID as an IRI.
    Uuid,
    /// A fresh UUID as a string.
    StrUuid,
    /// The current instant as a dateTime.
    Now,
    /// A component of a structure,
    /// which is a tuple's element at a 0-based index or a map's value at a key.
    ///
    /// It is also written `?t[0]`.
    Get,
    /// The number of elements or pairs of a structure.
    Length,
    /// The label of a structure, or the empty string if it has none.
    Label,
    /// The keys of a map as a tuple.
    Keys,
    /// Whether a map has a key.
    HasKey,
    /// Whether the argument is a tuple.
    IsTuple,
    /// Whether the argument is a map.
    IsMap,
    /// A tuple of strings joined by a separator.
    StrJoin,
    /// A string split at a separator into a tuple of strings.
    StrSplit,
}

impl Operator {
    /// Returns whether the operator is nondeterministic, so that two calls of it need not agree.
    ///
    /// These operators are `RAND`, `UUID`, `STRUUID` and `NOW`.
    pub(crate) fn is_nondeterministic(&self) -> bool {
        matches!(
            self,
            Operator::Rand | Operator::Uuid | Operator::StrUuid | Operator::Now
        )
    }

    /// Returns the infix symbol of the operator,
    /// or `None` if the operator is only written as a call.
    pub fn infix(&self) -> Option<&'static str> {
        Some(match self {
            Operator::Equal => "=",
            Operator::Unequal => "!=",
            Operator::ValueEqual => "==",
            Operator::ValueUnequal => "!==",
            Operator::Greater => ">",
            Operator::GreaterEq => ">=",
            Operator::Less => "<",
            Operator::LessEq => "<=",
            Operator::Add => "+",
            Operator::Subtract => "-",
            Operator::Multiply => "*",
            Operator::Divide => "/",
            _ => return None,
        })
    }

    /// Returns the precedence of an infix operator, or `None` for any other operator.
    ///
    /// An operator with a higher precedence binds more tightly.
    /// The parser and the display of operations use the same numbers.
    pub const fn precedence(&self) -> Option<u8> {
        Some(match self {
            Operator::Equal
            | Operator::Unequal
            | Operator::ValueEqual
            | Operator::ValueUnequal
            | Operator::Greater
            | Operator::GreaterEq
            | Operator::Less
            | Operator::LessEq => 3,
            Operator::Add | Operator::Subtract => 4,
            Operator::Multiply | Operator::Divide => 5,
            _ => return None,
        })
    }
}

/// Declares the names by which the builtins are called.
///
/// An entry is `Variant => "name"`,
/// followed by `as "NAME"` if the name is not in upper case,
/// and by `| "OTHER"` for each further name of the builtin.
/// [name](Operator::name) and [from_name](Operator::from_name) are generated from the entries,
/// so that they cannot disagree.
macro_rules! operator_names {
    (@upper $name:literal) => {
        $name
    };
    (@upper $name:literal as $upper:literal) => {
        $upper
    };
    ($($variant:ident => $name:literal $(as $upper:literal)? $(| $other:literal)*,)+) => {
        impl Operator {
            /// Returns the name of the operator in a call, such as `STRLEN`.
            pub fn name(&self) -> &'static str {
                match self {
                    $(Operator::$variant => $name,)+
                }
            }

            /// Returns the operator with the given name in a call, written in any case,
            /// or `None` if there is none.
            ///
            /// The names are exactly those in [NAMES](Self::NAMES).
            /// Other names, like `equal` or `less`, remain free for predicates and function
            /// terms.
            pub fn from_name(name: &str) -> Option<Self> {
                Some(match name.to_ascii_uppercase().as_str() {
                    $(
                        operator_names!(@upper $name $(as $upper)?) $(| $other)* => {
                            Operator::$variant
                        }
                    )+
                    _ => return None,
                })
            }

            /// The names of the entries, in upper case and in the order of the variants.
            #[cfg(test)]
            const ENTRY_NAMES: &'static [&'static str] = &[
                $(operator_names!(@upper $name $(as $upper)?), $($other,)*)+
            ];
        }
    };
}

operator_names! {
    Equal => "EQUALITY",
    Unequal => "UNEQUALITY",
    ValueEqual => "VALUEEQUALITY",
    ValueUnequal => "VALUEUNEQUALITY",
    Greater => "NUMGREATER",
    GreaterEq => "NUMGREATEREQ",
    Less => "NUMLESS",
    LessEq => "NUMLESSEQ",
    Add => "SUM",
    Subtract => "SUBTRACTION",
    Multiply => "PRODUCT",
    Divide => "DIVISION",
    Negate => "NEGATION" | "INVERTSIGN",
    Abs => "ABS",
    Max => "MAX",
    Min => "MIN",
    And => "AND",
    Or => "OR",
    Not => "NOT",
    IsIri => "isIri" as "ISIRI",
    IsNumeric => "isNumeric" as "ISNUMERIC",
    StringLength => "STRLEN",
    UpperCase => "UCASE",
    LowerCase => "LCASE",
    Contains => "CONTAINS",
    StartsWith => "STRSTARTS",
    EndsWith => "STRENDS",
    Concat => "CONCAT",
    Str => "STR",
    IsString => "isString" as "ISSTRING",
    IsInteger => "isInteger" as "ISINTEGER",
    IsFloat => "isFloat" as "ISFLOAT",
    IsDouble => "isDouble" as "ISDOUBLE",
    IsNull => "isNull" as "ISNULL",
    IsDecimal => "isDecimal" as "ISDECIMAL",
    IsBoolean => "isBoolean" as "ISBOOLEAN",
    IsLangString => "isLangString" as "ISLANGSTRING",
    IsDateTime => "isDateTime" as "ISDATETIME",
    IsDate => "isDate" as "ISDATE",
    IsTime => "isTime" as "ISTIME",
    IsDuration => "isDuration" as "ISDURATION",
    IsDayTimeDuration => "isDayTimeDuration" as "ISDAYTIMEDURATION",
    IsYearMonthDuration => "isYearMonthDuration" as "ISYEARMONTHDURATION",
    Datatype => "DATATYPE",
    FullStr => "fullStr" as "FULLSTR",
    Lang => "LANG",
    StrLang => "STRLANG",
    LangMatches => "langMatches" as "LANGMATCHES",
    StrDt => "STRDT",
    CastInt => "INT",
    CastFloat => "FLOAT",
    CastDouble => "DOUBLE",
    CastIri => "IRI",
    CastDateTime => "DATETIME",
    CastDate => "DATE",
    CastTime => "TIME",
    CastDuration => "DURATION",
    CastDayTimeDuration => "DAYTIMEDURATION",
    CastYearMonthDuration => "YEARMONTHDURATION",
    Round => "ROUND",
    Pow => "POW",
    Log => "LOG",
    Sin => "SIN",
    Sqrt => "SQRT",
    Cos => "COS",
    Tan => "TAN",
    Ceil => "CEIL",
    Floor => "FLOOR",
    Rem => "REM",
    BitAnd => "BITAND",
    BitOr => "BITOR",
    BitXor => "BITXOR",
    BitShl => "BITSHL",
    BitShr => "BITSHR",
    Luka => "LUKA",
    Compare => "COMPARE",
    Levenshtein => "LEVENSHTEIN",
    StrRev => "STRREV",
    Trim => "STRTRIM",
    TrimStart => "STRTRIMSTART",
    TrimEnd => "STRTRIMEND",
    StrBefore => "STRBEFORE",
    StrAfter => "STRAFTER",
    UriEncode => "URIENCODE",
    UriDecode => "URIDECODE",
    Substr => "SUBSTR",
    Regex => "REGEX",
    Replace => "REPLACE",
    Md5 => "MD5",
    Sha1 => "SHA1",
    Sha256 => "SHA256",
    Sha384 => "SHA384",
    Sha512 => "SHA512",
    Year => "YEAR",
    Month => "MONTH",
    Day => "DAY",
    Hours => "HOURS",
    Minutes => "MINUTES",
    Seconds => "SECONDS",
    TzString => "TZ",
    Timezone => "TIMEZONE",
    Adjust => "ADJUST",
    Rand => "RAND",
    Uuid => "UUID",
    StrUuid => "STRUUID",
    Now => "NOW",
    Get => "GET",
    Length => "LENGTH",
    Label => "LABEL",
    Keys => "KEYS",
    HasKey => "HASKEY",
    IsTuple => "isTuple" as "ISTUPLE",
    IsMap => "isMap" as "ISMAP",
    StrJoin => "STRJOIN",
    StrSplit => "STRSPLIT",
}

impl Operator {
    /// Every name that a builtin can be called by.
    ///
    /// These names are reserved, in any case,
    /// so a program cannot use them as names of predicates or functions.
    /// They are the names that term syntax reserves ([BUILTIN_NAMES]).
    pub const NAMES: &'static [&'static str] = BUILTIN_NAMES;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The names that term syntax reserves are the names of the builtins, in their order.
    #[test]
    fn the_reserved_names_are_the_names_of_the_builtins() {
        assert_eq!(Operator::NAMES, Operator::ENTRY_NAMES);
    }

    /// Every name in [NAMES](Operator::NAMES) is found, in any case,
    /// and a builtin is found by the name that it is written with.
    #[test]
    fn every_listed_name_resolves() {
        for name in Operator::NAMES {
            let operator = Operator::from_name(name).expect("a listed name resolves");
            assert_eq!(
                Operator::from_name(&name.to_ascii_lowercase()),
                Some(operator),
                "`{name}` resolves case-insensitively"
            );
            assert_eq!(Operator::from_name(operator.name()), Some(operator));
        }
    }

    /// No name appears twice in [NAMES](Operator::NAMES).
    #[test]
    fn the_listed_names_have_no_repeats() {
        let mut sorted = Operator::NAMES.to_vec();
        sorted.sort_unstable();
        let count = sorted.len();
        sorted.dedup();
        assert_eq!(sorted.len(), count, "a name is listed twice");
    }
}
