//! The builtin operators.

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

    /// Returns the name of the operator in a call, such as `STRLEN`.
    pub fn name(&self) -> &'static str {
        match self {
            Operator::Equal => "EQUALITY",
            Operator::Unequal => "UNEQUALITY",
            Operator::ValueEqual => "VALUEEQUALITY",
            Operator::ValueUnequal => "VALUEUNEQUALITY",
            Operator::Greater => "NUMGREATER",
            Operator::GreaterEq => "NUMGREATEREQ",
            Operator::Less => "NUMLESS",
            Operator::LessEq => "NUMLESSEQ",
            Operator::Add => "SUM",
            Operator::Subtract => "SUBTRACTION",
            Operator::Multiply => "PRODUCT",
            Operator::Divide => "DIVISION",
            Operator::Negate => "NEGATION",
            Operator::Abs => "ABS",
            Operator::Max => "MAX",
            Operator::Min => "MIN",
            Operator::And => "AND",
            Operator::Or => "OR",
            Operator::Not => "NOT",
            Operator::IsIri => "isIri",
            Operator::IsNumeric => "isNumeric",
            Operator::StringLength => "STRLEN",
            Operator::UpperCase => "UCASE",
            Operator::LowerCase => "LCASE",
            Operator::Contains => "CONTAINS",
            Operator::StartsWith => "STRSTARTS",
            Operator::EndsWith => "STRENDS",
            Operator::Concat => "CONCAT",
            Operator::Str => "STR",
            Operator::IsString => "isString",
            Operator::IsInteger => "isInteger",
            Operator::IsFloat => "isFloat",
            Operator::IsDouble => "isDouble",
            Operator::IsNull => "isNull",
            Operator::IsDecimal => "isDecimal",
            Operator::IsBoolean => "isBoolean",
            Operator::IsLangString => "isLangString",
            Operator::IsDateTime => "isDateTime",
            Operator::IsDate => "isDate",
            Operator::IsTime => "isTime",
            Operator::IsDuration => "isDuration",
            Operator::IsDayTimeDuration => "isDayTimeDuration",
            Operator::IsYearMonthDuration => "isYearMonthDuration",
            Operator::Datatype => "DATATYPE",
            Operator::FullStr => "fullStr",
            Operator::Lang => "LANG",
            Operator::StrLang => "STRLANG",
            Operator::LangMatches => "langMatches",
            Operator::StrDt => "STRDT",
            Operator::CastInt => "INT",
            Operator::CastFloat => "FLOAT",
            Operator::CastDouble => "DOUBLE",
            Operator::CastIri => "IRI",
            Operator::CastDateTime => "DATETIME",
            Operator::CastDate => "DATE",
            Operator::CastTime => "TIME",
            Operator::CastDuration => "DURATION",
            Operator::CastDayTimeDuration => "DAYTIMEDURATION",
            Operator::CastYearMonthDuration => "YEARMONTHDURATION",
            Operator::Round => "ROUND",
            Operator::Pow => "POW",
            Operator::Log => "LOG",
            Operator::Sin => "SIN",
            Operator::Sqrt => "SQRT",
            Operator::Cos => "COS",
            Operator::Tan => "TAN",
            Operator::Ceil => "CEIL",
            Operator::Floor => "FLOOR",
            Operator::Rem => "REM",
            Operator::BitAnd => "BITAND",
            Operator::BitOr => "BITOR",
            Operator::BitXor => "BITXOR",
            Operator::BitShl => "BITSHL",
            Operator::BitShr => "BITSHR",
            Operator::Luka => "LUKA",
            Operator::Compare => "COMPARE",
            Operator::Levenshtein => "LEVENSHTEIN",
            Operator::StrRev => "STRREV",
            Operator::Trim => "STRTRIM",
            Operator::TrimStart => "STRTRIMSTART",
            Operator::TrimEnd => "STRTRIMEND",
            Operator::StrBefore => "STRBEFORE",
            Operator::StrAfter => "STRAFTER",
            Operator::UriEncode => "URIENCODE",
            Operator::UriDecode => "URIDECODE",
            Operator::Substr => "SUBSTR",
            Operator::Regex => "REGEX",
            Operator::Replace => "REPLACE",
            Operator::Md5 => "MD5",
            Operator::Sha1 => "SHA1",
            Operator::Sha256 => "SHA256",
            Operator::Sha384 => "SHA384",
            Operator::Sha512 => "SHA512",
            Operator::Year => "YEAR",
            Operator::Month => "MONTH",
            Operator::Day => "DAY",
            Operator::Hours => "HOURS",
            Operator::Minutes => "MINUTES",
            Operator::Seconds => "SECONDS",
            Operator::TzString => "TZ",
            Operator::Timezone => "TIMEZONE",
            Operator::Adjust => "ADJUST",
            Operator::Rand => "RAND",
            Operator::Uuid => "UUID",
            Operator::StrUuid => "STRUUID",
            Operator::Now => "NOW",
            Operator::Get => "GET",
            Operator::Length => "LENGTH",
            Operator::Label => "LABEL",
            Operator::Keys => "KEYS",
            Operator::HasKey => "HASKEY",
            Operator::IsTuple => "isTuple",
            Operator::IsMap => "isMap",
            Operator::StrJoin => "STRJOIN",
            Operator::StrSplit => "STRSPLIT",
        }
    }

    /// Every name that a builtin can be called by, in the order of the variants.
    ///
    /// These names are reserved, in any case,
    /// so a program cannot use them as names of predicates or functions.
    pub const NAMES: &'static [&'static str] = &[
        "EQUALITY",
        "UNEQUALITY",
        "VALUEEQUALITY",
        "VALUEUNEQUALITY",
        "NUMGREATER",
        "NUMGREATEREQ",
        "NUMLESS",
        "NUMLESSEQ",
        "SUM",
        "SUBTRACTION",
        "PRODUCT",
        "DIVISION",
        "NEGATION",
        "INVERTSIGN",
        "ABS",
        "MAX",
        "MIN",
        "AND",
        "OR",
        "NOT",
        "ISIRI",
        "ISNUMERIC",
        "STRLEN",
        "UCASE",
        "LCASE",
        "CONTAINS",
        "STRSTARTS",
        "STRENDS",
        "CONCAT",
        "STR",
        "ISSTRING",
        "ISINTEGER",
        "ISFLOAT",
        "ISDOUBLE",
        "ISNULL",
        "ISDECIMAL",
        "ISBOOLEAN",
        "ISLANGSTRING",
        "ISDATETIME",
        "ISDATE",
        "ISTIME",
        "ISDURATION",
        "ISDAYTIMEDURATION",
        "ISYEARMONTHDURATION",
        "DATATYPE",
        "FULLSTR",
        "LANG",
        "STRLANG",
        "LANGMATCHES",
        "STRDT",
        "INT",
        "FLOAT",
        "DOUBLE",
        "IRI",
        "DATETIME",
        "DATE",
        "TIME",
        "DURATION",
        "DAYTIMEDURATION",
        "YEARMONTHDURATION",
        "ROUND",
        "POW",
        "LOG",
        "SIN",
        "SQRT",
        "COS",
        "TAN",
        "CEIL",
        "FLOOR",
        "REM",
        "BITAND",
        "BITOR",
        "BITXOR",
        "BITSHL",
        "BITSHR",
        "LUKA",
        "COMPARE",
        "LEVENSHTEIN",
        "STRREV",
        "STRTRIM",
        "STRTRIMSTART",
        "STRTRIMEND",
        "STRBEFORE",
        "STRAFTER",
        "URIENCODE",
        "URIDECODE",
        "SUBSTR",
        "REGEX",
        "REPLACE",
        "MD5",
        "SHA1",
        "SHA256",
        "SHA384",
        "SHA512",
        "YEAR",
        "MONTH",
        "DAY",
        "HOURS",
        "MINUTES",
        "SECONDS",
        "TZ",
        "TIMEZONE",
        "ADJUST",
        "RAND",
        "UUID",
        "STRUUID",
        "NOW",
        "GET",
        "LENGTH",
        "LABEL",
        "KEYS",
        "HASKEY",
        "ISTUPLE",
        "ISMAP",
        "STRJOIN",
        "STRSPLIT",
    ];

    /// Returns the operator with the given name in a call, written in any case,
    /// or `None` if there is none.
    ///
    /// The names are exactly those in [NAMES](Self::NAMES).
    /// Other names, like `equal` or `less`, remain free for predicates and function terms.
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name.to_ascii_uppercase().as_str() {
            "EQUALITY" => Operator::Equal,
            "UNEQUALITY" => Operator::Unequal,
            "VALUEEQUALITY" => Operator::ValueEqual,
            "VALUEUNEQUALITY" => Operator::ValueUnequal,
            "NUMGREATER" => Operator::Greater,
            "NUMGREATEREQ" => Operator::GreaterEq,
            "NUMLESS" => Operator::Less,
            "NUMLESSEQ" => Operator::LessEq,
            "SUM" => Operator::Add,
            "SUBTRACTION" => Operator::Subtract,
            "PRODUCT" => Operator::Multiply,
            "DIVISION" => Operator::Divide,
            "NEGATION" | "INVERTSIGN" => Operator::Negate,
            "ABS" => Operator::Abs,
            "MAX" => Operator::Max,
            "MIN" => Operator::Min,
            "AND" => Operator::And,
            "OR" => Operator::Or,
            "NOT" => Operator::Not,
            "ISIRI" => Operator::IsIri,
            "ISNUMERIC" => Operator::IsNumeric,
            "STRLEN" => Operator::StringLength,
            "UCASE" => Operator::UpperCase,
            "LCASE" => Operator::LowerCase,
            "CONTAINS" => Operator::Contains,
            "STRSTARTS" => Operator::StartsWith,
            "STRENDS" => Operator::EndsWith,
            "CONCAT" => Operator::Concat,
            "STR" => Operator::Str,
            "ISSTRING" => Operator::IsString,
            "ISINTEGER" => Operator::IsInteger,
            "ISFLOAT" => Operator::IsFloat,
            "ISDOUBLE" => Operator::IsDouble,
            "ISNULL" => Operator::IsNull,
            "ISDECIMAL" => Operator::IsDecimal,
            "ISBOOLEAN" => Operator::IsBoolean,
            "ISLANGSTRING" => Operator::IsLangString,
            "ISDATETIME" => Operator::IsDateTime,
            "ISDATE" => Operator::IsDate,
            "ISTIME" => Operator::IsTime,
            "ISDURATION" => Operator::IsDuration,
            "ISDAYTIMEDURATION" => Operator::IsDayTimeDuration,
            "ISYEARMONTHDURATION" => Operator::IsYearMonthDuration,
            "DATATYPE" => Operator::Datatype,
            "FULLSTR" => Operator::FullStr,
            "LANG" => Operator::Lang,
            "STRLANG" => Operator::StrLang,
            "LANGMATCHES" => Operator::LangMatches,
            "STRDT" => Operator::StrDt,
            "INT" => Operator::CastInt,
            "FLOAT" => Operator::CastFloat,
            "DOUBLE" => Operator::CastDouble,
            "IRI" => Operator::CastIri,
            "DATETIME" => Operator::CastDateTime,
            "DATE" => Operator::CastDate,
            "TIME" => Operator::CastTime,
            "DURATION" => Operator::CastDuration,
            "DAYTIMEDURATION" => Operator::CastDayTimeDuration,
            "YEARMONTHDURATION" => Operator::CastYearMonthDuration,
            "ROUND" => Operator::Round,
            "POW" => Operator::Pow,
            "LOG" => Operator::Log,
            "SIN" => Operator::Sin,
            "SQRT" => Operator::Sqrt,
            "COS" => Operator::Cos,
            "TAN" => Operator::Tan,
            "CEIL" => Operator::Ceil,
            "FLOOR" => Operator::Floor,
            "REM" => Operator::Rem,
            "BITAND" => Operator::BitAnd,
            "BITOR" => Operator::BitOr,
            "BITXOR" => Operator::BitXor,
            "BITSHL" => Operator::BitShl,
            "BITSHR" => Operator::BitShr,
            "LUKA" => Operator::Luka,
            "COMPARE" => Operator::Compare,
            "LEVENSHTEIN" => Operator::Levenshtein,
            "STRREV" => Operator::StrRev,
            "STRTRIM" => Operator::Trim,
            "STRTRIMSTART" => Operator::TrimStart,
            "STRTRIMEND" => Operator::TrimEnd,
            "STRBEFORE" => Operator::StrBefore,
            "STRAFTER" => Operator::StrAfter,
            "URIENCODE" => Operator::UriEncode,
            "URIDECODE" => Operator::UriDecode,
            "SUBSTR" => Operator::Substr,
            "REGEX" => Operator::Regex,
            "REPLACE" => Operator::Replace,
            "MD5" => Operator::Md5,
            "SHA1" => Operator::Sha1,
            "SHA256" => Operator::Sha256,
            "SHA384" => Operator::Sha384,
            "SHA512" => Operator::Sha512,
            "YEAR" => Operator::Year,
            "MONTH" => Operator::Month,
            "DAY" => Operator::Day,
            "HOURS" => Operator::Hours,
            "MINUTES" => Operator::Minutes,
            "SECONDS" => Operator::Seconds,
            "TZ" => Operator::TzString,
            "TIMEZONE" => Operator::Timezone,
            "ADJUST" => Operator::Adjust,
            "RAND" => Operator::Rand,
            "UUID" => Operator::Uuid,
            "STRUUID" => Operator::StrUuid,
            "NOW" => Operator::Now,
            "GET" => Operator::Get,
            "LENGTH" => Operator::Length,
            "LABEL" => Operator::Label,
            "KEYS" => Operator::Keys,
            "HASKEY" => Operator::HasKey,
            "ISTUPLE" => Operator::IsTuple,
            "ISMAP" => Operator::IsMap,
            "STRJOIN" => Operator::StrJoin,
            "STRSPLIT" => Operator::StrSplit,
            _ => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every name in [NAMES](Operator::NAMES) is found, in any case.
    #[test]
    fn every_listed_name_resolves() {
        // The list and the lookup must agree,
        // so a name that is in one and not in the other is a mistake.
        for name in Operator::NAMES {
            assert!(
                Operator::from_name(name).is_some(),
                "`{name}` is listed but resolves to nothing"
            );
            assert!(
                Operator::from_name(&name.to_ascii_lowercase()).is_some(),
                "`{name}` resolves case-insensitively"
            );
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
