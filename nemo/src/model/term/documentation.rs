//! What the builtins do, as an editor shows it to someone writing a program:
//! the ways to call each, and what it computes.

use crate::model::{AggregateKind, Operator};

/// What a builtin operator or aggregate does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuiltinDocumentation {
    /// The ways to call it, one per line, in the type syntax of declarations:
    /// `STRLEN(text: string) -> int`, or `number + number -> number` for an infix operator.
    ///
    /// A trailing `…` repeats the argument before it.
    pub signatures: &'static [&'static str],
    /// What it computes, in a sentence or two.
    pub summary: &'static str,
}

/// Returns the documentation of a builtin.
const fn doc(signatures: &'static [&'static str], summary: &'static str) -> BuiltinDocumentation {
    BuiltinDocumentation {
        signatures,
        summary,
    }
}

impl Operator {
    /// Returns what the builtin does, as an editor shows it.
    pub fn documentation(&self) -> BuiltinDocumentation {
        match self {
            Operator::Equal => doc(
                &["any = any -> bool"],
                "Whether two values are the same term. \
                 Values of different types are different terms, never an error.",
            ),
            Operator::Unequal => doc(
                &["any != any -> bool"],
                "Whether two values are different terms.",
            ),
            Operator::ValueEqual => doc(
                &["any == any -> bool"],
                "Whether two values are equal as values: numbers compare across their types \
                 (`1 == 1.0`), points in time by their instant, and durations by their length. \
                 Comparing values of types without a common order is an error.",
            ),
            Operator::ValueUnequal => doc(
                &["any !== any -> bool"],
                "Whether two values are unequal as values, the negation of `==`.",
            ),
            Operator::Greater => doc(
                &["number > number -> bool", "string > string -> bool"],
                "Whether the first value is greater than the second. \
                 Numbers compare across their types, and so do durations; \
                 strings, booleans, points in time and tuples compare within their type.",
            ),
            Operator::GreaterEq => doc(
                &["number >= number -> bool", "string >= string -> bool"],
                "Whether the first value is greater than or equal to the second, \
                 in the order of `>`.",
            ),
            Operator::Less => doc(
                &["number < number -> bool", "string < string -> bool"],
                "Whether the first value is less than the second, in the order of `>`.",
            ),
            Operator::LessEq => doc(
                &["number <= number -> bool", "string <= string -> bool"],
                "Whether the first value is less than or equal to the second, \
                 in the order of `>`.",
            ),
            Operator::Add => doc(
                &[
                    "number + number -> number",
                    "duration + duration -> duration",
                    "datetime + duration -> datetime",
                ],
                "The sum. Two integers sum to an integer, and other numbers to the wider type. \
                 A duration moves a point in time. `SUM(…)` adds any number of arguments.",
            ),
            Operator::Subtract => doc(
                &[
                    "number - number -> number",
                    "duration - duration -> duration",
                    "datetime - duration -> datetime",
                    "datetime - datetime -> daytimeduration",
                ],
                "The difference. Two points in time are a duration apart.",
            ),
            Operator::Multiply => doc(
                &["number * number -> number", "duration * number -> duration"],
                "The product. `PRODUCT(…)` multiplies any number of arguments.",
            ),
            Operator::Divide => doc(
                &[
                    "number / number -> number",
                    "duration / number -> duration",
                    "duration / duration -> decimal",
                ],
                "The quotient. Integers divide exactly, into a decimal. \
                 Dividing by zero is an error.",
            ),
            Operator::Negate => doc(
                &["NEGATION(x: number) -> number"],
                "The number with its sign flipped.",
            ),
            Operator::Abs => doc(&["ABS(x: number) -> number"], "The absolute value."),
            Operator::Max => doc(
                &["MAX(x: number, …) -> number"],
                "The largest argument, as the type all the arguments promote to.",
            ),
            Operator::Min => doc(
                &["MIN(x: number, …) -> number"],
                "The smallest argument, as the type all the arguments promote to.",
            ),
            Operator::And => doc(
                &["AND(a: bool, …) -> bool"],
                "Whether every argument is true. \
                 A false argument decides the result even if another is an error.",
            ),
            Operator::Or => doc(
                &["OR(a: bool, …) -> bool"],
                "Whether some argument is true. \
                 A true argument decides the result even if another is an error.",
            ),
            Operator::Not => doc(&["NOT(a: bool) -> bool"], "The negation."),
            Operator::IsIri => doc(&["isIri(x: any) -> bool"], "Whether the value is an IRI."),
            Operator::IsNumeric => doc(
                &["isNumeric(x: any) -> bool"],
                "Whether the value is a number of any type.",
            ),
            Operator::StringLength => doc(
                &["STRLEN(text: string) -> int"],
                "The number of characters of the string.",
            ),
            Operator::UpperCase => doc(
                &["UCASE(text: string) -> string"],
                "The string in upper case. A language-tagged string keeps its tag.",
            ),
            Operator::LowerCase => doc(
                &["LCASE(text: string) -> string"],
                "The string in lower case. A language-tagged string keeps its tag.",
            ),
            Operator::Contains => doc(
                &["CONTAINS(text: string, part: string) -> bool"],
                "Whether the string contains the part.",
            ),
            Operator::StartsWith => doc(
                &["STRSTARTS(text: string, prefix: string) -> bool"],
                "Whether the string starts with the prefix.",
            ),
            Operator::EndsWith => doc(
                &["STRENDS(text: string, suffix: string) -> bool"],
                "Whether the string ends with the suffix.",
            ),
            Operator::Concat => doc(
                &["CONCAT(text: string, …) -> string"],
                "The strings joined end to end.",
            ),
            Operator::Str => doc(
                &["STR(x: any) -> string"],
                "The lexical form of the value as a plain string: \
                 an IRI without its brackets, a literal without its datatype.",
            ),
            Operator::IsString => doc(
                &["isString(x: any) -> bool"],
                "Whether the value is a plain string.",
            ),
            Operator::IsInteger => doc(
                &["isInteger(x: any) -> bool"],
                "Whether the value is an integer.",
            ),
            Operator::IsFloat => doc(
                &["isFloat(x: any) -> bool"],
                "Whether the value is a 32-bit float.",
            ),
            Operator::IsDouble => doc(
                &["isDouble(x: any) -> bool"],
                "Whether the value is a 64-bit double.",
            ),
            Operator::IsNull => doc(
                &["isNull(x: any) -> bool"],
                "Whether the value is a null: a value an existential rule invented, \
                 or a blank node.",
            ),
            Operator::IsDecimal => doc(
                &["isDecimal(x: any) -> bool"],
                "Whether the value is a decimal number. Integers are decimals.",
            ),
            Operator::IsBoolean => doc(
                &["isBoolean(x: any) -> bool"],
                "Whether the value is a boolean.",
            ),
            Operator::IsLangString => doc(
                &["isLangString(x: any) -> bool"],
                "Whether the value is a language-tagged string.",
            ),
            Operator::IsDateTime => doc(
                &["isDateTime(x: any) -> bool"],
                "Whether the value is a dateTime.",
            ),
            Operator::IsDate => doc(&["isDate(x: any) -> bool"], "Whether the value is a date."),
            Operator::IsTime => doc(&["isTime(x: any) -> bool"], "Whether the value is a time."),
            Operator::IsDuration => doc(
                &["isDuration(x: any) -> bool"],
                "Whether the value is a duration of any kind.",
            ),
            Operator::IsDayTimeDuration => doc(
                &["isDayTimeDuration(x: any) -> bool"],
                "Whether the value is a dayTimeDuration: a duration without months.",
            ),
            Operator::IsYearMonthDuration => doc(
                &["isYearMonthDuration(x: any) -> bool"],
                "Whether the value is a yearMonthDuration: a duration of months only.",
            ),
            Operator::Datatype => doc(
                &["DATATYPE(literal: number | string | bool) -> iri"],
                "The datatype IRI of a literal of any type, such as `xsd:integer`.",
            ),
            Operator::FullStr => doc(
                &["fullStr(x: any) -> string"],
                "The value written out in full as a plain string, datatype included.",
            ),
            Operator::Lang => doc(
                &["LANG(text: langstring) -> string"],
                "The language tag of a string, or the empty string for a literal without one.",
            ),
            Operator::StrLang => doc(
                &["STRLANG(text: string, language: string) -> langstring"],
                "The string tagged with the language.",
            ),
            Operator::LangMatches => doc(
                &["langMatches(tag: string, range: string) -> bool"],
                "Whether a language tag matches a language range, as in SPARQL: \
                 `\"en\"` matches `en-GB`, and `\"*\"` matches any tag.",
            ),
            Operator::StrDt => doc(
                &["STRDT(lexical: string, datatype: iri) -> any"],
                "The literal of the lexical form and the datatype. \
                 The datatype decides the type of the result.",
            ),
            Operator::CastInt => doc(
                &["INT(x: any) -> int"],
                "The value as an integer: a number without a fractional part, \
                 or a string that reads as one.",
            ),
            Operator::CastFloat => doc(
                &["FLOAT(x: any) -> float"],
                "The value as a 32-bit float: a number, or a string that reads as one.",
            ),
            Operator::CastDouble => doc(
                &["DOUBLE(x: any) -> double"],
                "The value as a 64-bit double: a number, or a string that reads as one.",
            ),
            Operator::CastIri => doc(&["IRI(text: string) -> iri"], "The string as an IRI."),
            Operator::CastDateTime => doc(
                &["DATETIME(x: any) -> datetime"],
                "The value as a dateTime: a date, or a string that reads as one.",
            ),
            Operator::CastDate => doc(
                &["DATE(x: any) -> date"],
                "The value as a date: the day of a dateTime, or a string that reads as one.",
            ),
            Operator::CastTime => doc(
                &["TIME(x: any) -> time"],
                "The value as a time: the time of day of a dateTime, \
                 or a string that reads as one.",
            ),
            Operator::CastDuration => doc(
                &["DURATION(x: any) -> duration"],
                "The value as a duration: a duration, or a string that reads as one.",
            ),
            Operator::CastDayTimeDuration => doc(
                &["DAYTIMEDURATION(x: any) -> daytimeduration"],
                "The value as a dayTimeDuration: the days and time of a duration, \
                 or a string that reads as one.",
            ),
            Operator::CastYearMonthDuration => doc(
                &["YEARMONTHDURATION(x: any) -> duration"],
                "The value as a yearMonthDuration: the years and months of a duration, \
                 or a string that reads as one. The zero duration is a dayTimeDuration.",
            ),
            Operator::Round => doc(
                &[
                    "ROUND(x: decimal) -> int",
                    "ROUND(x: double) -> double",
                    "ROUND(x: float) -> float",
                ],
                "The number rounded to the nearest integer, halves away from zero. \
                 A decimal becomes an integer, a double or a float stays one.",
            ),
            Operator::Pow => doc(
                &["POW(base: number, exponent: number) -> number"],
                "The base raised to the exponent. \
                 An exact base under a non-negative integer exponent stays exact; \
                 every other power is a double.",
            ),
            Operator::Log => doc(
                &["LOG(x: number, base: number) -> double"],
                "The logarithm of the number in the base.",
            ),
            Operator::Sin => doc(
                &[
                    "SIN(x: decimal | double) -> double",
                    "SIN(x: float) -> float",
                ],
                "The sine of an angle in radians.",
            ),
            Operator::Sqrt => doc(
                &[
                    "SQRT(x: decimal | double) -> double",
                    "SQRT(x: float) -> float",
                ],
                "The square root.",
            ),
            Operator::Cos => doc(
                &[
                    "COS(x: decimal | double) -> double",
                    "COS(x: float) -> float",
                ],
                "The cosine of an angle in radians.",
            ),
            Operator::Tan => doc(
                &[
                    "TAN(x: decimal | double) -> double",
                    "TAN(x: float) -> float",
                ],
                "The tangent of an angle in radians.",
            ),
            Operator::Ceil => doc(
                &[
                    "CEIL(x: decimal) -> int",
                    "CEIL(x: double) -> double",
                    "CEIL(x: float) -> float",
                ],
                "The number rounded up to an integer. \
                 A decimal becomes an integer, a double or a float stays one.",
            ),
            Operator::Floor => doc(
                &[
                    "FLOOR(x: decimal) -> int",
                    "FLOOR(x: double) -> double",
                    "FLOOR(x: float) -> float",
                ],
                "The number rounded down to an integer. \
                 A decimal becomes an integer, a double or a float stays one.",
            ),
            Operator::Rem => doc(
                &["REM(x: int, divisor: int) -> int"],
                "The remainder of dividing the integer by the divisor.",
            ),
            Operator::BitAnd => doc(
                &["BITAND(x: int, …) -> int"],
                "The bitwise and of the integers.",
            ),
            Operator::BitOr => doc(
                &["BITOR(x: int, …) -> int"],
                "The bitwise or of the integers.",
            ),
            Operator::BitXor => doc(
                &["BITXOR(x: int, …) -> int"],
                "The bitwise exclusive or of the integers.",
            ),
            Operator::BitShl => doc(
                &["BITSHL(x: int, bits: int) -> int"],
                "The integer shifted left by the number of bits.",
            ),
            Operator::BitShr => doc(
                &["BITSHR(x: int, bits: int) -> int"],
                "The integer shifted right by the number of bits, keeping its sign.",
            ),
            Operator::Luka => doc(
                &["LUKA(x: number, …) -> double"],
                "The Łukasiewicz t-norm of the arguments: \
                 their sum minus one less than their count, but at least 0.",
            ),
            Operator::Compare => doc(
                &[
                    "COMPARE(a: number, b: number) -> int",
                    "COMPARE(a: string, b: string) -> int",
                ],
                "-1, 0 or 1 as the first value is less than, equal to, \
                 or greater than the second, in the order of `<`.",
            ),
            Operator::Levenshtein => doc(
                &["LEVENSHTEIN(a: string, b: string) -> int"],
                "The edit distance of the strings: the fewest insertions, deletions and \
                 substitutions of single characters that turn one into the other.",
            ),
            Operator::StrRev => doc(
                &["STRREV(text: string) -> string"],
                "The string reversed. A language-tagged string keeps its tag.",
            ),
            Operator::Trim => doc(
                &["STRTRIM(text: string) -> string"],
                "The string without leading and trailing whitespace.",
            ),
            Operator::TrimStart => doc(
                &["STRTRIMSTART(text: string) -> string"],
                "The string without leading whitespace.",
            ),
            Operator::TrimEnd => doc(
                &["STRTRIMEND(text: string) -> string"],
                "The string without trailing whitespace.",
            ),
            Operator::StrBefore => doc(
                &["STRBEFORE(text: string, separator: string) -> string"],
                "The part of the string before the first occurrence of the separator, \
                 or the empty string if it does not occur.",
            ),
            Operator::StrAfter => doc(
                &["STRAFTER(text: string, separator: string) -> string"],
                "The part of the string after the first occurrence of the separator, \
                 or the empty string if it does not occur.",
            ),
            Operator::UriEncode => doc(
                &["URIENCODE(text: string) -> string"],
                "The string percent-encoded for use in a URI.",
            ),
            Operator::UriDecode => doc(
                &["URIDECODE(text: string) -> string"],
                "The string with its percent-encodings decoded.",
            ),
            Operator::Substr => doc(
                &[
                    "SUBSTR(text: string, start: int) -> string",
                    "SUBSTR(text: string, start: int, length: int) -> string",
                ],
                "The part of the string from the 1-based position on, \
                 to its end or of the given length.",
            ),
            Operator::Regex => doc(
                &[
                    "REGEX(text: string, pattern: string) -> bool",
                    "REGEX(text: string, pattern: string, flags: string) -> bool",
                ],
                "Whether the regular expression matches a part of the string. \
                 The flag `i` ignores case, and `q` reads the pattern literally.",
            ),
            Operator::Replace => doc(
                &[
                    "REPLACE(text: string, pattern: string, replacement: string) -> string",
                    "REPLACE(text: string, pattern: string, replacement: string, \
                     flags: string) -> string",
                ],
                "The string with every match of the regular expression replaced. \
                 The replacement refers to groups as `$1`. The flags are those of `REGEX`.",
            ),
            Operator::Md5 => doc(
                &["MD5(text: string) -> string"],
                "The MD5 digest of the string, in hexadecimal.",
            ),
            Operator::Sha1 => doc(
                &["SHA1(text: string) -> string"],
                "The SHA-1 digest of the string, in hexadecimal.",
            ),
            Operator::Sha256 => doc(
                &["SHA256(text: string) -> string"],
                "The SHA-256 digest of the string, in hexadecimal.",
            ),
            Operator::Sha384 => doc(
                &["SHA384(text: string) -> string"],
                "The SHA-384 digest of the string, in hexadecimal.",
            ),
            Operator::Sha512 => doc(
                &["SHA512(text: string) -> string"],
                "The SHA-512 digest of the string, in hexadecimal.",
            ),
            Operator::Year => doc(
                &[
                    "YEAR(x: datetime | date) -> int",
                    "YEAR(x: duration) -> int",
                ],
                "The year of a dateTime or a date, or the whole years of a duration.",
            ),
            Operator::Month => doc(
                &[
                    "MONTH(x: datetime | date) -> int",
                    "MONTH(x: duration) -> int",
                ],
                "The month of a dateTime or a date, \
                 or the months of a duration beyond its whole years.",
            ),
            Operator::Day => doc(
                &["DAY(x: datetime | date) -> int", "DAY(x: duration) -> int"],
                "The day of the month of a dateTime or a date, or the whole days of a duration.",
            ),
            Operator::Hours => doc(
                &[
                    "HOURS(x: datetime | time) -> int",
                    "HOURS(x: duration) -> int",
                ],
                "The hours of a dateTime, a time, or a duration.",
            ),
            Operator::Minutes => doc(
                &[
                    "MINUTES(x: datetime | time) -> int",
                    "MINUTES(x: duration) -> int",
                ],
                "The minutes of a dateTime, a time, or a duration.",
            ),
            Operator::Seconds => doc(
                &[
                    "SECONDS(x: datetime | time) -> decimal",
                    "SECONDS(x: duration) -> decimal",
                ],
                "The seconds of a dateTime, a time, or a duration, with their fraction.",
            ),
            Operator::TzString => doc(
                &["TZ(x: datetime | date | time) -> string"],
                "The timezone as a string, such as `+01:00` or `Z`, \
                 or the empty string for a value without one.",
            ),
            Operator::Timezone => doc(
                &["TIMEZONE(x: datetime | date | time) -> daytimeduration"],
                "The timezone as a duration. A value without a timezone has none to give.",
            ),
            Operator::Adjust => doc(
                &["ADJUST(x: datetime, timezone: daytimeduration) -> datetime"],
                "The point in time in the timezone the duration names.",
            ),
            Operator::Rand => doc(
                &["RAND() -> double"],
                "A random double from 0 up to, but not including, 1, \
                 drawn anew for every row.",
            ),
            Operator::Uuid => doc(&["UUID() -> iri"], "A fresh UUID as a `urn:uuid:` IRI."),
            Operator::StrUuid => doc(&["STRUUID() -> string"], "A fresh UUID as a string."),
            Operator::Now => doc(&["NOW() -> datetime"], "The current instant."),
            Operator::Get => doc(
                &[
                    "GET(structure: tuple, index: int) -> any",
                    "GET(structure: map, key: any) -> any",
                ],
                "The element of a tuple at a 0-based index, or the value of a map at a key. \
                 Also written `?t[0]`.",
            ),
            Operator::Length => doc(
                &["LENGTH(structure: tuple | map) -> int"],
                "The number of elements of a tuple, or of pairs of a map.",
            ),
            Operator::Label => doc(
                &["LABEL(structure: tuple | map) -> iri | string"],
                "The label of a tuple or a map, or the empty string if it has none.",
            ),
            Operator::Keys => doc(
                &["KEYS(structure: map) -> tuple"],
                "The keys of a map, as a tuple.",
            ),
            Operator::HasKey => doc(
                &["HASKEY(structure: map, key: any) -> bool"],
                "Whether the map holds the key.",
            ),
            Operator::IsTuple => doc(
                &["isTuple(x: any) -> bool"],
                "Whether the value is a tuple.",
            ),
            Operator::IsMap => doc(&["isMap(x: any) -> bool"], "Whether the value is a map."),
            Operator::StrJoin => doc(
                &["STRJOIN(parts: tuple, separator: string) -> string"],
                "The strings of a tuple joined by the separator.",
            ),
        }
    }
}

impl AggregateKind {
    /// Returns what the aggregate does, as an editor shows it.
    pub fn documentation(&self) -> BuiltinDocumentation {
        match self {
            AggregateKind::Count => doc(
                &["#count(x: any) -> int", "#count() -> int"],
                "The number of distinct values of the group. \
                 `#count()` counts the body's matches, \
                 and `#count(?x per ?y)` the distinct combinations of `?x` and `?y`.",
            ),
            AggregateKind::Min => doc(
                &["#min(x: any) -> any"],
                "The least value of the group, in the order of `<`.",
            ),
            AggregateKind::Max => doc(
                &["#max(x: any) -> any"],
                "The greatest value of the group, in the order of `<`.",
            ),
            AggregateKind::Sum => doc(
                &["#sum(x: number) -> number"],
                "The sum of the distinct values of the group. \
                 `#sum(?x per ?y)` sums a value once per combination of `?x` and `?y`.",
            ),
            AggregateKind::Average => doc(
                &["#avg(x: number) -> number"],
                "The mean of the distinct values of the group: \
                 their sum divided by their count, as `/` divides. \
                 `#avg(?x per ?y)` counts a value once per combination of `?x` and `?y`.",
            ),
            AggregateKind::Tuple => doc(
                &["#tuple(x: any) -> tuple"],
                "The distinct values of the group as a tuple, in ascending order.",
            ),
            AggregateKind::Map => doc(
                &["#map(key: any = value: any) -> map"],
                "The key–value pairs of the group as a map.",
            ),
        }
    }
}
