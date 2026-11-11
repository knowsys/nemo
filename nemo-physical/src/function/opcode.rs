//! The instruction set of stack programs.
//!
//! Every instruction has a [signature](OpCode::signature),
//! the domains of the operands that it accepts and of the results that it yields.

mod signature;

use crate::{
    function::opcode::signature::{Overload, Signature},
    value::{DomainSet, ValueDomain},
};

/// One instruction of a [StackProgram](super::StackProgram).
///
/// An instruction pops its operands off the stack and pushes one result.
/// Every instruction accepts any operands,
/// and an operand that it cannot use makes the result an evaluation error.
///
/// There is no instruction for inequality.
/// [Not](Self::Not) applied to [Equal](Self::Equal) gives SPARQL's `!=`,
/// since an error propagates through the negation and NaN stays unequal to itself.
/// [Not](Self::Not) applied to [Same](Self::Same) negates the identity.
///
/// A parameter of an instruction is data that varies within one computation:
/// the number of operands of a variadic instruction, whether an optional operand is present,
/// or the domains that a test accepts.
/// Operations that compute differently are separate instructions,
/// such as the digests and the trims.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpCode {
    /// Pushes the program constant with the given index.
    Constant(u16),
    /// Pushes the input with the given index, a value of the row.
    Input(u16),

    /// Pops two values and pushes whether they are the same term.
    ///
    /// This is the equality of joins and of duplicate elimination, and it never yields an error.
    Same,
    /// Pops two values and pushes whether they are equal,
    /// as [value_eq](crate::value::ValueRef::value_eq) decides.
    ///
    /// Unlike for [Same](Self::Same), numbers are equal if they have the same value,
    /// so that the integer `1` equals the double `1.0d`.
    Equal,
    /// Pops two values and pushes whether the first is smaller.
    ///
    /// The order is the one of [value_cmp](crate::value::ValueRef::value_cmp).
    /// If it is undefined, as for NaN, the result is false.
    Less,
    /// Pops two values and pushes whether the first is smaller or equal, as for [Less](Self::Less).
    LessOrEqual,
    /// Pops two values and pushes whether the first is greater, as for [Less](Self::Less).
    Greater,
    /// Pops two values and pushes whether the first is greater or equal, as for [Less](Self::Less).
    GreaterOrEqual,
    /// Pops two values and pushes `-1`, `0` or `1`,
    /// as the first is smaller than, equal to, or greater than the second.
    ///
    /// The order is the one of [Less](Self::Less).
    /// If it is undefined, as for NaN,
    /// the result is an [Incomparable](crate::value::EvaluationError::Incomparable) error.
    Compare,
    /// Pops the given number of numbers and pushes the largest,
    /// in the smallest numeric type that holds all of them.
    Maximum(u16),
    /// Pops the given number of numbers and pushes the smallest,
    /// in the smallest numeric type that holds all of them.
    Minimum(u16),

    /// Pops two numbers, two durations, or a point in time and a duration,
    /// and pushes their sum.
    Add,
    /// Pops two numbers, two durations, a point in time and a duration, or two points in time,
    /// and pushes their difference.
    ///
    /// The difference of two points in time is a dayTimeDuration.
    Subtract,
    /// Pops two numbers, or a duration and a number, and pushes their product.
    Multiply,
    /// Pops two numbers, a duration and a number, or two durations, and pushes their quotient.
    ///
    /// The quotient of two integers is a decimal, as in SPARQL, rather than a truncated integer.
    /// The quotient of two durations is an exact number.
    Divide,
    /// Pops a number and pushes its negation.
    Negate,
    /// Pops a number and pushes its absolute value.
    Absolute,
    /// Pops a number and pushes it rounded to the nearest integer value, with halves rounded up.
    ///
    /// A float stays a float and a double a double, and an exact number becomes an integer.
    Round,
    /// Pops a number and pushes it rounded up to the nearest integer value.
    ///
    /// A float stays a float and a double a double, and an exact number becomes an integer.
    Ceiling,
    /// Pops a number and pushes it rounded down to the nearest integer value.
    ///
    /// A float stays a float and a double a double, and an exact number becomes an integer.
    Floor,
    /// Pops two numbers and pushes the first raised to the second.
    Power,
    /// Pops a number and a base, and pushes the logarithm of the number to that base.
    Logarithm,
    /// Pops an angle in radians and pushes its sine.
    Sine,
    /// Pops an angle in radians and pushes its cosine.
    Cosine,
    /// Pops an angle in radians and pushes its tangent.
    Tangent,
    /// Pops a number and pushes its square root.
    SquareRoot,
    /// Pops two integers and pushes the remainder of their division,
    /// which has the sign of the first.
    Remainder,
    /// Pops two integers and pushes the first shifted left by the second.
    ShiftLeft,
    /// Pops two integers and pushes the first shifted right by the second, keeping its sign.
    ShiftRight,
    /// Pops the given number of integers and pushes their bitwise and.
    BitAnd(u16),
    /// Pops the given number of integers and pushes their bitwise or.
    BitOr(u16),
    /// Pops the given number of integers and pushes their bitwise xor.
    BitXor(u16),
    /// Pops the given number of numbers and pushes their Łukasiewicz t-norm.
    Lukasiewicz(u16),

    /// Pops two values and pushes the conjunction of their effective boolean values,
    /// in three-valued logic.
    And,
    /// Pops two values and pushes the disjunction of their effective boolean values,
    /// in three-valued logic.
    Or,
    /// Pops a value and pushes the negation of its effective boolean value.
    Not,

    /// Pops a value and pushes whether its domain is in the set.
    InDomains(DomainSet),
    /// Pops a value and pushes its datatype IRI.
    ///
    /// Only a literal has one.
    Datatype,
    /// Pops a value and pushes it in canonical term syntax, as a plain string.
    ///
    /// The string holds what the value's [Display](std::fmt::Display) writes.
    TermSyntax,
    /// Pops a value and pushes its lexical form as a plain string.
    ///
    /// A language string yields its text.
    Lexical,
    /// Pops a literal and pushes its language tag, or the empty string if it has none.
    Language,
    /// Pops a plain string and a tag, and pushes the string tagged with that language.
    StringLanguage,
    /// Pops a language tag and a language range, and pushes whether the tag matches the range.
    ///
    /// A range matches the tags that start with it, ignoring case and up to whole subtags,
    /// and `*` matches every nonempty tag.
    LanguageMatches,
    /// Pops a lexical form and a datatype IRI, and pushes the typed literal.
    TypedLiteral,
    /// Pops a value and pushes it cast to an integer.
    CastInteger,
    /// Pops a value and pushes it cast to a 32-bit float.
    CastFloat,
    /// Pops a value and pushes it cast to a 64-bit float.
    CastDouble,
    /// Pops a string or IRI and pushes it as an IRI.
    CastIri,
    /// Pops a value and pushes it cast to a dateTime.
    CastDateTime,
    /// Pops a value and pushes it cast to a date.
    CastDate,
    /// Pops a value and pushes it cast to a time.
    CastTime,
    /// Pops a value and pushes it cast to a duration.
    CastDuration,
    /// Pops a value and pushes it cast to a dayTimeDuration.
    CastDayTimeDuration,
    /// Pops a value and pushes it cast to a yearMonthDuration.
    CastYearMonthDuration,

    /// Pops a string and pushes its length in characters.
    StringLength,
    /// Pops a string and pushes it in upper case.
    UpperCase,
    /// Pops a string and pushes it in lower case.
    LowerCase,
    /// Pops a string and pushes it reversed.
    Reverse,
    /// Pops a string and pushes it with surrounding whitespace removed.
    Trim,
    /// Pops a string and pushes it with leading whitespace removed.
    TrimStart,
    /// Pops a string and pushes it with trailing whitespace removed.
    TrimEnd,
    /// Pops two strings and pushes whether the first contains the second.
    Contains,
    /// Pops two strings and pushes whether the first starts with the second.
    StartsWith,
    /// Pops two strings and pushes whether the first ends with the second.
    EndsWith,
    /// Pops two strings and pushes the part of the first before the first occurrence of the second.
    StringBefore,
    /// Pops two strings and pushes the part of the first after the first occurrence of the second.
    StringAfter,
    /// Pops a string and a 1-based start and pushes the substring from there.
    Substring,
    /// Pops a string, a 1-based start, and a length and pushes that substring.
    SubstringWith,
    /// Pops the given number of strings and pushes their concatenation.
    Concat(u16),
    /// Pops two strings and pushes their Levenshtein distance.
    Levenshtein,
    /// Pops a string and pushes it percent-encoded.
    UriEncode,
    /// Pops a string and pushes it percent-decoded.
    UriDecode,
    /// Pops a string, a pattern and possibly flags,
    /// and pushes whether the pattern matches somewhere in the string.
    Matches {
        /// Whether a flags operand is popped too.
        flags: bool,
    },
    /// Pops a string, a pattern, a replacement and possibly flags,
    /// and pushes the string with every match of the pattern replaced.
    Replace {
        /// Whether a flags operand is popped too.
        flags: bool,
    },
    /// Pops a string and pushes its MD5 digest in lowercase hex.
    Md5,
    /// Pops a string and pushes its SHA-1 digest in lowercase hex.
    Sha1,
    /// Pops a string and pushes its SHA-256 digest in lowercase hex.
    Sha256,
    /// Pops a string and pushes its SHA-384 digest in lowercase hex.
    Sha384,
    /// Pops a string and pushes its SHA-512 digest in lowercase hex.
    Sha512,

    /// Pops the given number of values and pushes them as a tuple,
    /// in the order they were pushed.
    Tuple(u16),
    /// Pops the given number of values and the label below them,
    /// and pushes the values as a tuple with that label.
    LabeledTuple(u16),
    /// Pops the given number of key-value pairs, each key pushed before its value,
    /// and pushes them as a map.
    ///
    /// A key given twice is a [Malformed](crate::value::EvaluationError::Malformed) error.
    Map(u16),
    /// Pops the given number of key-value pairs and the label below them,
    /// and pushes the pairs as a map with that label.
    LabeledMap(u16),
    /// Pops a structure and a key,
    /// and pushes the tuple's element at the 0-based integer index, or the map's value at the key.
    ///
    /// An absent component is a [Missing](crate::value::EvaluationError::Missing) error.
    Get,
    /// Pops a structure and pushes its number of elements or pairs.
    Length,
    /// Pops a structure and pushes its label as an IRI,
    /// or the empty string if it has none.
    Label,
    /// Pops a map and pushes its keys as a tuple, in term order.
    Keys,
    /// Pops a map and a key, and pushes whether the map has the key.
    HasKey,
    /// Pops a tuple of plain strings and a separator, and pushes the strings joined by it.
    JoinStrings,
    /// Pops a plain string and a separator,
    /// and pushes the tuple of the parts between the occurrences of the separator.
    ///
    /// The parts before the first and after the last occurrence are included, even if empty.
    SplitString,

    /// Pops a dateTime, date or duration and pushes its year,
    /// or the whole years of a duration.
    Year,
    /// Pops a dateTime, date or duration and pushes its month,
    /// or the months of a duration beyond its whole years.
    Month,
    /// Pops a dateTime, date or duration and pushes its day,
    /// or the whole days of a duration.
    Day,
    /// Pops a dateTime, time or duration and pushes its hours.
    Hours,
    /// Pops a dateTime, time or duration and pushes its minutes.
    Minutes,
    /// Pops a dateTime, time or duration and pushes its seconds,
    /// an exact number that may have a fraction.
    Seconds,
    /// Pops a dateTime, date or time and pushes its timezone as a plain string,
    /// or the empty string if it has none.
    TimezoneString,
    /// Pops a dateTime, date or time and pushes its timezone as a dayTimeDuration.
    ///
    /// A value without a timezone is a [Missing](crate::value::EvaluationError::Missing) error.
    TimezoneDuration,
    /// Pops a dateTime, date or time and a dayTimeDuration,
    /// and pushes the value adjusted to the timezone that the duration gives.
    Adjust,

    /// Pushes a random double in `[0, 1)`.
    Random,
    /// Pushes a fresh UUID as a `urn:uuid:` IRI.
    UuidIri,
    /// Pushes a fresh UUID as a plain string.
    UuidString,
    /// Pushes the instant of the [evaluation context](crate::function::EvaluationContext)
    /// as a dateTime, SPARQL's [`NOW`](https://www.w3.org/TR/sparql12-query/#func-now).
    ///
    /// The instant is the same for every evaluation in one context.
    Now,
}

impl OpCode {
    /// Returns the [Signature] of the instruction, the domains that it accepts and yields.
    pub(crate) fn signature(self) -> Signature {
        use DomainSet as D;
        use Overload::{Keeps, Plain, Promoted, Shared};
        use Signature::{Each, Fixed, Lenient, Promoting, Push};

        match self {
            OpCode::Constant(_) | OpCode::Input(_) => Push,

            OpCode::Same => Fixed(&[Plain(&[D::ANY, D::ANY], D::BOOLEAN)]),
            // Values of one domain compare with each other.
            // Numbers also compare across their domains, and so do durations.
            // An IRI or a null is unequal to a value of any other domain.
            OpCode::Equal => Fixed(&[
                Shared(&[D::ANY, D::ANY], D::BOOLEAN),
                Plain(&[D::NUMERIC, D::NUMERIC], D::BOOLEAN),
                Plain(&[D::DURATION, D::DURATION], D::BOOLEAN),
                Plain(&[IRI_OR_NULL, D::ANY], D::BOOLEAN),
                Plain(&[D::ANY, IRI_OR_NULL], D::BOOLEAN),
            ]),
            OpCode::Less | OpCode::LessOrEqual | OpCode::Greater | OpCode::GreaterOrEqual => {
                Fixed(&[
                    Plain(&[D::NUMERIC, D::NUMERIC], D::BOOLEAN),
                    Plain(&[D::ORDERED_DURATION, D::ORDERED_DURATION], D::BOOLEAN),
                    Shared(&[ORDERED, ORDERED], D::BOOLEAN),
                ])
            }
            OpCode::Compare => Fixed(&[
                Plain(&[D::NUMERIC, D::NUMERIC], D::INTEGER),
                Plain(&[D::ORDERED_DURATION, D::ORDERED_DURATION], D::INTEGER),
                Shared(&[ORDERED, ORDERED], D::INTEGER),
            ]),
            OpCode::Maximum(count) | OpCode::Minimum(count) => Promoting {
                count: usize::from(count),
            },

            // A point in time moves by a duration and keeps its domain.
            // Two points in time of one domain are a dayTimeDuration apart.
            OpCode::Add => Fixed(&[
                Promoted { integral: true },
                Plain(&[D::DURATION, D::DURATION], D::ORDERED_DURATION),
                Keeps(&[D::TEMPORAL, D::DURATION], 0),
                Keeps(&[D::DURATION, D::TEMPORAL], 1),
            ]),
            OpCode::Subtract => Fixed(&[
                Promoted { integral: true },
                Plain(&[D::DURATION, D::DURATION], D::ORDERED_DURATION),
                Keeps(&[D::TEMPORAL, D::DURATION], 0),
                Shared(&[D::TEMPORAL, D::TEMPORAL], D::DAY_TIME_DURATION),
            ]),
            OpCode::Multiply => Fixed(&[
                Promoted { integral: true },
                Plain(&[D::DURATION, D::NUMERIC], D::ORDERED_DURATION),
                Plain(&[D::NUMERIC, D::DURATION], D::ORDERED_DURATION),
            ]),
            OpCode::Divide => Fixed(&[
                Promoted { integral: false },
                Plain(&[D::DURATION, D::NUMERIC], D::ORDERED_DURATION),
                Plain(&[D::DURATION, D::DURATION], D::EXACT),
            ]),
            OpCode::Negate | OpCode::Absolute => Fixed(&[Keeps(&[D::NUMERIC], 0)]),
            // An exact number rounds to an integer, a float or a double within its domain.
            OpCode::Round | OpCode::Ceiling | OpCode::Floor => {
                Fixed(&[Plain(&[D::EXACT], D::INTEGER), Keeps(&[FLOATING], 0)])
            }
            // A float stays a float, and every other number computes as a double.
            OpCode::Sine | OpCode::Cosine | OpCode::Tangent | OpCode::SquareRoot => Fixed(&[
                Plain(&[D::FLOAT], D::FLOAT),
                Plain(&[EXACT_OR_DOUBLE], D::DOUBLE),
            ]),
            // An exact base with a non-negative integer exponent stays exact,
            // and every other power is a double.
            OpCode::Power => Fixed(&[
                Plain(&[D::INTEGER, INTEGER_OR_FLOATING], INTEGER_OR_DOUBLE),
                Plain(&[D::INTEGER, D::DECIMAL], D::DOUBLE),
                Plain(&[D::DECIMAL, INTEGER_OR_FLOATING], EXACT_OR_DOUBLE),
                Plain(&[D::DECIMAL, D::DECIMAL], D::DOUBLE),
                Plain(&[FLOATING, D::NUMERIC], D::DOUBLE),
            ]),
            OpCode::Logarithm => Fixed(&[Plain(&[D::NUMERIC, D::NUMERIC], D::DOUBLE)]),
            OpCode::Remainder | OpCode::ShiftLeft | OpCode::ShiftRight => {
                Fixed(&[Plain(&[D::INTEGER, D::INTEGER], D::INTEGER)])
            }
            OpCode::BitAnd(count) | OpCode::BitOr(count) | OpCode::BitXor(count) => Each {
                leading: &[],
                count: usize::from(count),
                operand: D::INTEGER,
                result: D::INTEGER,
            },
            OpCode::Lukasiewicz(count) => Each {
                leading: &[],
                count: usize::from(count),
                operand: D::NUMERIC,
                result: D::DOUBLE,
            },

            OpCode::And | OpCode::Or => Lenient {
                count: 2,
                operand: D::TRUTHY,
                result: D::BOOLEAN,
            },
            OpCode::Not => Fixed(&[Plain(&[D::TRUTHY], D::BOOLEAN)]),

            OpCode::InDomains(_) => Fixed(&[Plain(&[D::ANY], D::BOOLEAN)]),
            OpCode::Datatype => Fixed(&[Plain(&[D::LITERAL], D::IRI)]),
            OpCode::TermSyntax | OpCode::Lexical => Fixed(&[Plain(&[D::ANY], D::STRING)]),
            OpCode::Language => Fixed(&[Plain(&[D::LITERAL], D::STRING)]),
            OpCode::StringLanguage => Fixed(&[Plain(&[D::STRING, D::STRING], D::LANGUAGE_STRING)]),
            OpCode::LanguageMatches => Fixed(&[Plain(&[D::STRING, D::STRING], D::BOOLEAN)]),
            // The datatype decides the result's domain.
            OpCode::TypedLiteral => Fixed(&[Plain(&[D::ANY, D::ANY], D::ANY)]),
            // A cast also parses lexical forms,
            // so whether it succeeds depends on the value, not only on its domain.
            OpCode::CastInteger => Fixed(&[Plain(&[D::ANY], D::INTEGER)]),
            OpCode::CastFloat => Fixed(&[Plain(&[D::ANY], D::FLOAT)]),
            OpCode::CastDouble => Fixed(&[Plain(&[D::ANY], D::DOUBLE)]),
            OpCode::CastIri => Fixed(&[Plain(&[STRINGLIKE_OR_IRI], D::IRI)]),
            OpCode::CastDateTime => Fixed(&[Plain(&[D::ANY], D::DATE_TIME)]),
            OpCode::CastDate => Fixed(&[Plain(&[D::ANY], D::DATE)]),
            OpCode::CastTime => Fixed(&[Plain(&[D::ANY], D::TIME)]),
            OpCode::CastDuration => Fixed(&[Plain(&[D::ANY], D::DURATION)]),
            OpCode::CastDayTimeDuration => Fixed(&[Plain(&[D::ANY], D::DAY_TIME_DURATION)]),
            // The result is a yearMonthDuration,
            // except for the zero duration, which is a dayTimeDuration.
            OpCode::CastYearMonthDuration => Fixed(&[Plain(&[D::ANY], D::ORDERED_DURATION)]),

            OpCode::StringLength => Fixed(&[Plain(&[D::STRINGLIKE], D::INTEGER)]),
            // A transformed string keeps its language tag.
            OpCode::UpperCase
            | OpCode::LowerCase
            | OpCode::Reverse
            | OpCode::Trim
            | OpCode::TrimStart
            | OpCode::TrimEnd => Fixed(&[Keeps(&[D::STRINGLIKE], 0)]),
            // The second string is plain or has the tag of the first.
            OpCode::Contains | OpCode::StartsWith | OpCode::EndsWith => Fixed(&[
                Plain(&[D::STRING, D::STRING], D::BOOLEAN),
                Plain(&[D::LANGUAGE_STRING, D::STRINGLIKE], D::BOOLEAN),
            ]),
            // A part of a tagged string is tagged,
            // but if the second string does not occur, the result is a plain empty string.
            OpCode::StringBefore | OpCode::StringAfter => Fixed(&[
                Plain(&[D::STRING, D::STRING], D::STRING),
                Plain(&[D::LANGUAGE_STRING, D::STRINGLIKE], D::STRINGLIKE),
            ]),
            OpCode::Substring => Fixed(&[Keeps(&[D::STRINGLIKE, D::INTEGER], 0)]),
            OpCode::SubstringWith => Fixed(&[Keeps(&[D::STRINGLIKE, D::INTEGER, D::INTEGER], 0)]),
            OpCode::Concat(count) => Each {
                leading: &[],
                count: usize::from(count),
                operand: D::STRINGLIKE,
                result: D::STRINGLIKE,
            },
            OpCode::Levenshtein => Fixed(&[Plain(&[D::STRINGLIKE, D::STRINGLIKE], D::INTEGER)]),
            OpCode::UriEncode | OpCode::UriDecode => Fixed(&[Plain(&[D::STRINGLIKE], D::STRING)]),
            OpCode::Matches { flags: false } => {
                Fixed(&[Plain(&[D::STRINGLIKE, D::STRINGLIKE], D::BOOLEAN)])
            }
            OpCode::Matches { flags: true } => {
                Fixed(&[Plain(&[D::STRINGLIKE, D::STRINGLIKE, D::ANY], D::BOOLEAN)])
            }
            OpCode::Replace { flags: false } => {
                Fixed(&[Keeps(&[D::STRINGLIKE, D::STRINGLIKE, D::STRINGLIKE], 0)])
            }
            OpCode::Replace { flags: true } => Fixed(&[Keeps(
                &[D::STRINGLIKE, D::STRINGLIKE, D::STRINGLIKE, D::ANY],
                0,
            )]),
            OpCode::Md5 | OpCode::Sha1 | OpCode::Sha256 | OpCode::Sha384 | OpCode::Sha512 => {
                Fixed(&[Plain(&[D::STRINGLIKE], D::STRING)])
            }

            OpCode::Tuple(count) => Each {
                leading: &[],
                count: usize::from(count),
                operand: D::ANY,
                result: D::TUPLE,
            },
            // The label of a labeled structure lies below its content on the stack.
            OpCode::LabeledTuple(count) => Each {
                leading: &[STRINGLIKE_OR_IRI],
                count: usize::from(count),
                operand: D::ANY,
                result: D::TUPLE,
            },
            // A map's content is two values per pair.
            OpCode::Map(count) => Each {
                leading: &[],
                count: 2 * usize::from(count),
                operand: D::ANY,
                result: D::MAP,
            },
            OpCode::LabeledMap(count) => Each {
                leading: &[STRINGLIKE_OR_IRI],
                count: 2 * usize::from(count),
                operand: D::ANY,
                result: D::MAP,
            },
            OpCode::Get => Fixed(&[
                Plain(&[D::TUPLE, D::INTEGER], D::ANY),
                Plain(&[D::MAP, D::ANY], D::ANY),
            ]),
            OpCode::Length => Fixed(&[Plain(&[D::STRUCTURED], D::INTEGER)]),
            OpCode::Label => Fixed(&[Plain(&[D::STRUCTURED], IRI_OR_STRING)]),
            OpCode::Keys => Fixed(&[Plain(&[D::MAP], D::TUPLE)]),
            OpCode::HasKey => Fixed(&[Plain(&[D::MAP, D::ANY], D::BOOLEAN)]),
            OpCode::JoinStrings => Fixed(&[Plain(&[D::TUPLE, D::STRING], D::STRING)]),
            OpCode::SplitString => Fixed(&[Plain(&[D::STRING, D::STRING], D::TUPLE)]),

            OpCode::Year | OpCode::Month | OpCode::Day | OpCode::Hours | OpCode::Minutes => {
                Fixed(&[Plain(&[TEMPORAL_OR_DURATION], D::INTEGER)])
            }
            OpCode::Seconds => Fixed(&[Plain(&[TEMPORAL_OR_DURATION], D::EXACT)]),
            OpCode::TimezoneString => Fixed(&[Plain(&[D::TEMPORAL], D::STRING)]),
            OpCode::TimezoneDuration => Fixed(&[Plain(&[D::TEMPORAL], D::DAY_TIME_DURATION)]),
            OpCode::Adjust => Fixed(&[Keeps(&[D::TEMPORAL, D::DAY_TIME_DURATION], 0)]),

            OpCode::Random => Fixed(&[Plain(&[], D::DOUBLE)]),
            OpCode::UuidIri => Fixed(&[Plain(&[], D::IRI)]),
            OpCode::UuidString => Fixed(&[Plain(&[], D::STRING)]),
            OpCode::Now => Fixed(&[Plain(&[], D::DATE_TIME)]),
        }
    }

    /// Returns how many values the instruction pops, as its [signature](Self::signature) states.
    ///
    /// Every instruction pushes one value.
    pub(crate) fn pops(self) -> usize {
        self.signature().pops()
    }

    /// Returns whether the instruction's result is not determined by its operands.
    ///
    /// This is the case for the instructions that draw a random number or a UUID,
    /// and for [Now](Self::Now), which reads the evaluation context.
    pub(crate) fn is_nondeterministic(self) -> bool {
        matches!(
            self,
            OpCode::Random | OpCode::UuidIri | OpCode::UuidString | OpCode::Now
        )
    }
}

// The unions of domains that the signatures use beyond those of `DomainSet`.

/// The domains whose values can be ordered against other values of the same domain.
///
/// They are the plain strings, booleans, dateTimes, dates, times and tuples.
/// Numbers and durations can also be ordered across their domains.
const ORDERED: DomainSet = DomainSet::STRING
    .with(ValueDomain::Boolean)
    .with(ValueDomain::DateTime)
    .with(ValueDomain::Date)
    .with(ValueDomain::Time)
    .with(ValueDomain::Tuple);

/// The floats and the doubles.
const FLOATING: DomainSet = DomainSet::FLOAT.union(DomainSet::DOUBLE);

/// The integers, the floats and the doubles.
const INTEGER_OR_FLOATING: DomainSet = DomainSet::INTEGER.union(FLOATING);

/// The integers and the doubles.
const INTEGER_OR_DOUBLE: DomainSet = DomainSet::INTEGER.union(DomainSet::DOUBLE);

/// The exact numbers and the doubles.
const EXACT_OR_DOUBLE: DomainSet = DomainSet::EXACT.union(DomainSet::DOUBLE);

/// The IRIs and the nulls, which [Equal](OpCode::Equal) finds unequal to a value of any other
/// domain.
const IRI_OR_NULL: DomainSet = DomainSet::IRI.with(ValueDomain::Null);

/// The strings, plain and language-tagged, and the IRIs.
const STRINGLIKE_OR_IRI: DomainSet = DomainSet::STRINGLIKE.with(ValueDomain::Iri);

/// The IRIs and the plain strings.
const IRI_OR_STRING: DomainSet = DomainSet::IRI.with(ValueDomain::String);

/// The points in time and the durations.
const TEMPORAL_OR_DURATION: DomainSet = DomainSet::TEMPORAL.union(DomainSet::DURATION);
