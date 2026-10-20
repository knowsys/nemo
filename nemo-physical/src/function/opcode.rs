//! The instruction set of stack programs, with the [signature](OpCode::signature) of every instruction.

mod signature;

use crate::{
    function::opcode::signature::{Overload, Signature},
    value::{DomainSet, ValueDomain},
};

/// One instruction of a [StackProgram](super::StackProgram).
///
/// An instruction pops its operands off the stack and pushes one result.
/// Every instruction is total over the stack it finds:
/// an operand it cannot use makes the result an evaluation error.
/// There is no inequality instruction:
/// [Not](Self::Not) over [Equal](Self::Equal) is SPARQL's `!=`,
/// since errors propagate through the negation and the NaN stays unequal to itself,
/// and [Not](Self::Not) over [Same](Self::Same) negates the identity.
///
/// A parameter carries what varies as data under one computation:
/// how many operands a fold pops, whether an optional operand is present,
/// which domains a test accepts.
/// Operations that compute differently are instructions of their own,
/// as the digests and the trims are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpCode {
    /// Pushes the program constant with the given index.
    Constant(u16),
    /// Pushes the row input with the given index.
    Input(u16),

    /// Pops two values and pushes whether they are the same term.
    ///
    /// This is the equality of joins and duplicates, and it never errors.
    Same,
    /// Pops two values and pushes whether they are equal:
    /// numerically for numbers, by instant for dateTimes, dates and times,
    /// by length for durations, by term identity otherwise.
    Equal,
    /// Pops two values and pushes whether the first is smaller.
    Less,
    /// Pops two values and pushes whether the first is smaller or equal.
    LessOrEqual,
    /// Pops two values and pushes whether the first is greater.
    Greater,
    /// Pops two values and pushes whether the first is greater or equal.
    GreaterOrEqual,
    /// Pops two values and pushes -1, 0, or 1 by their order.
    Compare,
    /// Pops the given number of numbers and pushes the largest.
    Maximum(u16),
    /// Pops the given number of numbers and pushes the smallest.
    Minimum(u16),

    /// Pops two values and pushes their sum:
    /// of two numbers, of two durations, or of a point in time and a duration.
    Add,
    /// Pops two values and pushes their difference:
    /// of two numbers, of two durations, of a point in time and a duration,
    /// or of two points in time, a duration.
    Subtract,
    /// Pops two values and pushes their product:
    /// of two numbers, or of a duration and a number.
    Multiply,
    /// Pops two values and pushes their quotient:
    /// of two numbers, of a duration and a number, or of two durations, a decimal.
    ///
    /// Exact integers divide into decimals, as in SPARQL.
    Divide,
    /// Pops a number and pushes its negation.
    Negate,
    /// Pops a number and pushes its absolute value.
    Absolute,
    /// Pops a number and pushes it rounded to the nearest integer value,
    /// within its own domain.
    Round,
    /// Pops a number and pushes it rounded up to the nearest integer value,
    /// within its own domain.
    Ceiling,
    /// Pops a number and pushes it rounded down to the nearest integer value,
    /// within its own domain.
    Floor,
    /// Pops two numbers and pushes the first raised to the second.
    Power,
    /// Pops a number and a base and pushes the logarithm.
    Logarithm,
    /// Pops a number and pushes its sine.
    Sine,
    /// Pops a number and pushes its cosine.
    Cosine,
    /// Pops a number and pushes its tangent.
    Tangent,
    /// Pops a number and pushes its square root.
    SquareRoot,
    /// Pops two integers and pushes the remainder of their division.
    Remainder,
    /// Pops two integers and pushes the first shifted left by the second.
    ShiftLeft,
    /// Pops two integers and pushes the first shifted right arithmetically.
    ShiftRight,
    /// Pops the given number of integers and pushes their bitwise and.
    BitAnd(u16),
    /// Pops the given number of integers and pushes their bitwise or.
    BitOr(u16),
    /// Pops the given number of integers and pushes their bitwise xor.
    BitXor(u16),
    /// Pops the given number of numbers and pushes their Łukasiewicz t-norm.
    Lukasiewicz(u16),

    /// Pops two values and pushes their conjunction,
    /// three-valued over effective boolean values.
    And,
    /// Pops two values and pushes their disjunction,
    /// three-valued over effective boolean values.
    Or,
    /// Pops a value and pushes the negated effective boolean value.
    Not,

    /// Pops a value and pushes whether its domain is in the set.
    InDomains(DomainSet),
    /// Pops a value and pushes its datatype IRI.
    ///
    /// Only a literal has one.
    Datatype,
    /// Pops a value and pushes its canonical form as a plain string:
    /// strings quoted, typed literals with their datatype.
    Canonical,
    /// Pops a value and pushes its lexical form as a plain string.
    ///
    /// A language string yields its text.
    Lexical,
    /// Pops a literal and pushes its language tag,
    /// the empty string if it has none.
    Language,
    /// Pops a plain string and a tag and pushes the tagged string.
    StringLanguage,
    /// Pops a tag and a range and pushes whether the tag matches the range:
    /// case-insensitive subtag prefixes, `*` for any nonempty tag.
    LanguageMatches,
    /// Pops a lexical and a datatype IRI and pushes the typed literal.
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
    /// Pops two strings and pushes the part of the first before the second.
    StringBefore,
    /// Pops two strings and pushes the part of the first after the second.
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
    /// Pops a string and a pattern, and flags if the field says so,
    /// and pushes whether the pattern matches.
    Matches {
        /// Whether a flags argument is popped too.
        flags: bool,
    },
    /// Pops a string, a pattern, and a replacement, and flags if the field says so,
    /// and pushes the string with the matches replaced.
    Replace {
        /// Whether a flags argument is popped too.
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
    /// Pops the given number of values and then a label below them,
    /// and pushes them as a tuple carrying that label.
    LabeledTuple(u16),
    /// Pops the given number of key–value pairs, each key pushed before its value,
    /// and pushes them as a map.
    ///
    /// A key given twice is an error.
    Map(u16),
    /// Pops the given number of key–value pairs and then a label below them,
    /// and pushes them as a map carrying that label.
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
    /// Pops a map and pushes its keys as a tuple,
    /// in the canonical term order the map stores them in.
    Keys,
    /// Pops a map and a key and pushes whether the map holds the key.
    HasKey,
    /// Pops a tuple of plain strings and a separator and pushes the strings joined by it.
    JoinStrings,

    /// Pops a dateTime, date or duration and pushes its year, or its whole years.
    Year,
    /// Pops a dateTime, date or duration and pushes its month, or its months beyond the years.
    Month,
    /// Pops a dateTime, date or duration and pushes its day, or its whole days.
    Day,
    /// Pops a dateTime, time or duration and pushes its hours.
    Hours,
    /// Pops a dateTime, time or duration and pushes its minutes.
    Minutes,
    /// Pops a dateTime, time or duration and pushes its seconds,
    /// a decimal if fractional.
    Seconds,
    /// Pops a dateTime, date, or time and pushes its timezone as a plain string,
    /// empty if absent.
    TimezoneString,
    /// Pops a dateTime, date, or time and pushes its timezone as an `xsd:dayTimeDuration`,
    /// an error if absent.
    TimezoneDuration,
    /// Pops a dateTime, date, or time and a dayTimeDuration,
    /// and pushes the value adjusted to the timezone the duration names.
    Adjust,

    /// Pushes a random double in `[0, 1)`.
    Random,
    /// Pushes a fresh UUID as a `urn:uuid:` IRI.
    UuidIri,
    /// Pushes a fresh UUID as a plain string.
    UuidString,
    /// Pushes the run's instant as an `xsd:dateTime` value,
    /// SPARQL's [`NOW`](https://www.w3.org/TR/sparql12-query/#func-now).
    /// The instant is the same for every evaluation of one run.
    Now,
}

impl OpCode {
    /// Returns the instruction's [Signature]: the domains it accepts and yields.
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
            // An exact base under an integral, non-negative exponent stays exact,
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
            OpCode::Canonical | OpCode::Lexical => Fixed(&[Plain(&[D::ANY], D::STRING)]),
            OpCode::Language => Fixed(&[Plain(&[D::LITERAL], D::STRING)]),
            OpCode::StringLanguage => Fixed(&[Plain(&[D::STRING, D::STRING], D::LANGUAGE_STRING)]),
            OpCode::LanguageMatches => Fixed(&[Plain(&[D::STRING, D::STRING], D::BOOLEAN)]),
            // The datatype decides the result's domain.
            OpCode::TypedLiteral => Fixed(&[Plain(&[D::ANY, D::ANY], D::ANY)]),
            // A cast also reads lexical forms,
            // so whether it succeeds is a question of the value, not of its domain.
            OpCode::CastInteger => Fixed(&[Plain(&[D::ANY], D::INTEGER)]),
            OpCode::CastFloat => Fixed(&[Plain(&[D::ANY], D::FLOAT)]),
            OpCode::CastDouble => Fixed(&[Plain(&[D::ANY], D::DOUBLE)]),
            OpCode::CastIri => Fixed(&[Plain(&[STRINGLIKE_OR_IRI], D::IRI)]),
            OpCode::CastDateTime => Fixed(&[Plain(&[D::ANY], D::DATE_TIME)]),
            OpCode::CastDate => Fixed(&[Plain(&[D::ANY], D::DATE)]),
            OpCode::CastTime => Fixed(&[Plain(&[D::ANY], D::TIME)]),
            OpCode::CastDuration => Fixed(&[Plain(&[D::ANY], D::DURATION)]),
            OpCode::CastDayTimeDuration => Fixed(&[Plain(&[D::ANY], D::DAY_TIME_DURATION)]),
            // The zero duration is a dayTimeDuration.
            OpCode::CastYearMonthDuration => Fixed(&[Plain(&[D::ANY], D::ORDERED_DURATION)]),

            OpCode::StringLength => Fixed(&[Plain(&[D::STRINGLIKE], D::INTEGER)]),
            // A transformed string keeps its language tag.
            OpCode::UpperCase
            | OpCode::LowerCase
            | OpCode::Reverse
            | OpCode::Trim
            | OpCode::TrimStart
            | OpCode::TrimEnd => Fixed(&[Keeps(&[D::STRINGLIKE], 0)]),
            // The second string is plain or carries the tag of the first.
            OpCode::Contains | OpCode::StartsWith | OpCode::EndsWith => Fixed(&[
                Plain(&[D::STRING, D::STRING], D::BOOLEAN),
                Plain(&[D::LANGUAGE_STRING, D::STRINGLIKE], D::BOOLEAN),
            ]),
            // A part of a tagged string is tagged,
            // but a miss yields a plain empty string.
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
            // A labeled structure pops its label below its content.
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

    /// Returns how many values the instruction pops, as its [signature](Self::signature) states;
    /// every instruction pushes one.
    pub(crate) fn pops(self) -> usize {
        self.signature().pops()
    }

    /// Returns whether the instruction's result is not fixed by its operands:
    /// randomness, a UUID, or the run's instant.
    pub(crate) fn is_nondeterministic(self) -> bool {
        matches!(
            self,
            OpCode::Random | OpCode::UuidIri | OpCode::UuidString | OpCode::Now
        )
    }
}

// The unions the signature table names beyond those of [DomainSet].

/// The domains that order within themselves:
/// strings, booleans, dateTimes, dates, times, and tuples.
///
/// Numbers and durations also order across their domains.
const ORDERED: DomainSet = DomainSet::STRING
    .with(ValueDomain::Boolean)
    .with(ValueDomain::DateTime)
    .with(ValueDomain::Date)
    .with(ValueDomain::Time)
    .with(ValueDomain::Tuple);

/// The floats and the doubles.
const FLOATING: DomainSet = DomainSet::FLOAT.union(DomainSet::DOUBLE);

/// The integers, the floats and the doubles: the exponents an exact power may stay exact under.
const INTEGER_OR_FLOATING: DomainSet = DomainSet::INTEGER.union(FLOATING);

/// The integers and the doubles.
const INTEGER_OR_DOUBLE: DomainSet = DomainSet::INTEGER.union(DomainSet::DOUBLE);

/// The exact numbers and the doubles.
const EXACT_OR_DOUBLE: DomainSet = DomainSet::EXACT.union(DomainSet::DOUBLE);

/// The IRIs and the nulls, which `=` finds unequal to a value of any other domain.
const IRI_OR_NULL: DomainSet = DomainSet::IRI.with(ValueDomain::Null);

/// The strings, plain and language-tagged, and the IRIs.
const STRINGLIKE_OR_IRI: DomainSet = DomainSet::STRINGLIKE.with(ValueDomain::Iri);

/// The IRIs and the plain strings.
const IRI_OR_STRING: DomainSet = DomainSet::IRI.with(ValueDomain::String);

/// The points in time and the durations.
const TEMPORAL_OR_DURATION: DomainSet = DomainSet::TEMPORAL.union(DomainSet::DURATION);
