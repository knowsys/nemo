//! Errors of the operations on values.

use std::fmt;

/// Why an operation on values failed.
///
/// Following SPARQL error semantics, an evaluation error is a *value*:
/// it propagates through most operations,
/// and a row whose condition evaluates to an error is filtered out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EvaluationError {
    /// An operand has a type the operation is not defined on,
    /// so that no value of that type would do:
    /// a number where a string is required, an IRI for `DATATYPE`,
    /// durations of different kinds, a decimal where an integer is required.
    TypeMismatch,
    /// Two values were compared, for their order or for value equality,
    /// that cannot be compared:
    /// values of unrelated types, a NaN in a three-way comparison,
    /// a zoned and an unzoned point in time, tuples of different shapes,
    /// two distinct literals of an unknown datatype.
    Incomparable,
    /// A division by zero without an infinity to stand for the result:
    /// of exact numbers, or of a duration.
    DivisionByZero,
    /// A computation exceeded a limit:
    /// a number beyond the length limit of numbers,
    /// a point in time or a duration beyond its value space,
    /// an infinity where a finite value is required,
    /// or a match beyond the limits of the regular-expression engine.
    Overflow,
    /// The result is a value that has no storage representation,
    /// such as a string beyond the dictionary's length limit.
    Unsupported,
    /// An operand has a type the operation accepts, but a value it does not,
    /// alone or together with the other operands:
    /// a string that is not a lexical form of a cast's target type,
    /// an invalid pattern, flag or language tag, a NaN factor,
    /// a timezone out of range, a map key given twice.
    Malformed,
    /// A value lacks the component that was asked for:
    /// an index out of range, a key the map does not hold,
    /// the timezone of a point in time without one.
    Missing,
}

impl EvaluationError {
    /// The number of error kinds, whose discriminants are `0..COUNT`
    /// (for per-kind counters).
    pub const COUNT: usize = 7;

    /// Every kind in [index](Self::index) order.
    pub const ALL: [Self; Self::COUNT] = [
        EvaluationError::TypeMismatch,
        EvaluationError::Incomparable,
        EvaluationError::DivisionByZero,
        EvaluationError::Overflow,
        EvaluationError::Unsupported,
        EvaluationError::Malformed,
        EvaluationError::Missing,
    ];

    /// Returns the kind's index.
    pub fn index(self) -> usize {
        match self {
            EvaluationError::TypeMismatch => 0,
            EvaluationError::Incomparable => 1,
            EvaluationError::DivisionByZero => 2,
            EvaluationError::Overflow => 3,
            EvaluationError::Unsupported => 4,
            EvaluationError::Malformed => 5,
            EvaluationError::Missing => 6,
        }
    }
}

impl fmt::Display for EvaluationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            EvaluationError::TypeMismatch => "an operand has the wrong type",
            EvaluationError::Incomparable => "the operands cannot be compared",
            EvaluationError::DivisionByZero => "division by zero",
            EvaluationError::Overflow => "the computation exceeds a limit",
            EvaluationError::Unsupported => "the result cannot be stored",
            EvaluationError::Malformed => "an operand has an invalid value",
            EvaluationError::Missing => "the value has no such component",
        })
    }
}
