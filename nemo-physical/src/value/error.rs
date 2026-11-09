//! Errors of the operations on values.

use std::fmt;

/// The reason why an operation on values failed.
///
/// Following SPARQL's error semantics, an evaluation error is treated like a value.
/// It propagates through most operations,
/// and a row whose filter condition evaluates to an error is dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EvaluationError {
    /// An operand has a type that the operation is not defined on.
    ///
    /// No value of that type would be accepted. Examples:
    /// - a number where a string is required;
    /// - an IRI where a literal is required;
    /// - two durations of different kinds;
    /// - a decimal where an integer is required.
    TypeMismatch,
    /// Two values that cannot be compared were compared, by order or by value equality.
    ///
    /// Examples:
    /// - values of unrelated types;
    /// - a NaN in a three-way comparison;
    /// - a zoned and an unzoned dateTime that are too close to be ordered;
    /// - tuples of different shapes;
    /// - two distinct literals of a datatype without native support.
    Incomparable,
    /// A division by zero where no infinity can stand for the result,
    /// as for exact numbers and for durations.
    DivisionByZero,
    /// A computation exceeded a limit.
    ///
    /// Examples:
    /// - a number beyond the length limit of numbers;
    /// - a dateTime or a duration beyond its value space;
    /// - an infinity where a finite value is required;
    /// - a match beyond the limits of the regular-expression engine.
    Overflow,
    /// The result is a value that cannot be stored,
    /// such as a string beyond the dictionary's length limit.
    Unsupported,
    /// An operand has a type that the operation accepts, but a value that it does not accept,
    /// either alone or together with the other operands.
    ///
    /// Examples:
    /// - a string that is not a lexical form of a cast's target type;
    /// - an invalid pattern, flag or language tag;
    /// - a NaN factor for a duration;
    /// - a timezone offset out of range;
    /// - a key given twice for a map;
    /// - an empty separator for splitting a string.
    Malformed,
    /// A value lacks the component that was asked for.
    ///
    /// Examples:
    /// - an index out of range;
    /// - a key that the map does not contain;
    /// - the timezone offset of a dateTime without one.
    Missing,
}

impl EvaluationError {
    /// The number of error kinds.
    ///
    /// Every kind's [index](Self::index) is below it,
    /// so that counters per kind fit in an array of this length.
    pub const COUNT: usize = 7;

    /// Every kind, in the order of their [indices](Self::index).
    pub const ALL: [Self; Self::COUNT] = [
        EvaluationError::TypeMismatch,
        EvaluationError::Incomparable,
        EvaluationError::DivisionByZero,
        EvaluationError::Overflow,
        EvaluationError::Unsupported,
        EvaluationError::Malformed,
        EvaluationError::Missing,
    ];

    /// Returns the kind's index, which is below [COUNT](Self::COUNT) and differs for every kind.
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
