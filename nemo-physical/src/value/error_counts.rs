//! Counts of evaluation errors by kind.

use crate::value::EvaluationError;

/// The number of evaluation errors of each kind.
///
/// # Example
///
/// ```
/// use nemo_physical::value::{ErrorCounts, EvaluationError};
///
/// let mut counts = ErrorCounts::default();
/// counts.record(EvaluationError::TypeMismatch);
/// counts.record(EvaluationError::TypeMismatch);
///
/// let mut more = ErrorCounts::default();
/// more.record(EvaluationError::DivisionByZero);
/// counts.absorb(&more);
///
/// assert_eq!(counts.count(EvaluationError::TypeMismatch), 2);
/// assert_eq!(counts.total(), 3);
/// assert_eq!(
///     counts.iter().collect::<Vec<_>>(),
///     [(EvaluationError::TypeMismatch, 2), (EvaluationError::DivisionByZero, 1)]
/// );
/// ```
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ErrorCounts([u64; EvaluationError::COUNT]);

impl ErrorCounts {
    /// Counts one error of the given kind.
    pub fn record(&mut self, error: EvaluationError) {
        self.0[error.index()] += 1;
    }

    /// Returns the number of errors of the given kind.
    pub fn count(&self, error: EvaluationError) -> u64 {
        self.0[error.index()]
    }

    /// Returns the number of errors of every kind together.
    pub fn total(&self) -> u64 {
        self.0.iter().sum()
    }

    /// Returns whether no error was counted.
    pub fn is_empty(&self) -> bool {
        self.0.iter().all(|&count| count == 0)
    }

    /// Adds the counts of another to these.
    pub fn absorb(&mut self, other: &ErrorCounts) {
        for (total, count) in self.0.iter_mut().zip(other.0) {
            *total += count;
        }
    }

    /// Returns the kinds counted at least once, with their counts,
    /// in the order of [EvaluationError::ALL].
    pub fn iter(&self) -> impl Iterator<Item = (EvaluationError, u64)> + '_ {
        EvaluationError::ALL
            .into_iter()
            .zip(self.0)
            .filter(|&(_, count)| count > 0)
    }
}
