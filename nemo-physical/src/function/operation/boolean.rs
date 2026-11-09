//! The logical connectives, in three-valued logic on effective boolean values.

use crate::{
    function::StackValue,
    value::{EvaluationError, ValueRef},
};

/// Returns the effective boolean value of a stack value, or its error if it is one.
fn truth(value: &StackValue<'_>) -> Result<bool, EvaluationError> {
    value.as_result()?.effective_boolean_value()
}

/// Returns the conjunction in three-valued logic,
/// SPARQL's [`logical-and`](https://www.w3.org/TR/sparql12-query/#func-logical-and).
///
/// If one operand is false, the result is false even if the other is an error.
pub(crate) fn and(left: &StackValue<'_>, right: &StackValue<'_>) -> StackValue<'static> {
    match (truth(left), truth(right)) {
        (Ok(false), _) | (_, Ok(false)) => false.into(),
        (Ok(true), Ok(true)) => true.into(),
        (Err(error), _) | (_, Err(error)) => error.into(),
    }
}

/// Returns the disjunction in three-valued logic,
/// SPARQL's [`logical-or`](https://www.w3.org/TR/sparql12-query/#func-logical-or).
///
/// If one operand is true, the result is true even if the other is an error.
pub(crate) fn or(left: &StackValue<'_>, right: &StackValue<'_>) -> StackValue<'static> {
    match (truth(left), truth(right)) {
        (Ok(true), _) | (_, Ok(true)) => true.into(),
        (Ok(false), Ok(false)) => false.into(),
        (Err(error), _) | (_, Err(error)) => error.into(),
    }
}

/// Returns the negation of the effective boolean value,
/// SPARQL's [`logical-not`](https://www.w3.org/TR/sparql12-query/#func-logical-not).
pub(crate) fn not(value: ValueRef<'_>) -> StackValue<'static> {
    match value.effective_boolean_value() {
        Ok(value) => (!value).into(),
        Err(error) => error.into(),
    }
}
