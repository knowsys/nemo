//! Effective boolean values and three-valued logic.

use crate::{
    function::StackValue,
    value::{EvaluationError, ValueRef},
};

/// Returns the effective boolean value of a stack value; an error stays an error.
fn truth(value: &StackValue<'_>) -> Result<bool, EvaluationError> {
    value.as_result()?.effective_boolean_value()
}

/// Returns the three-valued conjunction,
/// SPARQL's [`logical-and`](https://www.w3.org/TR/sparql12-query/#func-logical-and):
/// false wins over an error on either side.
pub(crate) fn and(left: &StackValue<'_>, right: &StackValue<'_>) -> StackValue<'static> {
    match (truth(left), truth(right)) {
        (Ok(false), _) | (_, Ok(false)) => false.into(),
        (Ok(true), Ok(true)) => true.into(),
        (Err(error), _) | (_, Err(error)) => error.into(),
    }
}

/// Returns the three-valued disjunction,
/// SPARQL's [`logical-or`](https://www.w3.org/TR/sparql12-query/#func-logical-or):
/// true wins over an error on either side.
pub(crate) fn or(left: &StackValue<'_>, right: &StackValue<'_>) -> StackValue<'static> {
    match (truth(left), truth(right)) {
        (Ok(true), _) | (_, Ok(true)) => true.into(),
        (Ok(false), Ok(false)) => false.into(),
        (Err(error), _) | (_, Err(error)) => error.into(),
    }
}

/// Returns the negated effective boolean value,
/// SPARQL's [`logical-not`](https://www.w3.org/TR/sparql12-query/#func-logical-not).
pub(crate) fn not(value: ValueRef<'_>) -> StackValue<'static> {
    match value.effective_boolean_value() {
        Ok(value) => (!value).into(),
        Err(error) => error.into(),
    }
}
