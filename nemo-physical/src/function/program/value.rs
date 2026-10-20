//! Values on the evaluation stack.

use crate::value::{DataValue, EvaluationError, ValueRef};

/// A value on the evaluation stack.
///
/// - A row input, a program constant, and a result that needs no allocation are borrowed.
/// - A result an operation had to construct, such as a string or a wide number, is owned.
/// - An evaluation error is a value too,
///   which propagates through the operations as SPARQL's error semantics prescribe.
#[derive(Debug, Clone)]
pub enum StackValue<'a> {
    /// A borrowed value.
    Value(ValueRef<'a>),
    /// A freshly constructed value.
    Owned(DataValue),
    /// A propagating evaluation error.
    Error(EvaluationError),
}

impl StackValue<'_> {
    /// Returns the value as a view, or the propagating error.
    pub fn as_result(&self) -> Result<ValueRef<'_>, EvaluationError> {
        match self {
            StackValue::Value(value) => Ok(*value),
            StackValue::Owned(value) => Ok(value.as_ref()),
            StackValue::Error(error) => Err(*error),
        }
    }
}

impl<'a> From<ValueRef<'a>> for StackValue<'a> {
    fn from(value: ValueRef<'a>) -> Self {
        StackValue::Value(value)
    }
}

impl From<DataValue> for StackValue<'_> {
    fn from(value: DataValue) -> Self {
        StackValue::Owned(value)
    }
}

impl From<EvaluationError> for StackValue<'_> {
    fn from(error: EvaluationError) -> Self {
        StackValue::Error(error)
    }
}

impl From<Result<DataValue, EvaluationError>> for StackValue<'_> {
    fn from(result: Result<DataValue, EvaluationError>) -> Self {
        match result {
            Ok(value) => StackValue::Owned(value),
            Err(error) => StackValue::Error(error),
        }
    }
}

impl From<Result<bool, EvaluationError>> for StackValue<'_> {
    fn from(result: Result<bool, EvaluationError>) -> Self {
        match result {
            Ok(value) => value.into(),
            Err(error) => StackValue::Error(error),
        }
    }
}

impl From<bool> for StackValue<'_> {
    fn from(value: bool) -> Self {
        StackValue::Value(ValueRef::Boolean(value))
    }
}
