//! This module defines operations on boolean values

use crate::datavalues::{AnyDataValue, DataValue};

use super::{BinaryFunction, UnaryFunction};

/// Given two [AnyDataValue]s,
/// check if both are boolean and return a pair of [bool]
/// if this is the case.
/// Returns `None` otherwise.
fn bool_pair_from_any(
    parameter_first: AnyDataValue,
    parameter_second: AnyDataValue,
) -> Option<(bool, bool)> {
    if let Some(first_string) = parameter_first.to_boolean() {
        if let Some(second_string) = parameter_second.to_boolean() {
            return Some((first_string, second_string));
        }
    }

    None
}

/// Boolean conjunction
///
/// Returns `true` if both of its inputs are `true`.
///
/// Returns `None` if one of the inputs is not in the boolean value range.
#[derive(Debug, Copy, Clone)]
pub struct BooleanConjunction;
impl BinaryFunction for BooleanConjunction {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        bool_pair_from_any(parameter_first, parameter_second)
            .map(|(first_bool, second_bool)| AnyDataValue::new_boolean(first_bool && second_bool))
    }
}

/// Boolean disjunction
///
/// Returns `true` if at least one of its inputs is `true`.
///
/// Returns `None` if one of the inputs is not in the boolean value range.
#[derive(Debug, Copy, Clone)]
pub struct BooleanDisjunction;
impl BinaryFunction for BooleanDisjunction {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        bool_pair_from_any(parameter_first, parameter_second)
            .map(|(first_bool, second_bool)| AnyDataValue::new_boolean(first_bool || second_bool))
    }
}

/// Boolean Negation
///
/// Returns `true` if its input is `false`,
/// and returns `false` if its input it `true`.
///
/// Returns `None` if the input is not in the boolean value range.
#[derive(Debug, Copy, Clone)]
pub struct BooleanNegation;
impl UnaryFunction for BooleanNegation {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        parameter
            .to_boolean()
            .map(|value| AnyDataValue::new_boolean(!value))
    }
}
