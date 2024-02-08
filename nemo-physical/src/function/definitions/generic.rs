//! This module defines functions that are relevant for all data types.

use crate::datavalues::{AnyDataValue, DataValue};

use super::{BinaryFunction, UnaryFunction};

/// Equal comparison
///
/// Returns `true` from the boolean value space
/// if both input values are equal and `false` if they are not.
#[derive(Debug, Copy, Clone)]
pub struct Equals;
impl BinaryFunction for Equals {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        if parameter_first == parameter_second {
            Some(AnyDataValue::new_boolean(true))
        } else {
            Some(AnyDataValue::new_boolean(false))
        }
    }
}

/// Unequal comparison
///
/// Returns `false` from the boolean value space
/// if both input values are equal and `true` if they are not.
#[derive(Debug, Copy, Clone)]
pub struct Unequals;
impl BinaryFunction for Unequals {
    fn evaluate(
        &self,
        parameter_first: AnyDataValue,
        parameter_second: AnyDataValue,
    ) -> Option<AnyDataValue> {
        if parameter_first == parameter_second {
            Some(AnyDataValue::new_boolean(false))
        } else {
            Some(AnyDataValue::new_boolean(true))
        }
    }
}

/// Canonical string representation
///
/// Returns the canonical string representation of the given value
#[derive(Debug, Copy, Clone)]
pub struct CanonicalString;
impl UnaryFunction for CanonicalString {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        Some(AnyDataValue::new_string(parameter.canonical_string()))
    }
}
