//! This module defines functions that are relevant for all data types.

use crate::datavalues::AnyDataValue;

use super::BinaryFunction;

/// Compares to values and returns `true` if they are equal
/// and `false` if they are not.
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

/// Compares to values and returns `false` if they are equal
/// and `true` if they are not.
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
