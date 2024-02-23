//! This module defines functions that check for a data type of a value.

use crate::datavalues::{AnyDataValue, DataValue};

use super::UnaryFunction;

/// Check if value is integer
///
/// Returns "true" from the boolean value space if value is an integer and "false" otherwise.
#[derive(Debug, Copy, Clone)]
pub struct CheckIsInteger;
impl UnaryFunction for CheckIsInteger {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        if parameter.to_i64().is_some() {
            Some(AnyDataValue::new_boolean(true))
        } else {
            Some(AnyDataValue::new_boolean(false))
        }
    }
}

/// Check if value is float
///
/// Returns "true" from the boolean value space if value is a float and "false" otherwise.
#[derive(Debug, Copy, Clone)]
pub struct CheckIsFloat;
impl UnaryFunction for CheckIsFloat {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        if parameter.to_f32().is_some() {
            Some(AnyDataValue::new_boolean(true))
        } else {
            Some(AnyDataValue::new_boolean(false))
        }
    }
}

/// Check if value is double
///
/// Returns "true" from the boolean value space if value is a double and "false" otherwise.
#[derive(Debug, Copy, Clone)]
pub struct CheckIsDouble;
impl UnaryFunction for CheckIsDouble {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        if parameter.to_f64().is_some() {
            Some(AnyDataValue::new_boolean(true))
        } else {
            Some(AnyDataValue::new_boolean(false))
        }
    }
}

/// Check if value is numeric
///
/// Returns "true" from the boolean value space if value is numeric and "false" otherwise.
#[derive(Debug, Copy, Clone)]
pub struct CheckIsNumeric;
impl UnaryFunction for CheckIsNumeric {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        if parameter.to_i64().is_some()
            || parameter.to_f32().is_some()
            || parameter.to_f64().is_some()
        {
            Some(AnyDataValue::new_boolean(true))
        } else {
            Some(AnyDataValue::new_boolean(false))
        }
    }
}

/// Check if value is a null
///
/// Returns "true" from the boolean value space if value is a null and "false" otherwise.
#[derive(Debug, Copy, Clone)]
pub struct CheckIsNull;
impl UnaryFunction for CheckIsNull {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        if parameter.to_null().is_some() {
            Some(AnyDataValue::new_boolean(true))
        } else {
            Some(AnyDataValue::new_boolean(false))
        }
    }
}

/// Check if value is an iri
///
/// Returns "true" from the boolean value space if value is an iri and "false" otherwise.
#[derive(Debug, Copy, Clone)]
pub struct CheckIsIri;
impl UnaryFunction for CheckIsIri {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        if parameter.to_iri().is_some() {
            Some(AnyDataValue::new_boolean(true))
        } else {
            Some(AnyDataValue::new_boolean(false))
        }
    }
}

/// Check if value is a string
///
/// Returns "true" from the boolean value space if value is a string and "false" otherwise.
#[derive(Debug, Copy, Clone)]
pub struct CheckIsString;
impl UnaryFunction for CheckIsString {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        if parameter.to_plain_string().is_some() {
            Some(AnyDataValue::new_boolean(true))
        } else {
            Some(AnyDataValue::new_boolean(false))
        }
    }
}
