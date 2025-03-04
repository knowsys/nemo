//! This module defines all supported boolean functions.

use crate::datavalues::{AnyDataValue, DataValue};

/// Trait for types on which boolean operations are defined
pub(crate) trait OperableBoolean {
    /// Boolean conjunction
    ///
    /// Return `true` if all of its inputs are `true`.
    ///
    /// Returns `true` if no parameters are given.
    /// Returns `None` if one of the inputs is not in the boolean value range.
    #[allow(unused)]
    fn boolean_conjunction(parameters: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Boolean disjunction
    ///
    /// Return `true` if one of its inputs is `true`.
    ///
    /// Returns `false` if no parameters are given.
    /// Returns `None` if one of the inputs is not in the boolean value range.
    #[allow(unused)]
    fn boolean_disjunction(parameters: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Boolean negation
    ///
    /// Returns `true` if its input is `false`,
    /// and returns `false` if its input it `true`.
    ///
    /// Returns `None` if the input is not in the boolean value range.
    #[allow(unused)]
    fn boolean_negation(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }
}

impl OperableBoolean for AnyDataValue {
    fn boolean_conjunction(parameters: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        let mut result = true;
        for parameter in parameters {
            if let Some(value) = parameter.to_boolean() {
                result &= value;
            } else {
                return None;
            }
        }
        Some(AnyDataValue::new_boolean(result))
    }

    fn boolean_disjunction(parameters: &[Self]) -> Option<Self>
    where
        Self: Sized,
    {
        let mut result = false;
        for parameter in parameters {
            if let Some(value) = parameter.to_boolean() {
                result |= value;
            } else {
                return None;
            }
        }
        Some(AnyDataValue::new_boolean(result))
    }

    fn boolean_negation(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        parameter
            .to_boolean()
            .map(|value| AnyDataValue::new_boolean(!value))
    }
}
