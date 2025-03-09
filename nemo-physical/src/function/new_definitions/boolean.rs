//! This module defines all supported boolean functions.

use crate::{
    datavalues::{AnyDataValue, DataValue},
    storagevalues::{double::Double, float::Float},
};

/// Trait for types on which boolean operations are defined
pub(crate) trait OperableBoolean: PartialEq {
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
        let mut result = Self::boolean_true();
        for parameter in parameters {
            if !Self::is_boolean(parameter) {
                return None;
            }

            if *parameter != Self::boolean_true() {
                result = Self::boolean_false();
            }
        }
        Some(result)
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
        let mut result = Self::boolean_false();
        for parameter in parameters {
            if !Self::is_boolean(parameter) {
                return None;
            }

            if *parameter != Self::boolean_true() {
                result = Self::boolean_false();
            }
        }
        Some(result)
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
        if !Self::is_boolean(&parameter) {
            return None;
        }

        if parameter == Self::boolean_true() {
            Some(Self::boolean_false())
        } else {
            Some(Self::boolean_true())
        }
    }

    /// Value representing `true` for this data type
    fn boolean_true() -> Self;

    /// Value representing `false` for this data type
    fn boolean_false() -> Self;

    /// Check if value is a boolean.
    fn is_boolean(value: &Self) -> bool;
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

    fn boolean_true() -> Self {
        AnyDataValue::new_boolean(true)
    }

    fn boolean_false() -> Self {
        AnyDataValue::new_boolean(false)
    }

    fn is_boolean(value: &Self) -> bool {
        value.to_boolean().is_some()
    }
}

impl OperableBoolean for i64 {
    fn boolean_true() -> Self {
        1
    }

    fn boolean_false() -> Self {
        0
    }

    fn is_boolean(_value: &Self) -> bool {
        true
    }
}

impl OperableBoolean for Float {
    fn boolean_true() -> Self {
        Float::new_unchecked(1.0)
    }

    fn boolean_false() -> Self {
        Float::new_unchecked(0.0)
    }

    fn is_boolean(_value: &Self) -> bool {
        true
    }
}

impl OperableBoolean for Double {
    fn boolean_true() -> Self {
        Double::new_unchecked(1.0)
    }

    fn boolean_false() -> Self {
        Double::new_unchecked(0.0)
    }

    fn is_boolean(_value: &Self) -> bool {
        true
    }
}
