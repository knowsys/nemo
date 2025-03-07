//! This module defines all supported functions relating to type checking.

use crate::{
    datavalues::{AnyDataValue, DataValue},
    storagevalues::{double::Double, float::Float, storagevalue::StorageValueT},
};

/// Trait for types that can return their value domain
pub(crate) trait OperableCheckType {
    /// Check if value is integer
    ///
    /// Returns "true" from the boolean value space if value is an integer and "false" otherwise.
    #[allow(unused)]
    fn check_is_integer(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Check if value is float
    ///
    /// Returns "true" from the boolean value space if value is a float and "false" otherwise.
    #[allow(unused)]
    fn check_is_float(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Check if value is double
    ///
    /// Returns "true" from the boolean value space if value is a double and "false" otherwise.
    #[allow(unused)]
    fn check_is_double(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Check if value is numeric
    ///
    /// Returns "true" from the boolean value space if value is numeric and "false" otherwise.
    #[allow(unused)]
    fn check_is_numeric(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Check if value is a null
    ///
    /// Returns "true" from the boolean value space if value is a null and "false" otherwise.
    #[allow(unused)]
    fn check_is_null(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Check if value is an iri
    ///
    /// Returns "true" from the boolean value space if value is an iri and "false" otherwise.
    #[allow(unused)]
    fn check_is_iri(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Check if value is a string
    ///
    /// Returns "true" from the boolean value space if value is a string and "false" otherwise.
    #[allow(unused)]
    fn check_is_string(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }
}

// Use default implementation for all storage values
impl OperableCheckType for i64 {}
impl OperableCheckType for Float {}
impl OperableCheckType for Double {}
impl OperableCheckType for StorageValueT {}

impl OperableCheckType for AnyDataValue {
    fn check_is_integer(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        if parameter.to_i64().is_some() {
            Some(AnyDataValue::new_boolean(true))
        } else {
            Some(AnyDataValue::new_boolean(false))
        }
    }

    fn check_is_float(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        if parameter.to_f32().is_some() {
            Some(AnyDataValue::new_boolean(true))
        } else {
            Some(AnyDataValue::new_boolean(false))
        }
    }

    fn check_is_double(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        if parameter.to_f64().is_some() {
            Some(AnyDataValue::new_boolean(true))
        } else {
            Some(AnyDataValue::new_boolean(false))
        }
    }

    fn check_is_numeric(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        if parameter.to_i64().is_some()
            || parameter.to_f32().is_some()
            || parameter.to_f64().is_some()
        {
            Some(AnyDataValue::new_boolean(true))
        } else {
            Some(AnyDataValue::new_boolean(false))
        }
    }

    fn check_is_null(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        if parameter.to_null().is_some() {
            Some(AnyDataValue::new_boolean(true))
        } else {
            Some(AnyDataValue::new_boolean(false))
        }
    }

    fn check_is_iri(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        if parameter.to_iri().is_some() {
            Some(AnyDataValue::new_boolean(true))
        } else {
            Some(AnyDataValue::new_boolean(false))
        }
    }

    fn check_is_string(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        if parameter.to_plain_string().is_some() {
            Some(AnyDataValue::new_boolean(true))
        } else {
            Some(AnyDataValue::new_boolean(false))
        }
    }
}
