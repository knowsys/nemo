//! This module defines all supported generic functions.

use crate::{
    datavalues::{AnyDataValue, DataValue},
    storagevalues::{double::Double, float::Float, storagevalue::StorageValueT},
};

/// Trait for types for which generic operations are defined
pub(crate) trait OperableGeneric {
    /// Equal comparison
    ///
    /// Returns `true` from the boolean value space
    /// if both input values are equal and `false` if they are not.
    #[allow(unused)]
    fn equals(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Unequal comparison
    ///
    /// Returns `false` from the boolean value space
    /// if both input values are equal and `true` if they are not.
    #[allow(unused)]
    fn unequals(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Canonical string representation
    ///
    /// Returns the canonical string representation of the given value
    #[allow(unused)]
    fn canonical_string(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Lexical value
    ///
    /// Return the lexical value of the given value as a string.
    #[allow(unused)]
    fn lexical_value(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }

    /// Datatype of a value
    ///
    /// Returns the data type of the input parameter as a string.
    #[allow(unused)]
    fn datatype(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }
}

impl OperableGeneric for i64 {}
impl OperableGeneric for Float {}
impl OperableGeneric for Double {}
impl OperableGeneric for StorageValueT {}

impl OperableGeneric for AnyDataValue {
    fn equals(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        if first == second {
            Some(AnyDataValue::new_boolean(true))
        } else {
            Some(AnyDataValue::new_boolean(false))
        }
    }

    fn unequals(first: Self, second: Self) -> Option<Self>
    where
        Self: Sized,
    {
        if first == second {
            Some(AnyDataValue::new_boolean(false))
        } else {
            Some(AnyDataValue::new_boolean(true))
        }
    }

    fn canonical_string(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        Some(AnyDataValue::new_plain_string(parameter.canonical_string()))
    }

    fn lexical_value(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        let result = if let Some(value) = parameter
            .to_language_tagged_string()
            .map(|(value, _)| value)
        {
            value
        } else {
            parameter.lexical_value()
        };

        Some(AnyDataValue::new_plain_string(result))
    }

    fn datatype(parameter: Self) -> Option<Self>
    where
        Self: Sized,
    {
        Some(AnyDataValue::new_iri(parameter.datatype_iri()))
    }
}
