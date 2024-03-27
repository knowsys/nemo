//! This module defines functions that are relevant for all data types.

use crate::{
    datatypes::StorageTypeName,
    datavalues::{AnyDataValue, DataValue},
};

use super::{BinaryFunction, FunctionTypePropagation, UnaryFunction};

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

    fn type_propagation(&self) -> FunctionTypePropagation {
        // TODO: This is playing it save, one should probably give booleans a special status
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
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

    fn type_propagation(&self) -> FunctionTypePropagation {
        // TODO: This is playing it save, one should probably give booleans a special status
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

/// Canonical string representation
///
/// Returns the canonical string representation of the given value
#[derive(Debug, Copy, Clone)]
pub struct CanonicalString;
impl UnaryFunction for CanonicalString {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        Some(AnyDataValue::new_plain_string(parameter.canonical_string()))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

/// Lexical value
///
/// Return the lexical value of the given value as a string.
#[derive(Debug, Copy, Clone)]
pub struct LexicalValue;
impl UnaryFunction for LexicalValue {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
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

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

/// Datatype of a value
///
/// Returns the data type of the input parameter as a string.
#[derive(Debug, Copy, Clone)]
pub struct Datatype;
impl UnaryFunction for Datatype {
    fn evaluate(&self, parameter: AnyDataValue) -> Option<AnyDataValue> {
        Some(AnyDataValue::new_plain_string(parameter.datatype_iri()))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}
