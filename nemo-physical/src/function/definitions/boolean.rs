//! This module defines operations on boolean values

use crate::{
    datavalues::{AnyDataValue, DataValue},
    storagevalues::StorageTypeName,
};

use super::{FunctionTypePropagation, NaryFunction, UnaryFunction};

/// Given a list of [AnyDataValue]s,
/// check if all of them are boolean and return a list of [bool]
/// if this is the case.
/// Returns `None` otherwise.
fn bool_list_from_any(parameters: &[AnyDataValue]) -> Option<Vec<bool>> {
    let result = parameters
        .iter()
        .map(|parameter| parameter.to_boolean())
        .collect::<Option<Vec<_>>>()?;
    Some(result)
}

/// Boolean conjunction
///
/// Return `true` if all of its inputs are `true`.
///
/// Returns `true` if no parameters are given.
/// Returns `None` if one of the inputs is not in the boolean value range.
#[derive(Debug, Copy, Clone)]
pub struct BooleanConjunction;
impl NaryFunction for BooleanConjunction {
    fn evaluate(&self, parameters: &[AnyDataValue]) -> Option<AnyDataValue> {
        if let Some(bools) = bool_list_from_any(parameters) {
            let result = bools.into_iter().all(|bool| bool);
            Some(AnyDataValue::new_boolean(result))
        } else {
            None
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

/// Boolean disjunction
///
/// Return `true` if one of its inputs is `true`.
///
/// Returns `false` if no parameters are given.
/// Returns `None` if one of the inputs is not in the boolean value range.
#[derive(Debug, Copy, Clone)]
pub struct BooleanDisjunction;
impl NaryFunction for BooleanDisjunction {
    fn evaluate(&self, parameters: &[AnyDataValue]) -> Option<AnyDataValue> {
        if let Some(bools) = bool_list_from_any(parameters) {
            let result = bools.into_iter().any(|bool| bool);
            Some(AnyDataValue::new_boolean(result))
        } else {
            None
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

    fn type_propagation(&self) -> FunctionTypePropagation {
        // TODO: This is playing it save, one should probably give booleans a special status
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}
