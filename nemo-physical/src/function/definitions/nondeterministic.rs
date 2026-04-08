//! This module defines nondeterministic (side-effecting) built-in functions.
//!
//! These functions must never be constant-folded — they produce a fresh value
//! on every invocation. [`FunctionTree::is_nondeterministic`] returns `true`
//! for any tree that contains one of these functions, which prevents
//! `special_function()` from collapsing the tree to a `Constant`.

use rand::random;
use uuid::Uuid;

use crate::{
    datatypes::StorageTypeName,
    datavalues::AnyDataValue,
    function::definitions::{FunctionTypePropagation, NaryFunction},
};

/// Return a pseudo-random double in the range [0, 1).
///
/// Corresponds to SPARQL `RAND()`.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct FuncRand;
impl NaryFunction for FuncRand {
    fn evaluate(&self, _parameters: &[AnyDataValue]) -> Option<AnyDataValue> {
        let value: f64 = random();
        AnyDataValue::new_double_from_f64(value).ok()
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(StorageTypeName::Double.bitset())
    }
}

/// Return a fresh UUID as an IRI.
///
/// Corresponds to SPARQL `UUID()`.
/// Returns a value of the form `<urn:uuid:…>`.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct FuncUuid;
impl NaryFunction for FuncUuid {
    fn evaluate(&self, _parameters: &[AnyDataValue]) -> Option<AnyDataValue> {
        let iri = format!("urn:uuid:{}", Uuid::new_v4());
        Some(AnyDataValue::new_iri(iri))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}

/// Return a fresh UUID as a plain string (without angle brackets).
///
/// Corresponds to SPARQL `STRUUID()`.
/// Returns a lowercase hyphenated UUID string, e.g. `"f81d4fae-7dec-11d0-a765-00a0c91e6bf6"`.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct FuncStruuid;
impl NaryFunction for FuncStruuid {
    fn evaluate(&self, _parameters: &[AnyDataValue]) -> Option<AnyDataValue> {
        Some(AnyDataValue::new_plain_string(Uuid::new_v4().to_string()))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }
}
