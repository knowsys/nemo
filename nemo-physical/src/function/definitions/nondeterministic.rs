//! This module defines nondeterministic (side-effecting) built-in functions.

use rand::random;
use uuid::Uuid;

use crate::{
    datatypes::StorageTypeName,
    datavalues::AnyDataValue,
    function::definitions::{FunctionTypePropagation, NullaryFunction},
};

/// Return a pseudo-random double in the range [0, 1).
///
/// Corresponds to SPARQL `RAND()`.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct FuncRand;
impl NullaryFunction for FuncRand {
    fn evaluate(&self) -> Option<AnyDataValue> {
        let value: f64 = random();
        AnyDataValue::new_double_from_f64(value).ok()
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(StorageTypeName::Double.bitset())
    }

    fn is_nondeterministic(&self) -> bool {
        true
    }
}

/// Return a fresh UUID as an IRI.
///
/// Corresponds to SPARQL `UUID()`, which leaves the UUID version implementation-defined.
/// Returns a value of the form `<urn:uuid:…>` containing a version 7 UUID,
/// which is lexicographically greater than all UUIDs generated before it.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct FuncUuid;
impl NullaryFunction for FuncUuid {
    fn evaluate(&self) -> Option<AnyDataValue> {
        let iri = format!("urn:uuid:{}", Uuid::now_v7());
        Some(AnyDataValue::new_iri(iri))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }

    fn is_nondeterministic(&self) -> bool {
        true
    }
}

/// Return a fresh UUID as a plain string (without angle brackets).
///
/// Corresponds to SPARQL `STRUUID()`, which leaves the UUID version implementation-defined.
/// Returns a lowercase hyphenated version 7 UUID string,
/// which is lexicographically greater than all UUIDs generated before it.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct FuncStruuid;
impl NullaryFunction for FuncStruuid {
    fn evaluate(&self) -> Option<AnyDataValue> {
        Some(AnyDataValue::new_plain_string(Uuid::now_v7().to_string()))
    }

    fn type_propagation(&self) -> FunctionTypePropagation {
        FunctionTypePropagation::KnownOutput(
            StorageTypeName::Id32
                .bitset()
                .union(StorageTypeName::Id64.bitset()),
        )
    }

    fn is_nondeterministic(&self) -> bool {
        true
    }
}
